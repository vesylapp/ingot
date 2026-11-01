//! Executing run queue items: SDK calls and opcode handling.

use super::driver::{DriverRequest, Op, Response};
use super::{Engine, LEASE_MS};
use crate::store::{MetaUpdate, SaveStep};
use crate::types::*;
use crate::util::{backoff_ms, parse_date_ms, parse_duration_ms, parse_timeout_at, sha1_uuid};
use serde_json::{Map, Value, json};
use std::sync::Arc;

/// Maximum sleep / wait duration.
const MAX_WAIT_MS: i64 = 366 * 86_400_000;
const MAX_OPS: usize = 1000;
/// Delay before re-checking an item blocked on a saturated concurrency key.
const BLOCKED_RETRY_MS: i64 = 5_000;

/// What the worker should do with an item after processing.
#[derive(Debug)]
pub enum Outcome {
    /// Remove the item.
    Done,
    /// Retry at `at` with the given attempt number.
    Retry { at: i64, attempt: u32 },
    /// Put back without consuming an attempt.
    Delay { at: i64, blocked_on: Option<String> },
}

fn err_obj(name: &str, message: &str) -> Value {
    json!({ "error": format!("{name}: {message}"), "name": name, "message": message })
}

impl Engine {
    pub async fn execute_item(self: &Arc<Self>, item: &mut QueueItem) -> anyhow::Result<Outcome> {
        let Some(run_id) = item.run_id else {
            return Ok(Outcome::Done);
        };
        let Some(mut state) = self.state.load_run(run_id).await? else {
            return Ok(Outcome::Done);
        };
        if state.meta.status.is_terminal() {
            return Ok(Outcome::Done);
        }
        let Some(f) = self.registry.get(state.meta.fn_id) else {
            self.finalize(
                &state.meta,
                &state.events,
                RunStatus::Failed,
                None,
                Some(err_obj("FunctionNotFound", "function no longer exists")),
                Some(&item.id),
            )
            .await?;
            return Ok(Outcome::Done);
        };
        let now = now_ms();

        // Timeouts.
        if let Some(start) = f.timeout_start_ms
            && state.meta.started_at.is_none()
            && now - state.meta.created_at > start
        {
            self.cancel_run_with(&state.meta, &state.events, Some(&item.id)).await?;
            return Ok(Outcome::Done);
        }
        if let (Some(finish), Some(started)) = (f.timeout_finish_ms, state.meta.started_at)
            && now - started > finish
        {
            self.cancel_run_with(&state.meta, &state.events, Some(&item.id)).await?;
            return Ok(Outcome::Done);
        }

        // Throttle applies to starting runs only, once.
        if item.kind == QueueKind::Start
            && let Some(t) = &state.meta.throttle
            && item.payload.get("throttled").is_none()
        {
            let emission = (t.period_ms / t.limit.max(1) as i64).max(1);
            let tolerance = emission * (t.limit as i64 + t.burst as i64 - 1).max(0);
            if let Err(retry_at) = self
                .state
                .gcra(&format!("throttle:{}", t.key), now, emission, tolerance)
                .await?
            {
                return Ok(Outcome::Delay {
                    at: retry_at,
                    blocked_on: None,
                });
            }
            if let Value::Object(m) = &mut item.payload {
                m.insert("throttled".into(), Value::Bool(true));
            }
        }

        // Concurrency.
        if !state.meta.concurrency.is_empty() {
            if let Some(key) = self
                .state
                .acquire_concurrency(&item.id, &state.meta.concurrency, now + LEASE_MS)
                .await?
            {
                return Ok(Outcome::Delay {
                    at: now + BLOCKED_RETRY_MS,
                    blocked_on: Some(key),
                });
            }
        }

        let result = self.execute_inner(&f, &mut state, item).await;

        if !state.meta.concurrency.is_empty() {
            let keys = self.state.release_concurrency(&item.id).await?;
            if !keys.is_empty() {
                self.state.wake_blocked(&keys, 1).await?;
                self.queue_notify.notify_one();
            }
        }
        result
    }

    async fn execute_inner(
        self: &Arc<Self>,
        f: &Arc<Function>,
        state: &mut RunState,
        item: &QueueItem,
    ) -> anyhow::Result<Outcome> {
        let run_id = state.meta.run_id;

        // A sleep wake-up resolves the sleep step, then continues with discovery.
        if item.kind == QueueKind::Sleep {
            let step_id = item.str_payload("step_id").unwrap_or_default().to_string();
            match self.state.save_step(run_id, &step_id, &Value::Null).await? {
                SaveStep::NoRun => return Ok(Outcome::Done),
                SaveStep::Saved { pending_empty, .. } => {
                    if !pending_empty {
                        return Ok(Outcome::Done);
                    }
                    self.maybe_reset_force_plan(&mut state.meta).await?;
                }
            }
            state.steps = self.state.load_steps(run_id).await?;
        }

        if state.meta.started_at.is_none() {
            let now = now_ms();
            self.state
                .update_meta(
                    run_id,
                    &MetaUpdate {
                        started_at: Some(now),
                        ..Default::default()
                    },
                )
                .await?;
            state.meta.started_at = Some(now);
            if let Some(finish) = f.timeout_finish_ms {
                let item = QueueItem::new(
                    Some(format!("{run_id}-finish-timeout")),
                    QueueKind::Cancel,
                    now + finish,
                    json!({ "reason": "finish_timeout" }),
                )
                .for_run(&state.meta);
                self.enqueue(item).await?;
            }
        }

        let step_id = match item.kind {
            QueueKind::Sleep => "step".to_string(),
            _ => item.str_payload("incoming").unwrap_or("step").to_string(),
        };
        let result = self
            .call_sdk(DriverRequest {
                function: f,
                state,
                step_id: &step_id,
                attempt: item.attempt,
                max_attempts: item.max_attempts,
                queue_item_id: &item.id,
            })
            .await;

        if let Some(v) = result.request_version
            && state.meta.request_version != v
            && state.meta.request_version == -1
        {
            self.state
                .update_meta(
                    run_id,
                    &MetaUpdate {
                        request_version: Some(v),
                        ..Default::default()
                    },
                )
                .await?;
            state.meta.request_version = v;
        }

        match result.response {
            Response::Complete(output) => {
                self.finalize(&state.meta, &state.events, RunStatus::Completed, Some(output), None, Some(&item.id))
                    .await?;
                Ok(Outcome::Done)
            }
            Response::Error {
                error,
                retryable,
                retry_at,
            } => {
                if retryable && item.attempt + 1 < item.max_attempts {
                    let at = retry_at.unwrap_or_else(|| {
                        now_ms() + backoff_ms(item.attempt, self.cfg.retry_interval_s)
                    });
                    tracing::debug!(run_id = %run_id, attempt = item.attempt, "function error, retrying");
                    return Ok(Outcome::Retry {
                        at,
                        attempt: item.attempt + 1,
                    });
                }
                self.finalize(&state.meta, &state.events, RunStatus::Failed, None, Some(error), Some(&item.id))
                    .await?;
                Ok(Outcome::Done)
            }
            Response::Ops {
                ops,
                no_retry,
                retry_at,
            } => self.handle_ops(f, state, item, ops, no_retry, retry_at).await,
        }
    }

    async fn handle_ops(
        self: &Arc<Self>,
        f: &Arc<Function>,
        state: &mut RunState,
        item: &QueueItem,
        mut ops: Vec<Op>,
        no_retry: bool,
        retry_at: Option<i64>,
    ) -> anyhow::Result<Outcome> {
        let run_id = state.meta.run_id;
        if ops.is_empty() {
            return Ok(Outcome::Done);
        }
        if ops.iter().all(Op::is_empty_none) {
            // Nothing executable came back; re-drive discovery.
            let job = format!("{run_id}-redrive-{}", ulid::Ulid::generate());
            self.enqueue_discovery(&state.meta, "redrive", Some(job)).await?;
            return Ok(Outcome::Done);
        }
        if ops.len() > MAX_OPS {
            self.finalize(
                &state.meta,
                &state.events,
                RunStatus::Failed,
                None,
                Some(err_obj("StepLimitExceeded", "too many steps reported in one response")),
                Some(&item.id),
            )
            .await?;
            return Ok(Outcome::Done);
        }

        // Checkpointed completion: persist any steps, then finish.
        if let Some(pos) = ops.iter().position(|o| o.op == "RunComplete") {
            for op in ops.iter().filter(|o| o.op == "StepRun" || o.op == "Step") {
                let v = step_value(op);
                self.state.save_step(run_id, &op.id, &v).await?;
            }
            let output = ops[pos].data.clone().unwrap_or(Value::Null);
            self.finalize(&state.meta, &state.events, RunStatus::Completed, Some(output), None, Some(&item.id))
                .await?;
            return Ok(Outcome::Done);
        }

        let async_ids: Vec<String> = ops
            .iter()
            .filter(|o| !matches!(o.op.as_str(), "None" | "DeferAdd" | "DeferAbort"))
            .map(|o| o.id.clone())
            .collect();
        if async_ids.len() > 1 {
            if !state.meta.force_step_plan {
                self.state
                    .update_meta(
                        run_id,
                        &MetaUpdate {
                            force_step_plan: Some(true),
                            ..Default::default()
                        },
                    )
                    .await?;
                state.meta.force_step_plan = true;
            }
            if state.meta.request_version >= 2 {
                self.state.set_pending(run_id, &async_ids).await?;
            }
        }

        // Waits are registered before anything else so racing events can't be missed.
        ops.sort_by_key(|o| if o.op == "WaitForEvent" { 0 } else { 1 });
        for op in &ops {
            match op.op.as_str() {
                "StepRun" | "Step" => {
                    self.step_completed(state, &op.id, &step_value(op), "", is_race(op))
                        .await?;
                }
                "StepError" => {
                    if !no_retry && item.attempt + 1 < item.max_attempts {
                        let at = retry_at.unwrap_or_else(|| {
                            now_ms() + backoff_ms(item.attempt, self.cfg.retry_interval_s)
                        });
                        return Ok(Outcome::Retry {
                            at,
                            attempt: item.attempt + 1,
                        });
                    }
                    let err = op.error.clone().unwrap_or_else(|| err_obj("Error", "step failed"));
                    self.step_completed(state, &op.id, &json!({ "error": err }), "-failure", false)
                        .await?;
                }
                "StepFailed" => {
                    let err = op.error.clone().unwrap_or_else(|| err_obj("Error", "step failed"));
                    self.step_completed(state, &op.id, &json!({ "error": err }), "-failure", false)
                        .await?;
                }
                "StepPlanned" => {
                    let mut job = format!("{run_id}-{}-plan", op.id);
                    if item.str_payload("incoming") == Some(op.id.as_str()) {
                        job = format!("{job}-{}", ulid::Ulid::generate());
                    }
                    let edge = QueueItem::new(
                        Some(job),
                        QueueKind::Edge,
                        now_ms(),
                        json!({ "incoming": op.id }),
                    )
                    .for_run(&state.meta);
                    self.enqueue(edge).await?;
                }
                "Sleep" => {
                    if let Some(o) = self.op_sleep(state, item, op).await? {
                        return Ok(o);
                    }
                }
                "WaitForEvent" => {
                    if let Some(o) = self.op_wait_for_event(state, item, op).await? {
                        return Ok(o);
                    }
                }
                "InvokeFunction" => {
                    if let Some(o) = self.op_invoke(f, state, item, op).await? {
                        return Ok(o);
                    }
                }
                "WaitForSignal" => {
                    if let Some(o) = self.op_wait_for_signal(state, item, op).await? {
                        return Ok(o);
                    }
                }
                "Gateway" => {
                    if let Some(o) = self.op_gateway(state, item, op).await? {
                        return Ok(o);
                    }
                }
                "AIGateway" => {
                    if let Some(o) = self.op_ai_gateway(state, item, op).await? {
                        return Ok(o);
                    }
                }
                "DiscoveryRequest" => {
                    self.enqueue_discovery(&state.meta, &op.id, None).await?;
                }
                "None" | "DeferAdd" | "DeferAbort" | "SyncRunComplete" => {}
                other => {
                    tracing::warn!(run_id = %run_id, op = other, "unsupported opcode");
                }
            }
        }
        Ok(Outcome::Done)
    }

    /// Save a step result and continue execution if no parallel steps remain.
    pub async fn step_completed(
        self: &Arc<Self>,
        state: &mut RunState,
        step_id: &str,
        value: &Value,
        job_suffix: &str,
        race: bool,
    ) -> anyhow::Result<()> {
        match self.state.save_step(state.meta.run_id, step_id, value).await? {
            SaveStep::NoRun => Ok(()),
            SaveStep::Saved { pending_empty, .. } => {
                if pending_empty || race {
                    self.maybe_reset_force_plan(&mut state.meta).await?;
                    let job = format!("{}-{step_id}{job_suffix}", state.meta.run_id);
                    self.enqueue_discovery(&state.meta, step_id, Some(job)).await?;
                }
                Ok(())
            }
        }
    }

    pub async fn maybe_reset_force_plan(&self, meta: &mut RunMeta) -> anyhow::Result<()> {
        if meta.request_version >= 2 && meta.force_step_plan {
            self.state
                .update_meta(
                    meta.run_id,
                    &MetaUpdate {
                        force_step_plan: Some(false),
                        ..Default::default()
                    },
                )
                .await?;
            meta.force_step_plan = false;
        }
        Ok(())
    }

    pub async fn enqueue_discovery(
        &self,
        meta: &RunMeta,
        outgoing: &str,
        job: Option<String>,
    ) -> anyhow::Result<()> {
        let job = job.unwrap_or_else(|| format!("{}-{outgoing}", meta.run_id));
        let item = QueueItem::new(
            Some(job),
            QueueKind::Edge,
            now_ms(),
            json!({ "incoming": "step", "outgoing": outgoing }),
        )
        .for_run(meta);
        self.enqueue(item).await?;
        Ok(())
    }

    /// Fail the current step (saving the error) and continue the run.
    async fn fail_step(
        self: &Arc<Self>,
        state: &mut RunState,
        step_id: &str,
        name: &str,
        message: &str,
    ) -> anyhow::Result<()> {
        let err = json!({ "error": { "name": name, "message": message } });
        self.step_completed(state, step_id, &err, "-failure", false).await
    }

    async fn op_sleep(
        self: &Arc<Self>,
        state: &mut RunState,
        item: &QueueItem,
        op: &Op,
    ) -> anyhow::Result<Option<Outcome>> {
        let now = now_ms();
        let spec = op
            .opt_str("duration")
            .filter(|s| !s.is_empty())
            .or(op.name.as_deref())
            .unwrap_or_default();
        let wake = if spec.len() >= 10 {
            parse_date_ms(spec).or_else(|| parse_duration_ms(spec).map(|d| now + d))
        } else {
            parse_duration_ms(spec).map(|d| now + d)
        };
        let Some(wake) = wake else {
            self.finalize(
                &state.meta,
                &state.events,
                RunStatus::Failed,
                None,
                Some(err_obj("InvalidSleep", &format!("invalid sleep duration: {spec}"))),
                Some(&item.id),
            )
            .await?;
            return Ok(Some(Outcome::Done));
        };
        if wake - now > MAX_WAIT_MS {
            self.finalize(
                &state.meta,
                &state.events,
                RunStatus::Failed,
                None,
                Some(err_obj("InvalidSleep", "sleep duration exceeds 366 days")),
                Some(&item.id),
            )
            .await?;
            return Ok(Some(Outcome::Done));
        }
        let sleep = QueueItem::new(
            Some(format!("{}-{}-sleep", state.meta.run_id, op.id)),
            QueueKind::Sleep,
            wake.max(now),
            json!({ "step_id": op.id }),
        )
        .for_run(&state.meta);
        // Sleep items must never be shifted by priority.
        let sleep = QueueItem {
            at: wake.max(now),
            ..sleep
        };
        self.enqueue(sleep).await?;
        Ok(None)
    }

    async fn op_wait_for_event(
        self: &Arc<Self>,
        state: &mut RunState,
        item: &QueueItem,
        op: &Op,
    ) -> anyhow::Result<Option<Outcome>> {
        let now = now_ms();
        let event = op
            .opt_str("event")
            .filter(|s| !s.is_empty())
            .or(op.name.as_deref())
            .unwrap_or_default()
            .to_string();
        let timeout = op.opt_str("timeout").unwrap_or_default();
        let expires_at = if timeout.is_empty() {
            Some(now)
        } else {
            parse_timeout_at(timeout, now)
        };
        let Some(expires_at) = expires_at.filter(|e| e - now <= MAX_WAIT_MS) else {
            self.finalize(
                &state.meta,
                &state.events,
                RunStatus::Failed,
                None,
                Some(err_obj("InvalidTimeout", &format!("invalid waitForEvent timeout: {timeout}"))),
                Some(&item.id),
            )
            .await?;
            return Ok(Some(Outcome::Done));
        };
        let expression = op.opt_str("if").filter(|s| !s.is_empty()).map(String::from);
        if let Some(e) = &expression
            && let Err(err) = self.compile(e)
        {
            self.fail_step(state, &op.id, "InvalidExpression", &err.to_string())
                .await?;
            return Ok(None);
        }
        let run_id = state.meta.run_id;
        let timeout_job = format!("{run_id}-{}-pause", op.id);
        let pause = Pause {
            id: sha1_uuid(&format!("{run_id}{}", op.id)),
            kind: PauseKind::WaitForEvent,
            run_id,
            fn_id: state.meta.fn_id,
            step_id: op.id.clone(),
            event: Some(event),
            expression,
            trigger_event: state.events.first().cloned(),
            trigger_event_id: state.meta.event_ids.first().copied(),
            expires_at,
            created_at: now,
            timeout_job: Some(timeout_job.clone()),
            correlation_id: None,
        };
        self.save_pause_with_timeout(&state.meta, &pause, timeout_job).await?;
        Ok(None)
    }

    async fn save_pause_with_timeout(
        &self,
        meta: &RunMeta,
        pause: &Pause,
        timeout_job: String,
    ) -> anyhow::Result<()> {
        self.state.save_pause(pause).await?;
        let item = QueueItem::new(
            Some(timeout_job),
            QueueKind::Pause,
            pause.expires_at,
            json!({ "pause_id": pause.id }),
        )
        .for_run(meta);
        let item = QueueItem {
            at: pause.expires_at,
            ..item
        };
        self.enqueue(item).await?;
        Ok(())
    }

    async fn op_invoke(
        self: &Arc<Self>,
        f: &Arc<Function>,
        state: &mut RunState,
        item: &QueueItem,
        op: &Op,
    ) -> anyhow::Result<Option<Outcome>> {
        let now = now_ms();
        let opts = op.opts.clone().unwrap_or(Value::Null);
        let target = opts
            .get("function_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let timeout = opts.get("timeout").and_then(|v| v.as_str()).unwrap_or_default();
        let expires_at = if timeout.is_empty() {
            now + 365 * 86_400_000
        } else {
            match parse_timeout_at(timeout, now) {
                Some(e) => e,
                None => {
                    self.finalize(
                        &state.meta,
                        &state.events,
                        RunStatus::Failed,
                        None,
                        Some(err_obj("InvalidTimeout", &format!("invalid invoke timeout: {timeout}"))),
                        Some(&item.id),
                    )
                    .await?;
                    return Ok(Some(Outcome::Done));
                }
            }
        };
        let run_id = state.meta.run_id;
        let correlation_id = format!("{run_id}.{}", op.id);
        let timeout_job = format!("{run_id}-{}-pause", op.id);
        let pause = Pause {
            id: sha1_uuid(&format!("{run_id}{}", op.id)),
            kind: PauseKind::Invoke,
            run_id,
            fn_id: state.meta.fn_id,
            step_id: op.id.clone(),
            event: Some(EVENT_FN_FINISHED.into()),
            expression: None,
            trigger_event: None,
            trigger_event_id: None,
            expires_at,
            created_at: now,
            timeout_job: Some(timeout_job.clone()),
            correlation_id: Some(correlation_id.clone()),
        };
        self.save_pause_with_timeout(&state.meta, &pause, timeout_job).await?;

        let payload = opts.get("payload").cloned().unwrap_or(Value::Null);
        let mut data: Map<String, Value> = payload
            .get("data")
            .and_then(|d| d.as_object())
            .cloned()
            .unwrap_or_default();
        data.insert(
            "_inngest".into(),
            json!({
                "source_app_id": f.app_id,
                "source_fn_id": f.id,
                "source_fn_v": f.version,
                "fn_id": target,
                "correlation_id": correlation_id,
                "expire": expires_at,
                "gid": ulid::Ulid::generate().to_string(),
                "name": op.display_name.clone().or(op.name.clone()).unwrap_or_default(),
            }),
        );
        self.publish_internal(vec![Event {
            id: payload
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            name: EVENT_FN_INVOKED.into(),
            data,
            user: payload.get("user").cloned(),
            ts: payload.get("ts").and_then(|v| v.as_i64()).unwrap_or(0),
            v: payload.get("v").and_then(|v| v.as_str()).map(String::from),
            meta: None,
        }])
        .await?;
        Ok(None)
    }

    async fn op_wait_for_signal(
        self: &Arc<Self>,
        state: &mut RunState,
        item: &QueueItem,
        op: &Op,
    ) -> anyhow::Result<Option<Outcome>> {
        let now = now_ms();
        let signal = op.opt_str("signal").unwrap_or_default().to_string();
        let timeout = op.opt_str("timeout").unwrap_or_default();
        let expires_at = if timeout.is_empty() {
            Some(now)
        } else {
            parse_timeout_at(timeout, now)
        };
        let Some(expires_at) = expires_at.filter(|e| e - now <= MAX_WAIT_MS) else {
            self.finalize(
                &state.meta,
                &state.events,
                RunStatus::Failed,
                None,
                Some(err_obj("InvalidTimeout", &format!("invalid waitForSignal timeout: {timeout}"))),
                Some(&item.id),
            )
            .await?;
            return Ok(Some(Outcome::Done));
        };
        let run_id = state.meta.run_id;
        for existing in self.state.pauses_for_signal(&signal).await? {
            if existing.run_id == run_id && existing.step_id == op.id {
                continue;
            }
            if op.opt_str("conflict") == Some("replace") {
                self.state.delete_pause(existing.id).await?;
                if let Some(job) = &existing.timeout_job {
                    self.state.remove_item(&crate::util::hash36(job)).await?;
                }
            } else {
                self.finalize(
                    &state.meta,
                    &state.events,
                    RunStatus::Failed,
                    None,
                    Some(err_obj(
                        "SignalConflict",
                        &format!("another run is already waiting for signal {signal}"),
                    )),
                    Some(&item.id),
                )
                .await?;
                return Ok(Some(Outcome::Done));
            }
        }
        let timeout_job = format!("{run_id}-{}-pause", op.id);
        let pause = Pause {
            id: sha1_uuid(&format!("{run_id}{}", op.id)),
            kind: PauseKind::Signal,
            run_id,
            fn_id: state.meta.fn_id,
            step_id: op.id.clone(),
            event: Some(signal),
            expression: None,
            trigger_event: None,
            trigger_event_id: None,
            expires_at,
            created_at: now,
            timeout_job: Some(timeout_job.clone()),
            correlation_id: None,
        };
        self.save_pause_with_timeout(&state.meta, &pause, timeout_job).await?;
        Ok(None)
    }

    async fn op_gateway(
        self: &Arc<Self>,
        state: &mut RunState,
        item: &QueueItem,
        op: &Op,
    ) -> anyhow::Result<Option<Outcome>> {
        let opts = op.opts.clone().unwrap_or(Value::Null);
        let url = opts.get("url").and_then(|v| v.as_str()).unwrap_or_default();
        let method = opts
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("GET")
            .to_uppercase();
        let mut rb = self.http.request(
            reqwest::Method::from_bytes(method.as_bytes()).unwrap_or(reqwest::Method::GET),
            url,
        );
        if let Some(h) = opts.get("headers").and_then(|h| h.as_object()) {
            for (k, v) in h {
                if let Some(v) = v.as_str() {
                    rb = rb.header(k, v);
                }
            }
        }
        if let Some(body) = opts.get("body").and_then(|b| b.as_str()) {
            rb = rb.body(body.to_string());
        }
        match rb.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let mut headers = Map::new();
                for (k, v) in resp.headers() {
                    let v = v.to_str().unwrap_or_default().to_string();
                    match headers.get_mut(k.as_str()) {
                        Some(Value::String(existing)) => {
                            existing.push_str(", ");
                            existing.push_str(&v);
                        }
                        _ => {
                            headers.insert(k.as_str().to_string(), Value::String(v));
                        }
                    }
                }
                let body = resp.text().await.unwrap_or_default();
                let value = json!({ "data": {
                    "url": url, "headers": headers, "body": body, "status_code": status,
                }});
                self.step_completed(state, &op.id, &value, "", false).await?;
                Ok(None)
            }
            Err(err) => {
                if item.attempt + 1 < item.max_attempts {
                    return Ok(Some(Outcome::Retry {
                        at: now_ms() + backoff_ms(item.attempt, self.cfg.retry_interval_s),
                        attempt: item.attempt + 1,
                    }));
                }
                let value = json!({ "error": { "name": "GatewayError", "message": err.to_string() } });
                self.step_completed(state, &op.id, &value, "-failure", false).await?;
                Ok(None)
            }
        }
    }

    async fn op_ai_gateway(
        self: &Arc<Self>,
        state: &mut RunState,
        item: &QueueItem,
        op: &Op,
    ) -> anyhow::Result<Option<Outcome>> {
        let opts = op.opts.clone().unwrap_or(Value::Null);
        let mut url = opts.get("url").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let key = opts.get("auth_key").and_then(|v| v.as_str()).unwrap_or_default();
        let format = opts.get("format").and_then(|v| v.as_str()).unwrap_or_default();
        if format == "gemini" && !key.is_empty() {
            if let Ok(mut u) = url::Url::parse(&url) {
                u.query_pairs_mut().append_pair("key", key);
                url = u.to_string();
            }
        }
        let mut rb = self
            .http
            .post(&url)
            .header("Content-Type", "application/json");
        if !key.is_empty() {
            rb = match format {
                "anthropic" => rb.header("x-api-key", key),
                "bedrock" => rb.header("Authorization", key),
                "gemini" => rb,
                _ => rb.header("Authorization", format!("Bearer {key}")),
            };
        }
        if let Some(h) = opts.get("headers").and_then(|h| h.as_object()) {
            for (k, v) in h {
                if let Some(v) = v.as_str() {
                    rb = rb.header(k, v);
                }
            }
        }
        let body = opts.get("body").cloned().unwrap_or(Value::Null);
        let result = match rb.json(&body).send().await {
            Ok(resp) if resp.status().as_u16() <= 299 => {
                let text = resp.text().await.unwrap_or_default();
                Ok(serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text)))
            }
            Ok(resp) => {
                let status = resp.status().as_u16();
                let text = resp.text().await.unwrap_or_default();
                Err(format!("AI request failed with status {status}: {text}"))
            }
            Err(err) => Err(err.to_string()),
        };
        match result {
            Ok(v) => {
                self.step_completed(state, &op.id, &json!({ "data": v }), "", false)
                    .await?;
                Ok(None)
            }
            Err(msg) => {
                if item.attempt + 1 < item.max_attempts {
                    return Ok(Some(Outcome::Retry {
                        at: now_ms() + backoff_ms(item.attempt, self.cfg.retry_interval_s),
                        attempt: item.attempt + 1,
                    }));
                }
                let value = json!({ "error": { "name": "AIGatewayError", "message": msg } });
                self.step_completed(state, &op.id, &value, "-failure", false).await?;
                Ok(None)
            }
        }
    }

    /// Handle a `cancel` queue item (start / finish timeouts).
    pub async fn handle_timeout_item(self: &Arc<Self>, item: &QueueItem) -> anyhow::Result<Outcome> {
        let Some(run_id) = item.run_id else {
            return Ok(Outcome::Done);
        };
        let Some(meta) = self.state.load_meta(run_id).await? else {
            return Ok(Outcome::Done);
        };
        if meta.status.is_terminal() {
            return Ok(Outcome::Done);
        }
        let should_cancel = match item.str_payload("reason") {
            Some("start_timeout") => meta.started_at.is_none(),
            Some("finish_timeout") => true,
            _ => false,
        };
        if should_cancel {
            let events = self.state.load_events(run_id).await?;
            self.cancel_run_with(&meta, &events, Some(&item.id)).await?;
        }
        Ok(Outcome::Done)
    }
}

fn step_value(op: &Op) -> Value {
    let data = op.data.clone().unwrap_or(Value::Null);
    if op.op == "Step" {
        data
    } else {
        json!({ "data": data })
    }
}

fn is_race(op: &Op) -> bool {
    op.opts
        .as_ref()
        .and_then(|o| o.get("parallelMode"))
        .and_then(|v| v.as_str())
        == Some("race")
}
