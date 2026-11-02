//! Pauses: waitForEvent, invoke, signals and cancelOn.

use super::Engine;
use super::execute::Outcome;
use crate::types::*;
use crate::util::{hash36, sha1_uuid};
use ingot_expr::Vars;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use ulid::Ulid;

/// Ignore pauses created more than this long after an event was received.
const PAUSE_CREATION_GRACE_MS: i64 = 5_000;

impl Engine {
    pub async fn match_pauses(self: &Arc<Self>, evt: &TrackedEvent, evt_json: &Value) -> anyhow::Result<()> {
        let pauses = self.state.pauses_for_event(&evt.event.name).await?;
        if pauses.is_empty() {
            return Ok(());
        }
        let now = now_ms();
        for p in pauses {
            if p.kind == PauseKind::Invoke {
                continue;
            }
            if p.expires_at < now {
                continue;
            }
            if p.trigger_event_id == Some(evt.internal_id) {
                continue;
            }
            if p.created_at > evt.received_at + PAUSE_CREATION_GRACE_MS {
                continue;
            }
            if p.kind == PauseKind::Cancel && evt.internal_id <= p.run_id {
                continue;
            }
            if let Some(expr) = &p.expression {
                let vars = Vars {
                    event: p.trigger_event.as_ref(),
                    async_: Some(evt_json),
                };
                match self.compile(expr).and_then(|x| x.eval_bool(vars)) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(err) => {
                        tracing::debug!(pause = %p.id, error = %err, "pause expression error");
                        continue;
                    }
                }
            }
            match p.kind {
                PauseKind::Cancel => {
                    tracing::info!(run_id = %p.run_id, event = %evt.event.name, "cancelling run (cancelOn)");
                    if !self.cancel_run(p.run_id).await? {
                        self.state.delete_pause(p.id).await?;
                    }
                }
                PauseKind::WaitForEvent => {
                    let extra = json!({ "foundEventID": evt.internal_id.to_string(), "timedOut": false });
                    self.resume_pause(&p, evt_json.clone(), "event", extra).await?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Consume a pause with `value` as the step result and continue the run.
    pub async fn resume_pause(
        self: &Arc<Self>,
        p: &Pause,
        value: Value,
        suffix: &str,
        trace_info: Value,
    ) -> anyhow::Result<bool> {
        let Some(pending_empty) = self.state.consume_pause(p, Some(&value)).await? else {
            return Ok(false);
        };
        let wrapped = p.kind != PauseKind::WaitForEvent
            && value
                .as_object()
                .is_some_and(|o| o.contains_key("data") || o.contains_key("error"));
        let output = if wrapped { value.clone() } else { json!({ "data": value }) };
        let failed = output.get("error").is_some_and(|e| !e.is_null());
        self.trace_resolved(p, trace_info, &output, failed);
        if let Some(job) = &p.timeout_job {
            self.state.remove_item(&hash36(job)).await?;
        }
        if pending_empty
            && let Some(mut meta) = self.state.load_meta(p.run_id).await?
        {
            self.maybe_reset_force_plan(&mut meta).await?;
            let job = format!("{}-{}-{suffix}", p.run_id, p.step_id);
            self.enqueue_discovery(&meta, &p.step_id, Some(job)).await?;
        }
        Ok(true)
    }

    /// Resume the caller of `step.invoke` when the invoked run finishes.
    pub async fn resume_invoke(
        self: &Arc<Self>,
        correlation_id: &str,
        data: &Map<String, Value>,
        finished_event_id: Ulid,
    ) -> anyhow::Result<()> {
        let Some((run, step)) = correlation_id.split_once('.') else {
            return Ok(());
        };
        let Ok(run_id) = Ulid::from_string(run) else {
            return Ok(());
        };
        let Some(p) = self.state.get_pause(sha1_uuid(&format!("{run_id}{step}"))).await? else {
            return Ok(());
        };
        if p.kind != PauseKind::Invoke {
            return Ok(());
        }
        let value = match data.get("error") {
            Some(e) if !e.is_null() => json!({ "error": e }),
            _ => json!({ "data": data.get("result").cloned().unwrap_or(Value::Null) }),
        };
        let extra = json!({
            "runID": data.get("run_id").and_then(|r| r.as_str()).filter(|r| !r.is_empty()),
            "returnEventID": finished_event_id.to_string(),
            "timedOut": false,
        });
        self.resume_pause(&p, value, "event", extra).await?;
        Ok(())
    }

    /// Resume a run waiting on `signal`. Returns the run id if one was waiting.
    pub async fn send_signal(self: &Arc<Self>, signal: &str, data: Value) -> anyhow::Result<Option<Ulid>> {
        for p in self.state.pauses_for_signal(signal).await? {
            let value = json!({ "data": { "signal": signal, "data": data } });
            if self.resume_pause(&p, value, "event", json!({ "timedOut": false })).await? {
                return Ok(Some(p.run_id));
            }
        }
        Ok(None)
    }

    /// A pause's timeout fired.
    pub async fn handle_pause_timeout(self: &Arc<Self>, item: &QueueItem) -> anyhow::Result<Outcome> {
        let Some(id) = item
            .str_payload("pause_id")
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
        else {
            return Ok(Outcome::Done);
        };
        let Some(p) = self.state.get_pause(id).await? else {
            return Ok(Outcome::Done);
        };
        let value = match p.kind {
            PauseKind::Invoke => json!({ "error": {
                "error": "InngestInvokeTimeoutError: Timed out waiting for invoked function to complete",
                "name": "InngestInvokeTimeoutError",
                "message": "Timed out waiting for invoked function to complete",
            }}),
            PauseKind::Cancel => {
                self.state.delete_pause(p.id).await?;
                return Ok(Outcome::Done);
            }
            _ => Value::Null,
        };
        // The timeout item itself is being processed; don't remove it from under the worker.
        let p = Pause {
            timeout_job: None,
            ..p
        };
        self.resume_pause(&p, value, "timeout", json!({ "timedOut": true })).await?;
        Ok(Outcome::Done)
    }
}
