//! Scheduling new runs: rate limiting, debounce, priority, concurrency keys, throttle,
//! singleton, idempotency, cancellation pauses and timeouts.

use super::{Engine, IDEMPOTENCY_TTL_MS};
use crate::store::CreateRun;
use crate::types::*;
use crate::util::hash36;
use oxidle_expr::Vars;
use serde_json::{Value, json};
use std::sync::Arc;
use ulid::Ulid;

#[derive(Debug, Default, Clone)]
pub struct ScheduleRequest {
    pub events: Vec<Value>,
    pub event_ids: Vec<Ulid>,
    /// User-level idempotency key (event id, cron time, batch id...).
    pub idempotency_key: Option<String>,
    pub batch_id: Option<Ulid>,
    pub cron: Option<String>,
    /// Earliest start time (ms); defaults to now / the event's future ts.
    pub at: Option<i64>,
    pub prevent_rate_limit: bool,
    pub prevent_debounce: bool,
    pub original_run_id: Option<Ulid>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScheduleOutcome {
    Scheduled(Ulid),
    Duplicate,
    RateLimited,
    Debounced,
    Batched,
    SingletonSkipped,
}

const PRIORITY_MAX: i64 = 43_200;

impl Engine {
    fn key_vars<'a>(events: &'a [Value]) -> Vars<'a> {
        Vars {
            event: events.first(),
            async_: None,
        }
    }

    /// Evaluate an optional key expression. Returns Ok(None) if it evaluated to `false`.
    fn eval_key(&self, expr: &str, events: &[Value]) -> Option<String> {
        match self
            .compile(expr)
            .and_then(|x| x.eval_key_opt(Self::key_vars(events)))
        {
            Ok(v) => v,
            Err(err) => {
                tracing::debug!(expr, error = %err, "key expression failed");
                Some("<invalid>".into())
            }
        }
    }

    pub async fn schedule(
        self: &Arc<Self>,
        f: &Arc<Function>,
        req: ScheduleRequest,
    ) -> anyhow::Result<ScheduleOutcome> {
        let now = now_ms();
        let events = &req.events;
        let first = events.first().cloned().unwrap_or(Value::Null);

        // 1. Rate limit (drops runs).
        if let Some(rl) = &f.rate_limit
            && !req.prevent_rate_limit
            && req.batch_id.is_none()
        {
            let key = match &rl.key {
                Some(k) => self.eval_key(k, events).map(|v| format!("{}-{}", f.id, hash36(&v))),
                None => Some(f.id.to_string()),
            };
            if let Some(key) = key {
                let emission = (rl.period_ms / rl.limit.max(1) as i64).max(1);
                let tolerance = (rl.limit / 10) as i64 * emission;
                if self
                    .state
                    .gcra(&format!("rl:{key}"), now, emission, tolerance)
                    .await?
                    .is_err()
                {
                    tracing::debug!(function = %f.slug, "run rate limited");
                    return Ok(ScheduleOutcome::RateLimited);
                }
            }
        }

        // 2. Debounce.
        if let Some(d) = &f.debounce
            && !req.prevent_debounce
        {
            let key_val = match &d.key {
                Some(k) => self.eval_key(k, events).unwrap_or_else(|| "false".into()),
                None => String::new(),
            };
            let key = format!("{}:{}", f.id, hash36(&key_val));
            let ts = first.get("ts").and_then(|t| t.as_i64()).unwrap_or(now);
            let res = self
                .state
                .debounce_upsert(
                    &key,
                    f.id,
                    req.event_ids.first().copied().unwrap_or_else(Ulid::generate),
                    &first,
                    ts,
                    d.period_ms,
                    d.timeout_ms,
                    now,
                )
                .await?;
            if !res.ignored {
                let job = format!("debounce-{}", res.id);
                if res.created {
                    let item = QueueItem::new(
                        Some(job),
                        QueueKind::Debounce,
                        res.fire_at,
                        json!({ "key": key, "debounce_id": res.id, "fn_id": f.id }),
                    );
                    self.enqueue(item).await?;
                } else {
                    self.state.reschedule(&hash36(&job), res.fire_at).await?;
                }
            }
            return Ok(ScheduleOutcome::Debounced);
        }

        let run_id = Ulid::generate();

        // 3. Priority.
        let priority_factor = match &f.priority_run {
            Some(expr) => self
                .compile(expr)
                .and_then(|x| x.eval_int(Self::key_vars(events)))
                .unwrap_or(0)
                .clamp(-PRIORITY_MAX, PRIORITY_MAX),
            None => 0,
        };

        // 4. Concurrency & throttle keys.
        let mut concurrency = Vec::new();
        for c in &f.concurrency {
            let key = match (&c.key, c.scope) {
                (None, _) => format!("f:{}", f.id),
                (Some(k), scope) => {
                    let Some(v) = self.eval_key(k, events) else { continue };
                    let (prefix, scope_id) = match scope {
                        ConcurrencyScope::Fn => ("f", f.id.to_string()),
                        ConcurrencyScope::Env => ("e", ENV_ID.to_string()),
                        ConcurrencyScope::Account => ("a", ACCOUNT_ID.to_string()),
                    };
                    format!("{prefix}:{scope_id}:{}", hash36(&v))
                }
            };
            concurrency.push(ConcurrencyKey {
                key,
                limit: c.limit,
            });
        }
        let throttle = match &f.throttle {
            Some(t) => {
                let key = match &t.key {
                    Some(k) => self
                        .eval_key(k, events)
                        .map(|v| format!("{}-{}", hash36(&f.id.to_string()), hash36(&v))),
                    None => Some(hash36(&f.id.to_string())),
                };
                key.map(|key| ThrottleKey {
                    key,
                    limit: t.limit,
                    burst: t.burst,
                    period_ms: t.period_ms,
                })
            }
            None => None,
        };

        // 5. Singleton.
        let mut singleton_key = None;
        if let Some(s) = &f.singleton {
            let key = match &s.key {
                Some(k) => self.eval_key(k, events).map(|v| format!("{}-{}", f.id, hash36(&v))),
                None => Some(f.id.to_string()),
            };
            if let Some(key) = key {
                let kv_key = format!("singleton:{key}");
                if !self.acquire_singleton(&kv_key, s.mode, run_id, &first).await? {
                    tracing::debug!(function = %f.slug, "run skipped: singleton");
                    return Ok(ScheduleOutcome::SingletonSkipped);
                }
                singleton_key = Some(kv_key);
            }
        }

        // 6. Run state + idempotency.
        let idem_source = req
            .idempotency_key
            .clone()
            .filter(|k| !k.is_empty())
            .or_else(|| req.batch_id.map(|b| b.to_string()))
            .or_else(|| req.event_ids.first().map(|e| e.to_string()))
            .unwrap_or_else(|| run_id.to_string());
        let idempotency_key = format!("{}-{}", hash36(&f.id.to_string()), hash36(&idem_source));
        let trigger_name = first.get("name").and_then(|n| n.as_str()).unwrap_or_default();
        let meta = RunMeta {
            run_id,
            fn_id: f.id,
            fn_slug: f.slug.clone(),
            fn_version: f.version,
            app_id: f.app_id,
            status: RunStatus::Running,
            created_at: now,
            started_at: None,
            ended_at: None,
            event_ids: req.event_ids.clone(),
            batch_id: req.batch_id,
            cron: req.cron.clone(),
            original_run_id: req.original_run_id,
            idempotency_key: idempotency_key.clone(),
            request_version: -1,
            force_step_plan: false,
            priority_factor,
            concurrency,
            throttle,
            singleton_key: singleton_key.clone(),
            max_attempts: f.max_attempts(),
            suppress_lifecycle: trigger_name == EVENT_FN_FAILED || trigger_name == EVENT_FN_FINISHED,
        };
        match self.state.create_run(&meta, events, IDEMPOTENCY_TTL_MS).await? {
            CreateRun::Created => {}
            CreateRun::Duplicate(existing) => {
                if let Some(k) = &singleton_key {
                    self.state.kv_del(k, Some(&run_id.to_string())).await?;
                }
                tracing::debug!(function = %f.slug, existing, "run skipped: idempotency");
                return Ok(ScheduleOutcome::Duplicate);
            }
        }
        self.meta
            .insert_run(&RunRecord {
                run_id,
                fn_id: f.id,
                fn_slug: f.slug.clone(),
                app_id: f.app_id,
                fn_version: f.version,
                event_id: req.event_ids.first().copied(),
                batch_id: req.batch_id,
                cron: req.cron.clone(),
                original_run_id: req.original_run_id,
                status: RunStatus::Running,
                started_at: now,
                ended_at: None,
                output: None,
            })
            .await?;

        // 7. Cancellation pauses and start timeout (not for batches).
        if req.batch_id.is_none() {
            let trigger_event_id = req.event_ids.first().copied();
            for c in &f.cancel {
                let expires_at = c
                    .timeout
                    .as_deref()
                    .and_then(|t| crate::util::parse_timeout_at(t, now))
                    .unwrap_or(now + 365 * 86_400_000);
                let id = crate::util::sha1_uuid(&format!(
                    "{}-{}-{}",
                    idempotency_key,
                    c.event,
                    c.if_expr.as_deref().unwrap_or_default()
                ));
                self.state
                    .save_pause(&Pause {
                        id,
                        kind: PauseKind::Cancel,
                        run_id,
                        fn_id: f.id,
                        step_id: String::new(),
                        event: Some(c.event.clone()),
                        expression: c.if_expr.clone(),
                        trigger_event: Some(first.clone()),
                        trigger_event_id,
                        expires_at,
                        created_at: now,
                        timeout_job: None,
                        correlation_id: None,
                    })
                    .await?;
            }
            if let Some(start) = f.timeout_start_ms {
                let item = QueueItem::new(
                    Some(format!("{run_id}-start-timeout")),
                    QueueKind::Cancel,
                    now + start,
                    json!({ "reason": "start_timeout" }),
                )
                .for_run(&meta);
                self.enqueue(item).await?;
            }
        }

        // 8. Enqueue the start item.
        let event_ts = first.get("ts").and_then(|t| t.as_i64()).unwrap_or(0);
        let at = req.at.unwrap_or_else(|| {
            if req.batch_id.is_none() && event_ts > now {
                event_ts
            } else {
                now
            }
        });
        let item = QueueItem::new(
            Some(format!("{}:{}", f.id, idempotency_key)),
            QueueKind::Start,
            at,
            json!({ "incoming": "step" }),
        )
        .for_run(&meta);
        self.enqueue(item).await?;
        tracing::debug!(function = %f.slug, run_id = %run_id, "run scheduled");
        Ok(ScheduleOutcome::Scheduled(run_id))
    }

    /// Take the singleton lock for a new run. Returns false if the run should be skipped.
    async fn acquire_singleton(
        self: &Arc<Self>,
        kv_key: &str,
        mode: SingletonMode,
        run_id: Ulid,
        new_event: &Value,
    ) -> anyhow::Result<bool> {
        for _ in 0..3 {
            let Some(holder) = self.state.kv_set_nx(kv_key, &run_id.to_string(), None).await? else {
                return Ok(true);
            };
            // Stale lock (holder finished or vanished): clear it and retry.
            let holder_id = Ulid::from_string(&holder).ok();
            let alive = match holder_id {
                Some(h) => self
                    .state
                    .load_meta(h)
                    .await?
                    .is_some_and(|m| !m.status.is_terminal()),
                None => false,
            };
            if !alive {
                self.state.kv_del(kv_key, Some(&holder)).await?;
                continue;
            }
            match mode {
                SingletonMode::Skip => return Ok(false),
                SingletonMode::Cancel => {
                    if let Some(h) = holder_id {
                        let _ = new_event;
                        self.cancel_run(h).await?;
                    }
                    self.state.kv_del(kv_key, Some(&holder)).await?;
                }
            }
        }
        Ok(false)
    }
}
