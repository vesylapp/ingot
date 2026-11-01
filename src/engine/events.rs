//! Routing of received events to function triggers, invocations, batches and pauses.

use super::Engine;
use super::schedule::{ScheduleOutcome, ScheduleRequest};
use crate::types::*;
use oxidle_expr::Vars;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};

/// Max events processed concurrently by the router.
const ROUTER_CONCURRENCY: usize = 64;

/// Events from one request are handled in order; separate requests run concurrently.
/// On shutdown the router stops receiving, then handles everything already queued.
pub fn spawn_router(
    engine: Arc<Engine>,
    mut rx: mpsc::Receiver<Vec<TrackedEvent>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(ROUTER_CONCURRENCY));
        loop {
            let events = tokio::select! {
                events = rx.recv() => events,
                _ = engine.router_stop.notified() => {
                    rx.close();
                    continue;
                }
            };
            let Some(events) = events else { break };
            let permit = sem.clone().acquire_owned().await.expect("semaphore");
            let e = engine.clone();
            tokio::spawn(async move {
                for evt in events {
                    if let Err(err) = e.handle_event(&evt).await {
                        tracing::error!(event = %evt.event.name, id = %evt.internal_id, error = %err, "event handling failed");
                    }
                }
                drop(permit);
            });
        }
        // Wait for the batches still being handled.
        let _ = sem.acquire_many(ROUTER_CONCURRENCY as u32).await;
    })
}

impl Engine {
    pub async fn handle_event(self: &Arc<Self>, evt: &TrackedEvent) -> anyhow::Result<()> {
        let evt_json = evt.event.to_json();
        let name = evt.event.name.as_str();

        if name == EVENT_FN_INVOKED {
            self.handle_invoke_event(evt, &evt_json).await?;
        } else {
            for f in self.registry.for_event(name) {
                if let Err(err) = self.trigger_function(&f, evt, &evt_json).await {
                    tracing::error!(function = %f.slug, error = %err, "failed to trigger function");
                }
            }
        }

        if name == EVENT_FN_FINISHED
            && let Some(cid) = evt
                .event
                .data
                .get("correlation_id")
                .and_then(|c| c.as_str())
                .filter(|c| !c.is_empty())
            {
                self.resume_invoke(cid, &evt.event.data, evt.internal_id).await?;
            }

        self.match_pauses(evt, &evt_json).await
    }

    async fn trigger_function(
        self: &Arc<Self>,
        f: &Arc<Function>,
        evt: &TrackedEvent,
        evt_json: &Value,
    ) -> anyhow::Result<()> {
        let patterns = crate::function::event_patterns(&evt.event.name);
        for t in &f.triggers {
            let Some(ev) = &t.event else { continue };
            if !patterns.iter().any(|p| p == ev) {
                continue;
            }
            if let Some(expr) = t.expression.as_deref().filter(|e| !e.is_empty()) {
                let ok = self
                    .compile(expr)
                    .and_then(|x| x.eval_bool(Vars { event: Some(evt_json), async_: None }));
                match ok {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(err) => {
                        tracing::debug!(function = %f.slug, error = %err, "trigger expression error");
                        continue;
                    }
                }
            }
            self.initialize(f, evt, evt_json).await?;
            return Ok(());
        }
        Ok(())
    }

    /// Start a function for an event: via batching if configured, otherwise schedule directly.
    pub async fn initialize(
        self: &Arc<Self>,
        f: &Arc<Function>,
        evt: &TrackedEvent,
        evt_json: &Value,
    ) -> anyhow::Result<ScheduleOutcome> {
        if let Some(b) = &f.batch {
            let eligible = match b.if_expr.as_deref() {
                Some(expr) => self
                    .compile(expr)
                    .and_then(|x| x.eval_bool(Vars { event: Some(evt_json), async_: None }))
                    .unwrap_or(false),
                None => true,
            };
            if eligible {
                self.append_batch(f, b, evt, evt_json).await?;
                return Ok(ScheduleOutcome::Batched);
            }
        }
        self.schedule(f, ScheduleRequest {
            events: vec![evt_json.clone()],
            event_ids: vec![evt.internal_id],
            idempotency_key: Some(evt.event.id.clone()),
            ..Default::default()
        })
        .await
    }

    async fn append_batch(
        self: &Arc<Self>,
        f: &Arc<Function>,
        b: &BatchEvents,
        evt: &TrackedEvent,
        evt_json: &Value,
    ) -> anyhow::Result<()> {
        let key_val = match b.key.as_deref() {
            Some(k) => self
                .compile(k)
                .and_then(|x| x.eval_key(Vars { event: Some(evt_json), async_: None }))
                .unwrap_or_else(|_| "<invalid>".into()),
            None => "default".into(),
        };
        let key = format!("{}:{}", f.id, crate::util::hash36(&key_val));
        let res = self
            .state
            .batch_append(&key, f.id, evt_json, evt.internal_id, b.max_size)
            .await?;
        if res.duplicate {
            return Ok(());
        }
        let timeout_job = format!("batch-{}", res.batch_id);
        if res.created {
            let item = QueueItem::new(
                Some(timeout_job.clone()),
                QueueKind::ScheduleBatch,
                now_ms() + b.timeout_ms,
                json!({ "key": key, "batch_id": res.batch_id.to_string(), "fn_id": f.id }),
            );
            self.enqueue(item).await?;
        }
        if res.len >= b.max_size {
            self.state.remove_item(&crate::util::hash36(&timeout_job)).await?;
            self.start_batch(f, &key, res.batch_id).await?;
        }
        Ok(())
    }

    /// Close a batch and schedule a run with its events.
    pub async fn start_batch(
        self: &Arc<Self>,
        f: &Arc<Function>,
        key: &str,
        batch_id: ulid::Ulid,
    ) -> anyhow::Result<()> {
        let Some(pairs) = self.state.batch_take(key, batch_id).await? else {
            return Ok(());
        };
        if pairs.is_empty() {
            return Ok(());
        }
        let (event_ids, events): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();
        self.schedule(f, ScheduleRequest {
            events,
            event_ids,
            idempotency_key: Some(format!("{}-{}", f.id, batch_id)),
            batch_id: Some(batch_id),
            prevent_rate_limit: true,
            prevent_debounce: true,
            ..Default::default()
        })
        .await?;
        Ok(())
    }

    async fn handle_invoke_event(
        self: &Arc<Self>,
        evt: &TrackedEvent,
        evt_json: &Value,
    ) -> anyhow::Result<()> {
        let meta = evt.event.data.get("_inngest");
        let target = meta
            .and_then(|m| m.get("fn_id"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let correlation_id = meta
            .and_then(|m| m.get("correlation_id"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let f = self.registry.by_slug(target).or_else(|| {
            uuid::Uuid::parse_str(target)
                .ok()
                .and_then(|id| self.registry.get(id))
        });
        let Some(f) = f else {
            if !correlation_id.is_empty() {
                self.invoke_failed(&correlation_id, &format!("could not find function with ID: {target}"))
                    .await?;
            }
            return Ok(());
        };
        match self.initialize(&f, evt, evt_json).await? {
            ScheduleOutcome::RateLimited if !correlation_id.is_empty() => {
                self.invoke_failed(&correlation_id, "invoked function is rate limited").await?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Emit a synthetic `function.finished` that fails the invoking step.
    async fn invoke_failed(&self, correlation_id: &str, message: &str) -> anyhow::Result<()> {
        let data = json!({
            "function_id": "",
            "run_id": "",
            "correlation_id": correlation_id,
            "error": { "name": "Error", "message": message },
        });
        self.publish_internal(vec![Event {
            id: String::new(),
            name: EVENT_FN_FINISHED.into(),
            data: data.as_object().cloned().unwrap_or_default(),
            user: None,
            ts: 0,
            v: None,
            meta: None,
        }])
        .await?;
        Ok(())
    }
}
