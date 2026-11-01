//! Cron triggers: one self-perpetuating queue item chain per (function, version, cron).

use super::Engine;
use super::execute::Outcome;
use super::schedule::ScheduleRequest;
use crate::cron::CronSchedule;
use crate::types::*;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;

pub fn spawn(engine: Arc<Engine>) {
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(Duration::from_secs(60));
        loop {
            iv.tick().await;
            if let Err(err) = engine.ensure_crons().await {
                tracing::warn!(error = %err, "cron health check failed");
            }
        }
    });
}

fn jitter_ms(job: &str, jitter: Option<&str>) -> i64 {
    let Some(max) = jitter.and_then(crate::util::parse_duration_ms).filter(|m| *m > 1000) else {
        return 0;
    };
    1000 + (crate::util::xxhash(job) % (max - 1000) as u64) as i64
}

impl Engine {
    /// Make sure every cron trigger has its next occurrence enqueued.
    pub async fn ensure_crons(&self) -> anyhow::Result<()> {
        let now = now_ms();
        for f in self.registry.all() {
            for t in &f.triggers {
                if let Some(expr) = &t.cron {
                    self.enqueue_next_cron(&f, expr, t.jitter.as_deref(), now).await?;
                }
            }
        }
        Ok(())
    }

    async fn enqueue_next_cron(
        &self,
        f: &Function,
        expr: &str,
        jitter: Option<&str>,
        after: i64,
    ) -> anyhow::Result<()> {
        let sched = match CronSchedule::parse(expr) {
            Ok(s) => s,
            Err(err) => {
                tracing::warn!(function = %f.slug, cron = expr, error = %err, "invalid cron");
                return Ok(());
            }
        };
        let Some(next) = sched.next_after(after) else {
            return Ok(());
        };
        let job = format!("cron:{}:{}:{}:{}", f.id, f.version, expr, next);
        let at = next + jitter_ms(&job, jitter);
        let item = QueueItem::new(
            Some(job),
            QueueKind::Cron,
            at,
            json!({ "fn_id": f.id, "version": f.version, "cron": expr, "scheduled_at": next }),
        );
        self.enqueue(item).await?;
        Ok(())
    }

    pub async fn handle_cron_item(self: &Arc<Self>, item: &QueueItem) -> anyhow::Result<Outcome> {
        let p = &item.payload;
        let Some(f) = p
            .get("fn_id")
            .and_then(|v| v.as_str())
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
            .and_then(|id| self.registry.get(id))
        else {
            return Ok(Outcome::Done);
        };
        let version = p.get("version").and_then(|v| v.as_i64()).unwrap_or(0);
        let expr = p.get("cron").and_then(|v| v.as_str()).unwrap_or_default();
        let scheduled_at = p.get("scheduled_at").and_then(|v| v.as_i64()).unwrap_or(0);
        let trigger = f.triggers.iter().find(|t| t.cron.as_deref() == Some(expr));
        if version != f.version as i64 || trigger.is_none() {
            return Ok(Outcome::Done);
        }
        let now = now_ms();
        let scheduled = chrono::DateTime::from_timestamp_millis(scheduled_at)
            .unwrap_or_default()
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let fire = chrono::DateTime::from_timestamp_millis(now)
            .unwrap_or_default()
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let evt = Event {
            id: scheduled.clone(),
            name: EVENT_CRON.into(),
            data: json!({ "cron": expr, "scheduledAt": scheduled, "fireAt": fire })
                .as_object()
                .cloned()
                .unwrap_or_default(),
            user: None,
            ts: now,
            v: None,
            meta: None,
        };
        let tracked = TrackedEvent {
            internal_id: ulid::Ulid::generate(),
            event: evt,
            received_at: now,
        };
        self.meta.insert_events(std::slice::from_ref(&tracked)).await?;
        let evt_json: Value = tracked.event.to_json();
        self.schedule(
            &f,
            ScheduleRequest {
                events: vec![evt_json],
                event_ids: vec![tracked.internal_id],
                idempotency_key: Some(scheduled),
                cron: Some(expr.to_string()),
                prevent_debounce: true,
                prevent_rate_limit: true,
                ..Default::default()
            },
        )
        .await?;
        self.enqueue_next_cron(&f, expr, trigger.and_then(|t| t.jitter.as_deref()), scheduled_at)
            .await?;
        Ok(Outcome::Done)
    }
}
