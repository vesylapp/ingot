//! Queue workers: lease due items, process them, extend leases while running.

use super::execute::Outcome;
use super::{Engine, LEASE_MS};
use crate::types::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

pub fn spawn_workers(engine: Arc<Engine>) {
    tokio::spawn(async move {
        let workers = engine.cfg.queue_workers;
        let sem = Arc::new(Semaphore::new(workers));
        let tick = Duration::from_millis(engine.cfg.tick_ms);
        loop {
            let available = sem.available_permits();
            if available == 0 {
                // All workers busy: wait for one to free up.
                let p = sem.clone().acquire_owned().await.expect("semaphore");
                drop(p);
                continue;
            }
            let items = match engine
                .state
                .lease(now_ms(), available.min(100) as i64, LEASE_MS)
                .await
            {
                Ok(items) => items,
                Err(err) => {
                    tracing::error!(error = %err, "queue lease failed");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            let leased = items.len();
            for item in items {
                let permit = sem.clone().acquire_owned().await.expect("semaphore");
                let e = engine.clone();
                tokio::spawn(async move {
                    e.process(item).await;
                    drop(permit);
                });
            }
            if leased == 0 {
                tokio::select! {
                    _ = engine.queue_notify.notified() => {}
                    _ = tokio::time::sleep(tick) => {}
                }
            }
        }
    });
}

impl Engine {
    async fn process(self: &Arc<Self>, mut item: QueueItem) {
        let lease_id = item.lease_id.clone().unwrap_or_default();
        let id = item.id.clone();
        let extender = {
            let e = self.clone();
            let id = id.clone();
            tokio::spawn(async move {
                let mut iv = tokio::time::interval(Duration::from_millis((LEASE_MS / 3) as u64));
                iv.tick().await;
                loop {
                    iv.tick().await;
                    let until = now_ms() + LEASE_MS;
                    match e.state.extend_lease(&id, &lease_id, until).await {
                        Ok(true) => {
                            let _ = e.state.extend_concurrency(&id, until).await;
                        }
                        Ok(false) => break,
                        Err(err) => tracing::warn!(error = %err, "lease extension failed"),
                    }
                }
            })
        };

        let result = match item.kind {
            QueueKind::Start | QueueKind::Edge | QueueKind::Sleep | QueueKind::EdgeError => {
                self.execute_item(&mut item).await
            }
            QueueKind::Pause => self.handle_pause_timeout(&item).await,
            QueueKind::Cancel => self.handle_timeout_item(&item).await,
            QueueKind::Debounce => self.handle_debounce_item(&item).await,
            QueueKind::ScheduleBatch => self.handle_batch_item(&item).await,
            QueueKind::Cron => self.handle_cron_item(&item).await,
        };
        extender.abort();

        let outcome = match result {
            Ok(o) => o,
            Err(err) => {
                tracing::error!(item = %item.id, kind = item.kind.as_str(), run_id = ?item.run_id, error = %err, "queue item failed");
                if item.attempt + 1 < item.max_attempts.max(3) {
                    Outcome::Retry {
                        at: now_ms() + crate::util::backoff_ms(item.attempt, Some(1)).min(10_000) * (item.attempt as i64 + 1),
                        attempt: item.attempt + 1,
                    }
                } else {
                    Outcome::Done
                }
            }
        };
        let res = match outcome {
            Outcome::Done => self.state.ack(&item).await,
            Outcome::Retry { at, attempt } => self.state.requeue(&item, at, attempt, None).await,
            Outcome::Delay { at, blocked_on } => {
                self.state
                    .requeue(&item, at, item.attempt, blocked_on.as_deref())
                    .await
            }
        };
        if let Err(err) = res {
            tracing::error!(item = %item.id, error = %err, "failed to settle queue item");
        }
    }

    async fn handle_debounce_item(self: &Arc<Self>, item: &QueueItem) -> anyhow::Result<Outcome> {
        let key = item.str_payload("key").unwrap_or_default().to_string();
        let id = item.str_payload("debounce_id").unwrap_or_default().to_string();
        let Some(fn_id) = item
            .str_payload("fn_id")
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
        else {
            return Ok(Outcome::Done);
        };
        let Some((event_id, event)) = self.state.debounce_take(&key, &id).await? else {
            return Ok(Outcome::Done);
        };
        let Some(f) = self.registry.get(fn_id) else {
            return Ok(Outcome::Done);
        };
        let event_ids = vec![event_id];
        self.schedule(
            &f,
            super::schedule::ScheduleRequest {
                events: vec![event],
                event_ids,
                idempotency_key: Some(format!("debounce-{id}")),
                prevent_debounce: true,
                prevent_rate_limit: true,
                ..Default::default()
            },
        )
        .await?;
        Ok(Outcome::Done)
    }

    async fn handle_batch_item(self: &Arc<Self>, item: &QueueItem) -> anyhow::Result<Outcome> {
        let key = item.str_payload("key").unwrap_or_default().to_string();
        let batch = item
            .str_payload("batch_id")
            .and_then(|b| ulid::Ulid::from_string(b).ok());
        let f = item
            .str_payload("fn_id")
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
            .and_then(|id| self.registry.get(id));
        if let (Some(batch_id), Some(f)) = (batch, f) {
            self.start_batch(&f, &key, batch_id).await?;
        }
        Ok(Outcome::Done)
    }
}
