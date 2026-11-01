//! Periodic cleanup: expired state (`gc`) and history retention (`prune`).
//!
//! One node at a time does the work: each pass first takes a lease in the shared state
//! store, so replicas don't repeat each other's deletes. A pass has a time budget and deletes
//! in batches, so a backlog is worked off over several passes instead of one long transaction.

use super::Engine;
use crate::types::now_ms;
use std::sync::Arc;
use std::time::{Duration, Instant};

const INTERVAL: Duration = Duration::from_secs(60);
/// Longer than any pass, so a node that dies mid-pass only blocks maintenance briefly.
const LEASE_TTL_MS: i64 = 5 * 60_000;
pub const LEASE_KEY: &str = "lease:maintenance";
/// Time budget for the history prune within one pass.
const PRUNE_BUDGET: Duration = Duration::from_secs(40);
/// Rows per delete transaction.
pub const PRUNE_BATCH: i64 = 5000;

pub fn spawn(engine: Arc<Engine>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(INTERVAL);
        let mut shutdown = engine.shutdown_rx();
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = shutdown.wait_for(|s| *s) => return,
            }
            engine.maintenance_pass().await;
        }
    });
}

impl Engine {
    /// Run one maintenance pass if no other node is running one. Returns whether it ran.
    pub async fn maintenance_pass(self: &Arc<Self>) -> bool {
        match self
            .state
            .kv_set_nx(LEASE_KEY, &self.node_id, Some(LEASE_TTL_MS))
            .await
        {
            Ok(None) => {}
            Ok(Some(_)) => return false,
            Err(err) => {
                tracing::warn!(error = %err, "maintenance lease failed");
                return false;
            }
        }

        let now = now_ms();
        if let Err(err) = self.state.gc(now).await {
            tracing::warn!(error = %err, "state gc failed");
        }
        if self.cfg.retention_days > 0 {
            let before = now - self.cfg.retention_days as i64 * 86_400_000;
            let started = Instant::now();
            match self
                .meta
                .prune(before, PRUNE_BATCH, started + PRUNE_BUDGET)
                .await
            {
                Ok(p) => {
                    let m = crate::metrics::get();
                    m.retention_deleted("function_runs", p.runs);
                    m.retention_deleted("run_spans", p.spans);
                    m.retention_deleted("events", p.events);
                    if p.runs + p.spans + p.events > 0 {
                        tracing::info!(
                            runs = p.runs,
                            spans = p.spans,
                            events = p.events,
                            complete = p.complete,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "pruned history"
                        );
                    }
                }
                Err(err) => tracing::warn!(error = %err, "history prune failed"),
            }
        }

        if let Err(err) = self.state.kv_del(LEASE_KEY, Some(&self.node_id)).await {
            tracing::warn!(error = %err, "maintenance lease release failed");
        }
        true
    }
}
