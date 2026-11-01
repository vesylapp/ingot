//! Graceful shutdown.
//!
//! `begin_shutdown` stops leasing new queue items and flips readiness off. In-flight queue
//! items then get most of the budget to finish (HTTP stays up, since SDKs call back into the
//! API to finish a step); any still running are handed back so another node picks them up
//! right away. Then HTTP stops accepting connections, the router routes every event already
//! accepted, and run timelines are flushed.

use super::Engine;
use crate::types::now_ms;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// Part of the shutdown budget kept for routing accepted events and flushing timelines:
/// a quarter of it, at most this much.
const ROUTER_RESERVE: Duration = Duration::from_secs(5);

impl Engine {
    pub fn shutdown_rx(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    pub fn is_shutting_down(&self) -> bool {
        *self.shutdown.borrow()
    }

    /// Stop taking new work: queue workers stop leasing and readiness reports unavailable.
    pub fn begin_shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    /// Wait for in-flight queue items until `until`, then hand back any still running so
    /// another node picks them up right away instead of after the lease expires. Keep serving
    /// HTTP meanwhile: SDKs call back into the API (checkpoints, sends) to finish a step.
    pub async fn drain_queue(self: &Arc<Self>, until: Instant) {
        self.begin_shutdown();
        let permits = self.cfg.queue_workers.max(1) as u32;
        let idle = tokio::time::timeout_at(until.into(), self.workers.acquire_many(permits))
            .await
            .is_ok();
        if idle {
            return;
        }
        let items: Vec<_> = self.inflight.lock().values().cloned().collect();
        let mut released = 0;
        for item in &items {
            match self.state.requeue(item, now_ms(), item.attempt, None).await {
                Ok(()) => released += 1,
                Err(err) => tracing::warn!(item = %item.id, error = %err, "failed to release queue item"),
            }
        }
        tracing::warn!(released, "shutdown deadline reached; released in-flight queue items");
    }

    /// Route every event already accepted, then flush run timelines. Call after the HTTP
    /// server has stopped, so no new events arrive while the router drains.
    pub async fn drain_events(self: &Arc<Self>, deadline: Instant) {
        self.router_stop.notify_one();
        let router = self.router.lock().take();
        if let Some(router) = router {
            let deadline = deadline.max(Instant::now() + Duration::from_secs(1));
            if tokio::time::timeout_at(deadline.into(), router).await.is_err() {
                tracing::error!("event router did not finish before the shutdown deadline");
            }
        }
        if tokio::time::timeout(Duration::from_secs(2), self.trace.flush())
            .await
            .is_err()
        {
            tracing::warn!("timed out flushing run timelines");
        }
    }

    /// The whole sequence for a `grace` budget: in-flight items get most of it, the rest is
    /// kept for stopping HTTP and routing accepted events. `stop_http` must stop the server and
    /// resolve once its in-flight requests are done.
    pub async fn shutdown<F>(self: &Arc<Self>, grace: Duration, stop_http: F)
    where
        F: std::future::Future<Output = ()>,
    {
        let started = Instant::now();
        let deadline = started + grace;
        self.begin_shutdown();
        self.drain_queue(deadline - (grace / 4).min(ROUTER_RESERVE)).await;
        if tokio::time::timeout_at(deadline.into(), stop_http).await.is_err() {
            tracing::warn!("HTTP requests still open at the shutdown deadline");
        }
        self.drain_events(deadline).await;
        tracing::info!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            "shutdown drained"
        );
    }
}
