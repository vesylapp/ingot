//! The execution engine: event routing, scheduling, the queue workers and the SDK driver.

pub mod cron;
pub mod driver;
pub mod events;
pub mod execute;
pub mod finalize;
pub mod maintenance;
pub mod ops;
pub mod pauses;
pub mod registry;
pub mod schedule;
pub mod shutdown;
pub mod sync;
pub mod trace;
pub mod worker;

use crate::auth::SigningKey;
use crate::config::Config;
use crate::store::{MetaStore, StateStore};
use crate::types::*;
use oxidle_expr::ExprCache;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;

pub use registry::Registry;

/// Run idempotency window (event `id` dedup).
pub const IDEMPOTENCY_TTL_MS: i64 = 24 * 3600 * 1000;
/// Queue lease duration; leases are extended while an item is processing.
pub const LEASE_MS: i64 = 30_000;

pub struct Engine {
    pub cfg: Config,
    pub meta: Arc<dyn MetaStore>,
    pub state: Arc<dyn StateStore>,
    pub registry: Registry,
    pub http: reqwest::Client,
    pub signing: Option<SigningKey>,
    pub exprs: ExprCache,
    /// Wakes queue workers when new work is enqueued locally.
    pub queue_notify: Notify,
    pub trace: trace::Tracer,
    /// Identifies this process (maintenance lease holder).
    pub node_id: String,
    events_tx: mpsc::Sender<Vec<TrackedEvent>>,
    /// Set once on shutdown: stops leasing, readiness, and background loops.
    shutdown: watch::Sender<bool>,
    /// One permit per queue worker; all permits free means nothing is in flight.
    workers: Arc<Semaphore>,
    /// Items leased by this node and still processing, by item id.
    inflight: Mutex<HashMap<String, QueueItem>>,
    router: Mutex<Option<JoinHandle<()>>>,
    /// Tells the router to stop receiving and drain what it has.
    router_stop: Notify,
}

impl Engine {
    pub fn new(
        cfg: Config,
        meta: Arc<dyn MetaStore>,
        state: Arc<dyn StateStore>,
    ) -> (Arc<Engine>, mpsc::Receiver<Vec<TrackedEvent>>) {
        let (events_tx, events_rx) = mpsc::channel(10_000);
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(2 * 3600))
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(64)
            .build()
            .expect("http client");
        let signing = cfg.signing_key.as_deref().map(SigningKey::new);
        let cfg_workers = cfg.queue_workers.max(1);
        let engine = Arc::new(Engine {
            registry: Registry::new(meta.clone()),
            trace: trace::Tracer::spawn(meta.clone()),
            cfg,
            meta,
            state,
            http,
            signing,
            exprs: ExprCache::new(10_000),
            queue_notify: Notify::new(),
            node_id: ulid::Ulid::generate().to_string(),
            events_tx,
            shutdown: watch::channel(false).0,
            workers: Arc::new(Semaphore::new(cfg_workers)),
            inflight: Mutex::new(HashMap::new()),
            router: Mutex::new(None),
            router_stop: Notify::new(),
        });
        (engine, events_rx)
    }

    /// Start background tasks: event router, queue workers, cron, app sync, gc.
    pub async fn start(self: &Arc<Self>, events_rx: mpsc::Receiver<Vec<TrackedEvent>>) -> anyhow::Result<()> {
        self.registry.reload().await?;
        *self.router.lock() = Some(events::spawn_router(self.clone(), events_rx));
        worker::spawn_workers(self.clone());
        cron::spawn(self.clone());
        sync::spawn(self.clone());
        maintenance::spawn(self.clone());
        // Pick up functions synced through other nodes.
        let e = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
            loop {
                tick.tick().await;
                match e.registry.refresh_if_changed().await {
                    Ok(true) => {
                        if let Err(err) = e.ensure_crons().await {
                            tracing::warn!(error = %err, "cron scheduling failed");
                        }
                    }
                    Ok(false) => {}
                    Err(err) => tracing::warn!(error = %err, "registry refresh failed"),
                }
            }
        });
        Ok(())
    }

    /// Enqueue an item and wake local workers if it is due soon.
    pub async fn enqueue(&self, item: QueueItem) -> anyhow::Result<bool> {
        let due_soon = item.at <= now_ms() + self.cfg.tick_ms as i64;
        let ok = self.state.enqueue(&item).await?;
        if ok && due_soon {
            self.queue_notify.notify_one();
        }
        if ok && item.kind.calls_sdk() && item.kind != QueueKind::Start {
            crate::metrics::get().sdk_request_scheduled(&self.fn_label(item.fn_id));
        }
        Ok(ok)
    }

    /// A function's slug for metric labels.
    pub fn fn_label(&self, fn_id: Option<uuid::Uuid>) -> String {
        fn_id
            .and_then(|id| self.registry.get(id))
            .map_or_else(|| "unknown".to_string(), |f| f.slug.clone())
    }

    /// Hand events to the router (they must already be persisted).
    pub async fn route_events(&self, events: Vec<TrackedEvent>) {
        if events.is_empty() {
            return;
        }
        if self.events_tx.send(events).await.is_err() {
            tracing::error!("event router channel closed");
        }
    }

    /// Persist and route already-identified events.
    pub async fn publish_tracked(&self, events: Vec<TrackedEvent>) -> anyhow::Result<()> {
        self.meta.insert_events(&events).await?;
        crate::metrics::get().events_received("internal", events.len());
        self.route_events(events).await;
        Ok(())
    }

    /// Persist and route internally generated events.
    pub async fn publish_internal(&self, events: Vec<Event>) -> anyhow::Result<Vec<TrackedEvent>> {
        let now = now_ms();
        let tracked: Vec<TrackedEvent> = events
            .into_iter()
            .map(|mut ev| {
                let internal_id = ulid::Ulid::generate();
                if ev.id.is_empty() {
                    ev.id = internal_id.to_string();
                }
                if ev.ts == 0 {
                    ev.ts = now;
                }
                TrackedEvent {
                    internal_id,
                    event: ev,
                    received_at: now,
                }
            })
            .collect();
        self.meta.insert_events(&tracked).await?;
        crate::metrics::get().events_received("internal", tracked.len());
        self.route_events(tracked.clone()).await;
        Ok(tracked)
    }

    pub fn compile(&self, src: &str) -> Result<oxidle_expr::Expression, oxidle_expr::ExprError> {
        self.exprs.get(src)
    }
}
