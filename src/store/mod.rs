//! Storage abstractions.
//!
//! * [`MetaStore`] holds durable, queryable data: apps, functions, events and run history.
//!   Implemented on SQLite or Postgres.
//! * [`StateStore`] holds the hot execution state: the queue, run state, pauses and
//!   flow-control bookkeeping. Implemented on SQL (default) or Redis.

pub mod redis;
pub mod search;
pub mod sql;

use crate::types::*;
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;
use uuid::Uuid;

#[derive(Debug, Default, Clone)]
pub struct EventQuery {
    pub name: Option<String>,
    /// Match any of these names (in addition to `name`).
    pub names: Vec<String>,
    pub cursor: Option<Ulid>,
    pub received_after: Option<i64>,
    pub received_before: Option<i64>,
    pub limit: i64,
    pub include_internal: bool,
}

#[derive(Debug, Default, Clone)]
pub struct RunQuery {
    pub fn_id: Option<Uuid>,
    pub status: Option<RunStatus>,
    /// Return runs with run_id < cursor (ordered by run_id desc).
    pub cursor: Option<Ulid>,
    pub limit: i64,
    // Dashboard filters.
    pub app_ids: Vec<Uuid>,
    /// Only runs whose trigger event has one of these names (`Some(vec![])` matches nothing).
    pub event_names: Option<Vec<String>>,
    /// Dashboard statuses: COMPLETED, FAILED, CANCELLED, SKIPPED, RUNNING, QUEUED.
    pub ui_statuses: Vec<String>,
    pub time_field: RunTimeField,
    pub from: Option<i64>,
    pub until: Option<i64>,
    /// Keyset cursor (time, run_id) when ordering by `time_field` desc.
    pub page_cursor: Option<(i64, Ulid)>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RunTimeField {
    #[default]
    Queued,
    Started,
    Ended,
}

/// A span in a run's timeline (step, attempt, finalization). Absent fields are left
/// unchanged on upsert; `queued_at`/`started_at` keep their first value.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Span {
    pub run_id: Ulid,
    pub span_id: String,
    pub parent_id: Option<String>,
    pub name: Option<String>,
    pub op: Option<String>,
    pub step_id: Option<String>,
    pub step_type: Option<String>,
    pub status: Option<String>,
    pub attempts: Option<i64>,
    pub queued_at: Option<i64>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub info: Option<Value>,
    pub output: Option<Value>,
}

#[async_trait]
pub trait MetaStore: Send + Sync {
    async fn get_app_by_name(&self, name: &str) -> Result<Option<App>>;
    async fn get_app(&self, id: Uuid) -> Result<Option<App>>;
    async fn list_apps(&self) -> Result<Vec<App>>;
    async fn upsert_app(&self, app: &App) -> Result<()>;
    async fn set_app_error(&self, id: Uuid, error: Option<&str>) -> Result<()>;
    async fn delete_app(&self, id: Uuid) -> Result<()>;

    /// All active (non-archived) functions.
    async fn list_functions(&self) -> Result<Vec<Function>>;
    async fn app_functions(&self, app_id: Uuid) -> Result<Vec<Function>>;
    /// Replace the app's active function set: upsert `fns`, archive the rest.
    async fn sync_functions(&self, app_id: Uuid, fns: &[Function]) -> Result<()>;
    /// Monotonic counter bumped on every sync, used to invalidate caches across nodes.
    async fn functions_generation(&self) -> Result<i64>;

    async fn insert_events(&self, events: &[TrackedEvent]) -> Result<()>;
    async fn get_event(&self, internal_id: Ulid) -> Result<Option<TrackedEvent>>;
    /// The events with these internal IDs that exist, in no particular order.
    async fn get_events(&self, internal_ids: &[Ulid]) -> Result<Vec<TrackedEvent>>;
    async fn list_events(&self, q: &EventQuery) -> Result<Vec<TrackedEvent>>;

    async fn insert_run(&self, run: &RunRecord) -> Result<()>;
    async fn finish_run(
        &self,
        run_id: Ulid,
        status: RunStatus,
        output: &Value,
        ended_at: i64,
    ) -> Result<()>;
    async fn get_run(&self, run_id: Ulid) -> Result<Option<RunRecord>>;
    async fn runs_for_event(&self, event_id: Ulid) -> Result<Vec<RunRecord>>;
    async fn list_runs(&self, q: &RunQuery) -> Result<Vec<RunRecord>>;
    /// Delete history older than `before` (ms) in transactions of up to `batch` rows: finished
    /// runs with their timelines, then events no remaining run refers to. Stops early at
    /// `deadline`; the next call continues where this one left off.
    async fn prune(&self, before: i64, batch: i64, deadline: std::time::Instant) -> Result<Pruned>;
    /// Check the store is reachable.
    async fn ping(&self) -> Result<()>;

    async fn count_runs(&self, q: &RunQuery) -> Result<i64>;
    async fn count_events(&self, q: &EventQuery) -> Result<i64>;
    /// Record when a run first started executing.
    async fn mark_run_started(&self, run_id: Ulid, at: i64) -> Result<()>;
    async fn upsert_spans(&self, spans: &[Span]) -> Result<()>;
    /// All spans of a run in creation order.
    async fn run_spans(&self, run_id: Ulid) -> Result<Vec<Span>>;
}

pub enum CreateRun {
    Created,
    /// A run with the same idempotency key exists.
    Duplicate(String),
}

#[derive(Debug, Clone, Copy)]
pub enum SaveStep {
    /// Saved (or already saved when `duplicate`). `pending_empty` is true when no
    /// parallel steps remain pending (or no pending set was tracked).
    Saved { pending_empty: bool, duplicate: bool },
    /// The run no longer exists or is finished.
    NoRun,
}

#[derive(Debug, Default, Clone)]
pub struct MetaUpdate {
    pub started_at: Option<i64>,
    pub request_version: Option<i32>,
    pub force_step_plan: Option<bool>,
}

pub struct DebounceUpsert {
    pub id: String,
    pub fire_at: i64,
    pub created: bool,
    /// Event was older than the stored one and ignored.
    pub ignored: bool,
}

pub struct BatchAppend {
    pub batch_id: Ulid,
    pub len: u32,
    pub created: bool,
    pub duplicate: bool,
}

/// Rows deleted by one [`MetaStore::prune`] call.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Pruned {
    pub runs: u64,
    pub spans: u64,
    pub events: u64,
    /// False when the deadline cut the pass short.
    pub complete: bool,
}

/// Queue size, for metrics. Leased items count as not ready: their `at` is the lease expiry.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct QueueStats {
    /// All items, including leased, blocked and future ones.
    pub total: i64,
    /// Items due now (`at <= now`) and waiting for a worker.
    pub ready: i64,
    /// Earliest `at` of any item.
    pub oldest_at: Option<i64>,
}

/// Outcome of a GCRA check: Ok, or the earliest time (ms) the request would be admitted.
pub type Gcra = std::result::Result<(), i64>;

#[async_trait]
pub trait StateStore: Send + Sync {
    // ---- runs ----
    async fn create_run(
        &self,
        meta: &RunMeta,
        events: &[Value],
        idempotency_ttl_ms: i64,
    ) -> Result<CreateRun>;
    async fn load_run(&self, run_id: Ulid) -> Result<Option<RunState>>;
    async fn load_meta(&self, run_id: Ulid) -> Result<Option<RunMeta>>;
    async fn update_meta(&self, run_id: Ulid, update: &MetaUpdate) -> Result<()>;
    async fn save_step(&self, run_id: Ulid, step_id: &str, value: &Value) -> Result<SaveStep>;
    async fn set_pending(&self, run_id: Ulid, ids: &[String]) -> Result<()>;
    /// Transition a running run to a terminal status. Returns false if it was already terminal.
    async fn end_run(&self, run_id: Ulid, status: RunStatus, at: i64) -> Result<bool>;
    async fn load_steps(&self, run_id: Ulid) -> Result<Vec<(String, Value)>>;
    async fn load_events(&self, run_id: Ulid) -> Result<Vec<Value>>;

    // ---- queue ----
    /// Enqueue; returns false if an item with the same id exists or was recently completed.
    async fn enqueue(&self, item: &QueueItem) -> Result<bool>;
    /// Lease up to `limit` due items for `lease_ms`.
    async fn lease(&self, now: i64, limit: i64, lease_ms: i64) -> Result<Vec<QueueItem>>;
    async fn extend_lease(&self, item_id: &str, lease_id: &str, until: i64) -> Result<bool>;
    /// Remove a leased item after processing, remembering its id for dedup.
    async fn ack(&self, item: &QueueItem) -> Result<()>;
    /// Return a leased item to the queue at `at` with `attempt`.
    async fn requeue(
        &self,
        item: &QueueItem,
        at: i64,
        attempt: u32,
        blocked_on: Option<&str>,
    ) -> Result<()>;
    /// Move a non-leased item to a new time. Returns false if absent or leased.
    async fn reschedule(&self, item_id: &str, at: i64) -> Result<bool>;
    async fn remove_item(&self, item_id: &str) -> Result<()>;
    async fn remove_run_items(&self, run_id: Ulid, except: Option<&str>) -> Result<()>;
    async fn run_items(&self, run_id: Ulid, limit: i64) -> Result<Vec<QueueItem>>;
    /// Earliest `at` of any item, for worker sleep scheduling.
    async fn next_due(&self) -> Result<Option<i64>>;
    /// Make up to `n` items blocked on any of `keys` due now.
    async fn wake_blocked(&self, keys: &[String], n: i64) -> Result<()>;
    /// Queue size at `now` (ms), for metrics.
    async fn queue_stats(&self, now: i64) -> Result<QueueStats>;
    /// Check the store is reachable.
    async fn ping(&self) -> Result<()>;

    // ---- concurrency ----
    /// Try to take a slot on every key. Returns the first saturated key on failure.
    async fn acquire_concurrency(
        &self,
        holder: &str,
        keys: &[ConcurrencyKey],
        expires_at: i64,
    ) -> Result<Option<String>>;
    async fn release_concurrency(&self, holder: &str) -> Result<Vec<String>>;
    async fn extend_concurrency(&self, holder: &str, expires_at: i64) -> Result<()>;
    async fn concurrency_in_use(&self, key: &str) -> Result<i64>;

    // ---- rate limiting ----
    /// GCRA: admitted iff `tat - now <= tolerance_ms`; on admit `tat = max(tat, now) + emission_ms`.
    async fn gcra(&self, key: &str, now: i64, emission_ms: i64, tolerance_ms: i64) -> Result<Gcra>;

    // ---- key/value ----
    /// Set if absent. Returns the existing value if present.
    async fn kv_set_nx(&self, key: &str, value: &str, ttl_ms: Option<i64>)
    -> Result<Option<String>>;
    async fn kv_get(&self, key: &str) -> Result<Option<String>>;
    /// Delete, optionally only if the current value equals `if_value`.
    async fn kv_del(&self, key: &str, if_value: Option<&str>) -> Result<bool>;

    // ---- pauses ----
    async fn save_pause(&self, pause: &Pause) -> Result<bool>;
    async fn get_pause(&self, id: Uuid) -> Result<Option<Pause>>;
    async fn pauses_for_event(&self, event: &str) -> Result<Vec<Pause>>;
    async fn pauses_for_signal(&self, signal: &str) -> Result<Vec<Pause>>;
    /// Atomically delete the pause and (if given) save `value` under `step_id`.
    /// Returns None if the pause no longer existed (someone else consumed it) or the run
    /// is gone; otherwise whether the pending parallel set is now empty.
    async fn consume_pause(&self, pause: &Pause, value: Option<&Value>) -> Result<Option<bool>>;
    async fn delete_pause(&self, id: Uuid) -> Result<()>;
    async fn delete_run_pauses(&self, run_id: Ulid) -> Result<()>;

    // ---- debounce ----
    #[allow(clippy::too_many_arguments)]
    async fn debounce_upsert(
        &self,
        key: &str,
        fn_id: Uuid,
        event_id: Ulid,
        event: &Value,
        event_ts: i64,
        period_ms: i64,
        timeout_ms: Option<i64>,
        now: i64,
    ) -> Result<DebounceUpsert>;
    /// Remove the debounce if its id matches, returning the stored (internal event id, event).
    async fn debounce_take(&self, key: &str, id: &str) -> Result<Option<(Ulid, Value)>>;

    // ---- batching ----
    async fn batch_append(
        &self,
        key: &str,
        fn_id: Uuid,
        event: &Value,
        event_id: Ulid,
        max_size: u32,
    ) -> Result<BatchAppend>;
    /// Close the batch and return its (internal event id, event) pairs in append order (once).
    async fn batch_take(&self, key: &str, batch_id: Ulid) -> Result<Option<Vec<(Ulid, Value)>>>;

    /// Periodic cleanup of expired keys/markers.
    async fn gc(&self, now: i64) -> Result<()>;
}
