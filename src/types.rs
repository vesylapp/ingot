//! Core domain types shared across the API, engine and stores.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use ulid::Ulid;
use uuid::Uuid;

/// Fixed account / environment identifiers used by self-hosted Inngest.
pub const ACCOUNT_ID: Uuid = Uuid::from_u128(0x00000000_0000_4000_a000_000000000000);
pub const ENV_ID: Uuid = Uuid::from_u128(0x00000000_0000_4000_b000_000000000000);

pub const EVENT_FN_FINISHED: &str = "inngest/function.finished";
pub const EVENT_FN_FAILED: &str = "inngest/function.failed";
pub const EVENT_FN_CANCELLED: &str = "inngest/function.cancelled";
pub const EVENT_FN_INVOKED: &str = "inngest/function.invoked";
pub const EVENT_CRON: &str = "inngest/scheduled.timer";

pub const DEFAULT_RETRIES: u32 = 4;
pub const MAX_RETRIES: u32 = 20;
pub const DEFAULT_FN_CONCURRENCY: u32 = 1000;

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// An event as sent by clients and passed to SDKs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default, deserialize_with = "de_object")]
    pub data: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<Value>,
    #[serde(default)]
    pub ts: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub v: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
}

fn de_object<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Map<String, Value>, D::Error> {
    match Value::deserialize(d)? {
        Value::Null => Ok(Map::new()),
        Value::Object(m) => Ok(m),
        _ => Err(serde::de::Error::custom("data must be an object")),
    }
}

impl Event {
    /// JSON shape sent to SDKs and stored in run state.
    pub fn to_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("id".into(), Value::String(self.id.clone()));
        m.insert("name".into(), Value::String(self.name.clone()));
        m.insert("data".into(), Value::Object(self.data.clone()));
        m.insert(
            "user".into(),
            self.user.clone().unwrap_or_else(|| Value::Object(Map::new())),
        );
        m.insert("ts".into(), Value::from(self.ts));
        if let Some(v) = &self.v {
            m.insert("v".into(), Value::String(v.clone()));
        }
        Value::Object(m)
    }
}

/// An event as received and stored by the server.
#[derive(Debug, Clone)]
pub struct TrackedEvent {
    pub internal_id: Ulid,
    pub event: Event,
    pub received_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
    Overflowed,
    Scheduled,
    Unknown,
    Skipped,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Running => "Running",
            RunStatus::Completed => "Completed",
            RunStatus::Failed => "Failed",
            RunStatus::Cancelled => "Cancelled",
            RunStatus::Overflowed => "Overflowed",
            RunStatus::Scheduled => "Scheduled",
            RunStatus::Unknown => "Unknown",
            RunStatus::Skipped => "Skipped",
        }
    }
    pub fn parse(s: &str) -> RunStatus {
        match s {
            "Running" => RunStatus::Running,
            "Completed" => RunStatus::Completed,
            "Failed" => RunStatus::Failed,
            "Cancelled" => RunStatus::Cancelled,
            "Overflowed" => RunStatus::Overflowed,
            "Scheduled" => RunStatus::Scheduled,
            "Skipped" => RunStatus::Skipped,
            _ => RunStatus::Unknown,
        }
    }
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled | RunStatus::Skipped
        )
    }
}

// ---------------------------------------------------------------------------
// Function configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trigger {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jitter: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConcurrencyScope {
    Fn,
    Env,
    Account,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConcurrencyLimit {
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub scope: ConcurrencyScope,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Throttle {
    pub limit: u32,
    pub period_ms: i64,
    #[serde(default)]
    pub burst: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateLimit {
    pub limit: u32,
    pub period_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Debounce {
    pub period_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchEvents {
    pub max_size: u32,
    pub timeout_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_expr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelOn {
    pub event: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_expr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SingletonMode {
    Skip,
    Cancel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Singleton {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub mode: SingletonMode,
}

/// Parsed, validated function definition used by the engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Function {
    pub id: Uuid,
    pub app_id: Uuid,
    pub app_name: String,
    pub slug: String,
    pub name: String,
    pub version: i32,
    pub url: String,
    pub retries: u32,
    pub triggers: Vec<Trigger>,
    #[serde(default)]
    pub concurrency: Vec<ConcurrencyLimit>,
    #[serde(default)]
    pub throttle: Option<Throttle>,
    #[serde(default)]
    pub rate_limit: Option<RateLimit>,
    #[serde(default)]
    pub debounce: Option<Debounce>,
    #[serde(default)]
    pub batch: Option<BatchEvents>,
    #[serde(default)]
    pub priority_run: Option<String>,
    #[serde(default)]
    pub cancel: Vec<CancelOn>,
    #[serde(default)]
    pub timeout_start_ms: Option<i64>,
    #[serde(default)]
    pub timeout_finish_ms: Option<i64>,
    #[serde(default)]
    pub singleton: Option<Singleton>,
    /// Raw config as sent by the SDK.
    pub config: Value,
}

impl Function {
    pub fn max_attempts(&self) -> u32 {
        self.retries + 1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct App {
    pub id: Uuid,
    pub name: String,
    pub url: String,
    pub sdk_language: String,
    pub sdk_version: String,
    pub framework: String,
    pub checksum: String,
    pub error: Option<String>,
    pub method: String,
    pub app_version: Option<String>,
    pub created_at: i64,
}

// ---------------------------------------------------------------------------
// Run state
// ---------------------------------------------------------------------------

/// Evaluated concurrency constraint attached to a run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConcurrencyKey {
    pub key: String,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThrottleKey {
    pub key: String,
    pub limit: u32,
    pub burst: u32,
    pub period_ms: i64,
}

/// Run metadata persisted in the state store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMeta {
    pub run_id: Ulid,
    pub fn_id: Uuid,
    pub fn_slug: String,
    pub fn_version: i32,
    pub app_id: Uuid,
    pub status: RunStatus,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub event_ids: Vec<Ulid>,
    pub batch_id: Option<Ulid>,
    pub cron: Option<String>,
    pub original_run_id: Option<Ulid>,
    pub idempotency_key: String,
    pub request_version: i32,
    pub force_step_plan: bool,
    pub priority_factor: i64,
    pub concurrency: Vec<ConcurrencyKey>,
    pub throttle: Option<ThrottleKey>,
    pub singleton_key: Option<String>,
    pub max_attempts: u32,
    /// True if the run's trigger is an internal fn-lifecycle event (no lifecycle events are emitted).
    pub suppress_lifecycle: bool,
}

/// Full run state needed to execute a step.
#[derive(Debug, Clone)]
pub struct RunState {
    pub meta: RunMeta,
    pub events: Vec<Value>,
    /// Memoized step results in save (stack) order.
    pub steps: Vec<(String, Value)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: Ulid,
    pub fn_id: Uuid,
    pub fn_slug: String,
    pub app_id: Uuid,
    pub fn_version: i32,
    pub event_id: Option<Ulid>,
    pub batch_id: Option<Ulid>,
    pub cron: Option<String>,
    pub original_run_id: Option<Ulid>,
    pub status: RunStatus,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub output: Option<Value>,
}

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueueKind {
    Start,
    Edge,
    Sleep,
    EdgeError,
    Pause,
    Debounce,
    ScheduleBatch,
    Cancel,
    Cron,
}

impl QueueKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueueKind::Start => "start",
            QueueKind::Edge => "edge",
            QueueKind::Sleep => "sleep",
            QueueKind::EdgeError => "edge-error",
            QueueKind::Pause => "pause",
            QueueKind::Debounce => "debounce",
            QueueKind::ScheduleBatch => "schedule-batch",
            QueueKind::Cancel => "cancel",
            QueueKind::Cron => "cron",
        }
    }
    pub fn parse(s: &str) -> Option<QueueKind> {
        Some(match s {
            "start" => QueueKind::Start,
            "edge" => QueueKind::Edge,
            "sleep" => QueueKind::Sleep,
            "edge-error" => QueueKind::EdgeError,
            "pause" => QueueKind::Pause,
            "debounce" => QueueKind::Debounce,
            "schedule-batch" => QueueKind::ScheduleBatch,
            "cancel" => QueueKind::Cancel,
            "cron" => QueueKind::Cron,
            _ => return None,
        })
    }
    /// Kinds that invoke the SDK and thus count against concurrency limits.
    pub fn uses_concurrency(&self) -> bool {
        matches!(
            self,
            QueueKind::Start | QueueKind::Edge | QueueKind::Sleep | QueueKind::EdgeError
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueItem {
    /// Unique item id (derived from job id when present).
    pub id: String,
    pub kind: QueueKind,
    pub run_id: Option<Ulid>,
    pub fn_id: Option<Uuid>,
    /// Earliest time the item may run (ms). Items are leased in `at` order.
    pub at: i64,
    /// Whether the id derives from a deterministic job id and should be remembered
    /// after completion to dedupe re-enqueues.
    #[serde(default)]
    pub dedupe: bool,
    pub attempt: u32,
    pub max_attempts: u32,
    pub payload: Value,
    /// Lease token set when leased.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
}

impl QueueItem {
    pub fn new(job_id: Option<String>, kind: QueueKind, at: i64, payload: Value) -> QueueItem {
        let dedupe = job_id.is_some();
        let id = match job_id {
            Some(j) => crate::util::hash36(&j),
            None => Ulid::generate().to_string(),
        };
        QueueItem {
            id,
            kind,
            run_id: None,
            fn_id: None,
            at,
            dedupe,
            attempt: 0,
            max_attempts: 1,
            payload,
            lease_id: None,
        }
    }
    pub fn for_run(mut self, meta: &RunMeta) -> QueueItem {
        self.run_id = Some(meta.run_id);
        self.fn_id = Some(meta.fn_id);
        self.max_attempts = meta.max_attempts;
        // Priority shifts immediately-due items earlier (or later) in lease order.
        if meta.priority_factor != 0 && self.at <= now_ms() + 2000 {
            self.at -= meta.priority_factor * 1000;
        }
        self
    }
    pub fn str_payload(&self, key: &str) -> Option<&str> {
        self.payload.get(key).and_then(|v| v.as_str())
    }
}

// ---------------------------------------------------------------------------
// Pauses
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseKind {
    WaitForEvent,
    Invoke,
    Signal,
    Cancel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pause {
    pub id: Uuid,
    pub kind: PauseKind,
    pub run_id: Ulid,
    pub fn_id: Uuid,
    /// Step id the result is saved under (empty for cancel pauses).
    pub step_id: String,
    /// Event name this pause is indexed by (waitForEvent / cancel); signal name for signals.
    pub event: Option<String>,
    pub expression: Option<String>,
    /// The run's triggering event, bound as `event` when evaluating `expression`.
    pub trigger_event: Option<Value>,
    pub trigger_event_id: Option<Ulid>,
    pub expires_at: i64,
    pub created_at: i64,
    /// Job id of the timeout queue item.
    pub timeout_job: Option<String>,
    pub correlation_id: Option<String>,
}
