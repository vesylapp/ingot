//! GraphQL scalars, enums, inputs and plain object types for the dashboard API.
//! Type and field names mirror Inngest's `gql.schema.graphql` so the official UI's
//! operations validate unchanged.
#![allow(clippy::enum_variant_names)]

use async_graphql::{Enum, InputObject, SimpleObject, Union, scalar};
use serde::{Deserialize, Serialize};
use serde_json::Value;

macro_rules! string_scalar {
    ($ty:ident, $name:literal) => {
        #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
        #[serde(transparent)]
        pub struct $ty(pub String);
        scalar!($ty, $name);
    };
}

string_scalar!(Time, "Time");
string_scalar!(Ulid, "ULID");
string_scalar!(Uuid, "UUID");
string_scalar!(Bytes, "Bytes");
string_scalar!(SpanMetadataKind, "SpanMetadataKind");
string_scalar!(SpanMetadataScope, "SpanMetadataScope");

macro_rules! json_scalar {
    ($ty:ident, $name:literal) => {
        #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
        #[serde(transparent)]
        pub struct $ty(pub Value);
        scalar!($ty, $name);
    };
}

json_scalar!(Unknown, "Unknown");
json_scalar!(Map, "Map");
json_scalar!(HttpHeaders, "HTTPHeaders");
json_scalar!(SpanMetadataValues, "SpanMetadataValues");

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct Int64(pub i64);
scalar!(Int64, "Int64");

pub fn time(ms: i64) -> Time {
    Time(crate::util::rfc3339_ms(ms))
}

pub fn parse_time(t: &Time) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(&t.0)
        .ok()
        .map(|d| d.timestamp_millis())
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum FunctionRunStatus {
    Completed,
    Failed,
    Cancelled,
    Running,
    Queued,
    Skipped,
}

impl FunctionRunStatus {
    pub fn as_filter(&self) -> &'static str {
        match self {
            FunctionRunStatus::Completed => "COMPLETED",
            FunctionRunStatus::Failed => "FAILED",
            FunctionRunStatus::Cancelled => "CANCELLED",
            FunctionRunStatus::Running => "RUNNING",
            FunctionRunStatus::Queued => "QUEUED",
            FunctionRunStatus::Skipped => "SKIPPED",
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug, Default)]
pub enum RunsV2OrderByField {
    #[default]
    QueuedAt,
    StartedAt,
    EndedAt,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum RunsOrderByDirection {
    Asc,
    Desc,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum RunTraceSpanStatus {
    Failed,
    Queued,
    Running,
    Completed,
    Waiting,
    Cancelled,
    Skipped,
}

impl RunTraceSpanStatus {
    pub fn parse(s: &str) -> RunTraceSpanStatus {
        match s {
            "FAILED" => RunTraceSpanStatus::Failed,
            "QUEUED" => RunTraceSpanStatus::Queued,
            "COMPLETED" => RunTraceSpanStatus::Completed,
            "WAITING" => RunTraceSpanStatus::Waiting,
            "CANCELLED" => RunTraceSpanStatus::Cancelled,
            "SKIPPED" => RunTraceSpanStatus::Skipped,
            _ => RunTraceSpanStatus::Running,
        }
    }
    pub fn is_open(&self) -> bool {
        matches!(
            self,
            RunTraceSpanStatus::Queued | RunTraceSpanStatus::Running | RunTraceSpanStatus::Waiting
        )
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum StepOp {
    Invoke,
    Run,
    Sleep,
    WaitForEvent,
    AiGateway,
    WaitForSignal,
}

impl StepOp {
    pub fn parse(s: &str) -> Option<StepOp> {
        Some(match s {
            "INVOKE" => StepOp::Invoke,
            "RUN" => StepOp::Run,
            "SLEEP" => StepOp::Sleep,
            "WAIT_FOR_EVENT" => StepOp::WaitForEvent,
            "AI_GATEWAY" => StepOp::AiGateway,
            "WAIT_FOR_SIGNAL" => StepOp::WaitForSignal,
            _ => return None,
        })
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum AppMethod {
    Serve,
    Connect,
    Api,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum FunctionTriggerTypes {
    Event,
    Cron,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum ConcurrencyScope {
    Account,
    Environment,
    Function,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum SingletonMode {
    Skip,
    Cancel,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum EventStatus {
    Running,
    Completed,
    Paused,
    Failed,
    PartiallyFailed,
    NoFunctions,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum RunDeferStatus {
    Scheduled,
    Aborted,
    Rejected,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(name = "ConnectV1ConnectionStatus")]
pub enum ConnectionStatus {
    Connected,
    Ready,
    Draining,
    Disconnecting,
    Disconnected,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug, Default)]
#[graphql(name = "ConnectV1WorkerConnectionsOrderByField")]
pub enum ConnectionsOrderByField {
    #[default]
    ConnectedAt,
    LastHeartbeatAt,
    DisconnectedAt,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(name = "ConnectV1WorkerConnectionsOrderByDirection")]
pub enum ConnectionsOrderByDirection {
    Asc,
    Desc,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum InsightsColumnType {
    String,
    Number,
    Boolean,
    Datetime,
    Json,
    Unknown,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum InsightsColumnHint {
    AppId,
    FunctionId,
    RunId,
    EventId,
    Session,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum InsightsDiagnosticSeverity {
    Info,
    Warning,
    Error,
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

#[derive(InputObject, Debug, Clone)]
pub struct RunsFilterV2 {
    pub from: Time,
    pub until: Option<Time>,
    #[graphql(default_with = "Some(RunsV2OrderByField::QueuedAt)")]
    pub time_field: Option<RunsV2OrderByField>,
    pub status: Option<Vec<FunctionRunStatus>>,
    #[graphql(name = "functionIDs")]
    pub function_ids: Option<Vec<Uuid>>,
    #[graphql(name = "appIDs")]
    pub app_ids: Option<Vec<Uuid>>,
    pub is_deferred: Option<bool>,
    pub query: Option<String>,
}

#[derive(InputObject, Debug, Clone)]
pub struct RunsV2OrderBy {
    pub field: RunsV2OrderByField,
    pub direction: RunsOrderByDirection,
}

#[derive(InputObject, Debug, Clone)]
pub struct EventsFilter {
    pub event_names: Option<Vec<String>>,
    pub from: Time,
    #[graphql(default = false)]
    pub include_internal_events: bool,
    pub query: Option<String>,
    pub until: Option<Time>,
}

#[derive(InputObject, Debug, Clone)]
pub struct EventQuery {
    #[graphql(default_with = "async_graphql::ID::from(\"local\")")]
    pub workspace_id: async_graphql::ID,
    pub event_id: async_graphql::ID,
}

#[derive(InputObject, Debug, Clone)]
pub struct FunctionQuery {
    #[graphql(default_with = "async_graphql::ID::from(\"local\")")]
    pub workspace_id: async_graphql::ID,
    pub function_slug: String,
}

#[derive(InputObject, Debug, Clone)]
pub struct FunctionRunQuery {
    #[graphql(default_with = "async_graphql::ID::from(\"local\")")]
    pub workspace_id: async_graphql::ID,
    pub function_run_id: async_graphql::ID,
}

#[derive(InputObject, Debug, Clone)]
pub struct TimeRangeInput {
    pub from: Time,
    pub until: Option<Time>,
}

#[derive(InputObject, Debug, Clone)]
pub struct AppsFilterV1 {
    pub method: Option<AppMethod>,
}

#[derive(InputObject, Debug, Clone)]
pub struct CreateAppInput {
    pub url: String,
}

#[derive(InputObject, Debug, Clone)]
pub struct UpdateAppInput {
    pub id: String,
    pub url: String,
}

#[derive(InputObject, Debug, Clone)]
pub struct RerunFromStepInput {
    #[graphql(name = "stepID")]
    pub step_id: String,
    pub input: Option<Bytes>,
}

#[derive(InputObject, Debug, Clone)]
#[graphql(name = "ConnectV1WorkerConnectionsFilter")]
pub struct ConnectionsFilter {
    pub from: Option<Time>,
    pub until: Option<Time>,
    #[graphql(default_with = "Some(ConnectionsOrderByField::ConnectedAt)")]
    pub time_field: Option<ConnectionsOrderByField>,
    pub status: Option<Vec<ConnectionStatus>>,
    #[graphql(name = "appIDs")]
    pub app_ids: Option<Vec<Uuid>>,
}

#[derive(InputObject, Debug, Clone)]
#[graphql(name = "ConnectV1WorkerConnectionsOrderBy")]
pub struct ConnectionsOrderBy {
    pub field: ConnectionsOrderByField,
    pub direction: ConnectionsOrderByDirection,
}

// ---------------------------------------------------------------------------
// Plain objects
// ---------------------------------------------------------------------------

#[derive(SimpleObject, Debug, Clone, Default)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub has_previous_page: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(name = "SDKFeatureStatus")]
pub struct SdkFeatureStatus {
    pub ready: bool,
    pub reason: Option<i32>,
}

#[derive(SimpleObject, Debug, Clone, Default)]
#[graphql(name = "SDKFeatureReadiness")]
pub struct SdkFeatureReadiness {
    pub ai_metadata_extraction: Option<SdkFeatureStatus>,
    pub extended_traces: Option<SdkFeatureStatus>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct FunctionTrigger {
    #[graphql(name = "type")]
    pub kind: FunctionTriggerTypes,
    pub value: String,
    pub condition: Option<String>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct CancellationConfiguration {
    pub event: String,
    pub timeout: Option<String>,
    pub condition: Option<String>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct RetryConfiguration {
    pub value: i32,
    pub is_default: Option<bool>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct EventsBatchConfiguration {
    pub max_size: i32,
    pub timeout: String,
    pub key: Option<String>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ConcurrencyLimitConfiguration {
    pub value: i32,
    pub is_plan_limit: Option<bool>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ConcurrencyConfiguration {
    pub scope: ConcurrencyScope,
    pub limit: ConcurrencyLimitConfiguration,
    pub key: Option<String>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct RateLimitConfiguration {
    pub limit: i32,
    pub period: String,
    pub key: Option<String>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct DebounceConfiguration {
    pub period: String,
    pub key: Option<String>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ThrottleConfiguration {
    pub burst: i32,
    pub key: Option<String>,
    pub limit: i32,
    pub period: String,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct SingletonConfiguration {
    pub mode: SingletonMode,
    pub key: Option<String>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct FunctionConfiguration {
    pub cancellations: Vec<CancellationConfiguration>,
    pub retries: RetryConfiguration,
    pub priority: Option<String>,
    pub events_batch: Option<EventsBatchConfiguration>,
    pub concurrency: Vec<ConcurrencyConfiguration>,
    pub rate_limit: Option<RateLimitConfiguration>,
    pub debounce: Option<DebounceConfiguration>,
    pub throttle: Option<ThrottleConfiguration>,
    pub singleton: Option<SingletonConfiguration>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct StepEventWait {
    pub event_name: Option<String>,
    pub expression: Option<String>,
    pub expiry_time: Time,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct EventSource {
    pub id: async_graphql::ID,
    pub name: Option<String>,
    pub source_kind: String,
}

// ---- trace ----

#[derive(SimpleObject, Debug, Clone)]
pub struct InvokeStepInfo {
    #[graphql(name = "triggeringEventID")]
    pub triggering_event_id: Ulid,
    #[graphql(name = "functionID")]
    pub function_id: String,
    pub timeout: Time,
    #[graphql(name = "returnEventID")]
    pub return_event_id: Option<Ulid>,
    #[graphql(name = "runID")]
    pub run_id: Option<Ulid>,
    pub timed_out: Option<bool>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct SleepStepInfo {
    pub sleep_until: Time,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct WaitForEventStepInfo {
    pub event_name: String,
    pub expression: Option<String>,
    pub timeout: Time,
    #[graphql(name = "foundEventID")]
    pub found_event_id: Option<Ulid>,
    pub timed_out: Option<bool>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct WaitForSignalStepInfo {
    pub signal: String,
    pub timeout: Time,
    pub timed_out: Option<bool>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct RunStepInfo {
    #[graphql(name = "type")]
    pub kind: Option<String>,
}

#[derive(Union, Debug, Clone)]
pub enum StepInfo {
    InvokeStepInfo(InvokeStepInfo),
    SleepStepInfo(SleepStepInfo),
    WaitForEventStepInfo(WaitForEventStepInfo),
    RunStepInfo(RunStepInfo),
    WaitForSignalStepInfo(WaitForSignalStepInfo),
}

#[derive(SimpleObject, Debug, Clone)]
pub struct RunTraceConcurrencyKey {
    pub scope: String,
    pub expression: String,
    pub value: String,
    pub expression_truncated: bool,
    pub value_truncated: bool,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct SpanMetadata {
    pub scope: SpanMetadataScope,
    pub kind: SpanMetadataKind,
    pub is_user: bool,
    pub values: SpanMetadataValues,
    pub updated_at: Time,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct UserlandSpan {
    pub span_name: Option<String>,
    pub span_kind: Option<String>,
    pub service_name: Option<String>,
    pub resource_attrs: Option<Bytes>,
    pub scope_name: Option<String>,
    pub scope_version: Option<String>,
    pub span_attrs: Option<Bytes>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct RunTraceSpanResponseInfo {
    pub status_code: i32,
    pub headers: HttpHeaders,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct RunTraceSpan {
    #[graphql(name = "appID")]
    pub app_id: Uuid,
    #[graphql(name = "functionID")]
    pub function_id: Uuid,
    #[graphql(name = "runID")]
    pub run_id: Ulid,
    #[graphql(name = "spanID")]
    pub span_id: String,
    #[graphql(name = "traceID")]
    pub trace_id: String,
    #[graphql(name = "groupID")]
    pub group_id: Option<String>,
    pub name: String,
    pub status: RunTraceSpanStatus,
    pub attempts: Option<i32>,
    pub duration: Option<i32>,
    #[graphql(name = "outputID")]
    pub output_id: Option<String>,
    pub queued_at: Time,
    pub scheduled_at: Option<Time>,
    pub started_at: Option<Time>,
    pub ended_at: Option<Time>,
    pub children_spans: Vec<RunTraceSpan>,
    pub step_op: Option<StepOp>,
    #[graphql(name = "stepID")]
    pub step_id: Option<String>,
    pub step_info: Option<StepInfo>,
    pub step_type: String,
    pub group_kind: Option<String>,
    pub origin: Option<String>,
    pub is_root: bool,
    #[graphql(name = "parentSpanID")]
    pub parent_span_id: Option<String>,
    pub is_userland: bool,
    pub userland_span: Option<UserlandSpan>,
    #[graphql(name = "debugRunID")]
    pub debug_run_id: Option<Ulid>,
    #[graphql(name = "debugSessionID")]
    pub debug_session_id: Option<Ulid>,
    pub debug_paused: bool,
    pub skip_reason: Option<String>,
    #[graphql(name = "skipExistingRunID")]
    pub skip_existing_run_id: Option<String>,
    pub custom_concurrency_keys: Vec<RunTraceConcurrencyKey>,
    pub metadata: Vec<SpanMetadata>,
    pub response: Option<RunTraceSpanResponseInfo>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct StepError {
    pub message: String,
    pub name: Option<String>,
    pub stack: Option<String>,
    pub cause: Option<Unknown>,
}

#[derive(SimpleObject, Debug, Clone, Default)]
pub struct RunTraceSpanOutput {
    pub input: Option<Bytes>,
    pub data: Option<Bytes>,
    pub error: Option<StepError>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct RunTraceTrigger {
    pub event_name: Option<String>,
    #[graphql(name = "IDs")]
    pub ids: Vec<Ulid>,
    pub payloads: Vec<Bytes>,
    pub timestamp: Time,
    pub is_batch: bool,
    #[graphql(name = "batchID")]
    pub batch_id: Option<Ulid>,
    pub cron: Option<String>,
}

// ---- sessions / insights / connect (not backed by data yet) ----

#[derive(SimpleObject, Debug, Clone)]
pub struct SessionKey {
    pub session_key: String,
    pub created_at: Time,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct SessionFunction {
    pub slug: String,
    pub name: String,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct SessionGroup {
    pub session_key: String,
    pub session_id: String,
    pub run_count: i32,
    pub failed_run_count: i32,
    pub failure_rate: f64,
    pub last_active_at: Time,
    pub functions: Vec<SessionFunction>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct SessionRun {
    pub id: String,
    pub function_slug: String,
    pub event_name: Option<String>,
    pub status: String,
    pub queued_at: Time,
    pub started_at: Option<Time>,
    pub ended_at: Option<Time>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct InsightsPathSegment {
    pub key: Option<String>,
    pub wildcard: bool,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct InsightsPathHint {
    pub path: Vec<InsightsPathSegment>,
    pub hint: InsightsColumnHint,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct InsightsQueryColumn {
    pub name: String,
    #[graphql(name = "type")]
    pub kind: InsightsColumnType,
    pub path_hints: Vec<InsightsPathHint>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct InsightsQueryInfo {
    pub primary_table: Option<String>,
    pub tables: Vec<String>,
    pub limited: bool,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct InsightsDiagnosticPosition {
    pub line: i32,
    pub column: i32,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct InsightsDiagnostic {
    pub start: InsightsDiagnosticPosition,
    pub end: InsightsDiagnosticPosition,
    pub severity: InsightsDiagnosticSeverity,
    pub code: String,
    pub message: String,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct InsightsQueryResult {
    pub columns: Vec<InsightsQueryColumn>,
    pub rows: Vec<Vec<Option<Unknown>>>,
    pub info: InsightsQueryInfo,
    pub diagnostics: Vec<InsightsDiagnostic>,
}
