//! Dashboard GraphQL API (`POST /v0/gql`), compatible with the Inngest dev-server UI.

mod trace;
mod types;

use crate::engine::Engine;
use crate::store::{EventQuery as StoreEventQuery, RunQuery, RunTimeField};
use crate::store::search::{PAGE_SCAN_LIMIT, RunSearch};
use crate::types as t;
use async_graphql::{Context, EmptySubscription, ID, Object, Result, Schema, SimpleObject};
use axum::body::Bytes as BodyBytes;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use std::sync::Arc;
use types::*;

pub type DashboardSchema = Schema<Query, Mutation, EmptySubscription>;

pub fn schema(engine: Arc<Engine>) -> DashboardSchema {
    Schema::build(Query, Mutation, EmptySubscription)
        .data(engine)
        .finish()
}

pub async fn handler(State(schema): State<DashboardSchema>, body: BodyBytes) -> Response {
    let req: async_graphql::Request = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                [(header::CONTENT_TYPE, "application/json")],
                json!({ "errors": [{ "message": format!("invalid request: {e}") }] }).to_string(),
            )
                .into_response();
        }
    };
    let resp = schema.execute(req).await;
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()),
    )
        .into_response()
}

fn engine<'a>(ctx: &Context<'a>) -> &'a Arc<Engine> {
    ctx.data_unchecked::<Arc<Engine>>()
}

fn err(e: anyhow::Error) -> async_graphql::Error {
    async_graphql::Error::new(e.to_string())
}

/// A search expression that does not compile. The dashboard shows errors with this code under
/// the search box.
fn invalid_expression(msg: &str) -> async_graphql::Error {
    use async_graphql::ErrorExtensions;
    async_graphql::Error::new(msg).extend_with(|_, e| e.set("code", "expression_invalid"))
}

fn parse_ulid(s: &str) -> Result<ulid::Ulid> {
    ulid::Ulid::from_string(s).map_err(|_| async_graphql::Error::new(format!("invalid ULID: {s}")))
}

fn json_string(v: &Value) -> String {
    serde_json::to_string(v).unwrap_or_default()
}

/// Dashboard status of a run.
pub fn run_status(r: &t::RunRecord) -> FunctionRunStatus {
    match r.status {
        t::RunStatus::Completed => FunctionRunStatus::Completed,
        t::RunStatus::Failed => FunctionRunStatus::Failed,
        t::RunStatus::Cancelled => FunctionRunStatus::Cancelled,
        t::RunStatus::Skipped => FunctionRunStatus::Skipped,
        _ if r.exec_started_at.is_none() => FunctionRunStatus::Queued,
        _ => FunctionRunStatus::Running,
    }
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

pub struct App(t::App);

#[Object]
impl App {
    async fn id(&self) -> ID {
        ID(self.0.id.to_string())
    }
    #[graphql(name = "externalID")]
    async fn external_id(&self) -> &str {
        &self.0.name
    }
    async fn name(&self) -> &str {
        &self.0.name
    }
    async fn sdk_language(&self) -> &str {
        &self.0.sdk_language
    }
    async fn sdk_version(&self) -> &str {
        &self.0.sdk_version
    }
    async fn framework(&self) -> Option<&str> {
        Some(self.0.framework.as_str()).filter(|f| !f.is_empty())
    }
    async fn url(&self) -> Option<&str> {
        Some(self.0.url.as_str())
    }
    async fn checksum(&self) -> Option<&str> {
        Some(self.0.checksum.as_str()).filter(|c| !c.is_empty())
    }
    async fn error(&self) -> Option<&str> {
        self.0.error.as_deref()
    }
    async fn functions(&self, ctx: &Context<'_>) -> Result<Vec<Function>> {
        let fns = engine(ctx).meta.app_functions(self.0.id).await.map_err(err)?;
        Ok(fns.into_iter().map(|f| Function(Arc::new(f))).collect())
    }
    async fn method(&self) -> AppMethod {
        match self.0.method.as_str() {
            "connect" => AppMethod::Connect,
            "api" => AppMethod::Api,
            _ => AppMethod::Serve,
        }
    }
    async fn app_version(&self) -> Option<&str> {
        self.0.app_version.as_deref()
    }
    async fn sdk_feature_readiness(&self) -> SdkFeatureReadiness {
        SdkFeatureReadiness::default()
    }
    async fn connected(&self) -> bool {
        self.0.error.is_none()
    }
    async fn function_count(&self, ctx: &Context<'_>) -> Result<i32> {
        let fns = engine(ctx).meta.app_functions(self.0.id).await.map_err(err)?;
        Ok(fns.len() as i32)
    }
    async fn autodiscovered(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Function
// ---------------------------------------------------------------------------

pub struct Function(Arc<t::Function>);

fn raw_str(f: &t::Function, path: &[&str]) -> Option<String> {
    let mut v = &f.config;
    for p in path {
        v = v.get(p)?;
    }
    v.as_str().map(String::from)
}

#[Object]
impl Function {
    async fn id(&self) -> String {
        self.0.id.to_string()
    }
    async fn name(&self) -> &str {
        &self.0.name
    }
    async fn slug(&self) -> &str {
        &self.0.slug
    }
    async fn failure_handler(&self, ctx: &Context<'_>) -> Option<Function> {
        engine(ctx)
            .registry
            .by_slug(&format!("{}-failure", self.0.slug))
            .map(Function)
    }
    async fn config(&self) -> String {
        json_string(&self.0.config)
    }
    async fn configuration(&self) -> FunctionConfiguration {
        let f = &self.0;
        FunctionConfiguration {
            cancellations: f
                .cancel
                .iter()
                .map(|c| CancellationConfiguration {
                    event: c.event.clone(),
                    timeout: c.timeout.clone(),
                    condition: c.if_expr.clone(),
                })
                .collect(),
            retries: RetryConfiguration {
                value: f.retries as i32,
                is_default: Some(
                    f.config
                        .pointer("/steps/step/retries/attempts")
                        .is_none()
                        && f.config.get("retries").is_none(),
                ),
            },
            priority: f.priority_run.clone(),
            events_batch: f.batch.as_ref().map(|b| EventsBatchConfiguration {
                max_size: b.max_size as i32,
                timeout: raw_str(f, &["batchEvents", "timeout"]).unwrap_or_default(),
                key: b.key.clone(),
            }),
            concurrency: f
                .concurrency
                .iter()
                .map(|c| ConcurrencyConfiguration {
                    scope: match c.scope {
                        t::ConcurrencyScope::Fn => ConcurrencyScope::Function,
                        t::ConcurrencyScope::Env => ConcurrencyScope::Environment,
                        t::ConcurrencyScope::Account => ConcurrencyScope::Account,
                    },
                    limit: ConcurrencyLimitConfiguration {
                        value: c.limit as i32,
                        is_plan_limit: Some(false),
                    },
                    key: c.key.clone(),
                })
                .collect(),
            rate_limit: f.rate_limit.as_ref().map(|r| RateLimitConfiguration {
                limit: r.limit as i32,
                period: raw_str(f, &["rateLimit", "period"])
                    .unwrap_or_else(|| crate::util::format_go_duration(r.period_ms)),
                key: r.key.clone(),
            }),
            debounce: f.debounce.as_ref().map(|d| DebounceConfiguration {
                period: raw_str(f, &["debounce", "period"])
                    .unwrap_or_else(|| crate::util::format_go_duration(d.period_ms)),
                key: d.key.clone(),
            }),
            throttle: f.throttle.as_ref().map(|th| ThrottleConfiguration {
                burst: th.burst as i32,
                key: th.key.clone(),
                limit: th.limit as i32,
                period: raw_str(f, &["throttle", "period"])
                    .unwrap_or_else(|| crate::util::format_go_duration(th.period_ms)),
            }),
            singleton: f.singleton.as_ref().map(|s| SingletonConfiguration {
                mode: match s.mode {
                    t::SingletonMode::Skip => SingletonMode::Skip,
                    t::SingletonMode::Cancel => SingletonMode::Cancel,
                },
                key: s.key.clone(),
            }),
        }
    }
    async fn concurrency(&self) -> i32 {
        self.0
            .concurrency
            .iter()
            .find(|c| c.key.is_none() && c.scope == t::ConcurrencyScope::Fn)
            .map(|c| c.limit as i32)
            .unwrap_or(0)
    }
    async fn triggers(&self) -> Option<Vec<FunctionTrigger>> {
        Some(
            self.0
                .triggers
                .iter()
                .map(|tr| match (&tr.event, &tr.cron) {
                    (Some(e), _) => FunctionTrigger {
                        kind: FunctionTriggerTypes::Event,
                        value: e.clone(),
                        condition: tr.expression.clone().filter(|x| !x.is_empty()),
                    },
                    (None, c) => FunctionTrigger {
                        kind: FunctionTriggerTypes::Cron,
                        value: c.clone().unwrap_or_default(),
                        condition: None,
                    },
                })
                .collect(),
        )
    }
    async fn url(&self) -> &str {
        &self.0.url
    }
    #[graphql(name = "appID")]
    async fn app_id(&self) -> String {
        self.0.app_id.to_string()
    }
    async fn app(&self, ctx: &Context<'_>) -> Result<App> {
        let a = engine(ctx).meta.get_app(self.0.app_id).await.map_err(err)?;
        Ok(App(a.unwrap_or_else(|| t::App {
            id: self.0.app_id,
            name: self.0.app_name.clone(),
            url: String::new(),
            sdk_language: String::new(),
            sdk_version: String::new(),
            framework: String::new(),
            checksum: String::new(),
            error: None,
            method: "serve".into(),
            app_version: None,
            created_at: 0,
        })))
    }
}

/// Look up a function by id, including archived (unknown) ones as a placeholder.
fn function_for(e: &Engine, r: &t::RunRecord) -> Function {
    match e.registry.get(r.fn_id) {
        Some(f) => Function(f),
        None => Function(Arc::new(t::Function {
            id: r.fn_id,
            app_id: r.app_id,
            app_name: String::new(),
            slug: r.fn_slug.clone(),
            name: r.fn_slug.clone(),
            version: r.fn_version,
            url: String::new(),
            retries: 0,
            triggers: vec![],
            concurrency: vec![],
            throttle: None,
            rate_limit: None,
            debounce: None,
            batch: None,
            priority_run: None,
            cancel: vec![],
            timeout_start_ms: None,
            timeout_finish_ms: None,
            singleton: None,
            config: json!({}),
        })),
    }
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

pub struct FunctionRunV2(t::RunRecord);

#[Object]
impl FunctionRunV2 {
    async fn id(&self) -> Ulid {
        Ulid(self.0.run_id.to_string())
    }
    #[graphql(name = "appID")]
    async fn app_id(&self) -> Uuid {
        Uuid(self.0.app_id.to_string())
    }
    async fn app(&self, ctx: &Context<'_>) -> Result<App> {
        function_for(engine(ctx), &self.0).app(ctx).await
    }
    #[graphql(name = "functionID")]
    async fn function_id(&self) -> Uuid {
        Uuid(self.0.fn_id.to_string())
    }
    async fn function(&self, ctx: &Context<'_>) -> Function {
        function_for(engine(ctx), &self.0)
    }
    #[graphql(name = "traceID")]
    async fn trace_id(&self) -> String {
        self.0.run_id.to_string()
    }
    async fn queued_at(&self) -> Time {
        time(self.0.started_at)
    }
    async fn started_at(&self) -> Option<Time> {
        self.0.exec_started_at.map(time)
    }
    async fn ended_at(&self) -> Option<Time> {
        self.0.ended_at.map(time)
    }
    async fn status(&self) -> FunctionRunStatus {
        run_status(&self.0)
    }
    #[graphql(name = "sourceID")]
    async fn source_id(&self) -> Option<String> {
        None
    }
    #[graphql(name = "triggerIDs")]
    async fn trigger_ids(&self) -> Vec<Ulid> {
        self.0
            .event_id
            .iter()
            .map(|e| Ulid(e.to_string()))
            .collect()
    }
    async fn event_name(&self) -> Option<&str> {
        if self.0.cron.is_some() {
            return None;
        }
        self.0.event_name.as_deref()
    }
    async fn is_batch(&self) -> bool {
        self.0.batch_id.is_some()
    }
    async fn batch_created_at(&self) -> Option<Time> {
        self.0.batch_id.map(|b| time(b.timestamp_ms() as i64))
    }
    async fn cron_schedule(&self) -> Option<&str> {
        self.0.cron.as_deref()
    }
    async fn output(&self) -> Option<Bytes> {
        self.0.output.as_ref().map(|o| Bytes(json_string(o)))
    }
    async fn trace(&self, ctx: &Context<'_>) -> Result<Option<RunTraceSpan>> {
        Ok(Some(trace::build(engine(ctx), &self.0).await.map_err(err)?))
    }
    #[graphql(name = "hasAI")]
    async fn has_ai(&self) -> bool {
        false
    }
    async fn defers(&self) -> Vec<RunDefer> {
        vec![]
    }
    async fn sibling_defers(&self) -> Vec<RunDefer> {
        vec![]
    }
    async fn deferred_from(&self) -> Vec<RunDeferredFrom> {
        vec![]
    }
    async fn is_deferred(&self) -> bool {
        false
    }
}

#[derive(SimpleObject)]
pub struct RunDefer {
    #[graphql(name = "hashedDeferID")]
    hashed_defer_id: String,
    #[graphql(name = "userlandDeferID")]
    userland_defer_id: String,
    fn_slug: String,
    status: RunDeferStatus,
    function: Option<Function>,
    #[graphql(name = "runID")]
    run_id: Option<Ulid>,
    run: Option<FunctionRunV2>,
}

#[derive(SimpleObject)]
pub struct RunDeferredFrom {
    #[graphql(name = "runID")]
    run_id: Ulid,
    function: Function,
    run: Option<FunctionRunV2>,
}

#[derive(SimpleObject)]
pub struct FunctionRunV2Edge {
    node: FunctionRunV2,
    cursor: String,
}

pub struct RunsV2Connection {
    edges: Vec<FunctionRunV2Edge>,
    page_info: PageInfo,
    query: RunQuery,
    search: Option<RunSearch>,
}

#[Object]
impl RunsV2Connection {
    async fn edges(&self) -> &Vec<FunctionRunV2Edge> {
        &self.edges
    }
    async fn page_info(&self) -> &PageInfo {
        &self.page_info
    }
    async fn total_count(&self, ctx: &Context<'_>) -> Result<i32> {
        let meta = &*engine(ctx).meta;
        let n = match &self.search {
            Some(s) => s.count(meta, &self.query).await,
            None => meta.count_runs(&self.query).await,
        };
        Ok(n.map_err(err)? as i32)
    }
}

/// Legacy run type (used by the event detail view and cancelRun).
pub struct FunctionRun(t::RunRecord);

#[Object]
impl FunctionRun {
    async fn id(&self) -> ID {
        ID(self.0.run_id.to_string())
    }
    #[graphql(name = "functionID")]
    async fn function_id(&self) -> String {
        self.0.fn_id.to_string()
    }
    async fn function(&self, ctx: &Context<'_>) -> Option<Function> {
        Some(function_for(engine(ctx), &self.0))
    }
    async fn status(&self) -> Option<FunctionRunStatus> {
        Some(run_status(&self.0))
    }
    async fn waiting_for(&self) -> Option<StepEventWait> {
        None
    }
    async fn pending_steps(&self) -> Option<i32> {
        Some(0)
    }
    async fn started_at(&self) -> Option<Time> {
        Some(time(self.0.started_at))
    }
    async fn finished_at(&self) -> Option<Time> {
        self.0.ended_at.map(time)
    }
    async fn output(&self) -> Option<String> {
        self.0.output.as_ref().map(json_string)
    }
    #[graphql(name = "batchID")]
    async fn batch_id(&self) -> Option<Ulid> {
        self.0.batch_id.map(|b| Ulid(b.to_string()))
    }
    #[graphql(name = "eventID")]
    async fn event_id(&self) -> ID {
        ID(self.0.event_id.map(|e| e.to_string()).unwrap_or_default())
    }
    async fn cron(&self) -> Option<&str> {
        self.0.cron.as_deref()
    }
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

fn raw_event(e: &t::TrackedEvent) -> String {
    json_string(&e.event.to_json())
}

pub struct EventV2(t::TrackedEvent);

#[Object]
impl EventV2 {
    #[graphql(name = "envID")]
    async fn env_id(&self) -> Uuid {
        Uuid(t::ENV_ID.to_string())
    }
    async fn id(&self) -> Ulid {
        Ulid(self.0.internal_id.to_string())
    }
    async fn idempotency_key(&self) -> Option<&str> {
        Some(self.0.event.id.as_str()).filter(|i| !i.is_empty())
    }
    async fn name(&self) -> &str {
        &self.0.event.name
    }
    async fn occurred_at(&self) -> Time {
        time(if self.0.event.ts > 0 { self.0.event.ts } else { self.0.received_at })
    }
    async fn raw(&self) -> String {
        raw_event(&self.0)
    }
    async fn received_at(&self) -> Time {
        time(self.0.received_at)
    }
    async fn runs(&self, ctx: &Context<'_>) -> Result<Vec<FunctionRunV2>> {
        let runs = engine(ctx)
            .meta
            .runs_for_event(self.0.internal_id)
            .await
            .map_err(err)?;
        Ok(runs.into_iter().map(FunctionRunV2).collect())
    }
    async fn source(&self) -> Option<EventSource> {
        None
    }
    async fn version(&self) -> Option<&str> {
        self.0.event.v.as_deref()
    }
}

#[derive(SimpleObject)]
pub struct EventsEdge {
    node: EventV2,
    cursor: String,
}

pub struct EventsConnection {
    edges: Vec<EventsEdge>,
    page_info: PageInfo,
    query: StoreEventQuery,
}

#[Object]
impl EventsConnection {
    async fn edges(&self) -> &Vec<EventsEdge> {
        &self.edges
    }
    async fn page_info(&self) -> &PageInfo {
        &self.page_info
    }
    async fn total_count(&self, ctx: &Context<'_>) -> Result<i32> {
        Ok(engine(ctx).meta.count_events(&self.query).await.map_err(err)? as i32)
    }
}

/// Legacy event type (GetEvent).
pub struct Event(t::TrackedEvent);

#[Object]
impl Event {
    async fn id(&self) -> Ulid {
        Ulid(self.0.internal_id.to_string())
    }
    #[graphql(name = "externalID")]
    async fn external_id(&self) -> Option<&str> {
        Some(self.0.event.id.as_str())
    }
    async fn name(&self) -> Option<&str> {
        Some(&self.0.event.name)
    }
    async fn created_at(&self) -> Option<Time> {
        Some(time(self.0.received_at))
    }
    async fn payload(&self) -> Option<String> {
        Some(json_string(&Value::Object(self.0.event.data.clone())))
    }
    async fn status(&self, ctx: &Context<'_>) -> Result<Option<EventStatus>> {
        let runs = engine(ctx)
            .meta
            .runs_for_event(self.0.internal_id)
            .await
            .map_err(err)?;
        if runs.is_empty() {
            return Ok(Some(EventStatus::NoFunctions));
        }
        let failed = runs.iter().filter(|r| r.status == t::RunStatus::Failed).count();
        let running = runs.iter().any(|r| !r.status.is_terminal());
        Ok(Some(if running {
            EventStatus::Running
        } else if failed == runs.len() {
            EventStatus::Failed
        } else if failed > 0 {
            EventStatus::PartiallyFailed
        } else {
            EventStatus::Completed
        }))
    }
    async fn pending_runs(&self, ctx: &Context<'_>) -> Result<Option<i32>> {
        let runs = engine(ctx)
            .meta
            .runs_for_event(self.0.internal_id)
            .await
            .map_err(err)?;
        Ok(Some(runs.iter().filter(|r| !r.status.is_terminal()).count() as i32))
    }
    async fn total_runs(&self, ctx: &Context<'_>) -> Result<Option<i32>> {
        let runs = engine(ctx)
            .meta
            .runs_for_event(self.0.internal_id)
            .await
            .map_err(err)?;
        Ok(Some(runs.len() as i32))
    }
    async fn raw(&self) -> Option<String> {
        Some(raw_event(&self.0))
    }
    async fn function_runs(&self, ctx: &Context<'_>) -> Result<Option<Vec<FunctionRun>>> {
        let runs = engine(ctx)
            .meta
            .runs_for_event(self.0.internal_id)
            .await
            .map_err(err)?;
        Ok(Some(runs.into_iter().map(FunctionRun).collect()))
    }
}

// ---------------------------------------------------------------------------
// Connect (no workers are supported yet; types exist so the UI's queries validate)
// ---------------------------------------------------------------------------

#[derive(SimpleObject)]
#[graphql(name = "ConnectV1WorkerConnection")]
pub struct WorkerConnection {
    id: Ulid,
    gateway_id: Ulid,
    instance_id: String,
    worker_ip: String,
    max_worker_concurrency: Int64,
    app_name: Option<String>,
    #[graphql(name = "appID")]
    app_id: Option<Uuid>,
    app: Option<App>,
    connected_at: Time,
    last_heartbeat_at: Option<Time>,
    disconnected_at: Option<Time>,
    disconnect_reason: Option<String>,
    status: ConnectionStatus,
    group_hash: String,
    sdk_lang: String,
    sdk_version: String,
    sdk_platform: String,
    sync_id: Option<Uuid>,
    build_id: Option<String>,
    app_version: Option<String>,
    function_count: i32,
    cpu_cores: i32,
    mem_bytes: i32,
    os: String,
}

#[derive(SimpleObject)]
#[graphql(name = "ConnectV1WorkerConnectionEdge")]
pub struct WorkerConnectionEdge {
    node: WorkerConnection,
    cursor: String,
}

#[derive(SimpleObject)]
#[graphql(name = "ConnectV1WorkerConnectionsConnection")]
pub struct WorkerConnectionsConnection {
    edges: Vec<WorkerConnectionEdge>,
    page_info: PageInfo,
    total_count: i32,
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

pub struct Query;

#[Object]
impl Query {
    async fn apps(&self, ctx: &Context<'_>, filter: Option<AppsFilterV1>) -> Result<Vec<App>> {
        let apps = engine(ctx).meta.list_apps().await.map_err(err)?;
        let method = filter.and_then(|f| f.method);
        Ok(apps
            .into_iter()
            .filter(|a| match method {
                None => true,
                Some(AppMethod::Connect) => a.method == "connect",
                Some(AppMethod::Api) => a.method == "api",
                Some(AppMethod::Serve) => a.method == "serve",
            })
            .map(App)
            .collect())
    }

    async fn app(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<App>> {
        let Ok(id) = uuid::Uuid::parse_str(&id.0) else {
            return Ok(None);
        };
        Ok(engine(ctx).meta.get_app(id).await.map_err(err)?.map(App))
    }

    async fn event(&self, ctx: &Context<'_>, query: EventQuery) -> Result<Option<Event>> {
        let id = parse_ulid(&query.event_id)?;
        Ok(engine(ctx).meta.get_event(id).await.map_err(err)?.map(Event))
    }

    #[graphql(name = "eventV2")]
    async fn event_v2(&self, ctx: &Context<'_>, id: Ulid) -> Result<EventV2> {
        let id = parse_ulid(&id.0)?;
        engine(ctx)
            .meta
            .get_event(id)
            .await
            .map_err(err)?
            .map(EventV2)
            .ok_or_else(|| async_graphql::Error::new("event not found"))
    }

    #[graphql(name = "eventsV2")]
    async fn events_v2(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 40)] first: i32,
        after: Option<String>,
        filter: EventsFilter,
    ) -> Result<EventsConnection> {
        let first = first.clamp(1, 1000) as i64;
        let query = StoreEventQuery {
            names: filter.event_names.unwrap_or_default(),
            received_after: parse_time(&filter.from),
            received_before: filter.until.as_ref().and_then(parse_time),
            include_internal: filter.include_internal_events,
            limit: first + 1,
            ..Default::default()
        };
        let mut page_query = query.clone();
        page_query.cursor = after.as_deref().filter(|a| !a.is_empty()).map(parse_ulid).transpose()?;
        let mut events = engine(ctx).meta.list_events(&page_query).await.map_err(err)?;
        let has_next = events.len() as i64 > first;
        events.truncate(first as usize);
        let edges: Vec<EventsEdge> = events
            .into_iter()
            .map(|e| EventsEdge {
                cursor: e.internal_id.to_string(),
                node: EventV2(e),
            })
            .collect();
        Ok(EventsConnection {
            page_info: PageInfo {
                has_next_page: has_next,
                has_previous_page: after.is_some(),
                start_cursor: edges.first().map(|e| e.cursor.clone()),
                end_cursor: edges.last().map(|e| e.cursor.clone()),
            },
            edges,
            query,
        })
    }

    async fn function_by_slug(&self, ctx: &Context<'_>, query: FunctionQuery) -> Option<Function> {
        engine(ctx).registry.by_slug(&query.function_slug).map(Function)
    }

    async fn functions(&self, ctx: &Context<'_>) -> Option<Vec<Function>> {
        let mut fns = engine(ctx).registry.all();
        fns.sort_by(|a, b| a.slug.cmp(&b.slug));
        Some(fns.into_iter().map(Function).collect())
    }

    async fn function_run(&self, ctx: &Context<'_>, query: FunctionRunQuery) -> Result<Option<FunctionRun>> {
        let id = parse_ulid(&query.function_run_id)?;
        Ok(engine(ctx).meta.get_run(id).await.map_err(err)?.map(FunctionRun))
    }

    async fn runs(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 100)] first: i32,
        after: Option<String>,
        order_by: Vec<RunsV2OrderBy>,
        filter: RunsFilterV2,
    ) -> Result<RunsV2Connection> {
        let first = first.clamp(1, 1000) as i64;
        let time_field = match order_by
            .first()
            .map(|o| o.field)
            .or(filter.time_field)
            .unwrap_or_default()
        {
            RunsV2OrderByField::QueuedAt => RunTimeField::Queued,
            RunsV2OrderByField::StartedAt => RunTimeField::Started,
            RunsV2OrderByField::EndedAt => RunTimeField::Ended,
        };
        let query = RunQuery {
            fn_id: filter
                .function_ids
                .as_ref()
                .and_then(|f| f.first())
                .and_then(|f| uuid::Uuid::parse_str(&f.0).ok()),
            app_ids: filter
                .app_ids
                .unwrap_or_default()
                .iter()
                .filter_map(|a| uuid::Uuid::parse_str(&a.0).ok())
                .collect(),
            ui_statuses: filter
                .status
                .unwrap_or_default()
                .iter()
                .map(|s| s.as_filter().to_string())
                .collect(),
            time_field,
            from: parse_time(&filter.from),
            until: filter.until.as_ref().and_then(parse_time),
            limit: first + 1,
            ..Default::default()
        };
        let search = match filter.query.as_deref().map(RunSearch::parse).transpose() {
            Ok(s) => s.flatten(),
            Err(e) => return Err(invalid_expression(&e.0)),
        };
        let mut page_query = query.clone();
        page_query.page_cursor = after.as_deref().and_then(decode_run_cursor);
        let meta = &*engine(ctx).meta;
        let (mut runs, resume) = match &search {
            Some(s) => {
                let page = s.list(meta, &page_query, PAGE_SCAN_LIMIT).await.map_err(err)?;
                (page.runs, page.resume)
            }
            None => (meta.list_runs(&page_query).await.map_err(err)?, None),
        };
        // A search that hit its scan limit returns what it found so far and continues from
        // where it stopped reading on the next page.
        let has_next = resume.is_some() || runs.len() as i64 > first;
        runs.truncate(first as usize);
        let edges: Vec<FunctionRunV2Edge> = runs
            .into_iter()
            .map(|r| FunctionRunV2Edge {
                cursor: encode_run_cursor(&r, time_field),
                node: FunctionRunV2(r),
            })
            .collect();
        let end_cursor = match resume {
            Some((t, id)) => Some(format!("{t}:{id}")),
            None => edges.last().map(|e| e.cursor.clone()),
        };
        Ok(RunsV2Connection {
            page_info: PageInfo {
                has_next_page: has_next,
                has_previous_page: after.is_some(),
                start_cursor: edges.first().map(|e| e.cursor.clone()),
                end_cursor,
            },
            edges,
            query,
            search,
        })
    }

    async fn run(&self, ctx: &Context<'_>, #[graphql(name = "runID")] run_id: String) -> Result<Option<FunctionRunV2>> {
        let id = parse_ulid(&run_id)?;
        Ok(engine(ctx).meta.get_run(id).await.map_err(err)?.map(FunctionRunV2))
    }

    #[graphql(name = "runTraceSpanOutputByID")]
    async fn run_trace_span_output_by_id(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "outputID")] output_id: String,
    ) -> Result<RunTraceSpanOutput> {
        trace::output(engine(ctx), &output_id).await.map_err(err)
    }

    async fn run_trigger(&self, ctx: &Context<'_>, #[graphql(name = "runID")] run_id: String) -> Result<RunTraceTrigger> {
        let id = parse_ulid(&run_id)?;
        trace::trigger(engine(ctx), id).await.map_err(err)
    }

    async fn run_trace(&self, ctx: &Context<'_>, #[graphql(name = "runID")] run_id: String) -> Result<RunTraceSpan> {
        let id = parse_ulid(&run_id)?;
        let e = engine(ctx);
        let run = e
            .meta
            .get_run(id)
            .await
            .map_err(err)?
            .ok_or_else(|| async_graphql::Error::new("run not found"))?;
        trace::build(e, &run).await.map_err(err)
    }

    async fn insights(&self, _sql: String) -> InsightsQueryResult {
        let pos = InsightsDiagnosticPosition { line: 1, column: 1 };
        InsightsQueryResult {
            columns: vec![],
            rows: vec![],
            info: InsightsQueryInfo {
                primary_table: None,
                tables: vec![],
                limited: false,
            },
            diagnostics: vec![InsightsDiagnostic {
                start: pos.clone(),
                end: pos,
                severity: InsightsDiagnosticSeverity::Error,
                code: "unsupported".into(),
                message: "Insights queries are not supported by this server".into(),
            }],
        }
    }

    async fn session_keys(&self, _search: Option<String>) -> Vec<SessionKey> {
        vec![]
    }

    async fn sessions(
        &self,
        _session_key: String,
        _session_id_search: Option<String>,
        _time_range: Option<TimeRangeInput>,
    ) -> Vec<SessionGroup> {
        vec![]
    }

    async fn session_runs(
        &self,
        _session_key: String,
        _session_id: String,
        _time_range: Option<TimeRangeInput>,
    ) -> Vec<SessionRun> {
        vec![]
    }

    async fn worker_connections(
        &self,
        #[graphql(default = 100)] _first: i32,
        _after: Option<String>,
        _order_by: Vec<ConnectionsOrderBy>,
        _filter: ConnectionsFilter,
    ) -> WorkerConnectionsConnection {
        WorkerConnectionsConnection {
            edges: vec![],
            page_info: PageInfo::default(),
            total_count: 0,
        }
    }

    async fn worker_connection(&self, #[graphql(name = "connectionId")] _connection_id: Ulid) -> Option<WorkerConnection> {
        None
    }
}

fn encode_run_cursor(r: &t::RunRecord, field: RunTimeField) -> String {
    let t = match field {
        RunTimeField::Queued => Some(r.started_at),
        RunTimeField::Started => r.exec_started_at,
        RunTimeField::Ended => r.ended_at,
    };
    format!("{}:{}", t.unwrap_or(0), r.run_id)
}

fn decode_run_cursor(c: &str) -> Option<(i64, ulid::Ulid)> {
    let (t, id) = c.split_once(':')?;
    Some((t.parse().ok()?, ulid::Ulid::from_string(id).ok()?))
}

// ---------------------------------------------------------------------------
// Mutation
// ---------------------------------------------------------------------------

pub struct Mutation;

async fn app_by_url(e: &Engine, url: &str) -> Result<Option<t::App>> {
    let norm = crate::util::normalize_url(url);
    let apps = e.meta.list_apps().await.map_err(err)?;
    Ok(apps.into_iter().find(|a| a.url == norm))
}

#[Object]
impl Mutation {
    async fn create_app(&self, ctx: &Context<'_>, input: CreateAppInput) -> Result<App> {
        let e = engine(ctx);
        let url = input.url.trim().to_string();
        let result = e.sync_app_url(&url).await;
        if let Some(app) = app_by_url(e, &url).await? {
            return Ok(App(app));
        }
        // Not reachable yet: record it so the UI can show the error and we keep retrying.
        let norm = crate::util::normalize_url(&url);
        let app = t::App {
            id: crate::util::uuid_v5(&norm),
            name: norm.clone(),
            url: norm,
            sdk_language: String::new(),
            sdk_version: String::new(),
            framework: String::new(),
            checksum: String::new(),
            error: Some(result.err().unwrap_or_else(|| "unreachable".into())),
            method: "serve".into(),
            app_version: None,
            created_at: t::now_ms(),
        };
        e.meta.upsert_app(&app).await.map_err(err)?;
        Ok(App(app))
    }

    async fn update_app(&self, ctx: &Context<'_>, input: UpdateAppInput) -> Result<App> {
        let e = engine(ctx);
        let id = uuid::Uuid::parse_str(&input.id).map_err(|_| async_graphql::Error::new("invalid app id"))?;
        let mut app = e
            .meta
            .get_app(id)
            .await
            .map_err(err)?
            .ok_or_else(|| async_graphql::Error::new("app not found"))?;
        app.url = crate::util::normalize_url(&input.url);
        e.meta.upsert_app(&app).await.map_err(err)?;
        let _ = e.sync_app_url(&app.url).await;
        let app = e.meta.get_app(id).await.map_err(err)?.unwrap_or(app);
        Ok(App(app))
    }

    async fn delete_app(&self, ctx: &Context<'_>, id: String) -> Result<String> {
        let e = engine(ctx);
        let uid = uuid::Uuid::parse_str(&id).map_err(|_| async_graphql::Error::new("invalid app id"))?;
        e.meta.delete_app(uid).await.map_err(err)?;
        e.registry.reload().await.map_err(err)?;
        Ok(id)
    }

    async fn delete_app_by_name(&self, ctx: &Context<'_>, name: String) -> Result<bool> {
        let e = engine(ctx);
        let Some(app) = e.meta.get_app_by_name(&name).await.map_err(err)? else {
            return Ok(false);
        };
        e.meta.delete_app(app.id).await.map_err(err)?;
        e.registry.reload().await.map_err(err)?;
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    async fn invoke_function(
        &self,
        ctx: &Context<'_>,
        data: Option<Map>,
        function_slug: String,
        _meta: Option<Map>,
        user: Option<Map>,
        #[graphql(name = "debugSessionID")] _debug_session_id: Option<Ulid>,
        #[graphql(name = "debugRunID")] _debug_run_id: Option<Ulid>,
    ) -> Result<Option<bool>> {
        let ev = t::Event {
            id: String::new(),
            name: t::EVENT_FN_INVOKED.into(),
            data: data
                .and_then(|d| d.0.as_object().cloned())
                .unwrap_or_default(),
            user: user.map(|u| u.0),
            ts: 0,
            v: None,
            meta: None,
        };
        let id = engine(ctx).invoke_function(&function_slug, ev).await.map_err(err)?;
        if id.is_none() {
            return Err(async_graphql::Error::new(format!("function not found: {function_slug}")));
        }
        Ok(Some(true))
    }

    #[graphql(name = "cancelRun")]
    async fn cancel_run(&self, ctx: &Context<'_>, #[graphql(name = "runID")] run_id: Ulid) -> Result<FunctionRun> {
        let e = engine(ctx);
        let id = parse_ulid(&run_id.0)?;
        e.cancel_run(id).await.map_err(err)?;
        let run = e
            .meta
            .get_run(id)
            .await
            .map_err(err)?
            .ok_or_else(|| async_graphql::Error::new("run not found"))?;
        Ok(FunctionRun(run))
    }

    async fn rerun(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "runID")] run_id: Ulid,
        from_step: Option<RerunFromStepInput>,
        #[graphql(name = "debugSessionID")] _debug_session_id: Option<Ulid>,
        #[graphql(name = "debugRunID")] _debug_run_id: Option<Ulid>,
    ) -> Result<Ulid> {
        let id = parse_ulid(&run_id.0)?;
        let new_id = engine(ctx)
            .rerun(id, from_step.as_ref().map(|s| s.step_id.as_str()))
            .await
            .map_err(err)?
            .ok_or_else(|| async_graphql::Error::new("run could not be rescheduled"))?;
        Ok(Ulid(new_id.to_string()))
    }
}
