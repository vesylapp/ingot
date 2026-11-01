//! Public REST API (`/v1`), checkpointing and signals.

use super::{AppState, json_response, public_error};
use crate::store::{EventQuery, RunQuery};
use crate::types::*;
use crate::util::{rfc3339_ms, rfc3339_nanos};
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use serde::Deserialize;
use serde_json::{Value, json};
use ulid::Ulid;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/events", get(list_events))
        .route("/v1/events/{id}", get(get_event))
        .route("/v1/events/{id}/runs", get(event_runs))
        .route("/v1/runs/{id}", get(get_run).delete(cancel_run))
        .route("/v1/runs/{id}/jobs", get(run_jobs))
        .route("/v1/runs/{id}/metadata", post(ok_empty))
        .route("/v1/apps/{app}/functions", get(app_functions))
        .route("/v1/signals", post(signal))
        .route("/v1/cancellations", post(create_cancellation).get(list_cancellations))
        .route("/v1/cancellations/{id}", delete(delete_cancellation))
        .route("/v1/traces/userland", post(ok_empty))
        .route("/v1/checkpoint/{run_id}/async", post(checkpoint_async))
        .route("/v1/checkpoint/{run_id}/steps", post(checkpoint_steps))
        .route("/v1/http/runs/{run_id}/async", post(checkpoint_async))
        .route("/v1/http/runs/{run_id}/steps", post(checkpoint_steps))
        .route("/v1/prom/{env}", get(not_implemented))
}

fn envelope(data: Value) -> Response {
    json_response(
        StatusCode::OK,
        &json!({ "data": data, "metadata": { "fetched_at": rfc3339_nanos(now_ms()), "cached_until": null } }),
    )
}

async fn ok_empty() -> StatusCode {
    StatusCode::OK
}

async fn not_implemented() -> Response {
    public_error(StatusCode::NOT_IMPLEMENTED, "not implemented")
}

pub fn event_json(e: &TrackedEvent) -> Value {
    json!({
        "internal_id": e.internal_id.to_string(),
        "account_id": ACCOUNT_ID,
        "environment_id": ENV_ID,
        "source": "",
        "source_id": null,
        "received_at": rfc3339_ms(e.received_at),
        "id": e.event.id,
        "name": e.event.name,
        "data": e.event.data,
        "user": e.event.user.clone().unwrap_or_else(|| json!({})),
        "ts": e.event.ts,
        "v": e.event.v,
    })
}

pub fn run_json(r: &RunRecord) -> Value {
    let mut v = json!({
        "run_id": r.run_id.to_string(),
        "run_started_at": rfc3339_ms(r.started_at),
        "function_id": r.fn_id,
        "function_version": r.fn_version,
        "environment_id": ENV_ID,
        "event_id": r.event_id.map(|e| e.to_string()),
        "status": r.status.as_str(),
        "ended_at": r.ended_at.map(rfc3339_ms),
    });
    let m = v.as_object_mut().unwrap();
    if let Some(b) = r.batch_id {
        m.insert("batch_id".into(), Value::String(b.to_string()));
    }
    if let Some(o) = r.original_run_id {
        m.insert("original_run_id".into(), Value::String(o.to_string()));
    }
    if let Some(c) = &r.cron {
        m.insert("cron".into(), Value::String(c.clone()));
    }
    if r.status.is_terminal()
        && let Some(o) = &r.output
    {
        m.insert("output".into(), o.clone());
    }
    v
}

#[allow(clippy::result_large_err)]
fn parse_ulid(id: &str) -> Result<Ulid, Response> {
    Ulid::from_string(id).map_err(|_| public_error(StatusCode::BAD_REQUEST, &format!("invalid ID: {id}")))
}

fn parse_time(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.timestamp_millis())
}

#[derive(Deserialize)]
struct EventsQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    received_after: Option<String>,
    received_before: Option<String>,
    name: Option<String>,
}

async fn list_events(State(engine): State<AppState>, Query(q): Query<EventsQuery>) -> Response {
    let now = now_ms();
    let cursor = match q.cursor.as_deref().filter(|c| !c.is_empty()) {
        Some(c) => match parse_ulid(c) {
            Ok(u) => Some(u),
            Err(r) => return r,
        },
        None => None,
    };
    let after = match q.received_after.as_deref() {
        Some(s) => match parse_time(s) {
            Some(t) => t,
            None => return public_error(StatusCode::BAD_REQUEST, "invalid received_after"),
        },
        None => now - 3_600_000,
    };
    let before = match q.received_before.as_deref() {
        Some(s) => match parse_time(s) {
            Some(t) => t,
            None => return public_error(StatusCode::BAD_REQUEST, "invalid received_before"),
        },
        None => now,
    };
    let query = EventQuery {
        name: q.name.filter(|n| !n.is_empty()),
        cursor,
        received_after: Some(after),
        received_before: Some(before),
        limit: q.limit.unwrap_or(20).clamp(1, 51),
        include_internal: false,
    };
    match engine.meta.list_events(&query).await {
        Ok(evts) => envelope(Value::Array(evts.iter().map(event_json).collect())),
        Err(e) => super::internal(e),
    }
}

async fn get_event(State(engine): State<AppState>, Path(id): Path<String>) -> Response {
    let id = match parse_ulid(&id) {
        Ok(i) => i,
        Err(r) => return r,
    };
    match engine.meta.get_event(id).await {
        Ok(Some(e)) => envelope(event_json(&e)),
        Ok(None) => public_error(StatusCode::NOT_FOUND, "event not found"),
        Err(e) => super::internal(e),
    }
}

async fn event_runs(State(engine): State<AppState>, Path(id): Path<String>) -> Response {
    let id = match parse_ulid(&id) {
        Ok(i) => i,
        Err(r) => return r,
    };
    match engine.meta.runs_for_event(id).await {
        Ok(runs) => envelope(Value::Array(runs.iter().map(run_json).collect())),
        Err(e) => super::internal(e),
    }
}

async fn get_run(State(engine): State<AppState>, Path(id): Path<String>) -> Response {
    let id = match parse_ulid(&id) {
        Ok(i) => i,
        Err(r) => return r,
    };
    match engine.meta.get_run(id).await {
        Ok(Some(r)) => envelope(run_json(&r)),
        Ok(None) => public_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Unable to load function run: {id}"),
        ),
        Err(e) => super::internal(e),
    }
}

async fn cancel_run(State(engine): State<AppState>, Path(id): Path<String>) -> Response {
    let id = match parse_ulid(&id) {
        Ok(i) => i,
        Err(r) => return r,
    };
    match engine.cancel_run(id).await {
        Ok(true) => StatusCode::OK.into_response(),
        Ok(false) => public_error(StatusCode::NOT_FOUND, "run not found"),
        Err(e) => super::internal(e),
    }
}

async fn run_jobs(State(engine): State<AppState>, Path(id): Path<String>) -> Response {
    let id = match parse_ulid(&id) {
        Ok(i) => i,
        Err(r) => return r,
    };
    match engine.state.run_items(id, 10).await {
        Ok(items) => envelope(Value::Array(
            items
                .iter()
                .enumerate()
                .map(|(i, it)| {
                    json!({
                        "JobID": it.id,
                        "at": rfc3339_ms(it.at),
                        "position": i,
                        "kind": it.kind.as_str(),
                        "attempt": it.attempt,
                        "Raw": it,
                    })
                })
                .collect(),
        )),
        Err(e) => super::internal(e),
    }
}

async fn app_functions(State(engine): State<AppState>, Path(app): Path<String>) -> Response {
    let app = match engine.meta.get_app_by_name(&app).await {
        Ok(Some(a)) => a,
        Ok(None) => return json_response(StatusCode::OK, &json!([])),
        Err(e) => return super::internal(e),
    };
    match engine.meta.app_functions(app.id).await {
        Ok(fns) => json_response(
            StatusCode::OK,
            &Value::Array(
                fns.iter()
                    .map(|f| {
                        json!({
                            "internal_id": f.id,
                            "env_id": ENV_ID,
                            "app_id": f.app_id,
                            "id": f.slug,
                            "name": f.name,
                            "config": f.config,
                            "created_at": rfc3339_ms(app.created_at),
                            "archived_at": "0001-01-01T00:00:00Z",
                        })
                    })
                    .collect(),
            ),
        ),
        Err(e) => super::internal(e),
    }
}

#[derive(Deserialize)]
struct SignalBody {
    signal: String,
    #[serde(default)]
    data: Value,
}

async fn signal(State(engine): State<AppState>, body: Bytes) -> Response {
    let req: SignalBody = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return public_error(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    if req.signal.is_empty() {
        return public_error(StatusCode::BAD_REQUEST, "signal is required");
    }
    match engine.send_signal(&req.signal, req.data).await {
        Ok(Some(run_id)) => envelope(json!({ "run_id": run_id.to_string() })),
        Ok(None) => public_error(StatusCode::NOT_FOUND, "no run is waiting for this signal"),
        Err(e) => super::internal(e),
    }
}

#[derive(Deserialize)]
struct CancellationBody {
    app_id: String,
    function_id: String,
    started_after: Option<String>,
    started_before: String,
    #[serde(rename = "if")]
    if_expr: Option<String>,
}

/// Bulk-cancel running runs of a function started within a time window.
async fn create_cancellation(State(engine): State<AppState>, body: Bytes) -> Response {
    let req: CancellationBody = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return public_error(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let now = now_ms();
    let Some(before) = parse_time(&req.started_before) else {
        return public_error(StatusCode::BAD_REQUEST, "invalid started_before");
    };
    if before > now + 5000 {
        return public_error(StatusCode::BAD_REQUEST, "started_before must not be in the future");
    }
    let after = req.started_after.as_deref().and_then(parse_time);
    let expr = match req.if_expr.as_deref().filter(|e| !e.is_empty()) {
        Some(e) => match engine.compile(e) {
            Ok(x) => Some(x),
            Err(err) => return public_error(StatusCode::BAD_REQUEST, &format!("invalid expression: {err}")),
        },
        None => None,
    };
    let f = engine
        .registry
        .by_slug(&req.function_id)
        .or_else(|| engine.registry.by_slug(&format!("{}-{}", req.app_id, req.function_id)));
    let Some(f) = f else {
        return public_error(StatusCode::NOT_FOUND, "function not found");
    };
    let id = Ulid::generate();
    let engine2 = engine.clone();
    let fn_id = f.id;
    tokio::spawn(async move {
        let mut cursor = None;
        loop {
            let runs = match engine2
                .meta
                .list_runs(&RunQuery {
                    fn_id: Some(fn_id),
                    status: Some(RunStatus::Running),
                    cursor,
                    limit: 500,
                })
                .await
            {
                Ok(r) => r,
                Err(err) => {
                    tracing::error!(error = %err, "bulk cancellation failed");
                    return;
                }
            };
            if runs.is_empty() {
                break;
            }
            cursor = runs.last().map(|r| r.run_id);
            for r in runs {
                let ts = r.run_id.timestamp_ms() as i64;
                if ts > before || after.is_some_and(|a| ts < a) {
                    continue;
                }
                if let Some(x) = &expr {
                    let events = engine2.state.load_events(r.run_id).await.unwrap_or_default();
                    let ok = x
                        .eval_bool(oxidle_expr::Vars {
                            event: events.first(),
                            async_: None,
                        })
                        .unwrap_or(false);
                    if !ok {
                        continue;
                    }
                }
                if let Err(err) = engine2.cancel_run(r.run_id).await {
                    tracing::warn!(run_id = %r.run_id, error = %err, "bulk cancel failed");
                }
            }
        }
    });
    envelope(json!({
        "created_at": rfc3339_ms(now),
        "name": null,
        "id": id.to_string(),
        "account_id": ACCOUNT_ID,
        "environment_id": ENV_ID,
        "app_id": f.app_id,
        "function_internal_id": f.id,
        "function_id": f.slug,
        "started_after": after.map(rfc3339_ms),
        "started_before": rfc3339_ms(before),
        "if": req.if_expr,
        "kind": "bulk_run",
        "cause": "API",
        "queue_name": null,
    }))
}

async fn list_cancellations() -> Response {
    envelope(json!([]))
}

async fn delete_cancellation() -> Response {
    public_error(StatusCode::NOT_FOUND, "cancellation not found")
}

#[derive(Deserialize)]
struct CheckpointBody {
    #[serde(default)]
    steps: Vec<Value>,
}

async fn save_checkpoint_steps(engine: &AppState, run_id: Ulid, steps: &[Value]) -> Result<(), Response> {
    for s in steps {
        let op = s.get("op").and_then(|o| o.as_str()).unwrap_or_default();
        let id = s.get("id").and_then(|i| i.as_str()).unwrap_or_default();
        match op {
            "StepRun" | "Step" => {
                let data = s.get("data").cloned().unwrap_or(Value::Null);
                let v = if op == "Step" { data } else { json!({ "data": data }) };
                if let Err(e) = engine.state.save_step(run_id, id, &v).await {
                    return Err(super::internal(e));
                }
            }
            "StepPlanned" | "DeferAdd" | "DeferAbort" => {}
            other => {
                return Err(public_error(
                    StatusCode::BAD_REQUEST,
                    &format!("unsupported checkpoint opcode: {other}"),
                ));
            }
        }
    }
    Ok(())
}

async fn checkpoint_async(
    State(engine): State<AppState>,
    Path(run_id): Path<String>,
    body: Bytes,
) -> Response {
    let run_id = match parse_ulid(&run_id) {
        Ok(i) => i,
        Err(r) => return r,
    };
    let req: CheckpointBody = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return public_error(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    match engine.state.load_meta(run_id).await {
        Ok(Some(m)) if !m.status.is_terminal() => {}
        Ok(_) => return public_error(StatusCode::CONFLICT, "run is no longer running"),
        Err(e) => return super::internal(e),
    }
    match save_checkpoint_steps(&engine, run_id, &req.steps).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(r) => r,
    }
}

async fn checkpoint_steps(
    State(engine): State<AppState>,
    Path(run_id): Path<String>,
    body: Bytes,
) -> Response {
    let run_id = match parse_ulid(&run_id) {
        Ok(i) => i,
        Err(r) => return r,
    };
    if let Ok(req) = serde_json::from_slice::<CheckpointBody>(&body) {
        let _ = save_checkpoint_steps(&engine, run_id, &req.steps).await;
    }
    StatusCode::OK.into_response()
}
