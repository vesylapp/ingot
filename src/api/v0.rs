//! `/v0/runs/*` endpoints used by SDKs for large state and cancellation.

use super::{AppState, json_response};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Map, Value, json};
use ulid::Ulid;

fn err(status: StatusCode, msg: String) -> Response {
    json_response(status, &json!({ "error": msg, "status": status.as_u16() }))
}

fn parse_run(id: &str) -> Result<Ulid, Response> {
    Ulid::from_string(id).map_err(|_| err(StatusCode::BAD_REQUEST, format!("invalid run ID: {id}")))
}

pub async fn actions(State(engine): State<AppState>, Path(run_id): Path<String>) -> Response {
    let run_id = match parse_run(&run_id) {
        Ok(r) => r,
        Err(r) => return r,
    };
    match engine.state.load_meta(run_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return err(
                StatusCode::GONE,
                format!("runtime state is no longer available for runID: {run_id}"),
            );
        }
        Err(e) => return super::internal(e),
    }
    match engine.state.load_steps(run_id).await {
        Ok(steps) => {
            let map: Map<String, Value> = steps.into_iter().collect();
            json_response(StatusCode::OK, &Value::Object(map))
        }
        Err(e) => super::internal(e),
    }
}

pub async fn batch(State(engine): State<AppState>, Path(run_id): Path<String>) -> Response {
    let run_id = match parse_run(&run_id) {
        Ok(r) => r,
        Err(r) => return r,
    };
    match engine.state.load_meta(run_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return err(
                StatusCode::GONE,
                format!("runtime state is no longer available for runID: {run_id}"),
            );
        }
        Err(e) => return super::internal(e),
    }
    match engine.state.load_events(run_id).await {
        Ok(events) => json_response(StatusCode::OK, &Value::Array(events)),
        Err(e) => super::internal(e),
    }
}

pub async fn cancel_run(State(engine): State<AppState>, Path(run_id): Path<String>) -> Response {
    let run_id = match parse_run(&run_id) {
        Ok(r) => r,
        Err(r) => return r,
    };
    match engine.cancel_run(run_id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => err(StatusCode::NOT_FOUND, format!("run not found: {run_id}")),
        Err(e) => super::internal(e),
    }
}
