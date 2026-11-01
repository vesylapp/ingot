//! `POST /fn/register` and `DELETE /fn/remove`.

use super::{AppState, json_response, public_error};
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use serde::Deserialize;
use serde_json::{Value, json};

pub async fn register(State(engine): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let expected = headers
        .get("x-inngest-expected-server-kind")
        .and_then(|v| v.to_str().ok());
    let req: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({ "error": format!("Invalid request: {e}") }),
            );
        }
    };
    let res = engine.register_app(req, expected).await;
    json_response(
        StatusCode::from_u16(res.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        &res.body,
    )
}

#[derive(Deserialize)]
pub struct RemoveQuery {
    url: Option<String>,
}

pub async fn remove(State(engine): State<AppState>, Query(q): Query<RemoveQuery>) -> Response {
    let Some(url) = q.url.filter(|u| !u.is_empty()) else {
        return public_error(StatusCode::BAD_REQUEST, "url is required");
    };
    let url = crate::util::normalize_url(&url);
    let apps = match engine.meta.list_apps().await {
        Ok(a) => a,
        Err(e) => return super::internal(e),
    };
    let mut removed = 0;
    for app in apps.into_iter().filter(|a| a.url == url) {
        if let Err(e) = engine.meta.delete_app(app.id).await {
            return super::internal(e);
        }
        removed += 1;
    }
    if let Err(e) = engine.registry.reload().await {
        return super::internal(e);
    }
    if removed == 0 {
        return public_error(StatusCode::NOT_FOUND, "app not found");
    }
    json_response(StatusCode::OK, &json!({ "ok": true }))
}
