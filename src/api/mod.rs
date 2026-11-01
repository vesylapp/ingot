//! HTTP API: event ingestion, registration, the v0/v1 REST APIs.

mod events;
pub mod gql;
mod register;
mod v0;
mod v1;

use crate::engine::Engine;
use axum::Router;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use serde_json::{Value, json};
use std::sync::Arc;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};

pub type AppState = Arc<Engine>;

/// Max request body (event batches can be large).
const BODY_LIMIT: usize = 256 * 1024 * 1024;

pub fn router(engine: Arc<Engine>) -> Router {
    let authed = Router::new()
        .route("/fn/register", post(register::register))
        .route("/fn/remove", delete(register::remove))
        .route("/v0/runs/{run_id}", delete(v0::cancel_run))
        .route("/v0/runs/{run_id}/actions", get(v0::actions))
        .route("/v0/runs/{run_id}/batch", get(v0::batch))
        .merge(v1::router())
        .route_layer(middleware::from_fn_with_state(engine.clone(), signing_key_auth));

    let kind = HeaderValue::from_static(engine.cfg.server_kind());
    let mut router = Router::new();
    if !engine.cfg.no_ui {
        let schema = gql::schema(engine.clone());
        router = router.route_service("/v0/gql", post(gql::handler).with_state(schema));
    }
    let router = router
        .route("/health", get(health))
        .route("/e/{key}", post(events::ingest))
        .route("/e/", post(events::missing_key))
        .route("/e", post(events::missing_key))
        .route("/invoke/{slug}", post(events::invoke))
        .route("/dev", get(dev_info))
        .route("/v0/telemetry", post(ok_empty))
        .merge(authed);
    let router = match ui_dir(&engine) {
        Some(dir) => {
            tracing::info!(dir = %dir.display(), "serving dashboard UI");
            let shell = dir.join("_shell.html");
            router.fallback_service(
                tower_http::services::ServeDir::new(&dir)
                    .fallback(tower_http::services::ServeFile::new(shell)),
            )
        }
        None => router.fallback(not_found),
    };
    router
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .layer(
            CorsLayer::new()
                .allow_origin(AllowOrigin::mirror_request())
                .allow_methods(AllowMethods::list([
                    Method::GET,
                    Method::POST,
                    Method::PUT,
                    Method::DELETE,
                    Method::OPTIONS,
                ]))
                .allow_headers(AllowHeaders::mirror_request())
                .expose_headers([header::LINK])
                .max_age(std::time::Duration::from_secs(3600)),
        )
        .layer(middleware::from_fn(move |req: Request, next: Next| {
            let kind = kind.clone();
            async move {
                let mut resp = next.run(req).await;
                resp.headers_mut().insert("X-Inngest-Server-Kind", kind);
                resp
            }
        }))
        .with_state(engine)
}

/// Directory holding the built dashboard UI, if enabled and present.
fn ui_dir(engine: &Engine) -> Option<std::path::PathBuf> {
    if engine.cfg.no_ui {
        return None;
    }
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    match &engine.cfg.ui_dir {
        Some(d) => candidates.push(d.into()),
        None => {
            candidates.push("ui/dist".into());
            if let Ok(exe) = std::env::current_exe()
                && let Some(parent) = exe.parent()
            {
                candidates.push(parent.join("ui"));
            }
        }
    }
    let found = candidates.into_iter().find(|d| d.join("_shell.html").is_file());
    if found.is_none() {
        tracing::warn!("dashboard UI assets not found (build them with ./ui/build.sh or set --ui-dir)");
    }
    found
}

async fn signing_key_auth(State(engine): State<AppState>, req: Request, next: Next) -> Response {
    if let Some(key) = &engine.signing {
        let h = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        if !key.check_bearer(h) {
            return (
                StatusCode::UNAUTHORIZED,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                "Authentication failed\n",
            )
                .into_response();
        }
    }
    next.run(req).await
}

async fn health() -> Response {
    json_response(StatusCode::OK, &json!({ "status": 200, "message": "OK" }))
}

async fn ok_empty() -> StatusCode {
    StatusCode::OK
}

async fn not_found() -> Response {
    public_error(StatusCode::NOT_FOUND, "Not found")
}

async fn dev_info(State(engine): State<AppState>) -> Response {
    let features = json!({ "FEATURE_EVENTS": true, "FEATURE_CEL_SEARCH": false });
    if !engine.cfg.dev {
        // Self-hosted: only what the dashboard needs (no function configs or options).
        return json_response(
            StatusCode::OK,
            &json!({
                "version": env!("CARGO_PKG_VERSION"),
                "isSingleNodeService": false,
                "features": features,
            }),
        );
    }
    let fns: Vec<Value> = engine
        .registry
        .all()
        .iter()
        .map(|f| f.config.clone())
        .collect();
    json_response(
        StatusCode::OK,
        &json!({
            "version": env!("CARGO_PKG_VERSION"),
            "isSingleNodeService": true,
            "isMissingSigningKey": engine.signing.is_none(),
            "isMissingEventKey": engine.cfg.event_keys.is_empty(),
            "authed": false,
            "startOpts": { "dev": true, "sdkURLs": engine.cfg.sdk_urls },
            "functions": fns,
            "handlers": [],
            "features": features,
        }),
    )
}

/// JSON response with a trailing newline (as Go's json.Encoder writes).
pub fn json_response(status: StatusCode, body: &Value) -> Response {
    let mut s = serde_json::to_string(body).unwrap_or_else(|_| "{}".into());
    s.push('\n');
    (status, [(header::CONTENT_TYPE, "application/json")], s).into_response()
}

/// Inngest "publicerr" error body.
pub fn public_error(status: StatusCode, msg: &str) -> Response {
    json_response(
        status,
        &json!({ "error": msg, "data": null, "status": status.as_u16() }),
    )
}

/// Map internal errors to a 500.
pub fn internal(err: anyhow::Error) -> Response {
    tracing::error!(error = %err, "internal error");
    public_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string())
}
