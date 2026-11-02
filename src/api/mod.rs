//! HTTP API: event ingestion, registration, the v0/v1 REST APIs.

mod events;
pub mod gql;
mod register;
mod v0;
mod v1;

use crate::engine::Engine;
use axum::Router;
use axum::extract::{DefaultBodyLimit, MatchedPath, Request, State};
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
        .route("/metrics", get(metrics))
        .route("/ready", get(ready))
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
        .layer(middleware::from_fn(track_http))
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
    let features = json!({ "FEATURE_EVENTS": true, "FEATURE_CEL_SEARCH": true });
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

/// Prometheus metrics. Unauthenticated, as in the reference server.
async fn metrics(State(engine): State<AppState>) -> Response {
    match crate::metrics::get().render(&engine).await {
        Ok(body) => (
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(err) => {
            tracing::error!(error = %err, "failed to render metrics");
            (StatusCode::INTERNAL_SERVER_ERROR, "failed to gather metrics").into_response()
        }
    }
}

/// Record `ingot_http_request_duration_seconds`, labelled by route pattern (not raw path)
/// so cardinality stays bounded.
async fn track_http(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unmatched".to_string(), |p| p.as_str().to_string());
    let start = std::time::Instant::now();
    let resp = next.run(req).await;
    crate::metrics::get().http_request(
        method.as_str(),
        &route,
        resp.status().as_u16(),
        start.elapsed().as_secs_f64(),
    );
    resp
}

/// Readiness: 503 while shutting down or when either store fails a ping. `/health` stays a
/// plain liveness check, as in the reference server.
async fn ready(State(engine): State<AppState>) -> Response {
    if engine.is_shutting_down() {
        return json_status(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "status": 503, "message": "shutting down" }),
        );
    }
    let timeout = std::time::Duration::from_secs(2);
    let (database, state) = tokio::join!(
        tokio::time::timeout(timeout, engine.meta.ping()),
        tokio::time::timeout(timeout, engine.state.ping()),
    );
    let check = |r: Result<anyhow::Result<()>, tokio::time::error::Elapsed>| match r {
        Ok(Ok(())) => None,
        Ok(Err(err)) => Some(err.to_string()),
        Err(_) => Some("timed out".to_string()),
    };
    let (database, state) = (check(database), check(state));
    let checks = json!({
        "database": database.as_deref().unwrap_or("ok"),
        "state": state.as_deref().unwrap_or("ok"),
    });
    if database.is_some() || state.is_some() {
        tracing::warn!(%checks, "readiness check failed");
        return json_status(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "status": 503, "message": "store unavailable", "checks": checks }),
        );
    }
    json_status(StatusCode::OK, json!({ "status": 200, "message": "OK", "checks": checks }))
}

fn json_status(status: StatusCode, body: Value) -> Response {
    (status, axum::Json(body)).into_response()
}
