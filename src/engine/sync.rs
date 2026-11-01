//! App registration (`POST /fn/register`) and server-initiated app sync (PUT ping).

use super::Engine;
use crate::types::*;
use crate::util::{normalize_url, sha256_hex, uuid_v5};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub fn spawn(engine: Arc<Engine>) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(engine.cfg.poll_interval_s);
        // Initial sync of configured URLs, retried until each succeeds once.
        let mut pending: Vec<String> = engine.cfg.sdk_urls.clone();
        loop {
            let mut still = Vec::new();
            for url in pending.drain(..) {
                match engine.sync_app_url(&url).await {
                    Ok(()) => tracing::info!(url = %url, "app synced"),
                    Err(err) => {
                        tracing::warn!(url = %url, error = %err, "app sync failed; will retry");
                        still.push(url);
                    }
                }
            }
            pending = still;
            // Re-sync apps that previously failed.
            if let Ok(apps) = engine.meta.list_apps().await {
                for app in apps.into_iter().filter(|a| a.error.is_some() && a.method == "serve") {
                    if let Err(err) = engine.sync_app_url(&app.url).await {
                        tracing::debug!(app = %app.name, error = %err, "app re-sync failed");
                    }
                }
            }
            tokio::time::sleep(interval).await;
        }
    });
}

#[derive(Debug)]
pub struct RegisterResponse {
    pub status: u16,
    pub body: Value,
}

fn reg_err(status: u16, msg: &str) -> RegisterResponse {
    RegisterResponse {
        status,
        body: json!({ "error": msg, "data": null, "status": status }),
    }
}

impl Engine {
    /// PUT the app URL so the SDK registers itself with us.
    pub async fn sync_app_url(&self, url: &str) -> Result<(), String> {
        let body = crate::util::canonical_json(&json!({ "url": url }));
        let mut rb = self
            .http
            .put(url)
            .timeout(Duration::from_secs(10))
            .header("Content-Type", "application/json")
            .header("X-Inngest-Server-Kind", self.cfg.server_kind());
        if let Some(k) = &self.signing {
            rb = rb.header("X-Inngest-Signature", k.sign(&body, chrono::Utc::now().timestamp()));
        }
        let result = match rb.body(body).send().await {
            Err(err) => Err(if err.is_connect() || err.is_timeout() {
                "unreachable".to_string()
            } else {
                format!("unreachable: {err}")
            }),
            Ok(resp) => {
                let status = resp.status().as_u16();
                let text = resp.text().await.unwrap_or_default();
                match status {
                    200..=299 => Ok(()),
                    400 => {
                        let msg = serde_json::from_str::<Value>(&text)
                            .ok()
                            .and_then(|v| v.get("message").and_then(|m| m.as_str()).map(String::from))
                            .unwrap_or(text);
                        Err(map_deploy_error(&msg))
                    }
                    401 => Err("unauthorized".into()),
                    403 => Err("forbidden".into()),
                    404 => Err("url_not_found".into()),
                    500 => {
                        let msg = serde_json::from_str::<Value>(&text)
                            .ok()
                            .and_then(|v| v.get("message").and_then(|m| m.as_str()).map(String::from));
                        Err(msg.unwrap_or_else(|| "internal_server_error".into()))
                    }
                    s => Err(format!("unexpected status code {s}")),
                }
            }
        };
        if let Err(e) = &result {
            let norm = normalize_url(url);
            if let Ok(apps) = self.meta.list_apps().await {
                for app in apps.into_iter().filter(|a| a.url == norm) {
                    let _ = self.meta.set_app_error(app.id, Some(e)).await;
                }
            }
        }
        result
    }

    /// Process an SDK registration request.
    pub async fn register_app(&self, mut req: Value, expected_kind: Option<&str>) -> RegisterResponse {
        if let Some(k) = expected_kind.filter(|k| !k.is_empty())
            && k != self.cfg.server_kind()
        {
            return RegisterResponse {
                status: 400,
                body: json!({ "error": format!("Expected server kind {k}, got {}", self.cfg.server_kind()) }),
            };
        }
        if !req.is_object() {
            return RegisterResponse {
                status: 400,
                body: json!({ "error": "Invalid request: body must be a JSON object" }),
            };
        }
        let app_name = req
            .get("appName")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if app_name.is_empty() {
            return reg_err(400, "App ID required");
        }
        let url = normalize_url(req.get("url").and_then(|v| v.as_str()).unwrap_or_default());
        if let Some(obj) = req.as_object_mut() {
            obj.remove("featureObservations");
            obj.insert("url".into(), Value::String(url.clone()));
        }
        let checksum = sha256_hex(&crate::util::canonical_json(&req));
        let app_id = uuid_v5(&app_name);
        let sync_id = Uuid::new_v4();

        let existing_app = match self.meta.get_app_by_name(&app_name).await {
            Ok(a) => a,
            Err(err) => return reg_err(500, &format!("error loading app: {err}")),
        };
        if let Some(a) = &existing_app
            && a.checksum == checksum
            && a.error.is_none()
        {
            return RegisterResponse {
                status: 200,
                body: json!({ "ok": true, "modified": false, "sync_id": sync_id, "app_id": a.id }),
            };
        }

        let existing_fns: Vec<Function> = match &existing_app {
            Some(a) => self.meta.app_functions(a.id).await.unwrap_or_default(),
            None => vec![],
        };
        let raw_fns = req
            .get("functions")
            .and_then(|f| f.as_array())
            .cloned()
            .unwrap_or_default();
        let mut fns = Vec::with_capacity(raw_fns.len());
        let mut errors = Vec::new();
        for raw in &raw_fns {
            let slug = raw.get("id").and_then(|v| v.as_str()).unwrap_or_default();
            let prev = existing_fns
                .iter()
                .find(|f| f.slug == slug)
                .map(|f| (f.id, f.version))
                .or_else(|| {
                    self.registry
                        .by_slug(slug)
                        .filter(|f| f.app_name == app_name)
                        .map(|f| (f.id, f.version))
                });
            match crate::function::parse_function(raw, app_id, &app_name, prev) {
                Ok(f) => {
                    // Unchanged configs keep their version.
                    let f = match existing_fns.iter().find(|e| e.id == f.id) {
                        Some(e) if e.config == f.config => Function {
                            version: e.version,
                            ..f
                        },
                        _ => f,
                    };
                    fns.push(f)
                }
                Err(e) => errors.push(format!("{slug}: {e}")),
            }
        }
        if !errors.is_empty() {
            tracing::warn!(app = %app_name, errors = ?errors, "invalid functions in sync");
            return RegisterResponse {
                status: 400,
                body: json!({
                    "error": format!("At least one function is invalid: {}", errors.join("; ")),
                    "data": null,
                    "status": 400,
                }),
            };
        }

        let sdk = req.get("sdk").and_then(|v| v.as_str()).unwrap_or_default();
        let (sdk_language, sdk_version) = match sdk.split_once(':') {
            Some((l, v)) => (l.to_string(), v.trim_start_matches('v').to_string()),
            None => (sdk.to_string(), String::new()),
        };
        let deploy_type = req.get("deployType").and_then(|v| v.as_str()).unwrap_or("ping");
        let app = App {
            id: existing_app.as_ref().map(|a| a.id).unwrap_or(app_id),
            name: app_name.clone(),
            url,
            sdk_language,
            sdk_version,
            framework: req
                .get("framework")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            checksum,
            error: None,
            method: if deploy_type == "connect" { "connect".into() } else { "serve".into() },
            app_version: req.get("appVersion").and_then(|v| v.as_str()).map(String::from),
            created_at: existing_app.as_ref().map(|a| a.created_at).unwrap_or_else(now_ms),
        };
        let fns: Vec<Function> = fns
            .into_iter()
            .map(|f| Function { app_id: app.id, ..f })
            .collect();
        if let Err(err) = self.meta.upsert_app(&app).await {
            return reg_err(500, &format!("error saving app: {err}"));
        }
        if let Err(err) = self.meta.sync_functions(app.id, &fns).await {
            return reg_err(500, &format!("error saving functions: {err}"));
        }
        if let Err(err) = self.registry.reload().await {
            tracing::error!(error = %err, "registry reload failed");
        }
        if let Err(err) = self.ensure_crons().await {
            tracing::error!(error = %err, "cron scheduling failed");
        }
        tracing::info!(app = %app.name, functions = fns.len(), "app synced");
        RegisterResponse {
            status: 200,
            body: json!({ "ok": true, "modified": true, "app_id": app.id, "sync_id": sync_id }),
        }
    }
}

fn map_deploy_error(msg: &str) -> String {
    if msg == "Your signing key is invalid" {
        "invalid_signing_key".into()
    } else if msg.contains("No functions registered") {
        "no_functions".into()
    } else if msg.contains("function is invalid") {
        "invalid_function".into()
    } else if msg.contains("You didn't specify your workspace's signing key") {
        "missing_signing_key".into()
    } else if msg.contains("No INNGEST_ENV branch name found") {
        "missing_branch_env_name".into()
    } else {
        msg.to_string()
    }
}
