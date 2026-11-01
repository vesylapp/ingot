//! HTTP driver: calls the SDK serve endpoint for a run and parses the response.

use super::Engine;
use crate::types::*;
use serde::Deserialize;
use serde_json::{Map, Value, json};

/// Requests above this size switch to the `use_api` path (SDK fetches state itself).
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
/// Responses above this size are rejected.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
pub struct Op {
    pub op: String,
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default, rename = "displayName")]
    pub display_name: Option<String>,
    #[serde(default)]
    pub opts: Option<Value>,
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default)]
    pub error: Option<Value>,
    /// SDK-measured execution interval: `a` = start (unix ns), `b` = duration (ns).
    #[serde(default)]
    pub timing: Option<Value>,
}

impl Op {
    /// SDK-measured (start, end) in unix ms, if reported.
    pub fn timing_ms(&self) -> Option<(i64, i64)> {
        let t = self.timing.as_ref()?;
        let a = t.get("a")?.as_f64()?;
        let b = t.get("b").and_then(|b| b.as_f64()).unwrap_or(0.0);
        let start = (a / 1e6) as i64;
        (start > 0).then(|| (start, start + (b / 1e6) as i64))
    }
}

impl Op {
    pub fn opt_str(&self, key: &str) -> Option<&str> {
        self.opts
            .as_ref()
            .and_then(|o| o.get(key))
            .and_then(|v| v.as_str())
    }
    pub fn is_empty_none(&self) -> bool {
        self.op == "None" && self.id.is_empty()
    }
}

#[derive(Debug)]
pub enum Response {
    /// 200: function finished with output.
    Complete(Value),
    /// 206: generator opcodes.
    Ops {
        ops: Vec<Op>,
        no_retry: bool,
        retry_at: Option<i64>,
    },
    /// Any error (transport, non-2xx, bad response).
    Error {
        error: Value,
        retryable: bool,
        retry_at: Option<i64>,
    },
}

pub struct DriverResult {
    pub response: Response,
    pub request_version: Option<i32>,
}

pub struct DriverRequest<'a> {
    pub function: &'a Function,
    pub state: &'a RunState,
    pub step_id: &'a str,
    pub attempt: u32,
    pub max_attempts: u32,
    pub queue_item_id: &'a str,
}

fn error_value(name: &str, message: &str) -> Value {
    json!({ "error": format!("{name}: {message}"), "name": name, "message": message })
}

impl Engine {
    pub fn build_sdk_body(&self, req: &DriverRequest<'_>, use_api: bool) -> Value {
        let meta = &req.state.meta;
        let stack: Vec<&str> = req.state.steps.iter().map(|(id, _)| id.as_str()).collect();
        let mut steps = Map::new();
        if !use_api {
            for (id, v) in &req.state.steps {
                steps.insert(id.clone(), v.clone());
            }
        }
        let first = req.state.events.first().cloned().unwrap_or_else(|| json!({}));
        json!({
            "event": first,
            "events": if use_api { json!([]) } else { Value::Array(req.state.events.clone()) },
            "steps": steps,
            "ctx": {
                "fn_id": req.function.id,
                "run_id": meta.run_id.to_string(),
                "env": "",
                "step_id": req.step_id,
                "attempt": req.attempt,
                "max_attempts": req.max_attempts,
                "stack": { "stack": stack, "current": stack.len() },
                "qi_id": req.queue_item_id,
                "disable_immediate_execution": meta.force_step_plan,
                "use_api": use_api,
            },
            "version": meta.request_version,
            "use_api": use_api,
        })
    }

    pub async fn call_sdk(&self, req: DriverRequest<'_>) -> DriverResult {
        let mut body = crate::util::canonical_json(&self.build_sdk_body(&req, false));
        if body.len() > MAX_REQUEST_BYTES {
            body = crate::util::canonical_json(&self.build_sdk_body(&req, true));
        }
        let url = crate::util::with_step_id(&req.function.url, req.step_id);
        let mut rb = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .header("X-Run-ID", req.state.meta.run_id.to_string())
            .header("X-Inngest-Step-ID", req.step_id)
            .header("x-inngest-req-version", req.state.meta.request_version.to_string())
            .header("x-request-id", ulid::Ulid::generate().to_string())
            .header("x-inngest-job-id", req.queue_item_id);
        if let Some(k) = &self.signing {
            rb = rb.header("X-Inngest-Signature", k.sign(&body, chrono::Utc::now().timestamp()));
        } else {
            rb = rb.header("X-Inngest-Signature", "");
        }
        let resp = match rb.body(body).send().await {
            Ok(r) => r,
            Err(err) => {
                let kind = if err.is_timeout() { "timeout" } else { "connection error" };
                return DriverResult {
                    response: Response::Error {
                        error: error_value("Error", &format!("{kind}: {err}")),
                        retryable: true,
                        retry_at: None,
                    },
                    request_version: None,
                };
            }
        };
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let header = |n: &str| headers.get(n).and_then(|v| v.to_str().ok()).map(str::to_string);
        let from_sdk = header("x-inngest-sdk").is_some_and(|s| !s.is_empty());
        let no_retry = header("x-inngest-no-retry").as_deref() == Some("true");
        let retry_at = header("retry-after").and_then(|v| parse_retry_after(&v));
        let request_version = header("x-inngest-req-version").and_then(|v| v.parse().ok());
        let bytes = match resp.bytes().await {
            Ok(b) => b,
            Err(err) => {
                return DriverResult {
                    response: Response::Error {
                        error: error_value("Error", &format!("error reading response: {err}")),
                        retryable: true,
                        retry_at,
                    },
                    request_version,
                };
            }
        };
        if bytes.len() > MAX_RESPONSE_BYTES {
            return DriverResult {
                response: Response::Error {
                    error: error_value("OutputTooLarge", "SDK response exceeded 8MB"),
                    retryable: false,
                    retry_at: None,
                },
                request_version,
            };
        }
        let response = classify(status, &bytes, from_sdk, no_retry, retry_at);
        DriverResult {
            response,
            request_version,
        }
    }
}

fn classify(status: u16, body: &[u8], from_sdk: bool, no_retry: bool, retry_at: Option<i64>) -> Response {
    // Streaming responses wrap the real status/body.
    if status == 201 {
        let trimmed = std::str::from_utf8(body).unwrap_or("").trim();
        if let Ok(v) = serde_json::from_str::<Value>(trimmed)
            && let Some(inner_status) = v.get("status").and_then(|s| s.as_u64())
        {
            let inner_body = match v.get("body") {
                Some(Value::String(s)) => s.clone().into_bytes(),
                Some(other) => serde_json::to_vec(other).unwrap_or_default(),
                None => vec![],
            };
            return classify(inner_status as u16, &inner_body, from_sdk, no_retry, retry_at);
        }
    }
    if (200..300).contains(&status) && !from_sdk {
        return Response::Error {
            error: error_value("Error", "response did not come from an Inngest SDK"),
            retryable: true,
            retry_at,
        };
    }
    match status {
        200 => Response::Complete(parse_output(body)),
        206 => match parse_ops(body) {
            Ok(ops) => Response::Ops {
                ops,
                no_retry,
                retry_at,
            },
            Err(e) => Response::Error {
                error: error_value("Error", &format!("invalid opcode response: {e}")),
                retryable: true,
                retry_at,
            },
        },
        _ => {
            let parsed = serde_json::from_slice::<Value>(body).ok();
            let error = std_error(parsed, status, no_retry);
            Response::Error {
                error,
                retryable: !no_retry,
                retry_at,
            }
        }
    }
}

/// Build Inngest's standard error object from an SDK error body.
fn std_error(body: Option<Value>, status: u16, no_retry: bool) -> Value {
    let err_str = if no_retry {
        "NonRetriableError".to_string()
    } else {
        format!("invalid status code: {status}")
    };
    let mut out = Map::new();
    out.insert("error".into(), Value::String(err_str.clone()));
    match body {
        Some(Value::Object(m)) => {
            for k in ["name", "message", "stack", "cause", "data"] {
                if let Some(v) = m.get(k) {
                    out.insert(k.into(), v.clone());
                }
            }
            if !out.contains_key("message")
                && let Some(e) = m.get("error")
            {
                out.insert(
                    "message".into(),
                    match e {
                        Value::String(s) => Value::String(s.clone()),
                        other => Value::String(other.to_string()),
                    },
                );
            }
        }
        Some(Value::String(s)) => {
            out.insert("message".into(), Value::String(s));
        }
        _ => {}
    }
    out.entry("name").or_insert_with(|| Value::String("Error".into()));
    out.entry("message").or_insert_with(|| Value::String(err_str));
    Value::Object(out)
}

fn parse_output(body: &[u8]) -> Value {
    if body.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(body)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).into_owned()))
}

fn parse_ops(body: &[u8]) -> Result<Vec<Op>, serde_json::Error> {
    let v: Value = if body.is_empty() {
        Value::Array(vec![])
    } else {
        serde_json::from_slice(body)?
    };
    match v {
        Value::Array(items) => items
            .into_iter()
            .filter(|i| !i.is_null())
            .map(serde_json::from_value)
            .collect(),
        Value::Object(_) => Ok(vec![serde_json::from_value(v)?]),
        _ => Ok(vec![]),
    }
}

fn parse_retry_after(v: &str) -> Option<i64> {
    let now = now_ms();
    let at = if v.len() <= 7 {
        now + v.trim().parse::<i64>().ok()? * 1000
    } else {
        crate::util::parse_date_ms(v)?
    };
    Some(at.clamp(now + 1000, now + 86_400_000))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_responses() {
        match classify(200, b"{\"a\":1}", true, false, None) {
            Response::Complete(v) => assert_eq!(v, json!({"a": 1})),
            r => panic!("{r:?}"),
        }
        match classify(206, br#"[{"op":"StepRun","id":"abc","data":1}]"#, true, false, None) {
            Response::Ops { ops, .. } => assert_eq!(ops[0].op, "StepRun"),
            r => panic!("{r:?}"),
        }
        match classify(400, br#"{"name":"NonRetriableError","message":"nope"}"#, true, true, None) {
            Response::Error { error, retryable, .. } => {
                assert!(!retryable);
                assert_eq!(error["message"], "nope");
                assert_eq!(error["error"], "NonRetriableError");
            }
            r => panic!("{r:?}"),
        }
        match classify(200, b"{}", false, false, None) {
            Response::Error { retryable, .. } => assert!(retryable),
            r => panic!("{r:?}"),
        }
        let stream = br#"   {"status":200,"headers":{},"body":"{\"ok\":true}","version":2}"#;
        match classify(201, stream, true, false, None) {
            Response::Complete(v) => assert_eq!(v, json!({"ok": true})),
            r => panic!("{r:?}"),
        }
    }
}
