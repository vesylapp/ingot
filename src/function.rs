//! Parsing and validating SDK function configs into engine `Function`s.

use crate::types::*;
use crate::util::{normalize_url, parse_duration_ms, parse_go_duration_ms};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

fn err<T>(msg: impl Into<String>) -> Result<T, ConfigError> {
    Err(ConfigError(msg.into()))
}

fn get_str<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(|x| x.as_str()).filter(|s| !s.is_empty())
}

fn get_u32(v: &Value, key: &str) -> Option<u32> {
    v.get(key)
        .and_then(|x| x.as_f64())
        .map(|f| f.max(0.0) as u32)
}

/// Parse a raw SDK function config. `existing_id` is reused if the function was synced before.
pub fn parse_function(
    raw: &Value,
    app_id: Uuid,
    app_name: &str,
    existing: Option<(Uuid, i32)>,
) -> Result<Function, ConfigError> {
    let slug = match get_str(raw, "id") {
        Some(s) => s.to_string(),
        None => match get_str(raw, "name") {
            Some(n) => slugify(n),
            None => return err("function id is required"),
        },
    };
    let name = get_str(raw, "name").unwrap_or(&slug).to_string();

    // Steps: exactly one, keyed "step".
    let steps = raw.get("steps").and_then(|s| s.as_object());
    let Some(steps) = steps.filter(|s| !s.is_empty()) else {
        return err(format!("function {slug} must have a step"));
    };
    if steps.len() > 1 {
        return err(format!("function {slug} must have exactly one step"));
    }
    let step = steps.values().next().unwrap();
    let url = step
        .get("runtime")
        .and_then(|r| get_str(r, "url"))
        .map(normalize_url)
        .ok_or_else(|| ConfigError(format!("function {slug} step has no runtime url")))?;
    match url::Url::parse(&url).map(|u| u.scheme().to_string()) {
        Ok(s) if matches!(s.as_str(), "http" | "https" | "ws" | "wss") => {}
        _ => return err(format!("function {slug} has an invalid step url: {url}")),
    }

    let retries = step
        .get("retries")
        .and_then(|r| get_u32(r, "attempts"))
        .or_else(|| get_u32(raw, "retries"))
        .unwrap_or(DEFAULT_RETRIES)
        .min(MAX_RETRIES);

    // Triggers
    let mut triggers = Vec::new();
    for t in raw
        .get("triggers")
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default()
    {
        let trig: Trigger = serde_json::from_value(t)
            .map_err(|e| ConfigError(format!("invalid trigger: {e}")))?;
        match (&trig.event, &trig.cron) {
            (Some(ev), None) => {
                if ev.len() > 255 {
                    return err("trigger event name too long");
                }
                if let Some(expr) = trig.expression.as_deref().filter(|e| !e.is_empty()) {
                    oxidle_expr::Expression::compile(expr)
                        .map_err(|e| ConfigError(format!("invalid trigger expression: {e}")))?;
                }
            }
            (None, Some(c)) => {
                crate::cron::CronSchedule::parse(c)
                    .map_err(|e| ConfigError(format!("invalid cron expression '{c}': {e}")))?;
                if let Some(j) = trig.jitter.as_deref().filter(|j| !j.is_empty()) {
                    let ms = parse_duration_ms(j)
                        .ok_or_else(|| ConfigError(format!("invalid cron jitter: {j}")))?;
                    if ms != 0 && !(1000..=300_000).contains(&ms) {
                        return err("cron jitter must be between 1s and 5m");
                    }
                }
            }
            _ => return err("trigger must have exactly one of event or cron"),
        }
        if triggers.contains(&trig) {
            return err("duplicate trigger");
        }
        triggers.push(trig);
    }
    if triggers.len() > 10 {
        return err("too many triggers (max 10)");
    }

    let concurrency = parse_concurrency(raw.get("concurrency"))?;

    let throttle = match raw.get("throttle").filter(|v| v.is_object()) {
        Some(t) => {
            let period = get_str(t, "period")
                .and_then(parse_duration_ms)
                .ok_or_else(|| ConfigError("throttle period is invalid".into()))?;
            Some(Throttle {
                limit: get_u32(t, "limit").unwrap_or(1).max(1),
                period_ms: (period / 1000).max(1) * 1000,
                burst: get_u32(t, "burst").unwrap_or(0),
                key: get_str(t, "key").map(String::from),
            })
        }
        None => None,
    };

    let mut rate_limit = match raw.get("rateLimit").filter(|v| v.is_object()) {
        Some(r) => {
            let limit = get_u32(r, "limit").unwrap_or(0);
            if limit == 0 {
                return err("rateLimit limit must be greater than 0");
            }
            let period = get_str(r, "period")
                .and_then(parse_duration_ms)
                .ok_or_else(|| ConfigError("rateLimit period is invalid".into()))?;
            if period > 86_400_000 || period <= 0 {
                return err("rateLimit period must be between 1s and 24h");
            }
            Some(RateLimit {
                limit,
                period_ms: period,
                key: get_str(r, "key").map(String::from),
            })
        }
        None => None,
    };
    if let Some(key) = get_str(raw, "idempotency") {
        rate_limit = Some(RateLimit {
            limit: 1,
            period_ms: 86_400_000,
            key: Some(key.to_string()),
        });
    }

    let debounce = match raw.get("debounce").filter(|v| v.is_object()) {
        Some(d) => {
            let period = get_str(d, "period")
                .and_then(parse_duration_ms)
                .ok_or_else(|| ConfigError("debounce period is invalid".into()))?;
            if !(1000..=7 * 86_400_000).contains(&period) {
                return err("debounce period must be between 1s and 7d");
            }
            Some(Debounce {
                period_ms: period,
                key: get_str(d, "key").map(String::from),
                timeout_ms: get_str(d, "timeout").and_then(parse_duration_ms),
            })
        }
        None => None,
    };

    let batch = match raw.get("batchEvents").filter(|v| v.is_object()) {
        Some(b) => {
            let max_size = get_u32(b, "maxSize").unwrap_or(0);
            match get_str(b, "timeout") {
                Some(t) if max_size > 1 => {
                    let timeout = parse_go_duration_ms(t)
                        .ok_or_else(|| ConfigError(format!("invalid batch timeout: {t}")))?;
                    if timeout < 1000 {
                        return err("batch timeout must be at least 1s");
                    }
                    Some(BatchEvents {
                        max_size,
                        timeout_ms: timeout,
                        key: get_str(b, "key").map(String::from),
                        if_expr: get_str(b, "if").map(String::from),
                    })
                }
                _ => None,
            }
        }
        None => None,
    };

    let priority_run = raw
        .get("priority")
        .and_then(|p| get_str(p, "run"))
        .map(String::from);

    let mut cancel = Vec::new();
    for c in raw
        .get("cancel")
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default()
    {
        let Some(event) = get_str(&c, "event") else {
            return err("cancel requires an event");
        };
        let if_expr = match get_str(&c, "match") {
            Some(m) => Some(format!("event.{m} == async.{m}")),
            None => get_str(&c, "if").map(String::from),
        };
        if let Some(e) = &if_expr {
            let compiled = oxidle_expr::Expression::compile(e)
                .map_err(|e| ConfigError(format!("invalid cancel expression: {e}")))?;
            compiled
                .validate_restrictive()
                .map_err(|e| ConfigError(format!("invalid cancel expression: {e}")))?;
        }
        cancel.push(CancelOn {
            event: event.to_string(),
            if_expr,
            timeout: get_str(&c, "timeout").map(String::from),
        });
    }
    if cancel.len() > 5 {
        return err("too many cancellation events (max 5)");
    }

    let timeouts = raw.get("timeouts");
    let timeout_start_ms = timeouts
        .and_then(|t| get_str(t, "start"))
        .and_then(parse_duration_ms);
    let timeout_finish_ms = timeouts
        .and_then(|t| get_str(t, "finish"))
        .and_then(parse_duration_ms);

    let singleton = match raw.get("singleton").filter(|v| v.is_object()) {
        Some(s) => Some(Singleton {
            key: get_str(s, "key").map(String::from),
            mode: match get_str(s, "mode") {
                Some("cancel") => SingletonMode::Cancel,
                _ => SingletonMode::Skip,
            },
        }),
        None => None,
    };

    if batch.is_some()
        && (!cancel.is_empty() || debounce.is_some() || singleton.is_some() || priority_run.is_some())
    {
        return err("batching cannot be combined with cancellation, debounce, singleton or priority");
    }

    // Validate all key expressions compile.
    let mut exprs: Vec<&str> = Vec::new();
    exprs.extend(concurrency.iter().filter_map(|c| c.key.as_deref()));
    exprs.extend(throttle.iter().filter_map(|t| t.key.as_deref()));
    exprs.extend(rate_limit.iter().filter_map(|t| t.key.as_deref()));
    exprs.extend(debounce.iter().filter_map(|t| t.key.as_deref()));
    exprs.extend(batch.iter().filter_map(|t| t.key.as_deref()));
    exprs.extend(batch.iter().filter_map(|t| t.if_expr.as_deref()));
    exprs.extend(singleton.iter().filter_map(|t| t.key.as_deref()));
    exprs.extend(priority_run.as_deref());
    for e in exprs {
        oxidle_expr::Expression::compile(e)
            .map_err(|er| ConfigError(format!("invalid expression '{e}': {er}")))?;
    }

    let (id, version) = match existing {
        Some((id, v)) => (id, v + 1),
        None => (crate::util::uuid_v5(&format!("{name}{url}")), 1),
    };

    Ok(Function {
        id,
        app_id,
        app_name: app_name.to_string(),
        slug,
        name,
        version,
        url,
        retries,
        triggers,
        concurrency,
        throttle,
        rate_limit,
        debounce,
        batch,
        priority_run,
        cancel,
        timeout_start_ms,
        timeout_finish_ms,
        singleton,
        config: raw.clone(),
    })
}

fn parse_concurrency(v: Option<&Value>) -> Result<Vec<ConcurrencyLimit>, ConfigError> {
    let mut out = Vec::new();
    let Some(v) = v else { return Ok(out) };
    let items: Vec<Value> = match v {
        Value::Null => vec![],
        Value::Number(_) => vec![serde_json::json!({ "limit": v })],
        Value::Array(a) => a.clone(),
        Value::Object(o) if o.contains_key("fn") || o.contains_key("step") => {
            let mut all = Vec::new();
            for k in ["fn", "step"] {
                if let Some(Value::Array(a)) = o.get(k) {
                    all.extend(a.iter().cloned());
                }
            }
            all
        }
        Value::Object(_) => vec![v.clone()],
        _ => return err("invalid concurrency config"),
    };
    for item in items {
        let limit = get_u32(&item, "limit").unwrap_or(0);
        let scope = match get_str(&item, "scope").map(|s| s.to_ascii_lowercase()) {
            None => ConcurrencyScope::Fn,
            Some(s) if s == "fn" => ConcurrencyScope::Fn,
            Some(s) if s == "env" => ConcurrencyScope::Env,
            Some(s) if s == "account" => ConcurrencyScope::Account,
            Some(s) => return err(format!("invalid concurrency scope: {s}")),
        };
        let key = get_str(&item, "key").map(String::from);
        if scope != ConcurrencyScope::Fn && key.is_none() {
            return err("concurrency limits with env or account scope require a key");
        }
        if limit == 0 {
            continue;
        }
        out.push(ConcurrencyLimit { limit, key, scope });
    }
    if out.len() > 2 {
        return err("too many concurrency limits (max 2)");
    }
    out.sort_by_key(|c| c.limit);
    Ok(out)
}

fn slugify(name: &str) -> String {
    let mut out = String::new();
    let mut dash = false;
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            dash = false;
        } else if !dash && !out.is_empty() {
            out.push('-');
            dash = true;
        }
    }
    out.trim_end_matches('-').to_string()
}

/// Generate the candidate trigger patterns for an event name (exact + wildcards).
pub fn event_patterns(name: &str) -> Vec<String> {
    let mut out = vec![name.to_string()];
    for (i, c) in name.char_indices() {
        if c == '/' {
            out.push(format!("{}/*", &name[..i]));
        }
    }
    for (i, c) in name.char_indices() {
        if c == '.' {
            out.push(format!("{}.*", &name[..i]));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn patterns() {
        assert_eq!(
            event_patterns("app/user.created"),
            vec!["app/user.created", "app/*", "app/user.*"]
        );
    }

    #[test]
    fn parse_basic() {
        let raw = json!({
            "id": "my-app-hello",
            "name": "Hello",
            "triggers": [{"event": "app/hello"}],
            "steps": {"step": {"id": "step", "name": "step", "runtime": {"type": "http", "url": "http://127.0.0.1:3000/api/inngest?fnId=my-app-hello&stepId=step"}}},
            "concurrency": [{"limit": 5, "key": "event.data.user", "scope": "env"}, {"limit": 2}],
            "throttle": {"limit": 5, "period": "1m"},
            "idempotency": "event.data.id"
        });
        let f = parse_function(&raw, Uuid::nil(), "my-app", None).unwrap();
        assert_eq!(f.slug, "my-app-hello");
        assert_eq!(f.retries, 4);
        assert_eq!(f.url, "http://localhost:3000/api/inngest?fnId=my-app-hello&stepId=step");
        assert_eq!(f.concurrency.len(), 2);
        assert_eq!(f.concurrency[0].limit, 2);
        assert_eq!(f.rate_limit.as_ref().unwrap().limit, 1);
        assert_eq!(f.throttle.as_ref().unwrap().period_ms, 60_000);
    }
}
