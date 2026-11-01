//! `POST /e/{key}` event ingestion and `POST /invoke/{slug}`.

use super::{AppState, json_response, public_error};
use crate::types::*;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use base64::Engine as _;
use serde_json::value::RawValue;
use serde_json::{Map, Value, json};
use ulid::Ulid;

const MAX_EVENT_SIZE: usize = 3 * 1024 * 1024;
const MAX_EVENTS: usize = 5000;

fn api_error(status: StatusCode, msg: &str) -> Response {
    json_response(
        status,
        &json!({ "status": status.as_u16(), "message": "", "error": msg }),
    )
}

pub async fn missing_key() -> Response {
    api_error(StatusCode::UNAUTHORIZED, "Event key is required")
}

/// Seeded ULID generation (`x-inngest-event-id-seed: <millis>,<base64 10 bytes>`).
struct IdSeed {
    millis: u64,
    entropy: [u8; 10],
}

impl IdSeed {
    fn parse(h: &str) -> Option<IdSeed> {
        let (ms, ent) = h.split_once(',')?;
        let millis: u64 = ms.trim().parse().ok().filter(|m| *m > 0)?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(ent.trim())
            .ok()?;
        let entropy: [u8; 10] = bytes.try_into().ok()?;
        Some(IdSeed { millis, entropy })
    }

    fn id(&self, index: u32) -> Ulid {
        let mut e = self.entropy;
        let tail = u32::from_be_bytes([e[6], e[7], e[8], e[9]]).wrapping_add(index);
        e[6..10].copy_from_slice(&tail.to_be_bytes());
        let mut rand: u128 = 0;
        for b in e {
            rand = (rand << 8) | b as u128;
        }
        Ulid::from_parts(self.millis, rand)
    }
}

/// Normalize and validate an inbound event. Returns (status, message) on failure.
fn normalize(mut ev: Event, now: i64) -> Result<Event, (StatusCode, String)> {
    if ev.name.to_lowercase().starts_with("inngest/") {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("event name is reserved for internal use: {}", ev.name),
        ));
    }
    ev.data.remove("_inngest");
    if ev.name.is_empty() {
        return Err((StatusCode::NOT_ACCEPTABLE, "event name is empty".into()));
    }
    if ev.name.contains('\0') {
        return Err((StatusCode::NOT_ACCEPTABLE, "event name contains a NUL character".into()));
    }
    if ev.ts != 0 && !(315_532_800_000..4_102_444_800_000).contains(&ev.ts) {
        return Err((
            StatusCode::NOT_ACCEPTABLE,
            "event timestamp must be between 1980-01-01 and 2100-01-01".into(),
        ));
    }
    if ev.ts == 0 {
        ev.ts = now;
    }
    if ev.user.is_none() || ev.user.as_ref().is_some_and(|u| u.is_null()) {
        ev.user = Some(Value::Object(Map::new()));
    }
    Ok(ev)
}

pub async fn ingest(
    State(engine): State<AppState>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if key.is_empty() {
        return api_error(StatusCode::UNAUTHORIZED, "Event key is required");
    }
    if !engine.cfg.event_keys.is_empty() && !engine.cfg.event_keys.iter().any(|k| k == &key) {
        return api_error(StatusCode::UNAUTHORIZED, "Event key not found");
    }

    let text = String::from_utf8_lossy(&body);
    let trimmed = text.trim_start();
    let raws: Vec<Box<RawValue>> = match trimmed.as_bytes().first() {
        None => vec![],
        Some(b'{') => {
            if body.len() > MAX_EVENT_SIZE {
                return api_error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    &format!("Event is over the max size: Max {MAX_EVENT_SIZE} bytes / Size {} bytes", body.len()),
                );
            }
            match serde_json::from_str::<Box<RawValue>>(trimmed) {
                Ok(r) => vec![r],
                Err(e) => return api_error(StatusCode::BAD_REQUEST, &e.to_string()),
            }
        }
        Some(b'[') => match serde_json::from_str::<Vec<Box<RawValue>>>(trimmed) {
            Ok(v) => v,
            Err(e) => return api_error(StatusCode::BAD_REQUEST, &e.to_string()),
        },
        Some(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "Request body must contain an event object or an array of event objects",
            );
        }
    };
    if raws.len() > MAX_EVENTS {
        return api_error(
            StatusCode::BAD_REQUEST,
            &format!("Cannot have more than {MAX_EVENTS} events in a batch"),
        );
    }
    if raws.is_empty() {
        return json_response(StatusCode::OK, &json!({ "ids": [""], "status": 200 }));
    }

    let seed = headers
        .get("x-inngest-event-id-seed")
        .and_then(|h| h.to_str().ok())
        .and_then(IdSeed::parse);
    let now = now_ms();
    let mut ids: Vec<String> = Vec::with_capacity(raws.len());
    let mut tracked = Vec::with_capacity(raws.len());
    let mut failure: Option<(StatusCode, String)> = None;
    for (i, raw) in raws.iter().enumerate() {
        let size = raw.get().len();
        if size > MAX_EVENT_SIZE {
            ids.push(String::new());
            failure.get_or_insert((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("Event is over the max size: Max {MAX_EVENT_SIZE} bytes / Size {size} bytes"),
            ));
            continue;
        }
        let ev: Event = match serde_json::from_str(raw.get()) {
            Ok(e) => e,
            Err(e) => {
                // Undecodable items stop processing of the batch.
                failure.get_or_insert((StatusCode::BAD_REQUEST, e.to_string()));
                break;
            }
        };
        match normalize(ev, now) {
            Ok(mut ev) => {
                let internal_id = match &seed {
                    Some(s) => s.id(i as u32 + 1),
                    None => Ulid::generate(),
                };
                if ev.id.is_empty() {
                    ev.id = internal_id.to_string();
                }
                ids.push(internal_id.to_string());
                tracked.push(TrackedEvent {
                    internal_id,
                    event: ev,
                    received_at: now,
                });
            }
            Err(f) => {
                ids.push(String::new());
                failure.get_or_insert(f);
            }
        }
    }

    if let Err(err) = engine.meta.insert_events(&tracked).await {
        tracing::error!(error = %err, "failed to store events");
        return json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({ "ids": vec![""; ids.len()], "status": 500, "error": err.to_string() }),
        );
    }
    engine.route_events(tracked).await;

    match failure {
        None => json_response(StatusCode::OK, &json!({ "ids": ids, "status": 200 })),
        Some((status, msg)) => json_response(
            status,
            &json!({ "ids": ids, "status": status.as_u16(), "error": msg }),
        ),
    }
}

pub async fn invoke(
    State(engine): State<AppState>,
    Path(slug): Path<String>,
    body: Bytes,
) -> Response {
    if slug.is_empty() {
        return public_error(StatusCode::BAD_REQUEST, "Function slug is required");
    }
    let ev: Event = if body.is_empty() {
        serde_json::from_value(json!({})).unwrap()
    } else {
        match serde_json::from_slice(&body) {
            Ok(e) => e,
            Err(e) => return public_error(StatusCode::BAD_REQUEST, &e.to_string()),
        }
    };
    let Some(f) = engine.registry.by_slug(&slug) else {
        return public_error(StatusCode::NOT_FOUND, &format!("function not found: {slug}"));
    };
    let mut data = ev.data.clone();
    data.remove("_inngest");
    data.insert(
        "_inngest".into(),
        json!({ "fn_id": f.slug, "correlation_id": "", "expire": 0, "gid": "", "name": "", "source_app_id": "", "source_fn_id": "", "source_fn_v": 0 }),
    );
    let evt = Event {
        id: ev.id,
        name: EVENT_FN_INVOKED.into(),
        data,
        user: ev.user,
        ts: ev.ts,
        v: ev.v,
        meta: None,
    };
    match engine.publish_internal(vec![evt]).await {
        Ok(t) => json_response(
            StatusCode::OK,
            &json!({ "id": t[0].internal_id.to_string(), "status": 200 }),
        ),
        Err(e) => super::internal(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_ids() {
        let s = IdSeed::parse("1743130137367,eii2YKXRVTJPuA==").unwrap();
        let a = s.id(1);
        let b = s.id(1);
        assert_eq!(a, b);
        assert_ne!(s.id(1), s.id(2));
        assert_eq!(a.timestamp_ms(), 1743130137367);
    }

    #[test]
    fn normalize_rules() {
        let ev: Event = serde_json::from_value(json!({"name": "inngest/x"})).unwrap();
        assert_eq!(normalize(ev, 1).unwrap_err().0, StatusCode::BAD_REQUEST);
        let ev: Event = serde_json::from_value(json!({"name": ""})).unwrap();
        assert_eq!(normalize(ev, 1).unwrap_err().0, StatusCode::NOT_ACCEPTABLE);
        let ev: Event = serde_json::from_value(json!({"name": "a", "data": {"_inngest": 1, "x": 2}})).unwrap();
        let ev = normalize(ev, 5).unwrap();
        assert_eq!(ev.ts, 5);
        assert!(!ev.data.contains_key("_inngest"));
        assert_eq!(ev.user, Some(json!({})));
    }
}
