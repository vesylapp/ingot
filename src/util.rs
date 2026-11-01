//! Small shared helpers: hashing, durations, canonical JSON, URL normalization, backoff.

use rand::RngExt;
use serde_json::Value;
use uuid::Uuid;
use xxhash_rust::xxh64::xxh64;

/// xxhash64 rendered in base 36, as used throughout Inngest for keys.
pub fn hash36(s: &str) -> String {
    to_base36(xxh64(s.as_bytes(), 0))
}

pub fn xxhash(s: &str) -> u64 {
    xxh64(s.as_bytes(), 0)
}

fn to_base36(mut n: u64) -> String {
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".into();
    }
    let mut out = Vec::with_capacity(13);
    while n > 0 {
        out.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    out.reverse();
    String::from_utf8(out).unwrap()
}

/// UUIDv5 in the OID namespace (Go `uuid.NewSHA1(uuid.NameSpaceOID, ...)`).
pub fn uuid_v5(name: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}

/// Deterministic UUID derived from the sha1 of a string (pause ids).
pub fn sha1_uuid(s: &str) -> Uuid {
    use sha1::{Digest, Sha1};
    let digest = Sha1::digest(s.as_bytes());
    let mut b = [0u8; 16];
    b.copy_from_slice(&digest[..16]);
    b[6] = (b[6] & 0x0f) | 0x50;
    b[8] = (b[8] & 0x3f) | 0x80;
    Uuid::from_bytes(b)
}

pub fn sha256_hex(b: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(b))
}

/// RFC 8785 canonical JSON bytes.
pub fn canonical_json(v: &Value) -> Vec<u8> {
    serde_json_canonicalizer::to_vec(v).unwrap_or_else(|_| serde_json::to_vec(v).unwrap())
}

// ---------------------------------------------------------------------------
// Durations
// ---------------------------------------------------------------------------

/// Parse a str2duration string ("1h30m", "2d", "1w", "1.5h", "0") into milliseconds.
pub fn parse_duration_ms(s: &str) -> Option<i64> {
    parse_duration_ns(s).map(|ns| ns / 1_000_000)
}

pub fn parse_duration_ns(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (neg, mut rest) = match s.as_bytes()[0] {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    if rest == "0" {
        return Some(0);
    }
    if rest.is_empty() {
        return None;
    }
    let mut total: f64 = 0.0;
    while !rest.is_empty() {
        let num_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if num_end == 0 {
            return None;
        }
        let num: f64 = rest[..num_end].parse().ok()?;
        rest = &rest[num_end..];
        let unit_end = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let unit = &rest[..unit_end];
        rest = &rest[unit_end..];
        let mult: f64 = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 60e9,
            "h" => 3600e9,
            "d" => 86400e9,
            "w" => 7.0 * 86400e9,
            _ => return None,
        };
        total += num * mult;
    }
    let ns = total.round() as i64;
    Some(if neg { -ns } else { ns })
}

/// Go `time.ParseDuration` (no d/w units).
pub fn parse_go_duration_ms(s: &str) -> Option<i64> {
    if s.contains('d') || s.contains('w') {
        return None;
    }
    parse_duration_ms(s)
}

/// Parse a date string in one of the formats Inngest accepts. Returns unix ms.
pub fn parse_date_ms(s: &str) -> Option<i64> {
    use chrono::{DateTime, NaiveDate, NaiveDateTime};
    let s = s.trim();
    if let Ok(d) = DateTime::parse_from_rfc3339(s) {
        return Some(d.timestamp_millis());
    }
    if let Ok(d) = DateTime::parse_from_rfc2822(s) {
        return Some(d.timestamp_millis());
    }
    for f in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(d) = NaiveDateTime::parse_from_str(s, f) {
            return Some(d.and_utc().timestamp_millis());
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(d.and_hms_opt(0, 0, 0)?.and_utc().timestamp_millis());
    }
    None
}

/// Parse a timeout: either a duration (relative to `now`) or an absolute RFC3339 time.
pub fn parse_timeout_at(s: &str, now: i64) -> Option<i64> {
    if let Some(ms) = parse_duration_ms(s) {
        return Some(now + ms);
    }
    parse_date_ms(s)
}

/// Format milliseconds as a Go-style duration string ("24h0m0s").
pub fn format_go_duration(ms: i64) -> String {
    let total_s = ms / 1000;
    let h = total_s / 3600;
    let m = (total_s % 3600) / 60;
    let s = total_s % 60;
    let rem_ms = ms % 1000;
    if h > 0 {
        format!("{h}h{m}m{s}s")
    } else if m > 0 {
        format!("{m}m{s}s")
    } else if rem_ms > 0 {
        format!("{}s", ms as f64 / 1000.0)
    } else {
        format!("{s}s")
    }
}

pub fn rfc3339_ms(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap_or_default()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub fn rfc3339_nanos(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap_or_default()
        .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

// ---------------------------------------------------------------------------
// Backoff
// ---------------------------------------------------------------------------

const BACKOFF_TABLE_S: [i64; 10] = [15, 30, 60, 120, 300, 600, 1200, 2400, 3600, 7200];

/// Delay before retrying an item that just failed `attempt` (0-based).
pub fn backoff_ms(attempt: u32, retry_interval_s: Option<u64>) -> i64 {
    if let Some(n) = retry_interval_s.filter(|n| *n > 0) {
        return n as i64 * 1000;
    }
    let base = BACKOFF_TABLE_S[(attempt as usize).min(BACKOFF_TABLE_S.len() - 1)] * 1000;
    base + rand::rng().random_range(0..30_000)
}

// ---------------------------------------------------------------------------
// URLs
// ---------------------------------------------------------------------------

/// Normalize an app/step URL the same way Inngest does: drop deployId/probe params,
/// sort query keys, drop default ports and map loopback hosts to localhost.
pub fn normalize_url(raw: &str) -> String {
    let Ok(mut u) = url::Url::parse(raw) else {
        return raw.to_string();
    };
    let mut pairs: Vec<(String, String)> = u
        .query_pairs()
        .filter(|(k, _)| k != "deployId" && k != "probe")
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    pairs.sort();
    if pairs.is_empty() {
        u.set_query(None);
    } else {
        u.query_pairs_mut().clear().extend_pairs(pairs);
    }
    if matches!(u.host_str(), Some("127.0.0.1") | Some("0.0.0.0")) {
        let _ = u.set_host(Some("localhost"));
    }
    // url::Url already drops default ports for known schemes.
    u.to_string()
}

/// Replace (or add) the `stepId` query parameter on a step URL.
pub fn with_step_id(raw: &str, step_id: &str) -> String {
    let Ok(mut u) = url::Url::parse(raw) else {
        return raw.to_string();
    };
    let mut pairs: Vec<(String, String)> = u
        .query_pairs()
        .filter(|(k, _)| k != "stepId")
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    pairs.push(("stepId".into(), step_id.into()));
    pairs.sort();
    u.query_pairs_mut().clear().extend_pairs(pairs);
    u.to_string()
}

/// Truncate a string for logging.
pub fn trunc(s: &str, n: usize) -> &str {
    match s.char_indices().nth(n) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration_ms("1h30m"), Some(5_400_000));
        assert_eq!(parse_duration_ms("2d"), Some(172_800_000));
        assert_eq!(parse_duration_ms("1w"), Some(604_800_000));
        assert_eq!(parse_duration_ms("1.5h"), Some(5_400_000));
        assert_eq!(parse_duration_ms("500ms"), Some(500));
        assert_eq!(parse_duration_ms("0"), Some(0));
        assert_eq!(parse_duration_ms("10"), None);
        assert_eq!(parse_duration_ms(""), None);
        assert_eq!(parse_duration_ms("1x"), None);
        assert_eq!(parse_go_duration_ms("1d"), None);
        assert_eq!(format_go_duration(86_400_000), "24h0m0s");
    }

    #[test]
    fn urls() {
        assert_eq!(
            normalize_url("http://127.0.0.1:80/api/inngest?stepId=step&fnId=a&deployId=x"),
            "http://localhost/api/inngest?fnId=a&stepId=step"
        );
        assert_eq!(
            with_step_id("http://localhost:3000/api/inngest?fnId=a&stepId=step", "abc"),
            "http://localhost:3000/api/inngest?fnId=a&stepId=abc"
        );
    }

    #[test]
    fn uuids() {
        // Matches Go uuid.NewSHA1(uuid.NameSpaceOID, []byte("my-app")) version bits.
        let u = uuid_v5("my-app");
        assert_eq!(u.get_version_num(), 5);
    }
}
