//! Time helpers: Go-compatible duration parsing/formatting, Inngest's
//! `dateutil` parsing, and Go `time.Time` formatting.

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc};

/// A timestamp; like Go's time.Time it keeps the offset it was parsed with.
pub(crate) type Ts = DateTime<FixedOffset>;

fn utc(t: NaiveDateTime) -> Ts {
    Utc.from_utc_datetime(&t).fixed_offset()
}

const NS: i64 = 1;
const US: i64 = 1_000;
const MS: i64 = 1_000_000;
const SEC: i64 = 1_000_000_000;
const MIN: i64 = 60 * SEC;
const HOUR: i64 = 60 * MIN;

/// Go's RFC3339Nano formatting (trailing zeros trimmed, `Z` for UTC).
pub(crate) fn rfc3339_nano(t: &Ts) -> String {
    let mut s = t.format("%Y-%m-%dT%H:%M:%S").to_string();
    push_nanos(&mut s, t.timestamp_subsec_nanos());
    if t.offset().local_minus_utc() == 0 {
        s.push('Z');
    } else {
        s.push_str(&t.format("%:z").to_string());
    }
    s
}

fn push_nanos(s: &mut String, n: u32) {
    if n > 0 {
        let f = format!("{n:09}");
        s.push('.');
        s.push_str(f.trim_end_matches('0'));
    }
}

/// Parses a duration like Go's `time.ParseDuration` (`allow_days == false`) or
/// `str2duration.ParseDuration` (`allow_days == true`, adds `d` and `w`).
/// Returns nanoseconds.
pub(crate) fn parse_duration(orig: &str, allow_days: bool) -> Result<i64, String> {
    let invalid = || format!("time: invalid duration \"{orig}\"");
    let mut s = orig;
    let mut neg = false;
    if let Some(c) = s.chars().next()
        && (c == '-' || c == '+')
    {
        neg = c == '-';
        s = &s[1..];
    }
    if s == "0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(invalid());
    }
    let mut total: u64 = 0;
    while !s.is_empty() {
        let b = s.as_bytes();
        if !(b[0] == b'.' || b[0].is_ascii_digit()) {
            return Err(invalid());
        }
        // Leading integer.
        let mut i = 0;
        let mut v: u64 = 0;
        while i < b.len() && b[i].is_ascii_digit() {
            v = v
                .checked_mul(10)
                .and_then(|v| v.checked_add(u64::from(b[i] - b'0')))
                .filter(|v| *v <= i64::MAX as u64)
                .ok_or_else(invalid)?;
            i += 1;
        }
        let pre = i > 0;
        // Fraction.
        let mut f: u64 = 0;
        let mut scale: f64 = 1.0;
        let mut post = false;
        if i < b.len() && b[i] == b'.' {
            i += 1;
            let start = i;
            let mut overflow = false;
            while i < b.len() && b[i].is_ascii_digit() {
                if !overflow {
                    match f.checked_mul(10).and_then(|x| x.checked_add(u64::from(b[i] - b'0'))) {
                        Some(y) if y <= i64::MAX as u64 => {
                            f = y;
                            scale *= 10.0;
                        }
                        _ => overflow = true,
                    }
                }
                i += 1;
            }
            post = i > start;
        }
        if !pre && !post {
            return Err(invalid());
        }
        // Unit.
        let rest = &s[i..];
        let unit_len = rest
            .char_indices()
            .find(|(_, c)| *c == '.' || c.is_ascii_digit())
            .map_or(rest.len(), |(idx, _)| idx);
        if unit_len == 0 {
            return Err(format!("time: missing unit in duration \"{orig}\""));
        }
        let u = &rest[..unit_len];
        let unit: i64 = match u {
            "ns" => NS,
            "us" | "\u{00b5}s" | "\u{03bc}s" => US,
            "ms" => MS,
            "s" => SEC,
            "m" => MIN,
            "h" => HOUR,
            "d" if allow_days => 24 * HOUR,
            "w" if allow_days => 168 * HOUR,
            _ => {
                return Err(format!(
                    "time: unknown unit \"{u}\" in duration \"{orig}\""
                ));
            }
        };
        let unit = unit as u64;
        if v > (i64::MAX as u64) / unit {
            return Err(invalid());
        }
        let mut val = v * unit;
        if f > 0 {
            val += (f as f64 * (unit as f64 / scale)) as u64;
            if val > i64::MAX as u64 {
                return Err(invalid());
            }
        }
        total = total.checked_add(val).filter(|t| *t <= i64::MAX as u64).ok_or_else(invalid)?;
        s = &rest[unit_len..];
    }
    let t = total as i64;
    Ok(if neg { -t } else { t })
}

/// Formats nanoseconds like Go's `time.Duration.String()`.
pub(crate) fn go_duration(d: i64) -> String {
    if d == 0 {
        return "0s".into();
    }
    let neg = d < 0;
    let u = d.unsigned_abs();
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if u < SEC as u64 {
        let (prec, unit) = if u < US as u64 {
            (0, "ns")
        } else if u < MS as u64 {
            (3, "\u{00b5}s")
        } else {
            (6, "ms")
        };
        let p = 10u64.pow(prec);
        out.push_str(&(u / p).to_string());
        push_frac(&mut out, u % p, prec);
        out.push_str(unit);
        return out;
    }
    let secs = u / SEC as u64;
    let frac = u % SEC as u64;
    let h = secs / 3600;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    if h > 0 {
        out.push_str(&format!("{h}h{m}m"));
    } else if m > 0 {
        out.push_str(&format!("{m}m"));
    }
    out.push_str(&s.to_string());
    push_frac(&mut out, frac, 9);
    out.push('s');
    out
}

fn push_frac(out: &mut String, frac: u64, prec: u32) {
    if frac == 0 || prec == 0 {
        return;
    }
    let s = format!("{:0width$}", frac, width = prec as usize);
    out.push('.');
    out.push_str(s.trim_end_matches('0'));
}

/// Formats a timestamp like Go's `time.Time.String()`.
pub(crate) fn go_time(t: &Ts) -> String {
    let mut out = t.format("%Y-%m-%d %H:%M:%S").to_string();
    push_nanos(&mut out, t.timestamp_subsec_nanos());
    if t.offset().local_minus_utc() == 0 {
        out.push_str(" +0000 UTC");
    } else {
        let z = t.format("%z").to_string();
        out.push(' ');
        out.push_str(&z);
        out.push(' ');
        out.push_str(&z);
    }
    out
}

/// Go's `time.Parse(time.RFC3339, s)` (fractional seconds accepted).
pub(crate) fn parse_rfc3339(s: &str) -> Result<Ts, String> {
    let strict = s.as_bytes().get(10) == Some(&b'T') && !s.contains('t') && !s.contains('z');
    if !strict {
        return Err(format!("parsing time \"{s}\" as RFC3339: invalid format"));
    }
    DateTime::parse_from_rfc3339(s).map_err(|e| format!("parsing time \"{s}\" as RFC3339: {e}"))
}

/// Inngest `dateutil.ParseInt`: unix seconds, millis or nanos depending on size.
pub(crate) fn date_from_int(i: i64) -> Option<Ts> {
    if i < 946_684_800 {
        return None;
    }
    if i < 9_999_999_999 {
        return Utc.timestamp_opt(i, 0).single().map(|t| t.fixed_offset());
    }
    if i < 9_999_999_999_999 {
        return Some(Utc.timestamp_nanos(i.checked_mul(1_000_000)?).fixed_offset());
    }
    Some(Utc.timestamp_nanos(i).fixed_offset())
}

/// Inngest `dateutil.ParseString`: tries a list of common formats.
pub(crate) fn date_from_str(s: &str) -> Option<Ts> {
    // time.RFC3339 (Go accepts fractional seconds when parsing).
    if let Ok(t) = parse_rfc3339(s) {
        return Some(t);
    }
    // "2006-01-02T15:04:05" (optionally with fractional seconds).
    if let Ok(t) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(utc(t));
    }
    // Formats with a numeric zone offset.
    for fmt in [
        "%d %b %y %H:%M %z",          // RFC822Z
        "%a %b %d %H:%M:%S %z %Y",    // RubyDate
        "%a, %d %b %Y %H:%M:%S %z",   // RFC1123Z-like
    ] {
        if let Ok(t) = DateTime::<FixedOffset>::parse_from_str(s, fmt) {
            return Some(t);
        }
    }
    // Formats with a zone abbreviation (Go treats unknown abbreviations as UTC
    // offset 0); we replace the abbreviation with +0000.
    if let Some(replaced) = replace_zone_abbrev(s) {
        for fmt in [
            "%a, %d %b %Y %H:%M:%S %z",      // RFC1123
            "%d %b %y %H:%M %z",             // RFC822
            "%A, %d-%b-%y %H:%M:%S %z",      // RFC850
            "%a %b %e %H:%M:%S %z %Y",       // UnixDate
            "%Y-%m-%d %H:%M:%S%.f %z %z",    // Go's Time.String() layout (offset + abbrev)
        ] {
            if let Ok(t) = DateTime::<FixedOffset>::parse_from_str(&replaced, fmt) {
                return Some(t);
            }
        }
    }
    // Go's Time.String() layout: "2006-01-02 15:04:05.999999999 -0700 MST".
    if let Some((head, _abbrev)) = s.rsplit_once(' ')
        && let Ok(t) = DateTime::<FixedOffset>::parse_from_str(head, "%Y-%m-%d %H:%M:%S%.f %z")
    {
        return Some(t);
    }
    // ANSIC: "Mon Jan _2 15:04:05 2006".
    if let Ok(t) = NaiveDateTime::parse_from_str(s, "%a %b %e %H:%M:%S %Y") {
        return Some(utc(t));
    }
    // Stamp / StampMilli: "Jan _2 15:04:05" (year 0).
    for fmt in ["%Y %b %e %H:%M:%S%.f"] {
        if let Ok(t) = NaiveDateTime::parse_from_str(&format!("0000 {s}"), fmt) {
            return Some(utc(t));
        }
    }
    // "2006-01-02".
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return d.and_hms_opt(0, 0, 0).map(utc);
    }
    None
}

/// Replaces an all-uppercase zone abbreviation token (e.g. "MST", "UTC") with
/// "+0000". Returns None if there is none.
fn replace_zone_abbrev(s: &str) -> Option<String> {
    let mut found = false;
    let parts: Vec<String> = s
        .split(' ')
        .map(|tok| {
            let is_abbrev = (2..=5).contains(&tok.len())
                && tok.chars().all(|c| c.is_ascii_uppercase())
                && !found;
            if is_abbrev {
                found = true;
                "+0000".to_string()
            } else {
                tok.to_string()
            }
        })
        .collect();
    found.then(|| parts.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse_like_go() {
        assert_eq!(parse_duration("1h30m", false), Ok(90 * MIN));
        assert_eq!(parse_duration("1.5h", false), Ok(90 * MIN));
        assert_eq!(parse_duration("-2s", false), Ok(-2 * SEC));
        assert_eq!(parse_duration("0", false), Ok(0));
        assert_eq!(parse_duration("300ms", false), Ok(300 * MS));
        assert_eq!(parse_duration("1µs", false), Ok(US));
        assert_eq!(parse_duration("1us", false), Ok(US));
        assert_eq!(parse_duration("10ns", false), Ok(10));
        assert!(parse_duration("2d", false).is_err());
        assert_eq!(parse_duration("2d", true), Ok(48 * HOUR));
        assert_eq!(parse_duration("1w2d", true), Ok(216 * HOUR));
        assert_eq!(parse_duration(".5s", false), Ok(SEC / 2));
        assert!(parse_duration("", false).is_err());
        assert!(parse_duration("h", false).is_err());
        assert!(parse_duration("1", false).is_err());
        assert!(parse_duration("1x", true).is_err());
        assert!(parse_duration("99999999999999999999h", true).is_err());
        assert!(parse_duration("3000000h", true).is_err());
    }

    #[test]
    fn durations_format_like_go() {
        assert_eq!(go_duration(0), "0s");
        assert_eq!(go_duration(1), "1ns");
        assert_eq!(go_duration(999), "999ns");
        assert_eq!(go_duration(1000), "1µs");
        assert_eq!(go_duration(1500), "1.5µs");
        assert_eq!(go_duration(1_500_000), "1.5ms");
        assert_eq!(go_duration(1_500_000_000), "1.5s");
        assert_eq!(go_duration(90 * MIN), "1h30m0s");
        assert_eq!(go_duration(25 * HOUR), "25h0m0s");
        assert_eq!(go_duration(-HOUR), "-1h0m0s");
        assert_eq!(go_duration(HOUR + 500 * MS), "1h0m0.5s");
        assert_eq!(go_duration(i64::MIN), "-2562047h47m16.854775808s");
    }

    #[test]
    fn dates_parse() {
        let t = |s: &str| date_from_str(s).map(|d| d.with_timezone(&Utc).to_rfc3339());
        assert_eq!(t("2020-01-01T12:00:00").as_deref(), Some("2020-01-01T12:00:00+00:00"));
        assert_eq!(t("2020-01-01T12:00:00Z").as_deref(), Some("2020-01-01T12:00:00+00:00"));
        assert_eq!(t("2020-01-01T12:00:00+02:00").as_deref(), Some("2020-01-01T10:00:00+00:00"));
        assert_eq!(t("2020-01-01T12:00:00.5Z").as_deref(), Some("2020-01-01T12:00:00.500+00:00"));
        assert_eq!(t("2020-01-01").as_deref(), Some("2020-01-01T00:00:00+00:00"));
        assert_eq!(t("Mon, 02 Jan 2006 15:04:05 MST").as_deref(), Some("2006-01-02T15:04:05+00:00"));
        assert_eq!(t("Mon Jan  2 15:04:05 2006").as_deref(), Some("2006-01-02T15:04:05+00:00"));
        assert_eq!(t("Mon Jan 02 15:04:05 -0700 2006").as_deref(), Some("2006-01-02T22:04:05+00:00"));
        assert_eq!(
            t("2006-01-02 15:04:05.5 +0000 UTC").as_deref(),
            Some("2006-01-02T15:04:05.500+00:00")
        );
        assert_eq!(t("nope"), None);
        assert_eq!(t(""), None);
        assert!(date_from_int(1_774_512_121).is_some());
        assert_eq!(
            date_from_int(1_774_512_121_538).map(|d| d.timestamp_millis()),
            Some(1_774_512_121_538)
        );
        assert!(date_from_int(5).is_none());
    }
}
