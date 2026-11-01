//! Cron expression parsing with optional `TZ=`/`CRON_TZ=` prefix.

use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct CronSchedule {
    cron: Cron,
    tz: Tz,
}

impl CronSchedule {
    pub fn parse(spec: &str) -> Result<CronSchedule, String> {
        let spec = spec.trim();
        let (tz, rest) = if let Some(r) = spec
            .strip_prefix("TZ=")
            .or_else(|| spec.strip_prefix("CRON_TZ="))
        {
            let (tz, rest) = r
                .split_once(char::is_whitespace)
                .ok_or_else(|| "missing cron spec after timezone".to_string())?;
            let tz = Tz::from_str(tz).map_err(|_| format!("unknown timezone: {tz}"))?;
            (tz, rest.trim())
        } else {
            (Tz::UTC, spec)
        };
        let fields = rest.split_whitespace().count();
        if !rest.starts_with('@') && fields != 5 {
            return Err(format!("expected 5 fields, found {fields}"));
        }
        let cron = Cron::from_str(rest).map_err(|e| e.to_string())?;
        Ok(CronSchedule { cron, tz })
    }

    /// Next fire time strictly after `after_ms`.
    pub fn next_after(&self, after_ms: i64) -> Option<i64> {
        let after: DateTime<Utc> = DateTime::from_timestamp_millis(after_ms)?;
        let local = self.tz.from_utc_datetime(&after.naive_utc());
        let next = self.cron.find_next_occurrence(&local, false).ok()?;
        Some(next.with_timezone(&Utc).timestamp_millis())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_next() {
        let c = CronSchedule::parse("*/5 * * * *").unwrap();
        let t = DateTime::parse_from_rfc3339("2026-01-01T00:01:00Z").unwrap().timestamp_millis();
        let n = c.next_after(t).unwrap();
        assert_eq!(
            DateTime::from_timestamp_millis(n).unwrap().to_rfc3339(),
            "2026-01-01T00:05:00+00:00"
        );
        let c = CronSchedule::parse("TZ=America/New_York 0 9 * * *").unwrap();
        let n = c.next_after(t).unwrap();
        assert_eq!(
            DateTime::from_timestamp_millis(n).unwrap().to_rfc3339(),
            "2026-01-01T14:00:00+00:00"
        );
        assert!(CronSchedule::parse("* * * *").is_err());
    }
}
