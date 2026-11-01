//! Log output: human-readable text, or one JSON object per line.
//!
//! JSON lines carry Google Cloud Logging's special fields, so GKE picks up the severity and
//! summary line without parsing rules:
//!
//! ```json
//! {"time":"2026-11-01T12:00:00.000000Z","severity":"WARNING","level":"WARN",
//!  "message":"run finished","target":"oxidle::engine::finalize","run_id":"01J..."}
//! ```
//!
//! `level` keeps the original level name for filters written against it; event fields are
//! flattened to the top level, as in Inngest's JSON logs.

use serde_json::{Map, Value};
use std::fmt;
use std::io::IsTerminal;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

pub fn init(json: bool, filter: EnvFilter) {
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .event_format(JsonLine)
            .init();
    } else {
        // No color codes when logs go to a file or container log driver.
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(std::io::stdout().is_terminal())
            .init();
    }
}

/// Cloud Logging's name for a level.
fn severity(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARNING",
        Level::INFO => "INFO",
        Level::DEBUG | Level::TRACE => "DEBUG",
    }
}

pub struct JsonLine;

impl<S, N> FormatEvent<S, N> for JsonLine
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(&self, _ctx: &FmtContext<'_, S, N>, mut writer: Writer<'_>, event: &Event<'_>) -> fmt::Result {
        let meta = event.metadata();
        let mut fields = Fields(Map::new());
        event.record(&mut fields);
        let mut fields = fields.0;

        let mut line = Map::new();
        line.insert(
            "time".into(),
            chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
                .into(),
        );
        line.insert("severity".into(), severity(meta.level()).into());
        line.insert("level".into(), meta.level().as_str().into());
        line.insert(
            "message".into(),
            fields.remove("message").unwrap_or(Value::String(String::new())),
        );
        line.insert("target".into(), meta.target().into());
        for (k, v) in fields {
            line.entry(k).or_insert(v);
        }
        writeln!(writer, "{}", Value::Object(line))
    }
}

/// Collects an event's fields as JSON values.
struct Fields(Map<String, Value>);

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.0.insert(field.name().into(), value.to_string().into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}").into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn json_lines_carry_cloud_logging_fields() {
        let buf = Buf::default();
        let out = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .event_format(JsonLine)
            .with_writer(move || out.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(run_id = "01J", attempts = 3, ok = false, "run finished");
            tracing::error!(error = %"boom", "queue item failed");
        });
        let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<Value> = text.lines().map(|l| serde_json::from_str(l).unwrap()).collect();

        assert_eq!(lines[0]["severity"], "WARNING");
        assert_eq!(lines[0]["level"], "WARN");
        assert_eq!(lines[0]["message"], "run finished");
        assert_eq!(lines[0]["run_id"], "01J");
        assert_eq!(lines[0]["attempts"], 3);
        assert_eq!(lines[0]["ok"], false);
        assert!(lines[0]["time"].as_str().unwrap().ends_with('Z'));
        assert!(lines[0]["target"].as_str().unwrap().starts_with("oxidle::logging"));

        assert_eq!(lines[1]["severity"], "ERROR");
        assert_eq!(lines[1]["error"], "boom");
    }
}
