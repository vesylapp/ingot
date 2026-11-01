//! Prometheus metrics, served at `GET /metrics`.
//!
//! `inngest_*` metrics mirror the reference server: `inngest_queue_depth` is always present,
//! and the run / SDK request lifecycle counters are only recorded when
//! `INNGEST_EXPERIMENTAL_PROM_METRICS` is truthy, as in Inngest. `oxidle_*` metrics (and the
//! standard `process_*` metrics on Linux) are always recorded.
//!
//! Counters and histograms are per process; the queue gauges read the shared store, so with
//! several nodes they report the same value on each.

use crate::engine::Engine;
use crate::types::{RunStatus, now_ms};
use prometheus::{
    Encoder, Gauge, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};
use std::sync::LazyLock;

/// How an SDK request ended, with Inngest's labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SdkStatus {
    Success,
    /// A retryable error (including transport errors).
    Errored,
    /// A non-retryable error.
    Failed,
}

impl SdkStatus {
    fn as_str(self) -> &'static str {
        match self {
            SdkStatus::Success => "success",
            SdkStatus::Errored => "errored",
            SdkStatus::Failed => "failed",
        }
    }
}

pub struct Metrics {
    registry: Registry,
    lifecycle: bool,

    queue_depth: IntGauge,
    run_scheduled: IntCounterVec,
    run_started: IntCounterVec,
    run_ended: IntCounterVec,
    sdk_scheduled: IntCounterVec,
    sdk_started: IntCounterVec,
    sdk_ended: IntCounterVec,

    queue_ready: IntGauge,
    queue_lag: Gauge,
    queue_in_progress: IntGaugeVec,
    queue_processed: IntCounterVec,
    sdk_duration: HistogramVec,
    run_duration: HistogramVec,
    events_received: IntCounterVec,
    http_duration: HistogramVec,
    functions: IntGauge,
}

static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::new);

/// The process-wide metrics.
pub fn get() -> &'static Metrics {
    &METRICS
}

/// Same truthiness as Inngest: unset, empty, "0", "false" and "no" are off.
fn env_enabled(name: &str) -> bool {
    let v = std::env::var(name).unwrap_or_default();
    let v = v.trim();
    !(v.is_empty() || v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("no"))
}

fn counter(r: &Registry, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(Opts::new(name, help), labels).expect("valid counter");
    r.register(Box::new(c.clone())).expect("unique metric");
    c
}

fn histogram(r: &Registry, name: &str, help: &str, labels: &[&str], buckets: Vec<f64>) -> HistogramVec {
    let h = HistogramVec::new(HistogramOpts::new(name, help).buckets(buckets), labels)
        .expect("valid histogram");
    r.register(Box::new(h.clone())).expect("unique metric");
    h
}

fn gauge(r: &Registry, name: &str, help: &str) -> IntGauge {
    let g = IntGauge::new(name, help).expect("valid gauge");
    r.register(Box::new(g.clone())).expect("unique metric");
    g
}

impl Metrics {
    fn new() -> Metrics {
        let r = Registry::new();
        // Run durations include sleeps and waits, so the buckets reach a day.
        let run_buckets = vec![
            0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3600.0, 21600.0,
            86400.0,
        ];
        let request_buckets = prometheus::DEFAULT_BUCKETS.to_vec();

        let queue_lag = Gauge::new(
            "oxidle_queue_lag_seconds",
            "Age of the oldest queue item that is due but not yet picked up (0 when none)",
        )
        .expect("valid gauge");
        r.register(Box::new(queue_lag.clone())).expect("unique metric");
        let queue_in_progress = IntGaugeVec::new(
            Opts::new(
                "oxidle_queue_in_progress",
                "Queue items this node is processing right now",
            ),
            &["kind"],
        )
        .expect("valid gauge");
        r.register(Box::new(queue_in_progress.clone())).expect("unique metric");
        let build_info = IntGaugeVec::new(
            Opts::new("oxidle_build_info", "Always 1; labelled with the oxidle version"),
            &["version"],
        )
        .expect("valid gauge");
        r.register(Box::new(build_info.clone())).expect("unique metric");
        build_info.with_label_values(&[env!("CARGO_PKG_VERSION")]).set(1);

        #[cfg(target_os = "linux")]
        r.register(Box::new(prometheus::process_collector::ProcessCollector::for_self()))
            .expect("unique metric");

        Metrics {
            lifecycle: env_enabled("INNGEST_EXPERIMENTAL_PROM_METRICS"),
            queue_depth: gauge(
                &r,
                "inngest_queue_depth",
                "Total depth of all system queues including backlog and ready state items",
            ),
            run_scheduled: counter(
                &r,
                "inngest_function_run_scheduled_total",
                "The total number of function runs scheduled",
                &["fn"],
            ),
            run_started: counter(
                &r,
                "inngest_function_run_started_total",
                "The total number of function runs started",
                &["fn"],
            ),
            run_ended: counter(
                &r,
                "inngest_function_run_ended_total",
                "The total number of function runs ended",
                &["fn", "status"],
            ),
            sdk_scheduled: counter(
                &r,
                "inngest_sdk_req_scheduled_total",
                "The total number of SDK invocation/step execution scheduled",
                &["fn"],
            ),
            sdk_started: counter(
                &r,
                "inngest_sdk_req_started_total",
                "The total number of SDK invocation/step execution started",
                &["fn"],
            ),
            sdk_ended: counter(
                &r,
                "inngest_sdk_req_ended_total",
                "The total number of SDK invocation/step execution ended",
                &["fn", "status"],
            ),
            queue_ready: gauge(
                &r,
                "oxidle_queue_ready_items",
                "Queue items that are due now and waiting for a worker",
            ),
            queue_lag,
            queue_in_progress,
            queue_processed: counter(
                &r,
                "oxidle_queue_items_processed_total",
                "Queue items processed by this node, by kind and outcome (done, retry, delay, error)",
                &["kind", "outcome"],
            ),
            sdk_duration: histogram(
                &r,
                "oxidle_sdk_request_duration_seconds",
                "Duration of requests to SDK apps, by function and outcome",
                &["fn", "status"],
                request_buckets.clone(),
            ),
            run_duration: histogram(
                &r,
                "oxidle_function_run_duration_seconds",
                "Time from a run being scheduled to it ending, by function and status",
                &["fn", "status"],
                run_buckets,
            ),
            events_received: counter(
                &r,
                "oxidle_events_received_total",
                "Events received, by source (api, internal, cron)",
                &["source"],
            ),
            http_duration: histogram(
                &r,
                "oxidle_http_request_duration_seconds",
                "HTTP requests served, by method, route and status code",
                &["method", "route", "status"],
                request_buckets,
            ),
            functions: gauge(&r, "oxidle_functions", "Functions currently registered"),
            registry: r,
        }
    }

    /// Whether the Inngest lifecycle counters are recorded.
    pub fn lifecycle_enabled(&self) -> bool {
        self.lifecycle
    }

    pub fn run_scheduled(&self, function: &str) {
        if self.lifecycle {
            self.run_scheduled.with_label_values(&[function]).inc();
        }
    }

    pub fn run_started(&self, function: &str) {
        if self.lifecycle {
            self.run_started.with_label_values(&[function]).inc();
        }
    }

    /// A run reached a terminal status after `duration_ms` since it was scheduled.
    pub fn run_ended(&self, function: &str, status: RunStatus, duration_ms: i64) {
        let status = match status {
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
            _ => return,
        };
        if self.lifecycle {
            self.run_ended.with_label_values(&[function, status]).inc();
        }
        self.run_duration
            .with_label_values(&[function, status])
            .observe(duration_ms.max(0) as f64 / 1000.0);
    }

    /// An SDK request was queued: a follow-up step, a sleep wake-up or a retry.
    pub fn sdk_request_scheduled(&self, function: &str) {
        if self.lifecycle {
            self.sdk_scheduled.with_label_values(&[function]).inc();
        }
    }

    pub fn sdk_request_started(&self, function: &str) {
        if self.lifecycle {
            self.sdk_started.with_label_values(&[function]).inc();
        }
    }

    pub fn sdk_request_ended(&self, function: &str, status: SdkStatus, duration_ms: i64) {
        if self.lifecycle {
            self.sdk_ended.with_label_values(&[function, status.as_str()]).inc();
        }
        self.sdk_duration
            .with_label_values(&[function, status.as_str()])
            .observe(duration_ms.max(0) as f64 / 1000.0);
    }

    /// Track a queue item being processed; the gauge drops when the guard does.
    pub fn queue_item_started(&self, kind: &str) -> InProgress {
        let g = self.queue_in_progress.with_label_values(&[kind]);
        g.inc();
        InProgress(g)
    }

    pub fn queue_item_processed(&self, kind: &str, outcome: &str) {
        self.queue_processed.with_label_values(&[kind, outcome]).inc();
    }

    pub fn events_received(&self, source: &str, n: usize) {
        self.events_received
            .with_label_values(&[source])
            .inc_by(n as u64);
    }

    pub fn http_request(&self, method: &str, route: &str, status: u16, seconds: f64) {
        self.http_duration
            .with_label_values(&[method, route, &status.to_string()])
            .observe(seconds);
    }

    /// Refresh the store-backed gauges and encode everything in the Prometheus text format.
    pub async fn render(&self, engine: &Engine) -> anyhow::Result<String> {
        let now = now_ms();
        let q = engine.state.queue_stats(now).await?;
        self.queue_depth.set(q.total);
        self.queue_ready.set(q.ready);
        let lag_ms = q.oldest_at.map_or(0, |at| (now - at).max(0));
        self.queue_lag.set(lag_ms as f64 / 1000.0);
        self.functions.set(engine.registry.len() as i64);

        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf)?;
        Ok(String::from_utf8(buf)?)
    }
}

/// Decrements `oxidle_queue_in_progress` when dropped.
pub struct InProgress(IntGauge);

impl Drop for InProgress {
    fn drop(&mut self) {
        self.0.dec();
    }
}
