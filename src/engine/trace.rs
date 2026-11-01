//! Run timeline recording for the dashboard: steps, attempts and finalization spans.
//!
//! Spans are written asynchronously in batches so tracing never adds latency to execution.

use super::Engine;
use super::driver::Op;
use crate::store::{MetaStore, Span};
use crate::types::*;
use crate::util::rfc3339_ms;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use ulid::Ulid;

pub const FINALIZATION: &str = "finalization";

pub mod status {
    pub const QUEUED: &str = "QUEUED";
    pub const RUNNING: &str = "RUNNING";
    pub const COMPLETED: &str = "COMPLETED";
    pub const FAILED: &str = "FAILED";
    pub const WAITING: &str = "WAITING";
    pub const CANCELLED: &str = "CANCELLED";
}

enum Msg {
    Span(Box<Span>),
    Started(Ulid, i64),
    /// Answered once everything sent before it is written.
    Flush(oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct Tracer {
    tx: mpsc::UnboundedSender<Msg>,
}

impl Tracer {
    pub fn spawn(meta: Arc<dyn MetaStore>) -> Tracer {
        let (tx, mut rx) = mpsc::unbounded_channel::<Msg>();
        tokio::spawn(async move {
            let mut spans = Vec::new();
            let mut started = Vec::new();
            let mut flushed: Vec<oneshot::Sender<()>> = Vec::new();
            let push = |m: Msg,
                        spans: &mut Vec<Span>,
                        started: &mut Vec<(Ulid, i64)>,
                        flushed: &mut Vec<oneshot::Sender<()>>| match m {
                Msg::Span(s) => spans.push(*s),
                Msg::Started(r, t) => started.push((r, t)),
                Msg::Flush(done) => flushed.push(done),
            };
            while let Some(first) = rx.recv().await {
                push(first, &mut spans, &mut started, &mut flushed);
                // Coalesce whatever else is queued (bounded) into one write.
                let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
                while spans.len() + started.len() < 500 {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(m)) => push(m, &mut spans, &mut started, &mut flushed),
                        _ => break,
                    }
                }
                if let Err(err) = meta.upsert_spans(&spans).await {
                    tracing::warn!(error = %err, "failed to write spans");
                }
                for (run, at) in started.drain(..) {
                    if let Err(err) = meta.mark_run_started(run, at).await {
                        tracing::warn!(error = %err, "failed to mark run started");
                    }
                }
                spans.clear();
                for done in flushed.drain(..) {
                    let _ = done.send(());
                }
            }
        });
        Tracer { tx }
    }

    pub fn span(&self, s: Span) {
        let _ = self.tx.send(Msg::Span(Box::new(s)));
    }

    pub fn run_started(&self, run_id: Ulid, at: i64) {
        let _ = self.tx.send(Msg::Started(run_id, at));
    }

    /// Wait until everything recorded so far is written.
    pub async fn flush(&self) {
        let (done, wait) = oneshot::channel();
        if self.tx.send(Msg::Flush(done)).is_ok() {
            let _ = wait.await;
        }
    }
}

pub fn op_display_name(op: &Op) -> String {
    op.display_name
        .clone()
        .or_else(|| op.name.clone())
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| op.id.clone())
}

/// The dashboard's StepOp for an SDK opcode.
pub fn step_op(op: &str) -> Option<&'static str> {
    Some(match op {
        "StepRun" | "Step" | "StepError" | "StepFailed" | "StepPlanned" => "RUN",
        "Sleep" => "SLEEP",
        "WaitForEvent" => "WAIT_FOR_EVENT",
        "InvokeFunction" => "INVOKE",
        "WaitForSignal" => "WAIT_FOR_SIGNAL",
        "AIGateway" | "Gateway" => "AI_GATEWAY",
        _ => return None,
    })
}

impl Engine {
    /// Base span for a step reported by an op.
    pub fn step_span(&self, run_id: Ulid, op: &Op) -> Span {
        let kind = step_op(&op.op);
        let step_type = op
            .opts
            .as_ref()
            .and_then(|o| o.get("type"))
            .and_then(|t| t.as_str())
            .map(String::from)
            .or_else(|| kind.map(String::from));
        Span {
            run_id,
            span_id: op.id.clone(),
            name: Some(op_display_name(op)),
            op: kind.map(String::from),
            step_id: Some(op.id.clone()),
            step_type,
            ..Default::default()
        }
    }

    /// Record one execution attempt of a step (or of finalization).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn trace_attempt(
        &self,
        run_id: Ulid,
        parent: &str,
        attempt: u32,
        st: &str,
        t0: i64,
        t1: i64,
        output: Option<Value>,
    ) {
        self.trace.span(Span {
            run_id,
            span_id: format!("{parent}:a{attempt}"),
            parent_id: Some(parent.to_string()),
            name: Some(format!("Attempt {attempt}")),
            status: Some(st.into()),
            attempts: Some(attempt as i64 + 1),
            queued_at: Some(t0),
            started_at: Some(t0),
            ended_at: Some(t1),
            output,
            ..Default::default()
        });
    }

    /// Record a step's result from an execution attempt. `QUEUED` means it failed and will retry.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn trace_step_done(
        &self,
        run_id: Ulid,
        item: &QueueItem,
        op: &Op,
        st: &str,
        t0: i64,
        t1: i64,
        value: &Value,
    ) {
        let attempt = item.attempt;
        let (t0, t1) = op.timing_ms().unwrap_or((t0, t1));
        if st != status::COMPLETED || attempt > 0 {
            let attempt_status = if st == status::QUEUED { status::FAILED } else { st };
            self.trace_attempt(run_id, &op.id, attempt, attempt_status, t0, t1, Some(value.clone()));
        }
        let done = st != status::QUEUED;
        self.trace.span(Span {
            status: Some(st.into()),
            attempts: Some(attempt as i64 + 1),
            queued_at: Some(t0),
            started_at: Some(t0),
            ended_at: done.then_some(t1),
            output: done.then(|| value.clone()),
            ..self.step_span(run_id, op)
        });
    }

    /// Record a step that is now waiting (sleep, waitForEvent, invoke, signal).
    pub(crate) fn trace_waiting(&self, run_id: Ulid, op: &Op, info: Value) {
        let now = now_ms();
        self.trace.span(Span {
            status: Some(status::WAITING.into()),
            queued_at: Some(now),
            started_at: Some(now),
            info: Some(info),
            ..self.step_span(run_id, op)
        });
    }

    /// Record the function-level (non-step) outcome of an execution.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn trace_finalization(
        &self,
        run_id: Ulid,
        attempt: u32,
        st: &str,
        final_: bool,
        t0: i64,
        t1: i64,
        output: Value,
    ) {
        if attempt > 0 || st == status::FAILED {
            self.trace_attempt(run_id, FINALIZATION, attempt, st, t0, t1, Some(output.clone()));
        }
        self.trace.span(Span {
            run_id,
            span_id: FINALIZATION.into(),
            name: Some("Finalization".into()),
            step_type: Some("FINALIZATION".into()),
            status: Some(if final_ { st.into() } else { status::QUEUED.into() }),
            attempts: Some(attempt as i64 + 1),
            queued_at: Some(t0),
            started_at: Some(t0),
            ended_at: final_.then_some(t1),
            output: final_.then_some(output),
            ..Default::default()
        });
    }

    /// A waiting step (sleep / wait / invoke / signal) resolved.
    pub(crate) fn trace_resolved(&self, p: &Pause, extra: Value, output: &Value, failed: bool) {
        let mut info = p.span_info.clone().unwrap_or_else(|| json!({}));
        if let (Some(i), Some(e)) = (info.as_object_mut(), extra.as_object()) {
            for (k, v) in e {
                i.insert(k.clone(), v.clone());
            }
        }
        self.trace.span(Span {
            run_id: p.run_id,
            span_id: p.step_id.clone(),
            status: Some(if failed { status::FAILED } else { status::COMPLETED }.into()),
            ended_at: Some(now_ms()),
            info: Some(info),
            output: Some(output.clone()),
            ..Default::default()
        });
    }
}

pub fn ts(ms: i64) -> String {
    rfc3339_ms(ms)
}
