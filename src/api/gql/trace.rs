//! Building the dashboard's run trace tree from recorded spans.

use super::types::*;
use crate::engine::Engine;
use crate::engine::trace::FINALIZATION;
use crate::store::Span;
use crate::types as t;
use serde_json::Value;
use ulid::Ulid as RawUlid;

const ROOT_OUTPUT: &str = "root";

fn output_id(run: RawUlid, span: &str) -> String {
    format!("{run}:{span}")
}

fn str_field(v: Option<&Value>, key: &str) -> Option<String> {
    v.and_then(|i| i.get(key))
        .and_then(|x| x.as_str())
        .map(String::from)
}

fn bool_field(v: Option<&Value>, key: &str) -> Option<bool> {
    v.and_then(|i| i.get(key)).and_then(|x| x.as_bool())
}

fn step_info(op: Option<StepOp>, step_type: &str, info: Option<&Value>) -> Option<StepInfo> {
    let timeout = || Time(str_field(info, "timeout").unwrap_or_default());
    let ulid = |k: &str| str_field(info, k).map(Ulid);
    Some(match op? {
        StepOp::Run => StepInfo::RunStepInfo(RunStepInfo {
            kind: Some(step_type.to_string()).filter(|s| s != "RUN"),
        }),
        StepOp::Sleep => StepInfo::SleepStepInfo(SleepStepInfo {
            sleep_until: Time(str_field(info, "sleepUntil")?),
        }),
        StepOp::WaitForEvent => StepInfo::WaitForEventStepInfo(WaitForEventStepInfo {
            event_name: str_field(info, "eventName").unwrap_or_default(),
            expression: str_field(info, "expression"),
            timeout: timeout(),
            found_event_id: ulid("foundEventID"),
            timed_out: bool_field(info, "timedOut"),
        }),
        StepOp::Invoke => StepInfo::InvokeStepInfo(InvokeStepInfo {
            triggering_event_id: Ulid(str_field(info, "triggeringEventID").unwrap_or_default()),
            function_id: str_field(info, "functionID").unwrap_or_default(),
            timeout: timeout(),
            return_event_id: ulid("returnEventID"),
            run_id: ulid("runID"),
            timed_out: bool_field(info, "timedOut"),
        }),
        StepOp::WaitForSignal => StepInfo::WaitForSignalStepInfo(WaitForSignalStepInfo {
            signal: str_field(info, "signal").unwrap_or_default(),
            timeout: timeout(),
            timed_out: bool_field(info, "timedOut"),
        }),
        StepOp::AiGateway => return None,
    })
}

struct Ctx<'a> {
    run: &'a t::RunRecord,
    terminal: bool,
}

impl Ctx<'_> {
    fn base(&self, span_id: String, name: String, status: RunTraceSpanStatus, queued_at: i64) -> RunTraceSpan {
        RunTraceSpan {
            app_id: Uuid(self.run.app_id.to_string()),
            function_id: Uuid(self.run.fn_id.to_string()),
            run_id: Ulid(self.run.run_id.to_string()),
            span_id,
            trace_id: self.run.run_id.to_string(),
            group_id: None,
            name,
            status,
            attempts: None,
            duration: None,
            output_id: None,
            queued_at: time(queued_at),
            scheduled_at: None,
            started_at: None,
            ended_at: None,
            children_spans: vec![],
            step_op: None,
            step_id: None,
            step_info: None,
            step_type: String::new(),
            group_kind: None,
            origin: None,
            is_root: false,
            parent_span_id: None,
            is_userland: false,
            userland_span: None,
            debug_run_id: None,
            debug_session_id: None,
            debug_paused: false,
            skip_reason: None,
            skip_existing_run_id: None,
            custom_concurrency_keys: vec![],
            metadata: vec![],
            response: None,
        }
    }

    fn convert(&self, s: &Span, parent: &str) -> RunTraceSpan {
        let mut status = RunTraceSpanStatus::parse(s.status.as_deref().unwrap_or("RUNNING"));
        // Steps still open when the run ended were abandoned.
        if self.terminal && status.is_open() {
            status = RunTraceSpanStatus::Cancelled;
        }
        let op = s.op.as_deref().and_then(StepOp::parse);
        let step_type = s.step_type.clone().unwrap_or_default();
        let queued = s.queued_at.or(s.started_at).unwrap_or(self.run.started_at);
        let ended = if status.is_open() { None } else { s.ended_at };
        let mut out = self.base(
            s.span_id.clone(),
            s.name.clone().unwrap_or_else(|| s.span_id.clone()),
            status,
            queued,
        );
        out.attempts = s.attempts.map(|a| a as i32);
        out.started_at = s.started_at.map(time);
        out.ended_at = ended.map(time);
        out.duration = match (s.started_at, ended) {
            (Some(a), Some(b)) => Some((b - a) as i32),
            _ => None,
        };
        out.output_id = s.output.as_ref().map(|_| output_id(s.run_id, &s.span_id));
        out.step_op = op;
        out.step_id = s.step_id.clone();
        out.step_info = step_info(op, &step_type, s.info.as_ref());
        out.step_type = step_type;
        out.parent_span_id = Some(parent.to_string());
        out
    }
}

/// Build the run trace tree.
pub async fn build(e: &Engine, run: &t::RunRecord) -> anyhow::Result<RunTraceSpan> {
    let spans = e.meta.run_spans(run.run_id).await?;
    let status = match super::run_status(run) {
        FunctionRunStatus::Completed => RunTraceSpanStatus::Completed,
        FunctionRunStatus::Failed => RunTraceSpanStatus::Failed,
        FunctionRunStatus::Cancelled => RunTraceSpanStatus::Cancelled,
        FunctionRunStatus::Skipped => RunTraceSpanStatus::Skipped,
        FunctionRunStatus::Queued => RunTraceSpanStatus::Queued,
        FunctionRunStatus::Running => RunTraceSpanStatus::Running,
    };
    let ctx = Ctx {
        run,
        terminal: run.status.is_terminal(),
    };
    let root_id = run.run_id.to_string();
    let mut root = ctx.base(root_id.clone(), "Run".into(), status, run.started_at);
    root.is_root = true;
    root.started_at = run.exec_started_at.map(time);
    root.ended_at = run.ended_at.map(time);
    root.duration = match (run.exec_started_at, run.ended_at) {
        (Some(a), Some(b)) => Some((b - a) as i32),
        _ => None,
    };
    if run.status.is_terminal() && run.output.is_some() {
        root.output_id = Some(output_id(run.run_id, ROOT_OUTPUT));
    }

    // The UI rolls attempts up itself: it expects one flat child per execution attempt,
    // keyed by stepID (steps) or groupID (finalization), with a 0-based `attempts` number.
    let attempt_index = |a: &Span| -> i32 {
        a.span_id
            .rsplit_once(":a")
            .and_then(|(_, n)| n.parse().ok())
            .unwrap_or_else(|| a.attempts.map(|x| x as i32 - 1).unwrap_or(0))
    };
    let mut steps: Vec<RunTraceSpan> = Vec::new();
    let mut finals: Vec<RunTraceSpan> = Vec::new();
    for s in spans.iter().filter(|s| s.parent_id.is_none()) {
        let is_final = s.span_id == FINALIZATION;
        let mut attempts: Vec<&Span> = spans
            .iter()
            .filter(|c| c.parent_id.as_deref() == Some(s.span_id.as_str()))
            .collect();
        attempts.sort_by_key(|a| attempt_index(a));
        let shape = |mut node: RunTraceSpan, idx: i32| {
            node.attempts = Some(idx);
            if is_final {
                node.step_id = None;
                node.step_op = None;
                node.step_info = None;
                node.group_id = Some(FINALIZATION.to_string());
            } else {
                node.step_id = Some(s.step_id.clone().unwrap_or_else(|| s.span_id.clone()));
            }
            node
        };
        let out = if is_final { &mut finals } else { &mut steps };
        if attempts.is_empty() {
            let idx = s.attempts.map(|a| (a as i32 - 1).max(0)).unwrap_or(0);
            out.push(shape(ctx.convert(s, &root_id), idx));
            continue;
        }
        let step_node = ctx.convert(s, &root_id);
        let mut last_idx = 0;
        for a in &attempts {
            let mut node = ctx.convert(a, &root_id);
            node.name = step_node.name.clone();
            node.step_op = step_node.step_op;
            node.step_info = step_node.step_info.clone();
            node.step_type = step_node.step_type.clone();
            last_idx = attempt_index(a);
            out.push(shape(node, last_idx));
        }
        // A retry that is queued or in flight shows as the next attempt.
        if step_node.status.is_open() {
            let mut node = step_node;
            node.span_id = format!("{}:next", s.span_id);
            node.output_id = None;
            node.ended_at = None;
            out.push(shape(node, last_idx + 1));
        }
    }
    steps.sort_by(|a, b| a.queued_at.0.cmp(&b.queued_at.0));
    root.children_spans = steps;
    root.children_spans.extend(finals);
    Ok(root)
}

fn to_step_error(e: &Value) -> StepError {
    let s = |k: &str| e.get(k).and_then(|v| v.as_str()).map(String::from);
    StepError {
        message: s("message")
            .or_else(|| s("error"))
            .unwrap_or_else(|| e.to_string()),
        name: s("name"),
        stack: s("stack"),
        cause: e.get("cause").cloned().map(Unknown),
    }
}

fn split_output(v: &Value) -> RunTraceSpanOutput {
    let mut out = RunTraceSpanOutput::default();
    match v {
        Value::Object(m) if m.contains_key("error") && !m["error"].is_null() => {
            out.error = Some(to_step_error(&m["error"]));
        }
        Value::Object(m) if m.contains_key("data") || m.contains_key("input") => {
            out.data = m.get("data").map(|d| Bytes(super::json_string(d)));
            out.input = m.get("input").map(|d| Bytes(super::json_string(d)));
        }
        other => out.data = Some(Bytes(super::json_string(other))),
    }
    out
}

/// Resolve an `outputID` produced by [`build`].
pub async fn output(e: &Engine, id: &str) -> anyhow::Result<RunTraceSpanOutput> {
    let (run, span) = id
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid output id"))?;
    let run_id = RawUlid::from_string(run)?;
    if span == ROOT_OUTPUT {
        let Some(r) = e.meta.get_run(run_id).await? else {
            return Ok(RunTraceSpanOutput::default());
        };
        let out = r.output.unwrap_or(Value::Null);
        return Ok(if r.status == t::RunStatus::Failed {
            RunTraceSpanOutput {
                error: Some(to_step_error(&out)),
                ..Default::default()
            }
        } else {
            RunTraceSpanOutput {
                data: Some(Bytes(super::json_string(&out))),
                ..Default::default()
            }
        });
    }
    let spans = e.meta.run_spans(run_id).await?;
    Ok(spans
        .iter()
        .find(|s| s.span_id == span)
        .and_then(|s| s.output.as_ref())
        .map(split_output)
        .unwrap_or_default())
}

/// The events that triggered a run.
pub async fn trigger(e: &Engine, run_id: RawUlid) -> anyhow::Result<RunTraceTrigger> {
    let run = e
        .meta
        .get_run(run_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("run not found"))?;
    let mut events = e.state.load_events(run_id).await?;
    let mut ids: Vec<RawUlid> = match e.state.load_meta(run_id).await? {
        Some(m) => m.event_ids,
        None => vec![],
    };
    if events.is_empty()
        && let Some(eid) = run.event_id
        && let Some(ev) = e.meta.get_event(eid).await?
    {
        events = vec![ev.event.to_json()];
        ids = vec![eid];
    }
    if ids.is_empty() {
        ids.extend(run.event_id);
    }
    let first = events.first();
    let ts = first
        .and_then(|e| e.get("ts"))
        .and_then(|t| t.as_i64())
        .unwrap_or(run.started_at);
    Ok(RunTraceTrigger {
        event_name: if run.cron.is_some() {
            None
        } else {
            first
                .and_then(|e| e.get("name"))
                .and_then(|n| n.as_str())
                .map(String::from)
        },
        ids: ids.iter().map(|i| Ulid(i.to_string())).collect(),
        payloads: events.iter().map(|e| Bytes(super::json_string(e))).collect(),
        timestamp: time(ts),
        is_batch: run.batch_id.is_some(),
        batch_id: run.batch_id.map(|b| Ulid(b.to_string())),
        cron: run.cron.clone(),
    })
}
