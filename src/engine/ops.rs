//! Operator actions: manual invocation and reruns.

use super::Engine;
use super::schedule::{ScheduleOutcome, ScheduleRequest};
use crate::types::*;
use serde_json::{Value, json};
use std::sync::Arc;
use ulid::Ulid;

impl Engine {
    /// Invoke a function by slug with `ev` as the payload. Returns the invocation event id,
    /// or None if no such function exists.
    pub async fn invoke_function(self: &Arc<Self>, slug: &str, ev: Event) -> anyhow::Result<Option<Ulid>> {
        let Some(f) = self.registry.by_slug(slug) else {
            return Ok(None);
        };
        let mut data = ev.data;
        data.remove("_inngest");
        data.insert(
            "_inngest".into(),
            json!({ "fn_id": f.slug, "correlation_id": "", "expire": 0, "gid": "", "name": "",
                    "source_app_id": "", "source_fn_id": "", "source_fn_v": 0 }),
        );
        let tracked = self
            .publish_internal(vec![Event {
                id: ev.id,
                name: EVENT_FN_INVOKED.into(),
                data,
                user: ev.user,
                ts: ev.ts,
                v: ev.v,
                meta: None,
            }])
            .await?;
        Ok(tracked.first().map(|t| t.internal_id))
    }

    /// Rerun a run with the same trigger events. With `from_step`, memoized results of the
    /// steps that ran before it are reused so execution resumes at that step.
    pub async fn rerun(self: &Arc<Self>, run_id: Ulid, from_step: Option<&str>) -> anyhow::Result<Option<Ulid>> {
        let Some(run) = self.meta.get_run(run_id).await? else {
            return Ok(None);
        };
        let Some(f) = self.registry.get(run.fn_id) else {
            anyhow::bail!("function no longer exists");
        };
        let mut events = self.state.load_events(run_id).await?;
        let mut event_ids = match self.state.load_meta(run_id).await? {
            Some(m) => m.event_ids,
            None => vec![],
        };
        if events.is_empty()
            && let Some(eid) = run.event_id
            && let Some(e) = self.meta.get_event(eid).await?
        {
            events = vec![e.event.to_json()];
            event_ids = vec![eid];
        }
        if events.is_empty() {
            anyhow::bail!("trigger events for run {run_id} are no longer available");
        }
        let mut initial_steps: Vec<(String, Value)> = Vec::new();
        if let Some(step) = from_step {
            for (id, v) in self.state.load_steps(run_id).await? {
                if id == step {
                    break;
                }
                initial_steps.push((id, v));
            }
        }
        let outcome = self
            .schedule(
                &f,
                ScheduleRequest {
                    events,
                    event_ids,
                    idempotency_key: Some(Ulid::generate().to_string()),
                    batch_id: run.batch_id,
                    cron: run.cron.clone(),
                    prevent_rate_limit: true,
                    prevent_debounce: true,
                    original_run_id: Some(run_id),
                    initial_steps,
                    ..Default::default()
                },
            )
            .await?;
        Ok(match outcome {
            ScheduleOutcome::Scheduled(id) => Some(id),
            _ => None,
        })
    }
}
