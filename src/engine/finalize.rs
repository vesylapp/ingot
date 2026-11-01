//! Finishing runs: state transition, cleanup, history and lifecycle events.

use super::Engine;
use crate::types::*;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use ulid::Ulid;

impl Engine {
    /// Finish a run. Idempotent: only the first call for a run has any effect.
    pub async fn finalize(
        self: &Arc<Self>,
        meta: &RunMeta,
        events: &[Value],
        status: RunStatus,
        result: Option<Value>,
        error: Option<Value>,
        current_item: Option<&str>,
    ) -> anyhow::Result<()> {
        let now = now_ms();
        if !self.state.end_run(meta.run_id, status, now).await? {
            return Ok(());
        }
        self.state.remove_run_items(meta.run_id, current_item).await?;
        self.state.delete_run_pauses(meta.run_id).await?;
        if let Some(k) = &meta.singleton_key {
            self.state.kv_del(k, Some(&meta.run_id.to_string())).await?;
        }
        let output = match (&result, &error) {
            (Some(r), _) => r.clone(),
            (None, Some(e)) => e.clone(),
            _ => json!({}),
        };
        self.meta.finish_run(meta.run_id, status, &output, now).await?;
        tracing::info!(run_id = %meta.run_id, function = %meta.fn_slug, status = status.as_str(), "run finished");

        if meta.suppress_lifecycle {
            return Ok(());
        }
        let first = events.first().cloned().unwrap_or_else(|| json!({}));
        let correlation_id = first
            .get("data")
            .and_then(|d| d.get("_inngest"))
            .and_then(|i| i.get("correlation_id"))
            .and_then(|c| c.as_str())
            .map(String::from);
        let mut data = Map::new();
        data.insert("function_id".into(), Value::String(meta.fn_slug.clone()));
        data.insert("run_id".into(), Value::String(meta.run_id.to_string()));
        data.insert("event".into(), first);
        data.insert("events".into(), Value::Array(events.to_vec()));
        match status {
            RunStatus::Completed => {
                data.insert("result".into(), result.unwrap_or(Value::Null));
            }
            RunStatus::Failed => {
                data.insert("error".into(), error.unwrap_or_else(|| json!({})));
            }
            _ => {}
        }
        if let Some(cid) = &correlation_id {
            data.insert("correlation_id".into(), Value::String(cid.clone()));
        }
        data.insert("_inngest".into(), json!({ "status": status.as_str() }));

        let mut out = vec![Event {
            id: Ulid::generate().to_string(),
            name: EVENT_FN_FINISHED.into(),
            data: data.clone(),
            user: None,
            ts: now,
            v: None,
            meta: None,
        }];
        let extra = match status {
            RunStatus::Failed => Some(EVENT_FN_FAILED),
            RunStatus::Cancelled => Some(EVENT_FN_CANCELLED),
            _ => None,
        };
        if let Some(name) = extra {
            out.push(Event {
                id: meta.run_id.to_string(),
                name: name.into(),
                data,
                user: None,
                ts: now,
                v: None,
                meta: None,
            });
        }
        self.publish_internal(out).await?;
        Ok(())
    }

    /// Cancel a run by id. Returns false if the run does not exist.
    pub async fn cancel_run(self: &Arc<Self>, run_id: Ulid) -> anyhow::Result<bool> {
        let Some(meta) = self.state.load_meta(run_id).await? else {
            return Ok(false);
        };
        let events = self.state.load_events(run_id).await?;
        self.cancel_run_with(&meta, &events, None).await?;
        Ok(true)
    }

    pub async fn cancel_run_with(
        self: &Arc<Self>,
        meta: &RunMeta,
        events: &[Value],
        current_item: Option<&str>,
    ) -> anyhow::Result<()> {
        self.finalize(meta, events, RunStatus::Cancelled, None, None, current_item)
            .await
    }
}
