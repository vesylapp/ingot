//! Dashboard run search: CEL expressions over each run's trigger event and result.
//!
//! A query holds one expression per line, and a run matches when every line is true. The
//! variables are `event` (the run's first trigger event), `output` (the run's output once it
//! has finished) and `error` (the output of a failed run). A run whose expression fails to
//! evaluate does not match.
//!
//! When the query requires `event.name` to be one of a few literal names, that part runs in
//! SQL. If that is all the query says, listing and counting are plain indexed queries.
//! Anything else is evaluated here, over runs read from the store in pages; how many runs
//! one request reads is bounded, so a broad search over a long time range answers in pieces
//! rather than reading the whole history at once.

use super::{MetaStore, RunQuery, RunTimeField};
use crate::types::{RunRecord, RunStatus};
use anyhow::Result;
use ingot_expr::{Expression, Vars};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};
use ulid::Ulid;

/// Runs read from the store per round trip while evaluating.
const SCAN_CHUNK: i64 = 500;
/// Most runs one page request reads before returning what it has found so far.
pub const PAGE_SCAN_LIMIT: usize = 20_000;
/// Most runs a count reads before giving up.
pub const COUNT_SCAN_LIMIT: usize = 100_000;

/// The query was not valid CEL. The message is meant for the person who typed it.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct InvalidQuery(pub String);

/// A count that would read more runs than [`COUNT_SCAN_LIMIT`].
#[derive(Debug, thiserror::Error)]
#[error("too many runs to count for this search; narrow the time range or filter by event.name")]
pub struct CountTooBroad;

#[derive(Debug)]
pub struct RunSearch {
    exprs: Vec<Expression>,
    /// Required trigger event names, when the query pins them down.
    names: Option<Vec<String>>,
    /// The name requirement is the whole query; nothing needs evaluating.
    names_exact: bool,
    uses_event: bool,
    /// Runs read per round trip.
    chunk: i64,
}

/// One page of matching runs.
pub struct SearchPage {
    pub runs: Vec<RunRecord>,
    /// Set when the page stopped at the scan limit before finding enough runs: the keyset
    /// position (time, run_id) of the last run read, to continue from.
    pub resume: Option<(i64, Ulid)>,
}

impl RunSearch {
    /// Parses a dashboard query. Blank lines are ignored; `Ok(None)` if nothing is left.
    pub fn parse(query: &str) -> Result<Option<RunSearch>, InvalidQuery> {
        let mut exprs = Vec::new();
        for line in query.lines().map(str::trim).filter(|l| !l.is_empty()) {
            let e = Expression::compile_run_search(line).map_err(|err| {
                let msg = err.to_string();
                let msg = msg.strip_prefix("compile error: ").unwrap_or(&msg);
                InvalidQuery(format!("{msg}\n | {line}"))
            })?;
            exprs.push(e);
        }
        if exprs.is_empty() {
            return Ok(None);
        }

        // Every line must hold, so the allowed names are the intersection over the lines that
        // constrain them, and only exact if every line is a pure name check.
        let mut names: Option<BTreeSet<String>> = None;
        let mut names_exact = true;
        for e in &exprs {
            match e.event_names() {
                Some(n) => {
                    names_exact &= n.exact;
                    let set: BTreeSet<String> = n.names.into_iter().collect();
                    names = Some(match names {
                        Some(prev) => prev.intersection(&set).cloned().collect(),
                        None => set,
                    });
                }
                None => names_exact = false,
            }
        }
        let uses_event = exprs.iter().any(|e| e.references_event());
        Ok(Some(RunSearch {
            exprs,
            names: names.map(|n| n.into_iter().collect()),
            names_exact,
            uses_event,
            chunk: SCAN_CHUNK,
        }))
    }

    /// Reads `n` runs per round trip instead of the default (for tests).
    #[doc(hidden)]
    pub fn with_scan_chunk(mut self, n: i64) -> Self {
        self.chunk = n.max(1);
        self
    }

    /// Adds the part of the search that SQL can answer to `q`.
    pub fn narrow(&self, q: &mut RunQuery) {
        if let Some(names) = &self.names {
            q.event_names = Some(match q.event_names.take() {
                Some(prev) => prev.into_iter().filter(|n| names.contains(n)).collect(),
                None => names.clone(),
            });
        }
    }

    /// True if SQL alone answers the search (after [`RunSearch::narrow`]).
    pub fn is_sql_only(&self) -> bool {
        self.names.is_some() && self.names_exact
    }

    /// Up to `q.limit` matching runs, in the order and from the position `q` describes.
    pub async fn list(
        &self,
        meta: &dyn MetaStore,
        q: &RunQuery,
        scan_limit: usize,
    ) -> Result<SearchPage> {
        let mut q = q.clone();
        self.narrow(&mut q);
        if self.is_sql_only() {
            return Ok(SearchPage {
                runs: meta.list_runs(&q).await?,
                resume: None,
            });
        }
        let want = q.limit.max(1) as usize;
        let mut runs = Vec::new();
        let resume = self
            .scan(meta, &mut q, scan_limit, |r| {
                runs.push(r.clone());
                runs.len() < want
            })
            .await?;
        Ok(SearchPage { runs, resume })
    }

    /// The number of matching runs; fails with [`CountTooBroad`] past [`COUNT_SCAN_LIMIT`].
    pub async fn count(&self, meta: &dyn MetaStore, q: &RunQuery) -> Result<i64> {
        let mut q = q.clone();
        self.narrow(&mut q);
        if self.is_sql_only() {
            return meta.count_runs(&q).await;
        }
        if meta.count_runs(&q).await? > COUNT_SCAN_LIMIT as i64 {
            return Err(CountTooBroad.into());
        }
        // Counts ignore page positions, like `count_runs`.
        q.cursor = None;
        q.page_cursor = None;
        let mut n = 0;
        self.scan(meta, &mut q, usize::MAX, |_| {
            n += 1;
            true
        })
        .await?;
        Ok(n)
    }

    /// Reads runs from `q`'s position onwards, calling `on_match` with each match until it
    /// returns false. Stops after reading `scan_limit` runs (rounded up to a whole chunk) and
    /// returns the position to continue from, or `None` once the runs are exhausted or
    /// `on_match` has had enough.
    async fn scan(
        &self,
        meta: &dyn MetaStore,
        q: &mut RunQuery,
        scan_limit: usize,
        mut on_match: impl FnMut(&RunRecord) -> bool,
    ) -> Result<Option<(i64, Ulid)>> {
        q.limit = self.chunk.min(scan_limit.max(1) as i64);
        let mut scanned = 0;
        loop {
            let rows = meta.list_runs(q).await?;
            let Some(last) = rows.last() else {
                return Ok(None);
            };
            let next = (time_of(last, q.time_field), last.run_id);
            scanned += rows.len();
            for (r, matched) in rows.iter().zip(self.filter(meta, &rows).await?) {
                if matched && !on_match(r) {
                    return Ok(None);
                }
            }
            if (rows.len() as i64) < q.limit {
                return Ok(None);
            }
            if scanned >= scan_limit {
                return Ok(Some(next));
            }
            q.page_cursor = Some(next);
        }
    }

    /// Whether each run matches.
    async fn filter(&self, meta: &dyn MetaStore, runs: &[RunRecord]) -> Result<Vec<bool>> {
        let events: HashMap<Ulid, Value> = if self.uses_event {
            let ids: Vec<Ulid> = runs.iter().filter_map(|r| r.event_id).collect();
            meta.get_events(&ids)
                .await?
                .into_iter()
                .map(|e| {
                    (
                        e.internal_id,
                        serde_json::to_value(&e.event).unwrap_or(Value::Null),
                    )
                })
                .collect()
        } else {
            HashMap::new()
        };
        Ok(runs
            .iter()
            .map(|r| {
                // A run whose event has been pruned still knows the event's name.
                let fallback = r.event_name.as_ref().map(|n| json!({ "name": n }));
                let event = r
                    .event_id
                    .and_then(|id| events.get(&id))
                    .or(fallback.as_ref());
                self.matches(event, r)
            })
            .collect())
    }

    fn matches(&self, event: Option<&Value>, r: &RunRecord) -> bool {
        let vars = Vars {
            event,
            output: r.output.as_ref(),
            error: r.output.as_ref().filter(|_| r.status == RunStatus::Failed),
            ..Default::default()
        };
        self.exprs
            .iter()
            .all(|e| e.eval_bool(vars).unwrap_or(false))
    }
}

/// The run's value of the column `field` orders by (as the keyset cursor uses it).
fn time_of(r: &RunRecord, field: RunTimeField) -> i64 {
    match field {
        RunTimeField::Queued => Some(r.started_at),
        RunTimeField::Started => r.exec_started_at,
        RunTimeField::Ended => r.ended_at,
    }
    .unwrap_or(0)
}
