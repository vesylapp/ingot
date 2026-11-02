//! Dashboard run lists: keyset pagination returns every run exactly once, in order, for each
//! time field and filter, including runs that share a timestamp.
//!
//! Runs on SQLite, and on Postgres too when `OXIDLE_TEST_POSTGRES` is set.

use oxidle::store::sql::{PoolConfig, SqlStore};
use oxidle::store::{MetaStore, RunQuery, RunTimeField};
use oxidle::types::*;
use serde_json::json;
use ulid::Ulid;
use uuid::Uuid;

async fn stores() -> Vec<(String, SqlStore)> {
    let path = std::env::temp_dir().join(format!("oxidle-runs-{}.db", Ulid::generate()));
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let mut out = vec![("sqlite".to_string(), SqlStore::connect(&url, PoolConfig::default()).await.unwrap())];
    if let Ok(admin_url) = std::env::var("OXIDLE_TEST_POSTGRES") {
        sqlx::any::install_default_drivers();
        let admin = sqlx::AnyPool::connect(&admin_url).await.unwrap();
        let db = format!("oxidle_runs_{}", Ulid::generate().to_string().to_lowercase());
        sqlx::query(&format!("CREATE DATABASE {db}")).execute(&admin).await.unwrap();
        let url = format!("{}/{db}", admin_url.rsplit_once('/').unwrap().0);
        out.push(("postgres".to_string(), SqlStore::connect(&url, PoolConfig::default()).await.unwrap()));
    }
    out
}

/// 30 runs over two functions. Timestamps repeat in groups of three, so pages split ties.
async fn seed(meta: &dyn MetaStore) -> Vec<RunRecord> {
    let mut runs = Vec::new();
    for i in 0..30u64 {
        let t = 1_000_000 + (i / 3) as i64 * 1000;
        let status = match i % 5 {
            0 => RunStatus::Failed,
            1 => RunStatus::Running,
            _ => RunStatus::Completed,
        };
        let mut r = RunRecord {
            run_id: Ulid::from_parts(t as u64, i as u128),
            fn_id: Uuid::from_u128(1 + (i % 2) as u128),
            fn_slug: "app-fn".into(),
            app_id: Uuid::from_u128(9),
            fn_version: 1,
            event_id: None,
            batch_id: None,
            cron: None,
            original_run_id: None,
            status: RunStatus::Running,
            started_at: t,
            ended_at: None,
            output: None,
            event_name: Some("app/go".into()),
            exec_started_at: None,
        };
        meta.insert_run(&r).await.unwrap();
        // Started and ended times tie in a different grouping than queued times.
        let started = 2_000_000 + (i / 4) as i64 * 1000;
        meta.mark_run_started(r.run_id, started).await.unwrap();
        r.exec_started_at = Some(started);
        if status != RunStatus::Running {
            let ended = 3_000_000 + (i / 2) as i64 * 1000;
            meta.finish_run(r.run_id, status, &json!({}), ended).await.unwrap();
            r.ended_at = Some(ended);
        }
        r.status = status;
        runs.push(r);
    }
    runs
}

fn time(r: &RunRecord, field: RunTimeField) -> Option<i64> {
    match field {
        RunTimeField::Queued => Some(r.started_at),
        RunTimeField::Started => r.exec_started_at,
        RunTimeField::Ended => r.ended_at,
    }
}

/// Follows page cursors to the end, the way the dashboard does.
async fn all_pages(meta: &dyn MetaStore, q: RunQuery) -> Vec<Ulid> {
    let mut out = Vec::new();
    let mut page = q.clone();
    loop {
        let runs = meta.list_runs(&page).await.unwrap();
        out.extend(runs.iter().map(|r| r.run_id));
        let Some(last) = runs.last() else { break };
        if (runs.len() as i64) < page.limit {
            break;
        }
        page.page_cursor = Some((time(last, q.time_field).unwrap(), last.run_id));
        assert!(out.len() <= 30, "pagination does not terminate");
    }
    out
}

/// A filter's name, which runs it keeps, and the query that should select the same runs.
type Filter = (&'static str, fn(&RunRecord) -> bool, RunQuery);

#[tokio::test]
async fn keyset_pages_cover_every_run_once_in_order() {
    for (backend, store) in stores().await {
        let runs = seed(&store).await;
        let fields = [RunTimeField::Queued, RunTimeField::Started, RunTimeField::Ended];
        let filters: [Filter; 3] = [
            ("all", |_| true, RunQuery::default()),
            ("fn", |r| r.fn_id == Uuid::from_u128(1), RunQuery {
                fn_id: Some(Uuid::from_u128(1)),
                ..Default::default()
            }),
            ("failed", |r| r.status == RunStatus::Failed, RunQuery {
                ui_statuses: vec!["FAILED".into()],
                ..Default::default()
            }),
        ];
        for field in fields {
            for (name, keep, base) in &filters {
                let mut want: Vec<&RunRecord> =
                    runs.iter().filter(|r| keep(r) && time(r, field).is_some()).collect();
                want.sort_by_key(|r| std::cmp::Reverse((time(r, field), r.run_id)));
                let want: Vec<Ulid> = want.iter().map(|r| r.run_id).collect();
                let q = RunQuery {
                    time_field: field,
                    from: Some(0),
                    limit: 4,
                    ..base.clone()
                };
                let got = all_pages(&store, q).await;
                assert_eq!(got, want, "{backend}: {name} by {field:?}");
            }
        }
    }
}
