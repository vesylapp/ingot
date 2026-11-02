//! Dashboard run search: CEL queries select the same runs as evaluating them one by one,
//! across pages, whether answered in SQL or by scanning.
//!
//! Runs on SQLite, and on Postgres too when `INGOT_TEST_POSTGRES` is set.

use ingot::store::search::RunSearch;
use ingot::store::sql::{PoolConfig, SqlStore};
use ingot::store::{MetaStore, RunQuery, RunTimeField};
use ingot::types::*;
use serde_json::{Map, Value, json};
use ulid::Ulid;
use uuid::Uuid;

async fn stores() -> Vec<(String, SqlStore)> {
    let path = std::env::temp_dir().join(format!("ingot-search-{}.db", Ulid::generate()));
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let mut out = vec![(
        "sqlite".to_string(),
        SqlStore::connect(&url, PoolConfig::default())
            .await
            .unwrap(),
    )];
    if let Ok(admin_url) = std::env::var("INGOT_TEST_POSTGRES") {
        sqlx::any::install_default_drivers();
        let admin = sqlx::AnyPool::connect(&admin_url).await.unwrap();
        let db = format!(
            "ingot_search_{}",
            Ulid::generate().to_string().to_lowercase()
        );
        sqlx::query(&format!("CREATE DATABASE {db}"))
            .execute(&admin)
            .await
            .unwrap();
        let url = format!("{}/{db}", admin_url.rsplit_once('/').unwrap().0);
        out.push((
            "postgres".to_string(),
            SqlStore::connect(&url, PoolConfig::default())
                .await
                .unwrap(),
        ));
    }
    out
}

const NAMES: [&str; 3] = ["app/process-automations", "app/sync-rates", "app/index"];

/// What a run looks like to a query, for computing the expected matches.
struct Seeded {
    run: RunRecord,
    name: &'static str,
    n: i64,
}

/// 60 runs across three event names. Every fourth run's event has been pruned; failed runs
/// carry an error as output.
async fn seed(meta: &dyn MetaStore) -> Vec<Seeded> {
    let mut out = Vec::new();
    for i in 0..60u64 {
        // Timestamps repeat in pairs, so pages split ties.
        let t = 1_000_000 + (i / 2) as i64 * 1000;
        let name = NAMES[(i % 3) as usize];
        let n = i as i64;
        let internal_id = Ulid::from_parts(t as u64, 1000 + i as u128);
        if i % 4 != 3 {
            let mut data = Map::new();
            data.insert("n".into(), json!(n));
            data.insert(
                "tenant".into(),
                json!(if i % 2 == 0 { "acme" } else { "globex" }),
            );
            meta.insert_events(&[TrackedEvent {
                internal_id,
                received_at: t,
                event: Event {
                    id: format!("evt-{i}"),
                    name: name.into(),
                    data,
                    user: None,
                    ts: t,
                    v: None,
                    meta: None,
                },
            }])
            .await
            .unwrap();
        }
        let mut run = RunRecord {
            run_id: Ulid::from_parts(t as u64, i as u128),
            fn_id: Uuid::from_u128(1 + (i % 3) as u128),
            fn_slug: "app-fn".into(),
            app_id: Uuid::from_u128(9),
            fn_version: 1,
            event_id: Some(internal_id),
            batch_id: None,
            cron: None,
            original_run_id: None,
            status: RunStatus::Running,
            started_at: t,
            ended_at: None,
            output: None,
            event_name: Some(name.into()),
            exec_started_at: None,
        };
        meta.insert_run(&run).await.unwrap();
        let (status, output) = match i % 5 {
            0 => (
                RunStatus::Failed,
                json!({"name": "Error", "message": format!("boom {i}")}),
            ),
            1 => (RunStatus::Running, Value::Null),
            _ => (RunStatus::Completed, json!({"count": n * 10})),
        };
        if status != RunStatus::Running {
            meta.finish_run(run.run_id, status, &output, t + 500)
                .await
                .unwrap();
            run.ended_at = Some(t + 500);
            run.output = Some(output);
        }
        run.status = status;
        out.push(Seeded { run, name, n });
    }
    out
}

fn has_event(s: &Seeded) -> bool {
    s.n % 4 != 3
}

/// Pages through a search the way the dashboard does, with a small scan limit so that pages
/// stop early and resume.
async fn search_all(meta: &dyn MetaStore, query: &str, limit: i64, scan_limit: usize) -> Vec<Ulid> {
    let search = RunSearch::parse(query)
        .unwrap()
        .expect("non-empty query")
        .with_scan_chunk(4);
    let mut q = RunQuery {
        time_field: RunTimeField::Queued,
        from: Some(0),
        limit,
        ..Default::default()
    };
    let mut out = Vec::new();
    for _ in 0..200 {
        let page = search.list(meta, &q, scan_limit).await.unwrap();
        assert!(page.runs.len() as i64 <= limit);
        out.extend(page.runs.iter().map(|r| r.run_id));
        let next = match (page.resume, page.runs.last()) {
            (Some(pos), _) => pos,
            (None, Some(last)) if page.runs.len() as i64 == limit => (last.started_at, last.run_id),
            _ => return out,
        };
        q.page_cursor = Some(next);
    }
    panic!("search pagination does not terminate");
}

fn expected(runs: &[Seeded], keep: impl Fn(&Seeded) -> bool) -> Vec<Ulid> {
    let mut want: Vec<&Seeded> = runs.iter().filter(|s| keep(s)).collect();
    want.sort_by_key(|s| std::cmp::Reverse((s.run.started_at, s.run.run_id)));
    want.iter().map(|s| s.run.run_id).collect()
}

#[tokio::test]
async fn search_matches_per_run_evaluation() {
    type Keep = fn(&Seeded) -> bool;
    let cases: &[(&str, Keep, bool)] = &[
        // (query, which runs it selects, answered in SQL alone)
        (
            r#"event.name == "app/process-automations""#,
            |s| s.name == NAMES[0],
            true,
        ),
        (
            r#"event.name in ["app/sync-rates", "app/index"]"#,
            |s| s.name != NAMES[0],
            true,
        ),
        (
            "event.name == \"app/index\"\n\nevent.name == \"app/index\"",
            |s| s.name == NAMES[2],
            true,
        ),
        (r#"event.name == "app/none""#, |_| false, true),
        // Needs evaluating: a pruned event only has its name.
        (
            r#"event.name == "app/process-automations" && event.data.n >= 20"#,
            |s| s.name == NAMES[0] && has_event(s) && s.n >= 20,
            false,
        ),
        (
            r#"event.data.tenant == "acme""#,
            |s| has_event(s) && s.n % 2 == 0,
            false,
        ),
        // One condition per line; all must hold.
        (
            "event.data.tenant == \"globex\"\nevent.data.n < 30",
            |s| has_event(s) && s.n % 2 == 1 && s.n < 30,
            false,
        ),
        (
            r#"output.count > 300"#,
            |s| s.n % 5 >= 2 && s.n * 10 > 300,
            false,
        ),
        (
            r#"error.message.startsWith("boom")"#,
            |s| s.n % 5 == 0,
            false,
        ),
        // Expressions that fail to evaluate (no such field) match nothing.
        (r#"event.data.missing.deep == 1"#, |_| false, false),
    ];
    for (backend, store) in stores().await {
        let runs = seed(&store).await;
        for (query, keep, sql_only) in cases {
            let search = RunSearch::parse(query).unwrap().unwrap();
            assert_eq!(search.is_sql_only(), *sql_only, "{backend}: {query}");
            let want = expected(&runs, keep);
            // Scan limits below the page size make pages stop early and resume.
            for (limit, scan_limit) in [(7, 1_000_000), (5, 1), (3, 9), (100, 4)] {
                let got = search_all(&store, query, limit, scan_limit).await;
                assert_eq!(
                    got, want,
                    "{backend}: {query} (limit {limit}, scan {scan_limit})"
                );
            }
            let q = RunQuery {
                from: Some(0),
                ..Default::default()
            };
            for chunk in [500, 4, 1] {
                let search = RunSearch::parse(query)
                    .unwrap()
                    .unwrap()
                    .with_scan_chunk(chunk);
                let n = search.count(&store, &q).await.unwrap();
                assert_eq!(
                    n,
                    want.len() as i64,
                    "{backend}: count {query} (chunk {chunk})"
                );
            }
        }
    }
}

#[tokio::test]
async fn search_combines_with_other_filters() {
    for (backend, store) in stores().await {
        let runs = seed(&store).await;
        let search = RunSearch::parse(r#"event.name == "app/sync-rates""#)
            .unwrap()
            .unwrap();
        let q = RunQuery {
            from: Some(0),
            ui_statuses: vec!["FAILED".into()],
            limit: 100,
            ..Default::default()
        };
        let got: Vec<Ulid> = search
            .list(&store, &q, 1000)
            .await
            .unwrap()
            .runs
            .iter()
            .map(|r| r.run_id)
            .collect();
        let want = expected(&runs, |s| {
            s.name == NAMES[1] && s.run.status == RunStatus::Failed
        });
        assert!(!want.is_empty());
        assert_eq!(got, want, "{backend}");
        assert_eq!(
            search.count(&store, &q).await.unwrap(),
            want.len() as i64,
            "{backend}"
        );
    }
}

#[test]
fn invalid_and_blank_queries() {
    assert!(RunSearch::parse("").unwrap().is_none());
    assert!(RunSearch::parse("  \n\n ").unwrap().is_none());
    let err = RunSearch::parse("event.name == \"a\"\nevent.name ==").unwrap_err();
    assert!(err.0.ends_with("\n | event.name =="), "{}", err.0);
    let err = RunSearch::parse("nope.x == 1").unwrap_err();
    assert!(
        err.0.contains("undeclared reference to 'nope'"),
        "{}",
        err.0
    );
}
