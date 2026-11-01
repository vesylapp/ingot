//! History retention: what `prune` deletes and keeps, batching, deadlines, and the
//! maintenance lease that keeps replicas from repeating each other's work.
//!
//! Runs on SQLite, and on Postgres too when `OXIDLE_TEST_POSTGRES` is set.

use oxidle::store::sql::{PoolConfig, SqlStore};
use oxidle::store::{EventQuery, MetaStore, Pruned, Span, StateStore};
use oxidle::types::*;
use serde_json::{Map, json};
use std::sync::Arc;
use std::time::{Duration, Instant};
use ulid::Ulid;
use uuid::Uuid;

const DAY: i64 = 86_400_000;

async fn stores() -> Vec<(String, Arc<SqlStore>)> {
    let path = std::env::temp_dir().join(format!("oxidle-ret-{}.db", Ulid::generate()));
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let mut out = vec![(
        "sqlite".to_string(),
        Arc::new(SqlStore::connect(&url, PoolConfig::default()).await.unwrap()),
    )];
    if let Ok(admin_url) = std::env::var("OXIDLE_TEST_POSTGRES") {
        sqlx::any::install_default_drivers();
        let admin = sqlx::AnyPool::connect(&admin_url).await.unwrap();
        let db = format!("oxidle_ret_{}", Ulid::generate().to_string().to_lowercase());
        sqlx::query(&format!("CREATE DATABASE {db}")).execute(&admin).await.unwrap();
        let url = format!("{}/{db}", admin_url.rsplit_once('/').unwrap().0);
        out.push((
            "postgres".to_string(),
            Arc::new(SqlStore::connect(&url, PoolConfig::default()).await.unwrap()),
        ));
    }
    out
}

/// A run created at `created` (its ULID carries that time), ended at `ended` if set.
async fn run(meta: &dyn MetaStore, created: i64, ended: Option<i64>, event: Option<Ulid>) -> Ulid {
    let run_id = Ulid::from_parts(created as u64, rand_bits());
    meta.insert_run(&RunRecord {
        run_id,
        fn_id: Uuid::from_u128(1),
        fn_slug: "app-fn".into(),
        app_id: Uuid::from_u128(2),
        fn_version: 1,
        event_id: event,
        batch_id: None,
        cron: None,
        original_run_id: None,
        status: RunStatus::Running,
        started_at: created,
        ended_at: None,
        output: None,
        event_name: Some("app/go".into()),
        exec_started_at: None,
    })
    .await
    .unwrap();
    if let Some(ended) = ended {
        meta.finish_run(run_id, RunStatus::Completed, &json!({}), ended)
            .await
            .unwrap();
    }
    spans(meta, run_id).await;
    run_id
}

async fn spans(meta: &dyn MetaStore, run_id: Ulid) {
    let spans: Vec<Span> = ["a", "b", "finalization"]
        .iter()
        .map(|id| Span {
            run_id,
            span_id: id.to_string(),
            status: Some("COMPLETED".into()),
            output: Some(json!({"data": id})),
            ..Default::default()
        })
        .collect();
    meta.upsert_spans(&spans).await.unwrap();
}

async fn event(meta: &dyn MetaStore, received_at: i64) -> Ulid {
    let internal_id = Ulid::from_parts(received_at as u64, rand_bits());
    meta.insert_events(&[TrackedEvent {
        internal_id,
        received_at,
        event: Event {
            id: internal_id.to_string(),
            name: "app/go".into(),
            data: Map::new(),
            user: None,
            ts: received_at,
            v: None,
            meta: None,
        },
    }])
    .await
    .unwrap();
    internal_id
}

fn rand_bits() -> u128 {
    Ulid::generate().random()
}

async fn has_event(meta: &dyn MetaStore, id: Ulid) -> bool {
    meta.get_event(id).await.unwrap().is_some()
}

#[tokio::test]
async fn prune_deletes_old_history_and_keeps_what_runs_need() {
    for (name, store) in stores().await {
        eprintln!("--- {name}");
        let meta: &dyn MetaStore = &*store;
        let now = now_ms();
        let before = now - 7 * DAY;

        // Old events: one unused, one used by an old finished run, one by a still-running run.
        let unused = event(meta, now - 30 * DAY).await;
        let of_old_run = event(meta, now - 30 * DAY).await;
        let of_live_run = event(meta, now - 30 * DAY).await;
        let recent_event = event(meta, now - DAY).await;

        // Five old finished runs (several batches at batch size 2), one recent, one still running.
        let mut old = vec![run(meta, now - 20 * DAY, Some(now - 19 * DAY), Some(of_old_run)).await];
        for _ in 0..4 {
            old.push(run(meta, now - 20 * DAY, Some(now - 19 * DAY), None).await);
        }
        let recent = run(meta, now - 20 * DAY, Some(now - DAY), None).await;
        let sleeping = run(meta, now - 30 * DAY, None, Some(of_live_run)).await;
        // Timeline of an old run whose row is already gone.
        let orphan = Ulid::from_parts((now - 25 * DAY) as u64, rand_bits());
        spans(meta, orphan).await;

        let p = meta
            .prune(before, 2, Instant::now() + Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(
            p,
            Pruned { runs: 5, spans: 18, events: 2, complete: true },
            "{name}"
        );

        for r in &old {
            assert!(meta.get_run(*r).await.unwrap().is_none(), "{name}: old run kept");
            assert!(meta.run_spans(*r).await.unwrap().is_empty(), "{name}: old spans kept");
        }
        assert!(meta.run_spans(orphan).await.unwrap().is_empty(), "{name}: orphan spans kept");
        for r in [recent, sleeping] {
            assert!(meta.get_run(r).await.unwrap().is_some(), "{name}: retained run deleted");
            assert_eq!(meta.run_spans(r).await.unwrap().len(), 3, "{name}: retained spans deleted");
        }
        assert!(!has_event(meta, unused).await, "{name}: unused event kept");
        assert!(!has_event(meta, of_old_run).await, "{name}: pruned run's event kept");
        assert!(has_event(meta, of_live_run).await, "{name}: live run's event deleted");
        assert!(has_event(meta, recent_event).await, "{name}: recent event deleted");

        // Nothing left to do; a second pass is a no-op.
        let again = meta
            .prune(before, 2, Instant::now() + Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(again, Pruned { complete: true, ..Default::default() }, "{name}");
    }
}

#[tokio::test]
async fn prune_stops_at_the_deadline() {
    for (name, store) in stores().await {
        let meta: &dyn MetaStore = &*store;
        let now = now_ms();
        let r = run(meta, now - 20 * DAY, Some(now - 19 * DAY), None).await;
        let p = meta.prune(now - 7 * DAY, 100, Instant::now()).await.unwrap();
        assert!(!p.complete, "{name}");
        assert!(meta.get_run(r).await.unwrap().is_some(), "{name}");
        let events = meta.list_events(&EventQuery { limit: 10, ..Default::default() }).await;
        assert!(events.is_ok(), "{name}");
    }
}

#[tokio::test]
async fn maintenance_runs_on_one_node_at_a_time() {
    let (_, store) = stores().await.remove(0);
    let meta: Arc<dyn MetaStore> = store.clone();
    let state: Arc<dyn StateStore> = store.clone();
    let cfg = oxidle::config::Config {
        host: "127.0.0.1".into(),
        port: 0,
        sdk_urls: vec![],
        signing_key: None,
        event_keys: vec![],
        sqlite_dir: String::new(),
        redis_uri: None,
        postgres_uri: None,
        pg_max_idle: 1,
        pg_max_open: 2,
        pg_idle_mins: 5,
        pg_lifetime_mins: 30,
        poll_interval_s: 5,
        retry_interval_s: None,
        queue_workers: 1,
        tick_ms: 150,
        no_ui: true,
        ui_dir: None,
        dev: true,
        retention_days: 7,
        shutdown_timeout_s: 5,
    };
    let (engine, _rx) = oxidle::engine::Engine::new(cfg, meta, state.clone());
    let key = oxidle::engine::maintenance::LEASE_KEY;

    // Another node holds the lease: this one skips the pass.
    state.kv_set_nx(key, "other-node", Some(60_000)).await.unwrap();
    assert!(!engine.maintenance_pass().await);
    // Released: this node runs it, and releases the lease afterwards.
    state.kv_del(key, Some("other-node")).await.unwrap();
    assert!(engine.maintenance_pass().await);
    assert_eq!(state.kv_get(key).await.unwrap(), None);
}
