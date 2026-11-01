//! Backend-agnostic conformance suite for [`StateStore`].
//!
//! Every scenario runs against `SqlStore` on a temporary SQLite file, and against
//! `RedisStore` when `OXIDLE_TEST_REDIS` is set (e.g. `redis://127.0.0.1:6399`).

use oxidle::store::redis::RedisStore;
use oxidle::store::sql::{PoolConfig, SqlStore};
use oxidle::store::{CreateRun, MetaUpdate, SaveStep, StateStore};
use oxidle::types::*;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use ulid::Ulid;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

enum Cleanup {
    Sqlite(PathBuf),
    Redis { uri: String, prefix: String },
}

struct Backend {
    name: &'static str,
    store: Arc<dyn StateStore>,
    cleanup: Cleanup,
}

async fn backends() -> Vec<Backend> {
    let mut out = Vec::new();
    let path = std::env::temp_dir().join(format!("oxidle-state-{}.db", Ulid::generate()));
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let sql = SqlStore::connect(&url, PoolConfig::default())
        .await
        .expect("sqlite store");
    out.push(Backend {
        name: "sqlite",
        store: Arc::new(sql),
        cleanup: Cleanup::Sqlite(path),
    });
    if let Ok(uri) = std::env::var("OXIDLE_TEST_REDIS")
        && !uri.is_empty()
    {
        let prefix = format!("oxtest-{}", Ulid::generate());
        let store = RedisStore::connect(&uri, &prefix)
            .await
            .expect("redis store");
        out.push(Backend {
            name: "redis",
            store: Arc::new(store),
            cleanup: Cleanup::Redis { uri, prefix },
        });
    }
    out
}

async fn cleanup(c: Cleanup) {
    match c {
        Cleanup::Sqlite(path) => {
            for ext in ["", "-wal", "-shm"] {
                let mut p = path.clone().into_os_string();
                p.push(ext);
                let _ = std::fs::remove_file(p);
            }
        }
        Cleanup::Redis { uri, prefix } => {
            let Ok(client) = redis::Client::open(uri.as_str()) else {
                return;
            };
            let Ok(mut conn) = client.get_multiplexed_async_connection().await else {
                return;
            };
            let mut cursor: u64 = 0;
            loop {
                let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(format!("*{prefix}*"))
                    .arg("COUNT")
                    .arg(1000)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or((0, vec![]));
                if !keys.is_empty() {
                    let _: redis::RedisResult<i64> =
                        redis::cmd("DEL").arg(&keys).query_async(&mut conn).await;
                }
                if next == 0 {
                    break;
                }
                cursor = next;
            }
        }
    }
}

/// Defines a test running `$body(store)` against every configured backend.
macro_rules! conformance {
    ($name:ident, $body:path) => {
        #[tokio::test]
        async fn $name() {
            for b in backends().await {
                eprintln!("--- {} [{}]", stringify!($name), b.name);
                $body(&*b.store).await;
                drop(b.store);
                cleanup(b.cleanup).await;
            }
        }
    };
}

conformance!(
    runs_create_and_idempotency,
    runs_create_and_idempotency_impl
);
conformance!(runs_update_meta, runs_update_meta_impl);
conformance!(runs_steps_and_pending, runs_steps_and_pending_impl);
conformance!(runs_end_run, runs_end_run_impl);
conformance!(queue_enqueue_lease_ack, queue_enqueue_lease_ack_impl);
conformance!(queue_dedupe_after_ack, queue_dedupe_after_ack_impl);
conformance!(queue_lease_expiry, queue_lease_expiry_impl);
conformance!(queue_requeue_and_wake, queue_requeue_and_wake_impl);
conformance!(queue_reschedule, queue_reschedule_impl);
conformance!(queue_remove_and_run_items, queue_remove_and_run_items_impl);
conformance!(concurrency_limits, concurrency_limits_impl);
conformance!(
    concurrency_expiry_and_extend,
    concurrency_expiry_and_extend_impl
);
conformance!(gcra_rate_limit, gcra_impl);
conformance!(kv_ops, kv_impl);
conformance!(pauses_index_and_delete, pauses_index_impl);
conformance!(pauses_consume, pauses_consume_impl);
conformance!(debounce_ops, debounce_impl);
conformance!(batch_ops, batch_impl);
conformance!(gc_prunes, gc_impl);

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn meta(run_id: Ulid, idem: &str) -> RunMeta {
    RunMeta {
        run_id,
        fn_id: Uuid::from_u128(42),
        fn_slug: "app-fn".into(),
        fn_version: 3,
        app_id: Uuid::from_u128(7),
        status: RunStatus::Running,
        created_at: now_ms(),
        started_at: None,
        ended_at: None,
        event_ids: vec![Ulid::generate()],
        batch_id: None,
        cron: None,
        original_run_id: None,
        idempotency_key: idem.into(),
        request_version: -1,
        force_step_plan: false,
        priority_factor: 0,
        concurrency: vec![],
        throttle: None,
        singleton_key: None,
        max_attempts: 4,
        suppress_lifecycle: false,
    }
}

async fn new_run(s: &dyn StateStore) -> Ulid {
    let id = Ulid::generate();
    let r = s
        .create_run(&meta(id, &id.to_string()), &[json!({"name": "e"})], 60_000)
        .await
        .unwrap();
    assert!(matches!(r, CreateRun::Created));
    id
}

fn item(id: &str, at: i64, run: Option<Ulid>) -> QueueItem {
    QueueItem {
        id: id.into(),
        kind: QueueKind::Edge,
        run_id: run,
        fn_id: run.map(|_| Uuid::from_u128(42)),
        at,
        dedupe: false,
        attempt: 0,
        max_attempts: 3,
        payload: json!({"step": id, "n": [1, 2, {"x": null}], "empty": []}),
        lease_id: None,
    }
}

fn ids(items: &[QueueItem]) -> Vec<&str> {
    items.iter().map(|i| i.id.as_str()).collect()
}

fn saved(r: SaveStep) -> (bool, bool) {
    match r {
        SaveStep::Saved {
            pending_empty,
            duplicate,
        } => (pending_empty, duplicate),
        SaveStep::NoRun => panic!("expected Saved, got NoRun"),
    }
}

fn pause(kind: PauseKind, run_id: Ulid, step: &str, event: Option<&str>, expires_at: i64) -> Pause {
    Pause {
        id: Uuid::new_v4(),
        kind,
        run_id,
        fn_id: Uuid::from_u128(42),
        step_id: step.into(),
        event: event.map(String::from),
        expression: Some("async.data.id == event.data.id".into()),
        trigger_event: Some(json!({"name": "trigger", "data": {"id": 1}})),
        trigger_event_id: None,
        expires_at,
        created_at: now_ms(),
        timeout_job: Some(format!("{run_id}-{step}-timeout")),
        correlation_id: None,
    }
}

fn pause_ids(ps: &[Pause]) -> Vec<Uuid> {
    let mut v: Vec<Uuid> = ps.iter().map(|p| p.id).collect();
    v.sort();
    v
}

fn sorted(mut v: Vec<Uuid>) -> Vec<Uuid> {
    v.sort();
    v
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

async fn runs_create_and_idempotency_impl(s: &dyn StateStore) {
    let r1 = Ulid::generate();
    let events = vec![
        json!({"name": "a", "data": {"big": 9007199254740993u64, "arr": []}}),
        json!({"name": "b"}),
    ];
    let mut m = meta(r1, "idem-1");
    m.started_at = Some(1234);
    m.priority_factor = 3;
    assert!(matches!(
        s.create_run(&m, &events, 60_000).await.unwrap(),
        CreateRun::Created
    ));

    // Same idempotency key, different run: duplicate pointing at the first run.
    let r2 = Ulid::generate();
    match s
        .create_run(&meta(r2, "idem-1"), &events, 60_000)
        .await
        .unwrap()
    {
        CreateRun::Duplicate(id) => assert_eq!(id, r1.to_string()),
        CreateRun::Created => panic!("expected duplicate"),
    }
    assert!(s.load_meta(r2).await.unwrap().is_none());
    assert_eq!(s.kv_get("idem:idem-1").await.unwrap(), Some(r1.to_string()));

    let st = s.load_run(r1).await.unwrap().expect("run");
    assert_eq!(st.meta.run_id, r1);
    assert_eq!(st.meta.fn_slug, "app-fn");
    assert_eq!(st.meta.status, RunStatus::Running);
    assert_eq!(st.meta.started_at, Some(1234));
    assert_eq!(st.meta.priority_factor, 3);
    assert_eq!(st.meta.request_version, -1);
    assert_eq!(st.meta.event_ids, m.event_ids);
    assert_eq!(st.events, events);
    assert!(st.steps.is_empty());

    let lm = s.load_meta(r1).await.unwrap().expect("meta");
    assert_eq!(lm.idempotency_key, "idem-1");
    assert_eq!(s.load_events(r1).await.unwrap(), events);

    let missing = Ulid::generate();
    assert!(s.load_run(missing).await.unwrap().is_none());
    assert!(s.load_meta(missing).await.unwrap().is_none());
    assert!(s.load_events(missing).await.unwrap().is_empty());
    assert!(s.load_steps(missing).await.unwrap().is_empty());

    // Idempotency keys expire.
    let r3 = Ulid::generate();
    assert!(matches!(
        s.create_run(&meta(r3, "idem-short"), &[], 50)
            .await
            .unwrap(),
        CreateRun::Created
    ));
    let r4 = Ulid::generate();
    assert!(matches!(
        s.create_run(&meta(r4, "idem-short"), &[], 50)
            .await
            .unwrap(),
        CreateRun::Duplicate(_)
    ));
    sleep_ms(120).await;
    assert!(matches!(
        s.create_run(&meta(r4, "idem-short"), &[], 50)
            .await
            .unwrap(),
        CreateRun::Created
    ));
    assert!(s.load_meta(r4).await.unwrap().is_some());
}

async fn runs_update_meta_impl(s: &dyn StateStore) {
    let r = new_run(s).await;
    s.update_meta(
        r,
        &MetaUpdate {
            started_at: Some(100),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // started_at is set once.
    s.update_meta(
        r,
        &MetaUpdate {
            started_at: Some(200),
            request_version: Some(2),
            force_step_plan: None,
        },
    )
    .await
    .unwrap();
    let m = s.load_meta(r).await.unwrap().unwrap();
    assert_eq!(m.started_at, Some(100));
    assert_eq!(m.request_version, 2);
    assert!(!m.force_step_plan);

    s.update_meta(
        r,
        &MetaUpdate {
            force_step_plan: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let m = s.load_run(r).await.unwrap().unwrap().meta;
    assert!(m.force_step_plan);
    assert_eq!(m.request_version, 2);
    s.update_meta(
        r,
        &MetaUpdate {
            force_step_plan: Some(false),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!s.load_meta(r).await.unwrap().unwrap().force_step_plan);

    // Updating a missing run is a no-op and does not create it.
    let missing = Ulid::generate();
    s.update_meta(
        missing,
        &MetaUpdate {
            started_at: Some(1),
            request_version: Some(1),
            force_step_plan: Some(true),
        },
    )
    .await
    .unwrap();
    assert!(s.load_meta(missing).await.unwrap().is_none());
    s.set_pending(missing, &["a".into()]).await.unwrap();
    assert!(s.load_meta(missing).await.unwrap().is_none());
    assert!(matches!(
        s.save_step(missing, "a", &json!(1)).await.unwrap(),
        SaveStep::NoRun
    ));
}

async fn runs_steps_and_pending_impl(s: &dyn StateStore) {
    let r = new_run(s).await;
    // No pending set tracked: pending_empty is true.
    assert_eq!(
        saved(s.save_step(r, "b-step", &json!({"v": 1})).await.unwrap()),
        (true, false)
    );
    assert_eq!(
        saved(s.save_step(r, "a-step", &json!([])).await.unwrap()),
        (true, false)
    );
    assert_eq!(
        saved(s.save_step(r, "c-step", &Value::Null).await.unwrap()),
        (true, false)
    );
    // Duplicate does not overwrite or reorder.
    assert_eq!(
        saved(s.save_step(r, "b-step", &json!("other")).await.unwrap()),
        (true, true)
    );
    let steps = s.load_steps(r).await.unwrap();
    assert_eq!(
        steps,
        vec![
            ("b-step".to_string(), json!({"v": 1})),
            ("a-step".to_string(), json!([])),
            ("c-step".to_string(), Value::Null),
        ]
    );
    assert_eq!(s.load_run(r).await.unwrap().unwrap().steps, steps);

    // Already-saved ids are not pending.
    s.set_pending(r, &["p1".into(), "p2".into(), "a-step".into()])
        .await
        .unwrap();
    assert_eq!(
        saved(s.save_step(r, "unrelated", &json!(0)).await.unwrap()),
        (false, false)
    );
    assert_eq!(
        saved(s.save_step(r, "p1", &json!(1)).await.unwrap()),
        (false, false)
    );
    assert_eq!(
        saved(s.save_step(r, "p2", &json!(2)).await.unwrap()),
        (true, false)
    );
    // Drained: back to "no pending set".
    assert_eq!(
        saved(s.save_step(r, "p3", &json!(3)).await.unwrap()),
        (true, false)
    );

    // A duplicate save still drains the pending set.
    s.set_pending(r, &["p4".into(), "p5".into()]).await.unwrap();
    assert_eq!(
        saved(s.save_step(r, "p4", &json!(4)).await.unwrap()),
        (false, false)
    );
    s.set_pending(r, &["p4".into(), "p5".into()]).await.unwrap();
    // p4 is saved, so only p5 remains.
    assert_eq!(
        saved(s.save_step(r, "p5", &json!(5)).await.unwrap()),
        (true, false)
    );

    // set_pending with nothing outstanding / empty clears tracking.
    s.set_pending(r, &["p6".into()]).await.unwrap();
    s.set_pending(r, &[]).await.unwrap();
    assert_eq!(
        saved(s.save_step(r, "p7", &json!(7)).await.unwrap()),
        (true, false)
    );
    s.set_pending(r, &["p6".into(), "p8".into()]).await.unwrap();
    assert_eq!(
        saved(s.save_step(r, "p6", &json!(6)).await.unwrap()),
        (false, false)
    );
    assert_eq!(
        saved(s.save_step(r, "p6", &json!(6)).await.unwrap()),
        (false, true)
    );

    let order: Vec<String> = s
        .load_steps(r)
        .await
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(
        order,
        [
            "b-step",
            "a-step",
            "c-step",
            "unrelated",
            "p1",
            "p2",
            "p3",
            "p4",
            "p5",
            "p7",
            "p6"
        ]
    );
}

async fn runs_end_run_impl(s: &dyn StateStore) {
    let r = new_run(s).await;
    s.save_step(r, "s1", &json!(1)).await.unwrap();
    s.set_pending(r, &["x".into()]).await.unwrap();
    assert!(s.end_run(r, RunStatus::Completed, 5555).await.unwrap());
    assert!(!s.end_run(r, RunStatus::Failed, 6666).await.unwrap());
    assert!(
        !s.end_run(Ulid::generate(), RunStatus::Failed, 1)
            .await
            .unwrap()
    );

    let st = s
        .load_run(r)
        .await
        .unwrap()
        .expect("finished run stays readable");
    assert_eq!(st.meta.status, RunStatus::Completed);
    assert_eq!(st.meta.ended_at, Some(5555));
    assert_eq!(st.steps, vec![("s1".to_string(), json!(1))]);
    assert!(matches!(
        s.save_step(r, "s2", &json!(2)).await.unwrap(),
        SaveStep::NoRun
    ));
    assert!(matches!(
        s.save_step(r, "x", &json!(2)).await.unwrap(),
        SaveStep::NoRun
    ));
    assert_eq!(s.load_steps(r).await.unwrap().len(), 1);

    // Non-terminal statuses don't finish a run.
    let r2 = new_run(s).await;
    assert!(s.end_run(r2, RunStatus::Overflowed, 1).await.unwrap());
    assert_eq!(
        s.load_meta(r2).await.unwrap().unwrap().status,
        RunStatus::Overflowed
    );
    assert!(s.end_run(r2, RunStatus::Cancelled, 2).await.unwrap());
    assert!(!s.end_run(r2, RunStatus::Completed, 3).await.unwrap());
    let m = s.load_meta(r2).await.unwrap().unwrap();
    assert_eq!(m.status, RunStatus::Cancelled);
    assert_eq!(m.ended_at, Some(2));
}

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

async fn queue_enqueue_lease_ack_impl(s: &dyn StateStore) {
    let now = now_ms();
    assert_eq!(s.next_due().await.unwrap(), None);
    let run = Ulid::generate();
    assert!(s.enqueue(&item("a", now - 30, Some(run))).await.unwrap());
    assert!(s.enqueue(&item("b", now - 10, None)).await.unwrap());
    assert!(s.enqueue(&item("c", now - 20, Some(run))).await.unwrap());
    assert!(
        s.enqueue(&item("future", now + 10_000, None))
            .await
            .unwrap()
    );
    // Same id: rejected.
    assert!(!s.enqueue(&item("a", now - 5, None)).await.unwrap());
    assert_eq!(s.next_due().await.unwrap(), Some(now - 30));

    // Lease is limited and ordered by `at`.
    let leased = s.lease(now, 2, 30_000).await.unwrap();
    assert_eq!(ids(&leased), ["a", "c"]);
    let lease_id = leased[0].lease_id.clone().expect("lease id");
    for it in &leased {
        assert_eq!(it.lease_id.as_deref(), Some(lease_id.as_str()));
        assert_eq!(it.at, now + 30_000);
        assert_eq!(it.run_id, Some(run));
        assert_eq!(it.fn_id, Some(Uuid::from_u128(42)));
        assert_eq!(it.kind, QueueKind::Edge);
        assert_eq!(it.max_attempts, 3);
        assert_eq!(it.payload, item(&it.id, 0, None).payload);
    }
    let rest = s.lease(now, 10, 30_000).await.unwrap();
    assert_eq!(ids(&rest), ["b"]);
    assert_ne!(rest[0].lease_id, leased[0].lease_id);
    assert!(s.lease(now, 10, 30_000).await.unwrap().is_empty());
    assert!(s.lease(now, 0, 30_000).await.unwrap().is_empty());
    assert_eq!(s.next_due().await.unwrap(), Some(now + 10_000));

    // extend_lease needs the matching lease id.
    assert!(s.extend_lease("a", &lease_id, now + 60_000).await.unwrap());
    assert!(!s.extend_lease("a", "bogus", now + 90_000).await.unwrap());
    assert!(
        !s.extend_lease("missing", &lease_id, now + 90_000)
            .await
            .unwrap()
    );
    let items = s.run_items(run, 10).await.unwrap();
    assert_eq!(ids(&items), ["c", "a"]);
    assert_eq!(items[1].at, now + 60_000);
    assert_eq!(items[1].lease_id.as_deref(), Some(lease_id.as_str()));

    // ack with a wrong lease id leaves the item.
    let mut wrong = leased[0].clone();
    wrong.lease_id = Some("not-the-lease".into());
    s.ack(&wrong).await.unwrap();
    assert_eq!(s.run_items(run, 10).await.unwrap().len(), 2);
    s.ack(&leased[0]).await.unwrap();
    assert_eq!(ids(&s.run_items(run, 10).await.unwrap()), ["c"]);
    s.ack(&leased[1]).await.unwrap();
    assert!(s.run_items(run, 10).await.unwrap().is_empty());

    // Not leased: ack without a lease id removes it.
    let fut = item("future", 0, None);
    s.ack(&fut).await.unwrap();
    assert_eq!(s.next_due().await.unwrap(), Some(now + 30_000));
    s.ack(&rest[0]).await.unwrap();
    assert_eq!(s.next_due().await.unwrap(), None);

    // Acked non-dedupe ids can be enqueued again.
    assert!(s.enqueue(&item("a", now, None)).await.unwrap());
}

async fn queue_dedupe_after_ack_impl(s: &dyn StateStore) {
    let now = now_ms();
    let mut d = item("job-1", now - 1, None);
    d.dedupe = true;
    assert!(s.enqueue(&d).await.unwrap());
    assert!(!s.enqueue(&d).await.unwrap());
    let leased = s.lease(now, 10, 30_000).await.unwrap();
    assert_eq!(ids(&leased), ["job-1"]);
    assert!(leased[0].dedupe);
    s.ack(&leased[0]).await.unwrap();
    // Remembered after completion.
    assert!(!s.enqueue(&d).await.unwrap());
    // A non-dedupe enqueue of the same id ignores the done marker.
    let plain = item("job-1", now, None);
    assert!(s.enqueue(&plain).await.unwrap());
    s.remove_item("job-1").await.unwrap();
    assert!(!s.enqueue(&d).await.unwrap());

    // Removing (not acking) a dedupe item does not remember it.
    let mut d2 = item("job-2", now, None);
    d2.dedupe = true;
    assert!(s.enqueue(&d2).await.unwrap());
    s.remove_item("job-2").await.unwrap();
    assert!(s.enqueue(&d2).await.unwrap());
}

async fn queue_lease_expiry_impl(s: &dyn StateStore) {
    let now = now_ms();
    assert!(s.enqueue(&item("x", now - 1, None)).await.unwrap());
    let first = s.lease(now, 10, 50).await.unwrap();
    assert_eq!(ids(&first), ["x"]);
    assert!(s.lease(now + 49, 10, 50).await.unwrap().is_empty());
    assert_eq!(s.next_due().await.unwrap(), Some(now + 50));
    // Lease expired: due again, with a new lease id.
    let second = s.lease(now + 50, 10, 50).await.unwrap();
    assert_eq!(ids(&second), ["x"]);
    assert_ne!(first[0].lease_id, second[0].lease_id);
    assert_eq!(second[0].at, now + 100);
    // The stale lease can no longer extend, requeue or ack it.
    assert!(
        !s.extend_lease("x", first[0].lease_id.as_deref().unwrap(), now + 1000)
            .await
            .unwrap()
    );
    s.requeue(&first[0], now + 5000, 7, None).await.unwrap();
    s.ack(&first[0]).await.unwrap();
    let items = s.lease(now + 100, 10, 50).await.unwrap();
    assert_eq!(ids(&items), ["x"]);
    assert_eq!(items[0].attempt, 0);
}

async fn queue_requeue_and_wake_impl(s: &dyn StateStore) {
    let now = now_ms();
    let run = Ulid::generate();
    s.enqueue(&item("r1", now - 3, Some(run))).await.unwrap();
    s.enqueue(&item("r2", now - 2, Some(run))).await.unwrap();
    s.enqueue(&item("r3", now - 1, Some(run))).await.unwrap();
    let mut leased = s.lease(now, 10, 30_000).await.unwrap();
    assert_eq!(ids(&leased), ["r1", "r2", "r3"]);

    // Requeue with a changed payload and a blocking key.
    leased[0].payload = json!({"throttled": true});
    s.requeue(&leased[0], now + 60_000, 2, Some("ck"))
        .await
        .unwrap();
    s.requeue(&leased[1], now + 60_001, 1, Some("ck"))
        .await
        .unwrap();
    s.requeue(&leased[2], now + 60_002, 5, None).await.unwrap();
    let items = s.run_items(run, 10).await.unwrap();
    assert_eq!(ids(&items), ["r1", "r2", "r3"]);
    assert_eq!(items[0].at, now + 60_000);
    assert_eq!(items[0].attempt, 2);
    assert_eq!(items[0].lease_id, None);
    assert_eq!(items[0].payload, json!({"throttled": true}));
    assert_eq!(items[2].attempt, 5);
    assert!(s.lease(now, 10, 30_000).await.unwrap().is_empty());

    // Requeue without a lease id is a no-op.
    let mut unleased = items[2].clone();
    unleased.lease_id = None;
    s.requeue(&unleased, now - 100, 9, None).await.unwrap();
    assert_eq!(s.run_items(run, 10).await.unwrap()[2].attempt, 5);

    // Wake one item blocked on "ck": the earliest.
    s.wake_blocked(&["other".into()], 5).await.unwrap();
    s.wake_blocked(&["ck".into()], 1).await.unwrap();
    let woke = s.lease(now_ms() + 1, 10, 30_000).await.unwrap();
    assert_eq!(ids(&woke), ["r1"]);
    assert_eq!(woke[0].payload, json!({"throttled": true}));
    s.ack(&woke[0]).await.unwrap();
    // Leased items are no longer blocked: waking again only affects r2.
    s.wake_blocked(&["ck".into()], 10).await.unwrap();
    let woke = s.lease(now_ms() + 1, 10, 30_000).await.unwrap();
    assert_eq!(ids(&woke), ["r2"]);
    s.wake_blocked(&["ck".into()], 10).await.unwrap();
    assert!(s.lease(now_ms() + 1, 10, 30_000).await.unwrap().is_empty());

    // A re-blocked item can be woken again; leasing clears the block.
    s.requeue(&woke[0], now + 60_000, 1, Some("ck2"))
        .await
        .unwrap();
    let again = s.lease(now + 60_000, 10, 30_000).await.unwrap();
    assert_eq!(ids(&again), ["r2"]);
    s.requeue(&again[0], now + 120_000, 1, None).await.unwrap();
    s.wake_blocked(&["ck2".into()], 10).await.unwrap();
    assert_eq!(
        s.run_items(run, 10)
            .await
            .unwrap()
            .iter()
            .find(|i| i.id == "r2")
            .unwrap()
            .at,
        now + 120_000
    );
}

async fn queue_reschedule_impl(s: &dyn StateStore) {
    let now = now_ms();
    s.enqueue(&item("d1", now + 50_000, None)).await.unwrap();
    assert!(s.reschedule("d1", now + 70_000).await.unwrap());
    assert_eq!(s.next_due().await.unwrap(), Some(now + 70_000));
    assert!(s.reschedule("d1", now - 1).await.unwrap());
    assert!(!s.reschedule("missing", now).await.unwrap());
    let leased = s.lease(now, 10, 30_000).await.unwrap();
    assert_eq!(ids(&leased), ["d1"]);
    // Leased items cannot be rescheduled.
    assert!(!s.reschedule("d1", now + 1).await.unwrap());
    assert_eq!(s.next_due().await.unwrap(), Some(now + 30_000));

    // Rescheduling a blocked item keeps it blocked (wake still applies).
    let run = Ulid::generate();
    s.enqueue(&item("d2", now - 1, Some(run))).await.unwrap();
    let l = s.lease(now, 10, 30_000).await.unwrap();
    s.requeue(&l[0], now + 10_000, 0, Some("bk")).await.unwrap();
    assert!(s.reschedule("d2", now + 20_000).await.unwrap());
    assert_eq!(s.run_items(run, 1).await.unwrap()[0].at, now + 20_000);
    s.wake_blocked(&["bk".into()], 1).await.unwrap();
    assert!(s.run_items(run, 1).await.unwrap()[0].at <= now_ms());
}

async fn queue_remove_and_run_items_impl(s: &dyn StateStore) {
    let now = now_ms();
    let run = Ulid::generate();
    let other = Ulid::generate();
    s.enqueue(&item("i3", now + 3, Some(run))).await.unwrap();
    s.enqueue(&item("i1", now + 1, Some(run))).await.unwrap();
    s.enqueue(&item("i2", now + 2, Some(run))).await.unwrap();
    s.enqueue(&item("o1", now + 4, Some(other))).await.unwrap();
    s.enqueue(&item("free", now + 5, None)).await.unwrap();

    assert_eq!(
        ids(&s.run_items(run, 10).await.unwrap()),
        ["i1", "i2", "i3"]
    );
    assert_eq!(ids(&s.run_items(run, 2).await.unwrap()), ["i1", "i2"]);
    assert!(s.run_items(Ulid::generate(), 10).await.unwrap().is_empty());

    // A leased item is removed too.
    let l = s.lease(now + 2, 1, 30_000).await.unwrap();
    assert_eq!(ids(&l), ["i1"]);
    s.remove_run_items(run, Some("i2")).await.unwrap();
    assert_eq!(ids(&s.run_items(run, 10).await.unwrap()), ["i2"]);
    assert_eq!(ids(&s.run_items(other, 10).await.unwrap()), ["o1"]);
    s.remove_run_items(run, None).await.unwrap();
    assert!(s.run_items(run, 10).await.unwrap().is_empty());

    s.remove_item("o1").await.unwrap();
    s.remove_item("o1").await.unwrap();
    assert!(s.run_items(other, 10).await.unwrap().is_empty());
    assert_eq!(s.next_due().await.unwrap(), Some(now + 5));
    s.remove_item("free").await.unwrap();
    assert_eq!(s.next_due().await.unwrap(), None);
    assert!(s.lease(now + 100_000, 10, 1000).await.unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

fn ck(key: &str, limit: u32) -> ConcurrencyKey {
    ConcurrencyKey {
        key: key.into(),
        limit,
    }
}

async fn concurrency_limits_impl(s: &dyn StateStore) {
    let exp = now_ms() + 60_000;
    let keys = [ck("b", 1), ck("a", 2)];
    assert_eq!(s.acquire_concurrency("h1", &[], exp).await.unwrap(), None);
    assert_eq!(s.acquire_concurrency("h1", &keys, exp).await.unwrap(), None);
    assert_eq!(s.concurrency_in_use("a").await.unwrap(), 1);
    assert_eq!(s.concurrency_in_use("b").await.unwrap(), 1);
    // b is saturated: nothing is taken.
    assert_eq!(
        s.acquire_concurrency("h2", &keys, exp).await.unwrap(),
        Some("b".into())
    );
    assert_eq!(s.concurrency_in_use("a").await.unwrap(), 1);
    // Re-acquiring as the same holder is fine.
    assert_eq!(s.acquire_concurrency("h1", &keys, exp).await.unwrap(), None);
    assert_eq!(s.concurrency_in_use("b").await.unwrap(), 1);
    // a alone still has room for one more.
    assert_eq!(
        s.acquire_concurrency("h2", &[ck("a", 2)], exp)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        s.acquire_concurrency("h3", &[ck("a", 2)], exp)
            .await
            .unwrap(),
        Some("a".into())
    );
    // First saturated key in key order.
    assert_eq!(
        s.acquire_concurrency("h3", &[ck("b", 1), ck("a", 2)], exp)
            .await
            .unwrap(),
        Some("a".into())
    );

    let mut released = s.release_concurrency("h1").await.unwrap();
    released.sort();
    assert_eq!(released, ["a", "b"]);
    assert!(s.release_concurrency("h1").await.unwrap().is_empty());
    assert_eq!(s.concurrency_in_use("b").await.unwrap(), 0);
    assert_eq!(s.concurrency_in_use("a").await.unwrap(), 1);
    assert_eq!(s.acquire_concurrency("h3", &keys, exp).await.unwrap(), None);
    assert_eq!(s.concurrency_in_use("a").await.unwrap(), 2);
    assert_eq!(s.release_concurrency("h2").await.unwrap(), ["a"]);
    assert_eq!(s.concurrency_in_use("missing").await.unwrap(), 0);
}

async fn concurrency_expiry_and_extend_impl(s: &dyn StateStore) {
    // Expired holders don't count.
    assert_eq!(
        s.acquire_concurrency("h1", &[ck("c", 1)], now_ms() + 100)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        s.acquire_concurrency("h2", &[ck("c", 1)], now_ms() + 60_000)
            .await
            .unwrap(),
        Some("c".into())
    );
    sleep_ms(150).await;
    assert_eq!(s.concurrency_in_use("c").await.unwrap(), 0);
    assert_eq!(
        s.acquire_concurrency("h2", &[ck("c", 1)], now_ms() + 60_000)
            .await
            .unwrap(),
        None
    );
    assert_eq!(s.concurrency_in_use("c").await.unwrap(), 1);

    // Extension keeps the slot.
    assert_eq!(
        s.acquire_concurrency("h3", &[ck("d", 1), ck("e", 5)], now_ms() + 100)
            .await
            .unwrap(),
        None
    );
    s.extend_concurrency("h3", now_ms() + 60_000).await.unwrap();
    s.extend_concurrency("nobody", now_ms() + 60_000)
        .await
        .unwrap();
    sleep_ms(150).await;
    assert_eq!(s.concurrency_in_use("d").await.unwrap(), 1);
    assert_eq!(s.concurrency_in_use("e").await.unwrap(), 1);
    assert_eq!(
        s.acquire_concurrency("h4", &[ck("d", 1)], now_ms() + 60_000)
            .await
            .unwrap(),
        Some("d".into())
    );
    let mut rel = s.release_concurrency("h3").await.unwrap();
    rel.sort();
    assert_eq!(rel, ["d", "e"]);
    assert_eq!(
        s.acquire_concurrency("h4", &[ck("d", 1)], now_ms() + 60_000)
            .await
            .unwrap(),
        None
    );
}

// ---------------------------------------------------------------------------
// GCRA
// ---------------------------------------------------------------------------

async fn gcra_impl(s: &dyn StateStore) {
    let now = now_ms();
    // 1 per 100ms, no burst.
    assert_eq!(s.gcra("k1", now, 100, 0).await.unwrap(), Ok(()));
    assert_eq!(s.gcra("k1", now, 100, 0).await.unwrap(), Err(now + 100));
    assert_eq!(
        s.gcra("k1", now + 50, 100, 0).await.unwrap(),
        Err(now + 100)
    );
    assert_eq!(s.gcra("k1", now + 100, 100, 0).await.unwrap(), Ok(()));
    assert_eq!(
        s.gcra("k1", now + 100, 100, 0).await.unwrap(),
        Err(now + 200)
    );

    // Burst of 3 (tolerance 2 emissions).
    for _ in 0..3 {
        assert_eq!(s.gcra("k2", now, 100, 200).await.unwrap(), Ok(()));
    }
    assert_eq!(s.gcra("k2", now, 100, 200).await.unwrap(), Err(now + 100));
    assert_eq!(s.gcra("k2", now + 100, 100, 200).await.unwrap(), Ok(()));
    assert_eq!(
        s.gcra("k2", now + 100, 100, 200).await.unwrap(),
        Err(now + 200)
    );
    // After a long idle period the bucket is full again.
    for _ in 0..3 {
        assert_eq!(s.gcra("k2", now + 10_000, 100, 200).await.unwrap(), Ok(()));
    }
    // Keys are independent.
    assert_eq!(s.gcra("k3", now, 100, 0).await.unwrap(), Ok(()));
}

// ---------------------------------------------------------------------------
// KV
// ---------------------------------------------------------------------------

async fn kv_impl(s: &dyn StateStore) {
    assert_eq!(s.kv_get("k").await.unwrap(), None);
    assert_eq!(s.kv_set_nx("k", "v1", None).await.unwrap(), None);
    assert_eq!(
        s.kv_set_nx("k", "v2", None).await.unwrap(),
        Some("v1".into())
    );
    assert_eq!(s.kv_get("k").await.unwrap(), Some("v1".into()));
    assert!(!s.kv_del("k", Some("v2")).await.unwrap());
    assert_eq!(s.kv_get("k").await.unwrap(), Some("v1".into()));
    assert!(s.kv_del("k", Some("v1")).await.unwrap());
    assert_eq!(s.kv_get("k").await.unwrap(), None);
    assert!(!s.kv_del("k", None).await.unwrap());
    assert_eq!(s.kv_set_nx("k", "v3", Some(60_000)).await.unwrap(), None);
    assert!(s.kv_del("k", None).await.unwrap());

    // TTL expiry.
    assert_eq!(s.kv_set_nx("t", "a", Some(50)).await.unwrap(), None);
    assert_eq!(
        s.kv_set_nx("t", "b", Some(50)).await.unwrap(),
        Some("a".into())
    );
    sleep_ms(120).await;
    assert_eq!(s.kv_get("t").await.unwrap(), None);
    assert_eq!(s.kv_set_nx("t", "b", None).await.unwrap(), None);
    assert_eq!(s.kv_get("t").await.unwrap(), Some("b".into()));
}

// ---------------------------------------------------------------------------
// Pauses
// ---------------------------------------------------------------------------

async fn pauses_index_impl(s: &dyn StateStore) {
    let far = now_ms() + 3_600_000;
    let run = Ulid::generate();
    let other = Ulid::generate();
    let wait = pause(PauseKind::WaitForEvent, run, "w", Some("e1"), far);
    let cancel = pause(PauseKind::Cancel, run, "", Some("e1"), far);
    let sig = pause(PauseKind::Signal, run, "s", Some("sig1"), far);
    let inv = pause(PauseKind::Invoke, run, "i", Some("e1"), far);
    let other_wait = pause(PauseKind::WaitForEvent, other, "w", Some("e1"), far);
    for p in [&wait, &cancel, &sig, &inv, &other_wait] {
        assert!(s.save_pause(p).await.unwrap());
    }
    assert!(!s.save_pause(&wait).await.unwrap());

    let got = s.get_pause(wait.id).await.unwrap().expect("pause");
    assert_eq!(got.id, wait.id);
    assert_eq!(got.kind, PauseKind::WaitForEvent);
    assert_eq!(got.run_id, run);
    assert_eq!(got.expression, wait.expression);
    assert_eq!(got.trigger_event, wait.trigger_event);
    assert_eq!(got.timeout_job, wait.timeout_job);
    assert!(s.get_pause(Uuid::new_v4()).await.unwrap().is_none());
    // Invoke pauses are fetched by id only.
    assert!(s.get_pause(inv.id).await.unwrap().is_some());

    assert_eq!(
        pause_ids(&s.pauses_for_event("e1").await.unwrap()),
        sorted(vec![wait.id, cancel.id, other_wait.id])
    );
    assert_eq!(
        pause_ids(&s.pauses_for_signal("sig1").await.unwrap()),
        vec![sig.id]
    );
    assert!(s.pauses_for_event("sig1").await.unwrap().is_empty());
    assert_eq!(
        pause_ids(&s.pauses_for_event("signal:sig1").await.unwrap()),
        vec![sig.id]
    );
    assert!(s.pauses_for_signal("e1").await.unwrap().is_empty());
    assert!(s.pauses_for_event("nothing").await.unwrap().is_empty());

    s.delete_pause(cancel.id).await.unwrap();
    s.delete_pause(cancel.id).await.unwrap();
    assert!(s.get_pause(cancel.id).await.unwrap().is_none());
    assert_eq!(
        pause_ids(&s.pauses_for_event("e1").await.unwrap()),
        sorted(vec![wait.id, other_wait.id])
    );

    s.delete_run_pauses(run).await.unwrap();
    for p in [&wait, &sig, &inv] {
        assert!(s.get_pause(p.id).await.unwrap().is_none());
    }
    assert_eq!(
        pause_ids(&s.pauses_for_event("e1").await.unwrap()),
        vec![other_wait.id]
    );
    assert!(s.pauses_for_signal("sig1").await.unwrap().is_empty());
    assert!(s.get_pause(other_wait.id).await.unwrap().is_some());
    // A deleted pause can be saved again.
    assert!(s.save_pause(&wait).await.unwrap());
}

async fn pauses_consume_impl(s: &dyn StateStore) {
    let far = now_ms() + 3_600_000;
    let run = new_run(s).await;
    s.set_pending(run, &["w1".into(), "w2".into()])
        .await
        .unwrap();
    let p1 = pause(PauseKind::WaitForEvent, run, "w1", Some("ev"), far);
    let p2 = pause(PauseKind::WaitForEvent, run, "w2", Some("ev"), far);
    s.save_pause(&p1).await.unwrap();
    s.save_pause(&p2).await.unwrap();

    let v1 = json!({"name": "ev", "data": {"n": 1}});
    assert_eq!(s.consume_pause(&p1, Some(&v1)).await.unwrap(), Some(false));
    // Exactly once.
    assert_eq!(s.consume_pause(&p1, Some(&v1)).await.unwrap(), None);
    assert!(s.get_pause(p1.id).await.unwrap().is_none());
    assert_eq!(
        pause_ids(&s.pauses_for_event("ev").await.unwrap()),
        vec![p2.id]
    );
    assert_eq!(
        s.consume_pause(&p2, Some(&Value::Null)).await.unwrap(),
        Some(true)
    );
    assert_eq!(
        s.load_steps(run).await.unwrap(),
        vec![
            ("w1".to_string(), v1.clone()),
            ("w2".to_string(), Value::Null)
        ]
    );

    // Concurrent consumers: exactly one wins.
    let p3 = pause(PauseKind::WaitForEvent, run, "w3", Some("ev"), far);
    s.save_pause(&p3).await.unwrap();
    let (v3, v4) = (json!(3), json!(4));
    let (a, b) = tokio::join!(
        s.consume_pause(&p3, Some(&v3)),
        s.consume_pause(&p3, Some(&v4))
    );
    let wins = [a.unwrap(), b.unwrap()]
        .iter()
        .filter(|r| r.is_some())
        .count();
    assert_eq!(wins, 1);
    let w3 = s
        .load_steps(run)
        .await
        .unwrap()
        .into_iter()
        .filter(|(k, _)| k == "w3")
        .count();
    assert_eq!(w3, 1);

    // No value: just deletes the pause.
    let c = pause(PauseKind::Cancel, run, "", Some("ev"), far);
    s.save_pause(&c).await.unwrap();
    assert_eq!(s.consume_pause(&c, None).await.unwrap(), Some(true));
    assert_eq!(s.consume_pause(&c, None).await.unwrap(), None);

    // Step already saved: pause is consumed but the result is None.
    let dup = pause(PauseKind::WaitForEvent, run, "w1", Some("ev"), far);
    s.save_pause(&dup).await.unwrap();
    assert_eq!(
        s.consume_pause(&dup, Some(&json!("late"))).await.unwrap(),
        None
    );
    assert!(s.get_pause(dup.id).await.unwrap().is_none());
    assert_eq!(s.load_steps(run).await.unwrap()[0].1, v1);

    // Run finished: pause consumed, nothing saved.
    let p4 = pause(PauseKind::WaitForEvent, run, "w4", Some("ev"), far);
    s.save_pause(&p4).await.unwrap();
    s.end_run(run, RunStatus::Cancelled, now_ms())
        .await
        .unwrap();
    assert_eq!(s.consume_pause(&p4, Some(&json!(4))).await.unwrap(), None);
    assert!(s.get_pause(p4.id).await.unwrap().is_none());
    assert!(
        !s.load_steps(run)
            .await
            .unwrap()
            .iter()
            .any(|(k, _)| k == "w4")
    );

    // Unknown run.
    let ghost = pause(
        PauseKind::WaitForEvent,
        Ulid::generate(),
        "g",
        Some("ev"),
        far,
    );
    s.save_pause(&ghost).await.unwrap();
    assert_eq!(
        s.consume_pause(&ghost, Some(&json!(1))).await.unwrap(),
        None
    );
    assert_eq!(s.consume_pause(&ghost, None).await.unwrap(), None);
}

// ---------------------------------------------------------------------------
// Debounce
// ---------------------------------------------------------------------------

async fn debounce_impl(s: &dyn StateStore) {
    let f = Uuid::from_u128(9);
    let now = now_ms();
    let (e1, e2, e3) = (Ulid::generate(), Ulid::generate(), Ulid::generate());
    let ev = |n: i64| json!({"name": "d", "data": {"n": n}, "ts": n});

    let r = s
        .debounce_upsert("k", f, e1, &ev(1), 1000, 5000, Some(20_000), now)
        .await
        .unwrap();
    assert!(r.created && !r.ignored);
    assert_eq!(r.fire_at, now + 5000);
    let id = r.id.clone();

    let r = s
        .debounce_upsert("k", f, e2, &ev(2), 2000, 5000, Some(20_000), now + 1000)
        .await
        .unwrap();
    assert!(!r.created && !r.ignored);
    assert_eq!(r.id, id);
    assert_eq!(r.fire_at, now + 6000);

    // Older event is ignored.
    let r = s
        .debounce_upsert("k", f, e3, &ev(3), 500, 5000, Some(20_000), now + 2000)
        .await
        .unwrap();
    assert!(r.ignored && !r.created);
    assert_eq!(r.id, id);

    // Timeout caps the wait.
    let r = s
        .debounce_upsert("k", f, e3, &ev(4), 2000, 5000, Some(20_000), now + 17_000)
        .await
        .unwrap();
    assert_eq!(r.fire_at, now + 20_000);
    // ... but never below 1s.
    let r = s
        .debounce_upsert("k", f, e2, &ev(2), 2000, 5000, Some(20_000), now + 19_900)
        .await
        .unwrap();
    assert_eq!(r.fire_at, now + 20_900);

    assert_eq!(s.debounce_take("k", "wrong").await.unwrap(), None);
    assert_eq!(s.debounce_take("other", &id).await.unwrap(), None);
    assert_eq!(s.debounce_take("k", &id).await.unwrap(), Some((e2, ev(2))));
    assert_eq!(s.debounce_take("k", &id).await.unwrap(), None);

    // A new debounce with a new id after the take; no timeout.
    let r = s
        .debounce_upsert("k", f, e1, &ev(9), 9, 300, None, now)
        .await
        .unwrap();
    assert!(r.created);
    assert_ne!(r.id, id);
    assert_eq!(r.fire_at, now + 1000);
    assert_eq!(
        s.debounce_take("k", &r.id).await.unwrap(),
        Some((e1, ev(9)))
    );
}

// ---------------------------------------------------------------------------
// Batching
// ---------------------------------------------------------------------------

async fn batch_impl(s: &dyn StateStore) {
    let f = Uuid::from_u128(5);
    let e: Vec<Ulid> = (0..6).map(|_| Ulid::generate()).collect();
    let ev = |n: usize| json!({"name": "b", "data": {"n": n}});

    let a = s.batch_append("k", f, &ev(0), e[0], 3).await.unwrap();
    assert!(a.created && !a.duplicate);
    assert_eq!(a.len, 1);
    let b1 = a.batch_id;
    let a = s.batch_append("k", f, &ev(0), e[0], 3).await.unwrap();
    assert!(!a.created && a.duplicate);
    assert_eq!((a.batch_id, a.len), (b1, 1));
    let a = s.batch_append("k", f, &ev(1), e[1], 3).await.unwrap();
    assert_eq!(
        (a.batch_id, a.len, a.created, a.duplicate),
        (b1, 2, false, false)
    );
    // Other keys have their own batch.
    let o = s.batch_append("other", f, &ev(9), e[5], 3).await.unwrap();
    assert!(o.created);
    assert_ne!(o.batch_id, b1);
    let a = s.batch_append("k", f, &ev(2), e[2], 3).await.unwrap();
    assert_eq!((a.batch_id, a.len), (b1, 3));
    // Full batch closed: the next append opens a new one.
    let a = s.batch_append("k", f, &ev(3), e[3], 3).await.unwrap();
    assert!(a.created);
    assert_eq!(a.len, 1);
    let b2 = a.batch_id;
    assert_ne!(b2, b1);

    assert_eq!(
        s.batch_take("k", b1).await.unwrap(),
        Some(vec![(e[0], ev(0)), (e[1], ev(1)), (e[2], ev(2))])
    );
    assert_eq!(s.batch_take("k", b1).await.unwrap(), None);

    // Taking the open batch closes it.
    assert_eq!(
        s.batch_take("k", b2).await.unwrap(),
        Some(vec![(e[3], ev(3))])
    );
    let a = s.batch_append("k", f, &ev(4), e[4], 3).await.unwrap();
    assert!(a.created);
    assert_ne!(a.batch_id, b2);
    assert_eq!(
        s.batch_take("k", a.batch_id).await.unwrap(),
        Some(vec![(e[4], ev(4))])
    );
    assert_eq!(
        s.batch_take("other", o.batch_id).await.unwrap(),
        Some(vec![(e[5], ev(9))])
    );
    assert_eq!(s.batch_take("k", Ulid::generate()).await.unwrap(), None);

    // max_size 1: every append fills its own batch.
    let x = s.batch_append("one", f, &ev(0), e[0], 1).await.unwrap();
    let y = s.batch_append("one", f, &ev(1), e[1], 1).await.unwrap();
    assert!(x.created && y.created);
    assert_ne!(x.batch_id, y.batch_id);
}

// ---------------------------------------------------------------------------
// GC
// ---------------------------------------------------------------------------

async fn gc_impl(s: &dyn StateStore) {
    let now = now_ms();
    let run = Ulid::generate();
    let old_cancel = pause(PauseKind::Cancel, run, "", Some("c"), now - 30 * 60 * 1000);
    let recent_cancel = pause(PauseKind::Cancel, run, "", Some("c"), now - 60 * 1000);
    let old_wait = pause(
        PauseKind::WaitForEvent,
        run,
        "w",
        Some("c"),
        now - 30 * 60 * 1000,
    );
    for p in [&old_cancel, &recent_cancel, &old_wait] {
        s.save_pause(p).await.unwrap();
    }
    s.kv_set_nx("gk", "v", Some(10)).await.unwrap();
    s.kv_set_nx("keep", "v", None).await.unwrap();
    s.acquire_concurrency("h", &[ck("g", 1)], now + 10)
        .await
        .unwrap();
    sleep_ms(50).await;

    s.gc(now_ms()).await.unwrap();
    assert!(s.get_pause(old_cancel.id).await.unwrap().is_none());
    assert!(s.get_pause(recent_cancel.id).await.unwrap().is_some());
    assert!(s.get_pause(old_wait.id).await.unwrap().is_some());
    assert_eq!(
        pause_ids(&s.pauses_for_event("c").await.unwrap()),
        sorted(vec![recent_cancel.id, old_wait.id])
    );
    assert_eq!(s.kv_get("gk").await.unwrap(), None);
    assert_eq!(s.kv_get("keep").await.unwrap(), Some("v".into()));
    assert_eq!(s.concurrency_in_use("g").await.unwrap(), 0);
    // gc is idempotent.
    s.gc(now_ms()).await.unwrap();
}

// ---------------------------------------------------------------------------
// Redis-specific: key hygiene and expiry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn redis_key_hygiene() {
    let Ok(uri) = std::env::var("OXIDLE_TEST_REDIS") else {
        return;
    };
    if uri.is_empty() {
        return;
    }
    let prefix = format!("oxtest-{}", Ulid::generate());
    let s = RedisStore::connect(&uri, &prefix).await.unwrap();
    let client = redis::Client::open(uri.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let kconn = conn.clone();
    let keys = |pattern: String| {
        let mut conn = kconn.clone();
        async move {
            let mut out: Vec<String> = redis::cmd("KEYS")
                .arg(pattern)
                .query_async(&mut conn)
                .await
                .unwrap();
            out.sort();
            out
        }
    };

    // Finished runs expire; running ones don't.
    let r = new_run(&s).await;
    s.save_step(r, "a", &json!(1)).await.unwrap();
    let rk = format!("{{{prefix}:run:{r}}}");
    let ttl: i64 = redis::cmd("PTTL")
        .arg(&rk)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(ttl, -1);
    s.set_pending(r, &["b".into()]).await.unwrap();
    s.end_run(r, RunStatus::Completed, now_ms()).await.unwrap();
    for k in [rk.clone(), format!("{rk}:sl"), format!("{rk}:sv")] {
        let ttl: i64 = redis::cmd("PTTL")
            .arg(&k)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(ttl > 6 * 24 * 3600 * 1000, "{k} ttl {ttl}");
    }
    assert!(keys(format!("{rk}:pd")).await.is_empty());
    // set_pending after the end inherits the run's TTL.
    s.set_pending(r, &["c".into()]).await.unwrap();
    let ttl: i64 = redis::cmd("PTTL")
        .arg(format!("{rk}:pd"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(ttl > 0);

    // The queue leaves nothing behind once drained.
    let now = now_ms();
    let run = Ulid::generate();
    s.enqueue(&item("q1", now - 1, Some(run))).await.unwrap();
    s.enqueue(&item("q2", now - 1, Some(run))).await.unwrap();
    let l = s.lease(now, 10, 1000).await.unwrap();
    s.requeue(&l[0], now, 1, Some("blk")).await.unwrap();
    assert!(!keys(format!("{{{prefix}:q}}:blk:*")).await.is_empty());
    s.remove_run_items(run, None).await.unwrap();
    assert!(keys(format!("{{{prefix}:q}}*")).await.is_empty());

    // Pauses and their indexes are removed together.
    let p = pause(PauseKind::Cancel, run, "", Some("e"), now + 60_000);
    s.save_pause(&p).await.unwrap();
    s.delete_run_pauses(run).await.unwrap();
    assert!(keys(format!("{prefix}:p*")).await.is_empty());

    // Released concurrency leaves no keys behind.
    s.acquire_concurrency("h", &[ck("x", 1)], now + 60_000)
        .await
        .unwrap();
    s.release_concurrency("h").await.unwrap();
    assert!(keys(format!("{prefix}:c*")).await.is_empty());

    // GCRA and dedupe markers expire natively.
    assert_eq!(s.gcra("g", now, 100, 0).await.unwrap(), Ok(()));
    let ttl: i64 = redis::cmd("PTTL")
        .arg(format!("{prefix}:kv:gcra:g"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(ttl > 0 && ttl <= 1100);
    let mut d = item("dd", now - 1, None);
    d.dedupe = true;
    s.enqueue(&d).await.unwrap();
    let l = s.lease(now, 10, 1000).await.unwrap();
    s.ack(&l[0]).await.unwrap();
    let ttl: i64 = redis::cmd("PTTL")
        .arg(format!("{{{prefix}:q}}:done:dd"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(ttl > 11 * 3600 * 1000);

    drop(s);
    cleanup(Cleanup::Redis { uri, prefix }).await;
}
