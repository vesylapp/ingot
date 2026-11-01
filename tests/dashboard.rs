//! Dashboard GraphQL API: shapes the Inngest UI depends on.

use oxidle::config::Config;
use oxidle::engine::Engine;
use oxidle::store::sql::{PoolConfig, SqlStore};
use oxidle::store::{MetaStore, Span, StateStore};
use oxidle::types::*;
use serde_json::{Value, json};
use std::sync::Arc;
use ulid::Ulid;
use uuid::Uuid;

fn config() -> Config {
    Config {
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
        no_ui: false,
        ui_dir: None,
        dev: true,
        retention_days: 0,
    }
}

struct Fixture {
    schema: oxidle::api::gql::DashboardSchema,
    run_id: Ulid,
    _dir: tempdir::Dir,
}

mod tempdir {
    pub struct Dir(pub std::path::PathBuf);
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    pub fn new() -> Dir {
        let p = std::env::temp_dir().join(format!("oxidle-dash-{}", ulid::Ulid::generate()));
        std::fs::create_dir_all(&p).unwrap();
        Dir(p)
    }
}

async fn fixture() -> Fixture {
    let dir = tempdir::new();
    let url = format!("sqlite://{}/main.db?mode=rwc", dir.0.display());
    let sql = Arc::new(SqlStore::connect(&url, PoolConfig::default()).await.unwrap());
    let meta: Arc<dyn MetaStore> = sql.clone();
    let state: Arc<dyn StateStore> = sql.clone();

    let app_id = Uuid::new_v4();
    let fn_id = Uuid::new_v4();
    meta.upsert_app(&App {
        id: app_id,
        name: "my-app".into(),
        url: "http://localhost:3000/api/inngest".into(),
        sdk_language: "js".into(),
        sdk_version: "4.26.2".into(),
        framework: "express".into(),
        checksum: "x".into(),
        error: None,
        method: "serve".into(),
        app_version: None,
        created_at: now_ms(),
    })
    .await
    .unwrap();
    let raw = json!({
        "id": "my-app-flaky", "name": "Flaky",
        "triggers": [{"event": "app/go"}],
        "steps": {"step": {"id": "step", "name": "step", "runtime": {"type": "http", "url": "http://localhost:3000/api/inngest?fnId=my-app-flaky&stepId=step"}}}
    });
    let mut f = oxidle::function::parse_function(&raw, app_id, "my-app", None).unwrap();
    f.id = fn_id;
    meta.sync_functions(app_id, &[f]).await.unwrap();

    let run_id = Ulid::generate();
    let t0 = now_ms() - 10_000;
    meta.insert_run(&RunRecord {
        run_id,
        fn_id,
        fn_slug: "my-app-flaky".into(),
        app_id,
        fn_version: 1,
        event_id: Some(Ulid::generate()),
        batch_id: None,
        cron: None,
        original_run_id: None,
        status: RunStatus::Running,
        started_at: t0,
        ended_at: None,
        output: None,
        event_name: Some("app/go".into()),
        exec_started_at: None,
    })
    .await
    .unwrap();
    meta.mark_run_started(run_id, t0 + 5).await.unwrap();
    meta.finish_run(run_id, RunStatus::Completed, &json!({"ok": true}), t0 + 900)
        .await
        .unwrap();

    let span = |id: &str| Span {
        run_id,
        span_id: id.into(),
        ..Default::default()
    };
    meta.upsert_spans(&[
        // A step that failed once, then succeeded.
        Span {
            name: Some("unstable".into()),
            op: Some("RUN".into()),
            step_id: Some("hash1".into()),
            step_type: Some("RUN".into()),
            status: Some("COMPLETED".into()),
            attempts: Some(2),
            queued_at: Some(t0 + 10),
            started_at: Some(t0 + 10),
            ended_at: Some(t0 + 400),
            output: Some(json!({"data": 42})),
            ..span("hash1")
        },
        Span {
            parent_id: Some("hash1".into()),
            status: Some("FAILED".into()),
            queued_at: Some(t0 + 10),
            started_at: Some(t0 + 10),
            ended_at: Some(t0 + 20),
            output: Some(json!({"error": {"name": "Error", "message": "boom"}})),
            ..span("hash1:a0")
        },
        Span {
            parent_id: Some("hash1".into()),
            status: Some("COMPLETED".into()),
            queued_at: Some(t0 + 300),
            started_at: Some(t0 + 300),
            ended_at: Some(t0 + 400),
            output: Some(json!({"data": 42})),
            ..span("hash1:a1")
        },
        // A sleep (no attempts recorded).
        Span {
            name: Some("nap".into()),
            op: Some("SLEEP".into()),
            step_id: Some("hash2".into()),
            status: Some("COMPLETED".into()),
            queued_at: Some(t0 + 410),
            started_at: Some(t0 + 410),
            ended_at: Some(t0 + 800),
            info: Some(json!({"sleepUntil": "2026-01-01T00:00:00.000Z"})),
            ..span("hash2")
        },
        Span {
            name: Some("Finalization".into()),
            step_type: Some("FINALIZATION".into()),
            status: Some("COMPLETED".into()),
            attempts: Some(1),
            queued_at: Some(t0 + 850),
            started_at: Some(t0 + 850),
            ended_at: Some(t0 + 900),
            output: Some(json!({"data": {"ok": true}})),
            ..span("finalization")
        },
    ])
    .await
    .unwrap();

    let (engine, _rx) = Engine::new(config(), meta, state);
    engine.registry.reload().await.unwrap();
    Fixture {
        schema: oxidle::api::gql::schema(engine),
        run_id,
        _dir: dir,
    }
}

async fn query(f: &Fixture, q: &str) -> Value {
    let resp = f.schema.execute(q).await;
    assert!(resp.errors.is_empty(), "graphql errors: {:?}", resp.errors);
    serde_json::to_value(resp.data).unwrap()
}

#[tokio::test]
async fn trace_is_flat_per_attempt() {
    let f = fixture().await;
    let v = query(
        &f,
        &format!(
            r#"{{ runTrace(runID: "{}") {{ isRoot status childrenSpans {{
                name stepID attempts status groupID outputID stepOp
                stepInfo {{ __typename ... on SleepStepInfo {{ sleepUntil }} }} }} }} }}"#,
            f.run_id
        ),
    )
    .await;
    let root = &v["runTrace"];
    assert_eq!(root["isRoot"], true);
    assert_eq!(root["status"], "COMPLETED");
    let kids = root["childrenSpans"].as_array().unwrap();
    // Two attempts of the retried step, the sleep, then finalization.
    let summary: Vec<(String, Value, Value)> = kids
        .iter()
        .map(|k| (k["name"].as_str().unwrap().to_string(), k["stepID"].clone(), k["attempts"].clone()))
        .collect();
    assert_eq!(
        summary,
        vec![
            ("unstable".into(), json!("hash1"), json!(0)),
            ("unstable".into(), json!("hash1"), json!(1)),
            ("nap".into(), json!("hash2"), json!(0)),
            ("Finalization".into(), Value::Null, json!(0)),
        ]
    );
    assert_eq!(kids[0]["status"], "FAILED");
    assert_eq!(kids[1]["status"], "COMPLETED");
    assert_eq!(kids[2]["stepOp"], "SLEEP");
    assert_eq!(kids[2]["stepInfo"]["__typename"], "SleepStepInfo");
    assert_eq!(kids[3]["groupID"], "finalization");
    for i in [0, 1, 3] {
        assert!(!kids[i]["outputID"].is_null(), "span {i} has no outputID");
    }

    // Outputs resolve to data / error.
    let failed = kids[0]["outputID"].as_str().unwrap();
    let out = query(
        &f,
        &format!(r#"{{ runTraceSpanOutputByID(outputID: "{failed}") {{ data error {{ message name }} }} }}"#),
    )
    .await;
    assert_eq!(out["runTraceSpanOutputByID"]["error"]["message"], "boom");
    let ok = kids[1]["outputID"].as_str().unwrap();
    let out = query(
        &f,
        &format!(r#"{{ runTraceSpanOutputByID(outputID: "{ok}") {{ data }} }}"#),
    )
    .await;
    assert_eq!(out["runTraceSpanOutputByID"]["data"], "42");
}

#[tokio::test]
async fn runs_list_and_function_config() {
    let f = fixture().await;
    let v = query(
        &f,
        r#"{ runs(first: 10, orderBy: [{field: QUEUED_AT, direction: DESC}], filter: {from: "2020-01-01T00:00:00Z", status: [COMPLETED]}) {
              totalCount edges { node { id status eventName startedAt function { slug } app { name } } } } }"#,
    )
    .await;
    assert_eq!(v["runs"]["totalCount"], 1);
    let node = &v["runs"]["edges"][0]["node"];
    assert_eq!(node["id"], f.run_id.to_string());
    assert_eq!(node["status"], "COMPLETED");
    assert_eq!(node["eventName"], "app/go");
    assert_eq!(node["function"]["slug"], "my-app-flaky");
    assert_eq!(node["app"]["name"], "my-app");
    assert!(!node["startedAt"].is_null());

    let none = query(
        &f,
        r#"{ runs(first: 10, orderBy: [{field: QUEUED_AT, direction: DESC}], filter: {from: "2020-01-01T00:00:00Z", status: [RUNNING, QUEUED]}) { totalCount } }"#,
    )
    .await;
    assert_eq!(none["runs"]["totalCount"], 0);

    let fns = query(
        &f,
        r#"{ functionBySlug(query: {functionSlug: "my-app-flaky"}) { name triggers { type value } configuration { retries { value isDefault } } app { name method } } }"#,
    )
    .await;
    let func = &fns["functionBySlug"];
    assert_eq!(func["name"], "Flaky");
    assert_eq!(func["triggers"][0]["type"], "EVENT");
    assert_eq!(func["configuration"]["retries"]["value"], 4);
    assert_eq!(func["configuration"]["retries"]["isDefault"], true);
    assert_eq!(func["app"]["method"], "SERVE");
}
