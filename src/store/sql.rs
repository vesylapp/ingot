//! SQLite / Postgres implementation of [`MetaStore`] and [`StateStore`].
//!
//! Both dialects share one code path through sqlx's `Any` driver. Write transactions use
//! `BEGIN IMMEDIATE` on SQLite (serialized writers) and row/advisory locks on Postgres.

use super::*;
use anyhow::{Context, anyhow};
use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{Any, AnyPool, Row, Transaction};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Sqlite,
    Postgres,
}

pub struct SqlStore {
    pool: AnyPool,
    dialect: Dialect,
    seq: AtomicI64,
    /// Finished run state older than this is pruned by `gc` (0 = keep forever).
    retention_ms: AtomicI64,
}

/// How long completed job ids are remembered for enqueue dedup.
const DONE_TTL_MS: i64 = 12 * 3600 * 1000;

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_open: u32,
    pub max_idle: u32,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            max_open: 100,
            max_idle: 10,
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_secs(1800),
        }
    }
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS apps (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  sdk_language TEXT NOT NULL,
  sdk_version TEXT NOT NULL,
  framework TEXT NOT NULL,
  checksum TEXT NOT NULL,
  error TEXT,
  method TEXT NOT NULL,
  app_version TEXT,
  created_at BIGINT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS apps_name ON apps(name);
CREATE TABLE IF NOT EXISTS functions (
  id TEXT PRIMARY KEY,
  app_id TEXT NOT NULL,
  slug TEXT NOT NULL,
  name TEXT NOT NULL,
  version BIGINT NOT NULL,
  config TEXT NOT NULL,
  def TEXT NOT NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  archived_at BIGINT
);
CREATE INDEX IF NOT EXISTS functions_app ON functions(app_id);
CREATE INDEX IF NOT EXISTS functions_slug ON functions(slug);
CREATE TABLE IF NOT EXISTS generations (
  id TEXT PRIMARY KEY,
  value BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  internal_id TEXT PRIMARY KEY,
  event_id TEXT NOT NULL,
  name TEXT NOT NULL,
  data TEXT NOT NULL,
  user_data TEXT,
  ts BIGINT NOT NULL,
  v TEXT,
  received_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_name ON events(name, internal_id);
CREATE INDEX IF NOT EXISTS events_received ON events(received_at);
CREATE TABLE IF NOT EXISTS function_runs (
  run_id TEXT PRIMARY KEY,
  fn_id TEXT NOT NULL,
  fn_slug TEXT NOT NULL,
  app_id TEXT NOT NULL,
  fn_version BIGINT NOT NULL,
  event_id TEXT,
  batch_id TEXT,
  cron TEXT,
  original_run_id TEXT,
  started_at BIGINT NOT NULL,
  status TEXT NOT NULL,
  ended_at BIGINT,
  output TEXT
);
CREATE INDEX IF NOT EXISTS function_runs_event ON function_runs(event_id);
CREATE INDEX IF NOT EXISTS function_runs_fn ON function_runs(fn_id, run_id);
CREATE TABLE IF NOT EXISTS run_state (
  run_id TEXT PRIMARY KEY,
  meta TEXT NOT NULL,
  events TEXT NOT NULL,
  pending TEXT,
  status TEXT NOT NULL,
  updated_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS run_steps (
  run_id TEXT NOT NULL,
  step_id TEXT NOT NULL,
  seq BIGINT NOT NULL,
  data TEXT NOT NULL,
  PRIMARY KEY (run_id, step_id)
);
CREATE TABLE IF NOT EXISTS queue_items (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  run_id TEXT,
  fn_id TEXT,
  at BIGINT NOT NULL,
  attempt BIGINT NOT NULL,
  max_attempts BIGINT NOT NULL,
  payload TEXT NOT NULL,
  dedupe BIGINT NOT NULL,
  lease_id TEXT,
  blocked_on TEXT
);
CREATE INDEX IF NOT EXISTS queue_items_at ON queue_items(at);
CREATE INDEX IF NOT EXISTS queue_items_run ON queue_items(run_id);
CREATE INDEX IF NOT EXISTS queue_items_blocked ON queue_items(blocked_on);
CREATE TABLE IF NOT EXISTS queue_done (
  id TEXT PRIMARY KEY,
  expires_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS concurrency (
  key TEXT NOT NULL,
  holder TEXT NOT NULL,
  expires_at BIGINT NOT NULL,
  PRIMARY KEY (key, holder)
);
CREATE INDEX IF NOT EXISTS concurrency_holder ON concurrency(holder);
CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  expires_at BIGINT
);
CREATE TABLE IF NOT EXISTS pauses (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  event TEXT,
  data TEXT NOT NULL,
  expires_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS pauses_event ON pauses(event);
CREATE INDEX IF NOT EXISTS pauses_run ON pauses(run_id);
CREATE TABLE IF NOT EXISTS debounces (
  key TEXT PRIMARY KEY,
  id TEXT NOT NULL,
  fn_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  event TEXT NOT NULL,
  event_ts BIGINT NOT NULL,
  timeout_at BIGINT,
  fire_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
  id TEXT PRIMARY KEY,
  key TEXT NOT NULL,
  fn_id TEXT NOT NULL,
  len BIGINT NOT NULL,
  open BIGINT NOT NULL,
  created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS batches_key ON batches(key, open);
CREATE TABLE IF NOT EXISTS batch_events (
  batch_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  seq BIGINT NOT NULL,
  event TEXT NOT NULL,
  PRIMARY KEY (batch_id, event_id)
);
"#;

fn ulid_s(u: Ulid) -> String {
    u.to_string()
}

fn parse_ulid(s: &str) -> Result<Ulid> {
    Ulid::from_string(s).map_err(|e| anyhow!("invalid ulid {s}: {e}"))
}

fn parse_uuid(s: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(|e| anyhow!("invalid uuid {s}: {e}"))
}

fn to_json(v: &impl serde::Serialize) -> String {
    serde_json::to_string(v).expect("serialize")
}

impl SqlStore {
    pub async fn connect(url: &str, cfg: PoolConfig) -> Result<SqlStore> {
        sqlx::any::install_default_drivers();
        let dialect = if url.starts_with("postgres") {
            Dialect::Postgres
        } else {
            Dialect::Sqlite
        };
        let mut opts = AnyPoolOptions::new()
            .idle_timeout(cfg.idle_timeout)
            .max_lifetime(cfg.max_lifetime);
        opts = match dialect {
            Dialect::Postgres => opts.max_connections(cfg.max_open).min_connections(0),
            Dialect::Sqlite => opts
                .max_connections(8)
                .after_connect(|conn, _| {
                    Box::pin(async move {
                        use sqlx::Executor;
                        conn.execute("PRAGMA journal_mode=WAL").await?;
                        conn.execute("PRAGMA synchronous=NORMAL").await?;
                        conn.execute("PRAGMA busy_timeout=10000").await?;
                        conn.execute("PRAGMA foreign_keys=OFF").await?;
                        Ok(())
                    })
                }),
        };
        let pool = opts
            .connect(url)
            .await
            .with_context(|| format!("connecting to {}", redact(url)))?;
        let store = SqlStore {
            pool,
            dialect,
            seq: AtomicI64::new(0),
            retention_ms: AtomicI64::new(0),
        };
        store.migrate().await?;
        Ok(store)
    }

    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    pub fn set_retention_ms(&self, ms: i64) {
        self.retention_ms.store(ms, Ordering::Relaxed);
    }

    async fn migrate(&self) -> Result<()> {
        for stmt in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::query(stmt)
                .execute(&self.pool)
                .await
                .with_context(|| format!("migration failed: {stmt}"))?;
        }
        Ok(())
    }

    async fn tx(&self) -> Result<Transaction<'static, Any>> {
        Ok(match self.dialect {
            Dialect::Sqlite => self.pool.begin_with("BEGIN IMMEDIATE").await?,
            Dialect::Postgres => self.pool.begin().await?,
        })
    }

    /// Serialize concurrent writers on `key` (Postgres only; SQLite writers are already serialized).
    async fn lock(&self, tx: &mut Transaction<'static, Any>, key: &str) -> Result<()> {
        if self.dialect == Dialect::Postgres {
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(crate::util::xxhash(key) as i64)
                .execute(&mut **tx)
                .await?;
        }
        Ok(())
    }

    fn skip_locked(&self) -> &'static str {
        match self.dialect {
            Dialect::Postgres => " FOR UPDATE SKIP LOCKED",
            Dialect::Sqlite => "",
        }
    }

    fn next_seq(&self) -> i64 {
        let c = self.seq.fetch_add(1, Ordering::Relaxed) % 1000;
        now_ms() * 1000 + c
    }

    async fn bump_generation(&self) -> Result<()> {
        sqlx::query(
            "INSERT INTO generations (id, value) VALUES ('functions', 1)
             ON CONFLICT (id) DO UPDATE SET value = generations.value + 1",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn redact(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(mut u) if u.password().is_some() => {
            let _ = u.set_password(Some("***"));
            u.to_string()
        }
        _ => url.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Row decoding
// ---------------------------------------------------------------------------

fn row_app(r: &AnyRow) -> Result<App> {
    Ok(App {
        id: parse_uuid(&r.try_get::<String, _>("id")?)?,
        name: r.try_get("name")?,
        url: r.try_get("url")?,
        sdk_language: r.try_get("sdk_language")?,
        sdk_version: r.try_get("sdk_version")?,
        framework: r.try_get("framework")?,
        checksum: r.try_get("checksum")?,
        error: r.try_get("error")?,
        method: r.try_get("method")?,
        app_version: r.try_get("app_version")?,
        created_at: r.try_get("created_at")?,
    })
}

fn row_event(r: &AnyRow) -> Result<TrackedEvent> {
    let data: String = r.try_get("data")?;
    let user: Option<String> = r.try_get("user_data")?;
    Ok(TrackedEvent {
        internal_id: parse_ulid(&r.try_get::<String, _>("internal_id")?)?,
        received_at: r.try_get("received_at")?,
        event: Event {
            id: r.try_get("event_id")?,
            name: r.try_get("name")?,
            data: serde_json::from_str(&data).unwrap_or_default(),
            user: user.and_then(|u| serde_json::from_str(&u).ok()),
            ts: r.try_get("ts")?,
            v: r.try_get("v")?,
            meta: None,
        },
    })
}

fn opt_ulid(s: Option<String>) -> Option<Ulid> {
    s.and_then(|s| Ulid::from_string(&s).ok())
}

fn row_run(r: &AnyRow) -> Result<RunRecord> {
    let output: Option<String> = r.try_get("output")?;
    Ok(RunRecord {
        run_id: parse_ulid(&r.try_get::<String, _>("run_id")?)?,
        fn_id: parse_uuid(&r.try_get::<String, _>("fn_id")?)?,
        fn_slug: r.try_get("fn_slug")?,
        app_id: parse_uuid(&r.try_get::<String, _>("app_id")?)?,
        fn_version: r.try_get::<i64, _>("fn_version")? as i32,
        event_id: opt_ulid(r.try_get("event_id")?),
        batch_id: opt_ulid(r.try_get("batch_id")?),
        cron: r.try_get("cron")?,
        original_run_id: opt_ulid(r.try_get("original_run_id")?),
        status: RunStatus::parse(&r.try_get::<String, _>("status")?),
        started_at: r.try_get("started_at")?,
        ended_at: r.try_get("ended_at")?,
        output: output.map(|o| serde_json::from_str(&o).unwrap_or(Value::String(o))),
    })
}

fn row_item(r: &AnyRow) -> Result<QueueItem> {
    let kind: String = r.try_get("kind")?;
    let payload: String = r.try_get("payload")?;
    Ok(QueueItem {
        id: r.try_get("id")?,
        kind: QueueKind::parse(&kind).ok_or_else(|| anyhow!("unknown queue kind {kind}"))?,
        run_id: opt_ulid(r.try_get("run_id")?),
        fn_id: r
            .try_get::<Option<String>, _>("fn_id")?
            .and_then(|s| Uuid::parse_str(&s).ok()),
        at: r.try_get("at")?,
        dedupe: r.try_get::<i64, _>("dedupe")? != 0,
        attempt: r.try_get::<i64, _>("attempt")? as u32,
        max_attempts: r.try_get::<i64, _>("max_attempts")? as u32,
        payload: serde_json::from_str(&payload)?,
        lease_id: r.try_get("lease_id")?,
    })
}

fn row_pause(r: &AnyRow) -> Result<Pause> {
    let data: String = r.try_get("data")?;
    Ok(serde_json::from_str(&data)?)
}

const ITEM_COLS: &str = "id, kind, run_id, fn_id, at, attempt, max_attempts, payload, dedupe, lease_id";
const RUN_COLS: &str = "run_id, fn_id, fn_slug, app_id, fn_version, event_id, batch_id, cron, original_run_id, started_at, status, ended_at, output";

// ---------------------------------------------------------------------------
// MetaStore
// ---------------------------------------------------------------------------

#[async_trait]
impl MetaStore for SqlStore {
    async fn get_app_by_name(&self, name: &str) -> Result<Option<App>> {
        let row = sqlx::query("SELECT * FROM apps WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(row_app).transpose()
    }

    async fn get_app(&self, id: Uuid) -> Result<Option<App>> {
        let row = sqlx::query("SELECT * FROM apps WHERE id = $1")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(row_app).transpose()
    }

    async fn list_apps(&self) -> Result<Vec<App>> {
        let rows = sqlx::query("SELECT * FROM apps ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(row_app).collect()
    }

    async fn upsert_app(&self, a: &App) -> Result<()> {
        sqlx::query(
            "INSERT INTO apps (id, name, url, sdk_language, sdk_version, framework, checksum, error, method, app_version, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (id) DO UPDATE SET name = $2, url = $3, sdk_language = $4, sdk_version = $5,
               framework = $6, checksum = $7, error = $8, method = $9, app_version = $10",
        )
        .bind(a.id.to_string())
        .bind(&a.name)
        .bind(&a.url)
        .bind(&a.sdk_language)
        .bind(&a.sdk_version)
        .bind(&a.framework)
        .bind(&a.checksum)
        .bind(a.error.clone())
        .bind(&a.method)
        .bind(a.app_version.clone())
        .bind(a.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn set_app_error(&self, id: Uuid, error: Option<&str>) -> Result<()> {
        sqlx::query("UPDATE apps SET error = $2 WHERE id = $1")
            .bind(id.to_string())
            .bind(error.map(String::from))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_app(&self, id: Uuid) -> Result<()> {
        let mut tx = self.tx().await?;
        sqlx::query("UPDATE functions SET archived_at = $2 WHERE app_id = $1 AND archived_at IS NULL")
            .bind(id.to_string())
            .bind(now_ms())
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM apps WHERE id = $1")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.bump_generation().await
    }

    async fn list_functions(&self) -> Result<Vec<Function>> {
        let rows = sqlx::query("SELECT def FROM functions WHERE archived_at IS NULL")
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(|r| Ok(serde_json::from_str(&r.try_get::<String, _>("def")?)?))
            .collect()
    }

    async fn app_functions(&self, app_id: Uuid) -> Result<Vec<Function>> {
        let rows = sqlx::query(
            "SELECT def FROM functions WHERE app_id = $1 AND archived_at IS NULL ORDER BY slug",
        )
        .bind(app_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| Ok(serde_json::from_str(&r.try_get::<String, _>("def")?)?))
            .collect()
    }

    async fn sync_functions(&self, app_id: Uuid, fns: &[Function]) -> Result<()> {
        let now = now_ms();
        let mut tx = self.tx().await?;
        sqlx::query("UPDATE functions SET archived_at = $2 WHERE app_id = $1 AND archived_at IS NULL")
            .bind(app_id.to_string())
            .bind(now)
            .execute(&mut *tx)
            .await?;
        for f in fns {
            sqlx::query(
                "INSERT INTO functions (id, app_id, slug, name, version, config, def, created_at, updated_at, archived_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, NULL)
                 ON CONFLICT (id) DO UPDATE SET app_id = $2, slug = $3, name = $4, version = $5,
                   config = $6, def = $7, updated_at = $8, archived_at = NULL",
            )
            .bind(f.id.to_string())
            .bind(app_id.to_string())
            .bind(&f.slug)
            .bind(&f.name)
            .bind(f.version as i64)
            .bind(to_json(&f.config))
            .bind(to_json(f))
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.bump_generation().await
    }

    async fn functions_generation(&self) -> Result<i64> {
        let row = sqlx::query("SELECT value FROM generations WHERE id = 'functions'")
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.try_get::<i64, _>("value")).transpose()?.unwrap_or(0))
    }

    async fn insert_events(&self, events: &[TrackedEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut tx = self.tx().await?;
        for e in events {
            sqlx::query(
                "INSERT INTO events (internal_id, event_id, name, data, user_data, ts, v, received_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (internal_id) DO NOTHING",
            )
            .bind(ulid_s(e.internal_id))
            .bind(&e.event.id)
            .bind(&e.event.name)
            .bind(to_json(&e.event.data))
            .bind(e.event.user.as_ref().map(to_json))
            .bind(e.event.ts)
            .bind(e.event.v.clone())
            .bind(e.received_at)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn get_event(&self, internal_id: Ulid) -> Result<Option<TrackedEvent>> {
        let row = sqlx::query("SELECT * FROM events WHERE internal_id = $1")
            .bind(ulid_s(internal_id))
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(row_event).transpose()
    }

    async fn list_events(&self, q: &EventQuery) -> Result<Vec<TrackedEvent>> {
        let mut sql = String::from("SELECT * FROM events WHERE 1=1");
        let mut n = 0;
        let mut p = || {
            n += 1;
            format!("${n}")
        };
        if q.name.is_some() {
            sql += &format!(" AND name = {}", p());
        }
        if q.cursor.is_some() {
            sql += &format!(" AND internal_id < {}", p());
        }
        if q.received_after.is_some() {
            sql += &format!(" AND received_at >= {}", p());
        }
        if q.received_before.is_some() {
            sql += &format!(" AND received_at <= {}", p());
        }
        if !q.include_internal {
            sql += " AND name NOT LIKE 'inngest/%'";
        }
        sql += &format!(" ORDER BY internal_id DESC LIMIT {}", p());
        let mut query = sqlx::query(&sql);
        if let Some(name) = &q.name {
            query = query.bind(name.clone());
        }
        if let Some(c) = q.cursor {
            query = query.bind(ulid_s(c));
        }
        if let Some(a) = q.received_after {
            query = query.bind(a);
        }
        if let Some(b) = q.received_before {
            query = query.bind(b);
        }
        query = query.bind(q.limit);
        let rows = query.fetch_all(&self.pool).await?;
        rows.iter().map(row_event).collect()
    }

    async fn insert_run(&self, r: &RunRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO function_runs (run_id, fn_id, fn_slug, app_id, fn_version, event_id, batch_id, cron, original_run_id, started_at, status, ended_at, output)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) ON CONFLICT (run_id) DO NOTHING",
        )
        .bind(ulid_s(r.run_id))
        .bind(r.fn_id.to_string())
        .bind(&r.fn_slug)
        .bind(r.app_id.to_string())
        .bind(r.fn_version as i64)
        .bind(r.event_id.map(ulid_s))
        .bind(r.batch_id.map(ulid_s))
        .bind(r.cron.clone())
        .bind(r.original_run_id.map(ulid_s))
        .bind(r.started_at)
        .bind(r.status.as_str())
        .bind(r.ended_at)
        .bind(r.output.as_ref().map(to_json))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn finish_run(
        &self,
        run_id: Ulid,
        status: RunStatus,
        output: &Value,
        ended_at: i64,
    ) -> Result<()> {
        sqlx::query("UPDATE function_runs SET status = $2, output = $3, ended_at = $4 WHERE run_id = $1")
            .bind(ulid_s(run_id))
            .bind(status.as_str())
            .bind(to_json(output))
            .bind(ended_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_run(&self, run_id: Ulid) -> Result<Option<RunRecord>> {
        let row = sqlx::query(&format!("SELECT {RUN_COLS} FROM function_runs WHERE run_id = $1"))
            .bind(ulid_s(run_id))
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(row_run).transpose()
    }

    async fn runs_for_event(&self, event_id: Ulid) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query(&format!(
            "SELECT {RUN_COLS} FROM function_runs WHERE event_id = $1 ORDER BY run_id"
        ))
        .bind(ulid_s(event_id))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_run).collect()
    }

    async fn prune(&self, before: i64) -> Result<()> {
        sqlx::query("DELETE FROM events WHERE received_at < $1")
            .bind(before)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM function_runs WHERE ended_at IS NOT NULL AND ended_at < $1")
            .bind(before)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_runs(&self, q: &RunQuery) -> Result<Vec<RunRecord>> {
        let mut sql = format!("SELECT {RUN_COLS} FROM function_runs WHERE 1=1");
        let mut n = 0;
        let mut p = || {
            n += 1;
            format!("${n}")
        };
        if q.fn_id.is_some() {
            sql += &format!(" AND fn_id = {}", p());
        }
        if q.status.is_some() {
            sql += &format!(" AND status = {}", p());
        }
        if q.cursor.is_some() {
            sql += &format!(" AND run_id < {}", p());
        }
        sql += &format!(" ORDER BY run_id DESC LIMIT {}", p());
        let mut query = sqlx::query(&sql);
        if let Some(f) = q.fn_id {
            query = query.bind(f.to_string());
        }
        if let Some(s) = q.status {
            query = query.bind(s.as_str());
        }
        if let Some(c) = q.cursor {
            query = query.bind(ulid_s(c));
        }
        query = query.bind(q.limit);
        let rows = query.fetch_all(&self.pool).await?;
        rows.iter().map(row_run).collect()
    }
}

// ---------------------------------------------------------------------------
// StateStore
// ---------------------------------------------------------------------------

impl SqlStore {
    async fn save_step_tx(
        &self,
        tx: &mut Transaction<'static, Any>,
        run_id: Ulid,
        step_id: &str,
        value: &Value,
    ) -> Result<SaveStep> {
        let row = sqlx::query(&format!(
            "SELECT status, pending FROM run_state WHERE run_id = $1{}",
            if self.dialect == Dialect::Postgres { " FOR UPDATE" } else { "" }
        ))
        .bind(ulid_s(run_id))
        .fetch_optional(&mut **tx)
        .await?;
        let Some(row) = row else { return Ok(SaveStep::NoRun) };
        if RunStatus::parse(&row.try_get::<String, _>("status")?).is_terminal() {
            return Ok(SaveStep::NoRun);
        }
        let inserted = sqlx::query(
            "INSERT INTO run_steps (run_id, step_id, seq, data) VALUES ($1, $2, $3, $4)
             ON CONFLICT (run_id, step_id) DO NOTHING",
        )
        .bind(ulid_s(run_id))
        .bind(step_id)
        .bind(self.next_seq())
        .bind(to_json(value))
        .execute(&mut **tx)
        .await?
        .rows_affected();
        let pending: Option<String> = row.try_get("pending")?;
        let mut pending_empty = true;
        if let Some(p) = pending {
            let mut ids: Vec<String> = serde_json::from_str(&p).unwrap_or_default();
            ids.retain(|i| i != step_id);
            pending_empty = ids.is_empty();
            sqlx::query("UPDATE run_state SET pending = $2 WHERE run_id = $1")
                .bind(ulid_s(run_id))
                .bind(if ids.is_empty() { None } else { Some(to_json(&ids)) })
                .execute(&mut **tx)
                .await?;
        }
        Ok(SaveStep::Saved {
            pending_empty,
            duplicate: inserted == 0,
        })
    }
}

#[async_trait]
impl StateStore for SqlStore {
    async fn create_run(
        &self,
        meta: &RunMeta,
        events: &[Value],
        idempotency_ttl_ms: i64,
    ) -> Result<CreateRun> {
        let now = now_ms();
        let idem_key = format!("idem:{}", meta.idempotency_key);
        let mut tx = self.tx().await?;
        self.lock(&mut tx, &idem_key).await?;
        let existing = sqlx::query("SELECT value, expires_at FROM kv WHERE key = $1")
            .bind(&idem_key)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(row) = existing {
            let expires: Option<i64> = row.try_get("expires_at")?;
            if expires.is_none_or(|e| e > now) {
                return Ok(CreateRun::Duplicate(row.try_get("value")?));
            }
        }
        sqlx::query(
            "INSERT INTO kv (key, value, expires_at) VALUES ($1, $2, $3)
             ON CONFLICT (key) DO UPDATE SET value = $2, expires_at = $3",
        )
        .bind(&idem_key)
        .bind(ulid_s(meta.run_id))
        .bind(now + idempotency_ttl_ms)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO run_state (run_id, meta, events, pending, status, updated_at) VALUES ($1, $2, $3, NULL, $4, $5)",
        )
        .bind(ulid_s(meta.run_id))
        .bind(to_json(meta))
        .bind(to_json(&events))
        .bind(meta.status.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(CreateRun::Created)
    }

    async fn load_run(&self, run_id: Ulid) -> Result<Option<RunState>> {
        let Some(row) = sqlx::query("SELECT meta, events FROM run_state WHERE run_id = $1")
            .bind(ulid_s(run_id))
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        let meta: RunMeta = serde_json::from_str(&row.try_get::<String, _>("meta")?)?;
        let events: Vec<Value> = serde_json::from_str(&row.try_get::<String, _>("events")?)?;
        let steps = self.load_steps(run_id).await?;
        Ok(Some(RunState {
            meta,
            events,
            steps,
        }))
    }

    async fn load_meta(&self, run_id: Ulid) -> Result<Option<RunMeta>> {
        let row = sqlx::query("SELECT meta FROM run_state WHERE run_id = $1")
            .bind(ulid_s(run_id))
            .fetch_optional(&self.pool)
            .await?;
        match row {
            Some(r) => Ok(Some(serde_json::from_str(&r.try_get::<String, _>("meta")?)?)),
            None => Ok(None),
        }
    }

    async fn update_meta(&self, run_id: Ulid, u: &MetaUpdate) -> Result<()> {
        let mut tx = self.tx().await?;
        let row = sqlx::query(&format!(
            "SELECT meta FROM run_state WHERE run_id = $1{}",
            if self.dialect == Dialect::Postgres { " FOR UPDATE" } else { "" }
        ))
        .bind(ulid_s(run_id))
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else { return Ok(()) };
        let mut meta: RunMeta = serde_json::from_str(&row.try_get::<String, _>("meta")?)?;
        if let Some(s) = u.started_at {
            meta.started_at.get_or_insert(s);
        }
        if let Some(v) = u.request_version {
            meta.request_version = v;
        }
        if let Some(f) = u.force_step_plan {
            meta.force_step_plan = f;
        }
        sqlx::query("UPDATE run_state SET meta = $2, updated_at = $3 WHERE run_id = $1")
            .bind(ulid_s(run_id))
            .bind(to_json(&meta))
            .bind(now_ms())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn save_step(&self, run_id: Ulid, step_id: &str, value: &Value) -> Result<SaveStep> {
        let mut tx = self.tx().await?;
        let r = self.save_step_tx(&mut tx, run_id, step_id, value).await?;
        tx.commit().await?;
        Ok(r)
    }

    async fn set_pending(&self, run_id: Ulid, ids: &[String]) -> Result<()> {
        let mut tx = self.tx().await?;
        // Only ids without a saved result are pending.
        let mut remaining = Vec::with_capacity(ids.len());
        for id in ids {
            let exists = sqlx::query("SELECT 1 AS x FROM run_steps WHERE run_id = $1 AND step_id = $2")
                .bind(ulid_s(run_id))
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?
                .is_some();
            if !exists {
                remaining.push(id.clone());
            }
        }
        sqlx::query("UPDATE run_state SET pending = $2 WHERE run_id = $1")
            .bind(ulid_s(run_id))
            .bind(if remaining.is_empty() { None } else { Some(to_json(&remaining)) })
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn end_run(&self, run_id: Ulid, status: RunStatus, at: i64) -> Result<bool> {
        let mut tx = self.tx().await?;
        let row = sqlx::query(&format!(
            "SELECT meta, status FROM run_state WHERE run_id = $1{}",
            if self.dialect == Dialect::Postgres { " FOR UPDATE" } else { "" }
        ))
        .bind(ulid_s(run_id))
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else { return Ok(false) };
        if RunStatus::parse(&row.try_get::<String, _>("status")?).is_terminal() {
            return Ok(false);
        }
        let mut meta: RunMeta = serde_json::from_str(&row.try_get::<String, _>("meta")?)?;
        meta.status = status;
        meta.ended_at = Some(at);
        sqlx::query("UPDATE run_state SET status = $2, meta = $3, pending = NULL, updated_at = $4 WHERE run_id = $1")
            .bind(ulid_s(run_id))
            .bind(status.as_str())
            .bind(to_json(&meta))
            .bind(at)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }

    async fn load_steps(&self, run_id: Ulid) -> Result<Vec<(String, Value)>> {
        let rows = sqlx::query("SELECT step_id, data FROM run_steps WHERE run_id = $1 ORDER BY seq")
            .bind(ulid_s(run_id))
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(|r| {
                let data: String = r.try_get("data")?;
                Ok((r.try_get("step_id")?, serde_json::from_str(&data)?))
            })
            .collect()
    }

    async fn load_events(&self, run_id: Ulid) -> Result<Vec<Value>> {
        let row = sqlx::query("SELECT events FROM run_state WHERE run_id = $1")
            .bind(ulid_s(run_id))
            .fetch_optional(&self.pool)
            .await?;
        match row {
            Some(r) => Ok(serde_json::from_str(&r.try_get::<String, _>("events")?)?),
            None => Ok(vec![]),
        }
    }

    // ---- queue ----

    async fn enqueue(&self, item: &QueueItem) -> Result<bool> {
        if item.dedupe {
            let done = sqlx::query("SELECT 1 AS x FROM queue_done WHERE id = $1 AND expires_at > $2")
                .bind(&item.id)
                .bind(now_ms())
                .fetch_optional(&self.pool)
                .await?;
            if done.is_some() {
                return Ok(false);
            }
        }
        let n = sqlx::query(
            "INSERT INTO queue_items (id, kind, run_id, fn_id, at, attempt, max_attempts, payload, dedupe, lease_id, blocked_on)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, NULL) ON CONFLICT (id) DO NOTHING",
        )
        .bind(&item.id)
        .bind(item.kind.as_str())
        .bind(item.run_id.map(ulid_s))
        .bind(item.fn_id.map(|f| f.to_string()))
        .bind(item.at)
        .bind(item.attempt as i64)
        .bind(item.max_attempts as i64)
        .bind(to_json(&item.payload))
        .bind(item.dedupe as i64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n > 0)
    }

    async fn lease(&self, now: i64, limit: i64, lease_ms: i64) -> Result<Vec<QueueItem>> {
        let lease_id = Ulid::generate().to_string();
        // A leased item's `at` is its lease expiry, so expired leases become due again.
        let sql = format!(
            "UPDATE queue_items SET lease_id = $3, at = $4, blocked_on = NULL
             WHERE id IN (SELECT id FROM queue_items WHERE at <= $1 ORDER BY at LIMIT $2{})
             RETURNING {ITEM_COLS}",
            self.skip_locked()
        );
        let rows = match self.dialect {
            Dialect::Sqlite => {
                let mut tx = self.tx().await?;
                let rows = sqlx::query(&sql)
                    .bind(now)
                    .bind(limit)
                    .bind(&lease_id)
                    .bind(now + lease_ms)
                    .fetch_all(&mut *tx)
                    .await?;
                tx.commit().await?;
                rows
            }
            Dialect::Postgres => {
                sqlx::query(&sql)
                    .bind(now)
                    .bind(limit)
                    .bind(&lease_id)
                    .bind(now + lease_ms)
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        let mut items: Vec<QueueItem> = rows.iter().map(row_item).collect::<Result<_>>()?;
        items.sort_by_key(|i| i.at);
        Ok(items)
    }

    async fn extend_lease(&self, item_id: &str, lease_id: &str, until: i64) -> Result<bool> {
        let n = sqlx::query("UPDATE queue_items SET at = $3 WHERE id = $1 AND lease_id = $2")
            .bind(item_id)
            .bind(lease_id)
            .bind(until)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    async fn ack(&self, item: &QueueItem) -> Result<()> {
        let mut tx = self.tx().await?;
        sqlx::query("DELETE FROM queue_items WHERE id = $1 AND (lease_id = $2 OR lease_id IS NULL)")
            .bind(&item.id)
            .bind(item.lease_id.clone())
            .execute(&mut *tx)
            .await?;
        if item.dedupe {
            sqlx::query(
                "INSERT INTO queue_done (id, expires_at) VALUES ($1, $2)
                 ON CONFLICT (id) DO UPDATE SET expires_at = $2",
            )
            .bind(&item.id)
            .bind(now_ms() + DONE_TTL_MS)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn requeue(
        &self,
        item: &QueueItem,
        at: i64,
        attempt: u32,
        blocked_on: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE queue_items SET at = $3, attempt = $4, lease_id = NULL, blocked_on = $5, payload = $6
             WHERE id = $1 AND lease_id = $2",
        )
        .bind(&item.id)
        .bind(item.lease_id.clone())
        .bind(at)
        .bind(attempt as i64)
        .bind(blocked_on.map(String::from))
        .bind(to_json(&item.payload))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn reschedule(&self, item_id: &str, at: i64) -> Result<bool> {
        let n = sqlx::query("UPDATE queue_items SET at = $2 WHERE id = $1 AND lease_id IS NULL")
            .bind(item_id)
            .bind(at)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    async fn remove_item(&self, item_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM queue_items WHERE id = $1")
            .bind(item_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn remove_run_items(&self, run_id: Ulid, except: Option<&str>) -> Result<()> {
        sqlx::query("DELETE FROM queue_items WHERE run_id = $1 AND id <> $2")
            .bind(ulid_s(run_id))
            .bind(except.unwrap_or(""))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn run_items(&self, run_id: Ulid, limit: i64) -> Result<Vec<QueueItem>> {
        let rows = sqlx::query(&format!(
            "SELECT {ITEM_COLS} FROM queue_items WHERE run_id = $1 ORDER BY at LIMIT $2"
        ))
        .bind(ulid_s(run_id))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_item).collect()
    }

    async fn next_due(&self) -> Result<Option<i64>> {
        let row = sqlx::query("SELECT MIN(at) AS at FROM queue_items")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.try_get::<Option<i64>, _>("at")?)
    }

    async fn wake_blocked(&self, keys: &[String], n: i64) -> Result<()> {
        let now = now_ms();
        for key in keys {
            sqlx::query(&format!(
                "UPDATE queue_items SET at = $2, blocked_on = NULL WHERE id IN
                 (SELECT id FROM queue_items WHERE blocked_on = $1 AND lease_id IS NULL ORDER BY at LIMIT $3{})",
                self.skip_locked()
            ))
            .bind(key)
            .bind(now)
            .bind(n)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    // ---- concurrency ----

    async fn acquire_concurrency(
        &self,
        holder: &str,
        keys: &[ConcurrencyKey],
        expires_at: i64,
    ) -> Result<Option<String>> {
        if keys.is_empty() {
            return Ok(None);
        }
        let now = now_ms();
        let mut tx = self.tx().await?;
        let mut sorted: Vec<&ConcurrencyKey> = keys.iter().collect();
        sorted.sort_by(|a, b| a.key.cmp(&b.key));
        for k in &sorted {
            self.lock(&mut tx, &format!("c:{}", k.key)).await?;
        }
        for k in &sorted {
            let row = sqlx::query(
                "SELECT COUNT(*) AS n FROM concurrency WHERE key = $1 AND expires_at > $2 AND holder <> $3",
            )
            .bind(&k.key)
            .bind(now)
            .bind(holder)
            .fetch_one(&mut *tx)
            .await?;
            let n: i64 = row.try_get("n")?;
            if n >= k.limit as i64 {
                return Ok(Some(k.key.clone()));
            }
        }
        for k in &sorted {
            sqlx::query(
                "INSERT INTO concurrency (key, holder, expires_at) VALUES ($1, $2, $3)
                 ON CONFLICT (key, holder) DO UPDATE SET expires_at = $3",
            )
            .bind(&k.key)
            .bind(holder)
            .bind(expires_at)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(None)
    }

    async fn release_concurrency(&self, holder: &str) -> Result<Vec<String>> {
        let rows = sqlx::query("DELETE FROM concurrency WHERE holder = $1 RETURNING key")
            .bind(holder)
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(|r| Ok(r.try_get::<String, _>("key")?))
            .collect()
    }

    async fn extend_concurrency(&self, holder: &str, expires_at: i64) -> Result<()> {
        sqlx::query("UPDATE concurrency SET expires_at = $2 WHERE holder = $1")
            .bind(holder)
            .bind(expires_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn concurrency_in_use(&self, key: &str) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) AS n FROM concurrency WHERE key = $1 AND expires_at > $2")
            .bind(key)
            .bind(now_ms())
            .fetch_one(&self.pool)
            .await?;
        Ok(row.try_get("n")?)
    }

    // ---- rate limiting ----

    async fn gcra(&self, key: &str, now: i64, emission_ms: i64, tolerance_ms: i64) -> Result<Gcra> {
        let k = format!("gcra:{key}");
        let mut tx = self.tx().await?;
        self.lock(&mut tx, &k).await?;
        let row = sqlx::query("SELECT value FROM kv WHERE key = $1")
            .bind(&k)
            .fetch_optional(&mut *tx)
            .await?;
        let tat: i64 = match row {
            Some(r) => r.try_get::<String, _>("value")?.parse().unwrap_or(now),
            None => now,
        }
        .max(now);
        if tat - now > tolerance_ms {
            return Ok(Err(tat - tolerance_ms));
        }
        let new_tat = tat + emission_ms;
        sqlx::query(
            "INSERT INTO kv (key, value, expires_at) VALUES ($1, $2, $3)
             ON CONFLICT (key) DO UPDATE SET value = $2, expires_at = $3",
        )
        .bind(&k)
        .bind(new_tat.to_string())
        .bind(new_tat + 1000)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Ok(()))
    }

    // ---- key/value ----

    async fn kv_set_nx(
        &self,
        key: &str,
        value: &str,
        ttl_ms: Option<i64>,
    ) -> Result<Option<String>> {
        let now = now_ms();
        let mut tx = self.tx().await?;
        self.lock(&mut tx, key).await?;
        let row = sqlx::query("SELECT value, expires_at FROM kv WHERE key = $1")
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(r) = row {
            let exp: Option<i64> = r.try_get("expires_at")?;
            if exp.is_none_or(|e| e > now) {
                return Ok(Some(r.try_get("value")?));
            }
        }
        sqlx::query(
            "INSERT INTO kv (key, value, expires_at) VALUES ($1, $2, $3)
             ON CONFLICT (key) DO UPDATE SET value = $2, expires_at = $3",
        )
        .bind(key)
        .bind(value)
        .bind(ttl_ms.map(|t| now + t))
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(None)
    }

    async fn kv_get(&self, key: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT value FROM kv WHERE key = $1 AND (expires_at IS NULL OR expires_at > $2)")
            .bind(key)
            .bind(now_ms())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.try_get("value")).transpose()?)
    }

    async fn kv_del(&self, key: &str, if_value: Option<&str>) -> Result<bool> {
        let n = match if_value {
            Some(v) => sqlx::query("DELETE FROM kv WHERE key = $1 AND value = $2")
                .bind(key)
                .bind(v)
                .execute(&self.pool)
                .await?
                .rows_affected(),
            None => sqlx::query("DELETE FROM kv WHERE key = $1")
                .bind(key)
                .execute(&self.pool)
                .await?
                .rows_affected(),
        };
        Ok(n > 0)
    }

    // ---- pauses ----

    async fn save_pause(&self, p: &Pause) -> Result<bool> {
        let index = match p.kind {
            PauseKind::Signal => p.event.as_ref().map(|s| format!("signal:{s}")),
            PauseKind::Invoke => None,
            _ => p.event.clone(),
        };
        let n = sqlx::query(
            "INSERT INTO pauses (id, run_id, kind, event, data, expires_at) VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(p.id.to_string())
        .bind(ulid_s(p.run_id))
        .bind(to_json(&p.kind).trim_matches('"').to_string())
        .bind(index)
        .bind(to_json(p))
        .bind(p.expires_at)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n > 0)
    }

    async fn get_pause(&self, id: Uuid) -> Result<Option<Pause>> {
        let row = sqlx::query("SELECT data FROM pauses WHERE id = $1")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(row_pause).transpose()
    }

    async fn pauses_for_event(&self, event: &str) -> Result<Vec<Pause>> {
        let rows = sqlx::query("SELECT data FROM pauses WHERE event = $1")
            .bind(event)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(row_pause).collect()
    }

    async fn pauses_for_signal(&self, signal: &str) -> Result<Vec<Pause>> {
        let rows = sqlx::query("SELECT data FROM pauses WHERE event = $1")
            .bind(format!("signal:{signal}"))
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(row_pause).collect()
    }

    async fn consume_pause(&self, p: &Pause, value: Option<&Value>) -> Result<Option<bool>> {
        let mut tx = self.tx().await?;
        let n = sqlx::query("DELETE FROM pauses WHERE id = $1")
            .bind(p.id.to_string())
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if n == 0 {
            return Ok(None);
        }
        let out = match value {
            Some(v) => match self.save_step_tx(&mut tx, p.run_id, &p.step_id, v).await? {
                SaveStep::Saved {
                    pending_empty,
                    duplicate: false,
                } => Some(pending_empty),
                _ => None,
            },
            None => Some(true),
        };
        tx.commit().await?;
        Ok(out)
    }

    async fn delete_pause(&self, id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM pauses WHERE id = $1")
            .bind(id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_run_pauses(&self, run_id: Ulid) -> Result<()> {
        sqlx::query("DELETE FROM pauses WHERE run_id = $1")
            .bind(ulid_s(run_id))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ---- debounce ----

    async fn debounce_upsert(
        &self,
        key: &str,
        fn_id: Uuid,
        event_id: Ulid,
        event: &Value,
        event_ts: i64,
        period_ms: i64,
        timeout_ms: Option<i64>,
        now: i64,
    ) -> Result<DebounceUpsert> {
        let mut tx = self.tx().await?;
        self.lock(&mut tx, &format!("d:{key}")).await?;
        let row = sqlx::query("SELECT id, event_ts, timeout_at FROM debounces WHERE key = $1")
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?;
        let wait = |timeout_at: Option<i64>| -> i64 {
            let mut ttl = period_ms;
            if let Some(t) = timeout_at {
                ttl = ttl.min(t - now);
            }
            now + ttl.max(1000)
        };
        let out = match row {
            Some(r) => {
                let id: String = r.try_get("id")?;
                let stored_ts: i64 = r.try_get("event_ts")?;
                let timeout_at: Option<i64> = r.try_get("timeout_at")?;
                if stored_ts > event_ts {
                    return Ok(DebounceUpsert {
                        id,
                        fire_at: 0,
                        created: false,
                        ignored: true,
                    });
                }
                let fire_at = wait(timeout_at);
                sqlx::query("UPDATE debounces SET event = $2, event_ts = $3, fire_at = $4, event_id = $5 WHERE key = $1")
                    .bind(key)
                    .bind(to_json(event))
                    .bind(event_ts)
                    .bind(fire_at)
                    .bind(ulid_s(event_id))
                    .execute(&mut *tx)
                    .await?;
                DebounceUpsert {
                    id,
                    fire_at,
                    created: false,
                    ignored: false,
                }
            }
            None => {
                let id = Ulid::generate().to_string();
                let timeout_at = timeout_ms.map(|t| now + t);
                let fire_at = wait(timeout_at);
                sqlx::query(
                    "INSERT INTO debounces (key, id, fn_id, event, event_ts, timeout_at, fire_at, event_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                )
                .bind(key)
                .bind(&id)
                .bind(fn_id.to_string())
                .bind(to_json(event))
                .bind(event_ts)
                .bind(timeout_at)
                .bind(fire_at)
                .bind(ulid_s(event_id))
                .execute(&mut *tx)
                .await?;
                DebounceUpsert {
                    id,
                    fire_at,
                    created: true,
                    ignored: false,
                }
            }
        };
        tx.commit().await?;
        Ok(out)
    }

    async fn debounce_take(&self, key: &str, id: &str) -> Result<Option<(Ulid, Value)>> {
        let rows = sqlx::query("DELETE FROM debounces WHERE key = $1 AND id = $2 RETURNING event_id, event")
            .bind(key)
            .bind(id)
            .fetch_all(&self.pool)
            .await?;
        match rows.first() {
            Some(r) => Ok(Some((
                parse_ulid(&r.try_get::<String, _>("event_id")?)?,
                serde_json::from_str(&r.try_get::<String, _>("event")?)?,
            ))),
            None => Ok(None),
        }
    }

    // ---- batching ----

    async fn batch_append(
        &self,
        key: &str,
        fn_id: Uuid,
        event: &Value,
        event_id: Ulid,
        max_size: u32,
    ) -> Result<BatchAppend> {
        let mut tx = self.tx().await?;
        self.lock(&mut tx, &format!("b:{key}")).await?;
        let row = sqlx::query("SELECT id, len FROM batches WHERE key = $1 AND open = 1")
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?;
        let (batch_id, len, created) = match row {
            Some(r) => (
                parse_ulid(&r.try_get::<String, _>("id")?)?,
                r.try_get::<i64, _>("len")?,
                false,
            ),
            None => {
                let id = Ulid::generate();
                sqlx::query(
                    "INSERT INTO batches (id, key, fn_id, len, open, created_at) VALUES ($1, $2, $3, 0, 1, $4)",
                )
                .bind(ulid_s(id))
                .bind(key)
                .bind(fn_id.to_string())
                .bind(now_ms())
                .execute(&mut *tx)
                .await?;
                (id, 0, true)
            }
        };
        let inserted = sqlx::query(
            "INSERT INTO batch_events (batch_id, event_id, seq, event) VALUES ($1, $2, $3, $4)
             ON CONFLICT (batch_id, event_id) DO NOTHING",
        )
        .bind(ulid_s(batch_id))
        .bind(ulid_s(event_id))
        .bind(len)
        .bind(to_json(event))
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let len = len + inserted as i64;
        let full = len >= max_size as i64;
        sqlx::query("UPDATE batches SET len = $2, open = $3 WHERE id = $1")
            .bind(ulid_s(batch_id))
            .bind(len)
            .bind(if full { 0i64 } else { 1i64 })
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(BatchAppend {
            batch_id,
            len: len as u32,
            created,
            duplicate: inserted == 0,
        })
    }

    async fn batch_take(&self, _key: &str, batch_id: Ulid) -> Result<Option<Vec<(Ulid, Value)>>> {
        let mut tx = self.tx().await?;
        let n = sqlx::query("DELETE FROM batches WHERE id = $1")
            .bind(ulid_s(batch_id))
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if n == 0 {
            return Ok(None);
        }
        let rows = sqlx::query("DELETE FROM batch_events WHERE batch_id = $1 RETURNING seq, event_id, event")
            .bind(ulid_s(batch_id))
            .fetch_all(&mut *tx)
            .await?;
        tx.commit().await?;
        let mut evts: Vec<(i64, Ulid, Value)> = rows
            .iter()
            .map(|r| {
                Ok((
                    r.try_get::<i64, _>("seq")?,
                    parse_ulid(&r.try_get::<String, _>("event_id")?)?,
                    serde_json::from_str(&r.try_get::<String, _>("event")?)?,
                ))
            })
            .collect::<Result<_>>()?;
        evts.sort_by_key(|(s, _, _)| *s);
        Ok(Some(evts.into_iter().map(|(_, id, e)| (id, e)).collect()))
    }

    async fn gc(&self, now: i64) -> Result<()> {
        let retention = self.retention_ms.load(Ordering::Relaxed);
        if retention > 0 {
            let cutoff = now - retention;
            sqlx::query(
                "DELETE FROM run_steps WHERE run_id IN
                 (SELECT run_id FROM run_state WHERE status <> 'Running' AND updated_at < $1)",
            )
            .bind(cutoff)
            .execute(&self.pool)
            .await?;
            sqlx::query("DELETE FROM run_state WHERE status <> 'Running' AND updated_at < $1")
                .bind(cutoff)
                .execute(&self.pool)
                .await?;
        }
        sqlx::query("DELETE FROM queue_done WHERE expires_at < $1")
            .bind(now)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM kv WHERE expires_at IS NOT NULL AND expires_at < $1")
            .bind(now)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM concurrency WHERE expires_at < $1")
            .bind(now)
            .execute(&self.pool)
            .await?;
        // Expired pauses whose timeout already fired linger only for cancel pauses.
        sqlx::query("DELETE FROM pauses WHERE kind = 'cancel' AND expires_at < $1")
            .bind(now - 20 * 60 * 1000)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
