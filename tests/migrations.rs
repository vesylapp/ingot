//! Schema migrations: idempotent, and safe when several nodes start at once.
//!
//! The Postgres test runs when `INGOT_TEST_POSTGRES` is set
//! (e.g. `postgres://postgres:pw@127.0.0.1:5499/postgres`).

use ingot::store::sql::{PoolConfig, SqlStore};
use sqlx::Row;
use ulid::Ulid;

async fn versions(url: &str) -> Vec<i64> {
    sqlx::any::install_default_drivers();
    let pool = sqlx::AnyPool::connect(url).await.unwrap();
    let rows = sqlx::query("SELECT version FROM schema_migrations ORDER BY version")
        .fetch_all(&pool)
        .await
        .unwrap();
    rows.iter().map(|r| r.get::<i64, _>("version")).collect()
}

#[tokio::test]
async fn sqlite_migrations_are_idempotent() {
    let path = std::env::temp_dir().join(format!("ingot-mig-{}.db", Ulid::generate()));
    let url = format!("sqlite://{}?mode=rwc", path.display());
    for _ in 0..3 {
        SqlStore::connect(&url, PoolConfig::default()).await.unwrap();
    }
    let v = versions(&url).await;
    assert!(v.len() >= 3);
    assert_eq!(v, (1..=v.len() as i64).collect::<Vec<_>>());
    for ext in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{ext}", path.display()));
    }
}

#[tokio::test]
async fn postgres_nodes_starting_together() {
    let Ok(admin_url) = std::env::var("INGOT_TEST_POSTGRES") else {
        eprintln!("skipped: set INGOT_TEST_POSTGRES");
        return;
    };
    // A fresh database, so every migration runs while the nodes race.
    sqlx::any::install_default_drivers();
    let admin = sqlx::AnyPool::connect(&admin_url).await.unwrap();
    let db = format!("ingot_mig_{}", Ulid::generate().to_string().to_lowercase());
    sqlx::query(&format!("CREATE DATABASE {db}")).execute(&admin).await.unwrap();
    let base = admin_url.rsplit_once('/').unwrap().0;
    let url = format!("{base}/{db}");

    let nodes: Vec<_> = (0..8)
        .map(|_| {
            let url = url.clone();
            tokio::spawn(async move { SqlStore::connect(&url, PoolConfig::default()).await })
        })
        .collect();
    for n in nodes {
        n.await.unwrap().expect("every node starts");
    }
    let v = versions(&url).await;
    assert_eq!(v, (1..=v.len() as i64).collect::<Vec<_>>());
    assert!(v.len() >= 3);

    sqlx::query(&format!("DROP DATABASE {db} WITH (FORCE)"))
        .execute(&admin)
        .await
        .unwrap();
}
