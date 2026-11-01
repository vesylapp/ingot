//! oxidle: an Inngest-compatible durable execution server.

pub mod api;
pub mod auth;
pub mod config;
pub mod cron;
pub mod engine;
pub mod function;
pub mod healthcheck;
pub mod logging;
pub mod metrics;
pub mod store;
pub mod types;
pub mod util;

use std::sync::Arc;

/// Build the stores and engine from config and serve until shutdown.
pub async fn run(cfg: config::Config) -> anyhow::Result<()> {
    let sql_url = match &cfg.postgres_uri {
        Some(u) => u.clone(),
        None => {
            std::fs::create_dir_all(&cfg.sqlite_dir)?;
            format!("sqlite://{}/main.db?mode=rwc", cfg.sqlite_dir.trim_end_matches('/'))
        }
    };
    let pool_cfg = store::sql::PoolConfig {
        max_open: cfg.pg_max_open,
        max_idle: cfg.pg_max_idle,
        idle_timeout: std::time::Duration::from_secs(cfg.pg_idle_mins * 60),
        max_lifetime: std::time::Duration::from_secs(cfg.pg_lifetime_mins * 60),
    };
    let sql = Arc::new(store::sql::SqlStore::connect(&sql_url, pool_cfg).await?);
    sql.set_retention_ms(cfg.retention_days as i64 * 86_400_000);
    tracing::info!(
        backend = if cfg.postgres_uri.is_some() { "postgres" } else { "sqlite" },
        "persistence ready"
    );
    let meta: Arc<dyn store::MetaStore> = sql.clone();
    let state: Arc<dyn store::StateStore> = match &cfg.redis_uri {
        Some(uri) => {
            let prefix = std::env::var("OXIDLE_REDIS_PREFIX").unwrap_or_else(|_| "oxidle".into());
            let r = store::redis::RedisStore::connect(uri, &prefix).await?;
            tracing::info!("execution state: redis");
            Arc::new(r)
        }
        None => sql.clone(),
    };

    let addr = format!("{}:{}", cfg.host, cfg.port);
    let grace = std::time::Duration::from_secs(cfg.shutdown_timeout_s);
    let (engine, events_rx) = engine::Engine::new(cfg, meta, state);
    engine.start(events_rx).await?;
    let app = api::router(engine.clone());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(addr = %addr, kind = engine.cfg.server_kind(), "oxidle listening");

    let (stop_http, mut http_stopped) = tokio::sync::watch::channel(false);
    let mut server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = http_stopped.wait_for(|s| *s).await;
            })
            .await
    });
    tokio::select! {
        _ = shutdown_signal() => {}
        res = &mut server => {
            // The server stopped on its own (e.g. the listener failed).
            res??;
            return Ok(());
        }
    }
    engine
        .shutdown(grace, async {
            stop_http.send_replace(true);
            if let Ok(Err(err)) = (&mut server).await {
                tracing::warn!(error = %err, "HTTP server error during shutdown");
            }
        })
        .await;
    server.abort();
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
    tracing::info!("shutting down");
}
