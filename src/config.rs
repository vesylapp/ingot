//! Command line / environment configuration, mirroring `inngest start` and `inngest dev`.

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "oxidle", version, about = "Inngest-compatible durable execution server")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Output logs as JSON.
    #[arg(long, global = true)]
    pub json: bool,

    /// Log level (trace, debug, info, warn, error).
    #[arg(short = 'l', long, global = true, env = "LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Enable verbose (debug) logging.
    #[arg(long, global = true)]
    pub verbose: bool,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the self-hosted server (production mode: keys required, persistence on).
    Start(ServerArgs),
    /// Run in development mode (no keys required, server kind "dev").
    Dev(ServerArgs),
    /// Probe a running server's HTTP endpoint; exits non-zero on failure (container healthchecks).
    Healthcheck(HealthcheckArgs),
    /// Inngest's command path, `alpha doctor healthcheck`, for existing healthcheck configs.
    #[command(hide = true)]
    Alpha {
        #[command(subcommand)]
        command: AlphaCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum AlphaCommand {
    Doctor {
        #[command(subcommand)]
        command: DoctorCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum DoctorCommand {
    Healthcheck(HealthcheckArgs),
}

/// Same flags and defaults as `inngest alpha doctor healthcheck`, plus `--ready`.
#[derive(Args, Debug, Clone)]
pub struct HealthcheckArgs {
    /// Server host.
    #[arg(long, env = "INNGEST_HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// Server port.
    #[arg(short = 'p', long, env = "INNGEST_PORT", default_value_t = 8288)]
    pub port: u16,

    /// URL scheme: http or https.
    #[arg(long, default_value = "http")]
    pub scheme: String,

    /// Probe timeout (e.g. 5s, 500ms).
    #[arg(long, default_value = "5s")]
    pub timeout: String,

    /// Probe /ready (stores reachable, not shutting down) instead of /health (process up).
    #[arg(long)]
    pub ready: bool,

    /// On failure, also print the response status and the start of the body.
    #[arg(long)]
    pub verbose: bool,

    /// Accepted for compatibility; oxidle has no Connect gateway to probe.
    #[arg(long, hide = true)]
    pub skip_connect_gateway: bool,

    /// Accepted for compatibility; oxidle has no Connect gateway to probe.
    #[arg(long, hide = true)]
    pub connect_gateway_port: Option<u16>,
}

#[derive(Args, Debug, Clone)]
pub struct ServerArgs {
    /// Path to a config file (json or yaml) — accepted for compatibility.
    #[arg(long, env = "INNGEST_CONFIG")]
    pub config: Option<String>,

    #[arg(long, env = "INNGEST_HOST", default_value = "0.0.0.0")]
    pub host: String,

    #[arg(short = 'p', long, env = "INNGEST_PORT", default_value_t = 8288)]
    pub port: u16,

    /// App URLs to sync on startup.
    #[arg(short = 'u', long = "sdk-url", env = "INNGEST_SDK_URL", value_delimiter = ',')]
    pub sdk_url: Vec<String>,

    /// Signing key (hex, optionally prefixed with signkey-<env>-).
    #[arg(long, env = "INNGEST_SIGNING_KEY")]
    pub signing_key: Option<String>,

    /// Event key(s) accepted by the event API.
    #[arg(long = "event-key", env = "INNGEST_EVENT_KEY", value_delimiter = ',')]
    pub event_key: Vec<String>,

    /// Directory for the SQLite database (main.db). Used when no Postgres URI is set.
    #[arg(long, env = "INNGEST_SQLITE_DIR", default_value = "./.inngest")]
    pub sqlite_dir: String,

    /// Redis URI for execution state and the queue.
    #[arg(long, env = "INNGEST_REDIS_URI")]
    pub redis_uri: Option<String>,

    /// Postgres URI for persistence (defaults to SQLite).
    #[arg(long, env = "INNGEST_POSTGRES_URI")]
    pub postgres_uri: Option<String>,

    #[arg(long, env = "INNGEST_POSTGRES_MAX_IDLE_CONNS", default_value_t = 10)]
    pub postgres_max_idle_conns: u32,

    #[arg(long, env = "INNGEST_POSTGRES_MAX_OPEN_CONNS", default_value_t = 100)]
    pub postgres_max_open_conns: u32,

    /// Minutes.
    #[arg(long, env = "INNGEST_POSTGRES_CONN_MAX_IDLE_TIME", default_value_t = 5)]
    pub postgres_conn_max_idle_time: u64,

    /// Minutes.
    #[arg(long, env = "INNGEST_POSTGRES_CONN_MAX_LIFETIME", default_value_t = 30)]
    pub postgres_conn_max_lifetime: u64,

    /// Seconds between app re-sync attempts for apps with errors.
    #[arg(long, env = "INNGEST_POLL_INTERVAL", default_value_t = 0)]
    pub poll_interval: u64,

    /// Fixed retry interval in seconds (default: exponential backoff table).
    #[arg(long, env = "INNGEST_RETRY_INTERVAL", default_value_t = 0)]
    pub retry_interval: u64,

    /// Number of concurrent queue workers.
    #[arg(long, env = "INNGEST_QUEUE_WORKERS", default_value_t = 100)]
    pub queue_workers: usize,

    /// Queue poll interval in milliseconds.
    #[arg(long, env = "INNGEST_TICK", default_value_t = 150)]
    pub tick: u64,

    #[arg(long, env = "INNGEST_CONNECT_GATEWAY_PORT", default_value_t = 8289)]
    pub connect_gateway_port: u16,

    /// Disable the UI and GraphQL API.
    #[arg(long, env = "INNGEST_NO_UI")]
    pub no_ui: bool,

    /// Directory with the built dashboard UI (default: ./ui/dist, then <binary dir>/ui).
    #[arg(long, env = "OXIDLE_UI_DIR")]
    pub ui_dir: Option<String>,

    /// Days to retain finished run state and events (0 = forever).
    #[arg(long, env = "OXIDLE_RETENTION_DAYS", default_value_t = 7)]
    pub retention_days: u64,

    /// Seconds to drain in-flight work on SIGTERM before exiting. Keep it below the
    /// orchestrator's kill timeout (Kubernetes: terminationGracePeriodSeconds, default 30).
    #[arg(long, env = "OXIDLE_SHUTDOWN_TIMEOUT", default_value_t = 25)]
    pub shutdown_timeout: u64,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub sdk_urls: Vec<String>,
    pub signing_key: Option<String>,
    pub event_keys: Vec<String>,
    pub sqlite_dir: String,
    pub redis_uri: Option<String>,
    pub postgres_uri: Option<String>,
    pub pg_max_idle: u32,
    pub pg_max_open: u32,
    pub pg_idle_mins: u64,
    pub pg_lifetime_mins: u64,
    pub poll_interval_s: u64,
    pub retry_interval_s: Option<u64>,
    pub queue_workers: usize,
    pub tick_ms: u64,
    pub no_ui: bool,
    pub ui_dir: Option<String>,
    pub dev: bool,
    pub retention_days: u64,
    pub shutdown_timeout_s: u64,
}

impl Config {
    pub fn from_args(a: ServerArgs, dev: bool) -> anyhow::Result<Config> {
        let event_keys: Vec<String> = a
            .event_key
            .into_iter()
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty())
            .collect();
        let signing_key = a.signing_key.filter(|k| !k.trim().is_empty());
        if !dev {
            if event_keys.is_empty() {
                anyhow::bail!("Error: at least one event-key is required");
            }
            match &signing_key {
                None => anyhow::bail!("Error: signing-key is required"),
                Some(k) => {
                    let norm = crate::auth::normalize_key(k);
                    if hex::decode(norm).is_err() {
                        anyhow::bail!("Error: signing-key must be a valid hexadecimal string");
                    }
                }
            }
        }
        if a.postgres_max_open_conns <= 1 && a.postgres_uri.is_some() {
            anyhow::bail!("postgres-max-open-conns must be greater than 1");
        }
        Ok(Config {
            host: a.host,
            port: a.port,
            sdk_urls: a
                .sdk_url
                .into_iter()
                .map(|u| u.trim().to_string())
                .filter(|u| !u.is_empty())
                .collect(),
            signing_key,
            event_keys,
            sqlite_dir: a.sqlite_dir,
            redis_uri: a.redis_uri.filter(|s| !s.is_empty()),
            postgres_uri: a.postgres_uri.filter(|s| !s.is_empty()),
            pg_max_idle: a.postgres_max_idle_conns,
            pg_max_open: a.postgres_max_open_conns,
            pg_idle_mins: a.postgres_conn_max_idle_time,
            pg_lifetime_mins: a.postgres_conn_max_lifetime,
            poll_interval_s: if a.poll_interval == 0 { 5 } else { a.poll_interval },
            retry_interval_s: Some(a.retry_interval).filter(|n| *n > 0),
            queue_workers: a.queue_workers.max(1),
            tick_ms: a.tick.max(10),
            no_ui: a.no_ui,
            ui_dir: a.ui_dir,
            dev,
            retention_days: a.retention_days,
            shutdown_timeout_s: a.shutdown_timeout,
        })
    }

    pub fn server_kind(&self) -> &'static str {
        if self.dev { "dev" } else { "cloud" }
    }
}
