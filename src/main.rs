use clap::Parser;
use std::io::IsTerminal;
use oxidle::config::{Cli, Command, Config};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let level = if cli.verbose { "debug" } else { cli.log_level.as_str() };
    let filter = EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| {
        EnvFilter::new(format!("oxidle={level},tower_http=warn,sqlx=warn,{level}"))
    });
    let json = cli.json || std::env::var("LOG_HANDLER").is_ok_and(|h| h == "json");
    if json {
        tracing_subscriber::fmt().with_env_filter(filter).json().init();
    } else {
        // No color codes when logs go to a file or container log driver.
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(std::io::stdout().is_terminal())
            .init();
    }

    let (args, dev) = match cli.command {
        Command::Start(a) => (a, false),
        Command::Dev(a) => (a, true),
    };
    let cfg = match Config::from_args(args, dev) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = oxidle::run(cfg).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}
