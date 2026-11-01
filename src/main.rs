use clap::Parser;
use oxidle::config::{AlphaCommand, Cli, Command, Config, DoctorCommand};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let healthcheck = match &cli.command {
        Command::Healthcheck(a) => Some(a),
        Command::Alpha {
            command: AlphaCommand::Doctor {
                command: DoctorCommand::Healthcheck(a),
            },
        } => Some(a),
        _ => None,
    };
    if let Some(args) = healthcheck {
        if let Err(e) = oxidle::healthcheck::run(args).await {
            eprintln!("{e}");
            std::process::exit(1);
        }
        return;
    }

    let level = if cli.verbose { "debug" } else { cli.log_level.as_str() };
    let filter = EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| {
        EnvFilter::new(format!("oxidle={level},tower_http=warn,sqlx=warn,{level}"))
    });
    let json = cli.json || std::env::var("LOG_HANDLER").is_ok_and(|h| h == "json");
    oxidle::logging::init(json, filter);

    let (args, dev) = match cli.command {
        Command::Start(a) => (a, false),
        Command::Dev(a) => (a, true),
        Command::Healthcheck(_) | Command::Alpha { .. } => unreachable!("handled above"),
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
