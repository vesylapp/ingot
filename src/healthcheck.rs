//! `ingot healthcheck`: probe a running server from inside its container, so container
//! healthchecks don't need curl. Exits 0 when the endpoint answers 200, 1 otherwise.

use crate::config::HealthcheckArgs;
use std::time::Duration;

pub async fn run(args: &HealthcheckArgs) -> Result<(), String> {
    let timeout = crate::util::parse_duration_ms(&args.timeout)
        .filter(|ms| *ms > 0)
        .map(|ms| Duration::from_millis(ms as u64))
        .ok_or_else(|| format!("healthcheck: invalid --timeout {:?}", args.timeout))?;
    let path = if args.ready { "/ready" } else { "/health" };
    let url = format!("{}://{}:{}{path}", args.scheme, args.host, args.port);
    let fail = |msg: String| Err(format!("healthcheck: api {url}: {msg}"));

    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| format!("healthcheck: {e}"))?;
    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            // reqwest's own message is generic; the cause (e.g. connection refused) is below it.
            let mut msg = e.to_string();
            let mut source = std::error::Error::source(&e);
            while let Some(cause) = source {
                msg = format!("{msg}: {cause}");
                source = cause.source();
            }
            return fail(msg);
        }
    };
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    if args.verbose {
        let body = resp.text().await.unwrap_or_default();
        let snippet: String = body.chars().take(200).collect();
        return fail(format!("status {status}: {snippet}"));
    }
    fail(format!("status {status}"))
}
