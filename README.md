# oxidle

An Inngest-compatible durable execution server written in Rust. It is a drop-in replacement
for the self-hosted Inngest server (`inngest start`): existing Inngest SDK apps send events,
sync, and execute against it unchanged.

## Quick start

```sh
cargo build --release

# Self-hosted mode (same flags/env vars as `inngest start`)
./target/release/oxidle start \
  --event-key my-event-key \
  --signing-key 8b1d8a3c4f0e2d9b7a6c5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b \
  --sdk-url http://localhost:3000/api/inngest

# Development mode (no keys required, server kind "dev")
./target/release/oxidle dev
```

## Docker

Multi-arch images (linux/amd64, linux/arm64) are published to GitHub Container Registry by
`.github/workflows/docker.yml`:

| Tag | Updated by |
|---|---|
| `ghcr.io/benwyrosdick/oxidle:latest`, `:main` | every push to `main` |
| `:1.2.3`, `:1.2`, `:1` | pushing a `v1.2.3` tag (`:1` is skipped for `v0.x`) |
| `:sha-<short>` | every published build, for pinning |

To release, run `just bump` (patch), `just bump --minor` or `just bump --major` on a clean
`main`. It updates the version in `Cargo.toml` (zeroing the lower parts on a minor or major
bump, so 0.1.13 → 0.2.0), builds, commits, then pushes the commit and a `vX.Y.Z` tag, which
publishes the versioned image.

```sh
docker run -p 8288:8288 -v oxidle-data:/data ghcr.io/benwyrosdick/oxidle:latest \
  start --event-key my-event-key --signing-key 8b1d8a3c4f0e...
```

Every flag can also be set by environment variable (`INNGEST_EVENT_KEY`, `INNGEST_SIGNING_KEY`,
`INNGEST_POSTGRES_URI`, `INNGEST_REDIS_URI`, …; see [Configuration](#configuration)), which is
the easiest way to run it from another app's Compose file:

```yaml
services:
  inngest:
    image: ghcr.io/benwyrosdick/oxidle:latest
    ports: ["8288:8288"]
    environment:
      INNGEST_EVENT_KEY: my-event-key
      INNGEST_SIGNING_KEY: 8b1d8a3c4f0e2d9b7a6c5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b
      INNGEST_POSTGRES_URI: postgres://postgres:postgres@postgres:5432/postgres
      INNGEST_REDIS_URI: redis://redis:6379
      INNGEST_SDK_URL: http://app:3000/api/inngest
    depends_on:
      postgres: { condition: service_healthy }
      redis: { condition: service_started }
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_PASSWORD: postgres
    volumes: [pgdata:/var/lib/postgresql/data]
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "postgres"]
      interval: 2s
      retries: 30
  redis:
    image: redis:7-alpine
volumes:
  pgdata:
```

Your app then uses `INNGEST_BASE_URL=http://inngest:8288` inside the Compose network. Without
the Postgres/Redis variables oxidle uses SQLite in `/data`; mount a volume there to keep it.
The image runs as a non-root user (uid 10001), includes the dashboard (see
[Dashboard](#dashboard)), and declares a Docker `HEALTHCHECK` that runs `oxidle healthcheck`, so
Compose picks it up without extra configuration. Build it locally with `docker build -t oxidle .`
(`--build-arg UI=false` leaves the dashboard out).

GHCR makes a new package private by default. After the first publish, open the package's
settings on GitHub and set its visibility to public, or log hosts in with
`docker login ghcr.io` using a token that has `read:packages`.

Point your app at it exactly as you would a self-hosted Inngest:

```sh
INNGEST_DEV=0
INNGEST_BASE_URL=http://localhost:8288
INNGEST_EVENT_KEY=my-event-key
INNGEST_SIGNING_KEY=signkey-prod-8b1d8a3c...   # prefixed or bare hex both work
```

Send events with the SDK (`inngest.send(...)`) or plain HTTP:

```sh
curl -X POST http://localhost:8288/e/my-event-key -d '{"name":"app/user.created","data":{"id":1}}'
# {"ids":["01J..."],"status":200}
```

## Performance

Measured against the official self-hosted server (`inngest/inngest:v1.49.0`) with the same SDK
app and workload, all in Docker on one machine (Apple M4):

| Backend | oxidle runs/s (no steps / 3 steps) | Inngest runs/s | Latency p50 (oxidle / Inngest) |
|---|--:|--:|--:|
| SQLite | 790 / 582 | 10 / 8 | 18 ms / 325 ms |
| Postgres | 1,110 / 696 | 294 / 202 | 37 ms / 239 ms |
| Redis | 1,921 / 1,010 | 10 / 9 | 15 ms / 304 ms |
| Redis + Postgres | 1,898 / 758 | 346 / 223 | 22 ms / 241 ms |

oxidle also uses 7–33× less CPU per run and under 50 MB of memory (Inngest: 165–380 MB).
Inngest is slow whenever it writes history to SQLite (including its Redis-only setup, which still
uses SQLite for history) because it syncs SQLite to disk on every commit; with Postgres it reaches
200–350 runs/s. Methodology, charts and the full analysis: [docs/benchmarks.md](docs/benchmarks.md).

## Dashboard

oxidle serves Inngest's own dashboard UI (runs, run timelines with steps/attempts/outputs,
events, apps, functions, rerun / rerun-from-step, cancel, invoke) backed by a compatible
GraphQL API at `/v0/gql`.

The UI is Inngest's code (SSPL-licensed), so it is not vendored in this repository. The Docker
image builds it from Inngest's repository at the commit in `ui/INNGEST_REF` and serves it
automatically, with Inngest's license file next to it (`/usr/local/bin/ui/INNGEST-LICENSE.md`).
To run the binary outside Docker with the dashboard, build it once (uses bun inside Docker):

```sh
./ui/build.sh                 # writes ui/dist
./target/release/oxidle start ...   # serves ui/dist automatically (or pass --ui-dir)
```

Then open `http://localhost:8288`. `--no-ui` disables both the UI and `/v0/gql`.
As in the reference server, `/v0/gql` is unauthenticated — keep the port on a trusted network.

Not backed by data yet: Insights (SQL), Sessions, Connect worker lists. The UI's "Send event"
button posts with a placeholder event key, so it only works in `oxidle dev` mode.

## Metrics

`GET /metrics` serves Prometheus metrics on the main port. Like the reference server it is
unauthenticated, so keep the port on a trusted network or restrict the path at your proxy.

**Inngest-compatible** (same names and labels as `inngest start`, so existing scrape configs and
dashboards keep working):

| Metric | Type | Labels |
|---|---|---|
| `inngest_queue_depth` | gauge | — all queued items, including future and in-progress ones |
| `inngest_function_run_scheduled_total` / `_started_total` | counter | `fn` |
| `inngest_function_run_ended_total` | counter | `fn`, `status` (`completed`, `failed`, `cancelled`) |
| `inngest_sdk_req_scheduled_total` / `_started_total` | counter | `fn` |
| `inngest_sdk_req_ended_total` | counter | `fn`, `status` (`success`, `errored`, `failed`) |

As in Inngest, the `_total` counters are only recorded when `INNGEST_EXPERIMENTAL_PROM_METRICS=1`.
`fn` is the function slug (`<app>-<function>`). An SDK request is `errored` when it failed in a
retryable way (including network errors) and `failed` when not; a step that throws is reported by
the SDK as a normal response, so it counts as `success`, as in Inngest.

**oxidle-specific** (always on):

| Metric | Type | Labels |
|---|---|---|
| `oxidle_queue_ready_items` | gauge | — items due now and waiting for a worker |
| `oxidle_queue_lag_seconds` | gauge | — age of the oldest due item (0 when none is waiting) |
| `oxidle_queue_in_progress` | gauge | `kind` (`start`, `edge`, `sleep`, `pause`, `cron`, …) |
| `oxidle_queue_items_processed_total` | counter | `kind`, `outcome` (`done`, `retry`, `delay`, `error`) |
| `oxidle_sdk_request_duration_seconds` | histogram | `fn`, `status` |
| `oxidle_function_run_duration_seconds` | histogram | `fn`, `status` — scheduled to ended, including sleeps and waits |
| `oxidle_events_received_total` | counter | `source` (`api`, `internal`, `cron`) |
| `oxidle_http_request_duration_seconds` | histogram | `method`, `route` (pattern, e.g. `/e/{key}`), `status` |
| `oxidle_functions` | gauge | — functions currently registered |
| `oxidle_store_up` | gauge | `store` (`database`, `state`) — 1 if it answered a ping this scrape |
| `oxidle_retention_deleted_rows_total` | counter | `table` (`function_runs`, `run_spans`, `events`) |
| `oxidle_build_info` | gauge | `version` |
| `process_*` | various | standard process CPU, memory and file descriptors (Linux) |

The queue gauges read the shared store, so every node reports the same value; use `max`, not
`sum`, across nodes. Counters, histograms and `oxidle_queue_in_progress` are per node.

## Storage backends

| Flags | Durable data (apps, functions, events, history) | Execution state (queue, runs, pauses, flow control) |
|---|---|---|
| *(default)* | SQLite at `--sqlite-dir` (`./.inngest/main.db`) | SQLite |
| `--postgres-uri` | Postgres | Postgres (`SKIP LOCKED` queue, advisory locks) — multi-node safe |
| `--redis-uri` (+ either of the above) | SQLite / Postgres | Redis (atomic Lua scripts) — fastest |

Unlike the reference server's default (in-memory Redis that is lost on restart), every
oxidle configuration is durable. Multiple oxidle nodes can share Postgres or Redis.

## Configuration

All `inngest start` flags are accepted with the same `INNGEST_*` environment variables:
`--host`, `--port`, `--sdk-url`, `--signing-key`, `--event-key`, `--sqlite-dir`, `--redis-uri`,
`--postgres-uri`, `--postgres-max-idle-conns`, `--postgres-max-open-conns`,
`--postgres-conn-max-idle-time`, `--postgres-conn-max-lifetime`, `--poll-interval`,
`--retry-interval`, `--queue-workers`, `--tick`, `--no-ui`, `--json`, `--log-level`.

oxidle-specific: `--retention-days` / `OXIDLE_RETENTION_DAYS` (default 7) prunes finished run
state, events and history; `--shutdown-timeout` / `OXIDLE_SHUTDOWN_TIMEOUT` (seconds, default
25) bounds the drain on SIGTERM; `--ui-dir` / `OXIDLE_UI_DIR` points at the built dashboard;
`OXIDLE_REDIS_PREFIX` (default `oxidle`) namespaces Redis keys.

`--json` or `LOG_HANDLER=json` writes one JSON object per line with Google Cloud Logging's
fields, so GKE picks up each line's severity and summary:
`{"time":"…Z","severity":"WARNING","level":"WARN","message":"…","target":"oxidle::…",…}`.
Event fields (`run_id`, `function`, `error`, …) are top-level keys, as in Inngest's JSON logs, and
`level` is kept for filters written against it.

## Running in production

**Health.** `oxidle healthcheck` probes a running server from inside its container and exits
non-zero on failure, so healthchecks don't need curl. It takes the same flags and defaults as
`inngest alpha doctor healthcheck` (`--host 127.0.0.1`, `--port`/`INNGEST_PORT` 8288,
`--timeout 5s`, `--verbose`), which also works as a command name, plus `--ready` to probe `/ready`
instead of `/health`. `GET /health` is a liveness check (always 200 while the process runs, as in
Inngest). `GET /ready` is a readiness check: 503 while shutting down, or when the database or
the state store (Redis, if configured) doesn't answer a ping within 2 seconds, with the failing
check named in the response. `oxidle_store_up{store="database"|"state"}` reports the same on
`/metrics`, which keeps answering when a store is down.

**Shutdown.** On SIGTERM (or Ctrl-C) oxidle stops leasing queue items and reports not ready,
then gives in-flight steps most of the `--shutdown-timeout` budget to finish (HTTP stays up
meanwhile, since SDKs call back into the API to finish a step). Steps still running at that
point are handed back to the queue, so another node starts them at once instead of after the
30 s lease expires; such a step runs again, as Inngest's at-least-once steps allow. Then HTTP
stops accepting connections, every event already accepted is routed to its functions, and run
timelines are flushed. A crash (SIGKILL, OOM) skips all of this: in-flight steps rerun when
their lease expires, and events accepted but not yet routed are not recovered.

**Retention.** Every minute one node (chosen by a lease in the shared state store) deletes
history older than `--retention-days`: finished runs with their timelines, then events that no
remaining run refers to (so a run sleeping past the retention period keeps its trigger event),
plus finished run state and expired queue, idempotency and pause markers. Deletes run in
transactions of 5,000 rows with a time budget per pass, so a backlog is worked off over several
passes. `oxidle_retention_deleted_rows_total{table}` counts what was removed. No external
cleanup job is needed.

**Migrations** run at startup. On Postgres an advisory lock serializes nodes that start
together, and each migration runs in a transaction, so a failed migration leaves nothing
half-applied and is retried on the next start. A normal Deployment with surge rollouts works;
no ordered startup is needed.

**Kubernetes.** Point the readiness probe at `/ready` and the liveness probe at `/health`. Set
`terminationGracePeriodSeconds` a few seconds above `--shutdown-timeout` (the defaults, 30 and
25, fit). A `preStop` sleep of about 5 s lets load balancers drop the pod before SIGTERM arrives.

## Compatibility

Implemented to the wire spec of the Go server and JS SDK (see `docs/spec/`):

- **Event API**: `POST /e/{key}` (single/batch, per-event ids & partial-failure statuses,
  reserved-name and validation rules, seeded event ids), `POST /invoke/{slug}`, `GET /health`, CORS.
- **Sync**: `POST /fn/register` (checksums, versioning, archiving), `DELETE /fn/remove`,
  server-initiated `PUT` sync of `--sdk-url`s with periodic retry of failed apps.
- **Auth**: event keys; signing-key bearer auth (raw or hashed, prefix-insensitive);
  `X-Inngest-Signature` request signing over RFC 8785 canonical JSON.
- **Execution protocol**: request versions 1/2, all opcodes (`StepRun`, `StepError`,
  `StepFailed`, `StepPlanned`, `Sleep`, `WaitForEvent`, `InvokeFunction`, `WaitForSignal`,
  `Gateway` (step.fetch), `AIGateway` (step.ai.infer), `RunComplete`, `DiscoveryRequest`),
  parallel steps with pending-set coalescing, async checkpointing
  (`/v1/checkpoint/{run}/async`), streaming responses, the large-state `use_api` path
  (`/v0/runs/{id}/actions|batch`), retries with Inngest's backoff table and `Retry-After`.
- **Triggers & flow control**: event (incl. wildcards) and cron (with `TZ=`) triggers,
  trigger expressions, concurrency (fn/env/account scopes, keys), throttle, rate limit,
  idempotency, debounce, batching, priority, cancelOn, start/finish timeouts, singleton,
  event-id dedup, onFailure handlers and `inngest/function.{finished,failed,cancelled,invoked}`.
- **Expressions**: a dedicated CEL engine (`crates/expr`) reproducing Inngest's dialect
  (unknown/null semantics, JS truthiness, custom functions), checked against ~6,400 cases
  produced by the reference Go implementation.
- **REST API**: `/v1/events`, `/v1/events/{id}`, `/v1/events/{id}/runs`, `/v1/runs/{id}`
  (GET/DELETE), `/v1/runs/{id}/jobs`, `/v1/apps/{app}/functions`, `/v1/signals`,
  `/v1/cancellations` (bulk cancel).
- **Metrics**: `GET /metrics` with Inngest's metric names and labels (see [Metrics](#metrics)).

Not yet implemented: Connect (WebSocket workers, port 8289), realtime (`/v1/realtime/*`),
durable endpoints (`POST /v1/checkpoint`), and the `/v2` API.

## Testing

```sh
cargo test --workspace                          # unit, expression oracle, store conformance (SQLite)
OXIDLE_TEST_REDIS=redis://127.0.0.1:6379 cargo test --test state_store   # + Redis
OXIDLE_TEST_POSTGRES=postgres://postgres:pw@127.0.0.1:5432/postgres cargo test   # + Postgres (migrations, retention)

./e2e/run.sh                     # end-to-end with the real JS SDK (Docker), SQLite
BACKEND=postgres ./e2e/run.sh    # ... Postgres
BACKEND=redis ./e2e/run.sh       # ... Redis
TARGET=inngest ./e2e/run.sh      # same suite against the official inngest/inngest image
BENCH=1 N=2000 ./e2e/run.sh      # quick throughput check (combine with BACKEND/TARGET)
./bench/matrix.sh                # full oxidle-vs-Inngest benchmark matrix (Docker), see docs/benchmarks.md
KEEP=1 ./e2e/run.sh              # leave server + app running to explore the dashboard
./e2e/shutdown.sh                # SIGTERM mid-work: nothing lost, in-flight steps finished or handed back
```

The e2e suite (34 scenarios covering every feature above) passes against the official
Inngest server, which validates the expectations, and against oxidle on all three backends.

## Architecture

```
src/
  api/        HTTP routes (axum): events, registration, v0, v1, gql (dashboard)
  engine/     event router, scheduler (flow control), queue workers, executor,
              SDK driver, pauses, finalize, cron, app sync, trace (run timelines)
  store/      MetaStore + StateStore traits; sql.rs (SQLite/Postgres), redis.rs
  function.rs SDK function config parsing/validation
  metrics.rs  Prometheus metrics (GET /metrics)
crates/expr/  Inngest-dialect CEL engine
e2e/          JS SDK test app, test runner, benchmark
ui/build.sh   builds Inngest's dashboard UI into ui/dist
bench/        benchmark matrix (matrix.sh) and chart/table generator (report.py)
docs/spec/    wire/behavior spec extracted from the reference implementation
```
