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

# Or as a container (same arguments)
docker build -t oxidle .
docker run -p 8288:8288 -v oxidle-data:/data oxidle start --event-key ... --signing-key ...
```

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

PERF_PLACEHOLDER

## Dashboard

oxidle serves Inngest's own dashboard UI (runs, run timelines with steps/attempts/outputs,
events, apps, functions, rerun / rerun-from-step, cancel, invoke) backed by a compatible
GraphQL API at `/v0/gql`.

The UI is Inngest's code (SSPL-licensed), so it is not vendored here. Build it once from
Inngest's repository at a pinned commit (uses bun inside Docker):

```sh
./ui/build.sh                 # writes ui/dist
./target/release/oxidle start ...   # serves ui/dist automatically (or pass --ui-dir)
```

Then open `http://localhost:8288`. `--no-ui` disables both the UI and `/v0/gql`.
As in the reference server, `/v0/gql` is unauthenticated — keep the port on a trusted network.

Not backed by data yet: Insights (SQL), Sessions, Connect worker lists. The UI's "Send event"
button posts with a placeholder event key, so it only works in `oxidle dev` mode.

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
state, events and history; `--ui-dir` / `OXIDLE_UI_DIR` points at the built dashboard;
`OXIDLE_REDIS_PREFIX` (default `oxidle`) namespaces Redis keys.

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

Not yet implemented: Connect (WebSocket workers, port 8289), realtime (`/v1/realtime/*`),
durable endpoints (`POST /v1/checkpoint`), and the `/v2` API.

## Testing

```sh
cargo test --workspace                          # unit, expression oracle, store conformance (SQLite)
OXIDLE_TEST_REDIS=redis://127.0.0.1:6379 cargo test --test state_store   # + Redis

./e2e/run.sh                     # end-to-end with the real JS SDK (Docker), SQLite
BACKEND=postgres ./e2e/run.sh    # ... Postgres
BACKEND=redis ./e2e/run.sh       # ... Redis
TARGET=inngest ./e2e/run.sh      # same suite against the official inngest/inngest image
BENCH=1 N=2000 ./e2e/run.sh      # quick throughput check (combine with BACKEND/TARGET)
./bench/matrix.sh                # full oxidle-vs-Inngest benchmark matrix (Docker), see docs/benchmarks.md
KEEP=1 ./e2e/run.sh              # leave server + app running to explore the dashboard
```

The e2e suite (32 scenarios covering every feature above) passes against the official
Inngest server, which validates the expectations, and against oxidle on all three backends.

## Architecture

```
src/
  api/        HTTP routes (axum): events, registration, v0, v1, gql (dashboard)
  engine/     event router, scheduler (flow control), queue workers, executor,
              SDK driver, pauses, finalize, cron, app sync, trace (run timelines)
  store/      MetaStore + StateStore traits; sql.rs (SQLite/Postgres), redis.rs
  function.rs SDK function config parsing/validation
crates/expr/  Inngest-dialect CEL engine
e2e/          JS SDK test app, test runner, benchmark
ui/build.sh   builds Inngest's dashboard UI into ui/dist
bench/        benchmark matrix (matrix.sh) and chart/table generator (report.py)
docs/spec/    wire/behavior spec extracted from the reference implementation
```
