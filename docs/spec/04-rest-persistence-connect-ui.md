# Spec: REST API, SDK-called endpoints, persistence, Connect, UI

Derived from the Inngest Go server (`REF` = inngest repo) and JS SDK (`JS` = inngest-js/packages/inngest/src).

## 0. Process layout and auth
- Main HTTP port 8288. Mounts (`REF/pkg/devserver/devserver.go:875-884`):
  - `/` devAPI: `GET /dev` (404 in self-hosted mode), `POST /fn/register`, `DELETE /fn/remove`, `POST /dev/traces`, UI static + SPA fallback, `/v1/*`.
  - `/v0` coreapi; `/v2`, `/api/v2` (grpc-gateway, optional); `/metrics`, `/debug`.
  - Event API: `GET` health, `POST /e/{key}`, `POST /invoke/{slug}`.
- Every response: header `X-Inngest-Server-Kind: cloud` (self-hosted) or `dev`.
- Signing-key auth (`REF/pkg/authn/signing_key_strategy.go`): token from `Authorization: Bearer <tok>` (case-insensitive "bearer").
  No signing key configured => pass. Normalise both by stripping `^signkey-\w+-`. Accept if client == normalised key OR
  client == hex(sha256(hex_decode(normalised key))). Constant-time compare. Failure: 401 text/plain `Authentication failed\n`.
  SDK sends `prefix + hex(sha256(hexdecode(key)))`. SDK retries once with fallback key on 401/403.
- Fixed identity: account `00000000-0000-4000-a000-000000000000`, env `00000000-0000-4000-b000-000000000000`.
- Error body: `{"code"?: string, "error": string, "data": object|null, "status": int}` with HTTP status = status.

## 1. /v1 REST API (`REF/pkg/api/apiv1/apiv1.go:165-222`)
Envelope: `{"data": T, "metadata": {"fetched_at": RFC3339Nano, "cached_until"?: RFC3339Nano}}`. Auth: signing key.

| Method / path | Behaviour |
|---|---|
| `GET /v1/events` | `limit` (default 20, clamp 1..51), `cursor` (ULID, rows with internal_id < cursor), `received_after`, `received_before` (default now-1h..now), `name`. Excludes `inngest/%`. Order internal_id DESC. |
| `GET /v1/events/{internalID}` | Event; 400 bad ULID, 404 none |
| `GET /v1/events/{internalID}/runs` | FunctionRun[] |
| `GET /v1/runs/{runID}` | FunctionRun incl output; unknown => 500 `Unable to load function run: <id>` |
| `DELETE /v1/runs/{runID}` | cancel; 200 empty body; 404 not found; 400 bad ULID |
| `GET /v1/runs/{runID}/jobs` | JobResponse[] (limit 10) |
| `POST /v1/runs/{runID}/metadata` | `{"target":{...},"metadata":[...]}` -> 200 empty |
| `GET /v1/apps/{appName}/functions` | **bare array** of Function |
| `POST /v1/cancellations` | `{"app_id","function_id"(slug),"started_after"?,"started_before"(<= now+5s),"if"?}` -> Cancellation |
| `GET /v1/cancellations` | Cancellation[] |
| `DELETE /v1/cancellations/{id}` | `{"data":{"ok":true}}` or 404 |
| `POST /v1/signals` | `{"signal","data"}` -> `{"data":{"run_id"}}`, 404 when no waiter |
| `POST /v1/traces/userland` | OTLP; may accept and drop |

Shapes:
- FunctionRun: `{"run_id","run_started_at","function_id"(UUID),"function_version":int,"environment_id","event_id","batch_id"?,"original_run_id"?,"cron"?,"status","ended_at":time|null,"output"?:json}`.
  Status strings: Running, Completed, Failed, Cancelled, Overflowed, Scheduled, Unknown, Skipped. Output default `{}`; non-JSON wrapped as string.
- Event: `{"internal_id","account_id","environment_id","source","source_id":null,"received_at","id"?,"name","data","user"?,"ts"?:ms,"v"?}`.
- Function: `{"internal_id","env_id","app_id","id"(slug),"name","config":json,"created_at","archived_at"}` (archived_at zero time `0001-01-01T00:00:00Z`).
- Cancellation: `{"created_at","name":null,"id","account_id","environment_id","app_id","function_internal_id","function_id","started_after","started_before","if"?,"kind":"bulk_run","cause":"API","target_id"?,"queue_name":null}`.
- JobResponse: `{"JobID","at","position","kind","attempt","Raw"}`.

## 2. Endpoints SDKs call (besides /e and /fn/register)
Bearer hashed signing key + `x-inngest-sdk`, `x-inngest-env`.
- `GET /v0/runs/{runId}/actions`: unwrapped `{stepId: value}` (values `{data}`/`{error}`/`{input}`/raw). Missing: 410 `{"error":"runtime state is no longer available for runID: ...","status":410}`. Bad ULID: 400.
- `GET /v0/runs/{runId}/batch`: unwrapped array of events `[{name,data,id,ts,user,v}]`.
- `DELETE /v0/runs/{runId}`: cancel -> 204.
- Checkpoint API (`/v1/checkpoint`, legacy `/v1/http/runs`): sync/durable-endpoint runs. `POST /v1/checkpoint` body `{run_id, event:{name,data:{domain,method,path,fn,ip,content_type,query_params,body},id?,ts,v?}, steps?, ts, request_version, retries}` -> `{"data":{"fn_id","app_id","run_id","token"?,"realtime_token"}}` (realtime_token required non-empty). `POST /v1/checkpoint/{runId}/steps` `{run_id,fn_id,app_id,steps,ts}` -> 200. `POST /v1/checkpoint/{runId}/async` `{run_id,fn_id,qi_id,request_id,generation_id?,request_started_at,steps,ts}` -> 200, 409 stale. `GET /v1/http/runs/{runId}/output?token=` polls output.
- Realtime: `POST /v1/realtime/token` `[{"channel","name","kind":"run"}]` -> 201 `{"jwt"}`; `GET /v1/realtime/connect` (WS), `GET /v1/realtime/sse?token=`, `POST /v1/realtime/publish?channel=&topic=&run_id=`.
- Connect bootstrap: `POST /v0/connect/start`, `/v0/connect/flush` (protobuf).
- `GET /dev`: 404 in self-hosted.

## 3. Persistence (reference implementation)
SQL tables: apps, functions, events, function_runs, function_finishes, history, event_batches, spans, traces/trace_runs, queue_snapshot_chunks, worker_connections.
Redis: run state (`{estate}:metadata`, `inputs`, `actions`, `stack`, ...), pauses, queue (item hash + per-partition zsets, concurrency/throttle keys), batches/debounce.
Without `--redis-uri` uses in-memory miniredis snapshotted to SQL.

## 4. Connect (WebSocket workers) — deferred
Gateway port 8289 `/v0/connect`, protobuf over WS (subprotocol `v0.connect.inngest.com`). Proto: `REF/proto/connect/v1/connect.proto`; SDK spec `JS/components/connect/CONNECT_SDK_SPEC.md`.

## 5. UI
Embedded SPA (TanStack Start/Vite, `REF/ui/apps/dev-server-ui`), talks to `POST /v0/gql` (schema `REF/pkg/coreapi/gql.schema.graphql`), `/dev`, `/e/dev_key`, `/invoke/{slug}`. Reusable with a compatible GraphQL subset; run views use the spans model.
