# Spec: event ingestion, app sync/registration, auth, CLI

`GO/` = inngest Go server, `JS/` = inngest-js/packages/inngest/src.

## 1. Event ingestion
Routes (main port, default 0.0.0.0:8288):
- `POST /e/{key}` — event key in path (`GO/pkg/api/api.go:79`)
- `POST /invoke/{slug}` — no auth in OSS
- `GET /health` -> `{"status":200,"message":"OK"}\n`
- `POST /v2/events`, `/api/v2/events` — Bearer signing key, single event (optional)

Key check: empty key -> 401 `{"status":401,"message":"","error":"Event key is required"}`; unknown -> 401 `{"status":401,"message":"","error":"Event key not found"}`. Exact string match against configured keys.

Body parsing (`GO/pkg/eventstream/eventstream.go`): first token `{` single event (≤3 MiB); `[` array (each ≤3 MiB, ≤5000 items: "Cannot have more than 5000 events in a batch"). Empty body/`[]` -> `{"ids":[""],"status":200}` (quirk). Other -> "Request body must contain an event object or an array of event objects". Oversize: "Event is over the max size: Max 3145728 bytes / Size N bytes".

Event JSON: `{"id"?:string,"name":string,"data":object,"user"?:object,"ts"?:int ms,"v"?:string,"meta"?:{"sessions":{},"propagated_sessions":{}}}`. Non-object data fails decode.

Per-event normalization: reserved `inngest/` prefix (case-insensitive) -> 400 "event name is reserved for internal use: <name>"; delete `data._inngest`; ts==0 -> now ms; user nil -> {}; validation 406: "event name is empty", NUL char, ts outside 1980..2100.

Response: `{"ids":["<ULID>","",...],"status":200,"error"?:string}` + trailing `\n`. All events processed; ids[i]="" on failure; status (and HTTP status) = first failure's status (400/406/500). Undecodable item stops the batch (400 if no earlier failure). SDK treats `status != 200 || error` as failure and retries.

Event IDs: internal ID = ULID. Optional seed header `x-inngest-event-id-seed: "<millis>,<base64std(10 bytes)>"`: per event, `entropy[6:10] = BE_u32(entropy[6:10]) + index` (wrapping), index 1-based on /e/; ULID(millis, entropy). If `evt.id` empty, set it to the internal ULID string.

Dedup by `id`: not at ingest. Per function at scheduling: key `(fn uuid, event.id)` SET NX with 24h TTL -> skip duplicate run.

`POST /invoke/{slug}`: body Event; creates `inngest/function.invoked` event with `data._inngest = {fn_id: slug, ...}`; response `{"id":"<ULID>","status":200}`.

CORS: reflect Origin, methods GET/POST/PUT/DELETE/OPTIONS, headers *, max-age 3600; preflight 200 empty.

## 2. App sync
### Server-initiated ping (out-of-band)
`PUT <app url>` with `Content-Type: application/json`, `X-Inngest-Server-Kind: cloud`, `X-Inngest-Signature: t=<unix>&s=<hex>` (if signing key), body `{"url":"<app url>"}`. 10 s timeout. Done at startup for each `--sdk-url`, and every poll-interval for apps with saved error. Response error mapping: 400 `{"message"}` -> error string; 401 unauthorized; 403 forbidden; 404 url_not_found; 500 internal_server_error; network -> unreachable.
SDK then POSTs to `/fn/register` and answers the PUT with `{"message":..., "modified":bool}`.

### `POST /fn/register` (signing-key auth)
Headers: `x-inngest-sdk`, `x-inngest-framework`, `x-inngest-env`, `x-inngest-expected-server-kind`, `Authorization: Bearer <hashed key>`.
Body: `{"url","deployType":"ping","framework","appName","functions":[FunctionConfig],"sdk":"js:v4.x","v":"0.1","deployId"?,"capabilities"?:{},"appVersion"?,"featureObservations"?}`.
Processing:
1. If `X-Inngest-Expected-Server-Kind` set and != our kind -> 400 `{"error":"Expected server kind cloud, got dev"}`.
2. Bad JSON -> 400 `{"error":"Invalid request: <err>"}`.
3. Normalize URLs: drop `deployId`/`probe` query params, sort query keys, drop default ports, 127.0.0.1/0.0.0.0 -> localhost.
4. checksum = sha256 of request (minus featureObservations).
5. empty appName -> 400 `{"error":"App ID required","data":null,"status":400}`.
6. app UUID = UUIDv5(NameSpaceOID, appName).
7. Same checksum, no error -> `{"ok":true,"modified":false,"sync_id","app_id"}`.
8. fn UUID = UUIDv5(NameSpaceOID, fn.Name + stepURI), but reuse existing ID for same (app, slug). Archive removed functions. Version increments.
9. Invalid fn -> 400 `{"error":"At least one function is invalid","data":null,"status":400}`.
10. Success -> 200 `{"ok":true,"modified":true,"app_id","sync_id"}`.
SDK reads JSON `{status?=200, skipped?, modified?, error?}`; HTTP status ignored, so error bodies must carry `status`+`error`.

### FunctionConfig (from SDK)
```json
{"id":"my-app-send-email","name":"Send email",
 "triggers":[{"event":"app/user.created","expression":"event.data.x == 1"},{"cron":"0 * * * *","jitter":"30s"}],
 "steps":{"step":{"id":"step","name":"step","runtime":{"type":"http","url":"http://host/api/inngest?fnId=my-app-send-email&stepId=step"},"retries":{"attempts":3}}},
 "idempotency":"event.data.id",
 "batchEvents":{"maxSize":100,"timeout":"5s","key":"event.data.u","if":"expr"},
 "rateLimit":{"limit":10,"period":"1h","key":"event.data.u"},
 "throttle":{"limit":5,"period":"1m","burst":2,"key":"event.data.u"},
 "singleton":{"key":"event.data.u","mode":"skip"},
 "cancel":[{"event":"app/cancel","if":"event.data.id == async.data.id","timeout":"1h"}],
 "debounce":{"period":"5s","key":"event.data.u","timeout":"1m"},
 "timeouts":{"start":"10s","finish":"1h"},
 "priority":{"run":"event.data.prio"},
 "concurrency":5}
```
- concurrency: number | `{limit,key?,scope?:"fn"|"env"|"account"}` | array of up to 2 | `{"fn":[...],"step":[...]}`.
- retries default 4 (5 attempts), max 20. Top-level `retries` int also accepted.
- `idempotency` expr == rateLimit{limit:1, period:24h, key:expr}.
- onFailure fn: id `<fnId>-failure`, trigger `inngest/function.failed` with expression `event.data.function_id == '<fnId>'`.
- Validation: name required; exactly one step; URI http/https/ws/wss; ≤10 triggers; batch maxSize≥2, timeout≥1s; debounce period 1s..7d; rateLimit limit>0 period≤24h; cancel ≤5.

## 3. Auth
- Event keys: plain exact match; `--event-key` repeatable; `INNGEST_EVENT_KEY` comma-separated; required by `start`.
- Signing key: hex (optionally `signkey-<env>-` prefixed). Required by `start`.
- Bearer check: token after `Bearer ` (case-insensitive). Strip `^signkey-\w+-` from both. Accept token == key or token == hex(sha256(hex_decode(key))). Failure 401 text/plain `Authentication failed\n`.
- Routes behind auth: /fn/register, /fn/remove, /v1/* (except realtime/output), /v0/runs/*, /v0/connect/*, /v2/*. NOT: /e/*, /invoke/*, /health, /v0/gql.
- Request signing `X-Inngest-Signature: t=<unix secs>&s=<hex>`: `s = hex(HMAC_SHA256(key = ASCII bytes of prefix-stripped hex key string, msg = body || decimal(t)))`. Body is JCS-canonicalized JSON (server sends canonical bytes). SDK verifies within 5 min skew. Executor also sends `X-Run-ID`.

## 4. CLI (`inngest start`)
| Flag | Env | Default |
|---|---|---|
| --config | INNGEST_CONFIG | inngest.{json,yaml,yml} search |
| --host | INNGEST_HOST | 0.0.0.0 |
| --port, -p | INNGEST_PORT | 8288 |
| --sdk-url, -u (repeat) | INNGEST_SDK_URL (comma) | |
| --signing-key | INNGEST_SIGNING_KEY | required |
| --event-key (repeat) | INNGEST_EVENT_KEY (comma) | required |
| --sqlite-dir | INNGEST_SQLITE_DIR | ./.inngest (main.db) |
| --redis-uri | INNGEST_REDIS_URI | in-memory |
| --postgres-uri | INNGEST_POSTGRES_URI | sqlite |
| --postgres-max-idle-conns | | 10 |
| --postgres-max-open-conns | | 100 |
| --postgres-conn-max-idle-time | | 5 (min) |
| --postgres-conn-max-lifetime | | 30 (min) |
| --poll-interval | INNGEST_POLL_INTERVAL | 5s |
| --retry-interval | INNGEST_RETRY_INTERVAL | 0 |
| --queue-workers | INNGEST_QUEUE_WORKERS | 100 |
| --tick | INNGEST_TICK | 150 ms |
| --connect-gateway-port | | 8289 |
| --no-ui | INNGEST_NO_UI | false |
Global: --json, --verbose, --log-level/-l. Precedence: flag > env > config file > default.

Routes summary: `/health`, `/e/{key}`, `/invoke/{slug}`, `/dev` (404 in start), `/fn/register`, `/fn/remove`, `/v0/gql`, `/v0/runs/{id}` (DELETE), `/v0/runs/{id}/batch`, `/v0/runs/{id}/actions`, `/v0/telemetry`, `/v0/connect/*`, `/v1/*` (see 04), `/v2/*`, `/metrics`. Every response has `X-Inngest-Server-Kind: cloud`.
