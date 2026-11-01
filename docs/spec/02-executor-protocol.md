# Spec: executing functions on an SDK over HTTP

`G` = inngest/pkg, `J` = inngest-js/packages/inngest/src. Checked against Go HEAD (2026-10-31) and inngest-js 4.26.2.

Key facts:
1. JS SDK v4 async checkpointing is on by default when `ctx.fn_id` is present: SDK runs several steps in one request and POSTs each to `POST /v1/checkpoint/{runId}/async` (§9). Implement that endpoint (or omit `ctx.fn_id`).
2. In checkpointing mode completion arrives as 206 with a `RunComplete` op, not 200.
3. Request body must be JCS-canonical (RFC 8785) JSON; sign those exact bytes.

## 0. Registration shapes the URL
Each function has one step `"step"`, runtime url `https://host/api/inngest?fnId=<appId>-<fnId>&stepId=step`. `retries.attempts` is the retry count (default 4 => 5 attempts, max 20).

## 1. Request server -> SDK
- `POST` to step URL; set query `stepId` = hashed step id for planned step, else `"step"` (discovery). Query re-encoded with sorted keys. `fnId` unchanged.
- Headers: `Content-Type: application/json`; `X-Inngest-Signature: t=<unix>&s=<hex HMAC-SHA256(key, body||t)>` if key; `X-Run-ID: <run ulid>`; `X-Inngest-Step-ID: <stepId>`; `x-inngest-req-version: <-1|1|2>`; optional `x-request-id`, `x-inngest-generation-id`, `x-inngest-job-id`, `traceparent`.
- HMAC key = signing key with `^signkey-\w+-` stripped, as UTF-8 bytes. SDK verifies `canonicalize(JSON.parse(body)) + t`, rejects |now−t| > 5min. In dev mode SDK skips verification.
- Body (JCS):
```json
{"event": {...events[0]...},
 "events": [{"id","name","data","ts","v","user":{}}],
 "steps": {"<hashedStepId>": <stored value>},
 "ctx": {"fn_id":"<fn uuid>","run_id":"<ulid>","env":"","step_id":"step","attempt":0,"max_attempts":5,
         "stack":{"stack":["h1","h2"],"current":2},"qi_id":"<opaque>","request_id":"..","generation_id":1,"job_id":"..",
         "disable_immediate_execution":false,"use_api":false},
 "version": -1, "use_api": false}
```
  - `steps` contains every stored step in stack order.
  - Stored value formats: `{"data":X}`, `{"error":{name,message,stack?,cause?}}`, `{"input":[...]}`, or any other raw value (treated as data).
  - `ctx.attempt`: 0-based attempt of current queue item; `max_attempts` = retries+1.
  - `ctx.stack.stack`: step ids in save order; `current` = index after edge.Outgoing in stack, or 0... (effectively stack length).
  - `ctx.qi_id`: opaque, echoed back by SDK for checkpointing.
  - `disable_immediate_execution`: ForceStepPlan (§5).
  - `version`: run's stored request version, starting at -1. SDK rejects 0.
  - Large state (>4MB): `events:[]`, `steps:{}`, `use_api:true` (both ctx and top-level); SDK then fetches `/v0/runs/{id}/actions` and `/batch`.

## 2. Response SDK -> server
SDK headers: `x-inngest-sdk: inngest-js:v4.26.2`, `x-inngest-req-version: 2` (1 if optimizeParallelism:false), `x-inngest-no-retry`, `retry-after`, optional `x-inngest-signature`.
- From SDK iff `X-Inngest-SDK` non-empty (2xx without it => retryable error).
- no-retry: header present => retryable unless `"true"`.
- Retry-After: ≤7 chars => int seconds; else date. Clamp now+1s..now+24h.
- Body limit 8MB; request timeout 2h.

| Status | Meaning |
|---|---|
| 200 | function finished; body = raw JSON return value |
| 206 | JSON array of ops (single object accepted; `[]` = None) |
| 201 | streaming: whitespace keepalives then `{"status","headers","body","version":2}` – unwrap and treat `status` as real |
| 500 | retriable error `{"name","message","stack","__serialized":true}` |
| 400 | non-retriable error (`x-inngest-no-retry: true`) |
| 401 | bad signature |

Non-2xx: Err = `"invalid status code: N"` or `"NonRetriableError"` when no-retry.

Op shape:
```json
{"op":"StepRun","id":"<sha1 hex>","name":"...","displayName":"My step","opts":{}|null,"data":any|null,
 "error":{"name","message","stack","data"?,"cause"?}|null,"timing":{...},"userland":{"id":"my-step","index":1}}
```

## 3. Opcodes
Process `WaitForEvent`, `DeferAdd`, `DeferAbort` first, then others. >1000 ops fails the run.

| Op | Server action | Stored under steps[id] |
|---|---|---|
| None | nothing; if response is only None ops, re-drive discovery | — |
| Step | legacy; store data verbatim; enqueue discovery | raw data |
| StepRun | save `{"data": op.data}`; enqueue discovery | `{"data":X}` |
| StepError | if retryable (no NoRetry && attempt+1 < max): retry same item attempt+1 with backoff/Retry-After; else treat as StepFailed | — |
| StepFailed | save `{"error": op.error}`; enqueue discovery | `{"error":{...}}` |
| StepPlanned | enqueue edge item with stepId=id, attempt 0, dedupe job id `<fn>:<run>-<id>-plan` | — |
| Sleep | duration from `opts.duration` else `name` (JS uses name). ≥10 chars & parses as date => absolute. Else `1w2d3h4m5s`. Max 366d. Enqueue sleep item at wake; on wake save `null`, then discovery | `null` |
| WaitForEvent | opts `{"timeout","if"?,"event"?}`; if `event` empty use `name` as event name. Interpolate `event.*` refs from trigger event. Create pause + timeout item | matched: full event object (unwrapped); timeout: `null` |
| InvokeFunction | opts `{"function_id":slug|uuid,"payload":{"data":{},"v"?},"timeout"?}` (default 1y). Publish `inngest/function.invoked` with `data = payload.data + {"_inngest":{"fn_id","correlation_id":"<runID>.<id>","expire":ms,"gid","name","source_app_id","source_fn_id","source_fn_v"}}`. Pause on `inngest/function.finished` where `async.data.correlation_id == "<runID>.<id>"` | success `{"data":result}`; error `{"error":err}`; timeout `{"error":{"error":"InngestInvokeTimeoutError: Timed out waiting for invoked function to complete","name":"InngestInvokeTimeoutError","message":"Timed out waiting for invoked function to complete"}}` |
| WaitForSignal | opts `{"signal","timeout","conflict":"replace"|...}`; resumed by `POST /v1/signals` | matched `{"data":{"signal":id,"data":payload}}`; timeout `null` |
| Gateway (step.fetch) | opts `{url,method,headers,body}`; server makes HTTP call; any status ok | `{"data":{"url","headers":{k:"v"},"body":"str","status_code":N}}`; transport error final: `{"error":{"name":"GatewayError","message"}}` |
| AIGateway | opts `{type,url,headers,auth_key,format,body}`; POST body to url; auth: anthropic `x-api-key`, bedrock `Authorization: <key>`, gemini `?key=`, default `Authorization: Bearer` ; status>299 error | `{"data": <provider JSON>}` or `{"error":{"name":"AIGatewayError",...}}` |
| RunComplete | finalize Completed with result = op.data (raw). id = sha1("complete") | — |
| SyncRunComplete | sync API runs only | — |
| DiscoveryRequest | enqueue discovery | — |
| DeferAdd / DeferAbort | optional | — |

JS: `step.run`, `step.sendEvent`, `step.sendSignal` are `StepPlanned` then `StepRun`. `opts.type` e.g. `step.sendEvent`. `opts.parallelMode: "race"` inside group.parallel.

## 4. Step ID hashing
`sha1_hex(userId)` first use; repeats `sha1_hex(userId + ":" + n)`. Server treats ids as opaque.
Derived: pause id = uuid(sha1(runID + id)); invoke correlation id = `runID + "." + id`.

## 5. Parallelism and discovery
- Discovery (`stepId=step`): SDK replays with memoized steps. If exactly one unfulfilled op, StepPlanned, `disable_immediate_execution` false, no parallelMode: SDK runs it inline and returns `206 [StepRun|StepError|StepFailed]`. Else returns all new ops.
- Planned step (`stepId=<hash>`): SDK runs that step -> `206 [StepRun|StepError|StepFailed]`; 500 step-not-found if not found in 10s.
- >1 non-lazy ops in one 206 => set ForceStepPlan=true in run metadata (requests carry `disable_immediate_execution: true`).
- Request version ≥2: record pending set of all non-lazy op ids; when a step saves, remove from pending; enqueue discovery only when pending is empty (or parallelMode == race). When discovery enqueued in v2, clear ForceStepPlan.
- Version 1: every completion enqueues discovery; ForceStepPlan stays on.
- Discovery item after step finish: edge {Outgoing: id, Incoming: "step"}, attempt 0.

Retries:
- Step level (206 StepError): same queue item, attempt+1, backoff. Planned step retried with same stepId.
- SDK sends StepFailed itself when `max_attempts-1 == attempt` or NonRetriableError.
- Server converts StepError -> StepFailed when attempt+1 >= max or no-retry.
- Function level (non-2xx): retry same item attempt+1; on last attempt finalize Failed.
- Uncaught StepError in user code => SDK returns 400 no-retry.

## 6. Retry and backoff
- Default 4 retries (5 attempts), max 20; onFailure retries 1.
- ShouldRetry = attempt+1 < max.
- Backoff table indexed by 0-based attempt `[15s,30s,1m,2m,5m,10m,20m,40m,1h,2h]` capped at last, + 0–30s jitter. Retry-After overrides. `--retry-interval N` => linear N secs.
- Attempts are per queue item; successful async checkpoint resets attempts.

## 7. Run lifecycle & internal events
Status: Running=0, Completed=1, Failed=2, Cancelled=3, Overflowed=4, Scheduled=5, Unknown=6, Skipped=7.

Finalize: delete state, release concurrency, dequeue jobs, publish events (only for first event of batch; runs triggered by `inngest/function.failed`/`finished` emit nothing).
```json
{"function_id":"<fn slug>","run_id":"<ULID>","event":{...},"events":[...],
 "error":{...}?,"result":any?,"correlation_id":"<parentRun>.<hash>"?,
 "_inngest":{"status":"Completed"|"Failed"|"Cancelled"}}
```
- `inngest/function.finished`: always, random ULID id.
- `inngest/function.failed`: when Failed, id = run id.
- `inngest/function.cancelled`: when Cancelled, id = run id.
- error = `{"error":"<Err string>","name","message","stack","cause"?}`.
- Cancelled runs: no error, no result.
- Invoke resume: finished event resumes parent pause; `data.error` -> `{"error":..}`, else `{"data": data.result}`.
- onFailure: function `<slug>-failure`, trigger `inngest/function.failed` expr `event.data.function_id == '<slug>'`, retries 1; SDK requires `event.data.error`.
- `inngest/function.invoked` routes by `data._inngest.fn_id` == slug or uuid. If invoked run can't be scheduled (rate limited), emit synthetic function.finished with error `{"name":"Error","message":"invoked function is rate limited"}`.

## 8. use_api
`GET /v0/runs/{id}/actions` -> `{"<hashedId>": stored value}`; `GET /v0/runs/{id}/batch` -> array of events. Bearer hashed key. Errors `{"error","status"}` 400/410.

## 9. Checkpointing (JS v4 default)
Enabled when `!requestedRunStep && ctx.fn_id && !disable_immediate_execution`. Defaults bufferedSteps 1, maxRuntime 10s.
```
POST /v1/checkpoint/{runId}/async  (Bearer hashed signing key)
{"run_id","fn_id","qi_id","request_id","generation_id","request_started_at","steps":[StepRun ops],"ts"}
```
Server: accept only StepRun, Step, StepPlanned (ignore), DeferAdd, DeferAbort; else 400. Save StepRun as `{"data":X}` ignoring duplicates. Reset queue item attempts. 200 empty. 409 stale; 413 too large. SDK retries 5x then falls back to returning steps in 206. Missing/empty qi_id => immediate fallback.
Completion: `206 [...unflushed StepRun..., {"op":"RunComplete","id":"0737c22d3bfae812339732d14d8c7dbd6dc4e09c","data":<return>}]`. Persist StepRuns then finalize; do NOT enqueue discovery.

## 10. Limits
Step input/output 4MB; SDK response 8MB; request 4MB (else use_api); run state 32MB; 1000 ops/response; sleep/wait max 366d.
Fail-early (no retries): expression compile errors, state overflow, step overflow, signal conflict, timeout too long.
