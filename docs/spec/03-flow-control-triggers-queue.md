# Spec: triggers, expressions, flow control, queue, pauses, cancellation, durations

`REF/` = inngest Go server. Account/env IDs are constants.

## 0. Event model
- Expression view of event: `{name, data, user, id, ts: double, v?, meta?}`; data/user never null.
- Internal events: `inngest/function.finished`, `.failed`, `.cancelled`, `.invoked`, `inngest/scheduled.timer` (cron), `inngest/deferred.schedule`.
- Per event (concurrently): match functions (triggers + invoke lookup); if `inngest/function.finished` with `data.correlation_id` -> resume invoke; match pauses by exact event name.

## 1. Triggers
- `{event, expression?}` or `{cron, jitter?}`. ≤10 per fn.
- Wildcards: for incoming name generate candidates: name; each `/`-prefix + `/*`; each `.`-prefix + `.*`. `app/user.created` -> `["app/user.created","app/*","app/user.*"]`. Trigger matches iff `event` equals a candidate.
- One event can start N functions; per function, first matching trigger (pattern + expression true) starts it once. Expression vars `{event}`; non-bool/error => skip.
- Init: if batching enabled (maxSize>1 && timeout): `if` false/error => run unbatched; else append to batch. Otherwise Schedule with IdempotencyKey = event.id.
- Cron: 5-field (min hour dom month dow), `*`,`?`,ranges,steps,lists, names case-insensitive, dow 0-6. DOM/DOW OR'd when both restricted. `TZ=Area/City ` or `CRON_TZ=` prefix; default UTC.
  Jitter: fire = scheduled + (1s + xxhash64(jobID) % (jitter-1s)).
  Chain per (fn, version, expr): enqueue next fire; on fire: verify fn still has cron, schedule run with idempotency key = scheduledAt RFC3339 UTC, enqueue next.
  Cron event: `{id: "2026-10-31T09:00:00Z", name:"inngest/scheduled.timer", data:{cron, scheduledAt, fireAt}, ts: fireAt ms}`.

## 2. Expressions (CEL)
- Vars: `event`, `async` (maps). No `events`.
- Macros: has, all, exists, exists_one, map, filter. Stdlib: operators, in, size, contains, startsWith, endsWith, matches, conversions, timestamp/duration and accessors.
- Custom: `lowercase(s)`, `uppercase(s)`, `b64decode(s)`, `json_parse(s)`, `date(x)`, `now()`, `now_plus(dur)`, `now_minus(dur)`.
- Semantics differing from standard CEL (must replicate):
  1. Missing attributes are unknown ~ null: `==` true only if other side null; `!=` false if other null else true; `<`,`<=` true if LEFT unknown; `>`,`>=` false if left unknown; `size(unknown)`=0; `unknown + x` = x; other functions false; whole-result unknown => false.
  2. null vs concrete: `null == x` false; for ordering, null replaced by zero value of other operand's type.
  3. Mixed types: `+` error => string concat; numeric comparisons across int/double.
  4. `&&`, `||`, ternary use JS truthiness for non-bools (falsy: null, unknown, error, false, "", 0). Empty lists/maps truthy.
  5. `has(a.b.c)` with missing ancestor => false.
  6. Boolean contexts require bool; else no match.
  7. Key expressions stringified (`<nil>`, 1, 1.5, true).
- `cancelOn.if`: restricted (no macros).
- Contexts: trigger/batch/flow-control keys/priority: `event` (first event). waitForEvent/cancelOn: `event` = run trigger event, `async` = incoming.
- Pause expressions: interpolate `event.*` at creation; equivalently store trigger event and bind both.
- `match: "data.id"` => `event.data.id == async.data.id` (SDK side).

## 3. Flow control
Order in schedule: rateLimit -> debounce -> priority -> concurrency/throttle keys -> singleton -> create state (idempotency) -> cancel pauses & start timeout -> enqueue start at max(now, event.ts).

Run idempotency key = hash(fnID) + "-" + hash(k), k = event.id (cron: RFC3339 time; batch: `<fnID>-<batchID>`). 24h window. Duplicate => no new run.

### concurrency
- Number N => `[{scope: fn, limit: N}]`; object; array (≤2); scope fn|env|account (env/account require key).
- Fn limit without key: default 1000.
- Custom key: `"<f|e|a>:<scopeID>:<hash(evaluated)>"`; env/account keys share across functions.
- Counts concurrently EXECUTING queue items (start/edge/sleep/edge-error/pause-timeout). Sleeping/waiting/queued runs do not count. Released after each item.

### throttle `{limit, period, burst?, key?}`
- Only start items; delays (never drops). Key fnID[-hash(eval)].
- GCRA: emission = period/limit; admitted iff `tat - now <= emission*(limit+burst) - emission`; on admit `tat = max(tat,now)+emission`. From idle, limit+burst start immediately.

### rateLimit `{limit, period(≤24h), key?}`
- Drops excess runs (Skipped). Key fnID[-hash(eval)]; key evaluating to `false` => not limited.
- GCRA: burst = floor(limit/10); emission = period/limit; admit iff `tat - now <= burst*emission`; `tat = max(tat,now)+emission`.
- Not applied to batch runs, debounce fires, cron. Invokes are rate limited (rate-limited invoke fails the caller).

### idempotency expr => rateLimit{limit 1, period 24h, key expr}.

### debounce `{period(1s..7d), key?, timeout?}`
- Key per fn + eval(key).
- No active debounce: store event, t = now+timeout (if set); fire at now + min(period, t-now).
- Active: ignore if stored event.ts > new ts; else replace stored event with latest, fire at now + min(period, t-now) (≥1s).
- Fire: rotate key; schedule run with last event (skip rateLimit/debounce).

### batchEvents `{maxSize, timeout (Go duration ≥1s), key?, if?}`
- Batch key default "default" or eval(key). Append; first item schedules timeout at now+timeout; len >= maxSize => start immediately and rotate. Timeout => start.
- Run: `events` = all, `event` = events[0], idempotency key `<fnID>-<batchID>`. No rateLimit, no cancel pauses.

### priority `{run: expr}` -> int seconds clamp ±43200. Effect: score = runULID_ms - factor*1000 (higher runs earlier).

### cancelOn `[{event, if?, timeout?}]` ≤5. Pause with cancel=true; default expiry 365d.

### timeouts `{start?, finish?}`
- start: if run hasn't started within `start` of creation => cancel.
- finish: once started, if not finished within `finish` => cancel.

### singleton `{key?, mode: skip|cancel}`
- Key fnID[-hash(eval)]; eval false => not singleton.
- Lock holds runID across whole run (incl sleeps/waits), released at finalize.
- skip: new run skipped if lock held. cancel: cancel old run, start new.

## 4. Queue (Go design summary)
Kinds: start, edge, sleep, edge-error, pause (timeout), debounce, schedule-batch, cancel, cron.
Item: id = hash(jobID) (idempotent enqueue; dequeued job ids remembered 12h), at_ms, kind, attempt, max_attempts, payload, run identifiers, priority factor, concurrency keys, throttle, singleton.
Lease: lease 30s, extended every 15s. Scavenger requeues expired leases (at-least-once).
Retry: attempt+1 < max => requeue at backoff [15s,30s,1m,2m,5m,10m,20m,40m,1h,2h] + 0–30s jitter; Retry-After overrides.
Finalize: delete state, dequeue all run jobs, release singleton/semaphores, emit events.

## 5. Pauses
- Indexed by exact event name. Pause id = uuid(sha1(runID+stepID)).
- waitForEvent: expires = timeout ("" = now; duration or RFC3339; ≤366d). Release concurrency. Enqueue timeout item.
- Matching: skip if pause created > 5s after event received; cancel pauses: skip unless event ULID > runID; skip if pause.triggerEventID == evt internal id; evaluate expression must be true; ignore expired.
- Resume: atomically consume (save output under step id; only first wins), enqueue edge now, delete timeout, delete pause.
- Output: matched => raw event map `{name,data,user,id,ts,v?}`; timeout => null.
- Invoke: correlation id `<callerRunID>.<stepID>`; caller pause on `inngest/function.finished` with `async.data.correlation_id == "<cid>"`. Emitted event:
  `{"name":"inngest/function.invoked","id":ULID,"ts":now,"data":{...payload.data, "_inngest":{"source_app_id","source_fn_id","source_fn_v","fn_id","correlation_id","expire","gid","name"}}}`.
  Target lookup: fn uuid == fn_id or slug == fn_id; triggers not checked. Not found => emit `inngest/function.finished {function_id:"",run_id:"",correlation_id,error:{name:"Error",message:"could not find function with ID: X"}}`.
- Caller output: `{"data": result}` or `{"error": error}`; timeout: `{"error":{"name":"InngestInvokeTimeoutError","message":"Timed out waiting for invoked function to complete","error":"InngestInvokeTimeoutError: Timed out waiting for invoked function to complete"}}`.

## 6. Cancellation
- cancelOn pauses: on match, Cancel(run).
- Cancel = Finalize(cancel): delete state + queued items, release singleton, emit `function.finished` (`_inngest.status: Cancelled`, no result/error) and `function.cancelled`.
- REST: `DELETE /v1/runs/{id}`; `POST /api/v2/runs/{id}/cancel`; GraphQL `cancelRun`.
- Bulk `POST /v1/cancellations` (cloud only in OSS; optional): cancel runs of function started in window whose first event satisfies `if`.

## 7. Durations (str2duration)
`[+-]?(<digits>[.<digits>]<unit>)+` or "0"; units ns, us/µs, ms, s, m, h, d (24h), w (168h).
- sleep: len ≥ 10 & parses as date => absolute (past => 0); else duration; ≤366d.
- timeouts for waitForEvent/invoke/signal: duration or RFC3339. waitForEvent "" = now; invoke "" = +1y.
- batchEvents timeout: Go ParseDuration (no d/w).
