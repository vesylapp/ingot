//! Redis implementation of [`StateStore`].
//!
//! Every operation that the SQL store performs inside a transaction is a single Lua script
//! here (one `EVALSHA` round trip), so it is atomic on a Redis node. Observable semantics
//! match [`super::sql::SqlStore`].
//!
//! Key layout (`P` = prefix):
//!
//! | key | type | contents |
//! |-----|------|----------|
//! | `{P:run:<run>}` | hash | `meta` (base JSON), `events`, `status`, `started_at`, `ended_at`, `rv`, `fsp` |
//! | `{P:run:<run>}:sl` | list | step ids in save order |
//! | `{P:run:<run>}:sv` | hash | step id -> JSON value |
//! | `{P:run:<run>}:pd` | set | pending parallel step ids |
//! | `{P:q}:z` | zset | queue item id scored by `at` (lease expiry while leased) |
//! | `{P:q}:i:<id>` | hash | `d` (item body JSON), `r` (run id), `a` (attempt), `l` (lease id), `b` (blocked key) |
//! | `{P:q}:run:<run>` | set | item ids of a run |
//! | `{P:q}:blk:<key>` | zset | items blocked on a concurrency key, scored by `at` |
//! | `{P:q}:done:<id>` | string | completed dedupe item marker (12h TTL) |
//! | `P:kv:<key>` | string | key/value entries (`idem:*` and `gcra:*` included), native TTL |
//! | `P:c:<key>` | zset | concurrency holders scored by expiry |
//! | `P:ch:<holder>` | set | concurrency keys held by a holder |
//! | `P:p:<id>` | hash | pause: `id`, `d` (JSON), `xi` (index set key), `ri` (run set key) |
//! | `P:pidx:<event>` / `P:prun:<run>` | set | pause ids by event / signal index and by run |
//! | `P:pexp` | zset | cancel pause ids by expiry (pruned by `gc`) |
//! | `{P:deb:<key>}` | hash | debounce: `id`, `fn`, `eid`, `event`, `ts`, `ta`, `fa` |
//! | `{P:batch:<key>}:open` | string | id of the open batch for the key |
//! | `{P:batch:<key>}:m:<id>` / `:ev:<id>` / `:ids:<id>` | hash / list / set | batch meta, `[event_id, event]*` and event-id dedupe set |
//!
//! A run's keys share a hash tag, as do all queue keys. Scripts that span groups (create_run
//! with its idempotency key, consume_pause, concurrency) are atomic on a single node but
//! cross-slot on Redis Cluster.

use super::*;
use ::redis::Script;
use ::redis::aio::ConnectionManager;
use anyhow::{Context, anyhow};
use std::collections::HashMap;

/// How long completed job ids are remembered for enqueue dedup.
const DONE_TTL_MS: i64 = 12 * 3600 * 1000;
/// How long a finished run's state stays readable.
const FINISHED_RUN_TTL_MS: i64 = 7 * 24 * 3600 * 1000;
/// Extra lifetime of concurrency bookkeeping keys beyond the latest holder expiry.
const CONCURRENCY_SLACK_MS: i64 = 3600 * 1000;
/// Cancel pauses linger this long past expiry before `gc` removes them.
const CANCEL_PAUSE_GRACE_MS: i64 = 20 * 60 * 1000;

const LUA_TERMINAL: &str = r#"
local function is_terminal(st)
  return st == 'Completed' or st == 'Failed' or st == 'Cancelled' or st == 'Skipped'
end
"#;

/// Returns nil when the run is missing or finished, else {pending_empty, inserted}.
const LUA_SAVE_STEP: &str = r#"
local function save_step(run, step, val)
  local st = redis.call('HGET', run, 'status')
  if (not st) or is_terminal(st) then return nil end
  local ins = redis.call('HSETNX', run .. ':sv', step, val)
  if ins == 1 then redis.call('RPUSH', run .. ':sl', step) end
  local empty = 1
  local pd = run .. ':pd'
  if redis.call('EXISTS', pd) == 1 then
    redis.call('SREM', pd, step)
    if redis.call('SCARD', pd) > 0 then empty = 0 end
  end
  return {empty, ins}
end
"#;

const LUA_DEL_ITEM: &str = r#"
local function del_item(Q, id)
  local ik = Q .. ':i:' .. id
  local f = redis.call('HMGET', ik, 'r', 'b')
  redis.call('ZREM', Q .. ':z', id)
  if f[1] and f[1] ~= '' then redis.call('SREM', Q .. ':run:' .. f[1], id) end
  if f[2] then redis.call('ZREM', Q .. ':blk:' .. f[2], id) end
  return redis.call('DEL', ik)
end
"#;

const LUA_DEL_PAUSE: &str = r#"
local function del_pause(pk, pexp)
  local f = redis.call('HMGET', pk, 'id', 'xi', 'ri')
  if not f[1] then return 0 end
  if f[2] and f[2] ~= '' then redis.call('SREM', f[2], f[1]) end
  if f[3] then redis.call('SREM', f[3], f[1]) end
  redis.call('ZREM', pexp, f[1])
  redis.call('DEL', pk)
  return 1
end
"#;

// ---- runs ----

// KEYS: idem, run. ARGV: run_id, ttl, meta, events, status, started_at, ended_at, rv, fsp
const CREATE_RUN: &str = r#"
local ex = redis.call('GET', KEYS[1])
if ex then return ex end
if tonumber(ARGV[2]) > 0 then redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]) end
redis.call('HSET', KEYS[2], 'meta', ARGV[3], 'events', ARGV[4], 'status', ARGV[5], 'rv', ARGV[8], 'fsp', ARGV[9])
if ARGV[6] ~= '' then redis.call('HSET', KEYS[2], 'started_at', ARGV[6]) end
if ARGV[7] ~= '' then redis.call('HSET', KEYS[2], 'ended_at', ARGV[7]) end
return false
"#;

// KEYS: run. ARGV: started_at, rv, fsp ('' = unchanged)
const UPDATE_META: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if ARGV[1] ~= '' then redis.call('HSETNX', KEYS[1], 'started_at', ARGV[1]) end
if ARGV[2] ~= '' then redis.call('HSET', KEYS[1], 'rv', ARGV[2]) end
if ARGV[3] ~= '' then redis.call('HSET', KEYS[1], 'fsp', ARGV[3]) end
return 1
"#;

// KEYS: run. ARGV: step, value
const SAVE_STEP: &str = r#"
local r = save_step(KEYS[1], ARGV[1], ARGV[2])
if not r then return false end
return r
"#;

// KEYS: run. ARGV: ids...
const SET_PENDING: &str = r#"
local run = KEYS[1]
if redis.call('EXISTS', run) == 0 then return 0 end
local pd = run .. ':pd'
redis.call('DEL', pd)
for i = 1, #ARGV do
  if redis.call('HEXISTS', run .. ':sv', ARGV[i]) == 0 then redis.call('SADD', pd, ARGV[i]) end
end
local t = redis.call('PTTL', run)
if t > 0 and redis.call('EXISTS', pd) == 1 then redis.call('PEXPIRE', pd, t) end
return 1
"#;

// KEYS: run. ARGV: status, at, ttl
const END_RUN: &str = r#"
local run = KEYS[1]
local st = redis.call('HGET', run, 'status')
if (not st) or is_terminal(st) then return 0 end
redis.call('HSET', run, 'status', ARGV[1], 'ended_at', ARGV[2])
redis.call('DEL', run .. ':pd')
redis.call('PEXPIRE', run, ARGV[3])
redis.call('PEXPIRE', run .. ':sl', ARGV[3])
redis.call('PEXPIRE', run .. ':sv', ARGV[3])
return 1
"#;

// ---- queue ----

// KEYS: z. ARGV: Q, id, at, body, run, attempt, dedupe
const ENQUEUE: &str = r#"
local Q, id = ARGV[1], ARGV[2]
if ARGV[7] == '1' and redis.call('EXISTS', Q .. ':done:' .. id) == 1 then return 0 end
local ik = Q .. ':i:' .. id
if redis.call('EXISTS', ik) == 1 then return 0 end
redis.call('HSET', ik, 'd', ARGV[4], 'r', ARGV[5], 'a', ARGV[6])
redis.call('ZADD', KEYS[1], ARGV[3], id)
if ARGV[5] ~= '' then redis.call('SADD', Q .. ':run:' .. ARGV[5], id) end
return 1
"#;

// KEYS: z. ARGV: Q, now, limit, lease_id, until. Returns flat [id, body, attempt]*.
const LEASE: &str = r#"
local Q = ARGV[1]
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, tonumber(ARGV[3]))
local out = {}
for _, id in ipairs(ids) do
  local ik = Q .. ':i:' .. id
  local f = redis.call('HMGET', ik, 'd', 'a', 'b')
  redis.call('ZADD', KEYS[1], ARGV[5], id)
  if f[3] then
    redis.call('ZREM', Q .. ':blk:' .. f[3], id)
    redis.call('HDEL', ik, 'b')
  end
  redis.call('HSET', ik, 'l', ARGV[4])
  out[#out + 1] = id
  out[#out + 1] = f[1] or 'null'
  out[#out + 1] = f[2] or '0'
end
return out
"#;

// KEYS: z. ARGV: Q, id, lease_id, until
const EXTEND_LEASE: &str = r#"
local l = redis.call('HGET', ARGV[1] .. ':i:' .. ARGV[2], 'l')
if l and l == ARGV[3] then
  redis.call('ZADD', KEYS[1], ARGV[4], ARGV[2])
  return 1
end
return 0
"#;

// KEYS: z, done. ARGV: Q, id, lease_id ('' = none), dedupe, done_ttl
const ACK: &str = r#"
local ik = ARGV[1] .. ':i:' .. ARGV[2]
if redis.call('EXISTS', ik) == 1 then
  local l = redis.call('HGET', ik, 'l')
  if (not l) or (ARGV[3] ~= '' and l == ARGV[3]) then del_item(ARGV[1], ARGV[2]) end
end
if ARGV[4] == '1' then redis.call('SET', KEYS[2], '1', 'PX', ARGV[5]) end
return 1
"#;

// KEYS: z. ARGV: Q, id, lease_id, at, attempt, blocked ('' = none), body
const REQUEUE: &str = r#"
local Q, id = ARGV[1], ARGV[2]
if ARGV[3] == '' then return 0 end
local ik = Q .. ':i:' .. id
local f = redis.call('HMGET', ik, 'l', 'b')
if (not f[1]) or f[1] ~= ARGV[3] then return 0 end
if f[2] then redis.call('ZREM', Q .. ':blk:' .. f[2], id) end
redis.call('HSET', ik, 'a', ARGV[5], 'd', ARGV[7])
redis.call('HDEL', ik, 'l', 'b')
redis.call('ZADD', KEYS[1], ARGV[4], id)
if ARGV[6] ~= '' then
  redis.call('HSET', ik, 'b', ARGV[6])
  redis.call('ZADD', Q .. ':blk:' .. ARGV[6], ARGV[4], id)
end
return 1
"#;

// KEYS: z. ARGV: Q, id, at
const RESCHEDULE: &str = r#"
local Q, id = ARGV[1], ARGV[2]
local ik = Q .. ':i:' .. id
if redis.call('EXISTS', ik) == 0 then return 0 end
local f = redis.call('HMGET', ik, 'l', 'b')
if f[1] then return 0 end
redis.call('ZADD', KEYS[1], ARGV[3], id)
if f[2] then redis.call('ZADD', Q .. ':blk:' .. f[2], ARGV[3], id) end
return 1
"#;

// KEYS: z. ARGV: Q, id
const REMOVE_ITEM: &str = r#"
return del_item(ARGV[1], ARGV[2])
"#;

// KEYS: z, run set. ARGV: Q, except
const REMOVE_RUN_ITEMS: &str = r#"
local ids = redis.call('SMEMBERS', KEYS[2])
local n = 0
for _, id in ipairs(ids) do
  if id ~= ARGV[2] then n = n + del_item(ARGV[1], id) end
end
return n
"#;

// KEYS: z, run set. ARGV: Q. Returns flat [id, at, body, attempt, lease|nil]*.
const RUN_ITEMS: &str = r#"
local ids = redis.call('SMEMBERS', KEYS[2])
local out = {}
for _, id in ipairs(ids) do
  local f = redis.call('HMGET', ARGV[1] .. ':i:' .. id, 'd', 'a', 'l')
  local at = redis.call('ZSCORE', KEYS[1], id)
  if f[1] and at then
    out[#out + 1] = id
    out[#out + 1] = at
    out[#out + 1] = f[1]
    out[#out + 1] = f[2] or '0'
    out[#out + 1] = f[3] or ''
  end
end
return out
"#;

// KEYS: z. ARGV: Q, now, n, keys...
const WAKE_BLOCKED: &str = r#"
local Q = ARGV[1]
local n = tonumber(ARGV[3])
for i = 4, #ARGV do
  local bk = Q .. ':blk:' .. ARGV[i]
  local ids = redis.call('ZRANGE', bk, 0, n - 1)
  for _, id in ipairs(ids) do
    redis.call('ZADD', KEYS[1], ARGV[2], id)
    redis.call('HDEL', Q .. ':i:' .. id, 'b')
    redis.call('ZREM', bk, id)
  end
end
return 1
"#;

// ---- concurrency ----

// KEYS: holder set, concurrency zsets (sorted by key name). ARGV: holder, now, expires, slack, keys..., limits...
// Returns 0 when acquired, else the 1-based index of the first saturated key.
const ACQUIRE: &str = r#"
local holder, now, exp = ARGV[1], ARGV[2], tonumber(ARGV[3])
local nk = #KEYS - 1
for i = 1, nk do
  local k = KEYS[i + 1]
  redis.call('ZREMRANGEBYSCORE', k, '-inf', now)
  local n = redis.call('ZCARD', k)
  if redis.call('ZSCORE', k, holder) then n = n - 1 end
  if n >= tonumber(ARGV[4 + nk + i]) then return i end
end
local want = exp + tonumber(ARGV[4])
for i = 1, nk do
  local k = KEYS[i + 1]
  redis.call('ZADD', k, ARGV[3], holder)
  redis.call('SADD', KEYS[1], ARGV[4 + i])
  local t = redis.call('PTTL', k)
  if t < 0 or tonumber(now) + t < want then redis.call('PEXPIREAT', k, string.format('%d', want)) end
end
local t = redis.call('PTTL', KEYS[1])
if t < 0 or tonumber(now) + t < want then redis.call('PEXPIREAT', KEYS[1], string.format('%d', want)) end
return 0
"#;

// KEYS: holder set. ARGV: prefix, holder
const RELEASE: &str = r#"
local keys = redis.call('SMEMBERS', KEYS[1])
for _, k in ipairs(keys) do redis.call('ZREM', ARGV[1] .. ':c:' .. k, ARGV[2]) end
redis.call('DEL', KEYS[1])
return keys
"#;

// KEYS: holder set. ARGV: prefix, holder, expires, slack, now
const EXTEND_CONCURRENCY: &str = r#"
local keys = redis.call('SMEMBERS', KEYS[1])
local now = tonumber(ARGV[5])
local want = tonumber(ARGV[3]) + tonumber(ARGV[4])
for _, k in ipairs(keys) do
  local ck = ARGV[1] .. ':c:' .. k
  redis.call('ZADD', ck, 'XX', ARGV[3], ARGV[2])
  local t = redis.call('PTTL', ck)
  if t >= 0 and now + t < want then redis.call('PEXPIREAT', ck, string.format('%d', want)) end
end
if #keys > 0 then redis.call('PEXPIREAT', KEYS[1], string.format('%d', want)) end
return 1
"#;

// ---- rate limiting / kv ----

// KEYS: gcra key. ARGV: now, emission, tolerance. Returns {0} or {1, retry_at}.
const GCRA: &str = r#"
local now, em, tol = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local tat = tonumber(redis.call('GET', KEYS[1]) or '') or now
if tat < now then tat = now end
if tat - now > tol then return {1, tat - tol} end
local nt = tat + em
redis.call('SET', KEYS[1], string.format('%d', nt), 'PX', string.format('%d', nt + 1000 - now))
return {0}
"#;

// KEYS: key. ARGV: value, ttl ('' = none)
const KV_SET_NX: &str = r#"
local ex = redis.call('GET', KEYS[1])
if ex then return ex end
if ARGV[2] == '' then
  redis.call('SET', KEYS[1], ARGV[1])
elseif tonumber(ARGV[2]) > 0 then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
end
return false
"#;

// KEYS: key. ARGV: value
const KV_DEL_IF: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"#;

// ---- pauses ----

// KEYS: pause, index set ('' key unused when not indexed), run set, pexp. ARGV: id, data, indexed, cancel, expires
const SAVE_PAUSE: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local xi = ''
if ARGV[3] == '1' then
  xi = KEYS[2]
  redis.call('SADD', xi, ARGV[1])
end
redis.call('HSET', KEYS[1], 'id', ARGV[1], 'd', ARGV[2], 'xi', xi, 'ri', KEYS[3])
redis.call('SADD', KEYS[3], ARGV[1])
if ARGV[4] == '1' then redis.call('ZADD', KEYS[4], ARGV[5], ARGV[1]) end
return 1
"#;

// KEYS: index set. ARGV: prefix. Returns pause JSON blobs.
const PAUSES_FOR_INDEX: &str = r#"
local ids = redis.call('SMEMBERS', KEYS[1])
local out = {}
for _, id in ipairs(ids) do
  local d = redis.call('HGET', ARGV[1] .. ':p:' .. id, 'd')
  if d then out[#out + 1] = d else redis.call('SREM', KEYS[1], id) end
end
return out
"#;

// KEYS: pause, pexp
const DELETE_PAUSE: &str = r#"
return del_pause(KEYS[1], KEYS[2])
"#;

// KEYS: run set, pexp. ARGV: prefix
const DELETE_RUN_PAUSES: &str = r#"
local ids = redis.call('SMEMBERS', KEYS[1])
for _, id in ipairs(ids) do del_pause(ARGV[1] .. ':p:' .. id, KEYS[2]) end
redis.call('DEL', KEYS[1])
return #ids
"#;

// KEYS: pause, pexp, run. ARGV: has_value, step, value.
// Returns nil if the pause was gone (or the save did not happen), else pending_empty (0/1).
const CONSUME_PAUSE: &str = r#"
if del_pause(KEYS[1], KEYS[2]) == 0 then return false end
if ARGV[1] ~= '1' then return 1 end
local r = save_step(KEYS[3], ARGV[2], ARGV[3])
if (not r) or r[2] == 0 then return false end
return r[1]
"#;

// KEYS: pexp. ARGV: prefix, cutoff (exclusive), limit
const GC: &str = r#"
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '(' .. ARGV[2], 'LIMIT', 0, tonumber(ARGV[3]))
for _, id in ipairs(ids) do
  if del_pause(ARGV[1] .. ':p:' .. id, KEYS[1]) == 0 then redis.call('ZREM', KEYS[1], id) end
end
return #ids
"#;

// ---- debounce ----

// KEYS: debounce hash. ARGV: new_id, fn_id, event_id, event, event_ts, period, timeout ('' = none), now
// Returns {id, fire_at, created, ignored}.
const DEBOUNCE_UPSERT: &str = r#"
local now, period = tonumber(ARGV[8]), tonumber(ARGV[6])
local function wait(ta)
  local ttl = period
  if ta then ttl = math.min(ttl, ta - now) end
  if ttl < 1000 then ttl = 1000 end
  return now + ttl
end
local f = redis.call('HMGET', KEYS[1], 'id', 'ts', 'ta')
if f[1] then
  if tonumber(f[2]) > tonumber(ARGV[5]) then return {f[1], 0, 0, 1} end
  local ta = nil
  if f[3] and f[3] ~= '' then ta = tonumber(f[3]) end
  local fa = wait(ta)
  redis.call('HSET', KEYS[1], 'eid', ARGV[3], 'event', ARGV[4], 'ts', ARGV[5], 'fa', string.format('%d', fa))
  return {f[1], fa, 0, 0}
end
local ta = nil
local tas = ''
if ARGV[7] ~= '' then
  ta = now + tonumber(ARGV[7])
  tas = string.format('%d', ta)
end
local fa = wait(ta)
redis.call('HSET', KEYS[1], 'id', ARGV[1], 'fn', ARGV[2], 'eid', ARGV[3], 'event', ARGV[4], 'ts', ARGV[5], 'ta', tas, 'fa', string.format('%d', fa))
return {ARGV[1], fa, 1, 0}
"#;

// KEYS: debounce hash. ARGV: id. Returns {event_id, event} or nil.
const DEBOUNCE_TAKE: &str = r#"
local f = redis.call('HMGET', KEYS[1], 'id', 'eid', 'event')
if (not f[1]) or f[1] ~= ARGV[1] then return false end
redis.call('DEL', KEYS[1])
return {f[2], f[3]}
"#;

// ---- batching ----

// KEYS: open pointer. ARGV: B (tag base), new_id, fn_id, event_id, event, max_size, now
// Returns {batch_id, len, created, duplicate}.
const BATCH_APPEND: &str = r#"
local B = ARGV[1]
local id = redis.call('GET', KEYS[1])
local created = 0
if not id then
  id = ARGV[2]
  created = 1
  redis.call('HSET', B .. ':m:' .. id, 'fn', ARGV[3], 'len', 0, 'created_at', ARGV[7])
  redis.call('SET', KEYS[1], id)
end
local mk = B .. ':m:' .. id
local ins = redis.call('SADD', B .. ':ids:' .. id, ARGV[4])
local len
if ins == 1 then
  redis.call('RPUSH', B .. ':ev:' .. id, ARGV[4], ARGV[5])
  len = redis.call('HINCRBY', mk, 'len', 1)
else
  len = tonumber(redis.call('HGET', mk, 'len') or '0')
end
if len >= tonumber(ARGV[6]) then redis.call('DEL', KEYS[1]) end
return {id, len, created, 1 - ins}
"#;

// KEYS: open pointer. ARGV: B, batch_id. Returns flat [event_id, event]* or nil.
const BATCH_TAKE: &str = r#"
local B, id = ARGV[1], ARGV[2]
if redis.call('DEL', B .. ':m:' .. id) == 0 then return false end
if redis.call('GET', KEYS[1]) == id then redis.call('DEL', KEYS[1]) end
local evs = redis.call('LRANGE', B .. ':ev:' .. id, 0, -1)
redis.call('DEL', B .. ':ev:' .. id, B .. ':ids:' .. id)
return evs
"#;

struct Scripts {
    create_run: Script,
    update_meta: Script,
    save_step: Script,
    set_pending: Script,
    end_run: Script,
    enqueue: Script,
    lease: Script,
    extend_lease: Script,
    ack: Script,
    requeue: Script,
    reschedule: Script,
    remove_item: Script,
    remove_run_items: Script,
    run_items: Script,
    wake_blocked: Script,
    acquire: Script,
    release: Script,
    extend_concurrency: Script,
    gcra: Script,
    kv_set_nx: Script,
    kv_del_if: Script,
    save_pause: Script,
    pauses_for_index: Script,
    delete_pause: Script,
    delete_run_pauses: Script,
    consume_pause: Script,
    gc: Script,
    debounce_upsert: Script,
    debounce_take: Script,
    batch_append: Script,
    batch_take: Script,
}

impl Scripts {
    fn new() -> Scripts {
        let s = |parts: &[&str]| Script::new(&parts.concat());
        Scripts {
            create_run: s(&[CREATE_RUN]),
            update_meta: s(&[UPDATE_META]),
            save_step: s(&[LUA_TERMINAL, LUA_SAVE_STEP, SAVE_STEP]),
            set_pending: s(&[SET_PENDING]),
            end_run: s(&[LUA_TERMINAL, END_RUN]),
            enqueue: s(&[ENQUEUE]),
            lease: s(&[LEASE]),
            extend_lease: s(&[EXTEND_LEASE]),
            ack: s(&[LUA_DEL_ITEM, ACK]),
            requeue: s(&[REQUEUE]),
            reschedule: s(&[RESCHEDULE]),
            remove_item: s(&[LUA_DEL_ITEM, REMOVE_ITEM]),
            remove_run_items: s(&[LUA_DEL_ITEM, REMOVE_RUN_ITEMS]),
            run_items: s(&[RUN_ITEMS]),
            wake_blocked: s(&[WAKE_BLOCKED]),
            acquire: s(&[ACQUIRE]),
            release: s(&[RELEASE]),
            extend_concurrency: s(&[EXTEND_CONCURRENCY]),
            gcra: s(&[GCRA]),
            kv_set_nx: s(&[KV_SET_NX]),
            kv_del_if: s(&[KV_DEL_IF]),
            save_pause: s(&[SAVE_PAUSE]),
            pauses_for_index: s(&[PAUSES_FOR_INDEX]),
            delete_pause: s(&[LUA_DEL_PAUSE, DELETE_PAUSE]),
            delete_run_pauses: s(&[LUA_DEL_PAUSE, DELETE_RUN_PAUSES]),
            consume_pause: s(&[LUA_TERMINAL, LUA_SAVE_STEP, LUA_DEL_PAUSE, CONSUME_PAUSE]),
            gc: s(&[LUA_DEL_PAUSE, GC]),
            debounce_upsert: s(&[DEBOUNCE_UPSERT]),
            debounce_take: s(&[DEBOUNCE_TAKE]),
            batch_append: s(&[BATCH_APPEND]),
            batch_take: s(&[BATCH_TAKE]),
        }
    }
}

/// The immutable part of a queue item, stored as JSON in the item hash (`d`).
#[derive(serde::Serialize, serde::Deserialize)]
struct ItemBody {
    kind: QueueKind,
    #[serde(default)]
    run_id: Option<Ulid>,
    #[serde(default)]
    fn_id: Option<Uuid>,
    #[serde(default)]
    dedupe: bool,
    max_attempts: u32,
    payload: Value,
}

impl ItemBody {
    fn encode(item: &QueueItem) -> String {
        to_json(&ItemBody {
            kind: item.kind,
            run_id: item.run_id,
            fn_id: item.fn_id,
            dedupe: item.dedupe,
            max_attempts: item.max_attempts,
            payload: item.payload.clone(),
        })
    }

    fn decode(
        id: String,
        body: &str,
        at: i64,
        attempt: &str,
        lease_id: Option<String>,
    ) -> Result<QueueItem> {
        let b: ItemBody =
            serde_json::from_str(body).with_context(|| format!("decoding queue item {id}"))?;
        Ok(QueueItem {
            id,
            kind: b.kind,
            run_id: b.run_id,
            fn_id: b.fn_id,
            at,
            dedupe: b.dedupe,
            attempt: attempt.parse().unwrap_or(0),
            max_attempts: b.max_attempts,
            payload: b.payload,
            lease_id,
        })
    }
}

fn to_json(v: &impl serde::Serialize) -> String {
    serde_json::to_string(v).expect("serialize")
}

fn parse_ulid(s: &str) -> Result<Ulid> {
    Ulid::from_string(s).map_err(|e| anyhow!("invalid ulid {s}: {e}"))
}

/// Fields of the run hash read to build a [`RunMeta`].
const META_FIELDS: [&str; 6] = ["meta", "status", "started_at", "ended_at", "rv", "fsp"];

fn decode_meta(f: &[Option<String>]) -> Result<Option<RunMeta>> {
    let Some(raw) = f.first().cloned().flatten() else {
        return Ok(None);
    };
    let mut meta: RunMeta = serde_json::from_str(&raw)?;
    let int = |i: usize| {
        f.get(i)
            .cloned()
            .flatten()
            .and_then(|s| s.parse::<i64>().ok())
    };
    if let Some(s) = f.get(1).cloned().flatten() {
        meta.status = RunStatus::parse(&s);
    }
    meta.started_at = int(2);
    meta.ended_at = int(3);
    if let Some(v) = int(4) {
        meta.request_version = v as i32;
    }
    if let Some(v) = f.get(5).cloned().flatten() {
        meta.force_step_plan = v == "1";
    }
    Ok(Some(meta))
}

fn bool_arg(b: bool) -> &'static str {
    if b { "1" } else { "0" }
}

pub struct RedisStore {
    conn: ConnectionManager,
    prefix: String,
    /// Hash-tagged base of all queue keys: `{prefix:q}`.
    q: String,
    scripts: Scripts,
}

impl RedisStore {
    pub async fn connect(uri: &str, prefix: &str) -> Result<RedisStore> {
        let client = ::redis::Client::open(uri)
            .with_context(|| format!("invalid redis uri {}", redact(uri)))?;
        let conn = ConnectionManager::new(client)
            .await
            .with_context(|| format!("connecting to {}", redact(uri)))?;
        let store = RedisStore {
            conn,
            prefix: prefix.to_string(),
            q: format!("{{{prefix}:q}}"),
            scripts: Scripts::new(),
        };
        // Preload scripts so the first calls are plain EVALSHAs.
        let mut c = store.conn();
        for s in store.all_scripts() {
            s.load_async(&mut c)
                .await
                .context("loading redis scripts")?;
        }
        Ok(store)
    }

    fn all_scripts(&self) -> Vec<&Script> {
        let s = &self.scripts;
        vec![
            &s.create_run,
            &s.update_meta,
            &s.save_step,
            &s.set_pending,
            &s.end_run,
            &s.enqueue,
            &s.lease,
            &s.extend_lease,
            &s.ack,
            &s.requeue,
            &s.reschedule,
            &s.remove_item,
            &s.remove_run_items,
            &s.run_items,
            &s.wake_blocked,
            &s.acquire,
            &s.release,
            &s.extend_concurrency,
            &s.gcra,
            &s.kv_set_nx,
            &s.kv_del_if,
            &s.save_pause,
            &s.pauses_for_index,
            &s.delete_pause,
            &s.delete_run_pauses,
            &s.consume_pause,
            &s.gc,
            &s.debounce_upsert,
            &s.debounce_take,
            &s.batch_append,
            &s.batch_take,
        ]
    }

    fn conn(&self) -> ConnectionManager {
        self.conn.clone()
    }

    fn run_key(&self, run_id: Ulid) -> String {
        format!("{{{}:run:{run_id}}}", self.prefix)
    }
    fn qz(&self) -> String {
        format!("{}:z", self.q)
    }
    fn kv_key(&self, key: &str) -> String {
        format!("{}:kv:{key}", self.prefix)
    }
    fn holder_key(&self, holder: &str) -> String {
        format!("{}:ch:{holder}", self.prefix)
    }
    fn conc_key(&self, key: &str) -> String {
        format!("{}:c:{key}", self.prefix)
    }
    fn pause_key(&self, id: Uuid) -> String {
        format!("{}:p:{id}", self.prefix)
    }
    fn pause_index_key(&self, index: &str) -> String {
        format!("{}:pidx:{index}", self.prefix)
    }
    fn pause_run_key(&self, run_id: Ulid) -> String {
        format!("{}:prun:{run_id}", self.prefix)
    }
    fn pause_exp_key(&self) -> String {
        format!("{}:pexp", self.prefix)
    }
    fn debounce_key(&self, key: &str) -> String {
        format!("{{{}:deb:{key}}}", self.prefix)
    }
    fn batch_base(&self, key: &str) -> String {
        format!("{{{}:batch:{key}}}", self.prefix)
    }

    async fn pauses_for_index(&self, index: &str) -> Result<Vec<Pause>> {
        let raw: Vec<String> = self
            .scripts
            .pauses_for_index
            .key(self.pause_index_key(index))
            .arg(&self.prefix)
            .invoke_async(&mut self.conn())
            .await?;
        raw.iter().map(|d| Ok(serde_json::from_str(d)?)).collect()
    }
}

fn redact(uri: &str) -> String {
    match url::Url::parse(uri) {
        Ok(mut u) if u.password().is_some() => {
            let _ = u.set_password(Some("***"));
            u.to_string()
        }
        _ => uri.to_string(),
    }
}

/// Parses the `{pending_empty, inserted}` reply of the save-step snippet.
fn save_result(r: Option<(i64, i64)>) -> SaveStep {
    match r {
        None => SaveStep::NoRun,
        Some((empty, ins)) => SaveStep::Saved {
            pending_empty: empty == 1,
            duplicate: ins == 0,
        },
    }
}

#[async_trait]
impl StateStore for RedisStore {
    // ---- runs ----

    async fn create_run(
        &self,
        meta: &RunMeta,
        events: &[Value],
        idempotency_ttl_ms: i64,
    ) -> Result<CreateRun> {
        let existing: Option<String> = self
            .scripts
            .create_run
            .key(self.kv_key(&format!("idem:{}", meta.idempotency_key)))
            .key(self.run_key(meta.run_id))
            .arg(meta.run_id.to_string())
            .arg(idempotency_ttl_ms)
            .arg(to_json(meta))
            .arg(to_json(&events))
            .arg(meta.status.as_str())
            .arg(meta.started_at.map(|v| v.to_string()).unwrap_or_default())
            .arg(meta.ended_at.map(|v| v.to_string()).unwrap_or_default())
            .arg(meta.request_version)
            .arg(bool_arg(meta.force_step_plan))
            .invoke_async(&mut self.conn())
            .await?;
        Ok(match existing {
            Some(id) => CreateRun::Duplicate(id),
            None => CreateRun::Created,
        })
    }

    async fn load_run(&self, run_id: Ulid) -> Result<Option<RunState>> {
        let rk = self.run_key(run_id);
        let (fields, order, values): (Vec<Option<String>>, Vec<String>, HashMap<String, String>) =
            ::redis::pipe()
                .atomic()
                .cmd("HMGET")
                .arg(&rk)
                .arg(&META_FIELDS[..])
                .arg("events")
                .cmd("LRANGE")
                .arg(format!("{rk}:sl"))
                .arg(0)
                .arg(-1)
                .cmd("HGETALL")
                .arg(format!("{rk}:sv"))
                .query_async(&mut self.conn())
                .await?;
        let Some(meta) = decode_meta(&fields)? else {
            return Ok(None);
        };
        let events: Vec<Value> = match fields.get(META_FIELDS.len()).cloned().flatten() {
            Some(e) => serde_json::from_str(&e)?,
            None => vec![],
        };
        let steps = steps_in_order(order, values)?;
        Ok(Some(RunState {
            meta,
            events,
            steps,
        }))
    }

    async fn load_meta(&self, run_id: Ulid) -> Result<Option<RunMeta>> {
        let fields: Vec<Option<String>> = ::redis::cmd("HMGET")
            .arg(self.run_key(run_id))
            .arg(&META_FIELDS[..])
            .query_async(&mut self.conn())
            .await?;
        decode_meta(&fields)
    }

    async fn update_meta(&self, run_id: Ulid, u: &MetaUpdate) -> Result<()> {
        let _: i64 = self
            .scripts
            .update_meta
            .key(self.run_key(run_id))
            .arg(u.started_at.map(|v| v.to_string()).unwrap_or_default())
            .arg(u.request_version.map(|v| v.to_string()).unwrap_or_default())
            .arg(u.force_step_plan.map(bool_arg).unwrap_or_default())
            .invoke_async(&mut self.conn())
            .await?;
        Ok(())
    }

    async fn save_step(&self, run_id: Ulid, step_id: &str, value: &Value) -> Result<SaveStep> {
        let r: Option<(i64, i64)> = self
            .scripts
            .save_step
            .key(self.run_key(run_id))
            .arg(step_id)
            .arg(to_json(value))
            .invoke_async(&mut self.conn())
            .await?;
        Ok(save_result(r))
    }

    async fn set_pending(&self, run_id: Ulid, ids: &[String]) -> Result<()> {
        let _: i64 = self
            .scripts
            .set_pending
            .key(self.run_key(run_id))
            .arg(ids)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(())
    }

    async fn end_run(&self, run_id: Ulid, status: RunStatus, at: i64) -> Result<bool> {
        let n: i64 = self
            .scripts
            .end_run
            .key(self.run_key(run_id))
            .arg(status.as_str())
            .arg(at)
            .arg(FINISHED_RUN_TTL_MS)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(n == 1)
    }

    async fn load_steps(&self, run_id: Ulid) -> Result<Vec<(String, Value)>> {
        let rk = self.run_key(run_id);
        let (order, values): (Vec<String>, HashMap<String, String>) = ::redis::pipe()
            .atomic()
            .cmd("LRANGE")
            .arg(format!("{rk}:sl"))
            .arg(0)
            .arg(-1)
            .cmd("HGETALL")
            .arg(format!("{rk}:sv"))
            .query_async(&mut self.conn())
            .await?;
        steps_in_order(order, values)
    }

    async fn load_events(&self, run_id: Ulid) -> Result<Vec<Value>> {
        let raw: Option<String> = ::redis::cmd("HGET")
            .arg(self.run_key(run_id))
            .arg("events")
            .query_async(&mut self.conn())
            .await?;
        match raw {
            Some(e) => Ok(serde_json::from_str(&e)?),
            None => Ok(vec![]),
        }
    }

    // ---- queue ----

    async fn enqueue(&self, item: &QueueItem) -> Result<bool> {
        let n: i64 = self
            .scripts
            .enqueue
            .key(self.qz())
            .arg(&self.q)
            .arg(&item.id)
            .arg(item.at)
            .arg(ItemBody::encode(item))
            .arg(item.run_id.map(|r| r.to_string()).unwrap_or_default())
            .arg(item.attempt)
            .arg(bool_arg(item.dedupe))
            .invoke_async(&mut self.conn())
            .await?;
        Ok(n == 1)
    }

    async fn lease(&self, now: i64, limit: i64, lease_ms: i64) -> Result<Vec<QueueItem>> {
        if limit <= 0 {
            return Ok(vec![]);
        }
        let lease_id = Ulid::generate().to_string();
        let until = now + lease_ms;
        let flat: Vec<String> = self
            .scripts
            .lease
            .key(self.qz())
            .arg(&self.q)
            .arg(now)
            .arg(limit)
            .arg(&lease_id)
            .arg(until)
            .invoke_async(&mut self.conn())
            .await?;
        let mut out = Vec::with_capacity(flat.len() / 3);
        let mut it = flat.into_iter();
        while let (Some(id), Some(body), Some(attempt)) = (it.next(), it.next(), it.next()) {
            out.push(ItemBody::decode(
                id,
                &body,
                until,
                &attempt,
                Some(lease_id.clone()),
            )?);
        }
        Ok(out)
    }

    async fn extend_lease(&self, item_id: &str, lease_id: &str, until: i64) -> Result<bool> {
        let n: i64 = self
            .scripts
            .extend_lease
            .key(self.qz())
            .arg(&self.q)
            .arg(item_id)
            .arg(lease_id)
            .arg(until)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(n == 1)
    }

    async fn ack(&self, item: &QueueItem) -> Result<()> {
        let _: i64 = self
            .scripts
            .ack
            .key(self.qz())
            .key(format!("{}:done:{}", self.q, item.id))
            .arg(&self.q)
            .arg(&item.id)
            .arg(item.lease_id.as_deref().unwrap_or(""))
            .arg(bool_arg(item.dedupe))
            .arg(DONE_TTL_MS)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(())
    }

    async fn requeue(
        &self,
        item: &QueueItem,
        at: i64,
        attempt: u32,
        blocked_on: Option<&str>,
    ) -> Result<()> {
        let _: i64 = self
            .scripts
            .requeue
            .key(self.qz())
            .arg(&self.q)
            .arg(&item.id)
            .arg(item.lease_id.as_deref().unwrap_or(""))
            .arg(at)
            .arg(attempt)
            .arg(blocked_on.unwrap_or(""))
            .arg(ItemBody::encode(item))
            .invoke_async(&mut self.conn())
            .await?;
        Ok(())
    }

    async fn reschedule(&self, item_id: &str, at: i64) -> Result<bool> {
        let n: i64 = self
            .scripts
            .reschedule
            .key(self.qz())
            .arg(&self.q)
            .arg(item_id)
            .arg(at)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(n == 1)
    }

    async fn remove_item(&self, item_id: &str) -> Result<()> {
        let _: i64 = self
            .scripts
            .remove_item
            .key(self.qz())
            .arg(&self.q)
            .arg(item_id)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(())
    }

    async fn remove_run_items(&self, run_id: Ulid, except: Option<&str>) -> Result<()> {
        let _: i64 = self
            .scripts
            .remove_run_items
            .key(self.qz())
            .key(format!("{}:run:{run_id}", self.q))
            .arg(&self.q)
            .arg(except.unwrap_or(""))
            .invoke_async(&mut self.conn())
            .await?;
        Ok(())
    }

    async fn run_items(&self, run_id: Ulid, limit: i64) -> Result<Vec<QueueItem>> {
        let flat: Vec<String> = self
            .scripts
            .run_items
            .key(self.qz())
            .key(format!("{}:run:{run_id}", self.q))
            .arg(&self.q)
            .invoke_async(&mut self.conn())
            .await?;
        let mut out = Vec::with_capacity(flat.len() / 5);
        let mut it = flat.into_iter();
        while let (Some(id), Some(at), Some(body), Some(attempt), Some(lease)) =
            (it.next(), it.next(), it.next(), it.next(), it.next())
        {
            let at = parse_score(&at)?;
            let lease = (!lease.is_empty()).then_some(lease);
            out.push(ItemBody::decode(id, &body, at, &attempt, lease)?);
        }
        out.sort_by_key(|i| i.at);
        out.truncate(limit.max(0) as usize);
        Ok(out)
    }

    async fn next_due(&self) -> Result<Option<i64>> {
        let r: Vec<String> = ::redis::cmd("ZRANGE")
            .arg(self.qz())
            .arg(0)
            .arg(0)
            .arg("WITHSCORES")
            .query_async(&mut self.conn())
            .await?;
        r.get(1).map(|s| parse_score(s)).transpose()
    }

    async fn ping(&self) -> Result<()> {
        let _: String = ::redis::cmd("PING").query_async(&mut self.conn()).await?;
        Ok(())
    }

    async fn queue_stats(&self, now: i64) -> Result<QueueStats> {
        let (total, ready, first): (i64, i64, Vec<String>) = ::redis::pipe()
            .cmd("ZCARD")
            .arg(self.qz())
            .cmd("ZCOUNT")
            .arg(self.qz())
            .arg("-inf")
            .arg(now)
            .cmd("ZRANGE")
            .arg(self.qz())
            .arg(0)
            .arg(0)
            .arg("WITHSCORES")
            .query_async(&mut self.conn())
            .await?;
        Ok(QueueStats {
            total,
            ready,
            oldest_at: first.get(1).map(|s| parse_score(s)).transpose()?,
        })
    }

    async fn wake_blocked(&self, keys: &[String], n: i64) -> Result<()> {
        if keys.is_empty() || n <= 0 {
            return Ok(());
        }
        let _: i64 = self
            .scripts
            .wake_blocked
            .key(self.qz())
            .arg(&self.q)
            .arg(now_ms())
            .arg(n)
            .arg(keys)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(())
    }

    // ---- concurrency ----

    async fn acquire_concurrency(
        &self,
        holder: &str,
        keys: &[ConcurrencyKey],
        expires_at: i64,
    ) -> Result<Option<String>> {
        if keys.is_empty() {
            return Ok(None);
        }
        let mut sorted: Vec<&ConcurrencyKey> = keys.iter().collect();
        sorted.sort_by(|a, b| a.key.cmp(&b.key));
        let mut inv = self.scripts.acquire.key(self.holder_key(holder));
        for k in &sorted {
            inv.key(self.conc_key(&k.key));
        }
        inv.arg(holder)
            .arg(now_ms())
            .arg(expires_at)
            .arg(CONCURRENCY_SLACK_MS);
        for k in &sorted {
            inv.arg(&k.key);
        }
        for k in &sorted {
            inv.arg(k.limit);
        }
        let idx: i64 = inv.invoke_async(&mut self.conn()).await?;
        Ok(match idx {
            0 => None,
            i => Some(sorted[(i - 1) as usize].key.clone()),
        })
    }

    async fn release_concurrency(&self, holder: &str) -> Result<Vec<String>> {
        Ok(self
            .scripts
            .release
            .key(self.holder_key(holder))
            .arg(&self.prefix)
            .arg(holder)
            .invoke_async(&mut self.conn())
            .await?)
    }

    async fn extend_concurrency(&self, holder: &str, expires_at: i64) -> Result<()> {
        let _: i64 = self
            .scripts
            .extend_concurrency
            .key(self.holder_key(holder))
            .arg(&self.prefix)
            .arg(holder)
            .arg(expires_at)
            .arg(CONCURRENCY_SLACK_MS)
            .arg(now_ms())
            .invoke_async(&mut self.conn())
            .await?;
        Ok(())
    }

    async fn concurrency_in_use(&self, key: &str) -> Result<i64> {
        Ok(::redis::cmd("ZCOUNT")
            .arg(self.conc_key(key))
            .arg(format!("({}", now_ms()))
            .arg("+inf")
            .query_async(&mut self.conn())
            .await?)
    }

    // ---- rate limiting ----

    async fn gcra(&self, key: &str, now: i64, emission_ms: i64, tolerance_ms: i64) -> Result<Gcra> {
        let r: Vec<i64> = self
            .scripts
            .gcra
            .key(self.kv_key(&format!("gcra:{key}")))
            .arg(now)
            .arg(emission_ms)
            .arg(tolerance_ms)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(match r.as_slice() {
            [1, retry_at] => Err(*retry_at),
            _ => Ok(()),
        })
    }

    // ---- key/value ----

    async fn kv_set_nx(
        &self,
        key: &str,
        value: &str,
        ttl_ms: Option<i64>,
    ) -> Result<Option<String>> {
        Ok(self
            .scripts
            .kv_set_nx
            .key(self.kv_key(key))
            .arg(value)
            .arg(ttl_ms.map(|t| t.to_string()).unwrap_or_default())
            .invoke_async(&mut self.conn())
            .await?)
    }

    async fn kv_get(&self, key: &str) -> Result<Option<String>> {
        Ok(::redis::cmd("GET")
            .arg(self.kv_key(key))
            .query_async(&mut self.conn())
            .await?)
    }

    async fn kv_del(&self, key: &str, if_value: Option<&str>) -> Result<bool> {
        let n: i64 = match if_value {
            Some(v) => {
                self.scripts
                    .kv_del_if
                    .key(self.kv_key(key))
                    .arg(v)
                    .invoke_async(&mut self.conn())
                    .await?
            }
            None => {
                ::redis::cmd("DEL")
                    .arg(self.kv_key(key))
                    .query_async(&mut self.conn())
                    .await?
            }
        };
        Ok(n > 0)
    }

    // ---- pauses ----

    async fn save_pause(&self, p: &Pause) -> Result<bool> {
        let index = match p.kind {
            PauseKind::Signal => p.event.as_ref().map(|s| format!("signal:{s}")),
            PauseKind::Invoke => None,
            _ => p.event.clone(),
        };
        let n: i64 = self
            .scripts
            .save_pause
            .key(self.pause_key(p.id))
            .key(self.pause_index_key(index.as_deref().unwrap_or("")))
            .key(self.pause_run_key(p.run_id))
            .key(self.pause_exp_key())
            .arg(p.id.to_string())
            .arg(to_json(p))
            .arg(bool_arg(index.is_some()))
            .arg(bool_arg(p.kind == PauseKind::Cancel))
            .arg(p.expires_at)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(n == 1)
    }

    async fn get_pause(&self, id: Uuid) -> Result<Option<Pause>> {
        let d: Option<String> = ::redis::cmd("HGET")
            .arg(self.pause_key(id))
            .arg("d")
            .query_async(&mut self.conn())
            .await?;
        d.map(|d| Ok(serde_json::from_str(&d)?)).transpose()
    }

    async fn pauses_for_event(&self, event: &str) -> Result<Vec<Pause>> {
        self.pauses_for_index(event).await
    }

    async fn pauses_for_signal(&self, signal: &str) -> Result<Vec<Pause>> {
        self.pauses_for_index(&format!("signal:{signal}")).await
    }

    async fn consume_pause(&self, p: &Pause, value: Option<&Value>) -> Result<Option<bool>> {
        let r: Option<i64> = self
            .scripts
            .consume_pause
            .key(self.pause_key(p.id))
            .key(self.pause_exp_key())
            .key(self.run_key(p.run_id))
            .arg(bool_arg(value.is_some()))
            .arg(&p.step_id)
            .arg(value.map(to_json).unwrap_or_default())
            .invoke_async(&mut self.conn())
            .await?;
        Ok(r.map(|e| e == 1))
    }

    async fn delete_pause(&self, id: Uuid) -> Result<()> {
        let _: i64 = self
            .scripts
            .delete_pause
            .key(self.pause_key(id))
            .key(self.pause_exp_key())
            .invoke_async(&mut self.conn())
            .await?;
        Ok(())
    }

    async fn delete_run_pauses(&self, run_id: Ulid) -> Result<()> {
        let _: i64 = self
            .scripts
            .delete_run_pauses
            .key(self.pause_run_key(run_id))
            .key(self.pause_exp_key())
            .arg(&self.prefix)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(())
    }

    // ---- debounce ----

    async fn debounce_upsert(
        &self,
        key: &str,
        fn_id: Uuid,
        event_id: Ulid,
        event: &Value,
        event_ts: i64,
        period_ms: i64,
        timeout_ms: Option<i64>,
        now: i64,
    ) -> Result<DebounceUpsert> {
        let (id, fire_at, created, ignored): (String, i64, i64, i64) = self
            .scripts
            .debounce_upsert
            .key(self.debounce_key(key))
            .arg(Ulid::generate().to_string())
            .arg(fn_id.to_string())
            .arg(event_id.to_string())
            .arg(to_json(event))
            .arg(event_ts)
            .arg(period_ms)
            .arg(timeout_ms.map(|t| t.to_string()).unwrap_or_default())
            .arg(now)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(DebounceUpsert {
            id,
            fire_at,
            created: created == 1,
            ignored: ignored == 1,
        })
    }

    async fn debounce_take(&self, key: &str, id: &str) -> Result<Option<(Ulid, Value)>> {
        let r: Option<(String, String)> = self
            .scripts
            .debounce_take
            .key(self.debounce_key(key))
            .arg(id)
            .invoke_async(&mut self.conn())
            .await?;
        r.map(|(eid, ev)| Ok((parse_ulid(&eid)?, serde_json::from_str(&ev)?)))
            .transpose()
    }

    // ---- batching ----

    async fn batch_append(
        &self,
        key: &str,
        fn_id: Uuid,
        event: &Value,
        event_id: Ulid,
        max_size: u32,
    ) -> Result<BatchAppend> {
        let base = self.batch_base(key);
        let (id, len, created, duplicate): (String, i64, i64, i64) = self
            .scripts
            .batch_append
            .key(format!("{base}:open"))
            .arg(&base)
            .arg(Ulid::generate().to_string())
            .arg(fn_id.to_string())
            .arg(event_id.to_string())
            .arg(to_json(event))
            .arg(max_size)
            .arg(now_ms())
            .invoke_async(&mut self.conn())
            .await?;
        Ok(BatchAppend {
            batch_id: parse_ulid(&id)?,
            len: len as u32,
            created: created == 1,
            duplicate: duplicate == 1,
        })
    }

    async fn batch_take(&self, key: &str, batch_id: Ulid) -> Result<Option<Vec<(Ulid, Value)>>> {
        let base = self.batch_base(key);
        let flat: Option<Vec<String>> = self
            .scripts
            .batch_take
            .key(format!("{base}:open"))
            .arg(&base)
            .arg(batch_id.to_string())
            .invoke_async(&mut self.conn())
            .await?;
        let Some(flat) = flat else { return Ok(None) };
        let mut out = Vec::with_capacity(flat.len() / 2);
        let mut it = flat.into_iter();
        while let (Some(eid), Some(ev)) = (it.next(), it.next()) {
            out.push((parse_ulid(&eid)?, serde_json::from_str(&ev)?));
        }
        Ok(Some(out))
    }

    async fn gc(&self, now: i64) -> Result<()> {
        // Queue dedupe markers, kv entries, idempotency and GCRA keys expire natively;
        // expired concurrency holders are pruned on acquire and ignored by counts.
        loop {
            let n: i64 = self
                .scripts
                .gc
                .key(self.pause_exp_key())
                .arg(&self.prefix)
                .arg(now - CANCEL_PAUSE_GRACE_MS)
                .arg(500)
                .invoke_async(&mut self.conn())
                .await?;
            if n < 500 {
                return Ok(());
            }
        }
    }
}

fn parse_score(s: &str) -> Result<i64> {
    s.parse::<i64>()
        .or_else(|_| s.parse::<f64>().map(|f| f as i64))
        .map_err(|e| anyhow!("invalid score {s}: {e}"))
}

fn steps_in_order(
    order: Vec<String>,
    mut values: HashMap<String, String>,
) -> Result<Vec<(String, Value)>> {
    let mut steps = Vec::with_capacity(order.len());
    for id in order {
        if let Some(v) = values.remove(&id) {
            steps.push((id, serde_json::from_str(&v)?));
        }
    }
    Ok(steps)
}
