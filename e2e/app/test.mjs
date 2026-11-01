// E2E test runner: drives the e2e app through the real Inngest SDK and verifies via the REST API.
import { createHash, randomUUID } from "node:crypto";
import { Inngest } from "inngest";

const API = process.env.INNGEST_BASE_URL || "http://host.docker.internal:8288";
const APP = process.env.APP_URL || "http://localhost:3939";
const SIGNING_KEY = process.env.INNGEST_SIGNING_KEY;
const EVENT_KEY = process.env.INNGEST_EVENT_KEY;
const ONLY = process.env.ONLY ? process.env.ONLY.split(",") : null;
// The reference server's v1 API returns empty outputs; only check REST output against oxidle.
const STRICT_REST = process.env.TARGET !== "inngest";

const inngest = new Inngest({ id: "e2e-runner" });

function bearer() {
  const m = SIGNING_KEY.match(/^signkey-\w+-/);
  const prefix = m ? m[0] : "";
  const raw = SIGNING_KEY.slice(prefix.length);
  return prefix + createHash("sha256").update(Buffer.from(raw, "hex")).digest("hex");
}
const auth = { Authorization: `Bearer ${bearer()}` };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const uid = () => randomUUID().slice(0, 8);

async function api(path, opts = {}) {
  const res = await fetch(API + path, { ...opts, headers: { ...auth, ...(opts.headers || {}) } });
  const text = await res.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = text;
  }
  return { status: res.status, body };
}

async function send(events) {
  const res = await inngest.send(events);
  return res.ids;
}

async function runsFor(eventId) {
  const r = await api(`/v1/events/${eventId}/runs`);
  if (r.status !== 200) throw new Error(`runs lookup failed: ${r.status} ${JSON.stringify(r.body)}`);
  return r.body.data;
}

const TERMINAL = ["Completed", "Failed", "Cancelled"];

async function getRun(runId) {
  const r = await api(`/v1/runs/${runId}`);
  return r.status === 200 ? r.body.data : null;
}

/** Wait until `count` runs exist for the event and all are terminal (read via GET /v1/runs/:id). */
async function waitRuns(eventId, { count = 1, timeout = 30000 } = {}) {
  const deadline = Date.now() + timeout;
  let runs = [];
  while (Date.now() < deadline) {
    const listed = await runsFor(eventId);
    if (listed.length >= count) {
      runs = await Promise.all(listed.map((r) => getRun(r.run_id)));
      if (runs.every((r) => r && TERMINAL.includes(r.status))) return runs;
    }
    await sleep(250);
  }
  throw new Error(`timed out waiting for ${count} terminal run(s) for ${eventId}: ${JSON.stringify(runs)}`);
}

async function waitRun(eventId, opts) {
  const run = (await waitRuns(eventId, opts))[0];
  const results = (await appState()).results;
  if (run.status === "Completed") {
    const recorded = results[run.run_id];
    if (STRICT_REST) eq(run.output, recorded, "REST output vs recorded result:");
    run.output = recorded;
  }
  return run;
}

async function appState() {
  return (await fetch(APP + "/state")).json();
}

async function waitFor(fn, timeout = 20000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const v = await fn();
    if (v) return v;
    await sleep(250);
  }
  throw new Error("condition not met in time");
}

function canon(v) {
  if (Array.isArray(v)) return v.map(canon);
  if (v && typeof v === "object") return Object.fromEntries(Object.keys(v).sort().map((k) => [k, canon(v[k])]));
  return v;
}
function eq(actual, expected, msg = "") {
  const a = JSON.stringify(canon(actual));
  const e = JSON.stringify(canon(expected));
  if (a !== e) throw new Error(`${msg} expected ${e}, got ${a}`);
}
function ok(cond, msg) {
  if (!cond) throw new Error(msg);
}

const tests = {
  async hello() {
    const [id] = await send({ name: "e2e/hello", data: { name: "world" } });
    const run = await waitRun(id);
    eq(run.status, "Completed");
    eq(run.output, { message: "hello world" });
    const single = await api(`/v1/runs/${run.run_id}`);
    eq(single.body.data.run_id, run.run_id);
  },

  async steps() {
    const [id] = await send({ name: "e2e/steps", data: { n: 1 } });
    const run = await waitRun(id);
    eq(run.output, { a: 2, b: 4, c: 7 });
  },

  async sleep() {
    const [id] = await send({ name: "e2e/sleep", data: {} });
    const run = await waitRun(id);
    eq(run.status, "Completed");
    ok(run.output.slept >= 1900, `slept only ${run.output.slept}ms`);
  },

  async waitForEventMatch() {
    const k = uid();
    const [id] = await send({ name: "e2e/wait", data: { id: k } });
    await sleep(1500);
    await send({ name: "e2e/approve", data: { id: "other", by: "mallory" } });
    await send({ name: "e2e/approve", data: { id: k, by: "bob" } });
    const run = await waitRun(id);
    eq(run.output, { approved: "bob" });
  },

  async waitForEventTimeout() {
    const [id] = await send({ name: "e2e/wait", data: { id: uid(), timeout: "2s" } });
    const run = await waitRun(id);
    eq(run.output, { approved: null });
  },

  async waitForEventIf() {
    const ref = uid();
    const [id] = await send({ name: "e2e/wait-if", data: { min: 10, ref } });
    await sleep(1500);
    await send([
      { name: "e2e/amount", data: { amount: 5, ref } },
      { name: "e2e/amount", data: { amount: 50, ref: "nope" } },
    ]);
    await sleep(500);
    await send({ name: "e2e/amount", data: { amount: 20, ref } });
    const run = await waitRun(id);
    eq(run.output, { amount: 20 });
  },

  async retries() {
    const [id] = await send({ name: "e2e/flaky", data: {} });
    const run = await waitRun(id, { timeout: 30000 });
    eq(run.status, "Completed");
    eq(run.output, { attempts: 3 });
  },

  async failureAndOnFailure() {
    const [id] = await send({ name: "e2e/doomed", data: {} });
    const run = await waitRun(id);
    eq(run.status, "Failed");
    if (STRICT_REST) ok(JSON.stringify(run.output).includes("this always fails"), `unexpected output ${JSON.stringify(run.output)}`);
    await waitFor(async () => (await appState()).failures.some((f) => f.run === run.run_id));
    const f = (await appState()).failures.find((f) => f.run === run.run_id);
    eq(f.fn, "e2e-doomed");
    eq(f.message, "this always fails");
  },

  async parallel() {
    const [id] = await send({ name: "e2e/parallel", data: {} });
    const run = await waitRun(id);
    eq(run.output, { total: 6 });
  },

  async invoke() {
    const [id] = await send({ name: "e2e/parent", data: { x: 7 } });
    const run = await waitRun(id);
    eq(run.output, { fromChild: 14 });
  },

  async cancelOn() {
    const k = uid();
    const [id] = await send({ name: "e2e/cancellable", data: { id: k } });
    await sleep(1500);
    await send({ name: "e2e/cancel", data: { id: k } });
    const run = await waitRun(id, { timeout: 10000 });
    eq(run.status, "Cancelled");
  },

  async cancelViaApi() {
    const [id] = await send({ name: "e2e/cancellable", data: { id: uid() } });
    let runs = [];
    await waitFor(async () => (runs = await runsFor(id)).length === 1);
    await sleep(1000);
    const r = await api(`/v1/runs/${runs[0].run_id}`, { method: "DELETE" });
    eq(r.status, 200, "cancel status");
    const run = await waitRun(id, { timeout: 10000 });
    eq(run.status, "Cancelled");
  },

  async concurrencyLimit() {
    const ids = await send([1, 2, 3].map(() => ({ name: "e2e/limited", data: {} })));
    for (const id of ids) await waitRun(id, { timeout: 30000 });
    eq((await appState()).maxActive.limited, 1, "max concurrent");
  },

  async concurrencyKey() {
    const u = uid();
    const ids = await send([u, u, `${u}-other`].map((user) => ({ name: "e2e/keyed", data: { user } })));
    for (const id of ids) await waitRun(id, { timeout: 30000 });
    eq((await appState()).maxActive[`keyed-${u}`], 1, "max concurrent per key");
  },

  async throttle() {
    const ids = await send([1, 2, 3].map(() => ({ name: "e2e/throttled", data: {} })));
    const runs = [];
    for (const id of ids) runs.push(await waitRun(id, { timeout: 30000 }));
    const starts = runs.map((r) => r.output).sort();
    ok(starts[1] - starts[0] >= 1500, `throttle gap ${starts[1] - starts[0]}`);
    ok(starts[2] - starts[1] >= 1500, `throttle gap ${starts[2] - starts[1]}`);
  },

  async rateLimit() {
    const user = uid();
    const ids = await send([1, 2, 3].map(() => ({ name: "e2e/ratelimited", data: { user } })));
    await sleep(3000);
    let total = 0;
    for (const id of ids) total += (await runsFor(id)).length;
    eq(total, 1, "rate limited runs");
  },

  async debounce() {
    const k = uid();
    const ids = [];
    for (const seq of [1, 2, 3]) {
      ids.push(...(await send({ name: "e2e/debounced", data: { k, seq } })));
      await sleep(300);
    }
    const run = await waitRun(ids[2], { timeout: 15000 });
    eq(run.output, { last: 3 });
    eq((await runsFor(ids[0])).length + (await runsFor(ids[1])).length, 0, "earlier events have no runs");
  },

  async batchFull() {
    const tag = uid();
    const ids = await send([1, 2, 3].map((seq) => ({ name: "e2e/batched", data: { seq, tag } })));
    const result = await waitFor(async () =>
      Object.values((await appState()).results).find((r) => r && r.tag === tag && r.count !== undefined),
    15000);
    eq({ ...result, seqs: [...result.seqs].sort() }, { tag, count: 3, seqs: [1, 2, 3] });
    if (STRICT_REST) eq((await waitRun(ids[0], { timeout: 5000 })).output, result);
  },

  async batchTimeout() {
    const tag = uid();
    await send([1, 2].map((seq) => ({ name: "e2e/batch-timeout", data: { seq, tag } })));
    const result = await waitFor(async () =>
      Object.values((await appState()).results).find((r) => r && r.tag === tag),
    15000);
    eq(result, { tag, count: 2 });
  },

  async idempotencyExpression() {
    const key = uid();
    const ids = await send([1, 2].map(() => ({ name: "e2e/idem", data: { key } })));
    await waitFor(async () => (await runsFor(ids[0])).length + (await runsFor(ids[1])).length > 0);
    await sleep(2000);
    eq((await runsFor(ids[0])).length + (await runsFor(ids[1])).length, 1, "runs for duplicate key");
  },

  async eventIdDedup() {
    const eid = uid();
    const a = await send({ id: eid, name: "e2e/hello", data: { name: "a" } });
    const b = await send({ id: eid, name: "e2e/hello", data: { name: "b" } });
    await waitRun(a[0]);
    await sleep(1500);
    eq((await runsFor(b[0])).length, 0, "dedup by event id");
  },

  async wildcard() {
    const [id] = await send({ name: "e2e/wild.thing", data: {} });
    const run = await waitRun(id);
    eq(run.output, "e2e/wild.thing");
  },

  async triggerExpression() {
    const [no] = await send({ name: "e2e/filtered", data: { ok: false } });
    const [yes] = await send({ name: "e2e/filtered", data: { ok: true } });
    const run = await waitRun(yes);
    eq(run.output, "matched");
    eq((await runsFor(no)).length, 0, "filtered-out runs");
  },

  async signals() {
    const signal = `sig-${uid()}`;
    const [id] = await send({ name: "e2e/signal", data: { signal } });
    await sleep(1500);
    await inngest.sendSignal({ signal, data: { v: 1 } });
    const run = await waitRun(id);
    eq(run.output, { got: { v: 1 } });
  },

  async stepSendEvent() {
    const tag = uid();
    const [id] = await send({ name: "e2e/emit", data: { tag } });
    const run = await waitRun(id);
    eq(run.output, "sent");
    await waitFor(async () => (await appState()).events[tag]);
  },

  async singletonSkip() {
    const k = uid();
    const [a] = await send({ name: "e2e/single", data: { k } });
    await sleep(500);
    const [b] = await send({ name: "e2e/single", data: { k } });
    await waitRun(a, { timeout: 15000 });
    eq((await runsFor(b)).length, 0, "singleton second run");
  },

  async finishTimeout() {
    const [id] = await send({ name: "e2e/timeboxed", data: {} });
    const run = await waitRun(id, { timeout: 20000 });
    eq(run.status, "Cancelled");
  },

  async largeState() {
    const [id] = await send({ name: "e2e/big", data: {} });
    const run = await waitRun(id, { timeout: 60000 });
    eq(run.status, "Completed");
    eq(run.output, { total: 6 * 900 * 1024 });
  },

  async repeatedStepIds() {
    const [id] = await send({ name: "e2e/loop", data: { n: 5 } });
    const run = await waitRun(id);
    eq(run.output, { sum: 10 });
  },

  async eventApiErrors() {
    const bad = await fetch(`${API}/e/not-a-key`, { method: "POST", body: JSON.stringify({ name: "x" }) });
    eq(bad.status, 401, "bad key");
    const reserved = await fetch(`${API}/e/${EVENT_KEY}`, {
      method: "POST",
      body: JSON.stringify([{ name: "ok/one" }, { name: "inngest/nope" }]),
    });
    eq(reserved.status, 400, "reserved name");
    const body = await reserved.json();
    eq(body.ids.length, 2);
    ok(body.ids[0] !== "" && body.ids[1] === "", "partial ids");
  },

  async metrics() {
    // Unauthenticated, Prometheus text format, as in the reference server.
    const scrape = async () => {
      const res = await fetch(`${API}/metrics`);
      eq(res.status, 200, "metrics status");
      ok((res.headers.get("content-type") || "").startsWith("text/plain"), "content type");
      return res.text();
    };
    const value = (text, series) => {
      const line = text.split("\n").find((l) => l.startsWith(series + " "));
      return line ? Number(line.slice(series.length + 1)) : null;
    };
    const first = await scrape();
    ok(first.includes("# TYPE inngest_queue_depth gauge"), "queue depth gauge");
    ok(value(first, "inngest_queue_depth") !== null, "queue depth value");

    // Lifecycle counters (run.sh enables INNGEST_EXPERIMENTAL_PROM_METRICS on both servers).
    const fn = 'fn="e2e-hello"';
    const before = value(first, `inngest_function_run_ended_total{${fn},status="completed"}`) || 0;
    const [id] = await send({ name: "e2e/hello", data: { name: "metrics" } });
    await waitRun(id);
    let text;
    await waitFor(async () => {
      text = await scrape();
      return value(text, `inngest_function_run_ended_total{${fn},status="completed"}`) > before;
    });
    for (const series of [
      `inngest_function_run_scheduled_total{${fn}}`,
      `inngest_function_run_started_total{${fn}}`,
      `inngest_sdk_req_started_total{${fn}}`,
      `inngest_sdk_req_ended_total{${fn},status="success"}`,
    ]) {
      ok(value(text, series) >= 1, `missing ${series}`);
    }
    if (STRICT_REST) {
      ok(/^oxidle_build_info\{version="[^"]+"\} 1$/m.test(text), "build info");
      ok(value(text, 'oxidle_events_received_total{source="api"}') >= 1, "events received");
      ok(text.includes('oxidle_http_request_duration_seconds_count{method="POST",route="/e/{key}",status="200"}'), "http route label");
    }
  },

  async restApi() {
    const fns = await api(`/v1/apps/e2e/functions`);
    eq(fns.status, 200);
    ok(Array.isArray(fns.body) && fns.body.length >= 20, `functions: ${fns.body.length}`);
    const events = await api(`/v1/events?limit=5`);
    eq(events.status, 200);
    if (STRICT_REST) ok(events.body.data.length > 0, "events listed");
    const unauth = await fetch(`${API}/v1/events`);
    eq(unauth.status, 401, "auth required");
  },

  async cron() {
    if (!process.env.CRON) return "skipped (set CRON=1)";
    await waitFor(async () => (await appState()).events.cron, 75000);
  },
};

async function main() {
  // Trigger an app sync (the SDK registers itself with the server).
  const put = await fetch(`${APP}/api/inngest`, { method: "PUT" });
  const putBody = await put.text();
  if (put.status !== 200) throw new Error(`sync failed: ${put.status} ${putBody}`);
  console.log("synced:", putBody);
  await sleep(500);

  const names = Object.keys(tests).filter((n) => !ONLY || ONLY.includes(n));
  const started = Date.now();
  const results = await Promise.all(
    names.map(async (name) => {
      const t0 = Date.now();
      try {
        const note = await tests[name]();
        return { name, ok: true, ms: Date.now() - t0, note };
      } catch (err) {
        return { name, ok: false, ms: Date.now() - t0, err: err.message };
      }
    }),
  );
  for (const r of results) {
    console.log(`${r.ok ? "PASS" : "FAIL"} ${r.name} (${r.ms}ms)${r.note ? " " + r.note : ""}${r.err ? "\n     " + r.err : ""}`);
  }
  const failed = results.filter((r) => !r.ok).length;
  console.log(`\n${results.length - failed}/${results.length} passed in ${Date.now() - started}ms`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
