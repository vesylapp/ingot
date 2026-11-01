// E2E test app: a real Inngest JS SDK app exercising the server's feature surface.
import express from "express";
import { Inngest, NonRetriableError } from "inngest";
import { serve } from "inngest/express";

export const inngest = new Inngest({ id: "e2e" });

// In-memory observations the test runner reads via GET /state.
const state = {
  attempts: {},
  active: {},
  maxActive: {},
  starts: {},
  failures: [],
  events: {},
  results: {},
  bench: { done: 0, first: 0, last: 0, lat: [] },
};
const bump = (obj, key) => (obj[key] = (obj[key] || 0) + 1);
// Each bench event carries its send time (data.t), so completions give end-to-end latency.
function benchDone(event) {
  const b = state.bench;
  b.done++;
  b.last = Date.now();
  if (event.data.t) b.lat.push(b.last - event.data.t);
}
const sleepMs = (ms) => new Promise((r) => setTimeout(r, ms));

async function tracked(group, fn) {
  bump(state.active, group);
  state.maxActive[group] = Math.max(state.maxActive[group] || 0, state.active[group]);
  try {
    return await fn();
  } finally {
    state.active[group]--;
  }
}

// Record each function's return value so tests can verify results independently of the REST API.
const create = (opts, handler) =>
  inngest.createFunction(opts, async (ctx) => {
    const v = await handler(ctx);
    state.results[ctx.runId] = v === undefined ? null : v;
    return v;
  });

const child = create({ id: "child", triggers: [{ event: "e2e/child" }] }, async ({ event, step }) => {
  const doubled = await step.run("double", () => event.data.x * 2);
  return { doubled };
});

const fns = [
  create({ id: "hello", triggers: [{ event: "e2e/hello" }] }, async ({ event }) => {
    return { message: `hello ${event.data.name}` };
  }),

  create({ id: "steps", triggers: [{ event: "e2e/steps" }] }, async ({ event, step }) => {
    const a = await step.run("a", () => event.data.n + 1);
    const b = await step.run("b", () => a * 2);
    const c = await step.run("c", () => b + 3);
    return { a, b, c };
  }),

  create({ id: "sleeper", triggers: [{ event: "e2e/sleep" }] }, async ({ step }) => {
    const before = await step.run("before", () => Date.now());
    await step.sleep("nap", "2s");
    const after = await step.run("after", () => Date.now());
    return { slept: after - before };
  }),

  create({ id: "waiter", triggers: [{ event: "e2e/wait" }] }, async ({ event, step }) => {
    const approval = await step.waitForEvent("approval", {
      event: "e2e/approve",
      match: "data.id",
      timeout: event.data.timeout || "30s",
    });
    return { approved: approval ? approval.data.by : null };
  }),

  create({ id: "wait-if", triggers: [{ event: "e2e/wait-if" }] }, async ({ step }) => {
    const ev = await step.waitForEvent("big", {
      event: "e2e/amount",
      if: "async.data.amount > event.data.min && async.data.ref == event.data.ref",
      timeout: "30s",
    });
    return { amount: ev ? ev.data.amount : null };
  }),

  create(
    { id: "flaky", retries: 3, triggers: [{ event: "e2e/flaky" }] },
    async ({ event, step, runId }) => {
      const v = await step.run("unstable", () => {
        const n = bump(state.attempts, runId);
        if (n < 3) throw new Error(`attempt ${n} failed`);
        return n;
      });
      return { attempts: v };
    },
  ),

  create(
    {
      id: "doomed",
      retries: 0,
      triggers: [{ event: "e2e/doomed" }],
      onFailure: async ({ event, error }) => {
        state.failures.push({ fn: event.data.function_id, message: error.message, run: event.data.run_id });
        return "handled";
      },
    },
    async () => {
      throw new NonRetriableError("this always fails");
    },
  ),

  create({ id: "parallel", triggers: [{ event: "e2e/parallel" }] }, async ({ step }) => {
    const results = await Promise.all([
      step.run("p1", () => 1),
      step.run("p2", () => 2),
      step.run("p3", () => 3),
    ]);
    const total = await step.run("sum", () => results.reduce((a, b) => a + b, 0));
    return { total };
  }),

  child,

  create({ id: "parent", triggers: [{ event: "e2e/parent" }] }, async ({ event, step }) => {
    const res = await step.invoke("call-child", {
      function: child,
      data: { x: event.data.x },
    });
    return { fromChild: res.doubled };
  }),

  create(
    {
      id: "cancellable",
      cancelOn: [{ event: "e2e/cancel", match: "data.id" }],
      triggers: [{ event: "e2e/cancellable" }],
    },
    async ({ step }) => {
      await step.sleep("long", "60s");
      return "should not finish";
    },
  ),

  create(
    { id: "limited", concurrency: { limit: 1 }, triggers: [{ event: "e2e/limited" }] },
    async ({ step }) => {
      await step.run("work", () => tracked("limited", () => sleepMs(700)));
      return "ok";
    },
  ),

  create(
    { id: "keyed", concurrency: [{ limit: 1, key: "event.data.user" }], triggers: [{ event: "e2e/keyed" }] },
    async ({ event, step }) => {
      await step.run("work", () => tracked(`keyed-${event.data.user}`, () => sleepMs(500)));
      return "ok";
    },
  ),

  create(
    { id: "throttled", throttle: { limit: 1, period: "2s" }, triggers: [{ event: "e2e/throttled" }] },
    async ({ runId }) => {
      state.starts[runId] = Date.now();
      return Date.now();
    },
  ),

  create(
    { id: "ratelimited", rateLimit: { limit: 1, period: "1m", key: "event.data.user" }, triggers: [{ event: "e2e/ratelimited" }] },
    async ({ event }) => event.data.user,
  ),

  create(
    { id: "debounced", debounce: { period: "2s", key: "event.data.k" }, triggers: [{ event: "e2e/debounced" }] },
    async ({ event }) => ({ last: event.data.seq }),
  ),

  create(
    { id: "batched", batchEvents: { maxSize: 3, timeout: "5s" }, triggers: [{ event: "e2e/batched" }] },
    async ({ events }) => ({ tag: events[0].data.tag, count: events.length, seqs: events.map((e) => e.data.seq) }),
  ),

  create(
    { id: "batch-timeout", batchEvents: { maxSize: 100, timeout: "2s" }, triggers: [{ event: "e2e/batch-timeout" }] },
    async ({ events }) => ({ tag: events[0].data.tag, count: events.length }),
  ),

  create(
    { id: "idem", idempotency: "event.data.key", triggers: [{ event: "e2e/idem" }] },
    async ({ event }) => event.data.key,
  ),

  create({ id: "wild", triggers: [{ event: "e2e/wild.*" }] }, async ({ event }) => event.name),

  create(
    { id: "filtered", triggers: [{ event: "e2e/filtered", if: "event.data.ok == true" }] },
    async () => "matched",
  ),

  create({ id: "signaller", triggers: [{ event: "e2e/signal" }] }, async ({ event, step }) => {
    const sig = await step.waitForSignal("sig", { signal: event.data.signal, timeout: "30s" });
    return { got: sig ? sig.data : null };
  }),

  create({ id: "emitter", triggers: [{ event: "e2e/emit" }] }, async ({ event, step }) => {
    await step.sendEvent("send", { name: "e2e/emitted", data: { tag: event.data.tag } });
    return "sent";
  }),

  create({ id: "receiver", triggers: [{ event: "e2e/emitted" }] }, async ({ event }) => {
    state.events[event.data.tag] = true;
    return event.data.tag;
  }),

  create(
    { id: "single", singleton: { key: "event.data.k", mode: "skip" }, triggers: [{ event: "e2e/single" }] },
    async ({ step }) => {
      await step.sleep("hold", "3s");
      return "done";
    },
  ),

  create(
    { id: "timeboxed", timeouts: { finish: "2s" }, triggers: [{ event: "e2e/timeboxed" }] },
    async ({ step }) => {
      await step.sleep("long", "10s");
      await step.run("after", () => "late");
      return "should be cancelled";
    },
  ),

  create({ id: "big-output", triggers: [{ event: "e2e/big" }] }, async ({ step }) => {
    const chunks = [];
    for (let i = 0; i < 6; i++) {
      chunks.push(await step.run(`chunk-${i}`, () => "x".repeat(900 * 1024)));
    }
    return { total: chunks.reduce((a, c) => a + c.length, 0) };
  }),

  create({ id: "loop-steps", triggers: [{ event: "e2e/loop" }] }, async ({ event, step }) => {
    let sum = 0;
    for (let i = 0; i < event.data.n; i++) {
      sum += await step.run("item", () => i);
    }
    return { sum };
  }),

  inngest.createFunction({ id: "bench-1", triggers: [{ event: "bench/one" }] }, async ({ event }) => {
    benchDone(event);
    return 1;
  }),

  inngest.createFunction({ id: "bench-3", triggers: [{ event: "bench/three" }] }, async ({ event, step }) => {
    const a = await step.run("a", () => 1);
    const b2 = await step.run("b", () => a + 1);
    const c = await step.run("c", () => b2 + 1);
    benchDone(event);
    return c;
  }),

  create({ id: "cron-fn", triggers: [{ cron: "* * * * *" }] }, async ({ event }) => {
    state.events.cron = event.data.scheduledAt || true;
    return "tick";
  }),
];

const app = express();
app.use(express.json({ limit: "50mb" }));
app.use("/api/inngest", serve({ client: inngest, functions: fns }));
app.get("/state", (_req, res) => res.json(state));
app.post("/bench/reset", (_req, res) => {
  state.bench = { done: 0, first: Date.now(), last: 0, lat: [] };
  res.json(state.bench);
});
app.get("/bench", (_req, res) => {
  const { lat, ...b } = state.bench;
  const sorted = [...lat].sort((x, y) => x - y);
  const pct = (q) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] : null);
  res.json({ ...b, p50: pct(0.5), p95: pct(0.95), p99: pct(0.99), max: sorted.at(-1) ?? null });
});
const port = Number(process.env.PORT || 3939);
app.listen(port, () => console.log(`e2e app listening on ${port}`));
