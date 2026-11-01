// Benchmarks against a running server. Prints one JSON line.
//   MODE=throughput: send N events in batches, measure time until all runs complete.
//   MODE=latency:    send RATE events/s one at a time for DURATION s, report end-to-end
//                    latency percentiles (event sent -> function's last step finished).
// KIND=one (no steps) | three (3 sequential steps).
import { Inngest } from "inngest";

const APP = process.env.APP_URL || "http://localhost:3939";
const MODE = process.env.MODE || "throughput";
const N = Number(process.env.N || 2000);
const KIND = process.env.KIND || "three";
const RATE = Number(process.env.RATE || 20);
const DURATION = Number(process.env.DURATION || 15);
const TIMEOUT_S = Number(process.env.TIMEOUT_S || 600);
const BATCH = 200;
const inngest = new Inngest({ id: "bench-runner" });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const put = await fetch(`${APP}/api/inngest`, { method: "PUT" });
if (put.status !== 200) throw new Error(`sync failed ${put.status}`);
await sleep(500);
await fetch(`${APP}/bench/reset`, { method: "POST" });

const t0 = Date.now();
let total = N;
if (MODE === "latency") {
  total = RATE * DURATION;
  const sends = [];
  for (let i = 0; i < total; i++) {
    const due = t0 + (i * 1000) / RATE;
    if (due > Date.now()) await sleep(due - Date.now());
    sends.push(inngest.send({ name: `bench/${KIND}`, data: { i, t: Date.now() } }));
  }
  await Promise.all(sends);
} else {
  const sends = [];
  for (let i = 0; i < N; i += BATCH) {
    const evts = Array.from({ length: Math.min(BATCH, N - i) }, (_, j) => ({
      name: `bench/${KIND}`,
      data: { i: i + j, t: Date.now() },
    }));
    sends.push(inngest.send(evts));
  }
  await Promise.all(sends);
}
const sent = Date.now() - t0;
let b;
const deadline = Date.now() + TIMEOUT_S * 1000;
while (Date.now() < deadline) {
  b = await (await fetch(`${APP}/bench`)).json();
  if (b.done >= total) break;
  await sleep(100);
}
const elapsed = (b.last || Date.now()) - t0;
console.log(
  JSON.stringify({
    mode: MODE,
    kind: KIND,
    n: total,
    done: b.done,
    send_ms: sent,
    total_ms: elapsed,
    runs_per_sec: Math.round((b.done / elapsed) * 1000),
    p50_ms: b.p50,
    p95_ms: b.p95,
    p99_ms: b.p99,
    max_ms: b.max,
  }),
);
