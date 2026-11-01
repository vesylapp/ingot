// Throughput benchmark: send N events and measure time until all runs complete.
import { Inngest } from "inngest";

const APP = process.env.APP_URL || "http://localhost:3939";
const N = Number(process.env.N || 2000);
const KIND = process.env.KIND || "three"; // "one" (single step) or "three" (3 steps)
const BATCH = 200;
const inngest = new Inngest({ id: "bench-runner" });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const put = await fetch(`${APP}/api/inngest`, { method: "PUT" });
if (put.status !== 200) throw new Error(`sync failed ${put.status}`);
await sleep(500);
await fetch(`${APP}/bench/reset`, { method: "POST" });

const t0 = Date.now();
const sends = [];
for (let i = 0; i < N; i += BATCH) {
  const evts = Array.from({ length: Math.min(BATCH, N - i) }, (_, j) => ({ name: `bench/${KIND}`, data: { i: i + j } }));
  sends.push(inngest.send(evts));
}
await Promise.all(sends);
const sent = Date.now() - t0;
let b;
const deadline = Date.now() + 600_000;
while (Date.now() < deadline) {
  b = await (await fetch(`${APP}/bench`)).json();
  if (b.done >= N) break;
  await sleep(100);
}
const total = (b.last || Date.now()) - t0;
console.log(JSON.stringify({ kind: KIND, n: N, done: b.done, send_ms: sent, total_ms: total, runs_per_sec: Math.round((b.done / total) * 1000) }));
