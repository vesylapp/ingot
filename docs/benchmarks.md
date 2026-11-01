# Benchmarks

oxidle compared with the official self-hosted Inngest server (`inngest start`) on each
storage backend, using the same SDK app, the same workload and the same Docker network.

![Throughput: completed runs per second, by backend](benchmarks/throughput.svg)

![Latency at light load: p50 and p95, by backend](benchmarks/latency.svg)

## Results

<!-- results:start -->
<!-- results:end -->

## What is measured

Every run uses the e2e test app (`e2e/app`, Inngest JS SDK v4 on Express, Node 22) and two
functions:

- **no steps**: `bench-1` returns immediately (one SDK request per run).
- **3 steps**: `bench-3` runs three sequential `step.run` calls and returns (four SDK requests per
  run, since the SDK reports each step back to the server).

Three measurements per configuration:

- **Throughput**: 2,000 events sent in batches of 200 as fast as possible. The measurement is the
  time from the first send to the last run completing, as seen by the app.
- **Latency**: 100 events sent one at a time at 5 events/s, a rate both servers keep up with.
  Each event carries its send time, and the app records how long after that the function's last
  step finished.
- **Resources**: CPU time read from each container's cgroup (`cpu.stat`) before and after a run,
  and the server's peak anonymous memory (`memory.stat`) sampled every second.

A 200-event warm-up runs first and is not recorded. Each measurement is repeated three times and
the median is reported.

## What the backends mean

The servers split storage differently, so the same flag means slightly different things:

| `BACKEND` | oxidle | Inngest |
|---|---|---|
| `sqlite` | everything in SQLite (`--sqlite-dir`) | history in SQLite; queue and run state in its built-in in-memory Redis |
| `postgres` | everything in Postgres (`--postgres-uri`) | history in Postgres; queue and run state in its built-in in-memory Redis |
| `redis` | queue and run state in Redis (`--redis-uri`); history in SQLite | queue and run state in Redis (`--redis-uri`); history in SQLite |

Both servers otherwise run with default settings. Postgres 16 and Redis 7 run in their own
containers and are counted in the CPU figures.

## Environment

- Apple M4 (10 cores), 24 GB RAM, macOS 27.0.1
- OrbStack (Docker 29.8.2, Linux 7.2.9 arm64 VM)
- `inngest/inngest:v1.49.0` (arm64 image) and oxidle 0.1.0 built from `./Dockerfile` (release profile)
- Postgres `postgres:16-alpine`, Redis `redis:7-alpine`, app on `node:22-alpine`
- No CPU or memory limits on containers. The server, database, SDK app and load generator all share
  the same machine, so absolute numbers include the cost of the app itself; the comparison is
  between servers under identical conditions.

## About the Inngest numbers

Inngest's throughput here is far below oxidle's, so the configuration was checked before
publishing. Each change below was measured with a 300–500 event burst against the no-steps
function:

| Inngest configuration | runs/s |
|---|--:|
| defaults (v1.49.0) | 11 |
| `--tick 10` (poll the queue every 10 ms instead of 150 ms) | 7–10 |
| `--queue-workers 1000` (default 100) | 7–20 |
| `EXPERIMENTAL_KEY_QUEUES_ENABLE=true` | 11 |
| external Redis (`--redis-uri`) | 12 |
| v1.30.0 | 13 |
| v1.11.13 | 10 |

None of these move it much, and CPU stays low (about 20 ms per run, well under one core in total),
so the limit is in how the queue schedules runs rather than in CPU or storage. Idle latency tells
the same story: a single run takes around 200 ms from event to completion on Inngest, against
around 20 ms on oxidle. These are numbers for the single-node self-hosted server; Inngest Cloud
runs a different, horizontally scaled deployment and is not measured here.

The server logs from each configuration are saved next to the raw results
(`bench/results/matrix-<target>-<backend>.log`); the Inngest logs show no errors during the runs.

## Reproduce

```sh
./bench/matrix.sh                             # full matrix, ~70 min (Inngest dominates)
TARGETS=oxidle BACKENDS=redis REPS=1 ./bench/matrix.sh   # a single cell
python3 bench/report.py bench/results/<file>.jsonl     # regenerate charts + the tables above
```

Knobs: `N` (events per throughput run), `REPS`, `RATE` / `DURATION` (latency run),
`INNGEST_IMAGE`, `SERVER_ARGS` (extra flags for both servers), `SPECS`
(e.g. `"throughput:one latency:three"`). The raw results behind this page are in
`bench/results/matrix.jsonl`.

For a quicker check against a server running on the host, `BENCH=1 ./e2e/run.sh` runs the same
throughput workload through the e2e harness.
