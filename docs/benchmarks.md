# Benchmarks

Ingot compared with the official self-hosted Inngest server (`inngest start`) on each
storage backend, using the same SDK app, the same workload and the same Docker network.

![Throughput: completed runs per second, by backend](benchmarks/throughput.svg)

![Latency at light load: p50 and p95, by backend](benchmarks/latency.svg)

## Results

<!-- results:start -->
Medians of 3 repetitions. Throughput runs send 2,000 events in batches of 200.

### Throughput (runs/s, higher is better)

| Backend | Function | Ingot 0.1.3 | Inngest 1.49.0 | Ingot advantage |
|---|---|--:|--:|--:|
| SQLite | no steps | 748 | 8 | **94×** |
| SQLite | 3 steps | 584 | 7 | **83×** |
| Postgres | no steps | 1,291 | 321 | **4.0×** |
| Postgres | 3 steps | 758 | 216 | **3.5×** |
| Redis | no steps | 2,353 | 9 | **261×** |
| Redis | 3 steps | 1,027 | 7 | **147×** |
| Redis + Postgres | no steps | 1,925 | 360 | **5.3×** |
| Redis + Postgres | 3 steps | 884 | 240 | **3.7×** |

### Latency at light load (ms, lower is better)

Event sent → last step of a 3-step function finished, 100 events sent one at a time at a steady rate (so p99 is the slowest run).

| Backend | Server | p50 | p95 | p99 |
|---|---|--:|--:|--:|
| SQLite | Ingot 0.1.3 | 17 | 32 | 44 |
| SQLite | Inngest 1.49.0 | 320 | 550 | 583 |
| Postgres | Ingot 0.1.3 | 38 | 51 | 72 |
| Postgres | Inngest 1.49.0 | 243 | 386 | 429 |
| Redis | Ingot 0.1.3 | 15 | 21 | 38 |
| Redis | Inngest 1.49.0 | 324 | 612 | 948 |
| Redis + Postgres | Ingot 0.1.3 | 21 | 28 | 37 |
| Redis + Postgres | Inngest 1.49.0 | 240 | 389 | 435 |

### Resource use

CPU is the total CPU time spent by the server container plus the database container (Postgres/Redis), divided by runs completed, during the 3-step throughput run. Memory is the server container's peak anonymous (heap) memory; it excludes the database container.

| Backend | Server | CPU per run (ms) | Peak server memory (MB) |
|---|---|--:|--:|
| SQLite | Ingot 0.1.3 | 1.66 | 49 |
| SQLite | Inngest 1.49.0 | 44.45 | 429 |
| Postgres | Ingot 0.1.3 | 4.91 | 52 |
| Postgres | Inngest 1.49.0 | 29.92 | 253 |
| Redis | Ingot 0.1.3 | 0.81 | 46 |
| Redis | Inngest 1.49.0 | 29.79 | 354 |
| Redis + Postgres | Ingot 0.1.3 | 1.46 | 43 |
| Redis + Postgres | Inngest 1.49.0 | 27.60 | 189 |
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
  step finished. Latency is measured in its own session on a freshly started server, so it is not
  affected by backlog left over from the throughput bursts.
- **Resources**: CPU time read from each container's cgroup (`cpu.stat`) before and after a run,
  and the server's peak anonymous memory (`memory.stat`) sampled every second across both sessions.

A 200-event warm-up runs first and is not recorded. Each measurement is repeated three times and
the median is reported.

## What the backends mean

The servers split storage differently, so the same flag means slightly different things:

| `BACKEND` | Ingot | Inngest |
|---|---|---|
| `sqlite` | everything in SQLite (`--sqlite-dir`) | history in SQLite; queue and run state in its built-in in-memory Redis |
| `postgres` | everything in Postgres (`--postgres-uri`) | history in Postgres; queue and run state in its built-in in-memory Redis |
| `redis` | queue and run state in Redis (`--redis-uri`); history in SQLite | queue and run state in Redis (`--redis-uri`); history in SQLite |
| `redis-postgres` | queue and run state in Redis; history in Postgres | queue and run state in Redis; history in Postgres (Inngest's recommended production setup) |

Both servers otherwise run with default settings. Postgres 16 and Redis 7 run in their own
containers and are counted in the CPU figures. SQLite files live on the container's own
filesystem, where a synced 4 KB write takes about 0.6 ms on this machine; that matters for
Inngest (see below).

## Environment

- Apple M4 (10 cores), 24 GB RAM, macOS 27.0.1
- OrbStack (Docker 29.8.2, Linux 7.2.9 arm64 VM)
- `inngest/inngest:v1.49.0` (arm64 image) and Ingot 0.1.3 built from `./Dockerfile` (release profile)
- Postgres `postgres:16-alpine`, Redis `redis:7-alpine`, app on `node:22-alpine`
- Nothing else running on the machine during the runs.
- No CPU or memory limits on containers. The server, database, SDK app and load generator all share
  the same machine, so absolute numbers include the cost of the app itself; the comparison is
  between servers under identical conditions.

## About the Inngest numbers

**SQLite is Inngest's bottleneck.** On the SQLite and Redis rows Inngest manages under 10 runs/s;
with Postgres anywhere in the picture it reaches 200–360.

- *Redis is really in use on the Redis row.* Inngest logs `using external redis`, and the external
  Redis held about 5,000 keys during the run. But `--redis-uri` only moves the queue and run
  state; the database for history and traces is chosen separately and is SQLite unless
  `--postgres-uri` is set. So the Redis row still writes every trace span and history row to
  SQLite, and is limited by the same thing as the SQLite row.
- *The cost is fsync.* Inngest opens SQLite with only `cache=shared`: rollback journal (no WAL),
  full sync on every commit, no busy timeout, and the pure-Go SQLite driver. Moving the same
  SQLite file to RAM (`--tmpfs`) takes it from about 10 to about 300 runs/s:

  | Inngest 1.49.0, 1,000-event burst | no steps | 3 steps |
  |---|--:|--:|
  | SQLite, on disk (default) | 10 | 8 |
  | SQLite in RAM (`--tmpfs`) | 306 | 270 |
  | Redis + SQLite in RAM | 408 | 308 |
  | Redis + Postgres | 339 | 226 |

  Under load its logs fill with `error inserting span` / `error inserting trace run` errors
  (`context canceled`, tens of thousands per session) and `critical section took longer than
  boundary` warnings, where scheduling a single event waited seconds on SQLite. On a disk with
  faster fsync than this VM's the SQLite rows would improve, but it remains the bottleneck.

Ingot opens SQLite in WAL mode with `synchronous=NORMAL` and writes run timelines through a
batched background writer, so fsync matters far less to it: the same RAM-disk test moves it from
748 to 1,024 runs/s (no steps), where Inngest gains about 30×. Its SQLite rows trail Postgres for a different reason: SQLite allows
one writer at a time, and Ingot keeps queue, run state and history in that one file, so every
write takes the same lock (`BEGIN IMMEDIATE`). Postgres lets workers write different rows
concurrently, and Redis keeps the hot queue/state path out of SQL entirely.

Before finding the fsync cause, these settings were tried on the SQLite configuration (300–500
event bursts, no-steps function); none help, because none touch the SQLite writes:

| Inngest configuration (SQLite) | runs/s |
|---|--:|
| defaults (v1.49.0) | 11 |
| `--tick 10` (poll the queue every 10 ms instead of 150 ms) | 7–10 |
| `--queue-workers 1000` (default 100) | 7–20 |
| `EXPERIMENTAL_KEY_QUEUES_ENABLE=true` | 11 |
| v1.30.0 | 13 |
| v1.11.13 | 10 |

**Postgres needs a bigger idle pool.** With the default `--postgres-max-idle-conns 10` (of 100
open), Inngest keeps opening and closing Postgres connections under load until the container runs
out of local ports (`connect: cannot assign requested address`); that caused one slow repetition
in each Postgres configuration above (29 and 28 runs/s, 3 steps). The medians are unaffected. Raising the idle pool fixes it. The same flag, given to both
servers (2 repetitions each):

| `--postgres-max-idle-conns 100` | no steps | 3 steps |
|---|--:|--:|
| Ingot 0.1.3 | 1,109–1,284 | 592–728 |
| Inngest 1.49.0 | 507–531 | 394–398 |

So on a tuned Postgres setup Ingot is roughly 2× faster; on the defaults, and on any setup that
keeps SQLite, the gap is much larger. Memory is a consistent difference either way: Inngest's
server peaks at 190–430 MB depending on the backend; Ingot stays at or under 52 MB in every
configuration.

These are numbers for the single-node self-hosted server. Inngest Cloud is a different,
horizontally scaled deployment and is not measured here.

## Reproduce

```sh
./bench/matrix.sh                             # full matrix, about an hour (Inngest dominates)
TARGETS=ingot BACKENDS=redis REPS=1 ./bench/matrix.sh   # a single cell
python3 bench/report.py bench/results/<file>.jsonl     # regenerate charts + the tables above
```

Knobs: `N` (events per throughput run), `REPS`, `RATE` / `DURATION` (latency run),
`INNGEST_IMAGE`, `SERVER_ARGS` (extra flags for both servers), `SERVER_DOCKER_ARGS` (extra
`docker run` flags for the server), `KEEP_UP=1` (leave the last session running), `SPECS`
(sessions separated by `|`, each on a fresh server; default
`"throughput:one throughput:three|latency:three"`). The raw results behind this page are in
`bench/results/matrix.jsonl`; each run also saves the server logs next to its results file
(`<name>-<target>-<backend>-<session>.log`, not committed).

For a quicker check against a server running on the host, `BENCH=1 ./e2e/run.sh` runs the same
throughput workload through the e2e harness.

## Why Ingot is faster

The biggest driver is how much database work each run does: **Inngest makes about 46 database
commits per 3-step run; Ingot makes about 4.**

Measured on the Redis + Postgres setup (the most like-for-like comparison): 1,000 three-step runs
plus the 200 no-step warm-up runs, counted from Postgres's own statistics and exact row counts
afterwards.

| | Postgres commits | Rows written | Redis commands |
|---|--:|--:|--:|
| Ingot 0.1.3 | 5,047 | ~7,800 (4,200 timeline spans, events, runs) | 78K |
| Inngest 1.49.0 | 54,865 | ~29,600 (13,800 spans, 6,000 history, 3,800 traces, events, runs, …) | 171K |

Inngest writes each trace span and history entry as it happens, mostly in its own transaction.
Ingot stores less telemetry (4 timeline rows per 3-step run: one per step plus finalization,
which is what the dashboard needs) and hands those rows to a background writer that batches them
into a few commits, off the execution path. That one difference explains most of the gap:

- **On SQLite and Redis-only setups**, every commit is synced to disk, at about 0.6 ms per sync
  on this machine. 46 commits at several syncs each comes to roughly 80–100 ms per run, which
  predicts the ~10 runs/s measured. Moving Inngest's SQLite file to RAM takes it to ~300 runs/s.
- **On Postgres setups**, the commits show up as database CPU instead.

### Where the CPU goes

CPU per completed 3-step run, by container, on Redis + Postgres. The median repetition for each
server; Inngest's slow repetition hit the connection churn described above.

| CPU per run (ms) | Server | Postgres | Redis | SDK app |
|---|--:|--:|--:|--:|
| Ingot 0.1.3 (892 runs/s) | 0.55 | 0.71 | 0.14 | 1.56 |
| Inngest 1.49.0 (240 runs/s) | 8.88 | 17.77 | 1.02 | 3.48 |

- **Postgres** is Inngest's largest cost, about two-thirds of its server-side CPU, against 0.7 ms
  for Ingot.
- **The server process itself** is the second driver: 8.9 ms against 0.55 ms (about 16×), covering
  Inngest's tracing pipeline, capacity checks and roughly twice as many Redis calls. Together
  with the database work, Inngest's server side uses about 6.6 of this machine's 10 cores at
  ~240 runs/s.
- **Ingot is not the bottleneck in its own numbers.** Its server and databases use about 1.4 ms
  per run, roughly 1.25 of 10 cores at ~900 runs/s, while the Node SDK app uses more CPU per run
  than Ingot's server does. The Ingot figures on this page are a floor, set largely by the test
  app, not Ingot's ceiling.

### Latency

Latency is driven differently. Inngest checks its queue on a timer (`--tick`, 150 ms by
default), while Ingot wakes a worker as soon as work is queued. Polling explains part of
Inngest's ~240 ms idle latency: with `--tick 10` its median dropped from 238 to 168 ms
(p95 391 → 286 ms). The rest is per-step work, including the synchronous writes above.

### Trade-offs

- **Part of the gap is Ingot doing less.** It doesn't keep Inngest's per-event `history` table
  or its detailed OpenTelemetry spans, so tools that read those wouldn't find them.
- **Ingot's SQLite setting trades some durability for speed.** With WAL and
  `synchronous=NORMAL`, a power loss (not a process crash) can lose the last few commits, though
  the file won't be corrupted. Inngest syncs SQLite fully on every commit.

### Reproducing the breakdown

Every result line from `bench/matrix.sh` includes `cpu_ms_by_container`. To inspect the databases
after a run, keep the last session's containers up:

```sh
KEEP_UP=1 TARGETS=inngest BACKENDS=redis-postgres SPECS="throughput:three" N=1000 REPS=1 ./bench/matrix.sh
docker exec bench-pg psql -U postgres -c "select xact_commit from pg_stat_database where datname='postgres'"
docker exec bench-db redis-cli info stats | grep total_commands_processed
docker rm -f bench-app bench-server bench-db bench-pg && docker network rm oxbench
```

Postgres's statistics counters lag a little behind the transactions, so wait a few seconds after
the run before reading them. For the latency check, add `SERVER_ARGS="--tick 10"`; for the RAM-disk
SQLite test, `SERVER_DOCKER_ARGS="--tmpfs /tmp/db"`.
