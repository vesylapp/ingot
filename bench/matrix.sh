#!/usr/bin/env bash
# Benchmark matrix: {oxidle, inngest} x {sqlite, postgres, redis}, everything in Docker on one
# network so both servers see identical networking. Appends one JSON line per measurement to $OUT.
#
#   ./bench/matrix.sh
#   TARGETS=oxidle BACKENDS=redis REPS=1 ./bench/matrix.sh
#
# Knobs: N (events per throughput run), REPS, RATE/DURATION (latency runs), INNGEST_IMAGE,
# SPECS (sessions separated by "|", e.g. "throughput:one|latency:three"),
# OXIDLE_IMAGE (built from ./Dockerfile unless SKIP_BUILD=1), SERVER_ARGS (extra flags for both servers),
# SERVER_DOCKER_ARGS (extra `docker run` flags for the server, e.g. "--tmpfs /tmp/db"), OUT.
# BACKENDS may also include redis-postgres (queue/state in Redis, history in Postgres).
set -euo pipefail
cd "$(dirname "$0")/.."

TARGETS=${TARGETS:-"oxidle inngest"}
BACKENDS=${BACKENDS:-"sqlite postgres redis"}
N=${N:-2000}
REPS=${REPS:-3}
RATE=${RATE:-5}
DURATION=${DURATION:-20}
INNGEST_IMAGE=${INNGEST_IMAGE:-inngest/inngest:v1.49.0}
OXIDLE_IMAGE=${OXIDLE_IMAGE:-oxidle:bench}
OUT=${OUT:-bench/results/$(date +%Y%m%d-%H%M%S).jsonl}
NET=oxbench
KEY=8b1d8a3c4f0e2d9b7a6c5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b
EVENT_KEY=bench-event-key
mkdir -p "$(dirname "$OUT")"

[[ -n "${SKIP_BUILD:-}" ]] || docker build -q -t "$OXIDLE_IMAGE" . >/dev/null
docker pull -q "$INNGEST_IMAGE" >/dev/null

teardown() {
  [[ -n "${SAMPLER:-}" ]] && kill "$SAMPLER" 2>/dev/null || true
  [[ -n "${CELL:-}" ]] && docker logs bench-server >"${OUT%.jsonl}-$CELL.log" 2>&1 || true
  CELL=
  docker rm -f bench-app bench-server bench-db bench-pg >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap teardown EXIT

# CPU time (µs) used so far by the containers that do server work.
cpu_usec() {
  local total=0 c v
  for c in "$@"; do
    v=$(docker exec "$c" sh -c 'grep ^usage_usec /sys/fs/cgroup/cpu.stat' | awk '{print $2}')
    total=$((total + v))
  done
  echo "$total"
}

# Background sampler: peak anonymous memory (bytes) of the server container, written to $1.
sample_mem() {
  local file=$1 peak=0 v
  while true; do
    v=$(docker exec bench-server sh -c 'grep "^anon " /sys/fs/cgroup/memory.stat' 2>/dev/null | awk '{print $2}')
    if [[ -n "$v" && "$v" -gt "$peak" ]]; then peak=$v; echo "$peak" >"$file"; fi
    sleep 1
  done
}

bench() { # mode kind
  docker exec -e MODE="$1" -e KIND="$2" -e N="$N" -e TIMEOUT_S="${TIMEOUT_S:-900}" -e RATE="$RATE" -e DURATION="$DURATION" \
    -e APP_URL=http://localhost:3939 -e INNGEST_DEV=0 -e INNGEST_BASE_URL=http://bench-server:8288 \
    -e INNGEST_SIGNING_KEY="signkey-test-$KEY" -e INNGEST_EVENT_KEY="$EVENT_KEY" \
    bench-app node bench.mjs
}

# Each session gets a freshly started server, so latency isn't skewed by backlog from the bursts.
IFS='|' read -r -a SESSIONS <<<"${SPECS:-throughput:one throughput:three|latency:three}"

for target in $TARGETS; do
  for backend in $BACKENDS; do
   for specs in "${SESSIONS[@]}"; do
    teardown
    docker network create "$NET" >/dev/null
    args=(start --port 8288 --signing-key "$KEY" --event-key "$EVENT_KEY" --sqlite-dir /tmp/db ${SERVER_ARGS:-})
    workers=(bench-server)
    case "$backend" in
      postgres)
        docker run -d --name bench-db --network "$NET" -e POSTGRES_PASSWORD=pw postgres:16-alpine >/dev/null
        until docker exec bench-db pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
        sleep 1
        args+=(--postgres-uri postgres://postgres:pw@bench-db:5432/postgres)
        workers+=(bench-db) ;;
      redis)
        docker run -d --name bench-db --network "$NET" redis:7-alpine >/dev/null
        sleep 1
        args+=(--redis-uri redis://bench-db:6379)
        workers+=(bench-db) ;;
      redis-postgres)
        docker run -d --name bench-db --network "$NET" redis:7-alpine >/dev/null
        docker run -d --name bench-pg --network "$NET" -e POSTGRES_PASSWORD=pw postgres:16-alpine >/dev/null
        until docker exec bench-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
        sleep 1
        args+=(--redis-uri redis://bench-db:6379 --postgres-uri postgres://postgres:pw@bench-pg:5432/postgres)
        workers+=(bench-db bench-pg) ;;
    esac
    if [[ "$target" == inngest ]]; then
      docker run -d --name bench-server --network "$NET" ${SERVER_ENV:-} ${SERVER_DOCKER_ARGS:-} "$INNGEST_IMAGE" inngest "${args[@]}" >/dev/null
      version=$(docker exec bench-server inngest version 2>/dev/null | head -1)
    else
      docker run -d --name bench-server --network "$NET" -e RUST_LOG=oxidle=warn ${SERVER_DOCKER_ARGS:-} "$OXIDLE_IMAGE" "${args[@]}" >/dev/null
      version=$(docker exec bench-server oxidle --version | awk '{print $2}')
    fi
    docker run -d --name bench-app --network "$NET" -v "$PWD/e2e/app:/app" -w /app \
      -e INNGEST_DEV=0 -e INNGEST_BASE_URL=http://bench-server:8288 \
      -e INNGEST_SIGNING_KEY="signkey-test-$KEY" -e INNGEST_EVENT_KEY="$EVENT_KEY" \
      -e INNGEST_SERVE_ORIGIN=http://bench-app:3939 \
      node:22-alpine sh -c "npm install --silent --no-audit --no-fund && node app.mjs" >/dev/null
    until docker exec bench-app wget -qO- http://bench-server:8288/health >/dev/null 2>&1; do sleep 0.5; done
    until docker exec bench-app wget -qO- http://localhost:3939/state >/dev/null 2>&1; do sleep 0.5; done
    echo "== $target ($version) backend=$backend: $specs"
    CELL=$target-$backend-${specs%%:*}

    # Warm-up (not recorded): sync, JIT, connection pools.
    N=200 bench throughput one >/dev/null

    memfile=$(mktemp)
    echo 0 >"$memfile"
    sample_mem "$memfile" &
    SAMPLER=$!
    for rep in $(seq 1 "$REPS"); do
      for spec in $specs; do
        spec=${spec/:/ }
        set -- $spec
        c0=$(cpu_usec "${workers[@]}")
        line=$(bench "$1" "$2")
        c1=$(cpu_usec "${workers[@]}")
        echo "$line" | jq -c --arg t "$target" --arg b "$backend" --arg v "$version" \
          --argjson rep "$rep" --argjson cpu "$((c1 - c0))" \
          '. + {target: $t, backend: $b, version: $v, rep: $rep, cpu_ms_per_run: (if .done > 0 then ($cpu / 1000) / .done * 1000 | round / 1000 else null end)}' \
          | tee -a "$OUT"
      done
    done
    kill "$SAMPLER" 2>/dev/null || true
    wait "$SAMPLER" 2>/dev/null || true
    SAMPLER=

    jq -nc --arg t "$target" --arg b "$backend" --argjson m "$(cat "$memfile")" \
      '{mode: "memory", target: $t, backend: $b, peak_anon_mb: ($m / 1048576 | round)}' | tee -a "$OUT"
    rm -f "$memfile"
   done
  done
done
echo "results: $OUT"
