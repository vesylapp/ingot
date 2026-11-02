#!/usr/bin/env bash
# Run the end-to-end suite: ingot server on the host + the JS SDK test app in Docker.
#   ./e2e/run.sh                 # sqlite backend
#   BACKEND=postgres ./e2e/run.sh
#   BACKEND=redis ./e2e/run.sh
#   ONLY=hello,steps ./e2e/run.sh
#   TARGET=inngest ./e2e/run.sh  # run the same suite against the reference Go server
#   BENCH=1 N=2000 ./e2e/run.sh  # throughput benchmark (works with TARGET/BACKEND too)
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=${PORT:-8288}
APP_PORT=${APP_PORT:-3939}
# The server gets the bare hex key; the SDK gets the prefixed form (exercises prefix handling).
SERVER_KEY=${SERVER_KEY:-8b1d8a3c4f0e2d9b7a6c5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b}
SIGNING_KEY=${SIGNING_KEY:-signkey-test-$SERVER_KEY}
TARGET=${TARGET:-ingot}
EVENT_KEY=${EVENT_KEY:-e2e-event-key}
BACKEND=${BACKEND:-sqlite}
WORK=$(mktemp -d)
LOG=${LOG:-$WORK/ingot.log}

cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  docker rm -f ingot-e2e-app >/dev/null 2>&1 || true
  docker rm -f ingot-e2e-ref >/dev/null 2>&1 || true
  if [[ "$BACKEND" != "sqlite" ]]; then docker rm -f ingot-e2e-db >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

# A leftover server (e.g. from KEEP=1) would answer our health checks and be tested instead.
for p in "$PORT" "$APP_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
    echo "port $p is already in use; stop whatever is listening (lsof -iTCP:$p -sTCP:LISTEN) or set PORT/APP_PORT" >&2
    exit 1
  fi
done

APP_ORIGIN="http://localhost:$APP_PORT"
if [[ "$TARGET" == "inngest" ]]; then
  # Reference server in Docker; it reaches the app via the host.
  APP_ORIGIN="http://host.docker.internal:$APP_PORT"
  docker rm -f ingot-e2e-ref >/dev/null 2>&1 || true
  docker run -d --name ingot-e2e-ref -p "$PORT:8288" -e INNGEST_EXPERIMENTAL_PROM_METRICS=1 \
    inngest/inngest:${INNGEST_VERSION:-latest} \
    inngest start --event-key "$EVENT_KEY" --signing-key "$SERVER_KEY" --retry-interval 1 >/dev/null
  until curl -sf "http://127.0.0.1:$PORT/health" >/dev/null; do sleep 0.3; done
  echo "reference inngest up"
else
if [[ -z "${BIN:-}" ]]; then
  cargo build ${CARGO_FLAGS:-} -q
  BIN=target/${PROFILE:-debug}/ingot
fi

EXTRA=()
case "$BACKEND" in
  postgres)
    docker rm -f ingot-e2e-db >/dev/null 2>&1 || true
    docker run -d --name ingot-e2e-db -e POSTGRES_PASSWORD=pw -p 5499:5432 postgres:16-alpine >/dev/null
    until docker exec ingot-e2e-db pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
    sleep 1
    EXTRA+=(--postgres-uri "postgres://postgres:pw@127.0.0.1:5499/postgres")
    ;;
  redis)
    docker rm -f ingot-e2e-db >/dev/null 2>&1 || true
    docker run -d --name ingot-e2e-db -p 6398:6379 redis:7-alpine >/dev/null
    sleep 1
    EXTRA+=(--redis-uri "redis://127.0.0.1:6398")
    ;;
esac

RUST_LOG=${RUST_LOG:-ingot=debug} INNGEST_EXPERIMENTAL_PROM_METRICS=1 "$BIN" start \
  --port "$PORT" --signing-key "$SERVER_KEY" --event-key "$EVENT_KEY" \
  --sqlite-dir "$WORK/db" --retry-interval 1 ${EXTRA[@]+"${EXTRA[@]}"} >"$LOG" 2>&1 &
SERVER_PID=$!
until curl -sf "http://127.0.0.1:$PORT/health" >/dev/null; do
  kill -0 "$SERVER_PID" 2>/dev/null || { echo "server died:"; cat "$LOG"; exit 1; }
  sleep 0.2
done
echo "ingot up (backend=$BACKEND, log=$LOG)"
fi

docker rm -f ingot-e2e-app >/dev/null 2>&1 || true
docker run -d --name ingot-e2e-app -p "$APP_PORT:3939" \
  -v "$PWD/e2e/app:/app" -w /app \
  -e INNGEST_DEV=0 -e INNGEST_BASE_URL="http://host.docker.internal:$PORT" \
  -e INNGEST_SIGNING_KEY="$SIGNING_KEY" -e INNGEST_EVENT_KEY="$EVENT_KEY" \
  -e INNGEST_SERVE_ORIGIN="$APP_ORIGIN" \
  node:22-alpine sh -c "npm install --silent --no-audit --no-fund && node app.mjs" >/dev/null
until curl -sf "http://127.0.0.1:$APP_PORT/state" >/dev/null; do
  if ! docker ps -q -f name=ingot-e2e-app | grep -q .; then docker logs ingot-e2e-app; exit 1; fi
  sleep 0.5
done
echo "app up"

if [[ -n "${BENCH:-}" ]]; then
  for kind in one three; do
    docker exec -e KIND=$kind -e N="${N:-2000}" -e APP_URL="http://localhost:3939" \
      -e INNGEST_DEV=0 -e INNGEST_BASE_URL="http://host.docker.internal:$PORT" \
      -e INNGEST_SIGNING_KEY="$SIGNING_KEY" -e INNGEST_EVENT_KEY="$EVENT_KEY" \
      ingot-e2e-app node bench.mjs
  done
  exit 0
fi

set +e
docker exec -e TARGET="$TARGET" -e ONLY="${ONLY:-}" -e CRON="${CRON:-}" -e APP_URL="http://localhost:3939" \
  -e INNGEST_DEV=0 -e INNGEST_BASE_URL="http://host.docker.internal:$PORT" \
  -e INNGEST_SIGNING_KEY="$SIGNING_KEY" -e INNGEST_EVENT_KEY="$EVENT_KEY" \
  ingot-e2e-app node test.mjs
STATUS=$?
if [[ -n "${KEEP:-}" ]]; then
  echo "environment kept up: dashboard at http://localhost:$PORT (Ctrl-C to stop)"
  while true; do sleep 3600; done
fi
if [[ $STATUS -ne 0 ]]; then
  if [[ "$TARGET" == "inngest" ]]; then docker logs --tail "${LOG_LINES:-60}" ingot-e2e-ref 2>&1
  else echo "--- server log tail ---"; tail -n "${LOG_LINES:-60}" "$LOG"; fi
  echo "--- app log tail ---"; docker logs --tail 40 ingot-e2e-app 2>&1
fi
exit $STATUS
