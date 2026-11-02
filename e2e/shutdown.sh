#!/usr/bin/env bash
# Graceful shutdown checks: SIGTERM the server mid-work, restart it on the same database, and
# verify nothing was lost and in-flight work was finished or handed back.
#   ./e2e/shutdown.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=8288
APP_PORT=3939
KEY=8b1d8a3c4f0e2d9b7a6c5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b
EVENT_KEY=e2e-event-key
BEARER=$(python3 -c "import hashlib; print(hashlib.sha256(bytes.fromhex('$KEY')).hexdigest())")
WORK=$(mktemp -d)
FAILED=0

cleanup() {
  [[ -n "${PID:-}" ]] && kill "$PID" 2>/dev/null || true
  docker rm -f ingot-e2e-app >/dev/null 2>&1 || true
}
trap cleanup EXIT

for p in "$PORT" "$APP_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
    echo "port $p is already in use" >&2
    exit 1
  fi
done

cargo build -q
BIN=target/debug/ingot

# start <db dir> <log> <shutdown timeout>
start() {
  INGOT_SHUTDOWN_TIMEOUT=$3 RUST_LOG=ingot=info "$BIN" start --port $PORT --signing-key $KEY \
    --event-key $EVENT_KEY --sqlite-dir "$1" --retry-interval 1 >"$2" 2>&1 &
  PID=$!
  for _ in $(seq 1 100); do curl -sf "localhost:$PORT/health" >/dev/null && return; sleep 0.2; done
  echo "server failed to start:"; cat "$2"; exit 1
}

stop() { kill -TERM "$PID"; wait "$PID" || true; PID=; }

app_state() { curl -s "localhost:$APP_PORT/$1"; }
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
send() { curl -sf -X POST "localhost:$PORT/e/$EVENT_KEY" --data-binary "$1" | json 'd["ids"][0]'; }
run_status() {
  curl -s -H "Authorization: Bearer $BEARER" "localhost:$PORT/v1/events/$1/runs" \
    | json 'd["data"][0]["status"] if d.get("data") else ""'
}
check() {
  if [[ "$2" == "$3" ]]; then echo "  ok   $1"; else echo "  FAIL $1: expected $3, got $2"; FAILED=1; fi
}

start "$WORK/db" "$WORK/boot.log" 25
docker rm -f ingot-e2e-app >/dev/null 2>&1 || true
docker run -d --name ingot-e2e-app -p "$APP_PORT:3939" -v "$PWD/e2e/app:/app" -w /app \
  -e INNGEST_DEV=0 -e INNGEST_BASE_URL="http://host.docker.internal:$PORT" \
  -e INNGEST_SIGNING_KEY="signkey-test-$KEY" -e INNGEST_EVENT_KEY="$EVENT_KEY" \
  -e INNGEST_SERVE_ORIGIN="http://localhost:$APP_PORT" \
  node:22-alpine sh -c "npm install --silent --no-audit --no-fund && node app.mjs" >/dev/null
until app_state state >/dev/null 2>&1; do sleep 0.5; done
curl -sf -X PUT "localhost:$APP_PORT/api/inngest" >/dev/null
stop

echo "accepted events are routed before exit"
N=2000
start "$WORK/db" "$WORK/burst.log" 25
curl -sf -X POST "localhost:$APP_PORT/bench/reset" >/dev/null
python3 -c "import json; print(json.dumps([{'name':'bench/three','data':{'i':i}} for i in range($N)]))" >"$WORK/burst.json"
curl -sf -X POST "localhost:$PORT/e/$EVENT_KEY" --data-binary @"$WORK/burst.json" -o /dev/null
stop
start "$WORK/db" "$WORK/burst-2.log" 25
for _ in $(seq 1 60); do
  done_n=$(app_state bench | json 'd["done"]')
  [[ "$done_n" -ge $N ]] && break
  sleep 1
done
check "runs completed after restart" "$done_n" "$N"
stop

echo "a step that finishes within the budget completes on this node"
start "$WORK/db" "$WORK/short.log" 10
before=$(app_state state | json 'd["attempts"].get("slow", 0)')
id=$(send '{"name":"e2e/slow","data":{"ms":1500}}')
until [[ $(app_state state | json 'd["attempts"].get("slow", 0)') -gt $before ]]; do sleep 0.1; done
kill -TERM "$PID"
sleep 0.5
check "/ready while draining" "$(curl -s -o /dev/null -w '%{http_code}' "localhost:$PORT/ready")" 503
check "/health while draining" "$(curl -s -o /dev/null -w '%{http_code}' "localhost:$PORT/health")" 200
wait "$PID" || true; PID=
start "$WORK/db" "$WORK/short-2.log" 10
sleep 2
check "run status" "$(run_status "$id")" Completed
check "step attempts" "$(app_state state | json 'd["attempts"]["slow"]')" "$((before + 1))"
check "nothing handed back" "$(grep -c 'released in-flight' "$WORK/short.log" || true)" 0
stop

echo "a step still running at the deadline is handed back"
start "$WORK/db" "$WORK/long.log" 3
before=$(app_state state | json 'd["attempts"].get("slow", 0)')
id=$(send '{"name":"e2e/slow","data":{"ms":10000}}')
until [[ $(app_state state | json 'd["attempts"].get("slow", 0)') -gt $before ]]; do sleep 0.1; done
stop
check "released" "$(grep -c 'released in-flight queue items released=1' "$WORK/long.log")" 1
start "$WORK/db" "$WORK/long-2.log" 3
t0=$(date +%s)
until [[ "$(run_status "$id")" == Completed || $(($(date +%s) - t0)) -gt 40 ]]; do sleep 1; done
elapsed=$(($(date +%s) - t0))
# Handed back: it reruns at once (~10s). Waiting out the 30s lease would take ~40s.
check "completed without waiting for the lease" "$([[ $elapsed -lt 25 ]] && echo yes || echo "no (${elapsed}s)")" yes
stop

if [[ $FAILED -ne 0 ]]; then echo "shutdown checks FAILED (logs in $WORK)"; exit 1; fi
echo "shutdown checks passed"
