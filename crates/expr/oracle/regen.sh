#!/usr/bin/env bash
# Regenerate tests/fixtures/go_oracle.jsonl from the reference Go implementation: every
# expression in corpus.txt is evaluated against every dataset in tests/fixtures/datasets.json
# by Inngest's own pkg/expressions.
#   ./crates/expr/oracle/regen.sh                                # clone inngest at ui/INNGEST_REF
#   INNGEST_SRC=/path/to/inngest ./crates/expr/oracle/regen.sh   # reuse an existing checkout
# Needs Go (the toolchain Inngest's go.mod asks for is fetched automatically).
set -euo pipefail
cd "$(dirname "$0")"
INNGEST_REF=${INNGEST_REF:-$(cat ../../../ui/INNGEST_REF)}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

if [[ -n "${INNGEST_SRC:-}" ]]; then
  SRC=$(cd "$INNGEST_SRC" && pwd)
else
  git clone --filter=blob:none https://github.com/inngest/inngest.git "$WORK/inngest" >/dev/null 2>&1
  git -C "$WORK/inngest" checkout -q "$INNGEST_REF"
  SRC="$WORK/inngest"
fi

# Build main.go as a package of the Inngest module without writing into the checkout.
printf '{"Replace":{"%s/cmd/exproracle/main.go":"%s/main.go"}}' "$SRC" "$PWD" >"$WORK/overlay.json"
(cd "$SRC" && go build -mod=vendor -overlay "$WORK/overlay.json" -o "$WORK/exproracle" ./cmd/exproracle)

python3 regen.py "$WORK/exproracle" corpus.txt ../tests/fixtures/datasets.json ../tests/fixtures/go_oracle.jsonl
