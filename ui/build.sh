#!/usr/bin/env bash
# Build Inngest's dev-server UI (SSPL-licensed, not vendored here) with bun into ui/dist.
#   ./ui/build.sh                                # clone inngest at the commit in ui/INNGEST_REF
#   INNGEST_SRC=/path/to/inngest ./ui/build.sh   # reuse an existing checkout
# The Docker image runs the same build (ui/build-ui.sh) in its own stage.
set -euo pipefail
cd "$(dirname "$0")"
INNGEST_REF=${INNGEST_REF:-$(cat INNGEST_REF)}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

if [[ -n "${INNGEST_SRC:-}" ]]; then
  SRC="$INNGEST_SRC"
else
  git clone --filter=blob:none https://github.com/inngest/inngest.git "$WORK/inngest" >/dev/null
  git -C "$WORK/inngest" checkout -q "$INNGEST_REF"
  SRC="$WORK/inngest"
fi

docker run --rm -v "$SRC:/src:ro" -v "$PWD/build-ui.sh:/build-ui.sh:ro" -v "$WORK:/work" \
  oven/bun:1 /build-ui.sh /src /work/dist
rm -rf dist && cp -R "$WORK/dist" dist
echo "UI built into $PWD/dist"
