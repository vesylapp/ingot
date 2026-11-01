#!/usr/bin/env bash
# Build Inngest's dev-server UI (SSPL-licensed, not vendored here) with bun into ui/dist.
#   ./ui/build.sh                       # clone inngest at $INNGEST_REF
#   INNGEST_SRC=/path/to/inngest ./ui/build.sh   # reuse an existing checkout
set -euo pipefail
cd "$(dirname "$0")"
OUT="$PWD/dist"
INNGEST_REF=${INNGEST_REF:-236eeec}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

if [[ -n "${INNGEST_SRC:-}" ]]; then
  SRC="$INNGEST_SRC"
else
  git clone --filter=blob:none https://github.com/inngest/inngest.git "$WORK/inngest" >/dev/null
  git -C "$WORK/inngest" checkout -q "$INNGEST_REF"
  SRC="$WORK/inngest"
fi

# Only the UI workspace and the GraphQL schema (used by codegen) are needed.
mkdir -p "$WORK/build/pkg"
cp -R "$SRC/ui" "$WORK/build/ui"
cp -R "$SRC/pkg/coreapi" "$WORK/build/pkg/coreapi"
# Only the dev-server UI and its shared packages are needed.
# (shared components import generated types from apps/dashboard sources, so keep those files
# but drop its package.json so it isn't installed as a workspace.)
rm -rf "$WORK/build/ui/apps/support"
rm -f "$WORK/build/ui/apps/dashboard/package.json"
find "$WORK/build/ui" -name node_modules -type d -prune -exec rm -rf {} +
# The repo enforces pnpm via preinstall guards; drop them so bun can install.
find "$WORK/build/ui" -name package.json -not -path "*/node_modules/*" | while read -r f; do
  python3 - "$f" <<'PY'
import json, sys
p = sys.argv[1]
d = json.load(open(p))
if d.get("scripts", {}).pop("preinstall", None) is not None:
    json.dump(d, open(p, "w"), indent=2)
PY
done

docker run --rm -v "$WORK/build:/build" -w /build/ui oven/bun:1 sh -c '
  set -e
  bun install
  cd apps/dev-server-ui
  # Same as the package "build" script minus the type-check, which does not affect output.
  bun --bun x graphql-codegen --config codegen.ts
  bun --bun x vite build --mode=production
'
rm -rf "$OUT" && cp -R "$WORK/build/ui/apps/dev-server-ui/dist/client" "$OUT"
echo "UI built into $OUT"
