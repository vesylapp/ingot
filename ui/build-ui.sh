#!/usr/bin/env bash
# Build Inngest's dev-server UI with bun. Runs wherever bun is installed (the oven/bun image in
# ./ui/build.sh and the Dockerfile).
#   build-ui.sh <inngest checkout> <output dir>
set -euo pipefail
SRC=$1
OUT=$2
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# Only the UI workspace and the GraphQL schema (used by codegen) are needed.
mkdir -p "$WORK/pkg"
cp -R "$SRC/ui" "$WORK/ui"
cp -R "$SRC/pkg/coreapi" "$WORK/pkg/coreapi"
# Only the dev-server UI and its shared packages are needed. Shared components import
# generated types from apps/dashboard sources, so keep those files but drop its package.json
# so it isn't installed as a workspace.
rm -rf "$WORK/ui/apps/support"
rm -f "$WORK/ui/apps/dashboard/package.json"
find "$WORK/ui" -name node_modules -type d -prune -exec rm -rf {} +
# The repo enforces pnpm via preinstall guards; drop them so bun can install.
find "$WORK/ui" -name package.json -not -path "*/node_modules/*" -exec bun -e '
  const f = process.argv[1];
  const d = JSON.parse(await Bun.file(f).text());
  if (d.scripts?.preinstall !== undefined) {
    delete d.scripts.preinstall;
    await Bun.write(f, JSON.stringify(d, null, 2));
  }
' {} \;

# Runs search is answered by ingot without a result cap, but the runs page disables the
# search button once a time range holds 1000+ runs unless the "duckdb-insights" flag is on
# (which also shows pages ingot does not serve). Drop the cap. Fails if upstream changes it.
bun -e '
  const f = process.argv[1];
  const from = "searchLimit={duckdbInsightsEnabled ? undefined : 1000}";
  const src = await Bun.file(f).text();
  if (!src.includes(from)) {
    console.error(`build-ui.sh: "${from}" not found in ${f}; update this patch for the new INNGEST_REF`);
    process.exit(1);
  }
  await Bun.write(f, src.replace(from, "searchLimit={undefined}"));
' "$WORK/ui/apps/dev-server-ui/src/routes/_dashboard/runs/index.tsx"

cd "$WORK/ui"
bun install
cd apps/dev-server-ui
# Same as the package "build" script minus the type-check, which does not affect output.
bun --bun x graphql-codegen --config codegen.ts
bun --bun x vite build --mode=production

rm -rf "$OUT"
cp -R dist/client "$OUT"
# The UI is Inngest's code; ship its license with it.
cp "$SRC/LICENSE.md" "$OUT/INNGEST-LICENSE.md" 2>/dev/null || cp "$SRC/LICENSE" "$OUT/INNGEST-LICENSE" 2>/dev/null || true
echo "UI built into $OUT"
