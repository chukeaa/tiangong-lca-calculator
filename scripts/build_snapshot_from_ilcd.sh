#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f .env ]; then
  set -a
  # shellcheck source=/dev/null
  source .env
  set +a
fi

CARGO_BIN="${CARGO_BIN:-}"
if [ -z "$CARGO_BIN" ]; then
  if command -v cargo >/dev/null 2>&1; then
    CARGO_BIN="$(command -v cargo)"
  elif [ -x "$HOME/.cargo/bin/cargo" ]; then
    CARGO_BIN="$HOME/.cargo/bin/cargo"
  else
    echo "missing required command: cargo" >&2
    exit 1
  fi
fi

DB_URL="${DATABASE_URL:-${CONN:-}}"
if [ -z "$DB_URL" ]; then
  echo "missing DB connection: set DATABASE_URL or CONN" >&2
  exit 1
fi

usage() {
  cat <<'USAGE'
Build one computable LCA snapshot artifact from source tables (`processes`/`flows`/`lciamethods`).

Usage:
  scripts/build_snapshot_from_ilcd.sh [options]

Options:
  --snapshot-id <uuid>         explicit snapshot id (default: auto generated)
  --process-states <csv|all>   process state_code filter, use "all" to disable filter (default: "100,101,...,199")
  --include-user-id <uuid>     include processes from this user_id in addition to --process-states
  --process-limit <n>          limit processes for debug snapshot, 0 = no limit (default: 0)
  --provider-rule <rule>       provider matching rule (default: split_by_process_volume)
  --self-loop-cutoff <float>   drop technosphere diagonal edges with |value| >= cutoff (default: 0.999999)
  --singular-eps <float>       epsilon for near-singular diagonal checks (default: 1e-12)
  --report-dir <path>          write coverage report json/md files (default: reports/snapshot-coverage)
  --method-id <uuid>           LCIA method id (default: auto pick largest factor-count method)
  --method-version <version>   LCIA method version (required if --method-id is set)
  --no-lcia                    skip C matrix build (impact stage disabled)
  -h, --help                   show this help
USAGE
}

for arg in "$@"; do
  if [ "$arg" = "-h" ] || [ "$arg" = "--help" ]; then
    usage
    exit 0
  fi
done

# snapshot_builder expects DATABASE_URL; keep CONN compatibility for this script.
export DATABASE_URL="$DB_URL"

echo "[info] using artifact-first snapshot builder (Rust)"
echo "[info] command: $CARGO_BIN run -p solver-worker --bin snapshot_builder --release -- $*"

"$CARGO_BIN" run -p solver-worker --bin snapshot_builder --release -- "$@"
