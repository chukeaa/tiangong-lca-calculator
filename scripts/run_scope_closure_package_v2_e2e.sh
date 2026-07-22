#!/usr/bin/env bash
set -euo pipefail

worker_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace_root="$(git -C "$worker_root" rev-parse --show-superproject-working-tree)"
database_root="$workspace_root/database-engine"

if [[ -z "$workspace_root" || ! -d "$database_root/supabase" ]]; then
  echo "database-engine submodule not found from Worker checkout" >&2
  exit 2
fi

(
  cd "$database_root"
  supabase db reset
)

eval "$(cd "$database_root" && supabase status -o env)"
export DATABASE_URL="$DB_URL"
export S3_ENDPOINT="$STORAGE_S3_URL"
export S3_REGION="$S3_PROTOCOL_REGION"
export S3_BUCKET="lca-results-e2e"
export S3_ACCESS_KEY_ID="$S3_PROTOCOL_ACCESS_KEY_ID"
export S3_SECRET_ACCESS_KEY="$S3_PROTOCOL_ACCESS_KEY_SECRET"
export SNAPSHOT_BUILDER_BIN="$worker_root/target/debug/snapshot_builder"
export SNAPSHOT_REPORT_MODE="disabled"
export TIDAS_VALIDATE_BIN="$worker_root/crates/solver-worker/tests/fixtures/tidas_validate_e2e_stub.py"

cd "$worker_root"
cargo build -p solver-worker --bin snapshot_builder
cargo test -p solver-worker --test scope_closure_package_v2_e2e -- --ignored --nocapture
