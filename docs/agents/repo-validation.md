---
title: worker Validation Guide
docType: guide
scope: repo
status: active
authoritative: false
owner: worker
language: en
whenToUse:
  - when a tiangong-lca-worker change is ready for local validation
  - when deciding the minimum proof required for solver, worker, script, runtime-contract, or docpact changes
  - when writing PR validation notes for tiangong-lca-worker work
whenToUpdate:
  - when the repo gains new canonical validation wrappers
  - when change categories require different proof
  - when runtime SQL or parity-validation expectations change
checkPaths:
  - docs/agents/repo-validation.md
  - .docpact/config.yaml
  - .env.example
  - Cargo.toml
  - Makefile
  - crates/**
  - scripts/**
  - tools/bw25-validator/**
  - supabase/migrations/**
  - docs/lca-api-contract.md
  - docs/matrix-readiness-report-contract.md
  - docs/review-submit-fast-gate-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
  - docs/provider-linking.md
  - docs/implicit-regional-supply-mix-modeling.md
  - docs/implicit-regional-supply-mix-modeling.en.md
  - docs/tidas-package-contract.md
  - .github/workflows/**
  - .githooks/pre-push
  - scripts/docpact
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedAt: 2026-07-17
lastReviewedCommit: 6d61068445ebfad0fb3e07469f6d0468d692574a
lastReviewedNote: "Reviewed frozen source-closure proof and reviewed LCIA method locator aliases for Issue #127."
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-architecture.md
  - ../../docs/lca-api-contract.md
  - ../../docs/matrix-readiness-report-contract.md
  - ../../docs/review-submit-fast-gate-contract.md
  - ../../docs/tidas-package-contract.md
---

## Default Baseline

Unless the change is doc-only repo-maintenance work, the default baseline is:

```bash
make check
cargo clippy -p solver-worker --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Treat the last two commands as non-negotiable hard gates after code changes.

The local `pre-push` hook runs the docpact gate first and then runs `make check`. The GitHub `ci` workflow is manual-dispatch only, so ordinary branch pushes do not spend Actions minutes on standalone tests.

## Validation Matrix

| Change type | Minimum local proof | Additional proof when risk is higher | Notes |
| --- | --- | --- | --- |
| `crates/**` solver or worker code | `make check`; hard Clippy gate; hard format gate | run the narrow manual script that matches the touched area, such as snapshot build, full compute debug, or BW25 validation | Record which job family or worker path was exercised. |
| Calculation Bundle / all-unit directional LCI | `cargo test -p solver-worker calculation_bundle`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo check -p solver-worker --all-targets`; hard Clippy and format gates | with safe DB/S3 env, rebuild one snapshot, run `solve_all_unit`, verify manifest-last upload, all compressed/uncompressed hashes, exact 256-process boundaries, reviewed 25-method identities, recursively complete TIDAS source closure, directional LCI parity, and retry byte determinism | Old snapshots without `compiled_graph.release_evidence.source_datasets` must fail closed and be rebuilt. Never infer exchange IDs, versions, units, directions, provider output IDs, or source documents from matrix indices or mutable solve-time database state. |
| solver `worker_jobs` queue backend | `cargo test -p solver-worker worker_jobs`; `cargo test -p solver-worker maps_worker_jobs`; `cargo check -p solver-worker`; hard Clippy gate; hard format gate | when DB/S3 env is available, enqueue one safe `worker_queue=solver` job and run `solver-worker --queue-backend worker-jobs --mode worker` to verify claim/heartbeat/result projection; for legacy-table retirement, run against a schema where `public.lca_jobs` is absent or ignored | Keep `docs/lca-api-contract.md` and `docs/edge-function-integration.md` aligned with job kind, payload schema, worker_jobs result_ref, and optional legacy `lca_jobs` compatibility expectations. |
| versioned public-plus-owner-draft snapshot / LCIA evidence | `cargo test -p solver-worker calculation_evidence`; `cargo test -p solver-worker static_lcia_cache`; `cargo test -p solver-worker maps_exact_public_owner_draft_build_v2`; `cargo test -p solver-worker rejects_summary_only_lcia_manifest_before_build_execution`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo check -p solver-worker --all-targets`; baseline hard gates | run ignored `verifies_reviewed_release_bundle_bytes` with `LCIA_STATIC_CACHE_RELEASE_DIR=<next-public-root>` whenever the reviewed static bundle changes; in a non-production environment with DB/S3 available, enqueue one v2 build and verify public `100`, owner `0`, foreign/nonzero/collaboration rejection, snapshot-index source/identity hashes, per-method JSONL gap count, worker-only build result projection, and solve binding drift rejection | Never use a production data mutation as validation. Keep the complete reviewed manifest plus Edge/Next/Worker v2 source, scope, matrix, and release hashes byte-for-byte aligned; reject summary-only manifests during queue payload validation, and reject v1 source/evidence/solve downgrade. |
| snapshot-builder signed-flow linking or routing | `cargo test -p solver-worker --lib signed_flow`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo check -p solver-worker --all-targets`; hard Clippy/format gates; `./scripts/build_snapshot_from_ilcd.sh` when safe | exercise Product and Waste reference Input/Output with positive/negative amounts, opposite/same-sign candidates, multi-reference rejection, self-link exclusion, request-root flow-space closure, multi-candidate weights, and closed/open/cutoff evidence | Keep `docs/provider-linking.md` and both implicit regional supply mix docs aligned. Assert non-negative activity requirements and signed closure, not direction/type-based provider roles. |
| matrix-readiness / signed-balance closure gate | `cargo test -p solver-worker readiness`; `cargo check -p solver-worker --bin matrix_readiness`; hard Clippy gate for the touched binary/module | run `snapshot_builder` or `matrix_readiness --input <fixture> --out <report>` against the closest available target artifact; verify `balance_evidence`, `unresolved_balances`, and explicit boundary-policy behavior | Keep `docs/matrix-readiness-report-contract.md` aligned with schema, blocker/finding code, policy, and next_action changes. Use `PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig` on Homebrew setups. |
| review-submit fast gate | `cargo test -p solver-worker review_submit_gate`; `cargo check -p solver-worker --bin review_submit_gate`; for DB runner or `worker_jobs` changes also run `cargo test -p solver-worker worker_jobs`, `cargo test -p solver-worker review_submit_gate_runner`, and `cargo check -p solver-worker --bin review_submit_gate_runner`; hard Clippy gate for the touched binary/module | run `review_submit_gate --input <fixture> --out <report> --fail-on-blocked` against a representative dataset revision artifact; for live DB runner changes, run `review_submit_gate_runner --once` or `review_submit_gate_runner --worker-jobs --once` against a safe queued gate run when service-role DB and S3 artifact env are available | Keep `docs/review-submit-fast-gate-contract.md` aligned with blocker codes, policy defaults, targeted probe behavior, and DB result-recorder semantics. |
| maintenance worker_jobs / GC orchestration | `cargo check -p solver-worker --bin maintenance_worker`; `cargo check -p solver-worker --bin maintenance_enqueue`; run touched binaries such as `cargo check -p solver-worker --bin snapshot_gc --bin result_gc --bin package_gc --bin process_flow_graph_cache_builder`; `cargo test -p solver-worker --bin maintenance_worker`; `cargo test -p solver-worker --bin maintenance_enqueue`; run the touched GC/filter/cache binary tests such as `cargo test -p solver-worker snapshot_gc`, `cargo test -p solver-worker result_gc`, `cargo test -p solver-worker package_gc`, or `cargo test -p solver-worker --bin process_flow_graph_cache_builder`; hard Clippy gate for all targets | run a safe dry-run `lca.snapshot_gc`, `lca.result_gc`, `tidas.package_artifact_gc`, or `national_carbon.process_flow_graph_cache_build` worker job in dev when DB and storage env are available; legacy-table retirement should verify `result_gc` does not join `lca_jobs` and package GC can run without `lca_package_jobs` | Keep `docs/agents/repo-architecture.md`, `README.md`, deployment units, and the package/LCA retention docs aligned with job kind, payload, summary, and destructive-execute safety semantics. |
| package worker import or export flows | baseline gates | run the closest safe package-flow helper or record why live package proof is deferred | Package-job semantics are runtime-sensitive and may depend on storage or DB state. |
| package `worker_jobs` queue backend | `cargo test -p solver-worker --bin package_worker`; `cargo test -p solver-worker package_worker`; `cargo check -p solver-worker --bin package_worker`; hard Clippy gate; hard format gate | when DB/S3 env is available, enqueue one safe `worker_queue=package` job and run `package_worker --package-queue-backend worker-jobs` to verify claim/heartbeat/result projection; for legacy-table retirement, run against a schema where `public.lca_package_jobs` is absent or ignored | Keep `docs/tidas-package-contract.md` aligned with job kind, payload schema, continuation behavior, artifact projection, worker_jobs result_ref, and optional legacy `lca_package_jobs` compatibility expectations. |
| runtime SQL expectation docs or local migration helpers | baseline gates plus `./scripts/validate_additive_migration.sh` when the task touches migration expectations | record separately when durable schema proof is required in `database-engine` | Local migration files here are not the workspace-wide source of truth. |
| manual debug, parity, or target-validation scripts | run the touched script with safe args or `--help` when available, plus baseline gates if code changed nearby | `./scripts/run_full_compute_debug.sh`, `./scripts/run_bw25_validation.sh`, or `./scripts/validate_lcia_targets.sh` as applicable | `bw25-validator` is manual-only and out-of-band. |
| repo docs, `.env.example`, or docpact config only | `scripts/docpact validate-config --root . --strict`; `scripts/docpact lint --root . --worktree --mode enforce` | perform route checks for affected intent surfaces such as `solver-runtime`, `package-worker`, or `runtime-sql-boundary` | Refresh review metadata even when prose-only docs change. Keep `.env.example` secret-free. |

## Replay A Previously Successful Calculation

Use this acceptance flow when a matrix-build or allocation-semantics change needs proof against a previously successful calculation:

1. Select a completed calculation whose resolved process closure contains at least one process with co-product allocation data affected by the change. Keep one or more unaffected processes from the same scope as controls.
2. Capture the baseline before replay: the canonical original request and its hash; worker job ID; solve job ID; snapshot ID; result ID; result artifact URL, format, byte size, and SHA-256; and the snapshot process/flow/impact counts plus A/B/C nonzero counts.
3. Replay the same business request with new build and solve job IDs, a new requested snapshot ID, a new result ID, and fresh idempotency/request keys. Do not mutate or reuse the completed task records.
4. Verify that `allocation_semantics_version`, `link_semantics_version`, `technosphere_boundary_policy`, and `flow_identity_policy` participate in snapshot and review identity/reuse decisions. Confirm that changed semantics produce a newly built snapshot rather than reusing the baseline.
5. Export and independently validate both results, keeping separate output paths:

   ```bash
   ./scripts/export_latest_matrices.sh --result-id <old-result-id> --base-name before --no-latest-pointers
   ./scripts/export_latest_matrices.sh --result-id <new-result-id> --base-name after --no-latest-pointers
   ./scripts/run_bw25_validation.sh --result-id <old-result-id>
   ./scripts/run_bw25_validation.sh --result-id <new-result-id>
   ```

6. Compare by process and flow UUID rather than matrix index. Record the old/new A and B entries for affected processes, the old/new target LCIA values with absolute and relative deltas, and tolerance results for the unaffected control processes.
7. Perform every replay write only in staging or a local environment with isolated database and object-storage state. Production reads may be used to select or copy an authorized baseline, but production mutation, enqueue, snapshot creation, cache invalidation, or active-pointer changes are prohibited as validation steps.

## Minimum PR Note Quality

A good PR note for this repo should say:

1. which baseline gates ran
2. which job family, script, or manual parity helper was exercised
3. whether any required database-engine or edge-functions proof lives elsewhere

## Docpact Governance Notes

The repo's machine-readable governance source is `.docpact/config.yaml`.

That means:

- governed-doc rules, routing intents, ownership boundaries, and freshness live in `.docpact/config.yaml`
- `.github/workflows/ai-doc-lint.yml` is manual-dispatch fallback and should delegate to the same local docpact gate
- retained explanatory docs stay in `AGENTS.md`, this file, `repo-architecture.md`, `README.md`, and the narrow runtime-facing contract docs under `docs/*.md`

Do not recreate deleted `ai/*` files under a new name. Keep deterministic facts in config and explanatory material in retained source docs.

## Local Docpact Push Gate

Install the versioned local hook once per checkout:

```bash
./scripts/install-git-hooks.sh
```

The `pre-push` hook runs `scripts/docpact-gate.sh`, which delegates CLI lookup to `scripts/docpact` and performs strict config validation plus enforced lint before the push leaves the machine. It then runs `make check` as the local test gate. The wrapper checks `DOCPACT_BIN`, Cargo install locations, Homebrew install locations, and then `PATH`, so local agent shells should not fail only because bare `docpact` is unavailable. The default comparison base is `origin/main`. Override it for unusual stacks with `DOCPACT_BASE_REF=<ref>` or `scripts/docpact-gate.sh --base <ref>`. The gate writes its detailed report to a temporary file so normal pushes do not create `.docpact/runs/` artifacts.
