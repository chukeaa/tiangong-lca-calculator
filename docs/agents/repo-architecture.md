---
title: worker Architecture Notes
docType: guide
scope: repo
status: active
authoritative: false
owner: worker
language: en
whenToUse:
  - when you need a compact mental model of the solver stack before editing crates, workers, or runtime SQL expectations
  - when deciding which crate or binary owns a behavior change
  - when snapshot build, package flow, or contribution-path analysis is mentioned without exact paths
whenToUpdate:
  - when major crate boundaries or job families change
  - when result persistence or runtime SQL boundaries move
  - when the current map becomes misleading
checkPaths:
  - docs/agents/repo-architecture.md
  - .docpact/config.yaml
  - Cargo.toml
  - crates/**
  - scripts/**
  - tools/bw25-validator/**
  - supabase/migrations/**
  - docs/lca-api-contract.md
  - docs/matrix-readiness-report-contract.md
  - docs/review-submit-fast-gate-contract.md
  - docs/provider-linking.md
  - docs/implicit-regional-supply-mix-modeling.md
  - docs/implicit-regional-supply-mix-modeling.en.md
  - docs/tidas-package-contract.md
  - .githooks/pre-push
  - scripts/docpact
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedAt: 2026-07-17
lastReviewedCommit: 7822f8988e0702faa745c0e97509f851450d81e7
lastReviewedNote: "Reviewed Calculation Bundle v1 ownership, exact release evidence in snapshot artifacts, and bounded directional LCI/LCIA chunk generation for Issue #123."
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-validation.md
  - ../../docs/lca-api-contract.md
  - ../../docs/matrix-readiness-report-contract.md
  - ../../docs/review-submit-fast-gate-contract.md
---

## Repo Shape

This repo is a Rust workspace with three core layers:

- `crates/suitesparse-ffi`
- `crates/solver-core`
- `crates/solver-worker`

The runtime solves sparse systems asynchronously and keeps heavy compute out of the API layer.

## Core Solver Invariants

Keep these constraints in mind before editing `crates/solver-core/**` or worker solve flows:

- The runtime solves the sparse system `Mx = b` with `M = I - A`; preserve that modeling contract when reshaping matrix-build code.
- Do not introduce explicit matrix inversion for solve paths. Reuse factorization or sparse-solve flows instead.
- Heavy recomputation belongs in async worker jobs, not inline request handlers or API-edge adapters.
- If a change affects factorization reuse, provider matching, or snapshot payload shape, review worker and persistence paths together.

## Stable Path Map

| Path group | Role |
| --- | --- |
| `crates/suitesparse-ffi/**` | CSC matrix representation and SuiteSparse bindings |
| `crates/solver-core/**` | matrix build, factorization cache, solve orchestration, provider matching |
| `crates/solver-worker/src/**` | queue workers, package worker, snapshot builder, matrix-readiness verification, result persistence |
| `scripts/**` | manual validation, debug, diagnostics, and snapshot helpers |
| `tools/bw25-validator/**` | manual Brightway comparison tooling |
| `supabase/migrations/**` | local runtime-facing SQL expectations referenced by the worker runtime |
| `docs/lca-api-contract.md` | shared jobs/results/payload/status contract for edge and frontend consumers |
| `docs/matrix-readiness-report-contract.md` | worker-owned matrix-readiness report schema, blocker/finding codes, and next-action contract |
| `docs/review-submit-fast-gate-contract.md` | worker-owned review-submit fast gate schema, blocker codes, and targeted probe contract |
| `docs/edge-function-integration.md` | edge-facing enqueue, polling, and service-role integration contract |
| `docs/frontend-integration.md` | frontend-side solve/result interaction contract |
| `docs/provider-linking.md` | current provider-link runtime decision order, default rule, candidate eligibility, and diagnostics contract |
| `docs/implicit-regional-supply-mix-modeling.md` / `docs/implicit-regional-supply-mix-modeling.en.md` | Chinese and English modeling notes for implicit regional supply mix, exchange-location supply-region anchors, and annual-volume provider share semantics |
| `docs/tidas-package-contract.md` | package-worker async import/export contract |

## Current Runtime Families

### Solve and queue jobs

The worker currently covers families such as:

- `prepare_factorization`
- `solve_one`
- `solve_batch`
- `solve_all_unit`
- `invalidate_factorization`
- `rebuild_factorization`
- `analyze_contribution_path`
- `build_snapshot`
- `lcia_result_package_build`

These flows belong to the worker runtime, not to the API repo.

The main solver worker has two queue backends. The default `SOLVER_QUEUE_BACKEND=worker-jobs` path claims `public.worker_jobs` rows from `worker_queue=solver`, maps `job_kind=lca.*` and `job_kind=lcia_result.package_build` payloads back to internal `JobPayload` variants, heartbeats `phase/progress`, records terminal results through `worker_record_job_result`, and links LCA domain rows back to the canonical `worker_jobs` id where applicable. Ordinary solve jobs link `lca_results` / cache / latest / factorization rows; LCIA result package builds use `lca_results` plus `lca_latest_all_unit_results` as the worker-produced artifacts and then mark `lcia_result_packages` preview-ready through the database service-role command. Optional `lca_jobs` rows are best-effort compatibility only for ordinary LCA jobs; production worker_jobs paths must run when `public.lca_jobs` is absent. The `SOLVER_QUEUE_BACKEND=pgmq` path is legacy compatibility/debug only, consumes `pgmq` messages from `PGMQ_QUEUE`, and requires the explicit `ALLOW_LEGACY_JOB_TABLE_BACKEND=true` / `--allow-legacy-job-table-backend` guard.

Versioned `public_plus_owner_draft` snapshot builds keep actor visibility limited to process/flow rows and load LCIA methods from the reviewed, release-pinned static cache through `crates/solver-worker/src/static_lcia_cache.rs`. That module owns trusted-base retrieval, byte/decompression limits, raw and canonical hash verification, method/locator alias validation, and streaming factor normalization. `calculation_evidence.rs` owns the v2 source/bundle/25-method coverage binding. Gap evidence is deterministically spooled as JSONL rather than retained as an exchange-by-method object graph. Build-snapshot terminal projection comes from canonical `worker_jobs` diagnostics, including reuse-resolved snapshot ID and evidence, so optional `lca_jobs` is never required. Singular/factorization diagnostics use only the exact process/version pairs in the snapshot index.

### Snapshot builder and provider matching

The snapshot builder path owns sparse payload generation, provider matching, and snapshot artifact metadata.
The current provider-link runtime contract lives in `docs/provider-linking.md`. The modeling basis for implicit regional supply mix, exchange-location supply-region anchors, and annual-volume provider shares lives in `docs/implicit-regional-supply-mix-modeling.md` and `docs/implicit-regional-supply-mix-modeling.en.md`.

The process-column contract is one complete TIDAS Process revision per `quantitativeReference.referenceToReferenceFlow` and per snapshot matrix column. Non-reference co-product outputs remain rejected provider evidence; they do not create derived processes or matrix columns. When a co-product such as `B` must participate in calculation, upstream must publish another complete, independent Process whose quantitative reference is `B`.

`crates/solver-worker/src/tidas_process_semantics.rs` owns shared target-aware TIDAS exchange allocation semantics. Object/array allocations select the current quantitative-reference target, interpret TIDAS `Perc` as percentage divided by `100`, treat a missing target in an otherwise closed vector as sparse zero, treat an undeclared allocation as `1`, and fail closed for invalid declared vectors. It exposes only two bounded legacy fallbacks: a scalar `allocations.allocation = {}` is treated as undeclared, and one targetless full `100` / exact legacy `"100%"` entry is inferred for the reference only when the Process has exactly one physical `Output` whose sole valid internal ID equals the quantitative reference. Empty arrays, `[{}]`, multiple-output or multiple-entry targetless declarations, non-full targetless fractions, invalid Output IDs, and reference mismatches remain fail-closed. Explicit-zero and sparse-zero inputs are excluded from request-root provider closure, provider-gap diagnostics, and `A` writes; zero attributed elementary exchanges are also excluded from `B` and LCIA direction/factor-coverage evidence. Snapshot build config records `allocation_semantics_version = tidas-quantitative-reference-v2`; because the config participates in source fingerprinting, v1 and earlier snapshots are not reused. Coverage remains `snapshot_coverage.v2` with additive default-zero counters `legacy_empty_allocation_as_undeclared_count` and `legacy_single_output_target_inferred_count`, which make the compatibility paths observable without broadening them.

`crates/solver-worker/src/readiness.rs` owns the worker-side verification gate for automated data production. It turns snapshot coverage, sparse payloads, and optional compiled provider decisions into a machine-readable matrix-readiness report. Foundry and CLI callers should consume that report instead of reimplementing provider closure, singular-risk, LCIA, or factorization checks outside the worker. The stable report schema, blocker/finding codes, and next-action semantics live in `docs/matrix-readiness-report-contract.md`.

`crates/solver-worker/src/review_submit_gate.rs` owns the worker-side fast gate for dataset revision review submission. It layers revision freshness, process/exchange scans, provider evidence, sparse structural checks, and targeted RHS probes into a binary `passed` / `blocked` report without full matrix inversion or full `solve_all_unit`.

`crates/solver-worker/src/review_submit_gate_runner.rs`, `crates/solver-worker/src/worker_jobs.rs`, and `crates/solver-worker/src/bin/review_submit_gate_runner.rs` are the DB runtime bridge for that gate. The legacy mode claims persisted `dataset_review_submit_gate_runs`; the `--worker-jobs` mode claims child `review_submit.gate` jobs from `public.worker_jobs`. Both modes build a no-LCIA review-submit baseline plus draft overlay snapshot for the submitted process revision, compute the `json_ordered` checksum, execute `review_submit_gate`, and record the result through the database RPC. The root `review_submit.submit` job is created and advanced by the DB/Edge coordinator contract; worker only executes the numeric gate child job.

### Maintenance worker

`crates/solver-worker/src/bin/maintenance_enqueue.rs` is the operator/timer entrypoint that enqueues worker maintenance jobs through `public.worker_enqueue_job`. `crates/solver-worker/src/bin/maintenance_worker.rs` is the `worker_jobs` consumer for maintenance work that should be observable through the shared job lifecycle. It claims `worker_queue=maintenance` and dispatches these job kinds:

- `lca.snapshot_gc`
- `lca.result_gc`
- `tidas.package_artifact_gc`
- `national_carbon.process_flow_graph_cache_build`

The maintenance worker is intentionally a thin orchestrator over the existing `snapshot_gc`, `result_gc`, `package_gc`, and `process_flow_graph_cache_builder` binaries. Those binaries keep their own safety rules, object-first behavior, active snapshot/package protections, cache-prefix contracts, and PostgreSQL advisory locks where applicable. The process-flow graph builder emits the national-carbon global non-elementary process/flow graph, binary adjacency/edge payloads, worker-computed layouts, geo-map views, and browser lookup indexes; its `expanded2d` layout is grouped by level-3 classification before being fitted to a relation-first topology and uniform overview silhouette so the frontend does not derive layout coordinates at runtime. Cache v2 nodes and metadata expose separate level-1 and level-3 cluster ids/labels, while geo-map views include worker-derived process links, scoped graph indexes, and world/china projected layouts. The `worker_jobs` layer records dry-run/execute intent, phase/heartbeat, exit status, stdout/stderr tails, parsed `[summary]` metrics, and an operator-only `maintenance_gc_report` artifact metadata row for operator visibility.

### Package worker

The package worker handles:

- `export_package`
- `import_package`

It also owns package-job artifacts and diagnostics. The default `PACKAGE_QUEUE_BACKEND=worker-jobs` path claims `public.worker_jobs` rows from `worker_queue=package`, maps `job_kind=tidas.export_package|tidas.import_package` into the same `PackageJobPayload` variants, heartbeats package progress, records terminal `worker_jobs` results, and links package artifacts / export items / request-cache rows back to the canonical `worker_jobs` id. Optional `lca_package_jobs` rows are best-effort compatibility only; production worker_jobs paths must run when `public.lca_package_jobs` is absent. The `PACKAGE_QUEUE_BACKEND=pgmq` path is legacy compatibility/debug only, consumes `pgmq` messages from `lca_package_jobs`, and requires the explicit `ALLOW_LEGACY_JOB_TABLE_BACKEND=true` / `--allow-legacy-job-table-backend` guard.

### Result persistence

Result artifacts are persisted through the worker and supporting runtime storage flows instead of inlining heavy compute payloads into the API layer.

`crates/solver-worker/src/calculation_bundle.rs` owns canonical `tiangong.calculation-bundle.v1` generation. Snapshot compilation stores an additive exact `release_evidence` projection beside the sparse payload: quantitative references, source inventory identities, direct directional biosphere exchanges, and resolved technosphere edges with both exchange internal IDs and provider weights. New all-unit solves temporarily retain `x` only inside one fixed 256-process artifact chunk, derive directional LCI from those exact direct exchanges, retain H for the existing query view, and upload content-addressed sidecars before the manifest. Older snapshots without that projection must be rebuilt and are never guessed from A/B.

## Operational Baseline

- Solve result persistence is S3-only; treat `lca_results` as artifact metadata plus diagnostics, not as an inline result store.
- The worker uses a main DB pool plus an optional queue-only DB pool. The main pool is configured through `DATABASE_URL` / `CONN`, `DB_MAX_CONNECTIONS`, `DB_MIN_CONNECTIONS`, and `DB_ACQUIRE_TIMEOUT_SECONDS`; it should remain on a session/direct connection or session pooler when compute paths use SQLx bound queries. The queue-only pool is configured through `QUEUE_DATABASE_URL` / `QUEUE_CONN`, `QUEUE_DB_MAX_CONNECTIONS`, `QUEUE_DB_MIN_CONNECTIONS`, and `QUEUE_DB_ACQUIRE_TIMEOUT_SECONDS`; if no queue URL is set it reuses the main pool.
- `WORKER_ID`, `WORKER_JOBS_CLAIM_LIMIT`, and `WORKER_JOBS_LEASE_SECONDS` control solver `worker_jobs` claim diagnostics, batch size, and lease renewal. Keep the lease longer than a normal solve/snapshot heartbeat interval and use `BUILD_SNAPSHOT_MAX_CONCURRENCY` for actual snapshot build throttling.
- `build_snapshot` is globally throttled with a PostgreSQL transaction-level advisory lock (`BUILD_SNAPSHOT_MAX_CONCURRENCY`, default `1`) across worker instances; keep `WORKER_VT_SECONDS` larger than the worst-case lock wait plus build time.
- Runtime SQLx queries use non-persistent prepared statements so the worker does not reuse named prepared statements across PostgreSQL session reuse boundaries. High-frequency pgmq polling and archive operations use the queue-only pool plus `raw_sql` with validated queue-name literals so they can run through the simple query protocol on Supabase's 6543 transaction pooler without moving compute/package/snapshot queries onto that pooler.
- Snapshot-builder local reports under `reports/snapshot-coverage` are guarded optional diagnostics, not durable artifacts. `SNAPSHOT_REPORT_MODE`, `SNAPSHOT_REPORT_RETENTION_DAYS`, `SNAPSHOT_REPORT_MAX_FILES`, and `SNAPSHOT_REPORT_MIN_FREE_BYTES` control local report writes, retention, and low-disk skipping; object-store snapshot artifacts remain the durable compute payload.
- Queue enqueue and protected writes stay on service-side runtime paths guarded by existing RLS and `service_role` boundaries.
- Worker and snapshot paths require DB connectivity plus the required S3 env set before runtime validation is meaningful.
- Worker-owned DB pools set explicit PostgreSQL `application_name` values for observability. `snapshot_builder` also applies `SNAPSHOT_DB_STATEMENT_TIMEOUT_SECONDS` as a bounded statement timeout; `0` is reserved for targeted manual recovery, not normal production operation.

## Runtime SQL Boundary

This repo still documents and depends on runtime SQL expectations, but durable schema governance belongs in `database-engine`.

Use this rule:

- runtime compute truth here
- durable schema, migration, RPC, and policy truth there

## Cross-Repo Boundaries

- `edge-functions` owns request normalization, auth, enqueue, and polling API behavior
- `database-engine` owns durable schema governance
- `lca-workspace` owns root delivery completion after a child PR merges

## Common Misreads

- API behavior does not belong in the solver repo
- local migrations here are not the workspace-wide schema source of truth
- a merged child PR does not finish workspace delivery

## Local Docpact Push Gate

This repository has a versioned local `pre-push` hook under `.githooks/pre-push` that delegates to `scripts/docpact-gate.sh` and then runs `make check`. The gate resolves the CLI through `scripts/docpact`, so local agent shells do not need bare `docpact` on `PATH`. The hook is the local guard for docpact config validation, enforced doc-governance linting, and worker runtime tests; the GitHub `ci` workflow is manual-dispatch only.
