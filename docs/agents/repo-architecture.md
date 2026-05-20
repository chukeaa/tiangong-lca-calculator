---
title: calculator Architecture Notes
docType: guide
scope: repo
status: active
authoritative: false
owner: calculator
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
  - docs/implicit-regional-supply-mix-modeling.md
  - docs/implicit-regional-supply-mix-modeling.en.md
  - docs/tidas-package-contract.md
  - .githooks/pre-push
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedAt: 2026-05-20
lastReviewedCommit: f23848d58634dbf6f77df741210476f8d7bf61a1
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-validation.md
  - ../../docs/lca-api-contract.md
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
| `crates/solver-worker/src/**` | queue workers, package worker, snapshot builder, result persistence |
| `scripts/**` | manual validation, debug, diagnostics, and snapshot helpers |
| `tools/bw25-validator/**` | manual Brightway comparison tooling |
| `supabase/migrations/**` | local runtime-facing SQL expectations referenced by the calculator runtime |
| `docs/lca-api-contract.md` | shared jobs/results/payload/status contract for edge and frontend consumers |
| `docs/edge-function-integration.md` | edge-facing enqueue, polling, and service-role integration contract |
| `docs/frontend-integration.md` | frontend-side solve/result interaction contract |
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

These flows belong to the calculator runtime, not to the API repo.

### Snapshot builder and provider matching

The snapshot builder path owns sparse payload generation, provider matching, and snapshot artifact metadata.
The modeling basis for implicit regional supply mix, exchange-location supply-region anchors, and annual-volume provider shares lives in `docs/implicit-regional-supply-mix-modeling.md` and `docs/implicit-regional-supply-mix-modeling.en.md`.

### Package worker

The package worker handles:

- `export_package`
- `import_package`

It also owns package-job artifacts and diagnostics.

### Result persistence

Result artifacts are persisted through the worker and supporting runtime storage flows instead of inlining heavy compute payloads into the API layer.

## Operational Baseline

- Solve result persistence is S3-only; treat `lca_results` as artifact metadata plus diagnostics, not as an inline result store.
- The worker uses a main DB pool plus an optional queue-only DB pool. The main pool is configured through `DATABASE_URL` / `CONN`, `DB_MAX_CONNECTIONS`, `DB_MIN_CONNECTIONS`, and `DB_ACQUIRE_TIMEOUT_SECONDS`; it should remain on a session/direct connection or session pooler when compute paths use SQLx bound queries. The queue-only pool is configured through `QUEUE_DATABASE_URL` / `QUEUE_CONN`, `QUEUE_DB_MAX_CONNECTIONS`, `QUEUE_DB_MIN_CONNECTIONS`, and `QUEUE_DB_ACQUIRE_TIMEOUT_SECONDS`; if no queue URL is set it reuses the main pool.
- `build_snapshot` is globally throttled with a PostgreSQL transaction-level advisory lock (`BUILD_SNAPSHOT_MAX_CONCURRENCY`, default `1`) across worker instances; keep `WORKER_VT_SECONDS` larger than the worst-case lock wait plus build time.
- Runtime SQLx queries use non-persistent prepared statements so the worker does not reuse named prepared statements across PostgreSQL session reuse boundaries. High-frequency pgmq polling and archive operations use the queue-only pool plus `raw_sql` with validated queue-name literals so they can run through the simple query protocol on Supabase's 6543 transaction pooler without moving compute/package/snapshot queries onto that pooler.
- Queue enqueue and protected writes stay on service-side runtime paths guarded by existing RLS and `service_role` boundaries.
- Worker and snapshot paths require DB connectivity plus the required S3 env set before runtime validation is meaningful.

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

This repository has a versioned local `pre-push` hook under `.githooks/pre-push` that delegates to `scripts/docpact-gate.sh`. The hook is a local developer guard for docpact config validation and enforced doc-governance linting; CI remains the authoritative PR enforcement path.
