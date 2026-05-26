---
title: calculator Repo Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: calculator
language: en
whenToUse:
  - when a task may change solver runtime behavior, worker jobs, snapshot building, package-worker flows, or calculation-side validation tooling
  - when routing work from the workspace root into tiangong-lca-calculator
  - when deciding whether a change belongs here, in edge-functions, in database-engine, or in lca-workspace
whenToUpdate:
  - when runtime job families, validation gates, retained contract docs, or ownership boundaries change
  - when the runtime SQL boundary or package-worker contract changes
  - when repo-local documentation governance changes
checkPaths:
  - AGENTS.md
  - README.md
  - .docpact/**/*.yaml
  - docs/agents/**
  - docs/lca-api-contract.md
  - docs/matrix-readiness-report-contract.md
  - docs/review-submit-fast-gate-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
  - docs/implicit-regional-supply-mix-modeling.md
  - docs/implicit-regional-supply-mix-modeling.en.md
  - docs/tidas-package-contract.md
  - Cargo.toml
  - Makefile
  - crates/**
  - scripts/**
  - tools/bw25-validator/**
  - supabase/migrations/**
  - .github/workflows/**
  - .githooks/**
  - scripts/docpact
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedAt: 2026-05-26
lastReviewedCommit: 877f8318a1716786beb32bc86ac208c57a9168d9
related:
  - .docpact/config.yaml
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
  - docs/lca-api-contract.md
  - docs/matrix-readiness-report-contract.md
  - docs/review-submit-fast-gate-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
  - docs/tidas-package-contract.md
---

## Repo Contract

`tiangong-lca-calculator` owns the TianGong LCA solver runtime: sparse-matrix build and solve logic, worker job execution, snapshot building, provider matching, package import and export worker flows, runtime SQL expectations referenced by the solver, and calculation-side diagnostics tooling.

Start here when the task may change what the compute stack does.

## Documentation Roles

| Document | Owns | Does not own |
| --- | --- | --- |
| `AGENTS.md` | repo contract, branch and delivery rules, hard boundaries, minimal execution facts | deep runtime path maps, full proof matrix, or long setup prose |
| `.docpact/config.yaml` | machine-readable repo facts, routing intents, governed-doc rules, ownership, coverage, and freshness | explanatory prose or long-form walkthroughs |
| `docs/agents/repo-validation.md` | minimum proof by change type, manual validation helpers, PR validation note shape | repo contract, branch policy truth, or long setup notes |
| `docs/agents/repo-architecture.md` | compact repo mental model, stable path map, hotspot families, and common misreads | checklist-style proof guidance or current work queue |
| `README.md` | repo landing context, operator setup, and runtime overview | machine-readable routing or lint semantics |
| `docs/lca-api-contract.md` | shared jobs/results/payload/status contract for consumers | branch policy, proof matrix, or edge/frontend implementation details |
| `docs/matrix-readiness-report-contract.md` | calculator-owned matrix-readiness CLI and report artifact schema, blocker/finding codes, next_action semantics, and policy surface | HTTP endpoint contract or edge request/auth behavior |
| `docs/review-submit-fast-gate-contract.md` | calculator-owned review-submit fast gate schema, passed/blocked semantics, blocker codes, policy defaults, targeted probe contract, and DB runner result-recorder behavior | Edge HTTP API, persistence schema, or Next submit-review UX |
| `docs/edge-function-integration.md` | edge-facing enqueue, polling, and service-role integration contract | solver internals or frontend UX rules |
| `docs/frontend-integration.md` | frontend-facing solve/result interaction contract | edge auth implementation or solver internals |
| `docs/implicit-regional-supply-mix-modeling.md` / `docs/implicit-regional-supply-mix-modeling.en.md` | Chinese and English modeling basis for implicit regional supply mix, exchange-location supply-region anchors, and annual-volume provider share semantics | implementation checklist or consumer API contract |
| `docs/tidas-package-contract.md` | package-worker async import/export contract | generic solver runtime or branch policy truth |

## Load Order

Read in this order:

1. `AGENTS.md`
2. `.docpact/config.yaml`
3. `docs/agents/repo-validation.md` or `docs/agents/repo-architecture.md`
4. load only the narrow contract doc that matches the task:
   - `docs/lca-api-contract.md`
   - `docs/matrix-readiness-report-contract.md`
   - `docs/review-submit-fast-gate-contract.md`
   - `docs/edge-function-integration.md`
   - `docs/frontend-integration.md`
   - `docs/implicit-regional-supply-mix-modeling.md`
   - `docs/implicit-regional-supply-mix-modeling.en.md`
   - `docs/tidas-package-contract.md`
5. `README.md` only when you need longer setup or operator-facing context

Do not start from the root workspace or the edge repo if the change is really about compute truth.

## Operational Pointers

- path-level ownership, routing intents, governed-doc inventory, and lint rules live in `.docpact/config.yaml`
- minimum proof and manual helper expectations live in `docs/agents/repo-validation.md`
- stable path groups and hotspot families live in `docs/agents/repo-architecture.md`
- runtime-facing consumer contracts and report artifact contracts live in the narrow docs under `docs/*.md`
- repo-local documentation maintenance is enforced by `.github/workflows/ai-doc-lint.yml` with `docpact lint`
- the main routing intents are `solver-runtime`, `matrix-readiness`, `snapshot-and-provider`, `review-submit-gate`, `package-worker`, `runtime-sql-boundary`, `debug-and-parity`, `edge-api-boundary`, `frontend-integration`, `proof`, `repo-docs`, and `root-integration`

## Minimal Execution Facts

Keep these entry-level facts in `AGENTS.md`. Use `README.md` and `docs/agents/repo-validation.md` for the full setup and proof details.

- primary toolchain: `cargo` plus `make`
- routine branch base: `main`
- routine PR base: `main`
- canonical repo-wide check: `make check`
- hard post-edit gates:
  - `cargo clippy -p solver-worker --all-targets --all-features -- -D warnings`
  - `cargo fmt --all -- --check`
- high-value manual helpers:
  - `./scripts/build_snapshot_from_ilcd.sh`
  - `./scripts/run_full_compute_debug.sh`
  - `./scripts/run_bw25_validation.sh`
  - `./scripts/validate_additive_migration.sh`

## Ownership Boundaries

The authoritative path-level ownership map lives in `.docpact/config.yaml`.

At a human-readable level, this repo owns:

- `Cargo.toml`, `Makefile`, and `crates/**` for solver topology, sparse-runtime behavior, queue workers, snapshot builder flows, and package workers
- `scripts/**` and `tools/bw25-validator/**` for manual validation, parity, debug, snapshot, and diagnostics helpers
- `supabase/migrations/**` for runtime SQL expectations still referenced by the calculator runtime
- `README.md`, `docs/agents/**`, `docs/lca-api-contract.md`, `docs/matrix-readiness-report-contract.md`, `docs/review-submit-fast-gate-contract.md`, `docs/edge-function-integration.md`, `docs/frontend-integration.md`, `docs/implicit-regional-supply-mix-modeling.md`, `docs/implicit-regional-supply-mix-modeling.en.md`, `docs/tidas-package-contract.md`, and repo-local governed docs

This repo does not own:

- edge request normalization, auth, enqueue API, or polling API behavior
- durable schema governance, migrations as workspace-wide source of truth, or Supabase branch config truth
- workspace integration state after merge

Route those tasks to:

- `edge-functions` for request, response, auth, enqueue, and polling API behavior
- `database-engine` for durable schema, migration, RPC, policy, and Supabase branch-governance truth
- `lca-workspace` for root integration after merge

## Branch And Delivery Facts

- GitHub default branch: `main`
- true daily trunk: `main`
- routine branch base: `main`
- routine PR base: `main`
- branch model: `M1`

`tiangong-lca-calculator` does not use a separate promote line. Normal implementation merges to `main`, and later workspace delivery still requires a root submodule bump when the updated solver should ship.

## Operational Invariants

- solve result persistence is S3-only; `lca_results` stores artifact metadata and diagnostics, not inline payloads
- queue enqueue and protected writes must stay on service-side paths; do not move them to frontend clients or authenticated direct table writes
- runtime write paths assume `service_role` ownership boundaries and existing RLS restrictions on `lca_*` tables
- worker and snapshot flows expect DB connectivity plus the required S3 env set before runtime validation is meaningful

## Documentation Update Rules

- if a machine-readable repo fact, routing intent, or governed-doc rule changes, update `.docpact/config.yaml`
- if a human-readable repo contract, branch rule, or hard boundary changes, update `AGENTS.md`
- if proof expectations or manual validation helper guidance change, update `docs/agents/repo-validation.md`
- if repo shape, hotspot families, or path ownership explanation changes, update `docs/agents/repo-architecture.md`
- if shared jobs/results/payload/status semantics change, update `docs/lca-api-contract.md`
- if matrix-readiness report schema, blocker/finding codes, policy defaults, or next_action semantics change, update `docs/matrix-readiness-report-contract.md`
- if review-submit fast gate schema, blocker codes, policy defaults, targeted probe semantics, or DB runner result-recorder behavior changes, update `docs/review-submit-fast-gate-contract.md`
- if edge-facing enqueue, polling, or service-role integration guidance changes, update `docs/edge-function-integration.md`
- if frontend-facing solve/result interaction guidance changes, update `docs/frontend-integration.md`
- if implicit regional supply mix theory, exchange-location supply-region semantics, or annual-volume provider share semantics change, update both `docs/implicit-regional-supply-mix-modeling.md` and `docs/implicit-regional-supply-mix-modeling.en.md`
- if package-worker import/export contract changes, update `docs/tidas-package-contract.md`
- if landing context or operator setup changes, update `README.md`
- do not copy the same rule into multiple docs just to make it easier to find

## Hard Boundaries

- do not move solver or worker behavior into `edge-functions`
- do not treat local `supabase/migrations/**` as the workspace's durable schema governance source of truth
- do not weaken the Clippy or format gates
- do not treat a merged repo PR here as workspace-delivery complete if the root repo still needs a submodule bump

## Workspace Integration

A merged PR in `tiangong-lca-calculator` is repo-complete, not delivery-complete.

If the change must ship through the workspace:

1. merge the child PR into `tiangong-lca-calculator`
2. update the `lca-workspace` submodule pointer deliberately
3. complete any later workspace-level validation that depends on the updated solver snapshot

## Local Docpact Push Gate

Install the versioned local hook once per checkout:

```bash
./scripts/install-git-hooks.sh
```

The `pre-push` hook runs `scripts/docpact-gate.sh`, which delegates CLI lookup to `scripts/docpact` and performs strict config validation plus enforced lint before the push leaves the machine. The wrapper checks `DOCPACT_BIN`, Cargo install locations, Homebrew install locations, and then `PATH`, so local agent shells should not fail only because bare `docpact` is unavailable. The default comparison base is `origin/main`. Override it for unusual stacks with `DOCPACT_BASE_REF=<ref>` or `scripts/docpact-gate.sh --base <ref>`. The gate writes its detailed report to a temporary file so normal pushes do not create `.docpact/runs/` artifacts.
