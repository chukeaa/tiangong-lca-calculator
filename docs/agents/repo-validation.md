---
title: calculator Validation Guide
docType: guide
scope: repo
status: active
authoritative: false
owner: calculator
language: en
whenToUse:
  - when a tiangong-lca-calculator change is ready for local validation
  - when deciding the minimum proof required for solver, worker, script, runtime-contract, or docpact changes
  - when writing PR validation notes for tiangong-lca-calculator work
whenToUpdate:
  - when the repo gains new canonical validation wrappers
  - when change categories require different proof
  - when runtime SQL or parity-validation expectations change
checkPaths:
  - docs/agents/repo-validation.md
  - .docpact/config.yaml
  - Cargo.toml
  - Makefile
  - crates/**
  - scripts/**
  - tools/bw25-validator/**
  - supabase/migrations/**
  - docs/lca-api-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
  - docs/implicit-regional-supply-mix-modeling.md
  - docs/implicit-regional-supply-mix-modeling.en.md
  - docs/tidas-package-contract.md
  - .github/workflows/**
  - .githooks/pre-push
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedAt: 2026-05-20
lastReviewedCommit: f7c7d97e64dab987631281c3835eb7d2a343b94a
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-architecture.md
  - ../../docs/lca-api-contract.md
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

## Validation Matrix

| Change type | Minimum local proof | Additional proof when risk is higher | Notes |
| --- | --- | --- | --- |
| `crates/**` solver or worker code | `make check`; hard Clippy gate; hard format gate | run the narrow manual script that matches the touched area, such as snapshot build, full compute debug, or BW25 validation | Record which job family or worker path was exercised. |
| snapshot-builder or provider-matching behavior | baseline gates plus `./scripts/build_snapshot_from_ilcd.sh` when safe | run provider-link diagnostics export helpers when the task changes matching logic | Snapshot and provider diagnostics often need task-specific proof. |
| package worker import or export flows | baseline gates | run the closest safe package-flow helper or record why live package proof is deferred | Package-job semantics are runtime-sensitive and may depend on storage or DB state. |
| runtime SQL expectation docs or local migration helpers | baseline gates plus `./scripts/validate_additive_migration.sh` when the task touches migration expectations | record separately when durable schema proof is required in `database-engine` | Local migration files here are not the workspace-wide source of truth. |
| manual debug, parity, or target-validation scripts | run the touched script with safe args or `--help` when available, plus baseline gates if code changed nearby | `./scripts/run_full_compute_debug.sh`, `./scripts/run_bw25_validation.sh`, or `./scripts/validate_lcia_targets.sh` as applicable | `bw25-validator` is manual-only and out-of-band. |
| repo docs or docpact config only | `docpact validate-config --root . --strict`; `docpact lint --root . --worktree --mode enforce` | perform route checks for affected intent surfaces such as `solver-runtime`, `package-worker`, or `runtime-sql-boundary` | Refresh review metadata even when prose-only docs change. |

## Minimum PR Note Quality

A good PR note for this repo should say:

1. which baseline gates ran
2. which job family, script, or manual parity helper was exercised
3. whether any required database-engine or edge-functions proof lives elsewhere

## Docpact Governance Notes

The repo's machine-readable governance source is `.docpact/config.yaml`.

That means:

- governed-doc rules, routing intents, ownership boundaries, and freshness live in `.docpact/config.yaml`
- `.github/workflows/ai-doc-lint.yml` should validate config and run `docpact lint`
- retained explanatory docs stay in `AGENTS.md`, this file, `repo-architecture.md`, `README.md`, and the narrow runtime-facing contract docs under `docs/*.md`

Do not recreate deleted `ai/*` files under a new name. Keep deterministic facts in config and explanatory material in retained source docs.

## Local Docpact Push Gate

Install the versioned local hook once per checkout:

```bash
./scripts/install-git-hooks.sh
```

The `pre-push` hook runs `scripts/docpact-gate.sh`, which performs strict config validation and `docpact lint --mode enforce` before the push leaves the machine. The default comparison base is `origin/main`. Override it for unusual stacks with `DOCPACT_BASE_REF=<ref>` or `scripts/docpact-gate.sh --base <ref>`. The gate writes its detailed report to a temporary file so normal pushes do not create `.docpact/runs/` artifacts.
