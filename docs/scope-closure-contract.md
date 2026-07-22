---
title: LCIA Scope Closure Worker Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: en
whenToUse:
  - when changing lcia.scope_closure_check worker execution
  - when changing closure evidence, scan reuse, or package-build certificate binding
  - when coordinating closure contracts with database-engine, edge-functions, or tidas-tools
whenToUpdate:
  - when closure job payloads, release snapshots, traversal rules, validation protocols, artifacts, or certificate bindings change
checkPaths:
  - docs/scope-closure-contract.md
  - AGENTS.md
  - .docpact/config.yaml
  - crates/solver-worker/src/scope_closure.rs
  - crates/solver-worker/src/queue.rs
  - crates/solver-worker/src/types.rs
  - crates/solver-worker/src/db.rs
  - docs/lca-api-contract.md
  - docs/tidas-package-contract.md
  - docs/agents/repo-architecture.md
  - docs/agents/repo-validation.md
lastReviewedAt: 2026-07-22
lastReviewedCommit: 9990d44
lastReviewedNote: "Added the Issue #139 certificate-grade scope-closure executor and package-build evidence binding."
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/tidas-package-contract.md
  - docs/agents/repo-validation.md
---

# LCIA Scope Closure Worker Contract

## Ownership

The Worker owns execution of `lcia.scope_closure_check`: immutable-source verification, deterministic reference traversal, document validation, issue aggregation, artifact production, and lease-fenced terminal projection.

`database-engine` owns durable tables, policies, RPC signatures, request normalization, current-release snapshot creation, certificate minting, and atomic build binding. `tidas-tools` owns reference extraction and document-validation protocol semantics. Edge and Next consume those contracts; they do not recreate closure truth.

## Job and immutable input

The canonical job kind is `lcia.scope_closure_check` with payload schema `lcia.scope_closure_check.request.v1`. The claimed database envelope carries:

- `closure_check_id`
- `scan_execution_id`
- `data_snapshot_token`
- `request_fingerprint`

The Worker loads the full service input through `svc_lcia_scope_closure_check_get_worker_input`. It requires the normalized requested scope, scope/policy/request hashes, expected validator-scanner fingerprint, publication epoch, and `lcia.scope-closure-data-snapshot.v2`.

The V2 data snapshot is the only certificate-grade source boundary. It contains:

- the exact normalized requested scope;
- the current public publication and release-run identity;
- the release manifest hash;
- the complete `lca_release_dataset_versions` allowlist with exact dataset identity, role, source-process provenance, version-significant hash, semantic hash, and canonical content hash.

The Worker recomputes the PostgreSQL JSONB scope and snapshot hashes with the database's authoritative hash helper. A blank or inconsistent binding fails closed. Requested process roots that are present in the release must have role `unit_process`; a root absent from the release is reported as an incomplete source-boundary blocker.

## Frozen-source traversal

Traversal is a union traversal over all exact process and LCIA-method roots. It is:

- breadth-first, cycle-safe, and deterministic;
- deduplicated by `(dataset type, UUID, exact version)`;
- fetched in bounded batches of 96 identities;
- non-fail-fast for domain findings;
- checkpointed through the active Worker lease between batches.

Every database fetch is constrained to an identity in the frozen release manifest. The Worker canonicalizes each fetched JSON document and compares it with the release's `canonicalContentHash`. These conditions make the scan incomplete and block certificate issuance:

- an allowlisted identity is unreadable from the live source table;
- live content for an allowlisted exact identity has drifted from the release hash;
- a referenced identity exists live but is absent from the frozen release manifest.

An exact reference never falls back to another version. A missing exact identity is a complete negative finding when it is absent from both the release allowlist and the observed closure. The legacy omitted-version policy is normally `reject`. If a tracked future scope explicitly uses `latest_eligible`, candidates and the deterministic winner must come only from the frozen release manifest, and the resolution map records the policy, candidate universe, candidates, and selected identity.

`linkPolicy.providerUniversePolicy=scope_only` rejects a process provider outside the requested roots. `eligible_transitive_expansion-v1` may add a referenced process only when that exact identity is in the frozen release. Every accepted transitive process is part of the effective scope and evidence; the Worker never searches a mutable live provider universe.

## TIDAS validation

Reference extraction in `scope_closure.rs` mirrors the public `tidas.reference-extraction-result.v1` contract and is locked by the shared golden fixture under `crates/solver-worker/tests/fixtures/reference_extraction_v1/`.

Document validation uses only public TIDAS CLI surfaces:

1. `--describe --format json` verifies support for `document-validation-batch.v1`.
2. Uncached documents are spooled as canonical JSON plus an exact JSONL input manifest.
3. The Worker invokes profile `tidas-document-conformance.v1`.
4. JSONL stdout is consumed line by line, providing bounded pipe backpressure.
5. A nonzero command exit, malformed event, or missing final event is a system failure; document issues are domain blockers.

Document-validation evidence is cached only under the full immutable key: exact dataset identity, canonical content hash, validator package version, validation profile, report schema, engine fingerprint, and TIDAS schema-lock hash. Cached issue events are replayed into the current scan; cache identity never depends on a mutable row alone.

## Issues and affected roots

The Worker coalesces deterministic issue keys while retaining occurrence counts. Each issue records the primary source identity, JSON path, reference role, requested target identity, message, action, and blocker status. Graph analysis records every affected root and a deterministic witness path specific to that root. Result projection stores primary issues, occurrences, and affected-root rows through the database's V2 result RPC.

The scan never short-circuits after the first broken reference or invalid document. This gives the operator one stable issue set for the entire requested union.

## Evidence and artifacts

Each fresh scan produces deterministic content artifacts:

- `closure-bundle-v1.json`: requested bindings, TIDAS validation evidence, scan, and resolution map;
- `closure-snapshot-v1.json`: effective scope and immutable evidence hashes;
- `closure-issues-v1.jsonl`: one canonical issue per line;
- `closure-report-v1.xlsx`: a valid workbook tagged with the current `closureCheckId`.

Artifacts are uploaded and persisted before terminal projection. The report artifact manifest hash is recomputed from persisted database metadata. `evidenceHash` binds the immutable scan evidence but intentionally excludes the run-specific report artifact manifest. A certificate additionally binds the current closure check and its current report artifact manifest, so copied or stale reports cannot be substituted.

A certificate is available only for `status=passed` and `scanCompleteness=complete`. Domain blockers produce a complete blocked result. Cancellation, lease loss, validator failure, source drift, or another system failure cannot produce a valid certificate.

## Shared scans and retry behavior

`scan_execution_id` coordinates identical immutable work. The Worker claims it with the active job lease:

- acquired executions run normally;
- busy executions wait with lease heartbeats and bounded exponential backoff;
- completed executions may reuse immutable scan evidence only when the database verifies all request, policy, snapshot, and scan bindings.

Reuse does not copy the source run's report or result summary. The current run rebuilds and uploads a new XLSX tagged with its own `closureCheckId`, supplies a new result summary to the six-argument reuse finalizer, and receives a new target-scoped certificate bound to the new report manifest. Source `evidenceHash` remains immutable.

The early-failure RPC is safe before or after scan claim. It fails only the current run and releases a scan execution only when this job holds its lease; a waiter cannot destroy another run's reusable work.

## Package build binding

The database Build V2 command atomically enqueues `lcia_result.package_build` with a full closure binding:

- `closure_check_id`
- `closure_certificate_hash`
- `effective_scope_hash`
- `data_snapshot_token`
- `snapshot_id`
- `snapshot_hash`
- `closure_bundle_hash`
- `report_artifact_manifest_hash`

The Worker accepts this binding only all-or-none and validates every field against a currently valid, complete, passed closure check before package execution. It consumes the certificate and frozen snapshot; it does not rerun administrative closure.

Closure binding changes provenance and eligibility, not numerical computation. The existing package snapshot build, all-unit solve, result artifact, and ready-marking path remains unchanged. Result JSON, result refs, persisted package metadata, and audit context preserve `closureCheckId` so downstream consumers can prove which certificate authorized the unchanged numerical output.

## Required proof

For changes to this contract, run the repo baseline plus focused closure tests. At minimum, preserve proof for:

- TIDAS/Worker golden extraction parity;
- union traversal, shared dependencies, cycles, exact versions, and non-fail-fast aggregation;
- frozen-release live drift and live-only substitution rejection;
- omitted-version frozen candidates and winner provenance;
- bounded batches, deterministic hashes, cancellation, and valid check-scoped XLSX output;
- all-or-none package binding and database certificate mismatch rejection;
- shared-scan target-specific report/finalizer behavior.

Live integration proof, when available, must use isolated non-production database and object-storage state. Do not deploy or mutate production data as validation.
