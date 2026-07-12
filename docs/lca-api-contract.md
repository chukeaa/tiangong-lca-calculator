---
title: LCA API Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: zh-CN
whenToUse:
  - 当你需要共享的 jobs/results/payload/status 契约时
  - 当 edge-functions 或前端的集成行为依赖 worker runtime 输出语义时
whenToUpdate:
  - 当 job payload、状态机、结果 artifact、幂等规则或服务端权限边界变化时
checkPaths:
  - docs/lca-api-contract.md
  - docs/provider-linking.md
  - AGENTS.md
  - .docpact/config.yaml
  - crates/**
  - supabase/migrations/**
  - docs/matrix-readiness-report-contract.md
  - docs/review-submit-fast-gate-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
lastReviewedAt: 2026-07-12
lastReviewedCommit: 855d48a543ef3d2670ea933432296bb4fc2e2ffe
lastReviewedNote: "Reviewed for Issue #116 public-plus-owner-draft v2 scope, LCIA source proof, coverage evidence, and solve binding."
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/provider-linking.md
  - docs/matrix-readiness-report-contract.md
  - docs/review-submit-fast-gate-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
  - docs/agents/repo-validation.md
---

# LCA API Contract (Snapshot-First)

本文档定义本项目当前可用的作业/结果契约，供 Edge Function 与前端共用。

## 1. 范围与原则

- 数值核心固定为 `M = I - A`，只解 `M x = y`。
- `snapshot_builder` 对 elementary flow 的 `B` 采用 `gross` 口径（`Input/Output` 均按原始 `amount` 入模，不做方向符号翻转）。
- 计算入口是异步任务；默认统一队列路径使用 `worker_jobs(worker_queue=solver)`，legacy `lca_jobs` + `pgmq` 仅保留为显式兼容/debug 路径。前端不直连队列。
- worker 连接池可通过 `DB_MAX_CONNECTIONS`、`DB_MIN_CONNECTIONS` 和 `DB_ACQUIRE_TIMEOUT_SECONDS` 调整；默认采用 `max_connections = 8`、`min_connections = 1`、`acquire_timeout = 30s`、`idle_timeout = 5min` 与 `max_lifetime = 30min`，以保证长时求解与 artifact 落盘阶段有稳定连接窗口。
- 主路径读取 `lca_snapshot_artifacts`（artifact-first），旧 `lca_*_entries` 仅兼容回退。
- 所有写操作由服务端（Edge Function / worker，`service_role`）执行。

## 2. 关键表与职责

- `lca_network_snapshots`: snapshot 元信息（含 `source_hash`）。
- `lca_snapshot_artifacts`: snapshot 矩阵 artifact 元信息（`snapshot-hdf5:v1`）。
- `worker_jobs`: canonical worker 生命周期表；solver 队列任务使用 `worker_queue=solver`，用于服务端任务中心、operator 查询、lease fencing、状态、错误、进度和 result projection。
- `lca_jobs`: optional retained LCA domain/history 兼容表，用于历史诊断和 legacy pgmq/debug 路径；统一 `worker_jobs` 路径不得要求该表存在。
- `lca_results`: 作业结果主表（仅 artifact 元数据 + diagnostics）。
- `lca_active_snapshots`: 各 scope 的当前生效 snapshot 指针。
- `lca_result_cache`: 请求级缓存/去重状态。
- `lca_factorization_registry`: 分解状态注册表（当前 schema 已就绪，运行时待接入）。

## 3. 作业类型与 payload

legacy `lca_jobs.job_type` 与 worker payload `type` 必须一致。`worker_jobs` 路径使用 `job_kind` 表达统一队列类型，并在 worker runtime 内部映射回同一组 legacy payload `type`，从而复用既有求解和 artifact 持久化逻辑。

支持类型：

- `prepare_factorization`
- `solve_one`
- `solve_batch`
- `solve_all_unit`
- `invalidate_factorization`
- `rebuild_factorization`

### 3.1 `prepare_factorization`

```json
{
  "type": "prepare_factorization",
  "job_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "print_level": 0.0
}
```

### 3.2 `solve_one`

```json
{
  "type": "solve_one",
  "job_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "rhs": [0.0, 1.0, 0.0],
  "solve": {
    "return_x": true,
    "return_g": true,
    "return_h": true
  },
  "print_level": 0.0
}
```

### 3.3 `solve_batch`

```json
{
  "type": "solve_batch",
  "job_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "rhs_batch": [
    [1.0, 0.0, 0.0],
    [0.0, 1.0, 0.0]
  ],
  "solve": {
    "return_x": true,
    "return_g": true,
    "return_h": true
  },
  "print_level": 0.0
}
```

### 3.4 `solve_all_unit`

```json
{
  "type": "solve_all_unit",
  "job_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "solve": {
    "return_x": false,
    "return_g": false,
    "return_h": true
  },
  "unit_batch_size": 128,
  "print_level": 0.0
}
```

说明：

- worker 会按 `unit_batch_size` 分块构造单位需求向量（每个 process 一条 `amount=1`）。
- 为控制结果体积，`solve_all_unit` 仅支持 `return_h=true` 且 `return_x/return_g=false`。

### 3.5 兼容字段

worker 反序列化时 `model_version` 仍可作为 `snapshot_id` 的别名（兼容旧请求）。新实现应只发 `snapshot_id`。

### 3.6 `worker_jobs` solver 队列映射

solver worker 默认使用 `SOLVER_QUEUE_BACKEND=worker-jobs` / `--queue-backend worker-jobs` 的 `public.worker_jobs` claim 模式。该模式只领取 `worker_queue=solver` 的 LCA solve jobs。`SOLVER_QUEUE_BACKEND=pgmq` / `--queue-backend pgmq` 仅用于 legacy 兼容/debug，且必须显式设置 `ALLOW_LEGACY_JOB_TABLE_BACKEND=true` 或传入 `--allow-legacy-job-table-backend`；生产 worker 应保持关闭。

| `worker_jobs.job_kind` | `payload_schema_version` | legacy payload `type` | result schema |
| --- | --- | --- | --- |
| `lca.solve_one` | `lca.solve_one.request.v1` / `lca.solve_one.request.v2` | `solve_one` | `lca.solve.result.v1` |
| `lca.solve_batch` | `lca.solve_batch.request.v1` | `solve_batch` | `lca.solve.result.v1` |
| `lca.solve_all_unit` | `lca.solve_all_unit.request.v1` / `lca.solve_all_unit.request.v2` | `solve_all_unit` | `lca.solve.result.v1` |
| `lca.build_snapshot` | `lca.build_snapshot.request.v1` / `lca.build_snapshot.request.v2` | `build_snapshot` | `lca.snapshot.result.v1` |
| `lca.contribution_path` | `lca.contribution_path.request.v1` / `lca.contribution_path.request.v2` | `analyze_contribution_path` | `lca.contribution_path.result.v1` |
| `lca.factorization_prepare` | `lca.factorization_prepare.request.v1` | `prepare_factorization` | `lca.factorization_prepare.result.v1` |
| `lcia_result.package_build` | `lcia_result.package_build.request.v1` | `lcia_result_package_build` | `lcia_result.package_build.result.v1` |

`worker_jobs.payload_json` may use the legacy snake_case fields above, or Edge-friendly camelCase aliases such as `lcaJobId`, `snapshotId`, `rhsBatch`, `unitBatchSize`, `processId`, `impactId`, `requestRoots`, `noLcia`, `buildId`, `requestedBy`, `inputManifest`, `inputManifestHash`, `lciaMethodSet`, and `defaultImpactCategory`. Payloads must still carry a valid `lcaJobId` / `job_id` compatibility UUID when the task writes `lca_results`、`lca_result_cache`、`lca_latest_all_unit_results` 或 `lca_factorization_registry` rows keyed by historical `job_id` columns. 这些 columns 不再要求 `public.lca_jobs` FK 或 parent row。

### 3.7 `public_plus_owner_draft` versioned calculation contract

`lca.build_snapshot.request.v2` is reserved for the private-incubation scope `public_plus_owner_draft`. It must carry the complete Edge-produced contract; the worker does not infer or default omitted fields:

- `all_states=false`, `process_states="100"`;
- `include_user_id=<authenticated actor>`, `include_user_state_codes="0"`;
- `include_user_unassigned_only=true`, `include_user_review_free_only=true`;
- `scope_manifest` using `lca.data_scope.manifest.v1` and its canonical `scope_manifest_sha256`;
- `lcia_method_factor_source` using `lca.method_factor_source.request.v1` with database relation `public.lciamethods`;
- `lcia_factor_coverage_contract` using `lcia.factor_coverage.contract.v1` and `missing_factor_semantics=incomplete_coverage_not_zero`;
- `no_lcia=false`.

The worker independently enforces the frozen predicate after queue decoding and again in the snapshot builder. Processes and flows are eligible only when `state_code=100`, or when they are actor-owned `state_code=0` rows with both `team_id` and `review_id` null. LCIA methods are eligible only when `state_code=100`, or actor-owned `state_code=0`; collaboration guards are not applicable to `lciamethods`. Public states `101..199`, foreign drafts, owner nonzero rows, team drafts, and review drafts are rejected. The Rust row recheck is mandatory even though SQL already applies the same predicate.

LCIA method rows and factors are loaded from `public.lciamethods`, not from the frontend's static method cache. A versioned build records three deterministic SHA-256 values in `lca.method_factor_source.snapshot.v1`: the selected source snapshot, method manifest, and raw factor manifest. The source fingerprint also includes this proof, so a snapshot with different method/factor content cannot be reused.

Factor coverage matches elementary exchanges by `(elementary_flow_uuid, direction)`. Counts are `matched`, `unmatched`, `invalid`, and `unsupported_direction`. `matched` means the exchange key is present in at least one selected LCIA method; this union coverage answers whether the inventory flow is characterized anywhere in the selected method set and does not claim that every method has a nonzero factor. Gaps are never represented as complete zero coverage. An incomplete build uploads `lcia-uncharacterized-jsonl:v1` with `elementary_flow_uuid`, `flow_version`, `direction`, `exchange_id`, `amount`, and `reason`, then records its URL, SHA-256, and exact record count.

`snapshot-index-v1.json` carries top-level `calculation_evidence` (`lca.calculation_evidence.v1`) with the exact scope hash, method/factor snapshot proof, and `lcia.factor_coverage.v1`. Complete coverage requires zero gap counts and a null evidence artifact. Incomplete coverage requires `coverage_status=incomplete_coverage`, an artifact, and `record_count = unmatched + invalid + unsupported_direction`.

For this scope, `lca.solve_one.request.v2`, `lca.solve_all_unit.request.v2`, and `lca.contribution_path.request.v2` must carry `calculation_evidence_binding` equal to the snapshot-index evidence. The worker rejects missing, malformed, or drifted bindings before factorization/solve. A v1 solve against a bound snapshot is rejected, so the contract cannot silently downgrade. Successful scoped results repeat `calculation_evidence` in `lca_results.diagnostics` and job diagnostics; numeric trial results with gaps remain explicitly marked `incomplete_coverage`.

`lcia_result.package_build` 不是普通求解 API 的用户请求类型，而是 data product manager command 创建的后台构建任务。payload 必须来自数据库/Edge 的 service-role command 边界，包含 `buildId`、`requestedBy`、published-only `inputManifest`、`inputManifestHash`、`coverageMode`、`eligibleInputCount`、`includedInputCount`、`lciaMethodSet` 和可选 `defaultImpactCategory`。worker 只接受 `inputManifest.processes` 中 `stateCode/state_code` 为 `100..199` 的已发布过程；不会纳入 draft data。

On success, the worker records a terminal `worker_jobs` result with:

- `result_json.lcaJobId`
- `result_json.workerJobId`
- `result_json.snapshotId`
- `result_json.resultId` when a `lca_results` row was produced
- `result_ref = {"domainSource":"worker_jobs","workerJobId":"<uuid>","lcaJobId":"<uuid>","result":{"table":"lca_results","id":"<uuid>"}}` for solve/result-producing jobs
- `diagnostics.lcaJob` as an optional legacy `lca_jobs` projection; if the table is absent, the projection reports `legacyTableMissing=true`

On success or failure, the worker links `lca_results`, `lca_result_cache`, `lca_latest_all_unit_results`, and `lca_factorization_registry` rows back to the canonical `worker_jobs.id` where those rows exist. If optional `lca_jobs` exists, the worker also backfills `lca_jobs.worker_job_id`; if it does not exist, the compatibility write is skipped. On failure, the worker records `worker_jobs.status=failed` with `error_code=solver_worker_job_failed` and updates `lca_result_cache` failed state where a cache row exists; retained `lca_jobs.status/diagnostics` are best-effort compatibility only.

For `lcia_result.package_build`, worker builds a published-only snapshot using the package `buildId` as the requested snapshot/result compatibility key, computes and persists the all-unit LCIA result artifact plus query artifact, then calls service-role RPC `public.cmd_lcia_result_package_mark_ready(...)`. Success `result_ref` uses `{"domainSource":"worker_jobs","workerJobId":"<uuid>","buildId":"<uuid>","package":{"table":"lcia_result_packages","id":"<uuid>"}}`; failures use package-specific error codes and do not update `lca_result_cache` or optional legacy `lca_jobs`.

## 4. 作业状态机

legacy `lca_jobs.status` 允许值：

- `queued`
- `running`
- `ready`
- `completed`
- `failed`
- `stale`

legacy pgmq/debug 路径语义：

- `prepare_factorization`: `queued -> running -> ready`。
- `solve_one` / `solve_batch` / `solve_all_unit`: `queued -> running -> completed`。
- `invalidate_factorization`: 通常直接 `completed`。
- 失败路径统一落 `failed`，错误详情在 `lca_jobs.diagnostics`。

`worker_jobs` 路径的外层生命周期是 `queued/stale -> running -> completed|failed|cancelled`。`phase` 使用 `solve_one`、`solve_batch`、`solve_all_unit`、`build_snapshot`、`analyze_contribution_path`、`prepare_factorization` 或 `lcia_result_package_build`，`progress` 仅作为任务中心提示，不替代 domain artifact 状态。

## 5. 结果契约

`lca_results` 一行对应一次完成的求解任务（通常 `solve_one`/`solve_batch`/`solve_all_unit`），当前为 **S3-only**：

- 不再存 inline `payload`
- 必须写入 `artifact_url` / `artifact_sha256` / `artifact_byte_size` / `artifact_format`
- 当前 `artifact_format = hdf5:v1`
- 附加 retention 字段：`expires_at` / `is_pinned`
- `diagnostics.calculation_evidence`：versioned scoped snapshots 必须为非空，并与 snapshot-index binding 完全一致；legacy snapshots 为 `null`

`snapshot` artifact 当前格式：`snapshot-hdf5:v1`。

snapshot coverage diagnostics 会暴露 snapshot 构建阶段的 provider linking 和矩阵写入质量统计，用于解释供应链连接完整性。当前 coverage schema 为 `snapshot_coverage.v2`，在保留 `provider_decision_diagnostics` 兼容字段的同时，新增按用途分组的 summary：

Provider-link 的运行时决策顺序、默认 provider rule、candidate eligibility 和 provider diagnostics 维护在 `docs/provider-linking.md`。本文档只定义 worker/API 消费这些 coverage 与 artifact 字段的契约边界。

- `candidate_summary`：eligible provider candidate 数量分布。自动 provider linking 默认只把 reference output 计入 eligible candidate；同 flow 的非 reference output 通过 `provider_decision_diagnostics.candidate_eligibility_counts` 和逐条 candidate evidence 解释为 rejected diagnostics。
- `resolution_summary`：resolved strategy 与 unresolved reason 分布。
- `geography_summary`：地理层级、strategy × geography tier、supply-region anchor 来源、exchange location 覆盖情况和 location 粒度分布。
- `volume_weight_summary`：基于 `annualSupplyOrProductionVolume` 的权重数据可用性与 fallback-to-one 情况。
- `gap_summary`：no-provider gap 的 top flows 与 top processes。

`provider_decision_diagnostics` 中与 reference-output eligibility 相关的字段包括：

- `candidate_eligibility_counts`：provider output evidence 按 `accepted_reference_output`、`rejected_non_reference_output`、`unknown` 统计。
- `rejected_non_reference_output_count`：同 flow 但未进入自动 provider linking 的 non-reference output 数量。
- `unresolved_reason_counts.rejected_non_reference_only`：某 input flow 只有 non-reference same-flow outputs、没有 eligible reference-output provider。

`geography_summary` 中的 canonical 字段包括：

- `tier_counts`：所有 resolved provider decision 的地理匹配层级总计。
- `tier_counts_by_strategy`：按 resolved strategy 拆分的地理匹配层级，用于判断 `unique_provider` 或 `split_by_process_volume` 各自的本地匹配与地理 fallback 分布。
- `supply_region_source_counts`：供应区域 anchor 来源总计，典型 key 为 `exchange_location`、`consumer_process_location`、`unspecified`。
- `supply_region_source_counts_by_strategy`：按 resolved strategy 拆分的供应区域 anchor 来源，用于判断某个 link 策略实际使用 exchange-level location 还是 consumer process location。
- `exchange_location_present_count`：input exchange 中存在 exchange-level `location` 的总数。
- `exchange_location_present_count_by_strategy`：按 resolved strategy 拆分的 exchange-level `location` 覆盖数。
- `requested_location_granularity_counts`：目标供应区域粒度总计，例如 `subnational`、`country`、`region`、`global`、`unspecified`。
- `requested_location_granularity_counts_by_strategy`：按 resolved strategy 拆分的目标供应区域粒度。

`build_snapshot` job 运行和完成时，worker 会在 `worker_jobs.diagnostics/result_json` 中记录全局构建并发锁与构建耗时信息；如果 optional `lca_jobs` 存在，也会 best-effort 写入 `lca_jobs.diagnostics.build_snapshot_lock` 与 `build_timing_sec`。这些字段属于诊断信息，不改变 job payload、状态机或 result artifact 主契约。

### 5.1 Matrix-readiness verification report

自动化 LCA 数据研制使用 worker 侧的 matrix-readiness gate 判断写入后的数据是否可被行业级计算链路接受。该 gate 不决定是否创建 process/flow，也不替代 CLI schema/ruleset gate；它只验证 provider closure、snapshot graph readiness 和 solver/LCIA compute stability。

可调用入口：

```bash
cargo run -p solver-worker --bin matrix_readiness -- \
  --input matrix-readiness-input.json \
  --out matrix-readiness-report.json
```

fresh `snapshot_builder` run 也会在 `report_dir` 下尝试写出 `matrix-readiness-<snapshot_id>.json`；该本地文件受 `SNAPSHOT_REPORT_*` retention 和低磁盘 guard 保护，跳过本地写入不改变 snapshot artifact 或 report schema。输入 `matrix_readiness_input.v1` 包含：

- `coverage`: snapshot coverage report。
- `payload`: `ModelSparseData` sparse payload。
- `compiled_graph`（可选）：fresh build 时包含逐边 provider decision、candidate providers、allocation weights、geography tier 和 failure reason。没有该字段时仍可验证 coverage/compute，但 provider evidence 会降级为空。
- `policy`: provider write percentage、unmatched / unresolved provider 容忍度、singular risk、LCIA factor、factorization 和 negative LCIA anomaly 策略。

输出 `matrix_readiness_report.v1` 包含：

- `status`: `passed` 或 `failed`。
- `next_action`: 例如 `publish_ready`、`repair_provider_closure_then_recheck`、`repair_compute_stability_then_recheck`。
- `metrics.provider_closure`: input edge、written edge、unmatched provider、multi-provider unresolved 和 equal-fallback 统计。
- `metrics.graph_readiness`: process/flow/impact scale、A/B/C/M nnz、reference/allocation closure 和 singular risk。
- `metrics.compute_stability`: factorization readiness、matrix validation report、sample unit solves、non-finite count 和 negative LCIA count。
- `provider_evidence`: 每条 input edge 的 consumer、flow、candidate providers、resolution strategy、failure reason、allocation weights、ambiguity 和 confidence。
- `findings` / `blockers`: machine-readable issue codes、severity、message 和 detail payload。

当前 matrix-readiness 只通过 worker CLI 与 `snapshot_builder` report artifact 暴露；本节不表示 Edge/API 已提供 HTTP 调用入口。稳定 code、`blockers` / `findings` / `next_action` 规则、policy 默认值和调用方消费约束由 `docs/matrix-readiness-report-contract.md` 维护。

Foundry、CLI 或 Edge adapter 只能消费该 report 的 `status`、`next_action`、`blockers`、`metrics` 和 `provider_evidence`；不应在外部复制 worker runtime 的 provider resolution、singular-risk 或 UMFPACK readiness 规则。

### 5.2 Review-submit fast gate report

dataset revision 提交审核前使用 worker 侧 `review_submit_gate` 判断当前 revision 是否可进入审核流程。该 gate 输出二元结果：`passed` 或 `blocked`，不产生 `manual_review_required` 状态。

文件输入/输出入口：

```bash
cargo run -p solver-worker --bin review_submit_gate -- \
  --input review-submit-gate-input.json \
  --out review-submit-gate-report.json
```

数据库运行时入口：

```bash
cargo run -p solver-worker --bin review_submit_gate_runner -- --once
```

worker_jobs 运行时入口：

```bash
cargo run -p solver-worker --bin review_submit_gate_runner -- \
  --worker-jobs \
  --once
```

Edge/API 不直接运行数值 gate。legacy 路径中，Edge 通过数据库 RPC 创建、读取或 rerun `dataset_review_submit_gate_runs`；worker runner 领取 queued gate run，默认构造 no-LCIA review-submit baseline + draft overlay snapshot，执行 `review_submit_gate`，再通过 `cmd_dataset_review_submit_gate_record_result` 写回 `passed`、`blocked` 或 `error`。

新 `worker_jobs` 路径中，Edge 只 enqueue `job_kind=review_submit.gate`，worker 使用 `worker_claim_jobs` 领取、按阶段 heartbeat、执行同一 gate，然后用 `worker_record_job_result` 写回：

- `completed`：gate passed，result 中包含 `calculatorReport` 与权威 `datasetRevision.revisionChecksum`。
- `blocked`：gate blocked，`blocker_codes` 来自 report blockers，`resolution_scope=user`，`retryable=true`。
- `failed`：runner、S3、DB 或暂不支持的数据集类型错误，写入 operator diagnostics。

`worker_jobs` 模式不调用 final submit，也不修改 review-submit domain 状态；gate passed 后的 durable coordinator 属于 Edge / database 层。

输入 `review_submit_gate_input.v1` 复用 snapshot coverage、`ModelSparseData` sparse payload、compiled provider graph，并可附加 dataset revision checksum、target process indices 和 process/exchange scan records。输出 `review_submit_gate_report.v1` 包含：

- `status`: `passed` 或 `blocked`。
- `policy`: 默认 profile 为 `review_submit_fast.v1`。
- `metrics`: revision、process_scan、provider_scan、sparse_scan 和 targeted probe 统计。
- `blockers`: 提交审核硬失败 code、message 和 detail payload。

该 gate 先执行 revision/process/provider/flow/sparse 结构检查；只有没有结构 blocker 时才执行 sparse factorization readiness 与 targeted RHS solve。默认 targeted probe 只验证 `x/g` 稳定性，不计算 LCIA `h`。它不 materialize inverse，也不要求 full `solve_all_unit`。

稳定 blocker code、policy 默认值、快速验证顺序和 caller consumption 约束由 `docs/review-submit-fast-gate-contract.md` 维护。Edge 或 Next 在提交审核链路中应消费 DB gate result 里的 status、blockingReasons 和 calculatorReport，不应直接把 `matrix_readiness_report.v1` 的 blocker 当成提交审核结论。

## 6. 幂等与请求缓存（建议约束）

- `worker_jobs.idempotency_key` / `worker_jobs.request_hash`：同一业务请求重试时复用，避免重复创建 canonical worker job。
- `lca_results.job_id` / `lca_result_cache.job_id`：仅为历史 compatibility UUID，不要求 `lca_jobs` parent row。
- `lca_result_cache`：
  - 唯一键 `(scope, snapshot_id, request_key)`
  - 状态 `pending/running/ready/failed/stale`
  - 命中时直接返回已有 `result_id` 或进行中的 `job_id`
  - 当前实现中：
    - Edge 入队时写 `pending`
    - worker 开始求解时写 `running`
    - worker 成功落结果后写 `ready + result_id`
    - worker 失败时写 `failed + error_code/error_message`

## 7. 安全与权限边界

- `lca_*` 表已启用 RLS。
- `anon` 无权限。
- `authenticated` 仅可读“自己的 `lca_jobs` + 关联 `lca_results`”。
- 任何 enqueue / insert / update 必须经服务端 `service_role`。

## 8. 最小 SQL 约定（服务端）

legacy pgmq 路径：

1. 插入 job 行（`status=queued`，带 payload）。
2. 调 `public.lca_enqueue_job(text, jsonb)` RPC 投递消息（函数内部调用 `pgmq.send`）。
3. 返回 `job_id` 给调用方。
4. worker 消费后更新 `lca_jobs` 并写 `lca_results`。

统一 `worker_jobs` 路径：

1. 先创建或复用 `lca_result_cache` domain row，并生成 `lcaJobId` compatibility UUID；不要求创建 `lca_jobs` row。
2. 调 `public.worker_enqueue_job(...)` 创建 `job_kind=lca.*`、`worker_queue=solver` 的 job，payload 带 `lcaJobId` 和标准化求解参数。
3. 返回 `workerJobId` 与 `lcaJobId` 给 Edge / Next projection。
4. worker 使用 `worker_claim_jobs('solver', ...)` claim、heartbeat，并通过 `worker_record_job_result(...)` 写 canonical 终态；同时维护 `lca_results`、`lca_result_cache` domain/cache metadata，并回填 `worker_job_id`。optional `lca_jobs` 只做 best-effort compatibility update。

不建议前端直接调用 `pgmq.send`、直接写 `lca_jobs` 或直接写 `worker_jobs`。
