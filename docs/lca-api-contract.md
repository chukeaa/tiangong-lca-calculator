---
title: LCA API Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: calculator
language: zh-CN
whenToUse:
  - 当你需要共享的 jobs/results/payload/status 契约时
  - 当 edge-functions 或前端的集成行为依赖 calculator runtime 输出语义时
whenToUpdate:
  - 当 job payload、状态机、结果 artifact、幂等规则或服务端权限边界变化时
checkPaths:
  - docs/lca-api-contract.md
  - AGENTS.md
  - .docpact/config.yaml
  - crates/**
  - supabase/migrations/**
  - docs/matrix-readiness-report-contract.md
  - docs/review-submit-fast-gate-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
lastReviewedAt: 2026-05-26
lastReviewedCommit: 877f8318a1716786beb32bc86ac208c57a9168d9
related:
  - AGENTS.md
  - .docpact/config.yaml
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
- 计算入口是异步 `lca_jobs` + `pgmq`，不走前端直连队列。
- worker 连接池可通过 `DB_MAX_CONNECTIONS`、`DB_MIN_CONNECTIONS` 和 `DB_ACQUIRE_TIMEOUT_SECONDS` 调整；默认采用 `max_connections = 8`、`min_connections = 1`、`acquire_timeout = 30s`、`idle_timeout = 5min` 与 `max_lifetime = 30min`，以保证长时求解与 artifact 落盘阶段有稳定连接窗口。
- 主路径读取 `lca_snapshot_artifacts`（artifact-first），旧 `lca_*_entries` 仅兼容回退。
- 所有写操作由服务端（Edge Function / worker，`service_role`）执行。

## 2. 关键表与职责

- `lca_network_snapshots`: snapshot 元信息（含 `source_hash`）。
- `lca_snapshot_artifacts`: snapshot 矩阵 artifact 元信息（`snapshot-hdf5:v1`）。
- `lca_jobs`: 异步作业主表。
- `lca_results`: 作业结果主表（仅 artifact 元数据 + diagnostics）。
- `lca_active_snapshots`: 各 scope 的当前生效 snapshot 指针。
- `lca_result_cache`: 请求级缓存/去重状态。
- `lca_factorization_registry`: 分解状态注册表（当前 schema 已就绪，运行时待接入）。

## 3. 作业类型与 payload

`lca_jobs.job_type` 与 worker payload `type` 必须一致。

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

## 4. 作业状态机

`lca_jobs.status` 允许值：

- `queued`
- `running`
- `ready`
- `completed`
- `failed`
- `stale`

当前语义：

- `prepare_factorization`: `queued -> running -> ready`。
- `solve_one` / `solve_batch` / `solve_all_unit`: `queued -> running -> completed`。
- `invalidate_factorization`: 通常直接 `completed`。
- 失败路径统一落 `failed`，错误详情在 `lca_jobs.diagnostics`。

## 5. 结果契约

`lca_results` 一行对应一次完成的求解任务（通常 `solve_one`/`solve_batch`/`solve_all_unit`），当前为 **S3-only**：

- 不再存 inline `payload`
- 必须写入 `artifact_url` / `artifact_sha256` / `artifact_byte_size` / `artifact_format`
- 当前 `artifact_format = hdf5:v1`
- 附加 retention 字段：`expires_at` / `is_pinned`

`snapshot` artifact 当前格式：`snapshot-hdf5:v1`。

snapshot coverage diagnostics 会暴露 snapshot 构建阶段的 provider linking 和矩阵写入质量统计，用于解释供应链连接完整性。当前 coverage schema 为 `snapshot_coverage.v2`，在保留 `provider_decision_diagnostics` 兼容字段的同时，新增按用途分组的 summary：

- `candidate_summary`：provider candidate 数量分布。
- `resolution_summary`：resolved strategy 与 unresolved reason 分布。
- `geography_summary`：地理层级、strategy × geography tier、supply-region anchor 来源、exchange location 覆盖情况和 location 粒度分布。
- `volume_weight_summary`：基于 `annualSupplyOrProductionVolume` 的权重数据可用性与 fallback-to-one 情况。
- `gap_summary`：no-provider gap 的 top flows 与 top processes。

`geography_summary` 中的 canonical 字段包括：

- `tier_counts`：所有 resolved provider decision 的地理匹配层级总计。
- `tier_counts_by_strategy`：按 resolved strategy 拆分的地理匹配层级，用于判断 `unique_provider` 或 `split_by_process_volume` 各自的本地匹配与地理 fallback 分布。
- `supply_region_source_counts`：供应区域 anchor 来源总计，典型 key 为 `exchange_location`、`consumer_process_location`、`unspecified`。
- `supply_region_source_counts_by_strategy`：按 resolved strategy 拆分的供应区域 anchor 来源，用于判断某个 link 策略实际使用 exchange-level location 还是 consumer process location。
- `exchange_location_present_count`：input exchange 中存在 exchange-level `location` 的总数。
- `exchange_location_present_count_by_strategy`：按 resolved strategy 拆分的 exchange-level `location` 覆盖数。
- `requested_location_granularity_counts`：目标供应区域粒度总计，例如 `subnational`、`country`、`region`、`global`、`unspecified`。
- `requested_location_granularity_counts_by_strategy`：按 resolved strategy 拆分的目标供应区域粒度。

`build_snapshot` job 运行和完成时，`lca_jobs.diagnostics.build_snapshot_lock` 会记录全局构建并发锁信息，包括 `strategy`、`max_concurrency`、`slot`、`waiting` 与 `wait_sec`；当前 `strategy` 为 `postgres_transaction_advisory_lock`。完成时 `lca_jobs.diagnostics.build_timing_sec` 会记录 snapshot builder 主要阶段耗时。这些字段属于诊断信息，不改变 job payload、状态机或 result artifact 主契约。

### 5.1 Matrix-readiness verification report

自动化 LCA 数据研制使用 calculator 侧的 matrix-readiness gate 判断写入后的数据是否可被行业级计算链路接受。该 gate 不决定是否创建 process/flow，也不替代 CLI schema/ruleset gate；它只验证 provider closure、snapshot graph readiness 和 solver/LCIA compute stability。

可调用入口：

```bash
cargo run -p solver-worker --bin matrix_readiness -- \
  --input matrix-readiness-input.json \
  --out matrix-readiness-report.json
```

fresh `snapshot_builder` run 也会在 `report_dir` 下写出 `matrix-readiness-<snapshot_id>.json`。输入 `matrix_readiness_input.v1` 包含：

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

当前 matrix-readiness 只通过 calculator CLI 与 `snapshot_builder` report artifact 暴露；本节不表示 Edge/API 已提供 HTTP 调用入口。稳定 code、`blockers` / `findings` / `next_action` 规则、policy 默认值和调用方消费约束由 `docs/matrix-readiness-report-contract.md` 维护。

Foundry、CLI 或 Edge adapter 只能消费该 report 的 `status`、`next_action`、`blockers`、`metrics` 和 `provider_evidence`；不应在外部复制 calculator 的 provider resolution、singular-risk 或 UMFPACK readiness 规则。

### 5.2 Review-submit fast gate report

dataset revision 提交审核前使用 calculator 侧 `review_submit_gate` 判断当前 revision 是否可进入审核流程。该 gate 输出二元结果：`passed` 或 `blocked`，不产生 `manual_review_required` 状态。

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

Edge/API 不直接运行数值 gate。Edge 通过数据库 RPC 创建、读取或 rerun `dataset_review_submit_gate_runs`；calculator runner 领取 queued gate run，构造 request-root snapshot，执行 `review_submit_gate`，再通过 `cmd_dataset_review_submit_gate_record_result` 写回 `passed`、`blocked` 或 `error`。

输入 `review_submit_gate_input.v1` 复用 snapshot coverage、`ModelSparseData` sparse payload、compiled provider graph，并可附加 dataset revision checksum、target process indices 和 process/exchange scan records。输出 `review_submit_gate_report.v1` 包含：

- `status`: `passed` 或 `blocked`。
- `policy`: 默认 profile 为 `review_submit_fast.v1`。
- `metrics`: revision、process_scan、provider_scan、sparse_scan 和 targeted probe 统计。
- `blockers`: 提交审核硬失败 code、message 和 detail payload。

该 gate 先执行 revision/process/provider/flow/sparse 结构检查；只有没有结构 blocker 时才执行 sparse factorization readiness 与 targeted RHS solve。它不 materialize inverse，也不要求 full `solve_all_unit`。

稳定 blocker code、policy 默认值、快速验证顺序和 caller consumption 约束由 `docs/review-submit-fast-gate-contract.md` 维护。Edge 或 Next 在提交审核链路中应消费 DB gate result 里的 status、blockingReasons 和 calculatorReport，不应直接把 `matrix_readiness_report.v1` 的 blocker 当成提交审核结论。

## 6. 幂等与请求缓存（建议约束）

- `lca_jobs.idempotency_key`：同一业务请求重试时复用，避免重复创建 job。
- `lca_jobs.request_key`：可由 `snapshot_id + 需求向量 + solve选项 + 版本` 归一化哈希得到。
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

1. 插入 job 行（`status=queued`，带 payload）。
2. 调 `public.lca_enqueue_job(text, jsonb)` RPC 投递消息（函数内部调用 `pgmq.send`）。
3. 返回 `job_id` 给调用方。
4. worker 消费后更新 `lca_jobs` 并写 `lca_results`。

不建议前端直接调用 `pgmq.send` 或直接写 `lca_jobs`。
