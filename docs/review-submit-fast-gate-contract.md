---
title: Review Submit Fast Gate Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: calculator
language: zh-CN
whenToUse:
  - 当 dataset revision 提交审核前需要 calculator 侧数值稳定性快速 gate 时
  - 当 Edge、Foundry 或 Next 需要消费 review-submit gate report 时
  - 当 review_submit_gate 的 schema、policy、blocker 或 probe 规则变化时
whenToUpdate:
  - 当 crates/solver-worker/src/review_submit_gate.rs 的 report schema、policy 或 blocker code 变化时
  - 当 crates/solver-worker/src/bin/review_submit_gate.rs 的 CLI contract 变化时
  - 当 crates/solver-worker/src/bin/review_submit_gate_runner.rs 的 DB runner contract 变化时
  - 当提交审核前的 calculator-owned gate 与 matrix-readiness 的边界变化时
checkPaths:
  - docs/review-submit-fast-gate-contract.md
  - crates/solver-worker/src/review_submit_gate.rs
  - crates/solver-worker/src/bin/review_submit_gate.rs
  - crates/solver-worker/src/review_submit_gate_runner.rs
  - crates/solver-worker/src/bin/review_submit_gate_runner.rs
  - crates/solver-worker/src/readiness.rs
  - crates/solver-worker/src/compiled_graph.rs
  - docs/lca-api-contract.md
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
lastReviewedAt: 2026-05-26
lastReviewedCommit: 877f8318a1716786beb32bc86ac208c57a9168d9
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
---

# Review Submit Fast Gate Contract

`review_submit_gate` 是 calculator 侧的 dataset revision 提交审核前快速 gate。它输出二元结果：`passed` 或 `blocked`。

该 gate 不属于 Edge HTTP API，也不属于 Next UI 行为。Edge、Foundry 和 Next 可以消费 report，但不应复制 calculator 的 provider、sparse-structure、factorization probe 或 target solve 判断逻辑。

## 调用入口

calculator 暴露两个入口：

- `review_submit_gate`：纯文件输入/输出 CLI，适合 fixture、CI、Foundry 或手工诊断。
- `review_submit_gate_runner`：数据库运行时 runner，负责领取持久化 gate run、构造 snapshot、执行 calculator gate，并把结果写回数据库 RPC。

纯文件 CLI：

```bash
cargo run -p solver-worker --bin review_submit_gate -- \
  --input review-submit-gate-input.json \
  --out review-submit-gate-report.json
```

CLI 默认总是写出 report。调用方需要命令行级失败时，可以传 `--fail-on-blocked`，blocked report 会以 exit code `2` 返回。

DB runner：

```bash
cargo run -p solver-worker --bin review_submit_gate_runner -- --once
```

runner 读取 `DATABASE_URL` / `CONN` 与 S3 artifact 环境变量，直接访问 `public.dataset_review_submit_gate_runs`。它只领取：

- `policy_profile = review_submit_fast.v1`
- `report_schema_version = review_submit_gate_report.v1`
- `status = queued`，以及超过 `REVIEW_SUBMIT_GATE_STALE_RUNNING_SECONDS` 的 stale `running` 记录

领取后状态变为 `running`。执行完成后，runner 调用 `public.cmd_dataset_review_submit_gate_record_result` 写入 `passed`、`blocked` 或 `error`。`--once` 用于一次性处理一条或空转退出；常驻模式会按 `REVIEW_SUBMIT_GATE_POLL_MS` 轮询。

## 输入

输入 schema version 为 `review_submit_gate_input.v1`，核心字段为：

- `dataset_revision_id`: 被提交审核的 dataset revision。
- `expected_revision_checksum` / `actual_revision_checksum`: 用于判断 report 是否绑定当前 revision。
- `coverage`: snapshot coverage report。
- `payload`: `ModelSparseData` sparse payload。
- `compiled_graph`: provider decision、flow kind、technosphere/biosphere edge 与 process metadata。
- `target_process_indices`: 本次提交审核必须覆盖的 target / changed process index。
- `process_records`: calculator 可解释的 process/exchange scan record，用于 reference、allocation、duplicate fingerprint 和 service-loop 快速检查。
- `policy`: `review_submit_fast.v1` policy surface。

`process_records` 是提交审核快速 gate 的可选增强输入。没有它时，gate 仍可根据 `coverage`、`payload` 与 `compiled_graph` 执行 provider、sparse structure 和 probe 检查，但无法发现所有 JSON/process-level 历史事故模式。

DB runner 当前支持 `dataset_table = processes`。它使用 gate run 的 `dataset_id + dataset_version` 作为 request root，使用 `requested_by` 作为 snapshot builder 的 `include_user_id`，并以 gate run ID 作为请求 snapshot ID。runner 从 `processes.json_ordered` 计算稳定 SHA-256，与 gate run 的 `revision_checksum` 对比；不匹配会形成 `revision_report_stale` blocker。

当前 snapshot artifact 持久化 `coverage + payload + config`，不持久化 `compiled_graph`。因此 DB runner 的 report 会消费 artifact 中的 coverage/payload，并补充单个提交 process 的 `process_records`；compiled-graph 级 flow semantic examples 仍由文件 CLI / library input 保留为增强能力。

## 输出

输出 schema version 为 `review_submit_gate_report.v1`，核心字段为：

- `status`: `passed` 或 `blocked`。
- `policy`: 实际使用的 review-submit policy。
- `metrics.revision`: revision checksum freshness 结果。
- `metrics.process_scan`: process record、reference、exchange amount、allocation、duplicate fingerprint 和 service-loop 统计。
- `metrics.provider_scan`: provider missing、unresolved、equal fallback、allocation conservation 和 volume evidence 统计。
- `metrics.sparse_scan`: diagonal、duplicate column 和 flow/LCIA semantic 统计。
- `metrics.probe`: sparse factorization 与 targeted RHS solve probe 结果。
- `blockers`: stable blocker code、message 和 detail payload。

调用方必须以 `status` 和 `blockers[].code` 为准。`metrics` 用于展示、诊断和后续数据修复，不应被外部调用方重新解释成另一套 gate 规则。

DB runner 写回数据库时：

- `passed`：`blockingReasons = []`，`calculatorReport` 为 `review_submit_gate_report.v1`。
- `blocked`：`blockingReasons` 由 `report.blockers` 直接映射，`calculatorReport` 为完整 report。
- `error`：表示 runner、snapshot builder、artifact、DB 可见性或暂不支持的数据集类型导致 calculator 没有产出 passed/blocked 结论；`blockingReasons` 至少包含一个 runtime blocker，`calculatorReport.status = error`。

## Policy 默认值

默认 profile 为 `review_submit_fast.v1`。

默认策略：

- 要求 `expected_revision_checksum == actual_revision_checksum`。
- `allowed_scope_states = [0] + 100..=199`；`0` 表示提交审核前 draft root，`100..=199` 用于已审核 / 可用依赖数据兼容；`20` 等审核中状态不允许，仍会触发 `invalid_scope_state`。
- 阻断 provider equal fallback。
- 阻断 provider volume fallback evidence。
- provider missing 只记录在 `metrics.provider_scan.provider_missing_count`，不作为提交审核 blocker。
- impact-ready 提交要求 LCIA factors。
- 要求 target process probe。
- target probe 默认最多覆盖 `32` 个 process。
- 默认执行 sparse factorization probe 和 targeted RHS solve。
- 不执行完整矩阵求逆，不要求 full `solve_all_unit`。

## Blockers

`blockers` 表示提交审核硬失败。当前稳定 code 如下：

| Code | 触发条件 | 主要修复方向 |
| --- | --- | --- |
| `revision_report_stale` | revision checksum 缺失或不匹配 | 基于当前 revision 重跑 gate |
| `invalid_scope_state` | process record 的 `state_code` 不在 policy 允许范围 | 修正计算 scope 或 process lifecycle state |
| `duplicate_process_version` | 同一 process ID 多个版本同时进入 gate scope | 去重或明确只纳入目标版本 |
| `missing_or_zero_reference` | quantitative reference 缺失、指向不存在 exchange、amount 为 0，或 coverage 有 reference failure | 修复 reference exchange 和非零 reference amount |
| `invalid_exchange_amount` | exchange amount 缺失、不可解析、带非法文本、NaN 或 Infinity | 修复 exchange amount / 单位转换 |
| `invalid_allocation_fraction` | allocation fraction 不可解析、带 `%` 或超出允许数值范围 | 统一 allocation fraction 表达 |
| `duplicate_exchange_fingerprint` | 不同 process 的 flow/direction/amount fingerprint 完全一致 | 合并重复 process 或补充可区分 exchange |
| `service_loop_detected` | 同一 process 中同一 flow 的 input/output amount 相同或近似相同 | 修正自供给、循环或拆分 process |
| `provider_unresolved` | multi-provider input edge 未解析 | 补 provider evidence 或缩小候选集 |
| `provider_equal_fallback` | provider resolution 使用 equal fallback 且 policy 阻断 | 补 annual volume / evidence，避免等权兜底 |
| `provider_allocation_not_conserved` | provider allocation weight 非有限、负数、为空或 sum 不为 1 | 修复 provider allocation |
| `provider_volume_evidence_invalid` | provider volume evidence fallback-to-one 或缺失超过 policy 容忍 | 补充可比较 annual volume evidence |
| `flow_lcia_semantic_mismatch` | product/elementary flow 或 LCIA factor 语义错配 | 修复 flow kind、biosphere edge 或 LCIA factor mapping |
| `lcia_factor_missing_for_impact_submit` | impact-ready 提交要求 LCIA factors，但 `c_nnz = 0` | 补齐 LCIA factors 或改用非 impact 提交策略 |
| `sparse_matrix_zero_or_near_zero_diagonal` | `M = I - A` 对角线为 0 / near-zero，或 payload process count 无效 | 修复自环、reference 或 matrix structure |
| `duplicate_sparse_columns` | `M = I - A` 存在重复 sparse column signature | 排查重复结构和线性相关 process |
| `singular_risk_medium_or_high` | coverage singular risk 为 `medium` 或 `high` | 修复 matrix structure 后重跑 |
| `target_process_not_covered_by_probe` | target process list 缺失、越界或超过 probe limit | 明确 submitted / changed process list，或拆分 gate |
| `factorization_probe_failed` | sparse factorization readiness probe 失败 | 修复 matrix structure 后重跑 |
| `target_probe_non_finite_result` | targeted RHS solve 失败或产生 NaN / Infinity | 修复 compute stability / flow / LCIA 数据 |

## 快速验证顺序

Gate 按便宜到昂贵的顺序执行：

1. revision freshness。
2. process record scan：scope state、reference、exchange amount、allocation、duplicate fingerprint、service-loop。
3. provider scan：missing metric、unresolved、equal fallback、allocation conservation、volume evidence。
4. flow / LCIA semantic scan。
5. sparse structure scan：diagonal、duplicate sparse column、singular risk。
6. target process coverage check。
7. 仅当以上没有 blocker 时，执行 sparse factorization readiness 与 targeted RHS solve。

这个顺序让历史事故模式在 full solve 之前被挡住。它不会 materialize inverse，也不会默认跑 full `solve_all_unit`。

## 与 Matrix Readiness 的关系

`matrix_readiness` 面向 snapshot / matrix report artifact，回答“这个 snapshot 是否具备 provider closure、graph readiness 和基本 compute stability”。

`review_submit_gate` 面向 dataset revision 提交审核，回答“这个 revision 是否允许进入审核流程”。

两者共享 coverage、payload、compiled provider evidence 和 sparse solver 语义，但 blocker code 不相同。Edge 和 Next 在提交审核流程中应消费 `review_submit_gate_report.v1`，不应直接把 `matrix_readiness_report.v1` 的 blocker 当成提交审核结论。
