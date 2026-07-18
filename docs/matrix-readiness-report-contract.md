---
title: Matrix Readiness Report Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: zh-CN
whenToUse:
  - 当你需要消费或维护 matrix_readiness CLI 输出时
  - 当 snapshot_builder 的 matrix-readiness report artifact 语义变化时
  - 当 Foundry、CLI 或 Edge adapter 需要解释 readiness blockers、findings 或 next_action 时
whenToUpdate:
  - 当 crates/solver-worker/src/readiness.rs 的 report schema、policy、blocker、finding 或 next_action 规则变化时
  - 当 crates/solver-worker/src/bin/matrix_readiness.rs 的 CLI contract 变化时
  - 当 snapshot_builder 暴露 matrix-readiness artifact 的方式变化时
checkPaths:
  - docs/matrix-readiness-report-contract.md
  - docs/provider-linking.md
  - crates/solver-worker/src/readiness.rs
  - crates/solver-worker/src/bin/matrix_readiness.rs
  - crates/solver-worker/src/bin/snapshot_builder.rs
  - crates/solver-worker/src/compiled_graph.rs
  - docs/lca-api-contract.md
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
lastReviewedAt: 2026-07-17
lastReviewedCommit: 6d61068445ebfad0fb3e07469f6d0468d692574a
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/provider-linking.md
  - docs/lca-api-contract.md
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
---

# Matrix Readiness Report Contract

`matrix_readiness` 是 calculator 侧的 report/artifact contract，不是 Edge HTTP API contract。

当前暴露方式只有两类：

- CLI: `cargo run -p solver-worker --bin matrix_readiness -- --input <input.json> --out <report.json>`
- fresh `snapshot_builder` run 在 `report_dir` 下尝试写出的 `matrix-readiness-<snapshot_id>.json`

`snapshot_builder` 的本地 report 写入是 guarded optional artifact：默认会按 `SNAPSHOT_REPORT_RETENTION_DAYS` / `SNAPSHOT_REPORT_MAX_FILES` 清理 `reports/snapshot-coverage`，并在 `SNAPSHOT_REPORT_MODE=guarded` 且可用磁盘空间低于 `SNAPSHOT_REPORT_MIN_FREE_BYTES` 时跳过本地 report 写入。跳过本地文件不改变 matrix-readiness report schema，也不代表 snapshot artifact 或对象存储写入失败。

Edge、Foundry、CLI 或其他调用方可以消费 report 字段，但不应在外部复制 calculator 的 provider resolution、singular-risk、LCIA 或 UMFPACK readiness 判断逻辑。

Provider-link 的运行时决策顺序由 `docs/provider-linking.md` 维护。本文档只定义 matrix-readiness 如何消费 snapshot coverage、compiled graph 和 provider decisions；provider rule 顺序变化本身不改变 report schema，除非新增或删除 report 字段、blocker、finding 或 policy。

## 输入

输入 schema version 为 `matrix_readiness_input.v1`，核心字段为：

- `coverage`: snapshot coverage report。
- `payload`: `ModelSparseData` sparse payload。
- `compiled_graph`（可选）：fresh build 时包含逐边 provider decision、candidate providers、candidate reference-output eligibility、allocation weights、geography tier 和 failure reason。没有该字段时仍可验证 coverage/compute，并可根据 coverage 产生通用 provider closure blocker；但 `provider_evidence` 会降级为空，也无法产生带 candidate evidence 的 `provider_closure_reference_provider_missing` 专项 blocker。
- `policy`: provider write percentage、unmatched / unresolved provider 容忍度、singular risk、LCIA factor、factorization 和 negative LCIA anomaly 策略。

## 输出

输出 schema version 为 `matrix_readiness_report.v1`，核心字段为：

- `status`: `passed` 或 `failed`。
- `next_action`: calculator 给调用方的粗粒度下一步建议。
- `metrics.provider_closure`: input edge、written edge、unmatched provider、multi-provider unresolved 和 equal-fallback 统计。
- `metrics.graph_readiness`: process/flow/impact scale、A/B/C/M nnz、reference/allocation closure、两个 legacy allocation compatibility 计数和 singular risk。
- `metrics.compute_stability`: factorization readiness、matrix validation report、sample unit solves、non-finite count 和 negative LCIA count。
- `provider_evidence`: 每条实际 input edge 的 consumer、flow、candidate providers、candidate reference-output status / eligibility、resolution strategy、failure reason、allocation weights、ambiguity 和 confidence；被判定为 non-reference output 的 rejected candidate 也必须保留，供调用方解释 provider 缺口。
- `findings` / `blockers`: machine-readable issue codes、severity、message 和 detail payload。

## Blockers

`blockers` 表示 gate 的硬失败，报告 `status` 会变为 `failed`。当前稳定 code 与默认触发条件如下：

| Code | 触发条件 | 默认处置方向 | Policy 是否可放宽 |
| --- | --- | --- | --- |
| `provider_closure_write_pct_below_policy` | `coverage.matching.a_write_pct` 低于 `policy.min_provider_write_pct`，默认阈值为 `100.0` | 修复 provider closure 后重跑 | 是，调整 `min_provider_write_pct` |
| `provider_closure_unmatched` | `coverage.matching.unmatched_no_provider` 超过 `policy.max_unmatched_no_provider`，默认 `0` | 修复无 provider 的 input edge 后重跑 | 是，调整 `max_unmatched_no_provider` |
| `provider_closure_reference_provider_missing` | 某条实际 input provider decision 的 `failure_reason = rejected_non_reference_only`：存在同 flow 的 output candidate，但它们都不是各自 Process 的 quantitative reference output | 发布或引入一个以该 product flow 为 quantitative reference 的完整 Process 后重跑 | 否 |
| `provider_closure_multi_unresolved` | `coverage.matching.matched_multi_unresolved` 超过 `policy.max_multi_unresolved`，默认 `0` | 修复多 provider 决策后重跑 | 是，调整 `max_multi_unresolved` |
| `provider_closure_equal_fallback` | 存在 equal fallback，且 `policy.allow_equal_fallback = false` | 补充 provider volume / evidence 或显式允许 fallback | 是，设置 `allow_equal_fallback` |
| `reference_normalization_not_closed` | quantitative reference 存在 missing 或 invalid 计数 | 修复 process reference 后重跑 | 否 |
| `allocation_fraction_invalid` | 除两个有界 legacy fallback 外，已声明 allocation 的 target、fraction 或 targetless shape 无法按 TIDAS target-aware 规则安全解析，因而产生 invalid 计数 | 修复 allocation target / fraction 声明后重跑 | 否 |
| `singular_risk_high` | singular risk 为 `high`，且 `allow_high_singular_risk = false` | 修复矩阵结构或人工确认风险 | 是，设置 `allow_high_singular_risk` |
| `singular_risk_medium` | singular risk 为 `medium`，且 `allow_medium_singular_risk = false` | 复核矩阵结构或人工确认风险 | 是，设置 `allow_medium_singular_risk` |
| `lcia_factors_missing` | `require_lcia_factors = true` 且 `coverage.matrix_scale.c_nnz = 0` | 补齐 LCIA factors 后重跑 | 是，设置 `require_lcia_factors = false` |
| `factorization_not_ready` | `SolverService.prepare` 失败，包含结构校验或 UMFPACK factorization 失败 | 修复 compute stability 后重跑 | 否 |
| `sample_unit_solve_failed` | sample unit demand solve 对某个 process index 失败 | 修复 compute stability 后重跑 | 否 |
| `compute_non_finite_values` | sample solve 的 `x` / `g` / `h` 中存在 NaN 或 Infinity | 修复 compute stability 后重跑 | 否 |
| `negative_lcia_values` | sample solve 的 LCIA 输出低于 `-policy.negative_lcia_epsilon`，且 `negative_lcia_policy = blocker` | 复核 LCIA / matrix sign / inventory 数据后重跑 | 是，调整 `negative_lcia_policy` 或 `negative_lcia_epsilon` |

## Findings

`findings` 表示非阻塞发现。存在 warning 但没有 blocker 时，报告仍可为 `passed`，但 `next_action` 会变为 `manual_review_warnings`。

| Code | Severity | 触发条件 |
| --- | --- | --- |
| `provider_closure_no_input_edges` | `warning` | snapshot 没有 input edges 可检查 provider closure |
| `singular_risk_observed` | `info` | singular risk level 不是 `low` / `medium` / `high` 中的已知值 |
| `biosphere_entries_missing` | `warning` | `coverage.matrix_scale.b_nnz = 0`，结果可能缺少环境流信息 |
| `matrix_validation_warning` | `warning` | factorization matrix validation 返回 near-singular warning |
| `negative_lcia_values` | `info` / `warning` | 出现 negative LCIA，但 `negative_lcia_policy` 配置为 `ignore` 或 `warning` |

`metrics.graph_readiness.allocation_fraction_missing_count` 继续作为 coverage 指标保留，但不会产生 blanket warning。allocation 容器未声明表示该 Process 的 quantitative reference 使用默认 allocation factor `1.0`，是合法状态。Legacy scalar `allocations.allocation = {}` 是唯一按 undeclared/factor `1.0` 处理的空声明；它同时计入 `metrics.graph_readiness.legacy_empty_allocation_as_undeclared_count`。空数组、`[{}]` 和其他 malformed empty shape 仍然无效。

单个 targetless full allocation 只有在 Process 的物理 `Output` 恰好为 `1`、该 Output 的唯一有效 internal ID 等于 quantitative reference、且 fraction 为 canonical `100` 或 legacy string 精确 `"100%"` 时才推断为 factor `1.0`，并计入 `metrics.graph_readiness.legacy_single_output_target_inferred_count`。Multiple-output / multiple-entry targetless、非 full fraction、无效 Output ID、无法命中 quantitative reference 或其他无法安全解析的声明仍通过 `allocation_fraction_invalid` 阻断。这两个兼容计数本身不产生 blocker 或 finding。

Snapshot build config 使用 `allocation_semantics_version = tidas-quantitative-reference-v2`，且该字段进入 source fingerprint，所以 readiness 不会把 v1 或更早语义 snapshot 当作同一构建身份复用。`snapshot_coverage.v2` 和 `matrix_readiness_report.v1` 均保持现有 schema version；两个计数字段为 additive/default-zero，旧 artifact 缺少时按 `0` 读取。

`provider_closure_reference_provider_missing` 只针对实际 input provider decision，不会因为 Process 中一个没有被 demand 的 co-product 本身而触发。其 `details.examples[]` 必须给出 consumer index / ID / version / name、`flow_id`、candidate provider index / ID / process name、output exchange internal ID、是否为 reference output、normalized amount、allocation state 与 eligibility。它可以与通用的 `provider_closure_unmatched` 同时出现：前者解释“为什么没有合法 provider”，后者仍表达 coverage policy 失败。

## Next Action

`next_action` 是 calculator 给调用方的粗粒度下一步建议。生成时按 blocker 优先级短路：

| 条件 | `next_action` |
| --- | --- |
| 任一 blocker code 以 `provider_closure` 开头 | `repair_provider_closure_then_recheck` |
| 任一 blocker code 以 `factorization` 或 `compute` 开头 | `repair_compute_stability_then_recheck` |
| 任一 blocker code 以 `lcia` 开头 | `repair_lcia_factors_then_recheck` |
| 存在其他 blocker | `repair_graph_readiness_then_recheck` |
| 没有 blocker，但存在 warning finding | `manual_review_warnings` |
| 没有 blocker 且没有 warning finding | `publish_ready` |

调用方应把 `next_action` 当作路由提示，而不是唯一的判定来源；精确处置必须同时读取 `blockers[].code`、`findings[].code`、`severity` 和 `details`。

## Compute Sampling

Compute stability 当前按 `sample_solve_unit_limit` 采样 unit demand solve，默认上限为 `16`。

如果调用方验证的是指定目标 process，应在输入或命令参数中把采样范围配置到覆盖目标 process；否则 report 只能说明已采样 process 的 compute stability。
