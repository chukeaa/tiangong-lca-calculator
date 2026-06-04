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
  - crates/solver-worker/src/readiness.rs
  - crates/solver-worker/src/bin/matrix_readiness.rs
  - crates/solver-worker/src/bin/snapshot_builder.rs
  - crates/solver-worker/src/compiled_graph.rs
  - docs/lca-api-contract.md
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
lastReviewedAt: 2026-05-26
lastReviewedCommit: fe323a1f2d884e7f812e9ebd66d5a32146e80766
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
---

# Matrix Readiness Report Contract

`matrix_readiness` 是 calculator 侧的 report/artifact contract，不是 Edge HTTP API contract。

当前暴露方式只有两类：

- CLI: `cargo run -p solver-worker --bin matrix_readiness -- --input <input.json> --out <report.json>`
- fresh `snapshot_builder` run 在 `report_dir` 下写出的 `matrix-readiness-<snapshot_id>.json`

Edge、Foundry、CLI 或其他调用方可以消费 report 字段，但不应在外部复制 calculator 的 provider resolution、singular-risk、LCIA 或 UMFPACK readiness 判断逻辑。

## 输入

输入 schema version 为 `matrix_readiness_input.v1`，核心字段为：

- `coverage`: snapshot coverage report。
- `payload`: `ModelSparseData` sparse payload。
- `compiled_graph`（可选）：fresh build 时包含逐边 provider decision、candidate providers、candidate reference-output eligibility、allocation weights、geography tier 和 failure reason。没有该字段时仍可验证 coverage/compute，但 `provider_evidence` 会降级为空。
- `policy`: provider write percentage、unmatched / unresolved provider 容忍度、singular risk、LCIA factor、factorization 和 negative LCIA anomaly 策略。

## 输出

输出 schema version 为 `matrix_readiness_report.v1`，核心字段为：

- `status`: `passed` 或 `failed`。
- `next_action`: calculator 给调用方的粗粒度下一步建议。
- `metrics.provider_closure`: input edge、written edge、unmatched provider、multi-provider unresolved 和 equal-fallback 统计。
- `metrics.graph_readiness`: process/flow/impact scale、A/B/C/M nnz、reference/allocation closure 和 singular risk。
- `metrics.compute_stability`: factorization readiness、matrix validation report、sample unit solves、non-finite count 和 negative LCIA count。
- `provider_evidence`: 每条 input edge 的 consumer、flow、candidate providers、candidate reference-output status / eligibility、resolution strategy、failure reason、allocation weights、ambiguity 和 confidence。
- `findings` / `blockers`: machine-readable issue codes、severity、message 和 detail payload。

## Blockers

`blockers` 表示 gate 的硬失败，报告 `status` 会变为 `failed`。当前稳定 code 与默认触发条件如下：

| Code | 触发条件 | 默认处置方向 | Policy 是否可放宽 |
| --- | --- | --- | --- |
| `provider_closure_write_pct_below_policy` | `coverage.matching.a_write_pct` 低于 `policy.min_provider_write_pct`，默认阈值为 `100.0` | 修复 provider closure 后重跑 | 是，调整 `min_provider_write_pct` |
| `provider_closure_unmatched` | `coverage.matching.unmatched_no_provider` 超过 `policy.max_unmatched_no_provider`，默认 `0` | 修复无 provider 的 input edge 后重跑 | 是，调整 `max_unmatched_no_provider` |
| `provider_closure_multi_unresolved` | `coverage.matching.matched_multi_unresolved` 超过 `policy.max_multi_unresolved`，默认 `0` | 修复多 provider 决策后重跑 | 是，调整 `max_multi_unresolved` |
| `provider_closure_equal_fallback` | 存在 equal fallback，且 `policy.allow_equal_fallback = false` | 补充 provider volume / evidence 或显式允许 fallback | 是，设置 `allow_equal_fallback` |
| `reference_normalization_not_closed` | quantitative reference 存在 missing 或 invalid 计数 | 修复 process reference 后重跑 | 否 |
| `allocation_fraction_invalid` | allocation fraction 存在 invalid 计数 | 修复 allocation fraction 后重跑 | 否 |
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
| `allocation_fraction_missing` | `warning` | 存在 missing allocation fraction，但未达到 invalid blocker |
| `singular_risk_observed` | `info` | singular risk level 不是 `low` / `medium` / `high` 中的已知值 |
| `biosphere_entries_missing` | `warning` | `coverage.matrix_scale.b_nnz = 0`，结果可能缺少环境流信息 |
| `matrix_validation_warning` | `warning` | factorization matrix validation 返回 near-singular warning |
| `negative_lcia_values` | `info` / `warning` | 出现 negative LCIA，但 `negative_lcia_policy` 配置为 `ignore` 或 `warning` |

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
