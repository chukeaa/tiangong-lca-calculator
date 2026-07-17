---
title: Provider Linking Runtime Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: zh-CN
whenToUse:
  - when explaining or changing calculator provider-link runtime behavior
  - when changing snapshot_builder provider rule defaults or candidate filtering
  - when reviewing provider decision diagnostics, A-write coverage, or provider closure
whenToUpdate:
  - when provider candidate eligibility changes
  - when provider rule defaults or resolution order changes
  - when provider decision diagnostics change
checkPaths:
  - docs/provider-linking.md
  - docs/implicit-regional-supply-mix-modeling.md
  - docs/implicit-regional-supply-mix-modeling.en.md
  - docs/matrix-readiness-report-contract.md
  - crates/solver-worker/src/bin/snapshot_builder.rs
  - crates/solver-worker/src/compiled_graph.rs
  - crates/solver-worker/src/snapshot_artifacts.rs
lastReviewedAt: 2026-07-17
lastReviewedCommit: 6d61068445ebfad0fb3e07469f6d0468d692574a
related:
  - AGENTS.md
  - docs/implicit-regional-supply-mix-modeling.md
  - docs/implicit-regional-supply-mix-modeling.en.md
  - docs/matrix-readiness-report-contract.md
---

# Provider Linking Runtime Contract

本文档记录 calculator 当前 provider-link 的运行时决策逻辑。

边界：

- 本文档说明 runtime 怎么选 provider、怎么分配 input demand、怎么写入 `A`。
- `docs/implicit-regional-supply-mix-modeling.md` 和英文版说明这个方法的建模依据：regional supply mix、exchange-location supply-region anchor、annual-volume share。
- 两者必须一起维护：运行时顺序改变时，本文件和 implicit regional supply mix 文档都要同步。

## 运行阶段

Provider link 发生在 snapshot build 阶段，不发生在 solve 阶段。

主链路：

```text
process JSON
  -> parsed exchanges
  -> provider output candidates
  -> input exchange provider decisions
  -> technosphere edges
  -> A[provider, consumer]
  -> M = I - A
```

Provider-link 的结果直接决定：

- 哪些 product input exchange 能写入 `A`；
- multi-provider input demand 如何拆分到多个 provider；
- provider closure / A-write 覆盖率；
- matrix-readiness 和结果解释中的 provider evidence。

## Process、quantitative reference 与矩阵列

一个完整 TIDAS Process 只代表其 `quantitativeReference.referenceToReferenceFlow`：

- 一个 Process revision 在 snapshot 中只对应一个 process index 和一个矩阵列；
- 只有 quantitative reference output 能声明该 Process 可供应对应 product flow；
- 同一 Process 中的非 reference co-product output 只保留为数据集内容和 rejected candidate evidence，不生成额外矩阵列，也不成为 provider；
- 如果 co-product `B` 需要作为 root 或 provider 参与计算，上游必须提供另一个完整、独立、以 `B` 为 quantitative reference 的 TIDAS Process。worker 不从原 Process 临时派生不完整 Process。

因此，同一个联合生产来源可以由上游发布多个完整 Process，但矩阵身份始终来自这些实际 Process revisions，而不是由 snapshot builder 展开 co-products。

## TIDAS exchange allocation

Exchange allocation 在 provider matching 之前应用，用于得到当前 Process quantitative reference 对应的 exchange amount：

```text
normalized exchange amount = calculation amount * reference_scale * selected allocation fraction
```

运行时规则如下：

- calculation amount 按 `resultingAmount`、`meanAmount`、`meanValue` 的顺序选择；
- `allocations.allocation` 可以是 object 或 array；worker 按 `@internalReferenceToCoProduct == referenceToReferenceFlow` 选择目标项，不取第一项，也不对数组求和作为当前产品 fraction；
- `@allocatedFraction` 使用 TIDAS `Perc` 语义，JSON string 或 number 都按百分数解释并除以 `100`；带 `%` 后缀不是合法 `Perc`；
- allocation vector 的非零项闭合为 `100%`、但没有当前 reference target 时，按稀疏零处理，selected fraction 为 `0`；
- exchange 完全未声明 `allocations` 时，selected fraction 为 `1`；
- 仅 legacy scalar `allocations.allocation = {}` 视为“未声明 allocation”，selected fraction 为 `1`；这个例外不适用于空数组、`[{}]`、缺少 `allocation` 字段或带其他字段但缺少 target/fraction 的 object；
- 单个 targetless allocation entry 只有在 Process 的物理 `Output` exchange 恰好为 `1`、该 Output 具有唯一有效 internal ID 且该 ID 等于 quantitative reference 时才可推断为当前 reference；其 fraction 必须是 canonical full `100`，或 legacy string 精确为 `"100%"`，推断后的 selected fraction 为 `1`；
- 除上述两个有界 legacy 例外外，空数组、坏结构、缺失 target/fraction、多 entry targetless、multiple-output targetless、重复或未知 target、非有限/越界 fraction、非 full targetless fraction、总和不闭合都 fail closed，不能在 lenient 路径回退为 `1`。

selected fraction 为显式零或稀疏零的 Input 不进入 request-root provider closure，不计入 provider matching / missing-provider diagnostics，也不写入 `A`；零 attributed elementary exchange 同样不写入 `B`，也不参与 LCIA direction / factor-coverage evidence。否则会把不属于当前 quantitative reference 的零负担分支误判成供应链或 LCIA 缺口。

Exchange allocation 与下文的 multi-provider share 是两个独立阶段：前者决定 consumer column 的 attributed amount，后者只决定该 amount 在 eligible provider rows 之间的分布。

## Provider 候选与 eligibility

候选集合按 product/reference `flow_id` 建立：

- 遍历 process exchanges；
- `Output` exchange 进入同 `flow_id` 的 provider candidate set；
- candidate 保留 output internal id、reference-output 状态、normalized amount、allocation state 等诊断信息。

只有 reference output 是 eligible provider：

```text
Output.@dataSetInternalID == process.quantitativeReference.referenceToReferenceFlow
```

同 `flow_id` 的非 reference output 不参与自动 provider linking。它只作为 rejected candidate diagnostics 暴露，failure reason 可表现为 `rejected_non_reference_only`。

## Input edge 决策

对每条有 amount 的 `Input` exchange：

1. 计入 `input_edges_total`。
2. 查找同 `flow_id` 的 eligible providers。
3. 根据 provider 数量分支：

```text
0 providers
  -> NoProvider
  -> 不写 A

1 provider
  -> UniqueProvider
  -> A[provider, consumer] += amount

>1 providers
  -> resolve_multi_provider(provider_rule)
  -> resolved: 按 allocation share 写 A
  -> unresolved: 不写 A
```

单 provider case 不进入 multi-provider rule，直接以 weight `1.0` 写入 `A`。

## 当前默认 rule

`snapshot_builder` 当前默认：

```text
provider_rule = split_by_process_volume
```

该规则的 multi-provider 决策顺序是：

```text
eligible same-flow reference-output providers
  -> same-model provider subset, if available
  -> supply-region anchor
  -> best non-empty geography tier
  -> annual-volume provider shares within selected tier
  -> A[provider_i, consumer] += input_amount * share_i
```

### 1. 同 model_id 优先

如果 consumer process 有 `model_id`，并且 eligible providers 中至少一个 provider 具有相同 `model_id`，则 provider set 先收窄为同 `model_id` 子集。

如果 consumer 没有 `model_id`，或没有任何 eligible provider 与 consumer 同 `model_id`，则继续使用全部 eligible providers。

这一步是硬过滤，不是权重加成。它发生在 geography tier selection 之前，因此同 model provider 子集存在时，不同 model provider 不会因为地理更近或 annual volume 更大而进入本条 input demand 的 provider mix。

运行时这样处理的语义是：同一 `model_id` 内同时存在需求该 flow 的 consumer input 与供应该 flow 的 reference-output provider，表示模型已经在 product-flow 层面显式给出内部供应关系候选。这里的“显式”不是 exchange-level provider pointer，也不是现实交易证据；它表示 model 内部已经建模出可承担该 input demand 的供给侧 process。

### 2. Supply-region anchor

在同 model 过滤之后，calculator 为 input demand 选择 supply-region anchor：

```text
exchange.location
consumer process location
unspecified
```

有效 `exchange.location` 优先。consumer process location 只作为 input exchange 未声明或声明不可用时的默认供应区域。

### 3. Geography tier selection

给定 supply-region anchor 后，在当前 provider set 中选择第一个非空 geography tier：

```text
local / subnational
same country
same region
global
other
```

Annual volume 不跨 tier 比较。先选 tier，再在 tier 内按 volume 分配。

### 4. Annual-volume share

对选中 tier 内的 providers：

```text
raw_weight_i = annualSupplyOrProductionVolume_i, if finite and > 0
raw_weight_i = 1.0, otherwise
share_i = raw_weight_i / sum(raw_weight)
```

`1.0` 是缺失、非法、非有限或非正 annual volume 的固定正权重 fallback。它保留 provider 的参与资格，但不表示真实年产量等于 `1`。

写入矩阵：

```text
A[provider_i, consumer] += input_amount * share_i
```

同一 input demand 的 provider shares 总和必须为 `1`，因此 provider 分配只改变 provider row distribution，不改变 consumer column 的总 input demand。

## 其他 provider rules

当前代码还保留这些 rule，用于 replay、诊断或显式运行：

| Rule | 行为 |
| --- | --- |
| `strict_unique_provider` | multi-provider 直接 unresolved，只接受唯一 provider case |
| `best_provider_strict` | 先尝试同 `model_id` 子集；再按 geography/time score 选唯一 top1；top1 必须满足最低分和 top1/top2 ratio |
| `split_by_evidence` | 先尝试同 `model_id` 子集；对 score 达标 providers 按 evidence score 分配 |
| `split_by_evidence_hybrid` | 先尝试同 `model_id` 子集；evidence 不足时回退到 equal split |
| `split_equal` | 不看 score，eligible providers 平均分配 |

这些 rule 不是默认生产语义。不要从历史 replay 结论推断当前默认行为。

## Diagnostics

Provider decisions 至少应支持解释：

- `decision_kind`: unique, multi resolved, multi unresolved, no provider；
- `resolution_strategy`: unique, split by process volume, evidence, equal fallback 等；
- same-flow candidates 及 reference-output eligibility；
- candidate provider count 与 matched provider count；
- supply-region source 与 selected geography tier；
- annual volume fallback-to-one count；
- `legacy_empty_allocation_as_undeclared_count`：按 legacy scalar `{}` 兼容为未声明 allocation 的 exchange 数；
- `legacy_single_output_target_inferred_count`：在唯一物理 Output 的有效 internal ID 等于 quantitative reference 时推断 full targetless allocation 的 exchange 数；
- final provider allocations；
- no-provider 或 unresolved failure reason；
- `a_input_edges_written` 与 provider-present resolved coverage。

Matrix-readiness、diagnostics export 和人工 debug 应消费这些 provider decisions，而不是在外部重写 provider resolution。

Snapshot build config 记录 `allocation_semantics_version = tidas-quantitative-reference-v2`。该字段进入 source fingerprint，因此 v2 compatibility semantics 不会复用 v1 或更早语义构建的 snapshot。Coverage schema 保持 `snapshot_coverage.v2`，但 `allocation` summary 增加上述两个 default-zero 兼容计数；旧 artifact 缺少这些字段时按 `0` 读取。
