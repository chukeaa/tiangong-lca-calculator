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

# Signed-Flow Linking Runtime Contract

本文档记录 calculator 当前 signed-flow balance/link 的运行时决策逻辑。`provider`、`consumer`、`input demand` 仍作为兼容显示词汇保留，但不再定义矩阵链接的资格。

边界：

- 本文档说明 runtime 如何把 exchange 编译为有符号系数、选择相反符号的 reference port、应用 routing weight 并写入 `A`。
- `docs/implicit-regional-supply-mix-modeling.md` 和英文版说明这个方法的建模依据：regional supply mix、exchange-location supply-region anchor、annual-volume share。
- 两者必须一起维护：运行时顺序改变时，本文件和 implicit regional supply mix 文档都要同步。

## 运行阶段

Signed-flow link 发生在 snapshot build 阶段，不发生在 solve 阶段。

主链路：

```text
process JSON
  -> parsed exchanges
  -> signed coefficients
  -> quantitative-reference pivots
  -> opposite-sign reference-port candidates
  -> routing decisions
  -> balance contributions
  -> A[balancing process, dependent process]
  -> M = I - A
```

Provider-link 的结果直接决定：

- 哪些 technosphere residual coefficient 能被 reference port 闭合并写入 `A`；
- multi-candidate balance 如何按 routing weight 拆分；
- signed-flow closure / A-write 覆盖率；
- matrix-readiness 和结果解释中的 reference port、balance、unresolved evidence。

## Process、quantitative reference 与矩阵列

一个完整 TIDAS Process revision 在 snapshot 中只对应一个 process index 和一个矩阵列。它的 `quantitativeReference.referenceToReferenceFlow` 只做两件事：选择 reference exchange，并为该列定义 normalization pivot。它不通过 Product/Waste type 或 Input/Output 方向预先声明“供给”或“需求”。

- reference exchange 可以是 `Input` 或 `Output`，flow source type 可以是 Product、Waste、Elementary 或 Other；reference 有效性的核心条件是 internal ID 唯一命中，以及最终 calculation amount finite 且非零；
- 当前只支持一个 quantitative reference flow。数组包含多个 reference flow 时明确 fail closed，不能任取第一个；
- normalized reference coefficient 保留符号并归一为 `+1` 或 `-1`；
- 同一 Process 中的非-reference exchange 不生成额外矩阵列。若它需要独立成为另一个 activity pivot，上游仍须发布另一个完整 Process revision。

因此，同一个联合生产来源可以由上游发布多个完整 Process，但矩阵身份始终来自这些实际 Process revisions，而不是由 snapshot builder 展开 co-products。

## TIDAS exchange allocation

Exchange allocation 在 balance matching 之前应用，用于得到当前 Process quantitative reference 对应的 attributed exchange amount：

```text
raw coefficient = direction_sign * calculation amount
reference scale = 1 / abs(raw reference coefficient)
normalized residual coefficient = raw coefficient * reference scale * selected allocation fraction
normalized reference coefficient = sign(raw reference coefficient)
```

运行时规则如下：

- calculation amount 按 `resultingAmount`、`meanAmount`、`meanValue` 的顺序选择；
- `allocations.allocation` 可以是 object 或 array；worker 按 `@internalReferenceToCoProduct == referenceToReferenceFlow` 选择目标项，不取第一项，也不对数组求和作为当前产品 fraction；
- `@allocatedFraction` 使用 TIDAS `Perc` 语义，JSON string 或 number 都按百分数解释并除以 `100`；带 `%` 后缀不是合法 `Perc`；
- allocation vector 的非零项闭合为 `100%`、但没有当前 reference target 时，按稀疏零处理，selected fraction 为 `0`；
- exchange 完全未声明 `allocations` 时，selected fraction 为 `1`；
- 仅 legacy scalar `allocations.allocation = {}` 视为“未声明 allocation”，selected fraction 为 `1`；这个例外不适用于空数组、`[{}]`、缺少 `allocation` 字段或带其他字段但缺少 target/fraction 的 object；
- 单个 targetless allocation entry 只有在 Process 恰好有一个 reference exchange、该 exchange 具有唯一有效 internal ID 且等于 quantitative reference 时才可推断；不再要求它是物理 `Output`。fraction 必须是 canonical full `100`，或 legacy string 精确为 `"100%"`；
- 除上述两个有界 legacy 例外外，空数组、坏结构、缺失 target/fraction、多 entry targetless、重复或未知 target、非有限/越界 fraction、非 full targetless fraction、总和不闭合都 fail closed。

Reference pivot 本身不乘 allocation fraction；allocation 只作用于 non-reference residual coefficient。selected fraction 为显式零或稀疏零的 residual 不进入 request-root closure，不计入 matching diagnostics，也不写入 `A` 或 `B`。

Exchange allocation 与下文的 multi-provider share 是两个独立阶段：前者决定 consumer column 的 attributed amount，后者只决定该 amount 在 eligible provider rows 之间的分布。

## Flow space 与候选 eligibility

Flow 的 ILCD/TIDAS source type 先映射到计算空间：

- Product 和 Waste -> `technosphere`，参与 reference-port linking；
- Elementary -> `biosphere`，直接写入 `B`，不参与 technosphere closure；
- Other -> `reporting`，保留证据但不进入 `A/B`。

Technosphere 候选集合按 exact flow identity `(Flow UUID, resolved version)` 建立。Exchange 显式给出 `@version` 时只查询并绑定该 revision；省略版本时才按 snapshot visibility 规则确定一个版本，并在后续 compilation、artifact 与 release evidence 中冻结。一个 snapshot 可以同时包含同一 UUID 的多个被引用 revision，reference-port lookup、flow metadata、flow axis 和 diagnostics 都不得退化为 UUID-only key，也禁止跨 revision 或不兼容单位链接。

Exact identity 不表示加载数据库中的全部历史版本。Worker 先按 request/process closure 收集 exchange 实际引用的 identity；显式版本使用精确 `(UUID, version)` 查询，省略版本只查询一次 deterministic selected revision。最终 closure 确定后，仅保留其中 distinct referenced identities，再分配连续 `flow_idx`。未被 exchange 引用的历史 revision、以及只有 LCIA factor 但没有 inventory exchange 的 Flow，不进入 `B/C` axis、compiled graph、source closure 或 bundle。

Reference ports 使用 `HashMap<(UUID, version), candidates>` 分桶。每条 residual 只访问同 identity 的候选列表，避免逐 residual 扫描所有 Process；矩阵 `A` 仍是 Process × Process 的稀疏矩阵，其存储与 assembly 由实际 non-zero balance edge 决定，而不是由 Flow 历史版本数决定。

对同一 flow identity：

```text
eligible(reference, residual)
  = reference.is_quantitative_reference
  && reference.process != residual.process
  && sign(reference.coefficient) == -sign(residual.coefficient)
```

因此 Waste Input、Waste Output、Product Input、Product Output 都可能成为 reference port；能否平衡某个 residual 只由 normalized coefficient 的相反符号决定。自链接明确排除。同 flow 的 non-reference exchange 和同符号 reference port 只保留为 rejected candidate evidence。

## Signed balance 与 A 写入

对每条非零 technosphere non-reference exchange，设其 normalized residual coefficient 为 `c_r`。候选 reference port `i` 的 coefficient 为 `c_i ∈ {-1,+1}`，routing weight 为 `w_i >= 0` 且 `sum(w_i)=1`：

```text
activity_requirement_i = (-c_r / c_i) * w_i
A[balancing_process_i, dependent_process] += activity_requirement_i
closure = c_r + sum(c_i * activity_requirement_i) = 0
```

相反符号保证 activity requirement 非负。routing 只分配已确定的 balance magnitude，不改变 exchange 的符号或 reference pivot。

候选数量分支：

```text
0 opposite-sign reference ports
  -> NoOppositeSignReference
  -> 不写 A

1 reference port
  -> UniqueProvider compatibility decision
  -> weight = 1

>1 reference ports
  -> resolve_multi_provider(provider_rule)
  -> resolved: 按 routing weight 求 activity requirement 并写 A
  -> unresolved: 不写 A
```

`Waste Input +1000` 的 raw reference coefficient 是 `-1000`，归一后为 `-1`；`Waste Output -1000` 也是 `-1000`，归一后同样为 `-1`。二者作为 reference port 对相同 residual 产生相同数学结果。相反，`Waste Output +1000` 的 pivot 为 `+1`，只能平衡负 residual。

## Technosphere boundary policy

`snapshot_builder --technosphere-boundary-policy` 必须显式落入 snapshot config、source/review fingerprint 和 calculation-bundle evidence：

- `closed`（默认）：任一非零 residual 无法闭合即阻断 readiness；
- `open`：允许 unresolved balance 留在系统边界外，readiness 输出 warning 和逐边证据；
- `cutoff`：允许按明确 cutoff 边界省略该 balance，同样必须保留 warning 和逐边证据。

未知策略 fail closed。`open/cutoff` 不是“没有找到 provider 时静默跳过”的别名。

## 当前默认 rule

`snapshot_builder` 当前默认：

```text
provider_rule = split_by_process_volume
```

该规则的 multi-provider 决策顺序是：

```text
eligible same-flow opposite-sign reference ports
  -> same-model provider subset, if available
  -> supply-region anchor
  -> best non-empty geography tier
  -> annual-volume provider shares within selected tier
  -> activity_requirement_i = balance_magnitude * share_i
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
A[balancing_process_i, dependent_process] += activity_requirement_i
```

同一 residual 的 routing weights 总和必须为 `1`，因此 routing 只改变 balancing process row distribution，不改变待闭合 coefficient 的总量。

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

Compiled graph 和 readiness 至少应支持解释：

- `decision_kind`: unique, multi resolved, multi unresolved, no provider；
- provider decision、reference port、balance resolution 与 unresolved evidence 中的 Flow UUID/version；
- `resolution_strategy`: unique, split by process volume, evidence, equal fallback 等；
- reference port 的 process/exchange identity、raw direction/amount、raw coefficient 与 normalized coefficient；
- residual exchange identity、residual coefficient、required reference sign；
- same-flow candidates 及 opposite-sign eligibility；
- candidate provider count 与 matched provider count；
- supply-region source 与 selected geography tier；
- annual volume fallback-to-one count；
- `legacy_empty_allocation_as_undeclared_count`：按 legacy scalar `{}` 兼容为未声明 allocation 的 exchange 数；
- `legacy_single_reference_target_inferred_count`：在唯一 reference exchange 的有效 internal ID 等于 quantitative reference 时推断 full targetless allocation 的 exchange 数；旧 `legacy_single_output_target_inferred_count` 仅作为兼容投影保留；
- routing weights、activity requirements 与 closure residual；
- unresolved balance 与 failure reason；
- `residual_edges_total` 与 `a_balance_edges_written`。旧 `input_edges_total` / `a_input_edges_written` 仅作为兼容计数保留。

Matrix-readiness、diagnostics export 和人工 debug 应消费这些 provider decisions，而不是在外部重写 provider resolution。

Snapshot build config 记录 `allocation_semantics_version = tidas-reference-allocation-v3`、`link_semantics_version = signed-flow-balance-v1`、`technosphere_boundary_policy` 和 `flow_identity_policy = exact-flow-version-reference-unit-v2`。v2 表示 exact revision 可共存、按最终引用集合剪枝并进入 flow axis/diagnostics；这些字段进入 source/review fingerprint，UUID-only 旧 snapshot 不会被复用。Coverage schema 为 `snapshot_coverage.v3`；readiness input/report 为 v2。Calculation bundle 为 `tiangong.calculation-bundle.v2`，technosphere release edge 使用 residual/balancing/reference/activity 的中性字段。
