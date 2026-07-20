---
title: Implicit Regional Supply Mix Modeling
docType: theory
scope: repo
status: active
authoritative: false
owner: worker
language: zh-CN
whenToUse:
  - when reasoning about signed-flow routing based on process annual supply or production volume
  - when reasoning about explicit exchange location as a supply-region anchor
  - when evaluating whether a regional technosphere mix can be represented without an explicit market process
  - when implementing or reviewing calculator routing weights
whenToUpdate:
  - when signed-flow candidate or routing semantics change
  - when exchange location supply-region semantics change
  - when annual supply or production volume parsing semantics change
  - when calculator starts materializing explicit market processes instead of implicit direct links
checkPaths:
  - docs/provider-linking.md
  - docs/implicit-regional-supply-mix-modeling.md
  - docs/implicit-regional-supply-mix-modeling.en.md
  - crates/solver-worker/src/bin/snapshot_builder.rs
  - crates/solver-worker/src/compiled_graph.rs
  - crates/solver-worker/src/signed_flow.rs
  - crates/solver-worker/src/snapshot_artifacts.rs
lastReviewedAt: 2026-07-20
lastReviewedCommit: 4001a5f367ba1eb3a2405e71042d1fe0987acf88
related:
  - AGENTS.md
  - docs/agents/repo-architecture.md
  - docs/agents/repo-validation.md
  - docs/provider-linking.md
  - docs/lca-api-contract.md
  - docs/implicit-regional-supply-mix-modeling.en.md
---

# Implicit Regional Supply Mix Modeling

本文说明 regional supply mix 在 signed-flow linker 中的建模位置。运行时精确合同以 `docs/provider-linking.md` 为准。

## 一句话定义

Implicit regional supply mix 不判断谁是“需求”或“供给”。它只在一条 technosphere residual 已找到多个数学上合格的 reference ports 后，按 model、地理和年产量选择 routing weights；不创建额外 market process。

```text
signed coefficient / reference pivot
  -> exact same-flow, opposite-sign candidates
  -> same-model scope, if available
  -> supply-region anchor
  -> best non-empty geography tier
  -> annual-volume routing weights
  -> non-negative activity requirements
```

## Signed-flow 基础

Exchange 首先编译为有符号系数：

```text
c = direction_sign * calculation_amount
direction_sign(Input)  = -1
direction_sign(Output) = +1
```

方向和 amount 符号是两个独立维度。Product/Waste type 也不决定 link role。Process 的 quantitative reference 只选择 pivot exchange；其 raw coefficient 归一为 `+1` 或 `-1`。

一条 non-reference technosphere exchange 形成 residual `c_r`。Reference port `i` 只有在以下条件全部满足时才是候选：

- exact flow identity 兼容；
- 来自另一个 Process，禁止自链接；
- 是该 Process 的 quantitative reference exchange；
- `sign(c_i) = -sign(c_r)`。

Flow source type 只决定空间：Product/Waste 进入 technosphere，Elementary 进入 biosphere，Other 只保留 reporting evidence。

## Routing 与 balance 分离

候选资格由 signed-flow math 决定；regional supply mix 只决定候选之间的 routing weight `w_i`：

```text
w_i >= 0
sum(w_i) = 1
activity_requirement_i = (-c_r / c_i) * w_i
c_r + sum(c_i * activity_requirement_i) = 0
```

相反符号保证 activity requirement 非负。Annual volume、geography 或 model metadata 不得修改 coefficient 符号、reference pivot 或待闭合总量。

## Same-model 优先

如果 dependent Process 有 `model_id`，并且候选中存在相同 `model_id` 的 reference port，候选 scope 先收窄到该子集。这是 routing hard filter，不是 exchange-level provider pointer，也不是现实交易证明。

若没有同 model 候选，则使用更宽的 eligible candidate universe。这个 fallback 只放宽 routing scope，不放宽 exact-flow、opposite-sign、reference-port 或 self-link 规则。

## Supply-region anchor

Routing 的地理 anchor 按以下顺序解析：

```text
residual exchange.location
dependent process location
unspecified
```

Exchange location 优先，因为它可以表达该条 balance 希望使用的供应区域；Process location 仅作默认值。

## Geography tier

候选按第一个非空 tier 选择：

```text
local / subnational
same country
same region
global
other
```

Annual volume 不跨 tier 比较。先选 tier，再在 tier 内分配。

## Annual-volume weight

对选中 tier 的候选：

```text
raw_weight_i = annualSupplyOrProductionVolume_i, if finite and > 0
raw_weight_i = 1.0, otherwise
w_i = raw_weight_i / sum(raw_weight)
```

fallback `1.0` 只是固定正权重，不表示真实年产量等于 1。它必须在 diagnostics 中计数。

## Waste reference 的等价与差异

`Waste Input +1000` 的 raw reference coefficient 是 `-1000`，归一为 `-1`。`Waste Output -1000` 也是 `-1000`，归一为 `-1`。二者在链接数学上等价，均可平衡正 residual。

`Waste Output +1000` 的 pivot 则为 `+1`，只能平衡负 residual。差异来自 signed coefficient，不来自“waste output 一律是 demand”之类的隐式语义。

## Allocation 的边界

Allocation 在 routing 之前决定 non-reference exchange 对当前 quantitative reference 的 attributed residual：

```text
normalized residual = raw coefficient * reference_scale * selected allocation fraction
```

Reference pivot 本身不乘 allocation fraction。Target-aware allocation 以当前 quantitative reference internal ID 为目标，方向不是 target validity 的判断条件。单个 targetless full allocation 只在 reference exchange 唯一且 ID 明确时推断；多 targetless 或多个 quantitative reference 均 fail closed。

Allocation fraction 与 routing weight 不可混用：前者改变 residual magnitude，后者只把既定 balance 分配给多个 reference ports。

## Boundary policy

- `closed`：所有非零 technosphere residual 必须闭合，默认用于生产 snapshot；
- `open`：允许未闭合 balance 留在系统边界外；
- `cutoff`：允许按明确 cutoff 省略 balance。

`open/cutoff` 必须进入 snapshot config、fingerprint、readiness warning 和逐边 unresolved evidence，不能作为静默 fallback。

## 可审计证据

Snapshot/release/readiness 至少保留：flow UUID/version/reference unit、flow space/source type、raw direction/amount/coefficient、normalized reference/residual coefficient、候选 eligibility、routing strategy/weight、activity requirement、closure residual、boundary policy 和 unresolved reason。

Build identity 使用 `tidas-reference-allocation-v3`、`signed-flow-balance-v1` 和 `exact-flow-version-reference-unit-v2`。Exact Flow identity 是 `(UUID, resolved version)`；只编译最终 Process closure 实际引用的 revisions。Coverage 为 `snapshot_coverage.v3`；readiness input/report 为 v2；calculation bundle 为 v2。

## 限制

Implicit mix 是计算时 routing policy，不是独立 market dataset，不应被解释成数据库里真实存在的 market process、采购关系或统计供应链。需要可复用、可发布的 market identity 时，应显式建模完整 Process。
