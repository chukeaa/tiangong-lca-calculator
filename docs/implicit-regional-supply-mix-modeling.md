---
title: Implicit Regional Supply Mix Modeling
docType: theory
scope: repo
status: active
authoritative: false
owner: worker
language: zh-CN
whenToUse:
  - when reasoning about provider link semantics based on process annual supply or production volume
  - when reasoning about explicit exchange location as the supply-region anchor for provider linking
  - when evaluating whether a regional product supply mix can be represented without an explicit market process
  - when implementing or reviewing calculator provider allocation weights
whenToUpdate:
  - when provider allocation semantics change
  - when exchange location supply-region semantics change
  - when annual supply or production volume parsing semantics change
  - when calculator starts materializing explicit market processes instead of implicit direct links
checkPaths:
  - docs/provider-linking.md
  - docs/implicit-regional-supply-mix-modeling.md
  - docs/implicit-regional-supply-mix-modeling.en.md
  - crates/solver-worker/src/bin/snapshot_builder.rs
  - crates/solver-worker/src/compiled_graph.rs
  - crates/solver-worker/src/snapshot_artifacts.rs
lastReviewedAt: 2026-06-21
lastReviewedCommit: 20d18a12916cfe1929140493377445aced21eb35
related:
  - AGENTS.md
  - docs/agents/repo-architecture.md
  - docs/agents/repo-validation.md
  - docs/provider-linking.md
  - docs/lca-api-contract.md
  - docs/implicit-regional-supply-mix-modeling.en.md
---

# Implicit Regional Supply Mix Modeling

Implicit Regional Supply Mix Modeling 是 calculator 在 provider linking 阶段使用的一种区域供应组合建模方法。它在不显式创建 market process 的前提下，为 product input exchange 选择合适的供应区域，并把该 input demand 分配给同一 product/reference flow 的多个 provider。

该方法的核心定义是：

```text
product input demand
  -> model-consistent provider scope
  -> supply-region anchor
  -> geography tier
  -> provider set
  -> volume-based provider shares
  -> technosphere matrix links
```

它只决定一条 product input demand 应由哪些 provider 承担，以及各 provider 承担多少份额。它不改变 consumer process 对该 product/reference flow 的总需求量，也不把 annual supply / production volume 当作 exchange amount 使用。

## 基本假定

### 1. 同一供应区域内的 providers 可以形成代表性 supply mix

对于同一 product/reference flow，如果在同一 geography tier 内存在多个 provider，则这些 provider 可以被解释为该供应区域内对该 flow 的代表性供应结构。

这种代表性供应结构不要求每个 provider 都是完整市场统计中的一个显式 market participant。calculator 只要求它们满足两个条件：

- 它们能够提供同一 product/reference flow；
- 它们处在同一个已选定的供应区域 tier 内。

自动 provider linking 默认只把 process 的 quantitative reference output 视为可供应的 product/reference flow。也就是说，Output exchange 的 `@dataSetInternalID` 必须等于该 process 的 `quantitativeReference.referenceToReferenceFlow`。同 `flow_id` 的非 reference output 只作为 rejected candidate diagnostics 暴露，不自动参与 provider set。

在这个范围内，`annualSupplyOrProductionVolume` 可以作为 provider 间相对供应规模的结构化信号。它表达的是 share weight，不是额外的技术投入。

### 2. 同一 model 内的 provider 表示模型已显式给出内部供应关系

`model_id` 表示一组 process 属于同一个 lifecycle model 或同一次数据建模上下文。处在同一 model 内的 consumer 与 provider 通常共享更一致的系统边界、数据来源、建模假设、技术口径和版本语境。

更直白地说：当 consumer process 的 input exchange 需求某个 product/reference flow，且同一 model 内另一个 process 的 quantitative reference output 正是这个 flow 时，数据作者已经在同一个 model 里同时放置了需求侧和供给侧。calculator 将这种结构解释为该 model 对内部供应关系的显式声明：这个 input demand 在模型边界内已有指定的供应来源候选。

这个优先级不是地理距离、市场份额或真实交易关系的直接证据，也不是说 input exchange 在数据结构上明文指向了某一个 provider process。它表达的是 product-flow 层面的 in-model supply relationship：在同一个产品系统或数据包内部，供给侧 process 已经被显式建模出来，因此应优先保持这条内部技术链条闭合，减少跨模型拼接带来的系统边界漂移。

如果同一 model 内没有可用 provider，则 calculator 回到 regional supply mix 假定：在更宽的 provider universe 中，根据 supply-region anchor、geography tier 和 annual volume 构造代表性供应组合。这样可以在保留模型内部闭合优先级的同时，避免因 model metadata 不完整而把本可链接的 input demand 误判为 no-provider。

### 3. input exchange 可以显式声明供应区域

`processDataSet.exchanges.exchange[].location` 在 product input exchange 上表示 supply-region anchor。它说明该 input demand 希望从哪个地理区域的供应组合中取得。

例如：

```text
exchange.location = "CN"
```

表示这条 input demand 使用中国范围内的代表性供应结构。

```text
exchange.location = "GLO"
```

表示这条 input demand 使用 global supply mix。

该字段是普通 location string，推荐使用 TIDAS/ILCD location category code，例如 `CN`、`CN-BJ`、`RER`、`GLO`。它不是 localized text，也不是 exchange amount、单位或 LCIA 地理语义。

### 4. 未显式声明供应区域时使用 local-first 默认假定

如果 input exchange 没有提供可用的 `location`，calculator 使用 consumer process 的 `locationOfOperationSupplyOrProduction` 作为默认 supply-region anchor。

此时采用 local-first 默认假定：

```text
consumer local / subnational
same country / national average
same region
global
other
```

这表示供应链默认优先来自 consumer 所在地；如果本地没有可用 provider，再逐级扩大到全国、区域或 global provider。

### 5. geography tier selection 先于 volume weighting

calculator 先选择 geography tier，再在该 tier 内按 annual volume 计算 provider share。

不能把所有 geography tiers 的 provider 混在一起按 annual volume 排序。否则，一个 global provider 可能因为 annual volume 很大而覆盖本地或显式供应区域内的 provider，破坏供应区域语义。

因此 annual volume 的比较范围必须是：

```text
same product/reference flow
same selected geography tier
```

### 6. annual volume 决定 provider share，不决定 demand amount

exchange amount 表示 consumer process 每 reference unit 对某个 product input 的技术需求量。annual supply / production volume 表示 provider process 的年供应或年产出规模。

两者语义不同：

```text
exchange amount -> demand size
annual volume   -> provider allocation share
```

annual volume 只决定一条 demand 在多个 provider 之间如何分摊，不能直接乘到 consumer 的 input demand 上作为额外需求。

## 建模 Link 逻辑

calculator 对每条 product input exchange 执行以下 link 决策。

### Step 1: 确定 product/reference flow

从 input exchange 中确定被需求的 product/reference flow `f`。provider candidates 必须能够提供同一 `f`。默认自动链接规则下，只有 reference output 能证明一个 process 可供应该 product/reference flow；非 reference output 即使同 `flow_id`，也不会因为地理位置更近或存在 allocation fraction 而成为 provider。

如果没有可用 reference-output provider，calculator 不应构造虚拟 provider，也不应回退到任意非 reference output。该 exchange 应进入 provider-link diagnostics，由数据修复、补充 provider，或显式 market/co-product process 建模解决。

### Step 2: 确定 model-consistent provider scope

在同一 product/reference flow 的 reference-output providers 中，calculator 先判断是否存在与 consumer 同 `model_id` 的 provider。

如果存在，同 model providers 构成后续 regional supply mix 选择的 provider scope。这个 scope 表示当前 lifecycle model 已经显式给出该 input demand 的内部供应来源候选。

如果不存在，则 provider scope 保持为全部 eligible providers，后续由 regional supply mix 规则选择供应区域和 provider shares。

### Step 3: 确定 supply-region anchor

supply-region anchor 的优先级是：

```text
exchange.location
consumer process location
unspecified
```

若 `exchange.location` 存在且可解析为可用 location descriptor，则使用它作为 `g_jf`。若不存在或不可用，则使用 consumer process 的 `locationOfOperationSupplyOrProduction`。若二者都不可用，则进入 unspecified matching 逻辑，并应在 diagnostics 中暴露。

重要的是，`exchange.location` 一旦有效，就不应被 consumer process location 覆盖。consumer location 只提供默认供应区域。

### Step 4: 选择 geography tier

给定 supply-region anchor `g_jf` 后，calculator 在候选 providers 中选择最合适的 geography tier。

如果 anchor 来自 `exchange.location`，tier search 围绕这个显式供应区域展开。例如 `exchange.location = "CN"` 时，优先选择中国供应 tier；只有该 tier 无 provider 时，才从中国这个目标区域向更宽 tier 扩展。

如果 anchor 来自 consumer process location，则使用 local-first tier 顺序：

```text
local / subnational
same country / national average
same region
global
other
```

calculator 选择第一个非空 tier。

### Step 5: 在已选 tier 内计算 provider shares

在已选 geography tier 内，对同一 product/reference flow 的 providers 使用 annual volume 计算 raw weight 与 normalized share。

这一步只发生在已选 tier 内，不跨 tier 比较 annual volume。

### Step 6: 写入 technosphere matrix

将 consumer 的 input demand 按 provider shares 拆分，写入 `A[p_i, j]`。写入后，同一 input demand 的总量必须保持不变。

## 数学形式

设 consumer process 为 `j`，它对 product/reference flow `f` 的归一化 input demand 为：

```text
q_jf
```

该 demand 的 supply-region anchor 为：

```text
g_jf = exchange.location, if present and usable
g_jf = consumer process location, otherwise
```

在应用 reference-output eligibility、可用 same-model scope 和 `g_jf` 对应的 geography tier 选择后，provider 集合为：

```text
P_{f,g} = { p_1, p_2, ..., p_n }
```

对每个 provider `p_i`，从 `annualSupplyOrProductionVolume` 中解析数值前缀，定义 raw weight：

```text
r_i = annual_volume_i, if annual_volume_i is finite and > 0
r_i = 1.0,             otherwise
```

`1.0` 是固定默认正权重。它表示供应规模未知时保留 provider 的参与资格，不表示真实年产量等于 `1`，也不表示 provider 的供应规模一定最小。

provider share 为：

```text
s_i = r_i / sum(r_k for p_k in P_{f,g})
```

然后写入 technosphere matrix：

```text
A[p_i, j] += q_jf * s_i
```

由于：

```text
sum(s_i) = 1
```

因此：

```text
sum(A[p_i, j] for p_i in P_{f,g}) = q_jf
```

这个等式是该方法的核心矩阵约束：provider allocation 只改变供应者分布，不改变 consumer column 中该 input demand 的总量。

## Fallback `1.0` 的含义

fallback `1.0` 用于 annual volume 缺失、非法、非有限或非正的 provider。

它产生三种可解释状态：

- 所有 providers 都有有效 volume：shares 完全由 volume 决定；
- 所有 providers 都缺失有效 volume：所有 raw weights 都是 `1.0`，退化为等权 mix；
- 部分 providers 有有效 volume、部分缺失：有效 volume 作为更强供应规模证据，缺失 volume 的 providers 使用默认正权重。

第三种状态是有意保留的。缺失 volume 不应让 provider 自动消失；但 diagnostics 必须记录 fallback-to-one 的数量和比例，避免把 pseudocount 误读为真实供应规模。

如果非正 volume 在数据语义上表示 provider 不可供应，应在数据关系、availability 或候选筛选逻辑中表达，而不是让非正值作为 raw weight 进入矩阵。

## Allocation Fraction 与 Provider Eligibility 的边界

`allocation_fraction` 用于 exchange amount attribution：

```text
normalized exchange amount = raw amount * reference_scale * allocation_fraction
```

它可以继续缩放 input、output 或 elementary exchange 的归属量，但它不授予 provider eligibility。一个非 reference output 即使有 amount 与 allocation fraction，也只说明该 exchange 参与当前 process dataset 的分摊核算；它不等于该 process 可以自动供应这个 output flow 的 product input demand。

若未来需要支持 allocated co-product provider linking，应作为显式规则实现，并要求独立的产品语义、allocation、output amount 和 diagnostics 证据，而不是从同 `flow_id` 自动推断。

## 与显式 Market Process 的关系

该方法可以看作对线性 regional market process 的内联。

若存在显式 market process `m_f,g`，consumer `j` 可以连接到 market：

```text
A[m_f,g, j] += q_jf
```

market 再按 share 连接到 providers：

```text
A[p_i, m_f,g] += s_i
```

如果这个 market process 只表示 pass-through supply mix，不引入额外生产技术、损耗、价格约束、贸易转换或副产品处理，则可以在矩阵构建时消去 market node，直接得到：

```text
A[p_i, j] += q_jf * s_i
```

因此，该方法不是忽略 market mix，而是在 provider link 中直接表达 market mix。solver 仍使用相同的线性系统形式：

```text
M = I - A
```

差异在于 market mix 不作为独立 process 出现在 process index 中。它的可观察性必须通过 provider allocation diagnostics 提供。

如果需要显式展示 market process、表达进口份额、贸易限制、市场损耗、价格驱动分配或转换活动，应使用 materialized market process，而不是继续使用 direct provider links 表达这些语义。

## 矩阵计算性质

### 维度一致性

`q_jf` 是每 reference unit 的 product input amount。`s_i` 是无量纲 share。`q_jf * s_i` 仍是合法 technosphere coefficient。

annual volume 不直接进入 `A`，而是先归一化为 share。因此年产量维度不会混入技术系数矩阵。

### 列需求守恒

对同一 input demand，所有 provider shares 的和为 `1`。因此 provider 数量或 share 分布的变化不会改变 consumer process 的总 input demand。

### 非负与数值稳定性

raw weights 只接受正数；缺失或非法 volume 使用 `1.0`。因此 provider shares 非负，且归一化分母不会为 `0`。

这避免了除零，也避免了负 annual volume 产生语义不明的反向或抵消边。

### 对 `M = I - A` 的影响

该方法不改变 solver 的矩阵形式，只改变 `A` 中某些 product input demand 的 row distribution。

如果新的 provider links 暴露 self-loop、provider loop 或 singular risk，这些风险应通过现有 diagnostics 观察，而不是通过改变 share 定义掩盖。

## 数据语义要求

### `exchange.location`

在 product input exchange 上，`exchange.location` 表示供应区域：

- 类型上兼容普通 string；
- 推荐使用 TIDAS/ILCD location category code；
- 不使用 `StringMultiLang`；
- 不表示 exchange amount、单位或 localized label；
- 不应与 biosphere LCIA geography 混用。

### `annualSupplyOrProductionVolume`

`annualSupplyOrProductionVolume` 应保留为 `StringMultiLang`，并满足：

```text
数字 + 空格 + 文本
```

calculator 使用数值前缀作为 share weight，后续文本保留单位、参考流或统计口径信息。数据生产者应保证同一 product/reference flow 的 provider volumes 在语义上可比较。

## 适用边界

该方法适用于：

- product flow 的 provider allocation；
- 同一 product/reference flow 在同一 geography tier 内存在多个 provider；
- input exchange 需要显式或默认供应区域语义；
- annual supply / production volume 可作为相对供应规模信号；
- snapshot 构建需要 regional supply mix，但不 materialize market process。

该方法不适用于：

- elementary flow 的 biosphere matrix 构建；
- annual volume 单位或统计口径不可比较的 provider 集合；
- 需要显式 market node 的用户可解释模型；
- 需要贸易、进口、价格、损耗或转换过程的 market modeling；
- `exchange.location` 被用于 LCIA 地理语义而不是 product-input supply region 的场景。

## 诊断要求

provider-link diagnostics 应至少记录：

- supply-region anchor 来源：`exchange.location`、consumer process location 或 unspecified；
- 每个同 flow output 的 reference-output 状态与 eligibility outcome；
- 选中的 geography tier；
- provider candidates 与最终 provider set；
- 每个 provider 的 annual volume 解析状态；
- raw weight、normalized share 和 fallback-to-one 标记；
- no-provider、self-loop、provider-loop、singular-risk 等风险；
- 写入 `A` 后是否满足 input demand conservation。

这些 diagnostics 是该方法对外可解释性的组成部分。由于 market mix 没有 materialize 成独立 process，用户理解 supply mix 的主要入口就是 provider allocation diagnostics。
