---
title: Implicit Regional Supply Mix Modeling
docType: theory
scope: repo
status: active
authoritative: false
owner: calculator
language: zh-CN
whenToUse:
  - when reasoning about provider link semantics based on process annual supply or production volume
  - when evaluating whether a regional product supply mix can be represented without an explicit market process
  - when implementing or reviewing calculator provider allocation weights
whenToUpdate:
  - when provider allocation semantics change
  - when annual supply or production volume parsing semantics change
  - when calculator starts materializing explicit market processes instead of implicit direct links
checkPaths:
  - docs/implicit-regional-supply-mix-modeling.md
  - crates/solver-worker/src/bin/snapshot_builder.rs
  - crates/solver-worker/src/compiled_graph.rs
  - crates/solver-worker/src/snapshot_artifacts.rs
lastReviewedAt: 2026-05-18
lastReviewedCommit: 875cf25f34e56bf7d9bdfff9a140a40c8a311731
related:
  - AGENTS.md
  - docs/agents/repo-architecture.md
  - docs/agents/repo-validation.md
  - docs/lca-api-contract.md
---

# Implicit Regional Supply Mix Modeling

本文档说明 calculator 中 implicit regional supply mix 的建模方法：在不显式创建 market process 的前提下，用地区优先的 process annual supply / production volume 对同一 reference flow 的多个 provider 构建代表性供应 mix。

## 1. 建模目标

在 product system 中，一个 consumer process 对某个 product flow 的 input demand 通常不是对某个具体 provider 的自然指向，而是对该 flow 所代表功能的需求。若同一 reference flow 在目标地区存在多个可供给 process，则 calculator 需要把这条 demand 分配到一个代表性的供应组合上。

理想情况下，数据集中可以显式存在 market process。market process 以某个 reference product 作为 output，并按供应结构连接到若干生产 process。consumer 只需连接到 market process，由 market process 表示区域供应结构。

在没有显式 market process 或暂不 materialize market process 的情况下，calculator 可以用 implicit regional supply mix modeling 直接把 consumer demand 拆分到多个 provider process。该方法的目标是：

- 保持 consumer 对 reference flow 的总需求不变；
- 用地区内的供应规模表达 provider share；
- 避免把 national / regional / global 大体量 provider 覆盖 local supply 的语义错误；
- 保持 technosphere matrix 的维度一致性和可计算性。

## 2. 基本假定

### 2.1 同一地区的 reference flow 可以形成代表性供应 mix

对同一 product flow，在同一地理层级内存在多个 provider 时，这些 provider 可以被解释为该地区供给该 reference flow 的供应结构。若每个 provider 都给出 annual supply / production volume，该数值可以作为市场份额或供应份额的代理信号。

这不是断言 annual volume 是完整的市场统计，而是将其作为当前数据中最接近供应规模的结构化信号使用。其语义是相对权重，而不是额外技术投入量。

### 2.2 供应链默认 local-first

对一个 consumer process，供应链默认来自本地；如果本地没有可用 provider，再考虑全国平均；如果全国平均仍不可用，再考虑更大区域或 global provider。

因此 geography tier 的选择必须先于 volume weighting。annual volume 只在已选中的最优非空 geography tier 内比较，不能跨 tier 全局比较。否则，一个 global provider 的巨大 annual volume 可能压过本地 provider，违背 local-first 的供应链假定。

### 2.3 annual volume 是 provider share，不是 exchange amount

exchange amount 表示 consumer process 对某个 input flow 的技术需求量。annual supply / production volume 表示 provider process 的供应规模。两者的物理含义不同。

因此 annual volume 只能决定多个 provider 之间如何分摊一条 input demand，不能直接乘到 exchange amount 本身。换句话说，annual volume 参与 provider allocation share，而不是改变 consumer column 中该 input flow 的总需求。

## 3. 数学形式

设 consumer process 为 `j`，它对 product flow `f` 的归一化 input demand 为：

```text
q_jf
```

设在 local-first geography tier 选择后，flow `f` 的 provider 集合为：

```text
P_f = { p_1, p_2, ..., p_n }
```

每个 provider `p_i` 有一个 annual supply / production volume 数值前缀。定义 raw weight：

```text
r_i = annual_volume_i, if annual_volume_i is valid and > 0
r_i = 1.0,             otherwise
```

其中 fallback `1.0` 是 pseudocount，含义是“未知供应规模时给一个最小默认权重”，不是“真实年产量等于 1”。

provider share 为：

```text
s_i = r_i / sum(r_k for p_k in P_f)
```

然后把 consumer input demand 拆分写入 technosphere matrix `A`：

```text
A[p_i, j] += q_jf * s_i
```

由于所有 `s_i` 归一化后满足：

```text
sum(s_i) = 1
```

所以该 input flow 对 consumer column 的总需求保持不变：

```text
sum(A[p_i, j] for p_i in P_f) = q_jf
```

该性质是矩阵计算上的核心安全条件：provider allocation 只改变供应者分布，不放大或缩小需求量。

## 4. 与显式 market process 的关系

若存在显式 regional market process `m_f,g`，consumer `j` 可以先连接到该 market process：

```text
A[m_f,g, j] += q_jf
```

market process 再按 share `s_i` 连接到实际 provider：

```text
A[p_i, m_f,g] += s_i
```

在只关心线性 technosphere 传播、且 market process 本身不引入额外生产技术或损耗的情况下，可以把这个 market process 消去，得到直接边：

```text
A[p_i, j] += q_jf * s_i
```

这就是 implicit regional supply mix modeling。它等价于在矩阵构建阶段内联一个地区 market mix，但不在 process index 中增加额外节点。

这种做法的好处是：

- 不需要先构造和维护虚拟 market process 数据；
- snapshot 的 process index 仍只包含真实 process；
- provider allocation 的 share 仍然可以从数据中的 annual volume 推导；
- 对 solver 来说仍是同一个 `M = I - A` 形式。

代价是：

- market mix 不作为独立 process 暴露给用户；
- market mix 的诊断必须通过 provider allocation diagnostics 表达；
- 若未来需要显式市场建模、市场损耗、贸易转换或价格/进口约束，应改为 materialized market process。

## 5. Geography tier 选择原则

local-first 不是简单的地理打分，而是分层筛选。一个合理的 tier 顺序是：

```text
local / subnational
same country / national average
same region
global
other
```

计算时应选择第一个非空 tier，并只在该 tier 内做 volume weighting。

这个规则表达的是 LCA 建模中的默认供应假定：

- 如果有本地供应，优先认为 consumer 从本地供应结构取得该 flow；
- 如果没有本地供应，使用全国平均作为 next best representative supply；
- 如果全国也没有可用 provider，才扩大到区域或 global provider；
- annual volume 的比较只在同一 geography tier 内有意义。

这样的分层规则避免了一个常见误差：将供应规模和地理适配性混成一个全局分数，导致大规模非本地供应覆盖较小规模本地供应。

## 6. 矩阵计算性质

### 6.1 维度一致性

`q_jf` 是 consumer process 每 reference unit 所需的 product input amount。`s_i` 是无量纲 share。两者相乘后仍然是 technosphere coefficient，可以写入 `A[p_i, j]`。

annual volume 的单位不会直接进入 `A`，因此不会把年产量维度混入技术系数。annual volume 只通过归一化 share 影响 provider split。

### 6.2 列需求守恒

对同一个 input flow，provider allocation share 的和为 `1`。因此分配后 consumer column 对该 input flow 的总需求守恒。

这意味着该方法不会因为 provider 数量变化而改变 consumer process 的技术需求规模。provider 候选从 1 个变成多个时，变化的是供应者结构，而不是需求总量。

### 6.3 非负 share 与数值稳定性

annual volume 只接受合法正数。缺失、非法或非正值使用 `1.0` pseudocount。因此 raw weights 保证为正，share 也保证为正，且 sum 不为 0。

这避免了因为缺失数据导致除零，也避免了负 annual volume 把 provider edge 写成反向或抵消意义不明的 technosphere coefficient。

### 6.4 对 `M = I - A` 的影响

该方法不会改变 solver 的基本矩阵形式。它只改变 `A` 中某些 product input demand 的 row distribution。

如果新的供应分配暴露出 self-loop、provider loop 或 high diagonal risk，那是供应网络结构进入矩阵后的结果，不是 share 归一化本身的维度错误。此类风险应通过现有 self-loop cutoff、singular risk diagnostics 和 provider allocation diagnostics 观测，而不是通过改变 share 的数学定义掩盖。

## 7. Fallback `1.0` 的含义

当某个 provider 缺少有效 annual volume 时，raw weight fallback 为 `1.0`。这个规则有三种情况：

1. 所有 provider 都有合法 volume：share 完全由 volume 决定；
2. 所有 provider 都缺失有效 volume：所有 raw weight 都是 `1.0`，退化为等权代表性 mix；
3. 部分 provider 有合法 volume、部分缺失：有 volume 的 provider 按供应规模占主导，缺失 provider 保留一个最小默认权重。

第 3 种情况是有意的。它表达的是：有效 volume 是更强的供应规模证据；缺失数据不应让 provider 消失，但也不应和大规模 provider 等价。

因此 diagnostics 必须记录 fallback-to-one 的数量和比例，避免把 pseudocount 误读为真实供应规模。

## 8. 适用边界

该方法适用于：

- product flow 的 provider allocation；
- 同一 reference flow 在同一 geography tier 内存在多个 provider；
- annual supply / production volume 可作为供应规模 proxy 的场景；
- 暂不 materialize market process，但需要代表性 supply mix 的 snapshot 构建。

该方法不适用于：

- elementary flow 的 biosphere matrix 构建；
- annual volume 单位明显不可比且无法归一化的 provider 集合；
- 需要显式贸易、进口比例、价格、损耗或市场转换过程的模型；
- 需要把 market process 作为用户可解释节点输出的场景；
- annual volume 不是供应规模而是其他业务指标的数据源。

## 9. 数据语义要求

`annualSupplyOrProductionVolume` 在 TIDAS 中保持 localized text 形状。calculator 使用该字段时，只应解析 `#text` 的数值前缀，后缀作为上下文诊断信息保留。

解析层必须注意：

- object 和 array 两种 localized text 形状都要支持；
- 只接受有限正数作为 volume；
- 非法、缺失、空字符串、非正数都进入 fallback-to-one；
- 后缀不参与数值计算，但应在后续诊断中保留或可追踪；
- 同一 `flow_id` 和同一 geography tier 内，默认假定 volume 数值可比较。

如果未来发现同一 provider tier 内存在 `1000 kg` 和 `1 t` 这类等价但数值前缀不可直接比较的情况，应引入单位归一化或 suffix compatibility diagnostics，而不是改变 share 归一化原则。

## 10. 验证关注点

实现该建模方法后，至少需要验证：

- selected geography tier 符合 local-first；
- local provider 存在时不会被 national / regional / global provider 的 volume 覆盖；
- volume share 加和为 `1`；
- 缺失、非法、非正 volume 使用 raw weight `1.0`；
- unique provider 仍为 share `1.0`；
- request-root closure 和最终 `A` 矩阵构建使用同一 provider allocation 语义；
- snapshot coverage 可以观察 volume-weighted allocation 和 fallback-to-one 的规模。

这些验证不是单纯的代码覆盖要求，而是该方法是否保持 LCA 建模语义和矩阵计算安全性的必要证据。
