---
title: Implicit Regional Supply Mix Modeling
docType: theory
scope: repo
status: active
authoritative: false
owner: worker
language: en
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
  - docs/implicit-regional-supply-mix-modeling.md
  - docs/implicit-regional-supply-mix-modeling.en.md
  - crates/solver-worker/src/bin/snapshot_builder.rs
  - crates/solver-worker/src/compiled_graph.rs
  - crates/solver-worker/src/snapshot_artifacts.rs
lastReviewedAt: 2026-05-26
lastReviewedCommit: fe323a1f2d884e7f812e9ebd66d5a32146e80766
related:
  - AGENTS.md
  - docs/agents/repo-architecture.md
  - docs/agents/repo-validation.md
  - docs/lca-api-contract.md
---

# Implicit Regional Supply Mix Modeling

Implicit Regional Supply Mix Modeling is the calculator's regional supply-mix method for provider linking. Without materializing an explicit market process, it selects a supply region for a product input exchange and allocates that input demand to multiple providers of the same product/reference flow.

The method is defined by this sequence:

```text
product input demand
  -> supply-region anchor
  -> geography tier
  -> provider set
  -> volume-based provider shares
  -> technosphere matrix links
```

It determines which providers supply a product input demand and what share each provider receives. It does not change the consumer process's total demand for the product/reference flow, and it does not treat annual supply or production volume as an exchange amount.

## Core Assumptions

### 1. Providers in the Same Supply Region Can Form a Representative Supply Mix

For the same product/reference flow, multiple providers in the same geography tier can be interpreted as the representative supply structure for that flow in that supply region.

The method does not require every provider to be a complete market participant in an external market statistic. It requires two conditions:

- the providers supply the same product/reference flow;
- the providers belong to the selected supply-region tier.

Automatic provider linking treats only a process's quantitative reference output as evidence that the process supplies the product/reference flow. In practice, the Output exchange `@dataSetInternalID` must equal the process `quantitativeReference.referenceToReferenceFlow`. Same-`flow_id` non-reference outputs are exposed as rejected candidate diagnostics and do not automatically enter the provider set.

Within that scope, `annualSupplyOrProductionVolume` can be used as a structured signal for relative provider supply scale. It is a share weight, not an additional technical input.

### 2. An Input Exchange Can Explicitly Declare the Supply Region

On a product input exchange, `processDataSet.exchanges.exchange[].location` is the supply-region anchor. It declares the geographic supply mix from which the input demand should be supplied.

For example:

```text
exchange.location = "CN"
```

means that the input demand uses the representative supply structure for China.

```text
exchange.location = "GLO"
```

means that the input demand uses a global supply mix.

The field is a plain location string. Recommended values are TIDAS/ILCD location category codes such as `CN`, `CN-BJ`, `RER`, and `GLO`. It is not localized text, not an exchange amount or unit, and not a biosphere LCIA geography.

### 3. Missing Exchange Location Uses the Local-First Default

If the input exchange does not provide a usable `location`, the calculator uses the consumer process's `locationOfOperationSupplyOrProduction` as the default supply-region anchor.

This default follows local-first selection:

```text
consumer local / subnational
same country / national average
same region
global
other
```

This means supply is assumed to come first from the consumer's location. If no local provider is available, the search expands to national, regional, or global providers.

### 4. Geography Tier Selection Comes Before Volume Weighting

The calculator first selects the geography tier and only then computes provider shares from annual volume within that tier.

Providers from all geography tiers must not be globally ranked by annual volume. Otherwise, a large global provider could override providers in the intended local or explicitly selected supply region.

Annual volume is comparable only within this scope:

```text
same product/reference flow
same selected geography tier
```

### 5. Annual Volume Determines Provider Share, Not Demand Amount

Exchange amount represents the consumer process's technical input demand per reference unit. Annual supply or production volume represents the provider process's annual supply scale.

Their meanings are separate:

```text
exchange amount -> demand size
annual volume   -> provider allocation share
```

Annual volume only determines how one demand is split across providers. It must not be multiplied into the consumer input demand as an additional demand quantity.

## Modeling Link Logic

For each product input exchange, the calculator applies the following link decision.

### Step 1: Determine the Product/Reference Flow

The calculator identifies the demanded product/reference flow `f` from the input exchange. Provider candidates must provide the same `f`. Under the default automatic linking rule, only a reference output proves that a process supplies that product/reference flow. A non-reference output with the same `flow_id` does not become a provider only because it is geographically closer or has an allocation fraction.

If no reference-output provider is available, the calculator should not fabricate one and should not fall back to arbitrary non-reference outputs. The exchange should be reported through provider-link diagnostics and resolved through data repair, additional provider data, or explicit market/co-product process modeling.

### Step 2: Determine the Supply-Region Anchor

The supply-region anchor priority is:

```text
exchange.location
consumer process location
unspecified
```

If `exchange.location` is present and can be parsed into a usable location descriptor, it becomes `g_jf`. If it is missing or unusable, the calculator uses the consumer process's `locationOfOperationSupplyOrProduction`. If neither is usable, the calculator enters unspecified matching logic and should expose that in diagnostics.

An effective `exchange.location` must not be overridden by the consumer process location. Consumer location only supplies the default region.

### Step 3: Select the Geography Tier

Given the supply-region anchor `g_jf`, the calculator selects the most appropriate geography tier among provider candidates.

If the anchor comes from `exchange.location`, tier search is centered on that explicit supply region. For example, with `exchange.location = "CN"`, the calculator first selects providers in the China tier. Only when that tier has no provider should the search expand outward from the China target region.

If the anchor comes from the consumer process location, the calculator uses local-first tier order:

```text
local / subnational
same country / national average
same region
global
other
```

The calculator selects the first non-empty tier.

### Step 4: Compute Provider Shares Within the Selected Tier

Within the selected geography tier, providers of the same product/reference flow are weighted by annual volume.

Annual volume is not compared across tiers.

### Step 5: Write Technosphere Matrix Links

The consumer input demand is split by provider shares and written to `A[p_i, j]`. After writing, the total demand for that input must be conserved.

## Mathematical Form

Let `j` be the consumer process. Its normalized input demand for product/reference flow `f` is:

```text
q_jf
```

The supply-region anchor for that demand is:

```text
g_jf = exchange.location, if present and usable
g_jf = consumer process location, otherwise
```

After geography tier selection for `g_jf`, the provider set is:

```text
P_{f,g} = { p_1, p_2, ..., p_n }
```

For each provider `p_i`, parse the numeric prefix from `annualSupplyOrProductionVolume` and define the raw weight:

```text
r_i = annual_volume_i, if annual_volume_i is finite and > 0
r_i = 1.0,             otherwise
```

The `1.0` value is a fixed positive default weight. It keeps the provider in the mix when supply scale is unknown. It does not mean that real annual output equals `1`, and it does not mean that the provider is necessarily the smallest supplier.

Provider share is:

```text
s_i = r_i / sum(r_k for p_k in P_{f,g})
```

The calculator writes the split demand into the technosphere matrix:

```text
A[p_i, j] += q_jf * s_i
```

Because:

```text
sum(s_i) = 1
```

the input demand is conserved:

```text
sum(A[p_i, j] for p_i in P_{f,g}) = q_jf
```

This equality is the core matrix constraint of the method. Provider allocation changes the provider distribution, not the total demand in the consumer column.

## Meaning of the `1.0` Fallback

The `1.0` fallback is used when annual volume is missing, invalid, non-finite, or non-positive.

It creates three interpretable states:

- all providers have valid volume: shares are fully volume-driven;
- no provider has valid volume: all raw weights are `1.0`, so the mix becomes equal-weighted;
- some providers have valid volume and some do not: valid volume provides stronger supply-scale evidence, while missing-volume providers stay in the mix with the fixed positive default.

The third state is intentional. Missing volume should not automatically remove a provider. Diagnostics must report fallback-to-one counts and ratios so the pseudocount is not mistaken for observed supply scale.

If a non-positive volume means that a provider is unavailable, that meaning should be represented through data relationships, availability, or candidate filtering, not by letting the non-positive value enter the matrix as a raw weight.

## Boundary Between Allocation Fraction and Provider Eligibility

`allocation_fraction` is used for exchange amount attribution:

```text
normalized exchange amount = raw amount * reference_scale * allocation_fraction
```

It may continue to scale attributed input, output, or elementary exchange amounts, but it does not grant provider eligibility. A non-reference output with an amount and allocation fraction only shows that the exchange participates in the current process dataset's allocation accounting; it does not mean that the process can automatically supply product input demand for that output flow.

If allocated co-product provider linking is needed later, it should be implemented as an explicit rule with independent product-semantics, allocation, output-amount, and diagnostics evidence, not inferred from same-`flow_id` matching alone.

## Relationship to an Explicit Market Process

The method can be interpreted as inlining a linear regional market process.

If an explicit market process `m_f,g` exists, consumer `j` can link to the market:

```text
A[m_f,g, j] += q_jf
```

The market then links to providers by share:

```text
A[p_i, m_f,g] += s_i
```

If this market process is only a pass-through supply mix and does not introduce additional production technology, losses, price constraints, trade transformations, or coproduct handling, the market node can be eliminated during matrix construction:

```text
A[p_i, j] += q_jf * s_i
```

The method therefore does not ignore the market mix. It represents the market mix directly in provider links. The solver still uses the same linear system:

```text
M = I - A
```

The difference is that the market mix is not exposed as a separate process in the process index. Its observability must come from provider allocation diagnostics.

If a model needs explicit market processes, import shares, trade constraints, market losses, price-driven allocation, or transformation activities, those semantics should be represented by materialized market processes rather than direct provider links.

## Matrix Properties

### Dimensional Consistency

`q_jf` is the product input amount per reference unit. `s_i` is a dimensionless share. `q_jf * s_i` remains a valid technosphere coefficient.

Annual volume does not enter `A` directly. It is normalized into a share first, so annual production scale is not mixed into the technology coefficient matrix.

### Column Demand Conservation

For one input demand, provider shares sum to `1`. Therefore changes in provider count or share distribution do not change the consumer process's total input demand.

### Non-Negativity and Numerical Stability

Raw weights accept only positive numbers. Missing or invalid volume uses `1.0`. Provider shares are therefore non-negative, and the normalization denominator is never `0`.

This avoids division by zero and prevents negative annual volume from creating provider edges with unclear reverse or cancellation semantics.

### Effect on `M = I - A`

The method does not change the solver's matrix form. It only changes the row distribution of selected product input demands in `A`.

If new provider links reveal self-loops, provider loops, or singular risk, those risks should be observed through existing diagnostics rather than hidden by changing the share definition.

## Data Semantics

### `exchange.location`

On a product input exchange, `exchange.location` means supply region:

- it is compatible with plain string values;
- TIDAS/ILCD location category codes are recommended;
- it does not use `StringMultiLang`;
- it does not represent exchange amount, unit, or localized label;
- it must not be mixed with biosphere LCIA geography.

### `annualSupplyOrProductionVolume`

`annualSupplyOrProductionVolume` remains `StringMultiLang` and should satisfy:

```text
number + space + text
```

The calculator uses the numeric prefix as the share weight. The trailing text preserves unit, reference-flow, or statistical-scope information. Data producers should keep provider volumes comparable for the same product/reference flow.

## Scope and Boundaries

This method applies to:

- provider allocation for product flows;
- multiple providers of the same product/reference flow within one geography tier;
- input exchanges that need explicit or default supply-region semantics;
- datasets where annual supply or production volume is a relative supply-scale signal;
- snapshot construction that needs a regional supply mix without materializing market processes.

This method does not apply to:

- elementary-flow biosphere matrix construction;
- provider sets whose annual volume units or statistical scopes are not comparable;
- user-facing models that require explicit market nodes;
- market modeling that requires trade, import, price, loss, or transformation processes;
- cases where `exchange.location` is used as LCIA geography rather than product-input supply region.

## Diagnostics

Provider-link diagnostics should record at least:

- source of the supply-region anchor: `exchange.location`, consumer process location, or unspecified;
- reference-output status and eligibility outcome for each same-flow output;
- selected geography tier;
- provider candidates and final provider set;
- annual-volume parse status for each provider;
- raw weight, normalized share, and fallback-to-one flag;
- no-provider, self-loop, provider-loop, and singular-risk behavior;
- whether matrix entries conserve the input demand after allocation.

These diagnostics are part of the method's external interpretability. Because the market mix is not materialized as a separate process, provider allocation diagnostics are the main way users inspect the resulting supply mix.
