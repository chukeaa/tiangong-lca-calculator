---
title: Implicit Regional Supply Mix Modeling
docType: theory
scope: repo
status: active
authoritative: false
owner: worker
language: en
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
---

# Implicit Regional Supply Mix Modeling

This document explains where regional supply-mix routing sits in the signed-flow linker. `docs/provider-linking.md` is the runtime contract.

## Definition

An implicit regional supply mix does not decide which exchange is a demand or a supply. After a technosphere residual has multiple mathematically eligible reference ports, it uses model, geography, and annual-volume evidence to choose routing weights without materializing an additional market process.

```text
signed coefficient / reference pivot
  -> exact same-flow, opposite-sign candidates
  -> same-model scope, if available
  -> supply-region anchor
  -> best non-empty geography tier
  -> annual-volume routing weights
  -> non-negative activity requirements
```

## Signed-flow basis

Each exchange first becomes a signed coefficient:

```text
c = direction_sign * calculation_amount
direction_sign(Input)  = -1
direction_sign(Output) = +1
```

Direction and amount sign are independent. Product/Waste type does not define a link role. The Process quantitative reference selects a pivot exchange whose raw coefficient is normalized to `+1` or `-1`.

A non-reference technosphere exchange forms residual `c_r`. Reference port `i` is eligible only when it has an exact compatible flow identity, belongs to another Process, is that Process's quantitative reference exchange, and satisfies `sign(c_i) = -sign(c_r)`.

Flow source type only selects the computation space: Product/Waste are technosphere, Elementary is biosphere, and Other is reporting-only.

## Routing is separate from balance

Signed-flow math defines eligibility. The regional supply mix only defines routing weight `w_i` among eligible candidates:

```text
w_i >= 0
sum(w_i) = 1
activity_requirement_i = (-c_r / c_i) * w_i
c_r + sum(c_i * activity_requirement_i) = 0
```

Opposite signs guarantee non-negative activity requirements. Annual volume, geography, and model metadata must not alter coefficient signs, the reference pivot, or the magnitude to be balanced.

## Same-model priority

When the dependent Process has a `model_id` and at least one candidate has the same `model_id`, routing first narrows to that subset. This is a hard routing filter, not an exchange-level provider pointer or evidence of a real transaction.

Without a same-model candidate, routing uses the wider eligible universe. This fallback never relaxes exact-flow, opposite-sign, reference-port, or self-link rules.

## Supply-region anchor

The geographic anchor resolves in this order:

```text
residual exchange.location
dependent process location
unspecified
```

Exchange location wins because it can express the desired supply region for this balance. Process location is the default.

## Geography tier

Routing selects the first non-empty tier:

```text
local / subnational
same country
same region
global
other
```

Annual volume is never compared across tiers. Select the tier first, then distribute within it.

## Annual-volume weight

Within the selected tier:

```text
raw_weight_i = annualSupplyOrProductionVolume_i, if finite and > 0
raw_weight_i = 1.0, otherwise
w_i = raw_weight_i / sum(raw_weight)
```

The `1.0` fallback is a fixed positive routing weight, not a claim that real annual output equals one. Diagnostics must count its use.

## Equivalent and different Waste references

`Waste Input +1000` gives `c_ref = -1000`, normalized to `-1`. `Waste Output -1000` also gives `c_ref = -1000`, normalized to `-1`. They are mathematically equivalent reference ports and can both balance a positive residual.

`Waste Output +1000` instead normalizes to `+1` and can only balance a negative residual. The distinction comes from the signed coefficient, not an implicit rule such as “every waste output is a demand.”

## Allocation boundary

Allocation runs before routing and determines the non-reference exchange residual attributed to the current quantitative reference:

```text
normalized residual = raw coefficient * reference_scale * selected allocation fraction
```

The reference pivot itself is not multiplied by allocation. Target-aware allocation uses the quantitative-reference internal ID; direction does not determine target validity. One targetless full allocation may be inferred only when the reference exchange and ID are unique. Multiple targetless entries or multiple quantitative references fail closed.

Allocation fraction and routing weight are different: the former changes residual magnitude; the latter distributes an already determined balance across reference ports.

## Boundary policy

- `closed`: every non-zero technosphere residual must close; this is the production default.
- `open`: an unresolved balance may remain outside the system boundary.
- `cutoff`: a balance may be omitted under an explicit cutoff boundary.

`open/cutoff` must participate in snapshot config and fingerprints and must produce readiness warnings plus per-edge unresolved evidence. They are not silent fallbacks.

## Audit evidence

Snapshot, release, and readiness evidence retain flow UUID/version/reference unit, flow space/source type, raw direction/amount/coefficient, normalized reference/residual coefficient, candidate eligibility, routing strategy/weight, activity requirement, closure residual, boundary policy, and unresolved reason.

Build identity uses `tidas-reference-allocation-v3`, `signed-flow-balance-v1`, and `exact-flow-version-reference-unit-v2`. Exact Flow identity is `(UUID, resolved version)`, and compilation retains only revisions referenced by the final Process closure. Coverage is `snapshot_coverage.v3`; readiness input/report are v2; calculation bundles are v2.

## Limitation

An implicit mix is a calculation-time routing policy, not a standalone market dataset. It must not be interpreted as a persisted market Process, procurement relationship, or statistical supply chain. Model a complete explicit Process when a reusable, publishable market identity is required.
