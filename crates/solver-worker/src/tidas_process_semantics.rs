//! Shared TIDAS process calculation semantics.

use std::collections::HashSet;
use std::hash::BuildHasher;

use anyhow::{Context, bail};
use serde_json::Value;

/// Versioned allocation semantics applied before signed-flow linking.
pub const TIDAS_ALLOCATION_SEMANTICS_VERSION: &str = "tidas-reference-allocation-v3";

/// Versioned signed-flow linking semantics applied after allocation.
pub const SIGNED_FLOW_LINK_SEMANTICS_VERSION: &str = "signed-flow-balance-v1";

/// Backward-compatible name for the active allocation semantics identity.
pub const TIDAS_PROCESS_SEMANTICS_VERSION: &str = TIDAS_ALLOCATION_SEMANTICS_VERSION;

// TIDAS `Perc` permits at most three decimal places. Allow a one-unit difference
// in the least-significant percentage digit when checking a closed allocation
// vector (for example, 33.333 + 33.333 + 33.333 = 99.999).
const ALLOCATION_SUM_TOLERANCE: f64 = 0.000_010_000_001;

/// Allocation selected for one process quantitative reference.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TidasAllocationResolution {
    /// The exchange has no `allocations` container.
    Undeclared,
    /// A legacy scalar `allocation: {}` placeholder is treated as undeclared.
    LegacyEmptyUndeclared,
    /// The allocation vector explicitly contains the quantitative reference.
    Explicit { fraction: f64 },
    /// A single targetless full allocation was safely inferred for the unique reference pivot.
    LegacyInferredReference { fraction: f64 },
    /// The closed sparse vector omits the quantitative reference, implying zero.
    SparseZero,
}

/// Resolves one exchange allocation for the process quantitative reference.
///
/// TIDAS stores `@allocatedFraction` as `Perc`, so both JSON strings and numbers
/// are interpreted as percentages and divided by 100. A declared allocation is
/// accepted only when its object/array is non-empty, every target is a unique
/// known exchange, every fraction is finite and within 0..=100, and the complete
/// vector sums to 100% within the three-decimal `Perc` tolerance. Two bounded
/// legacy shapes are normalized: a scalar empty object is undeclared, and one
/// targetless full allocation is attributed to the quantitative reference only
/// when the reference internal ID uniquely resolves to one Process exchange.
pub fn resolve_tidas_exchange_allocation<S: BuildHasher>(
    exchange: &Value,
    reference_internal_id: &str,
    valid_exchange_internal_ids: &HashSet<String, S>,
    reference_exchange_count: usize,
) -> anyhow::Result<TidasAllocationResolution> {
    let reference_internal_id = reference_internal_id.trim();
    if reference_internal_id.is_empty() {
        bail!("quantitative reference internal ID is empty");
    }

    let Some(allocations) = exchange.get("allocations") else {
        return Ok(TidasAllocationResolution::Undeclared);
    };
    let allocations = allocations
        .as_object()
        .context("allocations must be an object")?;
    let allocation = allocations
        .get("allocation")
        .context("allocations.allocation is missing")?;

    if allocation
        .as_object()
        .is_some_and(serde_json::Map::is_empty)
    {
        return Ok(TidasAllocationResolution::LegacyEmptyUndeclared);
    }

    let entries = match allocation {
        Value::Object(_) => vec![allocation],
        Value::Array(entries) if !entries.is_empty() => entries.iter().collect(),
        Value::Array(_) => bail!("allocations.allocation must not be an empty array"),
        _ => bail!("allocations.allocation must be an object or array"),
    };

    if entries.len() == 1 {
        let entry = entries[0]
            .as_object()
            .context("allocations.allocation[0] must be an object")?;
        if !entry.contains_key("@internalReferenceToCoProduct") {
            if reference_exchange_count != 1
                || !valid_exchange_internal_ids.contains(reference_internal_id)
            {
                bail!(
                    "targetless allocation can only be inferred when the quantitative reference uniquely identifies one Process exchange"
                );
            }
            let raw_fraction = entry.get("@allocatedFraction").context(
                "allocations.allocation[0].@allocatedFraction is missing for targetless allocation",
            )?;
            let fraction = parse_legacy_targetless_full_fraction(raw_fraction).context(
                "invalid allocations.allocation[0].@allocatedFraction for targetless allocation",
            )?;
            return Ok(TidasAllocationResolution::LegacyInferredReference { fraction });
        }
    }

    let mut seen_targets = HashSet::with_capacity(entries.len());
    let mut selected_fraction = None;
    let mut fraction_sum = 0.0;

    for (index, entry) in entries.into_iter().enumerate() {
        let entry = entry
            .as_object()
            .with_context(|| format!("allocations.allocation[{index}] must be an object"))?;
        let target = entry
            .get("@internalReferenceToCoProduct")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|target| !target.is_empty())
            .with_context(|| {
                format!(
                    "allocations.allocation[{index}].@internalReferenceToCoProduct is missing or invalid"
                )
            })?;

        if !valid_exchange_internal_ids.contains(target) {
            bail!(
                "allocations.allocation[{index}] references unknown exchange internal ID {target}"
            );
        }
        if !seen_targets.insert(target.to_owned()) {
            bail!("duplicate allocation target internal ID {target}");
        }

        let raw_fraction = entry.get("@allocatedFraction").with_context(|| {
            format!("allocations.allocation[{index}].@allocatedFraction is missing")
        })?;
        let fraction = parse_tidas_perc(raw_fraction).with_context(|| {
            format!("invalid allocations.allocation[{index}].@allocatedFraction")
        })?;
        fraction_sum += fraction;
        if !fraction_sum.is_finite() {
            bail!("allocation fraction sum is non-finite");
        }

        if target == reference_internal_id {
            selected_fraction = Some(fraction);
        }
    }

    if (fraction_sum - 1.0).abs() > ALLOCATION_SUM_TOLERANCE {
        bail!(
            "allocation fractions must sum to 100%; actual sum is {}%",
            fraction_sum * 100.0
        );
    }

    Ok(
        selected_fraction.map_or(TidasAllocationResolution::SparseZero, |fraction| {
            TidasAllocationResolution::Explicit { fraction }
        }),
    )
}

/// Returns the exchange amount value used for calculation, in TIDAS precedence.
#[must_use]
pub fn preferred_calculation_amount_value(exchange: &Value) -> Option<&Value> {
    exchange
        .get("resultingAmount")
        .or_else(|| exchange.get("meanAmount"))
        .or_else(|| exchange.get("meanValue"))
}

fn parse_tidas_perc(value: &Value) -> anyhow::Result<f64> {
    let percentage = match value {
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                bail!("percentage is empty");
            }
            if trimmed.contains('%') {
                bail!("percentage must not include a percent sign");
            }
            trimmed
                .parse::<f64>()
                .context("percentage string is not numeric")?
        }
        Value::Number(number) => number
            .as_f64()
            .context("percentage number cannot be represented as f64")?,
        _ => bail!("percentage must be a string or number"),
    };

    if !percentage.is_finite() {
        bail!("percentage is non-finite");
    }
    if !(0.0..=100.0).contains(&percentage) {
        bail!("percentage must be within 0..=100");
    }
    Ok(percentage / 100.0)
}

fn parse_legacy_targetless_full_fraction(value: &Value) -> anyhow::Result<f64> {
    if value.as_str().is_some_and(|text| text.trim() == "100%") {
        return Ok(1.0);
    }

    let fraction = parse_tidas_perc(value)?;
    if fraction.to_bits() != 1.0_f64.to_bits() {
        bail!("targetless allocation fraction must be exactly 100%");
    }
    Ok(1.0)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::{Value, json};

    use super::{
        SIGNED_FLOW_LINK_SEMANTICS_VERSION, TIDAS_ALLOCATION_SEMANTICS_VERSION,
        TIDAS_PROCESS_SEMANTICS_VERSION, TidasAllocationResolution,
        preferred_calculation_amount_value,
        resolve_tidas_exchange_allocation as resolve_tidas_exchange_allocation_with_count,
    };

    fn outputs(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|id| (*id).to_owned()).collect()
    }

    fn resolve_tidas_exchange_allocation(
        exchange: &Value,
        reference_internal_id: &str,
        valid_exchange_internal_ids: &HashSet<String>,
    ) -> anyhow::Result<TidasAllocationResolution> {
        resolve_tidas_exchange_allocation_with_count(
            exchange,
            reference_internal_id,
            valid_exchange_internal_ids,
            usize::from(valid_exchange_internal_ids.contains(reference_internal_id)),
        )
    }

    fn assert_fraction(resolution: TidasAllocationResolution, expected: f64) {
        let TidasAllocationResolution::Explicit { fraction } = resolution else {
            panic!("expected explicit allocation, got {resolution:?}");
        };
        assert!((fraction - expected).abs() <= f64::EPSILON);
    }

    #[test]
    fn semantics_version_is_stable() {
        assert_eq!(
            TIDAS_PROCESS_SEMANTICS_VERSION,
            "tidas-reference-allocation-v3"
        );
        assert_eq!(
            TIDAS_ALLOCATION_SEMANTICS_VERSION,
            "tidas-reference-allocation-v3"
        );
        assert_eq!(SIGNED_FLOW_LINK_SEMANTICS_VERSION, "signed-flow-balance-v1");
    }

    #[test]
    fn missing_allocations_container_is_undeclared() {
        let resolution = resolve_tidas_exchange_allocation(&json!({}), "1", &outputs(&["1"]))
            .expect("resolve undeclared");
        assert_eq!(resolution, TidasAllocationResolution::Undeclared);
    }

    #[test]
    fn scalar_empty_allocation_is_legacy_undeclared() {
        let resolution = resolve_tidas_exchange_allocation(
            &json!({ "allocations": { "allocation": {} } }),
            "1",
            &outputs(&["1"]),
        )
        .expect("resolve legacy empty placeholder");
        assert_eq!(resolution, TidasAllocationResolution::LegacyEmptyUndeclared);
    }

    #[test]
    fn targetless_full_allocation_is_inferred_for_the_unique_reference_exchange() {
        for fraction in [json!("100"), json!("100.000"), json!(100), json!("100%")] {
            let exchange = json!({
                "allocations": {
                    "allocation": { "@allocatedFraction": fraction }
                }
            });
            let resolution = resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1"]))
                .expect("infer targetless full allocation");
            let TidasAllocationResolution::LegacyInferredReference { fraction } = resolution else {
                panic!("expected legacy inferred allocation, got {resolution:?}");
            };
            assert!((fraction - 1.0).abs() <= f64::EPSILON);
        }
    }

    #[test]
    fn one_element_targetless_array_is_inferred_when_unambiguous() {
        let exchange = json!({
            "allocations": {
                "allocation": [{ "@allocatedFraction": "100" }]
            }
        });
        let resolution = resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1"]))
            .expect("infer one targetless array entry");
        assert_eq!(
            resolution,
            TidasAllocationResolution::LegacyInferredReference { fraction: 1.0 }
        );
    }

    #[test]
    fn targetless_allocation_requires_exactly_one_reference_exchange() {
        let exchange = json!({
            "allocations": {
                "allocation": { "@allocatedFraction": "100" }
            }
        });
        let valid_outputs = outputs(&["1"]);

        for reference_exchange_count in [0, 2] {
            let error = resolve_tidas_exchange_allocation_with_count(
                &exchange,
                "1",
                &valid_outputs,
                reference_exchange_count,
            )
            .expect_err("reject non-unique reference exchange");
            assert!(error.to_string().contains("uniquely identifies"));
        }

        let error = resolve_tidas_exchange_allocation_with_count(&exchange, "2", &valid_outputs, 1)
            .expect_err("reject unique output that is not the reference");
        assert!(error.to_string().contains("quantitative reference"));
    }

    #[test]
    fn targetless_non_full_or_malformed_allocations_are_rejected() {
        for fraction in [
            Some(json!("0")),
            Some(json!("1.5")),
            Some(json!("60")),
            Some(json!("94%")),
            Some(json!("99.999")),
            Some(json!("")),
            None,
        ] {
            let mut entry = serde_json::Map::new();
            if let Some(fraction) = fraction {
                entry.insert("@allocatedFraction".to_owned(), fraction);
            } else {
                entry.insert("legacyNote".to_owned(), json!(true));
            }
            let exchange = json!({
                "allocations": {
                    "allocation": Value::Object(entry)
                }
            });
            assert!(resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1"])).is_err());
        }
    }

    #[test]
    fn targetless_reference_inference_does_not_depend_on_other_exchange_directions() {
        let multiple_exchanges = json!({
            "allocations": {
                "allocation": { "@allocatedFraction": "100" }
            }
        });
        assert_eq!(
            resolve_tidas_exchange_allocation(&multiple_exchanges, "1", &outputs(&["1", "2"]))
                .expect("infer unique reference among multiple exchanges"),
            TidasAllocationResolution::LegacyInferredReference { fraction: 1.0 }
        );
    }

    #[test]
    fn multiple_targetless_entries_remain_invalid() {
        let multiple_entries = json!({
            "allocations": {
                "allocation": [
                    { "@allocatedFraction": "60" },
                    { "@allocatedFraction": "40" }
                ]
            }
        });
        assert!(
            resolve_tidas_exchange_allocation(&multiple_entries, "1", &outputs(&["1"])).is_err()
        );
    }

    #[test]
    fn object_allocation_resolves_explicit_reference() {
        let exchange = json!({
            "allocations": {
                "allocation": {
                    "@internalReferenceToCoProduct": "1",
                    "@allocatedFraction": "100.000"
                }
            }
        });
        let resolution = resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1"]))
            .expect("resolve object");
        assert_fraction(resolution, 1.0);
    }

    #[test]
    fn string_half_percent_is_divided_by_one_hundred() {
        let exchange = json!({
            "allocations": {
                "allocation": [
                    {
                        "@internalReferenceToCoProduct": "1",
                        "@allocatedFraction": "0.500"
                    },
                    {
                        "@internalReferenceToCoProduct": "2",
                        "@allocatedFraction": "99.500"
                    }
                ]
            }
        });
        let resolution = resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1", "2"]))
            .expect("resolve half percent");
        assert_fraction(resolution, 0.005);
    }

    #[test]
    fn numeric_one_percent_is_divided_by_one_hundred() {
        let exchange = json!({
            "allocations": {
                "allocation": [
                    {
                        "@internalReferenceToCoProduct": "1",
                        "@allocatedFraction": 1
                    },
                    {
                        "@internalReferenceToCoProduct": "2",
                        "@allocatedFraction": 99
                    }
                ]
            }
        });
        let resolution = resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1", "2"]))
            .expect("resolve one percent");
        assert_fraction(resolution, 0.01);
    }

    #[test]
    fn array_selects_reference_target_independent_of_order() {
        let forward = json!({
            "allocations": {
                "allocation": [
                    {
                        "@internalReferenceToCoProduct": "1",
                        "@allocatedFraction": "60.000"
                    },
                    {
                        "@internalReferenceToCoProduct": "2",
                        "@allocatedFraction": "40.000"
                    }
                ]
            }
        });
        let reversed = json!({
            "allocations": {
                "allocation": [
                    {
                        "@internalReferenceToCoProduct": "2",
                        "@allocatedFraction": "40.000"
                    },
                    {
                        "@internalReferenceToCoProduct": "1",
                        "@allocatedFraction": "60.000"
                    }
                ]
            }
        });
        let valid_outputs = outputs(&["1", "2"]);

        assert_fraction(
            resolve_tidas_exchange_allocation(&forward, "1", &valid_outputs)
                .expect("resolve forward A"),
            0.6,
        );
        assert_fraction(
            resolve_tidas_exchange_allocation(&reversed, "1", &valid_outputs)
                .expect("resolve reversed A"),
            0.6,
        );
        assert_fraction(
            resolve_tidas_exchange_allocation(&forward, "2", &valid_outputs).expect("resolve B"),
            0.4,
        );
    }

    #[test]
    fn closed_vector_without_reference_is_sparse_zero() {
        let exchange = json!({
            "allocations": {
                "allocation": {
                    "@internalReferenceToCoProduct": "2",
                    "@allocatedFraction": "100.000"
                }
            }
        });
        let resolution = resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1", "2"]))
            .expect("resolve sparse zero");
        assert_eq!(resolution, TidasAllocationResolution::SparseZero);
    }

    #[test]
    fn explicit_zero_for_reference_is_valid() {
        let exchange = json!({
            "allocations": {
                "allocation": [
                    {
                        "@internalReferenceToCoProduct": "1",
                        "@allocatedFraction": "0"
                    },
                    {
                        "@internalReferenceToCoProduct": "2",
                        "@allocatedFraction": "100"
                    }
                ]
            }
        });
        let resolution = resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1", "2"]))
            .expect("resolve explicit zero");
        assert_fraction(resolution, 0.0);
    }

    #[test]
    fn three_decimal_rounding_tolerance_accepts_99_999_percent() {
        let exchange = json!({
            "allocations": {
                "allocation": [
                    {
                        "@internalReferenceToCoProduct": "1",
                        "@allocatedFraction": "33.333"
                    },
                    {
                        "@internalReferenceToCoProduct": "2",
                        "@allocatedFraction": "33.333"
                    },
                    {
                        "@internalReferenceToCoProduct": "3",
                        "@allocatedFraction": "33.333"
                    }
                ]
            }
        });
        let resolution =
            resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1", "2", "3"]))
                .expect("accept three-decimal rounding");
        assert_fraction(resolution, 0.333_33);
    }

    #[test]
    fn duplicate_target_is_rejected() {
        let exchange = json!({
            "allocations": {
                "allocation": [
                    {
                        "@internalReferenceToCoProduct": "1",
                        "@allocatedFraction": "60"
                    },
                    {
                        "@internalReferenceToCoProduct": "1",
                        "@allocatedFraction": "40"
                    }
                ]
            }
        });
        let error = resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1"]))
            .expect_err("reject duplicate target");
        assert!(error.to_string().contains("duplicate"));
    }

    #[test]
    fn unknown_target_is_rejected() {
        let exchange = json!({
            "allocations": {
                "allocation": {
                    "@internalReferenceToCoProduct": "2",
                    "@allocatedFraction": "100"
                }
            }
        });
        let error = resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1"]))
            .expect_err("reject unknown target");
        assert!(error.to_string().contains("unknown exchange"));
    }

    #[test]
    fn empty_array_is_rejected() {
        let exchange = json!({ "allocations": { "allocation": [] } });
        let error = resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1"]))
            .expect_err("reject empty array");
        assert!(error.to_string().contains("empty array"));
    }

    #[test]
    fn malformed_allocation_shapes_and_fields_are_rejected() {
        let cases = [
            json!({ "allocations": {} }),
            json!({ "allocations": { "allocation": "100" } }),
            json!({ "allocations": { "allocation": [{}] } }),
            json!({
                "allocations": {
                    "allocation": {
                        "@internalReferenceToCoProduct": null,
                        "@allocatedFraction": "100"
                    }
                }
            }),
            json!({
                "allocations": {
                    "allocation": {
                        "@internalReferenceToCoProduct": "",
                        "@allocatedFraction": "100"
                    }
                }
            }),
            json!({
                "allocations": {
                    "allocation": {
                        "@internalReferenceToCoProduct": "1"
                    }
                }
            }),
        ];

        for exchange in cases {
            assert!(resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1"])).is_err());
        }
    }

    #[test]
    fn allocation_sum_mismatch_is_rejected() {
        let exchange = json!({
            "allocations": {
                "allocation": [
                    {
                        "@internalReferenceToCoProduct": "1",
                        "@allocatedFraction": "60"
                    },
                    {
                        "@internalReferenceToCoProduct": "2",
                        "@allocatedFraction": "30"
                    }
                ]
            }
        });
        let error = resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1", "2"]))
            .expect_err("reject sum mismatch");
        assert!(error.to_string().contains("sum to 100%"));
    }

    #[test]
    fn percent_sign_is_rejected() {
        let exchange = json!({
            "allocations": {
                "allocation": {
                    "@internalReferenceToCoProduct": "1",
                    "@allocatedFraction": "100%"
                }
            }
        });
        let error = resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1"]))
            .expect_err("reject percent sign");
        assert!(format!("{error:#}").contains("percent sign"));
    }

    #[test]
    fn non_finite_and_out_of_range_percentages_are_rejected() {
        for fraction in [
            json!("NaN"),
            json!("Infinity"),
            json!("-0.001"),
            json!("100.001"),
        ] {
            let exchange = json!({
                "allocations": {
                    "allocation": {
                        "@internalReferenceToCoProduct": "1",
                        "@allocatedFraction": fraction
                    }
                }
            });
            assert!(resolve_tidas_exchange_allocation(&exchange, "1", &outputs(&["1"])).is_err());
        }
    }

    #[test]
    fn preferred_amount_uses_resulting_then_mean_then_legacy_mean_value() {
        let all = json!({
            "resultingAmount": "3",
            "meanAmount": "2",
            "meanValue": "1"
        });
        let mean = json!({ "meanAmount": "2", "meanValue": "1" });
        let legacy = json!({ "meanValue": "1" });

        assert_eq!(
            preferred_calculation_amount_value(&all),
            Some(&Value::String("3".to_owned()))
        );
        assert_eq!(
            preferred_calculation_amount_value(&mean),
            Some(&Value::String("2".to_owned()))
        );
        assert_eq!(
            preferred_calculation_amount_value(&legacy),
            Some(&Value::String("1".to_owned()))
        );
        assert_eq!(preferred_calculation_amount_value(&json!({})), None);
    }
}
