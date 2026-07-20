//! Direction-neutral signed-flow primitives used by snapshot linking.

use anyhow::{bail, ensure};
use serde::{Deserialize, Serialize};

/// Numerical tolerance used for routing-weight and flow-balance closure checks.
pub const SIGNED_FLOW_CLOSURE_TOLERANCE: f64 = 1.0e-12;

/// Exchange direction before the amount sign is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum SignedFlowDirection {
    Input,
    Output,
}

impl SignedFlowDirection {
    /// Returns the algebraic incidence sign for this declared direction.
    #[must_use]
    pub const fn sign(self) -> f64 {
        match self {
            Self::Input => -1.0,
            Self::Output => 1.0,
        }
    }
}

/// Explicit handling policy for a non-zero technosphere coefficient that cannot be balanced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TechnosphereBoundaryPolicy {
    Closed,
    Open,
    Cutoff,
}

impl TechnosphereBoundaryPolicy {
    /// Parses the stable boundary-policy identifier used by CLI and snapshot artifacts.
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "closed" => Ok(Self::Closed),
            "open" => Ok(Self::Open),
            "cutoff" => Ok(Self::Cutoff),
            _ => bail!(
                "unsupported technosphere_boundary_policy={value}; expected closed, open, or cutoff"
            ),
        }
    }

    /// Returns the stable boundary-policy identifier used by persisted evidence.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::Cutoff => "cutoff",
        }
    }

    /// Returns whether unresolved non-zero technosphere balances are blockers.
    #[must_use]
    pub const fn requires_closure(self) -> bool {
        matches!(self, Self::Closed)
    }
}

/// A finite, non-zero quantitative-reference coefficient normalized to a signed unit pivot.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ReferencePivot {
    pub raw_coefficient: f64,
    pub scale: f64,
    pub coefficient: f64,
}

/// One normalized reference coefficient and its share of a residual flow balance.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WeightedReferencePort {
    pub reference_coefficient: f64,
    pub routing_weight: f64,
}

/// The activity requirement contributed by one routed reference port.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BalanceContribution {
    pub reference_coefficient: f64,
    pub routing_weight: f64,
    pub activity_requirement: f64,
}

/// A resolved signed-flow balance for one residual exchange.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignedFlowBalance {
    pub residual_coefficient: f64,
    pub contributions: Vec<BalanceContribution>,
    pub closure_residual: f64,
}

/// Compiles declared direction and signed amount into one algebraic coefficient.
pub fn signed_coefficient(
    direction: SignedFlowDirection,
    calculation_amount: f64,
) -> anyhow::Result<f64> {
    ensure!(
        calculation_amount.is_finite(),
        "exchange calculation amount must be finite"
    );
    let coefficient = direction.sign() * calculation_amount;
    ensure!(
        coefficient.is_finite(),
        "signed exchange coefficient must be finite"
    );
    Ok(coefficient)
}

/// Normalizes one finite, non-zero raw reference coefficient while retaining its sign.
pub fn normalize_reference_coefficient(raw_coefficient: f64) -> anyhow::Result<ReferencePivot> {
    ensure!(
        raw_coefficient.is_finite(),
        "quantitative reference coefficient must be finite"
    );
    ensure!(
        raw_coefficient != 0.0,
        "quantitative reference coefficient must be non-zero"
    );

    let scale = raw_coefficient.abs().recip();
    let coefficient = raw_coefficient.signum();
    ensure!(
        scale.is_finite() && scale > 0.0,
        "quantitative reference scale must be finite and positive"
    );
    Ok(ReferencePivot {
        raw_coefficient,
        scale,
        coefficient,
    })
}

/// Resolves one residual coefficient against one or more opposite-sign reference ports.
///
/// Routing weights must be finite, non-negative, and close to one. Every selected reference
/// coefficient must be finite, non-zero, and have the opposite sign to the residual. The returned
/// activity requirements are finite and non-negative, and the signed balance is checked before the
/// result is returned.
pub fn resolve_weighted_balance(
    residual_coefficient: f64,
    ports: &[WeightedReferencePort],
) -> anyhow::Result<SignedFlowBalance> {
    ensure!(
        residual_coefficient.is_finite(),
        "residual coefficient must be finite"
    );

    if residual_coefficient == 0.0 {
        ensure!(
            ports.is_empty(),
            "zero residual coefficient must not create routed balance contributions"
        );
        return Ok(SignedFlowBalance {
            residual_coefficient,
            contributions: Vec::new(),
            closure_residual: 0.0,
        });
    }
    ensure!(
        !ports.is_empty(),
        "non-zero residual coefficient requires at least one reference port"
    );

    let mut weight_sum = 0.0;
    for port in ports {
        ensure!(
            port.routing_weight.is_finite() && port.routing_weight >= 0.0,
            "routing weight must be finite and non-negative"
        );
        weight_sum += port.routing_weight;
    }
    ensure!(
        weight_sum.is_finite() && (weight_sum - 1.0).abs() <= SIGNED_FLOW_CLOSURE_TOLERANCE,
        "routing weights must close to one; sum={weight_sum}"
    );

    let mut contributions = Vec::with_capacity(ports.len());
    let mut balanced_coefficient = 0.0;
    for port in ports {
        let reference_coefficient = port.reference_coefficient;
        ensure!(
            reference_coefficient.is_finite() && reference_coefficient != 0.0,
            "reference port coefficient must be finite and non-zero"
        );
        if residual_coefficient * reference_coefficient >= 0.0 {
            bail!(
                "reference port coefficient must have the opposite sign to the residual coefficient"
            );
        }

        let activity_requirement =
            (-residual_coefficient / reference_coefficient) * port.routing_weight;
        ensure!(
            activity_requirement.is_finite() && activity_requirement >= 0.0,
            "activity requirement must be finite and non-negative"
        );
        balanced_coefficient += reference_coefficient * activity_requirement;
        contributions.push(BalanceContribution {
            reference_coefficient,
            routing_weight: port.routing_weight,
            activity_requirement,
        });
    }

    let closure_residual = residual_coefficient + balanced_coefficient;
    let closure_scale = residual_coefficient.abs().max(1.0);
    ensure!(
        closure_residual.abs() <= SIGNED_FLOW_CLOSURE_TOLERANCE * closure_scale,
        "signed flow balance does not close; residual={closure_residual}"
    );

    Ok(SignedFlowBalance {
        residual_coefficient,
        contributions,
        closure_residual,
    })
}

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::{
        SignedFlowDirection, TechnosphereBoundaryPolicy, WeightedReferencePort,
        normalize_reference_coefficient, resolve_weighted_balance, signed_coefficient,
    };

    #[test]
    fn technosphere_boundary_policy_has_stable_identifiers() {
        for (label, expected) in [
            ("closed", TechnosphereBoundaryPolicy::Closed),
            ("open", TechnosphereBoundaryPolicy::Open),
            ("cutoff", TechnosphereBoundaryPolicy::Cutoff),
        ] {
            let parsed = TechnosphereBoundaryPolicy::parse(label).expect("known boundary policy");
            assert_eq!(parsed, expected);
            assert_eq!(parsed.as_str(), label);
        }
        assert!(TechnosphereBoundaryPolicy::Closed.requires_closure());
        assert!(!TechnosphereBoundaryPolicy::Open.requires_closure());
        assert!(!TechnosphereBoundaryPolicy::Cutoff.requires_closure());
        assert!(TechnosphereBoundaryPolicy::parse("implicit").is_err());
    }

    #[test]
    fn signed_coefficient_combines_direction_and_amount_sign() {
        assert_eq!(
            signed_coefficient(SignedFlowDirection::Output, 2.0).expect("output positive"),
            2.0
        );
        assert_eq!(
            signed_coefficient(SignedFlowDirection::Input, 2.0).expect("input positive"),
            -2.0
        );
        assert_eq!(
            signed_coefficient(SignedFlowDirection::Output, -2.0).expect("output negative"),
            -2.0
        );
        assert_eq!(
            signed_coefficient(SignedFlowDirection::Input, -2.0).expect("input negative"),
            2.0
        );
        assert!(signed_coefficient(SignedFlowDirection::Output, f64::NAN).is_err());
        assert!(signed_coefficient(SignedFlowDirection::Input, f64::INFINITY).is_err());
    }

    #[test]
    fn reference_normalization_retains_the_signed_unit_pivot() {
        for (direction, amount, expected) in [
            (SignedFlowDirection::Output, 1_000.0, 1.0),
            (SignedFlowDirection::Input, 1_000.0, -1.0),
            (SignedFlowDirection::Output, -1_000.0, -1.0),
            (SignedFlowDirection::Input, -1_000.0, 1.0),
        ] {
            let raw = signed_coefficient(direction, amount).expect("signed coefficient");
            let pivot = normalize_reference_coefficient(raw).expect("reference pivot");
            assert_eq!(pivot.scale, 0.001);
            assert_eq!(pivot.coefficient, expected);
        }
        assert!(normalize_reference_coefficient(0.0).is_err());
        assert!(normalize_reference_coefficient(f64::NEG_INFINITY).is_err());
    }

    #[test]
    fn opposite_sign_reference_resolves_to_positive_activity() {
        let positive_provider = resolve_weighted_balance(
            -2.0,
            &[WeightedReferencePort {
                reference_coefficient: 1.0,
                routing_weight: 1.0,
            }],
        )
        .expect("negative residual against positive reference");
        assert_eq!(positive_provider.contributions[0].activity_requirement, 2.0);
        assert_eq!(positive_provider.closure_residual, 0.0);

        let negative_provider = resolve_weighted_balance(
            3.0,
            &[WeightedReferencePort {
                reference_coefficient: -1.0,
                routing_weight: 1.0,
            }],
        )
        .expect("positive residual against negative reference");
        assert_eq!(negative_provider.contributions[0].activity_requirement, 3.0);
        assert_eq!(negative_provider.closure_residual, 0.0);
    }

    #[test]
    fn same_sign_reference_is_not_a_balance_candidate() {
        assert!(
            resolve_weighted_balance(
                2.0,
                &[WeightedReferencePort {
                    reference_coefficient: 1.0,
                    routing_weight: 1.0,
                }],
            )
            .is_err()
        );
        assert!(
            resolve_weighted_balance(
                -2.0,
                &[WeightedReferencePort {
                    reference_coefficient: -1.0,
                    routing_weight: 1.0,
                }],
            )
            .is_err()
        );
    }

    #[test]
    fn multi_provider_weights_conserve_signed_flow() {
        let balance = resolve_weighted_balance(
            10.0,
            &[
                WeightedReferencePort {
                    reference_coefficient: -1.0,
                    routing_weight: 0.2,
                },
                WeightedReferencePort {
                    reference_coefficient: -1.0,
                    routing_weight: 0.3,
                },
                WeightedReferencePort {
                    reference_coefficient: -1.0,
                    routing_weight: 0.5,
                },
            ],
        )
        .expect("weighted balance");

        assert_eq!(balance.contributions.len(), 3);
        assert_eq!(balance.contributions[0].activity_requirement, 2.0);
        assert_eq!(balance.contributions[1].activity_requirement, 3.0);
        assert_eq!(balance.contributions[2].activity_requirement, 5.0);
        assert_eq!(balance.closure_residual, 0.0);
    }

    #[test]
    fn waste_input_and_negative_waste_output_references_are_equivalent() {
        let input_reference = normalize_reference_coefficient(
            signed_coefficient(SignedFlowDirection::Input, 1_000.0)
                .expect("waste input coefficient"),
        )
        .expect("waste input pivot");
        let negative_output_reference = normalize_reference_coefficient(
            signed_coefficient(SignedFlowDirection::Output, -1_000.0)
                .expect("negative waste output coefficient"),
        )
        .expect("negative waste output pivot");

        assert_eq!(input_reference, negative_output_reference);
        for pivot in [input_reference, negative_output_reference] {
            let balance = resolve_weighted_balance(
                5.0,
                &[WeightedReferencePort {
                    reference_coefficient: pivot.coefficient,
                    routing_weight: 1.0,
                }],
            )
            .expect("waste balance");
            assert_eq!(balance.contributions[0].activity_requirement, 5.0);
        }
    }
}
