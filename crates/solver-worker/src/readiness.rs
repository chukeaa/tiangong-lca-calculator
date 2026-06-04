#![allow(
    clippy::cast_precision_loss,
    clippy::missing_const_for_fn,
    clippy::module_name_repetitions,
    clippy::struct_excessive_bools,
    clippy::struct_field_names,
    clippy::too_many_lines
)]

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use solver_core::{
    ModelSparseData, NumericOptions, SolveOptions, SolverError, SolverService, ValidationReport,
    ValidationStatus,
};
use uuid::Uuid;

use crate::compiled_graph::{
    CompiledGraph, CompiledProviderCandidate, CompiledProviderCandidateEligibility,
    CompiledProviderDecision, CompiledProviderDecisionKind, CompiledProviderFailureReason,
    CompiledProviderGeographyTier, CompiledProviderOutputAllocationState,
    CompiledProviderResolutionStrategy, CompiledProviderSupplyRegionSource,
};
use crate::snapshot_artifacts::{SnapshotBuildConfig, SnapshotCoverageReport};

const REPORT_SCHEMA_VERSION: &str = "matrix_readiness_report.v1";
const INPUT_SCHEMA_VERSION: &str = "matrix_readiness_input.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessStatus {
    Passed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    Info,
    Warning,
    Blocker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComputeAnomalyPolicy {
    Ignore,
    Warning,
    Blocker,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MatrixReadinessPolicy {
    #[serde(default = "default_min_provider_write_pct")]
    pub min_provider_write_pct: f64,
    #[serde(default)]
    pub max_unmatched_no_provider: i64,
    #[serde(default)]
    pub max_multi_unresolved: i64,
    #[serde(default)]
    pub allow_equal_fallback: bool,
    #[serde(default)]
    pub allow_medium_singular_risk: bool,
    #[serde(default)]
    pub allow_high_singular_risk: bool,
    #[serde(default = "default_true")]
    pub require_lcia_factors: bool,
    #[serde(default = "default_true")]
    pub run_factorization: bool,
    #[serde(default = "default_sample_solve_unit_limit")]
    pub sample_solve_unit_limit: usize,
    #[serde(default = "default_negative_lcia_policy")]
    pub negative_lcia_policy: ComputeAnomalyPolicy,
    #[serde(default = "default_negative_lcia_epsilon")]
    pub negative_lcia_epsilon: f64,
}

impl Default for MatrixReadinessPolicy {
    fn default() -> Self {
        Self {
            min_provider_write_pct: default_min_provider_write_pct(),
            max_unmatched_no_provider: 0,
            max_multi_unresolved: 0,
            allow_equal_fallback: false,
            allow_medium_singular_risk: false,
            allow_high_singular_risk: false,
            require_lcia_factors: true,
            run_factorization: true,
            sample_solve_unit_limit: default_sample_solve_unit_limit(),
            negative_lcia_policy: default_negative_lcia_policy(),
            negative_lcia_epsilon: default_negative_lcia_epsilon(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixReadinessInput {
    #[serde(default = "default_input_schema_version")]
    pub schema_version: String,
    #[serde(default)]
    pub snapshot_id: Option<Uuid>,
    #[serde(default)]
    pub config: Option<SnapshotBuildConfig>,
    pub coverage: SnapshotCoverageReport,
    pub payload: ModelSparseData,
    #[serde(default)]
    pub compiled_graph: Option<CompiledGraph>,
    #[serde(default)]
    pub policy: MatrixReadinessPolicy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixReadinessReport {
    pub schema_version: String,
    pub generated_at_utc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<Uuid>,
    pub status: ReadinessStatus,
    pub next_action: String,
    pub policy: MatrixReadinessPolicy,
    pub metrics: MatrixReadinessMetrics,
    pub provider_evidence: Vec<ProviderDecisionEvidence>,
    pub findings: Vec<ReadinessFinding>,
    pub blockers: Vec<ReadinessFinding>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixReadinessMetrics {
    pub provider_closure: ProviderClosureMetrics,
    pub graph_readiness: GraphReadinessMetrics,
    pub compute_stability: ComputeStabilityMetrics,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderClosureMetrics {
    pub input_edges_total: i64,
    pub a_input_edges_written: i64,
    pub a_write_pct: f64,
    pub provider_present_resolved_pct: f64,
    pub unmatched_no_provider: i64,
    pub matched_multi_unresolved: i64,
    pub matched_multi_fallback_equal: i64,
    pub provider_evidence_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphReadinessMetrics {
    pub process_count: i64,
    pub flow_count: i64,
    pub impact_count: i64,
    pub a_nnz: i64,
    pub b_nnz: i64,
    pub c_nnz: i64,
    pub m_nnz_estimated: i64,
    pub m_sparsity_estimated: f64,
    pub reference_missing_count: i64,
    pub reference_invalid_count: i64,
    pub allocation_fraction_missing_count: i64,
    pub allocation_fraction_invalid_count: i64,
    pub singular_risk_level: String,
    pub m_zero_diagonal_count: i64,
    pub m_min_abs_diagonal: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComputeStabilityMetrics {
    pub factorization_checked: bool,
    pub factorization_ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<ValidationReport>,
    pub sampled_unit_solves: usize,
    pub non_finite_value_count: usize,
    pub negative_lcia_value_count: usize,
    pub min_lcia_value: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderDecisionEvidence {
    pub consumer_idx: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_process_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_process_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_process_name: Option<String>,
    pub flow_id: Uuid,
    pub candidate_provider_count: i32,
    pub matched_provider_count: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_strategy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    pub ambiguity: String,
    pub confidence: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub geography_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supply_region_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supply_region_location: Option<String>,
    pub candidates: Vec<ProviderCandidateEvidence>,
    pub allocations: Vec<ProviderAllocationEvidence>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderCandidateEvidence {
    pub provider_idx: i32,
    pub provider_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_exchange_internal_id: Option<String>,
    pub output_exchange_is_reference: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_normalized_amount: Option<f64>,
    pub output_allocation_state: String,
    pub eligibility: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_year: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annual_supply_or_production_volume: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderAllocationEvidence {
    pub provider_idx: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<Uuid>,
    pub weight: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadinessFinding {
    pub code: String,
    pub severity: FindingSeverity,
    pub message: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub details: Value,
}

pub fn verify_matrix_readiness(input: &MatrixReadinessInput) -> MatrixReadinessReport {
    let mut findings = Vec::new();
    let mut blockers = Vec::new();
    let provider_evidence = input
        .compiled_graph
        .as_ref()
        .map_or_else(Vec::new, provider_evidence_from_graph);

    add_provider_findings(input, &mut findings, &mut blockers);
    add_graph_findings(input, &mut findings, &mut blockers);

    let compute_stability = if input.policy.run_factorization {
        run_compute_stability(&input.payload, input.policy, &mut findings, &mut blockers)
    } else {
        ComputeStabilityMetrics {
            factorization_checked: false,
            factorization_ready: false,
            validation: None,
            sampled_unit_solves: 0,
            non_finite_value_count: 0,
            negative_lcia_value_count: 0,
            min_lcia_value: None,
        }
    };

    let metrics = MatrixReadinessMetrics {
        provider_closure: ProviderClosureMetrics {
            input_edges_total: input.coverage.matching.input_edges_total,
            a_input_edges_written: input.coverage.matching.a_input_edges_written,
            a_write_pct: input.coverage.matching.a_write_pct,
            provider_present_resolved_pct: input.coverage.matching.provider_present_resolved_pct,
            unmatched_no_provider: input.coverage.matching.unmatched_no_provider,
            matched_multi_unresolved: input.coverage.matching.matched_multi_unresolved,
            matched_multi_fallback_equal: input.coverage.matching.matched_multi_fallback_equal,
            provider_evidence_count: provider_evidence.len(),
        },
        graph_readiness: GraphReadinessMetrics {
            process_count: input.coverage.matrix_scale.process_count,
            flow_count: input.coverage.matrix_scale.flow_count,
            impact_count: input.coverage.matrix_scale.impact_count,
            a_nnz: input.coverage.matrix_scale.a_nnz,
            b_nnz: input.coverage.matrix_scale.b_nnz,
            c_nnz: input.coverage.matrix_scale.c_nnz,
            m_nnz_estimated: input.coverage.matrix_scale.m_nnz_estimated,
            m_sparsity_estimated: input.coverage.matrix_scale.m_sparsity_estimated,
            reference_missing_count: input.coverage.reference.missing_reference_count,
            reference_invalid_count: input.coverage.reference.invalid_reference_count,
            allocation_fraction_missing_count: input
                .coverage
                .allocation
                .allocation_fraction_missing_count,
            allocation_fraction_invalid_count: input
                .coverage
                .allocation
                .allocation_fraction_invalid_count,
            singular_risk_level: input.coverage.singular_risk.risk_level.clone(),
            m_zero_diagonal_count: input.coverage.singular_risk.m_zero_diagonal_count,
            m_min_abs_diagonal: input.coverage.singular_risk.m_min_abs_diagonal,
        },
        compute_stability,
    };

    let status = if blockers.is_empty() {
        ReadinessStatus::Passed
    } else {
        ReadinessStatus::Failed
    };
    let next_action = next_action(&blockers, &findings);

    MatrixReadinessReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        generated_at_utc: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        snapshot_id: input.snapshot_id,
        status,
        next_action,
        policy: input.policy,
        metrics,
        provider_evidence,
        findings,
        blockers,
    }
}

fn add_provider_findings(
    input: &MatrixReadinessInput,
    findings: &mut Vec<ReadinessFinding>,
    blockers: &mut Vec<ReadinessFinding>,
) {
    let matching = &input.coverage.matching;
    if matching.a_write_pct < input.policy.min_provider_write_pct {
        push_blocker(
            blockers,
            "provider_closure_write_pct_below_policy",
            format!(
                "provider closure wrote {}% of input edges; policy requires at least {}%",
                matching.a_write_pct, input.policy.min_provider_write_pct
            ),
            json!({
                "a_write_pct": matching.a_write_pct,
                "min_provider_write_pct": input.policy.min_provider_write_pct
            }),
        );
    }
    if matching.unmatched_no_provider > input.policy.max_unmatched_no_provider {
        push_blocker(
            blockers,
            "provider_closure_unmatched",
            format!(
                "{} input edges have no provider; policy allows {}",
                matching.unmatched_no_provider, input.policy.max_unmatched_no_provider
            ),
            json!({
                "unmatched_no_provider": matching.unmatched_no_provider,
                "max_unmatched_no_provider": input.policy.max_unmatched_no_provider
            }),
        );
    }
    if matching.matched_multi_unresolved > input.policy.max_multi_unresolved {
        push_blocker(
            blockers,
            "provider_closure_multi_unresolved",
            format!(
                "{} multi-provider edges are unresolved; policy allows {}",
                matching.matched_multi_unresolved, input.policy.max_multi_unresolved
            ),
            json!({
                "matched_multi_unresolved": matching.matched_multi_unresolved,
                "max_multi_unresolved": input.policy.max_multi_unresolved
            }),
        );
    }
    if !input.policy.allow_equal_fallback && matching.matched_multi_fallback_equal > 0 {
        push_blocker(
            blockers,
            "provider_closure_equal_fallback",
            format!(
                "{} multi-provider edges fell back to equal allocation",
                matching.matched_multi_fallback_equal
            ),
            json!({ "matched_multi_fallback_equal": matching.matched_multi_fallback_equal }),
        );
    }
    if matching.input_edges_total == 0 {
        findings.push(finding(
            "provider_closure_no_input_edges",
            FindingSeverity::Warning,
            "snapshot has no input edges to check for provider closure",
            Value::Null,
        ));
    }
}

fn add_graph_findings(
    input: &MatrixReadinessInput,
    findings: &mut Vec<ReadinessFinding>,
    blockers: &mut Vec<ReadinessFinding>,
) {
    let coverage = &input.coverage;
    if coverage.reference.missing_reference_count > 0
        || coverage.reference.invalid_reference_count > 0
    {
        push_blocker(
            blockers,
            "reference_normalization_not_closed",
            "quantitative reference normalization has missing or invalid references".to_owned(),
            json!({
                "missing_reference_count": coverage.reference.missing_reference_count,
                "invalid_reference_count": coverage.reference.invalid_reference_count
            }),
        );
    }
    if coverage.allocation.allocation_fraction_invalid_count > 0 {
        push_blocker(
            blockers,
            "allocation_fraction_invalid",
            "allocation fractions contain invalid values".to_owned(),
            json!({
                "allocation_fraction_invalid_count": coverage.allocation.allocation_fraction_invalid_count
            }),
        );
    }
    if coverage.allocation.allocation_fraction_missing_count > 0 {
        findings.push(finding(
            "allocation_fraction_missing",
            FindingSeverity::Warning,
            "some exchanges are missing explicit allocation fractions",
            json!({
                "allocation_fraction_missing_count": coverage.allocation.allocation_fraction_missing_count
            }),
        ));
    }

    match coverage.singular_risk.risk_level.as_str() {
        "high" if !input.policy.allow_high_singular_risk => push_blocker(
            blockers,
            "singular_risk_high",
            "matrix singular risk is high".to_owned(),
            json!({
                "m_zero_diagonal_count": coverage.singular_risk.m_zero_diagonal_count,
                "m_min_abs_diagonal": coverage.singular_risk.m_min_abs_diagonal
            }),
        ),
        "medium" if !input.policy.allow_medium_singular_risk => push_blocker(
            blockers,
            "singular_risk_medium",
            "matrix singular risk is medium and requires review by policy".to_owned(),
            json!({
                "prefilter_diag_abs_ge_cutoff": coverage.singular_risk.prefilter_diag_abs_ge_cutoff,
                "postfilter_a_diag_abs_ge_cutoff": coverage.singular_risk.postfilter_a_diag_abs_ge_cutoff
            }),
        ),
        "low" => {}
        other => findings.push(finding(
            "singular_risk_observed",
            FindingSeverity::Info,
            format!("matrix singular risk level is {other}"),
            json!({ "risk_level": other }),
        )),
    }

    if input.policy.require_lcia_factors && coverage.matrix_scale.c_nnz == 0 {
        push_blocker(
            blockers,
            "lcia_factors_missing",
            "LCIA factors are required but no characterization factors were written".to_owned(),
            json!({
                "impact_count": coverage.matrix_scale.impact_count,
                "c_nnz": coverage.matrix_scale.c_nnz
            }),
        );
    }
    if coverage.matrix_scale.b_nnz == 0 {
        findings.push(finding(
            "biosphere_entries_missing",
            FindingSeverity::Warning,
            "snapshot has no biosphere entries; compute results may be uninformative",
            Value::Null,
        ));
    }
}

fn run_compute_stability(
    payload: &ModelSparseData,
    policy: MatrixReadinessPolicy,
    findings: &mut Vec<ReadinessFinding>,
    blockers: &mut Vec<ReadinessFinding>,
) -> ComputeStabilityMetrics {
    let service = SolverService::new();
    let prepare = service.prepare(payload, NumericOptions::default());
    let mut metrics = ComputeStabilityMetrics {
        factorization_checked: true,
        factorization_ready: false,
        validation: None,
        sampled_unit_solves: 0,
        non_finite_value_count: 0,
        negative_lcia_value_count: 0,
        min_lcia_value: None,
    };

    match prepare {
        Ok(result) => {
            metrics.factorization_ready = true;
            metrics.validation = Some(result.diagnostics.validation.clone());
            if result.diagnostics.validation.status == ValidationStatus::WarningNearSingular {
                findings.push(finding(
                    "matrix_validation_warning",
                    FindingSeverity::Warning,
                    "factorization matrix validation emitted warnings",
                    json!({ "messages": result.diagnostics.validation.messages }),
                ));
            }
        }
        Err(error) => {
            if let SolverError::ValidationFailed(report) = &error {
                metrics.validation = Some(report.clone());
            }
            push_blocker(
                blockers,
                "factorization_not_ready",
                format!("factorization failed: {error}"),
                Value::Null,
            );
            return metrics;
        }
    }

    let sample_count = usize::try_from(payload.process_count)
        .unwrap_or_default()
        .min(policy.sample_solve_unit_limit);
    for process_idx in 0..sample_count {
        let mut rhs = vec![0.0_f64; usize::try_from(payload.process_count).unwrap_or_default()];
        rhs[process_idx] = 1.0;
        match service.solve_one(
            payload.model_version,
            NumericOptions::default(),
            &rhs,
            SolveOptions {
                return_x: true,
                return_g: true,
                return_h: true,
            },
        ) {
            Ok(result) => {
                metrics.sampled_unit_solves += 1;
                observe_values(result.x.as_deref(), &mut metrics);
                observe_values(result.g.as_deref(), &mut metrics);
                observe_lcia_values(result.h.as_deref(), policy, &mut metrics);
            }
            Err(error) => push_blocker(
                blockers,
                "sample_unit_solve_failed",
                format!("unit demand solve failed for process index {process_idx}: {error}"),
                json!({ "process_idx": process_idx }),
            ),
        }
    }

    if metrics.non_finite_value_count > 0 {
        push_blocker(
            blockers,
            "compute_non_finite_values",
            "sample unit solve produced non-finite values".to_owned(),
            json!({ "non_finite_value_count": metrics.non_finite_value_count }),
        );
    }
    if metrics.negative_lcia_value_count > 0 {
        let severity = match policy.negative_lcia_policy {
            ComputeAnomalyPolicy::Ignore => FindingSeverity::Info,
            ComputeAnomalyPolicy::Warning => FindingSeverity::Warning,
            ComputeAnomalyPolicy::Blocker => FindingSeverity::Blocker,
        };
        let item = finding(
            "negative_lcia_values",
            severity,
            "sample unit solve produced negative LCIA values below policy epsilon",
            json!({
                "negative_lcia_value_count": metrics.negative_lcia_value_count,
                "min_lcia_value": metrics.min_lcia_value,
                "negative_lcia_epsilon": policy.negative_lcia_epsilon
            }),
        );
        if severity == FindingSeverity::Blocker {
            blockers.push(item);
        } else {
            findings.push(item);
        }
    }

    metrics
}

fn observe_values(values: Option<&[f64]>, metrics: &mut ComputeStabilityMetrics) {
    for value in values.unwrap_or_default() {
        if !value.is_finite() {
            metrics.non_finite_value_count += 1;
        }
    }
}

fn observe_lcia_values(
    values: Option<&[f64]>,
    policy: MatrixReadinessPolicy,
    metrics: &mut ComputeStabilityMetrics,
) {
    for value in values.unwrap_or_default() {
        if !value.is_finite() {
            metrics.non_finite_value_count += 1;
            continue;
        }
        metrics.min_lcia_value = Some(
            metrics
                .min_lcia_value
                .map_or(*value, |current| current.min(*value)),
        );
        if *value < -policy.negative_lcia_epsilon {
            metrics.negative_lcia_value_count += 1;
        }
    }
}

fn provider_evidence_from_graph(graph: &CompiledGraph) -> Vec<ProviderDecisionEvidence> {
    let processes = graph
        .processes
        .iter()
        .map(|process| (process.process_idx, process))
        .collect::<BTreeMap<_, _>>();

    graph
        .provider_decisions
        .iter()
        .map(|decision| provider_decision_evidence(decision, &processes))
        .collect()
}

fn provider_decision_evidence(
    decision: &CompiledProviderDecision,
    processes: &BTreeMap<i32, &crate::compiled_graph::CompiledProcess>,
) -> ProviderDecisionEvidence {
    let consumer = processes.get(&decision.consumer_idx).copied();
    ProviderDecisionEvidence {
        consumer_idx: decision.consumer_idx,
        consumer_process_id: consumer.map(|process| process.process_id),
        consumer_process_version: consumer.map(|process| process.process_version.clone()),
        consumer_process_name: consumer.and_then(|process| process.process_name.clone()),
        flow_id: decision.flow_id,
        candidate_provider_count: decision.candidate_provider_count,
        matched_provider_count: decision.matched_provider_count,
        decision_kind: decision.decision_kind.map(provider_decision_kind_label),
        resolution_strategy: decision
            .resolution_strategy
            .map(provider_resolution_strategy_label),
        failure_reason: decision.failure_reason.map(provider_failure_reason_label),
        ambiguity: provider_ambiguity_label(decision).to_owned(),
        confidence: provider_confidence_label(decision).to_owned(),
        geography_tier: decision.geography_tier.map(provider_geography_tier_label),
        supply_region_source: decision
            .supply_region_source
            .map(provider_supply_region_source_label),
        supply_region_location: decision.supply_region_location.clone(),
        candidates: decision.candidates.iter().map(candidate_evidence).collect(),
        allocations: decision
            .allocations
            .iter()
            .map(|allocation| ProviderAllocationEvidence {
                provider_idx: allocation.provider_idx,
                provider_id: processes
                    .get(&allocation.provider_idx)
                    .map(|process| process.process_id),
                weight: allocation.weight,
            })
            .collect(),
    }
}

fn candidate_evidence(candidate: &CompiledProviderCandidate) -> ProviderCandidateEvidence {
    ProviderCandidateEvidence {
        provider_idx: candidate.provider_idx,
        provider_id: candidate.provider_id,
        output_exchange_internal_id: candidate.output_exchange_internal_id.clone(),
        output_exchange_is_reference: candidate.output_exchange_is_reference,
        output_normalized_amount: candidate.output_normalized_amount,
        output_allocation_state: provider_output_allocation_state_label(
            candidate.output_allocation_state,
        ),
        eligibility: provider_candidate_eligibility_label(candidate.eligibility),
        process_name: candidate.process_name.clone(),
        location: candidate.location.clone(),
        reference_year: candidate.reference_year,
        annual_supply_or_production_volume: candidate.annual_supply_or_production_volume,
    }
}

fn next_action(blockers: &[ReadinessFinding], findings: &[ReadinessFinding]) -> String {
    if blockers
        .iter()
        .any(|item| item.code.starts_with("provider_closure"))
    {
        "repair_provider_closure_then_recheck".to_owned()
    } else if blockers
        .iter()
        .any(|item| item.code.starts_with("factorization") || item.code.starts_with("compute"))
    {
        "repair_compute_stability_then_recheck".to_owned()
    } else if blockers.iter().any(|item| item.code.starts_with("lcia")) {
        "repair_lcia_factors_then_recheck".to_owned()
    } else if !blockers.is_empty() {
        "repair_graph_readiness_then_recheck".to_owned()
    } else if findings
        .iter()
        .any(|item| item.severity == FindingSeverity::Warning)
    {
        "manual_review_warnings".to_owned()
    } else {
        "publish_ready".to_owned()
    }
}

fn push_blocker(blockers: &mut Vec<ReadinessFinding>, code: &str, message: String, details: Value) {
    blockers.push(finding(code, FindingSeverity::Blocker, message, details));
}

fn finding(
    code: &str,
    severity: FindingSeverity,
    message: impl Into<String>,
    details: Value,
) -> ReadinessFinding {
    ReadinessFinding {
        code: code.to_owned(),
        severity,
        message: message.into(),
        details,
    }
}

fn provider_ambiguity_label(decision: &CompiledProviderDecision) -> &'static str {
    match decision.decision_kind {
        Some(CompiledProviderDecisionKind::UniqueProvider) => "none",
        Some(CompiledProviderDecisionKind::MultiResolved) => "multi_provider_resolved",
        Some(CompiledProviderDecisionKind::MultiUnresolved) => "multi_provider_unresolved",
        Some(CompiledProviderDecisionKind::NoProvider) => "no_provider",
        None => "unknown",
    }
}

fn provider_confidence_label(decision: &CompiledProviderDecision) -> &'static str {
    match (
        decision.decision_kind,
        decision.used_equal_fallback,
        decision.failure_reason,
    ) {
        (Some(CompiledProviderDecisionKind::UniqueProvider), false, None) => "high",
        (Some(CompiledProviderDecisionKind::MultiResolved), false, None) => "medium",
        _ => "low",
    }
}

fn provider_decision_kind_label(kind: CompiledProviderDecisionKind) -> String {
    match kind {
        CompiledProviderDecisionKind::UniqueProvider => "unique_provider",
        CompiledProviderDecisionKind::MultiResolved => "multi_resolved",
        CompiledProviderDecisionKind::MultiUnresolved => "multi_unresolved",
        CompiledProviderDecisionKind::NoProvider => "no_provider",
    }
    .to_owned()
}

fn provider_resolution_strategy_label(strategy: CompiledProviderResolutionStrategy) -> String {
    match strategy {
        CompiledProviderResolutionStrategy::UniqueProvider => "unique_provider",
        CompiledProviderResolutionStrategy::BestProviderStrict => "best_provider_strict",
        CompiledProviderResolutionStrategy::SplitByEvidence => "split_by_evidence",
        CompiledProviderResolutionStrategy::SplitByProcessVolume => "split_by_process_volume",
        CompiledProviderResolutionStrategy::SplitEqual => "split_equal",
        CompiledProviderResolutionStrategy::SplitEqualFallback => "split_equal_fallback",
    }
    .to_owned()
}

fn provider_failure_reason_label(reason: CompiledProviderFailureReason) -> String {
    match reason {
        CompiledProviderFailureReason::NoProviderCandidates => "no_provider_candidates",
        CompiledProviderFailureReason::RejectedNonReferenceOnly => "rejected_non_reference_only",
        CompiledProviderFailureReason::RuleRequiresUniqueProvider => {
            "rule_requires_unique_provider"
        }
        CompiledProviderFailureReason::NoCandidateGeMinScore => "no_candidate_ge_min_score",
        CompiledProviderFailureReason::Top1BelowTop1MinScore => "top1_below_top1_min_score",
        CompiledProviderFailureReason::Top1Top2RatioTooClose => "top1_top2_ratio_too_close",
        CompiledProviderFailureReason::ScoreSumNonPositive => "score_sum_non_positive",
    }
    .to_owned()
}

fn provider_candidate_eligibility_label(
    eligibility: CompiledProviderCandidateEligibility,
) -> String {
    match eligibility {
        CompiledProviderCandidateEligibility::Unknown => "unknown",
        CompiledProviderCandidateEligibility::AcceptedReferenceOutput => {
            "accepted_reference_output"
        }
        CompiledProviderCandidateEligibility::RejectedNonReferenceOutput => {
            "rejected_non_reference_output"
        }
    }
    .to_owned()
}

fn provider_output_allocation_state_label(state: CompiledProviderOutputAllocationState) -> String {
    match state {
        CompiledProviderOutputAllocationState::Unknown => "unknown",
        CompiledProviderOutputAllocationState::Present => "present",
        CompiledProviderOutputAllocationState::Missing => "missing",
        CompiledProviderOutputAllocationState::Invalid => "invalid",
    }
    .to_owned()
}

fn provider_geography_tier_label(tier: CompiledProviderGeographyTier) -> String {
    match tier {
        CompiledProviderGeographyTier::LocalSubnational => "local_subnational",
        CompiledProviderGeographyTier::SameCountry => "same_country",
        CompiledProviderGeographyTier::SameRegion => "same_region",
        CompiledProviderGeographyTier::Global => "global",
        CompiledProviderGeographyTier::Other => "other",
    }
    .to_owned()
}

fn provider_supply_region_source_label(source: CompiledProviderSupplyRegionSource) -> String {
    match source {
        CompiledProviderSupplyRegionSource::ExchangeLocation => "exchange_location",
        CompiledProviderSupplyRegionSource::ConsumerProcessLocation => "consumer_process_location",
        CompiledProviderSupplyRegionSource::Unspecified => "unspecified",
    }
    .to_owned()
}

fn default_input_schema_version() -> String {
    INPUT_SCHEMA_VERSION.to_owned()
}

fn default_min_provider_write_pct() -> f64 {
    100.0
}

fn default_true() -> bool {
    true
}

fn default_sample_solve_unit_limit() -> usize {
    16
}

fn default_negative_lcia_policy() -> ComputeAnomalyPolicy {
    ComputeAnomalyPolicy::Warning
}

fn default_negative_lcia_epsilon() -> f64 {
    1e-12
}

#[cfg(test)]
mod tests {
    use solver_core::SparseTriplet;

    use super::*;
    use crate::compiled_graph::{
        CompiledAllocationStats, CompiledFlow, CompiledFlowKind, CompiledMatchingStats,
        CompiledProcess, CompiledProviderAllocation, CompiledReferenceStats,
    };
    use crate::graph_types::ScopeProcessPartition;
    use crate::snapshot_artifacts::{
        SNAPSHOT_COVERAGE_SCHEMA_VERSION, SnapshotAllocationCoverage, SnapshotCandidateSummary,
        SnapshotGapSummary, SnapshotGeographySummary, SnapshotMatchingCoverage,
        SnapshotMatrixScale, SnapshotProviderDecisionDiagnostics, SnapshotReferenceCoverage,
        SnapshotResolutionSummary, SnapshotSingularRisk, SnapshotVolumeWeightSummary,
    };

    #[test]
    fn passes_matrix_ready_fixture_with_provider_evidence() {
        let snapshot_id = Uuid::new_v4();
        let provider_id = Uuid::new_v4();
        let consumer_id = Uuid::new_v4();
        let flow_id = Uuid::new_v4();
        let input = fixture_input(snapshot_id, provider_id, consumer_id, flow_id, true);

        let report = verify_matrix_readiness(&input);

        assert_eq!(report.status, ReadinessStatus::Passed);
        assert_eq!(report.next_action, "publish_ready");
        assert!(report.blockers.is_empty());
        assert_eq!(report.provider_evidence.len(), 1);
        assert_eq!(
            report.provider_evidence[0].candidates[0].provider_id,
            provider_id
        );
        assert_eq!(report.provider_evidence[0].confidence, "high");
        assert!(report.metrics.compute_stability.factorization_ready);
        assert_eq!(report.metrics.compute_stability.sampled_unit_solves, 2);
    }

    #[test]
    fn blocks_known_provider_closure_failure() {
        let snapshot_id = Uuid::new_v4();
        let provider_id = Uuid::new_v4();
        let consumer_id = Uuid::new_v4();
        let flow_id = Uuid::new_v4();
        let input = fixture_input(snapshot_id, provider_id, consumer_id, flow_id, false);

        let report = verify_matrix_readiness(&input);

        assert_eq!(report.status, ReadinessStatus::Failed);
        assert_eq!(report.next_action, "repair_provider_closure_then_recheck");
        assert!(
            report
                .blockers
                .iter()
                .any(|blocker| blocker.code == "provider_closure_unmatched")
        );
        assert_eq!(report.provider_evidence[0].ambiguity, "no_provider");
        assert_eq!(report.provider_evidence[0].candidates.len(), 0);
    }

    fn fixture_input(
        snapshot_id: Uuid,
        provider_id: Uuid,
        consumer_id: Uuid,
        flow_id: Uuid,
        provider_closed: bool,
    ) -> MatrixReadinessInput {
        let matching = if provider_closed {
            SnapshotMatchingCoverage {
                input_edges_total: 1,
                matched_unique_provider: 1,
                matched_multi_provider: 0,
                unmatched_no_provider: 0,
                matched_multi_resolved: 0,
                matched_multi_unresolved: 0,
                matched_multi_fallback_equal: 0,
                a_input_edges_written: 1,
                a_write_pct: 100.0,
                provider_present_resolved_pct: 100.0,
                unique_provider_match_pct: 100.0,
                any_provider_match_pct: 100.0,
                provider_decision_diagnostics: SnapshotProviderDecisionDiagnostics::default(),
                candidate_summary: SnapshotCandidateSummary::default(),
                resolution_summary: SnapshotResolutionSummary::default(),
                geography_summary: SnapshotGeographySummary::default(),
                volume_weight_summary: SnapshotVolumeWeightSummary::default(),
                gap_summary: SnapshotGapSummary::default(),
            }
        } else {
            SnapshotMatchingCoverage {
                input_edges_total: 1,
                matched_unique_provider: 0,
                matched_multi_provider: 0,
                unmatched_no_provider: 1,
                matched_multi_resolved: 0,
                matched_multi_unresolved: 0,
                matched_multi_fallback_equal: 0,
                a_input_edges_written: 0,
                a_write_pct: 0.0,
                provider_present_resolved_pct: 0.0,
                unique_provider_match_pct: 0.0,
                any_provider_match_pct: 0.0,
                provider_decision_diagnostics: SnapshotProviderDecisionDiagnostics::default(),
                candidate_summary: SnapshotCandidateSummary::default(),
                resolution_summary: SnapshotResolutionSummary::default(),
                geography_summary: SnapshotGeographySummary::default(),
                volume_weight_summary: SnapshotVolumeWeightSummary::default(),
                gap_summary: SnapshotGapSummary::default(),
            }
        };

        MatrixReadinessInput {
            schema_version: INPUT_SCHEMA_VERSION.to_owned(),
            snapshot_id: Some(snapshot_id),
            config: None,
            coverage: SnapshotCoverageReport {
                schema_version: SNAPSHOT_COVERAGE_SCHEMA_VERSION.to_owned(),
                matching,
                reference: SnapshotReferenceCoverage {
                    process_total: 2,
                    normalized_process_count: 2,
                    missing_reference_count: 0,
                    invalid_reference_count: 0,
                },
                allocation: SnapshotAllocationCoverage {
                    exchange_total: 2,
                    allocation_fraction_present_pct: 100.0,
                    allocation_fraction_missing_count: 0,
                    allocation_fraction_invalid_count: 0,
                },
                singular_risk: SnapshotSingularRisk {
                    risk_level: "low".to_owned(),
                    prefilter_diag_abs_ge_cutoff: 0,
                    postfilter_a_diag_abs_ge_cutoff: 0,
                    m_zero_diagonal_count: 0,
                    m_min_abs_diagonal: 1.0,
                },
                matrix_scale: SnapshotMatrixScale {
                    process_count: 2,
                    flow_count: 1,
                    impact_count: 1,
                    a_nnz: i64::from(provider_closed),
                    b_nnz: 2,
                    c_nnz: 1,
                    m_nnz_estimated: if provider_closed { 3 } else { 2 },
                    m_sparsity_estimated: if provider_closed { 0.25 } else { 0.5 },
                },
            },
            payload: ModelSparseData {
                model_version: snapshot_id,
                process_count: 2,
                flow_count: 1,
                impact_count: 1,
                technosphere_entries: if provider_closed {
                    vec![SparseTriplet {
                        row: 0,
                        col: 1,
                        value: 0.1,
                    }]
                } else {
                    Vec::new()
                },
                biosphere_entries: vec![
                    SparseTriplet {
                        row: 0,
                        col: 0,
                        value: 1.0,
                    },
                    SparseTriplet {
                        row: 0,
                        col: 1,
                        value: 2.0,
                    },
                ],
                characterization_factors: vec![SparseTriplet {
                    row: 0,
                    col: 0,
                    value: 0.5,
                }],
            },
            compiled_graph: Some(CompiledGraph {
                processes: vec![
                    CompiledProcess {
                        process_idx: 0,
                        process_id: provider_id,
                        process_version: "01.00.000".to_owned(),
                        process_name: Some("provider".to_owned()),
                        model_id: None,
                        location: Some("CN".to_owned()),
                        reference_year: Some(2024),
                        annual_supply_or_production_volume: None,
                        partition: ScopeProcessPartition::Public,
                    },
                    CompiledProcess {
                        process_idx: 1,
                        process_id: consumer_id,
                        process_version: "01.00.000".to_owned(),
                        process_name: Some("consumer".to_owned()),
                        model_id: None,
                        location: Some("CN".to_owned()),
                        reference_year: Some(2024),
                        annual_supply_or_production_volume: None,
                        partition: ScopeProcessPartition::Private,
                    },
                ],
                flows: vec![CompiledFlow {
                    flow_idx: 0,
                    flow_id,
                    kind: CompiledFlowKind::Product,
                }],
                provider_outputs: Vec::new(),
                provider_decisions: vec![provider_decision(provider_id, flow_id, provider_closed)],
                technosphere_edges: Vec::new(),
                biosphere_edges: Vec::new(),
                reference_stats: CompiledReferenceStats::default(),
                allocation_stats: CompiledAllocationStats::default(),
                matching_stats: CompiledMatchingStats::default(),
            }),
            policy: MatrixReadinessPolicy::default(),
        }
    }

    fn provider_decision(
        provider_id: Uuid,
        flow_id: Uuid,
        provider_closed: bool,
    ) -> CompiledProviderDecision {
        if provider_closed {
            CompiledProviderDecision {
                consumer_idx: 1,
                flow_id,
                candidate_provider_count: 1,
                matched_provider_count: 1,
                candidates: vec![CompiledProviderCandidate {
                    provider_idx: 0,
                    provider_id,
                    output_exchange_internal_id: Some("1".to_owned()),
                    output_exchange_is_reference: true,
                    output_normalized_amount: Some(1.0),
                    output_allocation_state: CompiledProviderOutputAllocationState::Present,
                    eligibility: CompiledProviderCandidateEligibility::AcceptedReferenceOutput,
                    process_name: Some("provider".to_owned()),
                    location: Some("CN".to_owned()),
                    reference_year: Some(2024),
                    annual_supply_or_production_volume: Some(10.0),
                }],
                decision_kind: Some(CompiledProviderDecisionKind::UniqueProvider),
                resolution_strategy: Some(CompiledProviderResolutionStrategy::UniqueProvider),
                failure_reason: None,
                used_equal_fallback: false,
                volume_fallback_to_one_count: 0,
                geography_tier: Some(CompiledProviderGeographyTier::SameCountry),
                supply_region_source: Some(
                    CompiledProviderSupplyRegionSource::ConsumerProcessLocation,
                ),
                supply_region_location: Some("CN".to_owned()),
                exchange_location_present: false,
                allocations: vec![CompiledProviderAllocation {
                    provider_idx: 0,
                    weight: 1.0,
                }],
            }
        } else {
            CompiledProviderDecision {
                consumer_idx: 1,
                flow_id,
                candidate_provider_count: 0,
                matched_provider_count: 0,
                candidates: Vec::new(),
                decision_kind: Some(CompiledProviderDecisionKind::NoProvider),
                resolution_strategy: None,
                failure_reason: Some(CompiledProviderFailureReason::NoProviderCandidates),
                used_equal_fallback: false,
                volume_fallback_to_one_count: 0,
                geography_tier: None,
                supply_region_source: Some(
                    CompiledProviderSupplyRegionSource::ConsumerProcessLocation,
                ),
                supply_region_location: Some("CN".to_owned()),
                exchange_location_present: false,
                allocations: Vec::new(),
            }
        }
    }
}
