#![allow(
    clippy::cast_precision_loss,
    clippy::missing_const_for_fn,
    clippy::module_name_repetitions,
    clippy::struct_excessive_bools,
    clippy::struct_field_names,
    clippy::too_many_lines
)]

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use solver_core::{
    ModelSparseData, NumericOptions, SolveOptions, SolverError, SolverService, SparseTriplet,
    ValidationReport,
};
use uuid::Uuid;

use crate::compiled_graph::{CompiledFlowKind, CompiledGraph};
use crate::readiness::{FindingSeverity, ReadinessFinding};
use crate::snapshot_artifacts::{SnapshotBuildConfig, SnapshotCoverageReport};

const INPUT_SCHEMA_VERSION: &str = "review_submit_gate_input.v1";
const REPORT_SCHEMA_VERSION: &str = "review_submit_gate_report.v1";
const DEFAULT_POLICY_PROFILE: &str = "review_submit_fast.v1";
const DETAIL_LIMIT: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewSubmitGateStatus {
    Passed,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReviewSubmitGatePolicy {
    #[serde(default = "default_policy_profile")]
    pub policy_profile: String,
    #[serde(default = "default_true")]
    pub require_revision_checksum_match: bool,
    /// Compatibility-only lifecycle policy projection. The worker numerical gate does not
    /// evaluate process state codes.
    #[serde(default = "default_allowed_scope_states")]
    pub allowed_scope_states: Vec<i32>,
    #[serde(default = "default_false")]
    pub block_equal_fallback: bool,
    #[serde(default = "default_false")]
    pub block_provider_volume_fallback: bool,
    #[serde(default = "default_true")]
    pub require_lcia_for_impact_submit: bool,
    #[serde(default = "default_true")]
    pub require_target_process_probe: bool,
    #[serde(default = "default_true")]
    pub run_factorization_probe: bool,
    #[serde(default = "default_target_probe_limit")]
    pub target_probe_limit: usize,
    #[serde(default = "default_zero_epsilon")]
    pub zero_diagonal_epsilon: f64,
    #[serde(default = "default_duplicate_value_epsilon")]
    pub duplicate_value_epsilon: f64,
    #[serde(default = "default_allocation_sum_epsilon")]
    pub allocation_sum_epsilon: f64,
    #[serde(default = "default_service_loop_epsilon")]
    pub service_loop_epsilon: f64,
}

impl Default for ReviewSubmitGatePolicy {
    fn default() -> Self {
        Self {
            policy_profile: default_policy_profile(),
            require_revision_checksum_match: true,
            allowed_scope_states: default_allowed_scope_states(),
            block_equal_fallback: false,
            block_provider_volume_fallback: false,
            require_lcia_for_impact_submit: false,
            require_target_process_probe: true,
            run_factorization_probe: true,
            target_probe_limit: default_target_probe_limit(),
            zero_diagonal_epsilon: default_zero_epsilon(),
            duplicate_value_epsilon: default_duplicate_value_epsilon(),
            allocation_sum_epsilon: default_allocation_sum_epsilon(),
            service_loop_epsilon: default_service_loop_epsilon(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewSubmitGateInput {
    #[serde(default = "default_input_schema_version")]
    pub schema_version: String,
    #[serde(default)]
    pub dataset_revision_id: Option<Uuid>,
    #[serde(default)]
    pub expected_revision_checksum: Option<String>,
    #[serde(default)]
    pub actual_revision_checksum: Option<String>,
    #[serde(default)]
    pub snapshot_id: Option<Uuid>,
    #[serde(default)]
    pub config: Option<SnapshotBuildConfig>,
    pub coverage: SnapshotCoverageReport,
    pub payload: ModelSparseData,
    #[serde(default)]
    pub compiled_graph: Option<CompiledGraph>,
    #[serde(default)]
    pub target_process_indices: Vec<i32>,
    #[serde(default)]
    pub process_records: Vec<ReviewProcessRecord>,
    #[serde(default)]
    pub policy: ReviewSubmitGatePolicy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReviewProcessRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_idx: Option<i32>,
    pub process_id: Uuid,
    pub process_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_exchange_id: Option<String>,
    #[serde(default)]
    pub exchanges: Vec<ReviewExchangeRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReviewExchangeRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exchange_id: Option<String>,
    pub flow_id: Uuid,
    pub direction: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocation_fraction: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReviewSubmitGateReport {
    pub schema_version: String,
    pub generated_at_utc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_revision_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<Uuid>,
    pub status: ReviewSubmitGateStatus,
    pub policy: ReviewSubmitGatePolicy,
    pub metrics: ReviewSubmitGateMetrics,
    pub blockers: Vec<ReadinessFinding>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReviewSubmitGateMetrics {
    pub revision: RevisionGateMetrics,
    pub process_scan: ProcessScanMetrics,
    pub provider_scan: ProviderScanMetrics,
    pub sparse_scan: SparseScanMetrics,
    pub probe: ProbeMetrics,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RevisionGateMetrics {
    pub checksum_checked: bool,
    pub checksum_matched: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProcessScanMetrics {
    pub process_records_total: usize,
    /// Compatibility-only report projection. Lifecycle states are not evaluated by the worker
    /// numerical gate, so this remains zero.
    pub invalid_scope_state_count: usize,
    pub duplicate_process_version_groups: usize,
    pub invalid_exchange_amount_count: usize,
    pub invalid_allocation_fraction_count: usize,
    pub missing_or_zero_reference_count: usize,
    pub duplicate_exchange_fingerprint_groups: usize,
    pub service_loop_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProviderScanMetrics {
    pub provider_decisions_total: usize,
    pub provider_missing_count: i64,
    pub provider_unresolved_count: i64,
    pub equal_fallback_count: i64,
    pub allocation_not_conserved_count: usize,
    pub volume_evidence_invalid_count: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SparseScanMetrics {
    pub zero_or_near_zero_diagonal_count: usize,
    pub duplicate_sparse_column_groups: usize,
    pub flow_lcia_semantic_mismatch_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProbeMetrics {
    pub factorization_checked: bool,
    pub factorization_ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<ValidationReport>,
    pub target_indices_requested: usize,
    pub target_indices_probed: usize,
    pub non_finite_value_count: usize,
}

#[must_use]
pub fn verify_review_submit_gate(input: &ReviewSubmitGateInput) -> ReviewSubmitGateReport {
    let mut blockers = Vec::new();
    let mut metrics = ReviewSubmitGateMetrics::default();

    check_revision_freshness(input, &mut metrics, &mut blockers);
    check_process_records(input, &mut metrics, &mut blockers);
    check_provider_closure(input, &mut metrics);
    check_flow_semantics(input, &mut metrics, &mut blockers);
    check_sparse_structure(input, &mut metrics, &mut blockers);
    check_lcia_requirement(input, &mut blockers);
    check_target_probe_coverage(input, &mut metrics, &mut blockers);

    if blockers.is_empty() && input.policy.run_factorization_probe {
        run_target_probe(input, &mut metrics, &mut blockers);
    }

    let status = if blockers.is_empty() {
        ReviewSubmitGateStatus::Passed
    } else {
        ReviewSubmitGateStatus::Blocked
    };

    ReviewSubmitGateReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        generated_at_utc: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        dataset_revision_id: input.dataset_revision_id,
        snapshot_id: input.snapshot_id,
        status,
        policy: input.policy.clone(),
        metrics,
        blockers,
    }
}

fn check_revision_freshness(
    input: &ReviewSubmitGateInput,
    metrics: &mut ReviewSubmitGateMetrics,
    blockers: &mut Vec<ReadinessFinding>,
) {
    if !input.policy.require_revision_checksum_match {
        return;
    }
    metrics.revision.checksum_checked = true;
    metrics.revision.checksum_matched = input.expected_revision_checksum
        == input.actual_revision_checksum
        && input.expected_revision_checksum.is_some();
    if !metrics.revision.checksum_matched {
        push_blocker(
            blockers,
            "revision_report_stale",
            "review-submit gate input is not tied to the current dataset revision checksum",
            json!({
                "dataset_revision_id": input.dataset_revision_id,
                "expected_revision_checksum": input.expected_revision_checksum,
                "actual_revision_checksum": input.actual_revision_checksum
            }),
        );
    }
}

fn check_process_records(
    input: &ReviewSubmitGateInput,
    metrics: &mut ReviewSubmitGateMetrics,
    blockers: &mut Vec<ReadinessFinding>,
) {
    metrics.process_scan.process_records_total = input.process_records.len();
    check_duplicate_process_versions(input, metrics, blockers);
    check_exchanges(input, metrics, blockers);
    check_duplicate_exchange_fingerprints(input, metrics, blockers);
    check_service_loops(input, metrics, blockers);
}

fn check_duplicate_process_versions(
    input: &ReviewSubmitGateInput,
    metrics: &mut ReviewSubmitGateMetrics,
    blockers: &mut Vec<ReadinessFinding>,
) {
    let mut by_id = BTreeMap::<Uuid, Vec<&ReviewProcessRecord>>::new();
    for record in &input.process_records {
        by_id.entry(record.process_id).or_default().push(record);
    }

    let duplicate_groups = by_id
        .into_iter()
        .filter_map(|(process_id, records)| {
            (records.len() > 1).then(|| {
                json!({
                    "process_id": process_id,
                    "versions": records.iter().map(|record| record.process_version.clone()).collect::<Vec<_>>()
                })
            })
        })
        .collect::<Vec<_>>();

    metrics.process_scan.duplicate_process_version_groups = duplicate_groups.len();
    if !duplicate_groups.is_empty() {
        push_blocker(
            blockers,
            "duplicate_process_version",
            "multiple versions of the same process are present in the review-submit scope",
            json!({
                "duplicate_process_version_groups": duplicate_groups.len(),
                "examples": duplicate_groups.into_iter().take(DETAIL_LIMIT).collect::<Vec<_>>()
            }),
        );
    }
}

fn check_exchanges(
    input: &ReviewSubmitGateInput,
    metrics: &mut ReviewSubmitGateMetrics,
    blockers: &mut Vec<ReadinessFinding>,
) {
    let mut invalid_amounts = Vec::new();
    let mut invalid_allocations = Vec::new();
    let mut invalid_references = Vec::new();

    for process in &input.process_records {
        for exchange in &process.exchanges {
            if parse_numeric_text(exchange.amount.as_deref()).is_err() {
                invalid_amounts.push(exchange_detail(process, exchange));
            }
            if let Some(fraction) = exchange.allocation_fraction.as_deref() {
                match parse_numeric_text(Some(fraction)) {
                    Ok(value) if (0.0..=100.0).contains(&value) => {}
                    _ => invalid_allocations.push(exchange_detail(process, exchange)),
                }
            }
        }

        if !reference_is_valid(
            process,
            input.compiled_graph.as_ref(),
            input.policy.zero_diagonal_epsilon,
        ) {
            invalid_references.push(process_detail(process));
        }
    }

    metrics.process_scan.invalid_exchange_amount_count = invalid_amounts.len();
    metrics.process_scan.invalid_allocation_fraction_count = invalid_allocations.len();
    metrics.process_scan.missing_or_zero_reference_count = invalid_references.len();

    if !invalid_amounts.is_empty() || sparse_values_have_non_finite(&input.payload) {
        push_blocker(
            blockers,
            "invalid_exchange_amount",
            "review-submit scope contains missing, invalid, or non-finite exchange amounts",
            json!({
                "invalid_exchange_amount_count": invalid_amounts.len(),
                "examples": invalid_amounts.into_iter().take(DETAIL_LIMIT).collect::<Vec<_>>()
            }),
        );
    }
    if !invalid_allocations.is_empty() {
        push_blocker(
            blockers,
            "invalid_allocation_fraction",
            "review-submit scope contains invalid allocation fractions",
            json!({
                "invalid_allocation_fraction_count": invalid_allocations.len(),
                "examples": invalid_allocations.into_iter().take(DETAIL_LIMIT).collect::<Vec<_>>()
            }),
        );
    }
    if !invalid_references.is_empty()
        || input.coverage.reference.missing_reference_count > 0
        || input.coverage.reference.invalid_reference_count > 0
    {
        push_blocker(
            blockers,
            "missing_or_zero_reference",
            "quantitative reference is missing, invalid, or has a zero amount",
            json!({
                "process_record_reference_failures": invalid_references.len(),
                "coverage_missing_reference_count": input.coverage.reference.missing_reference_count,
                "coverage_invalid_reference_count": input.coverage.reference.invalid_reference_count,
                "examples": invalid_references.into_iter().take(DETAIL_LIMIT).collect::<Vec<_>>()
            }),
        );
    }
}

fn check_duplicate_exchange_fingerprints(
    input: &ReviewSubmitGateInput,
    metrics: &mut ReviewSubmitGateMetrics,
    blockers: &mut Vec<ReadinessFinding>,
) {
    let mut by_fingerprint = BTreeMap::<String, Vec<&ReviewProcessRecord>>::new();
    for record in &input.process_records {
        let fingerprint = exchange_fingerprint(record);
        by_fingerprint.entry(fingerprint).or_default().push(record);
    }

    let duplicate_groups = by_fingerprint
        .into_iter()
        .filter_map(|(fingerprint, records)| {
            (records.len() > 1).then(|| {
                json!({
                    "fingerprint": fingerprint,
                    "processes": records.iter().map(|record| process_detail(record)).collect::<Vec<_>>()
                })
            })
        })
        .collect::<Vec<_>>();

    metrics.process_scan.duplicate_exchange_fingerprint_groups = duplicate_groups.len();
    if !duplicate_groups.is_empty() {
        push_blocker(
            blockers,
            "duplicate_exchange_fingerprint",
            "different processes have identical exchange fingerprints in the review-submit scope",
            json!({
                "duplicate_exchange_fingerprint_groups": duplicate_groups.len(),
                "examples": duplicate_groups.into_iter().take(DETAIL_LIMIT).collect::<Vec<_>>()
            }),
        );
    }
}

fn check_service_loops(
    input: &ReviewSubmitGateInput,
    metrics: &mut ReviewSubmitGateMetrics,
    blockers: &mut Vec<ReadinessFinding>,
) {
    let mut loops = Vec::new();
    for process in &input.process_records {
        let mut inputs = BTreeMap::<Uuid, Vec<(&ReviewExchangeRecord, f64)>>::new();
        let mut outputs = BTreeMap::<Uuid, Vec<(&ReviewExchangeRecord, f64)>>::new();
        for exchange in &process.exchanges {
            let Ok(amount) = parse_numeric_text(exchange.amount.as_deref()) else {
                continue;
            };
            match normalized_direction(&exchange.direction).as_deref() {
                Some("input") => inputs
                    .entry(exchange.flow_id)
                    .or_default()
                    .push((exchange, amount)),
                Some("output") => outputs
                    .entry(exchange.flow_id)
                    .or_default()
                    .push((exchange, amount)),
                _ => {}
            }
        }

        for (flow_id, input_edges) in inputs {
            let Some(output_edges) = outputs.get(&flow_id) else {
                continue;
            };
            for (input_edge, input_amount) in &input_edges {
                for (output_edge, output_amount) in output_edges {
                    if (*input_amount - *output_amount).abs() <= input.policy.service_loop_epsilon {
                        loops.push(json!({
                            "process": process_detail(process),
                            "flow_id": flow_id,
                            "input_exchange_id": input_edge.exchange_id,
                            "output_exchange_id": output_edge.exchange_id,
                            "amount": input_amount
                        }));
                    }
                }
            }
        }
    }

    metrics.process_scan.service_loop_count = loops.len();
    if !loops.is_empty() {
        push_blocker(
            blockers,
            "service_loop_detected",
            "same process has matching input and output amounts for the same flow",
            json!({
                "service_loop_count": loops.len(),
                "examples": loops.into_iter().take(DETAIL_LIMIT).collect::<Vec<_>>()
            }),
        );
    }
}

fn check_provider_closure(input: &ReviewSubmitGateInput, metrics: &mut ReviewSubmitGateMetrics) {
    let matching = &input.coverage.matching;
    metrics.provider_scan.provider_missing_count = matching.unmatched_no_provider;
    metrics.provider_scan.provider_unresolved_count = matching.matched_multi_unresolved;
    metrics.provider_scan.equal_fallback_count = matching.matched_multi_fallback_equal;
    metrics.provider_scan.volume_evidence_invalid_count =
        matching.volume_weight_summary.fallback_to_one_count
            + matching
                .volume_weight_summary
                .decisions_partial_missing_count
            + matching.volume_weight_summary.decisions_all_missing_count;

    if let Some(graph) = &input.compiled_graph {
        metrics.provider_scan.provider_decisions_total = graph.provider_decisions.len();
        for decision in &graph.provider_decisions {
            if !provider_allocation_is_conserved(decision, input.policy.allocation_sum_epsilon) {
                metrics.provider_scan.allocation_not_conserved_count += 1;
            }
        }
    }
}

fn check_flow_semantics(
    input: &ReviewSubmitGateInput,
    metrics: &mut ReviewSubmitGateMetrics,
    blockers: &mut Vec<ReadinessFinding>,
) {
    let Some(graph) = &input.compiled_graph else {
        return;
    };
    let by_flow_id = graph
        .flows
        .iter()
        .map(|flow| (flow.flow_id, flow.kind))
        .collect::<BTreeMap<_, _>>();
    let by_flow_idx = graph
        .flows
        .iter()
        .map(|flow| (flow.flow_idx, flow.kind))
        .collect::<BTreeMap<_, _>>();

    let mut mismatches = Vec::new();
    for decision in &graph.provider_decisions {
        if by_flow_id.get(&decision.flow_id) != Some(&CompiledFlowKind::Product) {
            mismatches.push(json!({
                "kind": "provider_decision_non_product_flow",
                "consumer_idx": decision.consumer_idx,
                "flow_id": decision.flow_id
            }));
        }
    }
    for edge in &graph.technosphere_edges {
        if by_flow_id.get(&edge.flow_id) != Some(&CompiledFlowKind::Product) {
            mismatches.push(json!({
                "kind": "technosphere_non_product_flow",
                "provider_idx": edge.provider_idx,
                "consumer_idx": edge.consumer_idx,
                "flow_id": edge.flow_id
            }));
        }
    }
    for edge in &graph.biosphere_edges {
        if by_flow_idx.get(&edge.flow_idx) != Some(&CompiledFlowKind::Elementary) {
            mismatches.push(json!({
                "kind": "biosphere_non_elementary_flow",
                "process_idx": edge.process_idx,
                "flow_idx": edge.flow_idx
            }));
        }
    }
    for factor in &input.payload.characterization_factors {
        if by_flow_idx.get(&factor.col) == Some(&CompiledFlowKind::Product) {
            mismatches.push(json!({
                "kind": "lcia_factor_on_product_flow",
                "impact_idx": factor.row,
                "flow_idx": factor.col
            }));
        }
    }

    metrics.sparse_scan.flow_lcia_semantic_mismatch_count = mismatches.len();
    if !mismatches.is_empty() {
        push_blocker(
            blockers,
            "flow_lcia_semantic_mismatch",
            "product, elementary, or LCIA flow semantics are inconsistent",
            json!({
                "flow_lcia_semantic_mismatch_count": mismatches.len(),
                "examples": mismatches.into_iter().take(DETAIL_LIMIT).collect::<Vec<_>>()
            }),
        );
    }
}

fn check_sparse_structure(
    input: &ReviewSubmitGateInput,
    metrics: &mut ReviewSubmitGateMetrics,
    blockers: &mut Vec<ReadinessFinding>,
) {
    let Some(process_count) = valid_process_count(input.payload.process_count) else {
        push_blocker(
            blockers,
            "sparse_matrix_zero_or_near_zero_diagonal",
            "sparse payload has an invalid process count",
            json!({ "process_count": input.payload.process_count }),
        );
        return;
    };

    let a_map = aggregate_sparse_entries(&input.payload.technosphere_entries);
    let near_zero_diagonal = (0..process_count)
        .filter_map(|idx| {
            let idx_i32 = i32::try_from(idx).ok()?;
            let a_diag = a_map.get(&(idx_i32, idx_i32)).copied().unwrap_or(0.0);
            let m_diag = 1.0 - a_diag;
            (m_diag.abs() <= input.policy.zero_diagonal_epsilon).then_some(json!({
                "process_idx": idx_i32,
                "a_diagonal": a_diag,
                "m_diagonal": m_diag
            }))
        })
        .collect::<Vec<_>>();

    metrics.sparse_scan.zero_or_near_zero_diagonal_count = near_zero_diagonal.len();
    if !near_zero_diagonal.is_empty() {
        push_blocker(
            blockers,
            "sparse_matrix_zero_or_near_zero_diagonal",
            "M = I - A has a zero or near-zero diagonal",
            json!({
                "zero_or_near_zero_diagonal_count": near_zero_diagonal.len(),
                "examples": near_zero_diagonal.into_iter().take(DETAIL_LIMIT).collect::<Vec<_>>()
            }),
        );
    }

    let duplicate_columns =
        duplicate_m_columns(process_count, &a_map, input.policy.duplicate_value_epsilon);
    metrics.sparse_scan.duplicate_sparse_column_groups = duplicate_columns.len();
    if !duplicate_columns.is_empty() {
        push_blocker(
            blockers,
            "duplicate_sparse_columns",
            "M = I - A contains duplicate sparse columns",
            json!({
                "duplicate_sparse_column_groups": duplicate_columns.len(),
                "examples": duplicate_columns.into_iter().take(DETAIL_LIMIT).collect::<Vec<_>>()
            }),
        );
    }
}

fn check_lcia_requirement(input: &ReviewSubmitGateInput, blockers: &mut Vec<ReadinessFinding>) {
    if input.policy.require_lcia_for_impact_submit && input.coverage.matrix_scale.c_nnz == 0 {
        push_blocker(
            blockers,
            "lcia_factor_missing_for_impact_submit",
            "impact-ready review submission requires LCIA factors",
            json!({
                "impact_count": input.coverage.matrix_scale.impact_count,
                "c_nnz": input.coverage.matrix_scale.c_nnz
            }),
        );
    }
}

fn check_target_probe_coverage(
    input: &ReviewSubmitGateInput,
    metrics: &mut ReviewSubmitGateMetrics,
    blockers: &mut Vec<ReadinessFinding>,
) {
    let requested = unique_target_indices(&input.target_process_indices);
    metrics.probe.target_indices_requested = requested.len();

    if !input.policy.require_target_process_probe {
        return;
    }

    let process_count = valid_process_count(input.payload.process_count).unwrap_or_default();
    let invalid = requested
        .iter()
        .copied()
        .filter(|idx| usize::try_from(*idx).map_or(true, |idx| idx >= process_count))
        .collect::<Vec<_>>();

    let not_covered = requested.is_empty()
        || !invalid.is_empty()
        || requested.len() > input.policy.target_probe_limit;

    if not_covered {
        push_blocker(
            blockers,
            "target_process_not_covered_by_probe",
            "target process probe coverage is missing or incomplete",
            json!({
                "target_indices_requested": requested,
                "invalid_target_indices": invalid,
                "target_probe_limit": input.policy.target_probe_limit
            }),
        );
    }
}

fn run_target_probe(
    input: &ReviewSubmitGateInput,
    metrics: &mut ReviewSubmitGateMetrics,
    blockers: &mut Vec<ReadinessFinding>,
) {
    metrics.probe.factorization_checked = true;
    let service = SolverService::new();
    match service.prepare(&input.payload, NumericOptions::default()) {
        Ok(result) => {
            metrics.probe.factorization_ready = true;
            metrics.probe.validation = Some(result.diagnostics.validation);
        }
        Err(error) => {
            if let SolverError::ValidationFailed(report) = &error {
                metrics.probe.validation = Some(report.clone());
            }
            push_blocker(
                blockers,
                "factorization_probe_failed",
                "targeted sparse factorization probe failed",
                json!({ "error": error.to_string() }),
            );
            return;
        }
    }

    let Some(process_count) = valid_process_count(input.payload.process_count) else {
        return;
    };
    for process_idx in unique_target_indices(&input.target_process_indices)
        .into_iter()
        .take(input.policy.target_probe_limit)
    {
        let Ok(process_idx_usize) = usize::try_from(process_idx) else {
            continue;
        };
        if process_idx_usize >= process_count {
            continue;
        }
        let mut rhs = vec![0.0_f64; process_count];
        rhs[process_idx_usize] = 1.0;
        match service.solve_one(
            input.payload.model_version,
            NumericOptions::default(),
            &rhs,
            SolveOptions {
                return_x: true,
                return_g: true,
                return_h: false,
            },
        ) {
            Ok(result) => {
                metrics.probe.target_indices_probed += 1;
                metrics.probe.non_finite_value_count += non_finite_count(result.x.as_deref());
                metrics.probe.non_finite_value_count += non_finite_count(result.g.as_deref());
            }
            Err(error) => push_blocker(
                blockers,
                "target_probe_non_finite_result",
                "targeted process probe failed during solve",
                json!({
                    "process_idx": process_idx,
                    "error": error.to_string()
                }),
            ),
        }
    }

    if metrics.probe.non_finite_value_count > 0 {
        push_blocker(
            blockers,
            "target_probe_non_finite_result",
            "targeted process probe produced NaN or Infinity values",
            json!({ "non_finite_value_count": metrics.probe.non_finite_value_count }),
        );
    }
}

fn reference_is_valid(
    process: &ReviewProcessRecord,
    compiled_graph: Option<&CompiledGraph>,
    epsilon: f64,
) -> bool {
    let Some(reference_exchange_id) = process.reference_exchange_id.as_deref() else {
        return false;
    };
    let mut matching_exchanges = process
        .exchanges
        .iter()
        .filter(|exchange| exchange.exchange_id.as_deref() == Some(reference_exchange_id));
    let Some(exchange) = matching_exchanges.next() else {
        return false;
    };
    if matching_exchanges.next().is_some()
        || normalized_direction(&exchange.direction).is_none()
        || !parse_numeric_text(exchange.amount.as_deref())
            .is_ok_and(|amount| amount.abs() > epsilon)
    {
        return false;
    }

    compiled_graph.is_some_and(|graph| {
        graph
            .flows
            .iter()
            .any(|flow| flow.flow_id == exchange.flow_id)
    })
}

fn parse_numeric_text(value: Option<&str>) -> Result<f64, ()> {
    let Some(raw) = value else {
        return Err(());
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.contains('%') {
        return Err(());
    }
    let parsed = trimmed.parse::<f64>().map_err(|_| ())?;
    parsed.is_finite().then_some(parsed).ok_or(())
}

fn normalized_direction(direction: &str) -> Option<String> {
    match direction.trim().to_ascii_lowercase().as_str() {
        "input" | "in" => Some("input".to_owned()),
        "output" | "out" => Some("output".to_owned()),
        _ => None,
    }
}

fn exchange_fingerprint(process: &ReviewProcessRecord) -> String {
    let mut parts = process
        .exchanges
        .iter()
        .map(|exchange| {
            let amount = parse_numeric_text(exchange.amount.as_deref()).map_or_else(
                |()| exchange.amount.clone().unwrap_or_default(),
                format_numeric,
            );
            format!(
                "{}:{}:{}",
                exchange.flow_id,
                normalized_direction(&exchange.direction)
                    .unwrap_or_else(|| exchange.direction.clone()),
                amount
            )
        })
        .collect::<Vec<_>>();
    parts.sort();
    parts.join("|")
}

fn sparse_values_have_non_finite(payload: &ModelSparseData) -> bool {
    payload
        .technosphere_entries
        .iter()
        .chain(payload.biosphere_entries.iter())
        .chain(payload.characterization_factors.iter())
        .any(|entry| !entry.value.is_finite())
}

fn provider_allocation_is_conserved(
    decision: &crate::compiled_graph::CompiledProviderDecision,
    epsilon: f64,
) -> bool {
    if decision.allocations.is_empty() {
        return decision.matched_provider_count == 0;
    }
    let mut sum = 0.0_f64;
    for allocation in &decision.allocations {
        if !allocation.weight.is_finite() || allocation.weight < 0.0 {
            return false;
        }
        sum += allocation.weight;
    }
    (sum - 1.0).abs() <= epsilon
}

fn aggregate_sparse_entries(entries: &[SparseTriplet]) -> BTreeMap<(i32, i32), f64> {
    let mut out = BTreeMap::<(i32, i32), f64>::new();
    for entry in entries {
        *out.entry((entry.row, entry.col)).or_insert(0.0) += entry.value;
    }
    out
}

fn duplicate_m_columns(
    process_count: usize,
    a_map: &BTreeMap<(i32, i32), f64>,
    epsilon: f64,
) -> Vec<Value> {
    let mut m_map = BTreeMap::<(i32, i32), f64>::new();
    for (&(row, col), &value) in a_map {
        if value.abs() > epsilon {
            *m_map.entry((row, col)).or_insert(0.0) -= value;
        }
    }
    for idx in 0..process_count {
        let Ok(idx_i32) = i32::try_from(idx) else {
            continue;
        };
        *m_map.entry((idx_i32, idx_i32)).or_insert(0.0) += 1.0;
    }

    let mut columns = vec![Vec::<(i32, String)>::new(); process_count];
    for ((row, col), value) in m_map {
        let Ok(col_idx) = usize::try_from(col) else {
            continue;
        };
        if col_idx < process_count && value.abs() > epsilon {
            columns[col_idx].push((row, format_numeric(value)));
        }
    }
    for column in &mut columns {
        column.sort();
    }

    let mut by_signature = BTreeMap::<String, Vec<i32>>::new();
    for (idx, column) in columns.into_iter().enumerate() {
        let signature = column
            .into_iter()
            .map(|(row, value)| format!("{row}:{value}"))
            .collect::<Vec<_>>()
            .join(",");
        if let Ok(idx_i32) = i32::try_from(idx) {
            by_signature.entry(signature).or_default().push(idx_i32);
        }
    }

    by_signature
        .into_iter()
        .filter_map(|(signature, process_indices)| {
            (process_indices.len() > 1).then(|| {
                json!({
                    "process_indices": process_indices,
                    "signature": signature
                })
            })
        })
        .collect()
}

fn valid_process_count(process_count: i32) -> Option<usize> {
    usize::try_from(process_count)
        .ok()
        .filter(|count| *count > 0)
}

fn unique_target_indices(target_process_indices: &[i32]) -> Vec<i32> {
    target_process_indices
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn non_finite_count(values: Option<&[f64]>) -> usize {
    values
        .unwrap_or_default()
        .iter()
        .filter(|value| !value.is_finite())
        .count()
}

fn process_detail(process: &ReviewProcessRecord) -> Value {
    json!({
        "process_idx": process.process_idx,
        "process_id": process.process_id,
        "process_version": process.process_version,
        "process_name": process.process_name,
        "state_code": process.state_code
    })
}

fn exchange_detail(process: &ReviewProcessRecord, exchange: &ReviewExchangeRecord) -> Value {
    json!({
        "process": process_detail(process),
        "exchange_id": exchange.exchange_id,
        "flow_id": exchange.flow_id,
        "direction": exchange.direction,
        "amount": exchange.amount,
        "allocation_fraction": exchange.allocation_fraction
    })
}

fn format_numeric(value: f64) -> String {
    if value == 0.0 {
        "0.000000000000e0".to_owned()
    } else {
        format!("{value:.12e}")
    }
}

fn push_blocker(
    blockers: &mut Vec<ReadinessFinding>,
    code: &str,
    message: impl Into<String>,
    details: Value,
) {
    blockers.push(ReadinessFinding {
        code: code.to_owned(),
        severity: FindingSeverity::Blocker,
        message: message.into(),
        details,
    });
}

fn default_input_schema_version() -> String {
    INPUT_SCHEMA_VERSION.to_owned()
}

fn default_policy_profile() -> String {
    DEFAULT_POLICY_PROFILE.to_owned()
}

fn default_allowed_scope_states() -> Vec<i32> {
    std::iter::once(0).chain(100..=199).collect()
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

fn default_target_probe_limit() -> usize {
    32
}

fn default_zero_epsilon() -> f64 {
    1e-12
}

fn default_duplicate_value_epsilon() -> f64 {
    1e-12
}

fn default_allocation_sum_epsilon() -> f64 {
    1e-9
}

fn default_service_loop_epsilon() -> f64 {
    1e-12
}

#[cfg(test)]
mod tests {
    use solver_core::SparseTriplet;

    use super::*;
    use crate::compiled_graph::{
        CompiledAllocationStats, CompiledFlow, CompiledMatchingStats, CompiledProcess,
        CompiledProviderAllocation, CompiledProviderDecision, CompiledProviderDecisionKind,
        CompiledProviderResolutionStrategy, CompiledReferenceStats,
    };
    use crate::graph_types::ScopeProcessPartition;
    use crate::snapshot_artifacts::{
        SNAPSHOT_COVERAGE_SCHEMA_VERSION, SnapshotAllocationCoverage, SnapshotCandidateSummary,
        SnapshotGapSummary, SnapshotGeographySummary, SnapshotMatchingCoverage,
        SnapshotMatrixScale, SnapshotProviderDecisionDiagnostics, SnapshotReferenceCoverage,
        SnapshotResolutionSummary, SnapshotSingularRisk, SnapshotVolumeWeightSummary,
    };

    #[test]
    fn passes_clean_review_submit_fixture_with_target_probe() {
        let input = clean_input();

        let report = verify_review_submit_gate(&input);

        assert_eq!(report.status, ReviewSubmitGateStatus::Passed);
        assert!(report.blockers.is_empty());
        assert!(report.metrics.probe.factorization_checked);
        assert!(report.metrics.probe.factorization_ready);
        assert_eq!(report.metrics.probe.target_indices_probed, 1);
    }

    #[test]
    fn lifecycle_scope_states_do_not_block_numerical_gate() {
        let mut input = clean_input();
        input.process_records[0].state_code = Some(20);
        input.process_records[1].state_code = Some(100);
        input.policy.allowed_scope_states = vec![0, 100];

        let report = verify_review_submit_gate(&input);

        assert_eq!(report.status, ReviewSubmitGateStatus::Passed);
        assert!(!has_blocker(&report, "invalid_scope_state"));
        assert_eq!(report.metrics.process_scan.invalid_scope_state_count, 0);
        assert!(report.metrics.probe.factorization_checked);
        assert_eq!(report.policy.allowed_scope_states, vec![0, 100]);
    }

    #[test]
    fn default_policy_allows_no_lcia_snapshot() {
        let mut input = clean_input();
        input.coverage.matrix_scale.c_nnz = 0;
        input.payload.characterization_factors.clear();

        let report = verify_review_submit_gate(&input);

        assert_eq!(report.status, ReviewSubmitGateStatus::Passed);
        assert!(!has_blocker(
            &report,
            "lcia_factor_missing_for_impact_submit"
        ));
        assert_eq!(report.metrics.probe.target_indices_probed, 1);
    }

    #[test]
    fn explicit_lcia_required_policy_still_blocks_missing_c_matrix() {
        let mut input = clean_input();
        input.policy.require_lcia_for_impact_submit = true;
        input.coverage.matrix_scale.c_nnz = 0;
        input.payload.characterization_factors.clear();

        let report = verify_review_submit_gate(&input);

        assert_eq!(report.status, ReviewSubmitGateStatus::Blocked);
        assert!(has_blocker(
            &report,
            "lcia_factor_missing_for_impact_submit"
        ));
        assert!(!report.metrics.probe.factorization_checked);
    }

    #[test]
    fn blocks_historical_process_scan_failures_before_probe() {
        let mut input = clean_input();
        input.process_records.push(ReviewProcessRecord {
            process_idx: Some(2),
            process_id: input.process_records[1].process_id,
            process_version: "01.01.001".to_owned(),
            process_name: Some("consumer duplicate version".to_owned()),
            state_code: Some(20),
            reference_exchange_id: Some("ref".to_owned()),
            exchanges: vec![
                exchange(
                    "ref",
                    input.process_records[1].exchanges[0].flow_id,
                    "Output",
                    "0",
                ),
                ReviewExchangeRecord {
                    exchange_id: Some("bad_alloc".to_owned()),
                    flow_id: Uuid::new_v4(),
                    direction: "Input".to_owned(),
                    amount: Some("2.0".to_owned()),
                    allocation_fraction: Some("100%".to_owned()),
                },
            ],
        });

        let report = verify_review_submit_gate(&input);

        assert_eq!(report.status, ReviewSubmitGateStatus::Blocked);
        assert!(!has_blocker(&report, "invalid_scope_state"));
        assert!(has_blocker(&report, "duplicate_process_version"));
        assert!(has_blocker(&report, "missing_or_zero_reference"));
        assert!(has_blocker(&report, "invalid_allocation_fraction"));
        assert!(!report.metrics.probe.factorization_checked);
    }

    #[test]
    fn blocks_invalid_exchange_amount_text() {
        let mut input = clean_input();
        input.process_records[1].exchanges[1].amount = Some("not-a-number".to_owned());

        let report = verify_review_submit_gate(&input);

        assert!(has_blocker(&report, "invalid_exchange_amount"));
        assert!(!report.metrics.probe.factorization_checked);
    }

    #[test]
    fn blocks_duplicate_quantitative_reference_exchange_ids() {
        let mut input = clean_input();
        let duplicate_reference = input.process_records[1].exchanges[0].clone();
        input.process_records[1].exchanges.push(duplicate_reference);

        let report = verify_review_submit_gate(&input);

        assert!(has_blocker(&report, "missing_or_zero_reference"));
        assert!(!report.metrics.probe.factorization_checked);
    }

    #[test]
    fn accepts_input_quantitative_reference() {
        let mut input = clean_input();
        input.process_records[1].exchanges[0].direction = "Input".to_owned();

        let report = verify_review_submit_gate(&input);

        assert!(!has_blocker(&report, "missing_or_zero_reference"));
    }

    #[test]
    fn accepts_quantitative_reference_to_non_product_flow() {
        let mut input = clean_input();
        let elementary_flow_id = input
            .compiled_graph
            .as_ref()
            .and_then(|graph| {
                graph
                    .flows
                    .iter()
                    .find(|flow| flow.kind == CompiledFlowKind::Elementary)
            })
            .map(|flow| flow.flow_id)
            .expect("elementary flow fixture");
        input.process_records[1].exchanges[0].flow_id = elementary_flow_id;

        let report = verify_review_submit_gate(&input);

        assert!(!has_blocker(&report, "missing_or_zero_reference"));
    }

    #[test]
    fn accepts_negative_quantitative_reference_amount() {
        let mut input = clean_input();
        input.process_records[1].exchanges[0].amount = Some("-1.0".to_owned());

        let report = verify_review_submit_gate(&input);

        assert!(!has_blocker(&report, "missing_or_zero_reference"));
    }

    #[test]
    fn blocks_zero_quantitative_reference_amount() {
        let mut input = clean_input();
        input.process_records[1].exchanges[0].amount = Some("0".to_owned());

        let report = verify_review_submit_gate(&input);

        assert!(has_blocker(&report, "missing_or_zero_reference"));
        assert!(!report.metrics.probe.factorization_checked);
    }

    #[test]
    fn blocks_quantitative_reference_without_compiled_flow_metadata() {
        let mut input = clean_input();
        input.compiled_graph = None;

        let report = verify_review_submit_gate(&input);

        assert!(has_blocker(&report, "missing_or_zero_reference"));
        assert!(!report.metrics.probe.factorization_checked);
    }

    #[test]
    fn blocks_invalid_allocation_resolution_sentinel() {
        let mut input = clean_input();
        input.process_records[1].exchanges[1].allocation_fraction =
            Some("invalid:target_missing".to_owned());

        let report = verify_review_submit_gate(&input);

        assert!(has_blocker(&report, "invalid_allocation_fraction"));
        assert!(!report.metrics.probe.factorization_checked);
    }

    #[test]
    fn blocks_duplicate_exchange_fingerprint_and_service_loop() {
        let mut input = clean_input();
        let product_flow = input.process_records[0].exchanges[0].flow_id;
        let duplicate_a = ReviewProcessRecord {
            process_idx: Some(2),
            process_id: Uuid::new_v4(),
            process_version: "01.00.000".to_owned(),
            process_name: Some("duplicate a".to_owned()),
            state_code: Some(100),
            reference_exchange_id: Some("out".to_owned()),
            exchanges: vec![
                exchange("out", product_flow, "Output", "1.0"),
                exchange("in", product_flow, "Input", "1.0"),
            ],
        };
        let duplicate_b = ReviewProcessRecord {
            process_idx: Some(3),
            process_id: Uuid::new_v4(),
            process_version: "01.00.000".to_owned(),
            process_name: Some("duplicate b".to_owned()),
            state_code: Some(100),
            reference_exchange_id: Some("out".to_owned()),
            exchanges: duplicate_a.exchanges.clone(),
        };
        input.process_records = vec![duplicate_a, duplicate_b];

        let report = verify_review_submit_gate(&input);

        assert!(has_blocker(&report, "duplicate_exchange_fingerprint"));
        assert!(has_blocker(&report, "service_loop_detected"));
        assert!(!report.metrics.probe.factorization_checked);
    }

    #[test]
    fn records_provider_diagnostics_without_blocking() {
        let mut input = clean_input();
        let flow_id = input.compiled_graph.as_ref().unwrap().flows[0].flow_id;
        let graph = input.compiled_graph.as_mut().unwrap();
        graph.flows[0].kind = CompiledFlowKind::Elementary;
        graph.provider_decisions[0].decision_kind = Some(CompiledProviderDecisionKind::NoProvider);
        graph.provider_decisions[0].matched_provider_count = 0;
        graph.provider_decisions[0].allocations.clear();
        graph.provider_decisions[0].used_equal_fallback = true;
        graph.provider_decisions[0].volume_fallback_to_one_count = 1;
        input.coverage.matching.unmatched_no_provider = 1;
        input.coverage.matching.matched_multi_fallback_equal = 1;
        input
            .coverage
            .matching
            .volume_weight_summary
            .fallback_to_one_count = 1;
        graph.technosphere_edges[0].flow_id = flow_id;

        let report = verify_review_submit_gate(&input);

        assert_eq!(report.metrics.provider_scan.provider_missing_count, 1);
        assert_eq!(report.metrics.provider_scan.equal_fallback_count, 1);
        assert_eq!(
            report.metrics.provider_scan.volume_evidence_invalid_count,
            1
        );
        assert_eq!(
            report.metrics.provider_scan.allocation_not_conserved_count,
            0
        );
        assert!(!has_blocker(&report, "provider_missing"));
        assert!(!has_blocker(&report, "provider_equal_fallback"));
        assert!(!has_blocker(&report, "provider_allocation_not_conserved"));
        assert!(!has_blocker(&report, "provider_volume_evidence_invalid"));
        assert!(has_blocker(&report, "flow_lcia_semantic_mismatch"));
    }

    #[test]
    fn records_unresolved_multi_provider_without_blocking() {
        let mut input = clean_input();
        let graph = input.compiled_graph.as_mut().unwrap();
        graph.provider_decisions[0].decision_kind =
            Some(CompiledProviderDecisionKind::MultiUnresolved);
        graph.provider_decisions[0].matched_provider_count = 2;
        graph.provider_decisions[0].allocations.clear();
        input.coverage.matching.matched_multi_unresolved = 1;

        let report = verify_review_submit_gate(&input);

        assert_eq!(report.status, ReviewSubmitGateStatus::Passed);
        assert_eq!(report.metrics.provider_scan.provider_unresolved_count, 1);
        assert_eq!(
            report.metrics.provider_scan.allocation_not_conserved_count,
            1
        );
        assert!(!has_blocker(&report, "provider_unresolved"));
        assert!(!has_blocker(&report, "provider_allocation_not_conserved"));
    }

    #[test]
    fn blocks_sparse_and_probe_coverage_failures() {
        let mut input = clean_input();
        input.target_process_indices.clear();
        input.coverage.singular_risk.risk_level = "medium".to_owned();
        input.payload.technosphere_entries.push(SparseTriplet {
            row: 0,
            col: 0,
            value: 1.0,
        });

        let report = verify_review_submit_gate(&input);

        assert!(has_blocker(
            &report,
            "sparse_matrix_zero_or_near_zero_diagonal"
        ));
        assert!(!has_blocker(&report, "singular_risk_medium_or_high"));
        assert!(has_blocker(&report, "target_process_not_covered_by_probe"));
        assert!(!report.metrics.probe.factorization_checked);
    }

    #[test]
    fn does_not_block_medium_singular_risk_without_structural_symptom() {
        let mut input = clean_input();
        input.coverage.singular_risk.risk_level = "medium".to_owned();

        let report = verify_review_submit_gate(&input);

        assert_eq!(report.status, ReviewSubmitGateStatus::Passed);
        assert!(!has_blocker(
            &report,
            "sparse_matrix_zero_or_near_zero_diagonal"
        ));
        assert!(!has_blocker(&report, "singular_risk_medium_or_high"));
    }

    #[test]
    fn blocks_duplicate_sparse_columns() {
        let mut input = clean_input();
        input.payload.technosphere_entries = vec![
            SparseTriplet {
                row: 0,
                col: 0,
                value: 1.0,
            },
            SparseTriplet {
                row: 1,
                col: 1,
                value: 1.0,
            },
        ];

        let report = verify_review_submit_gate(&input);

        assert!(has_blocker(&report, "duplicate_sparse_columns"));
        assert!(!report.metrics.probe.factorization_checked);
    }

    #[test]
    fn blocks_factorization_probe_failure_without_full_solve_all_unit() {
        let mut input = clean_input();
        input.payload.technosphere_entries = vec![
            SparseTriplet {
                row: 0,
                col: 1,
                value: 1.0,
            },
            SparseTriplet {
                row: 1,
                col: 0,
                value: 1.0,
            },
        ];

        let report = verify_review_submit_gate(&input);

        assert!(has_blocker(&report, "factorization_probe_failed"));
        assert!(report.metrics.probe.factorization_checked);
        assert!(!report.metrics.probe.factorization_ready);
    }

    fn clean_input() -> ReviewSubmitGateInput {
        let snapshot_id = Uuid::new_v4();
        let provider_id = Uuid::new_v4();
        let consumer_id = Uuid::new_v4();
        let product_flow_id = Uuid::new_v4();
        let elementary_flow_id = Uuid::new_v4();
        ReviewSubmitGateInput {
            schema_version: INPUT_SCHEMA_VERSION.to_owned(),
            dataset_revision_id: Some(Uuid::new_v4()),
            expected_revision_checksum: Some("sha256:abc".to_owned()),
            actual_revision_checksum: Some("sha256:abc".to_owned()),
            snapshot_id: Some(snapshot_id),
            config: None,
            coverage: coverage(true),
            payload: ModelSparseData {
                model_version: snapshot_id,
                process_count: 2,
                flow_count: 2,
                impact_count: 1,
                technosphere_entries: vec![SparseTriplet {
                    row: 0,
                    col: 1,
                    value: 0.1,
                }],
                biosphere_entries: vec![
                    SparseTriplet {
                        row: 1,
                        col: 0,
                        value: 1.0,
                    },
                    SparseTriplet {
                        row: 1,
                        col: 1,
                        value: 2.0,
                    },
                ],
                characterization_factors: vec![SparseTriplet {
                    row: 0,
                    col: 1,
                    value: 0.5,
                }],
            },
            compiled_graph: Some(CompiledGraph {
                processes: vec![
                    process(0, provider_id, "provider"),
                    process(1, consumer_id, "consumer"),
                ],
                flows: vec![
                    CompiledFlow {
                        flow_idx: 0,
                        flow_id: product_flow_id,
                        flow_version: "01.00.000".to_owned(),
                        kind: CompiledFlowKind::Product,
                        space: crate::compiled_graph::CompiledFlowSpace::Technosphere,
                        source_type: crate::compiled_graph::CompiledSourceFlowType::Product,
                    },
                    CompiledFlow {
                        flow_idx: 1,
                        flow_id: elementary_flow_id,
                        flow_version: "01.00.000".to_owned(),
                        kind: CompiledFlowKind::Elementary,
                        space: crate::compiled_graph::CompiledFlowSpace::Biosphere,
                        source_type: crate::compiled_graph::CompiledSourceFlowType::Elementary,
                    },
                ],
                reference_ports: Vec::new(),
                balance_resolutions: Vec::new(),
                unresolved_balances: Vec::new(),
                provider_outputs: Vec::new(),
                provider_decisions: vec![CompiledProviderDecision {
                    consumer_idx: 1,
                    flow_id: product_flow_id,
                    flow_version: "01.00.000".to_owned(),
                    candidate_provider_count: 1,
                    matched_provider_count: 1,
                    candidates: Vec::new(),
                    decision_kind: Some(CompiledProviderDecisionKind::UniqueProvider),
                    resolution_strategy: Some(CompiledProviderResolutionStrategy::UniqueProvider),
                    failure_reason: None,
                    used_equal_fallback: false,
                    volume_fallback_to_one_count: 0,
                    geography_tier: None,
                    supply_region_source: None,
                    supply_region_location: None,
                    exchange_location_present: false,
                    allocations: vec![CompiledProviderAllocation {
                        provider_idx: 0,
                        weight: 1.0,
                    }],
                }],
                technosphere_edges: vec![crate::compiled_graph::CompiledTechnosphereEdge {
                    provider_idx: 0,
                    consumer_idx: 1,
                    flow_id: product_flow_id,
                    amount: 0.1,
                    provider_partition: ScopeProcessPartition::Public,
                    consumer_partition: ScopeProcessPartition::Private,
                    partition: crate::compiled_graph::CompiledEdgePartition::PublicToPrivate,
                }],
                biosphere_edges: vec![crate::compiled_graph::CompiledBiosphereEdge {
                    process_idx: 1,
                    flow_idx: 1,
                    amount: 2.0,
                    process_partition: ScopeProcessPartition::Private,
                }],
                reference_stats: CompiledReferenceStats::default(),
                allocation_stats: CompiledAllocationStats::default(),
                matching_stats: CompiledMatchingStats::default(),
                release_evidence: None,
            }),
            target_process_indices: vec![1],
            process_records: vec![
                ReviewProcessRecord {
                    process_idx: Some(0),
                    process_id: provider_id,
                    process_version: "01.00.000".to_owned(),
                    process_name: Some("provider".to_owned()),
                    state_code: Some(100),
                    reference_exchange_id: Some("ref".to_owned()),
                    exchanges: vec![exchange("ref", product_flow_id, "Output", "1.0")],
                },
                ReviewProcessRecord {
                    process_idx: Some(1),
                    process_id: consumer_id,
                    process_version: "01.00.000".to_owned(),
                    process_name: Some("consumer".to_owned()),
                    state_code: Some(100),
                    reference_exchange_id: Some("ref".to_owned()),
                    exchanges: vec![
                        exchange("ref", product_flow_id, "Output", "1.0"),
                        exchange("in", product_flow_id, "Input", "0.1"),
                    ],
                },
            ],
            policy: ReviewSubmitGatePolicy::default(),
        }
    }

    fn coverage(provider_closed: bool) -> SnapshotCoverageReport {
        SnapshotCoverageReport {
            schema_version: SNAPSHOT_COVERAGE_SCHEMA_VERSION.to_owned(),
            matching: SnapshotMatchingCoverage {
                input_edges_total: 1,
                matched_unique_provider: i64::from(provider_closed),
                matched_multi_provider: 0,
                unmatched_no_provider: i64::from(!provider_closed),
                matched_multi_resolved: 0,
                matched_multi_unresolved: 0,
                matched_multi_fallback_equal: 0,
                a_input_edges_written: i64::from(provider_closed),
                residual_edges_total: 1,
                a_balance_edges_written: i64::from(provider_closed),
                a_write_pct: if provider_closed { 100.0 } else { 0.0 },
                provider_present_resolved_pct: if provider_closed { 100.0 } else { 0.0 },
                unique_provider_match_pct: if provider_closed { 100.0 } else { 0.0 },
                any_provider_match_pct: if provider_closed { 100.0 } else { 0.0 },
                provider_decision_diagnostics: SnapshotProviderDecisionDiagnostics::default(),
                candidate_summary: SnapshotCandidateSummary::default(),
                resolution_summary: SnapshotResolutionSummary::default(),
                geography_summary: SnapshotGeographySummary::default(),
                volume_weight_summary: SnapshotVolumeWeightSummary::default(),
                gap_summary: SnapshotGapSummary::default(),
            },
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
                legacy_empty_allocation_as_undeclared_count: 0,
                legacy_single_output_target_inferred_count: 0,
                legacy_single_reference_target_inferred_count: 0,
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
                flow_count: 2,
                impact_count: 1,
                a_nnz: i64::from(provider_closed),
                b_nnz: 2,
                c_nnz: 1,
                m_nnz_estimated: 3,
                m_sparsity_estimated: 0.25,
            },
        }
    }

    fn process(process_idx: i32, process_id: Uuid, process_name: &str) -> CompiledProcess {
        CompiledProcess {
            process_idx,
            process_id,
            process_version: "01.00.000".to_owned(),
            process_name: Some(process_name.to_owned()),
            model_id: None,
            location: Some("CN".to_owned()),
            reference_year: Some(2024),
            annual_supply_or_production_volume: None,
            partition: ScopeProcessPartition::Private,
        }
    }

    fn exchange(
        exchange_id: &str,
        flow_id: Uuid,
        direction: &str,
        amount: &str,
    ) -> ReviewExchangeRecord {
        ReviewExchangeRecord {
            exchange_id: Some(exchange_id.to_owned()),
            flow_id,
            direction: direction.to_owned(),
            amount: Some(amount.to_owned()),
            allocation_fraction: None,
        }
    }

    fn has_blocker(report: &ReviewSubmitGateReport, code: &str) -> bool {
        report.blockers.iter().any(|blocker| blocker.code == code)
    }
}
