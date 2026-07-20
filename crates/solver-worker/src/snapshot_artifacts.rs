use std::collections::BTreeMap;
use std::path::Path;

use hdf5::File;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use solver_core::ModelSparseData;
use tempfile::Builder;
use uuid::Uuid;

use crate::calculation_evidence::LcaMethodFactorSourceSnapshot;
use crate::compiled_graph::CompiledGraph;
use crate::graph_types::{RequestRootProcess, SnapshotSelectionMode};

const SCHEMA_VERSION: u8 = 1;
const DATASET_SCHEMA_VERSION: &str = "schema_version";
const DATASET_FORMAT: &str = "format";
const DATASET_ENVELOPE_JSON: &str = "envelope_json";
const HDF5_DEFLATE_LEVEL: u8 = 4;
const HDF5_CHUNK_TARGET_BYTES: usize = 256 * 1024;

/// Snapshot matrix artifact format identifier.
pub const SNAPSHOT_ARTIFACT_FORMAT: &str = "snapshot-hdf5:v1";
/// Snapshot artifact file extension.
pub const SNAPSHOT_ARTIFACT_EXTENSION: &str = "h5";
/// Snapshot artifact content type.
pub const SNAPSHOT_ARTIFACT_CONTENT_TYPE: &str = "application/x-hdf5";
/// Snapshot coverage JSON schema identifier.
pub const SNAPSHOT_COVERAGE_SCHEMA_VERSION: &str = "snapshot_coverage.v3";

/// Snapshot build options persisted in artifact metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotBuildConfig {
    /// `state_code` selection used in builder.
    pub process_states: String,
    /// Optional `user_id` inclusion in process selection.
    #[serde(default)]
    pub include_user_id: Option<Uuid>,
    /// Named versioned visibility scope, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_scope: Option<String>,
    /// Canonical visibility manifest binding, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_manifest_sha256: Option<String>,
    /// Exact database method/factor snapshot proof used by the build.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lcia_method_factor_source: Option<LcaMethodFactorSourceSnapshot>,
    /// Snapshot selection mode (`filtered_library` / `request_roots_closure`).
    #[serde(default)]
    pub selection_mode: SnapshotSelectionMode,
    /// Explicit request roots for request-scoped graph builds.
    #[serde(default)]
    pub request_roots: Vec<RequestRootProcess>,
    /// Process cap (`0` means unlimited).
    pub process_limit: i32,
    /// Provider matching mode.
    pub provider_rule: String,
    /// Provider candidate eligibility mode.
    #[serde(default)]
    pub provider_candidate_eligibility_mode: String,
    /// Quantitative reference normalization mode (`strict`/`lenient`).
    #[serde(default = "default_strict_mode")]
    pub reference_normalization_mode: String,
    /// Allocation fraction mode (`strict`/`lenient`).
    #[serde(default = "default_strict_mode")]
    pub allocation_fraction_mode: String,
    /// Versioned TIDAS allocation/reference semantics used by matrix compilation.
    #[serde(default = "default_legacy_allocation_semantics_version")]
    pub allocation_semantics_version: String,
    /// Versioned signed-flow balance/link semantics used to derive activity requirements.
    #[serde(default = "default_legacy_link_semantics_version")]
    pub link_semantics_version: String,
    /// Explicit policy for unresolved non-zero technosphere balance coefficients.
    #[serde(default = "default_closed_boundary_policy")]
    pub technosphere_boundary_policy: String,
    /// Versioned exact flow identity and unit compatibility policy.
    #[serde(default = "default_flow_identity_policy")]
    pub flow_identity_policy: String,
    /// Biosphere sign convention (`signed`/`gross`).
    #[serde(default = "default_biosphere_sign_mode")]
    pub biosphere_sign_mode: String,
    /// Self-loop cutoff for technosphere diagonal filtering.
    pub self_loop_cutoff: f64,
    /// Near-singular epsilon.
    pub singular_eps: f64,
    /// Whether LCIA factors were enabled.
    pub has_lcia: bool,
    /// Optional lifecycle / caller purpose for source-hash isolation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_purpose: Option<String>,
    /// Optional dependency surface fingerprint for review-submit baseline reuse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_dependency_fingerprint: Option<String>,
    /// Optional authoritative root revision checksum for review-submit overlay reuse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_revision_checksum: Option<String>,
    /// Optional LCIA method id.
    pub method_id: Option<Uuid>,
    /// Optional LCIA method version.
    pub method_version: Option<String>,
}

/// Matching coverage diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotProviderDecisionDiagnostics {
    #[serde(default)]
    pub resolved_strategy_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub unresolved_reason_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub candidate_eligibility_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub rejected_non_reference_output_count: i64,
    #[serde(default)]
    pub volume_fallback_to_one_count: i64,
    #[serde(default)]
    pub geography_tier_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub supply_region_source_counts: BTreeMap<String, i64>,
}

/// Provider candidate distribution diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotCandidateSummary {
    #[serde(default)]
    pub candidate_count_histogram: BTreeMap<String, i64>,
}

/// Provider resolution diagnostics in the canonical v2 summary layout.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotResolutionSummary {
    #[serde(default)]
    pub resolved_strategy_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub unresolved_reason_counts: BTreeMap<String, i64>,
}

/// Geography and supply-region diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotGeographySummary {
    #[serde(default)]
    pub tier_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub tier_counts_by_strategy: BTreeMap<String, BTreeMap<String, i64>>,
    #[serde(default)]
    pub supply_region_source_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub supply_region_source_counts_by_strategy: BTreeMap<String, BTreeMap<String, i64>>,
    #[serde(default)]
    pub exchange_location_present_count: i64,
    #[serde(default)]
    pub exchange_location_present_count_by_strategy: BTreeMap<String, i64>,
    #[serde(default)]
    pub requested_location_granularity_counts: BTreeMap<String, i64>,
    #[serde(default)]
    pub requested_location_granularity_counts_by_strategy: BTreeMap<String, BTreeMap<String, i64>>,
}

/// Annual supply / production volume weight quality diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotVolumeWeightSummary {
    #[serde(default)]
    pub candidate_total: i64,
    #[serde(default)]
    pub valid_volume_count: i64,
    #[serde(default)]
    pub fallback_to_one_count: i64,
    #[serde(default)]
    pub decisions_total: i64,
    #[serde(default)]
    pub decisions_all_valid_count: i64,
    #[serde(default)]
    pub decisions_partial_missing_count: i64,
    #[serde(default)]
    pub decisions_all_missing_count: i64,
}

/// Top unmatched flow entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotUnmatchedFlowEntry {
    pub flow_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_name: Option<String>,
    pub count: i64,
}

/// Top process gap entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotProcessGapEntry {
    pub process_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_name: Option<String>,
    pub input_edges_total: i64,
    pub unmatched_no_provider: i64,
    pub a_write_pct: f64,
}

/// No-provider gap diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotGapSummary {
    #[serde(default)]
    pub unmatched_top_flows: Vec<SnapshotUnmatchedFlowEntry>,
    #[serde(default)]
    pub process_gap_top: Vec<SnapshotProcessGapEntry>,
}

/// Matching coverage diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotMatchingCoverage {
    pub input_edges_total: i64,
    pub matched_unique_provider: i64,
    pub matched_multi_provider: i64,
    pub unmatched_no_provider: i64,
    #[serde(default)]
    pub matched_multi_resolved: i64,
    #[serde(default)]
    pub matched_multi_unresolved: i64,
    #[serde(default)]
    pub matched_multi_fallback_equal: i64,
    #[serde(default)]
    pub a_input_edges_written: i64,
    #[serde(default)]
    pub residual_edges_total: i64,
    #[serde(default)]
    pub a_balance_edges_written: i64,
    #[serde(default)]
    pub a_write_pct: f64,
    #[serde(default)]
    pub provider_present_resolved_pct: f64,
    pub unique_provider_match_pct: f64,
    pub any_provider_match_pct: f64,
    #[serde(default)]
    pub provider_decision_diagnostics: SnapshotProviderDecisionDiagnostics,
    #[serde(default)]
    pub candidate_summary: SnapshotCandidateSummary,
    #[serde(default)]
    pub resolution_summary: SnapshotResolutionSummary,
    #[serde(default)]
    pub geography_summary: SnapshotGeographySummary,
    #[serde(default)]
    pub volume_weight_summary: SnapshotVolumeWeightSummary,
    #[serde(default)]
    pub gap_summary: SnapshotGapSummary,
}

/// Quantitative reference diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotReferenceCoverage {
    pub process_total: i64,
    pub normalized_process_count: i64,
    pub missing_reference_count: i64,
    pub invalid_reference_count: i64,
}

/// Allocation diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotAllocationCoverage {
    pub exchange_total: i64,
    pub allocation_fraction_present_pct: f64,
    pub allocation_fraction_missing_count: i64,
    pub allocation_fraction_invalid_count: i64,
    #[serde(default)]
    pub legacy_empty_allocation_as_undeclared_count: i64,
    #[serde(default)]
    pub legacy_single_output_target_inferred_count: i64,
    #[serde(default)]
    pub legacy_single_reference_target_inferred_count: i64,
}

/// Singular risk diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotSingularRisk {
    pub risk_level: String,
    pub prefilter_diag_abs_ge_cutoff: i64,
    pub postfilter_a_diag_abs_ge_cutoff: i64,
    pub m_zero_diagonal_count: i64,
    pub m_min_abs_diagonal: f64,
}

/// Matrix scale diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotMatrixScale {
    pub process_count: i64,
    pub flow_count: i64,
    pub impact_count: i64,
    pub a_nnz: i64,
    pub b_nnz: i64,
    pub c_nnz: i64,
    pub m_nnz_estimated: i64,
    pub m_sparsity_estimated: f64,
}

/// Snapshot coverage report persisted beside payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotCoverageReport {
    #[serde(default = "default_coverage_schema_version")]
    pub schema_version: String,
    pub matching: SnapshotMatchingCoverage,
    #[serde(default)]
    pub reference: SnapshotReferenceCoverage,
    #[serde(default)]
    pub allocation: SnapshotAllocationCoverage,
    pub singular_risk: SnapshotSingularRisk,
    pub matrix_scale: SnapshotMatrixScale,
}

fn default_coverage_schema_version() -> String {
    SNAPSHOT_COVERAGE_SCHEMA_VERSION.to_owned()
}

fn default_strict_mode() -> String {
    "strict".to_owned()
}

fn default_legacy_allocation_semantics_version() -> String {
    "legacy-unscoped-v0".to_owned()
}

fn default_legacy_link_semantics_version() -> String {
    "legacy-directional-link-v0".to_owned()
}

fn default_closed_boundary_policy() -> String {
    "closed".to_owned()
}

fn default_flow_identity_policy() -> String {
    "exact-flow-version-reference-unit-v1".to_owned()
}

fn default_biosphere_sign_mode() -> String {
    "signed".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotArtifactEnvelope {
    version: u8,
    format: String,
    snapshot_id: Uuid,
    config: SnapshotBuildConfig,
    coverage: SnapshotCoverageReport,
    payload: ModelSparseData,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compiled_graph: Option<CompiledGraph>,
}

/// Encoded snapshot artifact bytes and metadata.
#[derive(Debug, Clone)]
pub struct EncodedSnapshotArtifact {
    pub bytes: Vec<u8>,
    pub sha256: String,
    pub byte_size: usize,
    pub format: &'static str,
    pub content_type: &'static str,
    pub extension: &'static str,
}

/// Decoded snapshot artifact payload.
#[derive(Debug, Clone)]
pub struct DecodedSnapshotArtifact {
    pub snapshot_id: Uuid,
    pub config: SnapshotBuildConfig,
    pub coverage: SnapshotCoverageReport,
    pub payload: ModelSparseData,
    pub compiled_graph: Option<CompiledGraph>,
}

/// Encodes one snapshot matrix payload into `HDF5`.
pub fn encode_snapshot_artifact(
    snapshot_id: Uuid,
    config: SnapshotBuildConfig,
    coverage: SnapshotCoverageReport,
    payload: &ModelSparseData,
) -> anyhow::Result<EncodedSnapshotArtifact> {
    encode_snapshot_artifact_with_graph(snapshot_id, config, coverage, payload, None)
}

/// Encodes one snapshot matrix payload plus optional compiled graph metadata into `HDF5`.
pub fn encode_snapshot_artifact_with_graph(
    snapshot_id: Uuid,
    config: SnapshotBuildConfig,
    coverage: SnapshotCoverageReport,
    payload: &ModelSparseData,
    compiled_graph: Option<CompiledGraph>,
) -> anyhow::Result<EncodedSnapshotArtifact> {
    let envelope = SnapshotArtifactEnvelope {
        version: SCHEMA_VERSION,
        format: SNAPSHOT_ARTIFACT_FORMAT.to_owned(),
        snapshot_id,
        config,
        coverage,
        payload: payload.clone(),
        compiled_graph,
    };

    let json = serde_json::to_vec(&envelope)?;
    let temp = Builder::new()
        .prefix("lca-snapshot-artifact-")
        .suffix(".h5")
        .tempfile()?;
    write_hdf5_file(temp.path(), json.as_slice())?;
    let bytes = std::fs::read(temp.path())?;

    let mut hasher = Sha256::new();
    hasher.update(bytes.as_slice());
    let sha256 = format!("{:x}", hasher.finalize());

    Ok(EncodedSnapshotArtifact {
        byte_size: bytes.len(),
        bytes,
        sha256,
        format: SNAPSHOT_ARTIFACT_FORMAT,
        content_type: SNAPSHOT_ARTIFACT_CONTENT_TYPE,
        extension: SNAPSHOT_ARTIFACT_EXTENSION,
    })
}

/// Decodes snapshot artifact bytes into sparse payload.
pub fn decode_snapshot_artifact(bytes: &[u8]) -> anyhow::Result<DecodedSnapshotArtifact> {
    let temp = Builder::new()
        .prefix("lca-snapshot-artifact-read-")
        .suffix(".h5")
        .tempfile()?;
    std::fs::write(temp.path(), bytes)?;

    let file = File::open(temp.path())?;
    let format_bytes = file
        .dataset(DATASET_FORMAT)?
        .read_1d::<u8>()?
        .into_raw_vec();
    let format = String::from_utf8(format_bytes)?;
    if format != SNAPSHOT_ARTIFACT_FORMAT {
        return Err(anyhow::anyhow!(
            "unsupported snapshot artifact format: {format}"
        ));
    }

    let envelope_bytes = file
        .dataset(DATASET_ENVELOPE_JSON)?
        .read_1d::<u8>()?
        .into_raw_vec();
    let envelope: SnapshotArtifactEnvelope = serde_json::from_slice(&envelope_bytes)?;
    if envelope.payload.model_version != envelope.snapshot_id {
        return Err(anyhow::anyhow!(
            "snapshot payload model_version mismatch: payload={} envelope={}",
            envelope.payload.model_version,
            envelope.snapshot_id
        ));
    }

    Ok(DecodedSnapshotArtifact {
        snapshot_id: envelope.snapshot_id,
        config: envelope.config,
        coverage: envelope.coverage,
        payload: envelope.payload,
        compiled_graph: envelope.compiled_graph,
    })
}

fn write_hdf5_file(path: &Path, envelope_json: &[u8]) -> anyhow::Result<()> {
    let file = File::create(path)?;
    file.new_dataset_builder()
        .with_data(&[SCHEMA_VERSION])
        .create(DATASET_SCHEMA_VERSION)?;
    file.new_dataset_builder()
        .with_data(SNAPSHOT_ARTIFACT_FORMAT.as_bytes())
        .create(DATASET_FORMAT)?;
    if !hdf5::filters::deflate_available() {
        return Err(anyhow::anyhow!(
            "HDF5 deflate filter is unavailable; zlib-enabled HDF5 is required"
        ));
    }
    let chunk_len = envelope_json.len().clamp(1, HDF5_CHUNK_TARGET_BYTES);
    file.new_dataset_builder()
        .chunk((chunk_len,))
        .deflate(HDF5_DEFLATE_LEVEL)
        .with_data(envelope_json)
        .create(DATASET_ENVELOPE_JSON)?;
    file.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use hdf5::File;
    use hdf5::filters::Filter;
    use serde_json::json;
    use solver_core::{ModelSparseData, SparseTriplet};
    use std::collections::BTreeMap;
    use tempfile::Builder;

    use crate::compiled_graph::{
        CompiledAllocationStats, CompiledFlow, CompiledFlowKind, CompiledGraph,
        CompiledMatchingStats, CompiledReferenceStats,
    };

    use super::{
        DATASET_ENVELOPE_JSON, HDF5_DEFLATE_LEVEL, SNAPSHOT_ARTIFACT_FORMAT,
        SNAPSHOT_COVERAGE_SCHEMA_VERSION, SnapshotAllocationCoverage, SnapshotBuildConfig,
        SnapshotCandidateSummary, SnapshotCoverageReport, SnapshotGapSummary,
        SnapshotGeographySummary, SnapshotMatchingCoverage, SnapshotMatrixScale,
        SnapshotProviderDecisionDiagnostics, SnapshotReferenceCoverage, SnapshotResolutionSummary,
        SnapshotSelectionMode, SnapshotSingularRisk, SnapshotVolumeWeightSummary,
        decode_snapshot_artifact, encode_snapshot_artifact, encode_snapshot_artifact_with_graph,
    };

    #[test]
    #[allow(clippy::too_many_lines)]
    fn encode_decode_snapshot_artifact_roundtrip() {
        let snapshot_id = uuid::Uuid::new_v4();
        let config = SnapshotBuildConfig {
            process_states: crate::default_snapshot_process_states_arg(),
            include_user_id: None,
            data_scope: None,
            scope_manifest_sha256: None,
            lcia_method_factor_source: None,
            selection_mode: SnapshotSelectionMode::FilteredLibrary,
            request_roots: Vec::new(),
            process_limit: 0,
            provider_rule: "strict_unique_provider".to_owned(),
            provider_candidate_eligibility_mode: "reference_output_only".to_owned(),
            reference_normalization_mode: "strict".to_owned(),
            allocation_fraction_mode: "strict".to_owned(),
            allocation_semantics_version: "tidas-quantitative-reference-v2".to_owned(),
            link_semantics_version: "legacy-directional-link-v0".to_owned(),
            technosphere_boundary_policy: "closed".to_owned(),
            flow_identity_policy: "exact-flow-version-reference-unit-v1".to_owned(),
            biosphere_sign_mode: "gross".to_owned(),
            self_loop_cutoff: 0.999_999,
            singular_eps: 1e-12,
            has_lcia: true,
            artifact_purpose: None,
            root_dependency_fingerprint: None,
            root_revision_checksum: None,
            method_id: Some(uuid::Uuid::new_v4()),
            method_version: Some("01.00.000".to_owned()),
        };
        let coverage = SnapshotCoverageReport {
            schema_version: SNAPSHOT_COVERAGE_SCHEMA_VERSION.to_owned(),
            matching: SnapshotMatchingCoverage {
                input_edges_total: 10,
                matched_unique_provider: 7,
                matched_multi_provider: 2,
                unmatched_no_provider: 1,
                matched_multi_resolved: 1,
                matched_multi_unresolved: 1,
                matched_multi_fallback_equal: 0,
                a_input_edges_written: 8,
                residual_edges_total: 10,
                a_balance_edges_written: 8,
                a_write_pct: 80.0,
                provider_present_resolved_pct: 88.888_888_888_888_89,
                unique_provider_match_pct: 70.0,
                any_provider_match_pct: 90.0,
                provider_decision_diagnostics: SnapshotProviderDecisionDiagnostics {
                    resolved_strategy_counts: BTreeMap::from([
                        ("unique_provider".to_owned(), 7),
                        ("split_by_evidence".to_owned(), 1),
                    ]),
                    unresolved_reason_counts: BTreeMap::from([(
                        "rule_requires_unique_provider".to_owned(),
                        1,
                    )]),
                    candidate_eligibility_counts: BTreeMap::new(),
                    rejected_non_reference_output_count: 0,
                    volume_fallback_to_one_count: 0,
                    geography_tier_counts: BTreeMap::new(),
                    supply_region_source_counts: BTreeMap::new(),
                },
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
                exchange_total: 4,
                allocation_fraction_present_pct: 100.0,
                allocation_fraction_missing_count: 0,
                allocation_fraction_invalid_count: 0,
                legacy_empty_allocation_as_undeclared_count: 2,
                legacy_single_output_target_inferred_count: 1,
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
                a_nnz: 2,
                b_nnz: 2,
                c_nnz: 1,
                m_nnz_estimated: 4,
                m_sparsity_estimated: 0.0,
            },
        };
        let payload = ModelSparseData {
            model_version: snapshot_id,
            process_count: 2,
            flow_count: 2,
            impact_count: 1,
            technosphere_entries: vec![
                SparseTriplet {
                    row: 0,
                    col: 1,
                    value: 0.1,
                },
                SparseTriplet {
                    row: 1,
                    col: 0,
                    value: 0.2,
                },
            ],
            biosphere_entries: vec![
                SparseTriplet {
                    row: 0,
                    col: 0,
                    value: 1.0,
                },
                SparseTriplet {
                    row: 1,
                    col: 1,
                    value: -2.0,
                },
            ],
            characterization_factors: vec![SparseTriplet {
                row: 0,
                col: 1,
                value: 3.5,
            }],
        };

        let encoded =
            encode_snapshot_artifact(snapshot_id, config.clone(), coverage.clone(), &payload)
                .expect("encode");
        assert_eq!(encoded.format, SNAPSHOT_ARTIFACT_FORMAT);
        assert_eq!(encoded.byte_size, encoded.bytes.len());
        let file = write_and_open_hdf5(encoded.bytes.as_slice());
        let envelope_ds = file
            .dataset(DATASET_ENVELOPE_JSON)
            .expect("envelope_json dataset");
        assert!(envelope_ds.is_chunked());
        let filters = envelope_ds.filters();
        assert!(filters.iter().any(
            |filter| matches!(filter, Filter::Deflate(level) if *level == HDF5_DEFLATE_LEVEL)
        ));

        let decoded = decode_snapshot_artifact(encoded.bytes.as_slice()).expect("decode");
        assert_eq!(decoded.snapshot_id, snapshot_id);
        assert_eq!(decoded.config, config);
        assert_eq!(decoded.coverage, coverage);
        assert_eq!(decoded.payload, payload);
        assert!(decoded.compiled_graph.is_none());

        let product_flow_id = uuid::Uuid::new_v4();
        let graph = CompiledGraph {
            processes: Vec::new(),
            flows: vec![CompiledFlow {
                flow_idx: 0,
                flow_id: product_flow_id,
                kind: CompiledFlowKind::Product,
                space: crate::compiled_graph::CompiledFlowSpace::Technosphere,
                source_type: crate::compiled_graph::CompiledSourceFlowType::Product,
            }],
            reference_ports: Vec::new(),
            balance_resolutions: Vec::new(),
            unresolved_balances: Vec::new(),
            provider_outputs: Vec::new(),
            provider_decisions: Vec::new(),
            technosphere_edges: Vec::new(),
            biosphere_edges: Vec::new(),
            reference_stats: CompiledReferenceStats::default(),
            allocation_stats: CompiledAllocationStats {
                legacy_empty_allocation_as_undeclared_count: 2,
                legacy_single_output_target_inferred_count: 1,
                ..CompiledAllocationStats::default()
            },
            matching_stats: CompiledMatchingStats::default(),
            release_evidence: None,
        };
        let encoded_with_graph = encode_snapshot_artifact_with_graph(
            snapshot_id,
            config,
            coverage,
            &payload,
            Some(graph),
        )
        .expect("encode with graph");
        let decoded_with_graph =
            decode_snapshot_artifact(encoded_with_graph.bytes.as_slice()).expect("decode graph");
        let decoded_graph = decoded_with_graph.compiled_graph.expect("compiled graph");
        assert_eq!(decoded_graph.flows.len(), 1);
        assert_eq!(decoded_graph.flows[0].flow_id, product_flow_id);
        assert_eq!(decoded_graph.flows[0].kind, CompiledFlowKind::Product);
        assert_eq!(
            decoded_graph
                .allocation_stats
                .legacy_empty_allocation_as_undeclared_count,
            2
        );
        assert_eq!(
            decoded_graph
                .allocation_stats
                .legacy_single_output_target_inferred_count,
            1
        );
    }

    #[test]
    fn snapshot_build_config_defaults_legacy_biosphere_sign_mode() {
        let legacy = json!({
            "process_states": "100",
            "process_limit": 0,
            "provider_rule": "strict_unique_provider",
            "reference_normalization_mode": "strict",
            "allocation_fraction_mode": "strict",
            "self_loop_cutoff": 0.999_999,
            "singular_eps": 1e-12,
            "has_lcia": true,
            "method_id": null,
            "method_version": null
        });
        let parsed: SnapshotBuildConfig = serde_json::from_value(legacy).expect("parse legacy");
        assert_eq!(parsed.biosphere_sign_mode, "signed");
        assert_eq!(parsed.allocation_semantics_version, "legacy-unscoped-v0");
        assert_eq!(parsed.include_user_id, None);
        assert_eq!(
            parsed.selection_mode,
            SnapshotSelectionMode::FilteredLibrary
        );
        assert!(parsed.request_roots.is_empty());
    }

    #[test]
    fn allocation_coverage_defaults_legacy_fallback_counts_to_zero() {
        let parsed: SnapshotAllocationCoverage = serde_json::from_value(json!({
            "exchange_total": 4,
            "allocation_fraction_present_pct": 50.0,
            "allocation_fraction_missing_count": 2,
            "allocation_fraction_invalid_count": 0
        }))
        .expect("parse legacy allocation coverage");

        assert_eq!(parsed.legacy_empty_allocation_as_undeclared_count, 0);
        assert_eq!(parsed.legacy_single_output_target_inferred_count, 0);
    }

    fn write_and_open_hdf5(bytes: &[u8]) -> File {
        let temp = Builder::new()
            .prefix("lca-snapshot-artifact-test-")
            .suffix(".h5")
            .tempfile()
            .expect("create tempfile");
        std::fs::write(temp.path(), bytes).expect("write hdf5 bytes");
        File::open(temp.path()).expect("open hdf5 file")
    }
}
