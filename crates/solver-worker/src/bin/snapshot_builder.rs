#![allow(
    clippy::cast_precision_loss,
    clippy::collapsible_if,
    clippy::comparison_chain,
    clippy::format_push_string,
    clippy::needless_raw_string_hashes,
    clippy::reserve_after_initialization,
    clippy::struct_excessive_bools,
    clippy::struct_field_names,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::uninlined_format_args
)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};
use clap::Parser;
use rayon::prelude::*;
use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use solver_core::{ModelSparseData, SparseTriplet};
use solver_worker::calculation_evidence::{
    CALCULATION_EVIDENCE_SCHEMA_VERSION, FACTOR_COVERAGE_EVIDENCE_SCHEMA_VERSION,
    LcaCalculationEvidence, LcaMethodFactorSourceSnapshot, LciaFactorCoverageCounts,
    LciaFactorCoverageEvidence, LciaMethodFactorCoverage, LciaUncharacterizedEvidenceArtifact,
    LciaUncharacterizedRecord, MISSING_FACTOR_SEMANTICS, PUBLIC_PLUS_OWNER_DRAFT_SCOPE,
    PublicOwnerDraftBuildRequest, UNCHARACTERIZED_ARTIFACT_FORMAT, ValidatedPublicOwnerDraftScope,
    validate_calculation_evidence, validate_public_owner_draft_build_request,
};
use solver_worker::compiled_graph::{
    CompiledAllocationStats, CompiledBiosphereEdge, CompiledEdgePartition, CompiledFlow,
    CompiledFlowKind, CompiledGraph, CompiledMatchingStats, CompiledProcess,
    CompiledProviderAllocation, CompiledProviderCandidate, CompiledProviderCandidateEligibility,
    CompiledProviderDecision, CompiledProviderDecisionKind, CompiledProviderFailureReason,
    CompiledProviderGeographyTier, CompiledProviderOutput, CompiledProviderOutputAllocationState,
    CompiledProviderResolutionStrategy, CompiledProviderSupplyRegionSource, CompiledReferenceStats,
    CompiledTechnosphereEdge,
};
use solver_worker::db_pool::{APP_SNAPSHOT_BUILDER, WorkerDbPoolOptions};
use solver_worker::graph_types::{
    RequestRootProcess, ResolvedScopeProcess, ScopeProcessPartition, SnapshotSelectionMode,
};
use solver_worker::local_reports::{
    DEFAULT_LOCAL_SNAPSHOT_REPORT_MAX_FILES, DEFAULT_LOCAL_SNAPSHOT_REPORT_MIN_FREE_BYTES,
    DEFAULT_LOCAL_SNAPSHOT_REPORT_RETENTION_DAYS, LocalReportWriteOutcome, LocalSnapshotReportMode,
    LocalSnapshotReportPolicy, validate_local_snapshot_report_policy,
    write_optional_local_report_files,
};
use solver_worker::pgbouncer_sqlx::{self as sqlx, PgPool, Row};
use solver_worker::readiness::{
    MatrixReadinessInput, MatrixReadinessPolicy, MatrixReadinessReport, verify_matrix_readiness,
};
use solver_worker::snapshot_artifacts::{
    SNAPSHOT_ARTIFACT_FORMAT, SnapshotAllocationCoverage, SnapshotBuildConfig,
    SnapshotCandidateSummary, SnapshotCoverageReport, SnapshotGapSummary, SnapshotGeographySummary,
    SnapshotMatchingCoverage, SnapshotMatrixScale, SnapshotProcessGapEntry,
    SnapshotProviderDecisionDiagnostics, SnapshotReferenceCoverage, SnapshotResolutionSummary,
    SnapshotSingularRisk, SnapshotUnmatchedFlowEntry, SnapshotVolumeWeightSummary,
    decode_snapshot_artifact, encode_snapshot_artifact, encode_snapshot_artifact_with_graph,
};
use solver_worker::snapshot_index::{
    SnapshotImpactMapEntry, SnapshotIndexDocument, SnapshotProcessMapEntry,
};
use solver_worker::static_lcia_cache::{
    StaticLciaDirection, TrustedStaticCacheSource, VerifiedStaticLciaBundle,
    load_verified_static_lcia_bundle,
};
use solver_worker::storage::ObjectStoreClient;
use uuid::Uuid;

const REVIEW_SUBMIT_OVERLAY_ARTIFACT_PURPOSE: &str = "review_submit_overlay";
const REVIEW_SUBMIT_BASELINE_ARTIFACT_PURPOSE: &str = "review_submit_baseline";
const REVIEW_SUBMIT_BASELINE_TTL_SECONDS: i64 = 30 * 24 * 60 * 60;
const DEFAULT_SNAPSHOT_DB_STATEMENT_TIMEOUT_SECONDS: u64 = 900;
const SLOW_QUERY_LOG_THRESHOLD: Duration = Duration::from_secs(30);
const MAX_LCIA_GAP_EVIDENCE_RECORDS: u64 = 25_000_000;
const MAX_LCIA_GAP_EVIDENCE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Parser)]
#[command(name = "snapshot-builder")]
struct Cli {
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    #[arg(long, env = "CONN")]
    conn: Option<String>,
    #[arg(
        long,
        env = "SNAPSHOT_BUILDER_DB_MAX_CONNECTIONS",
        default_value_t = 4_u32
    )]
    db_max_connections: u32,
    #[arg(
        long,
        env = "SNAPSHOT_BUILDER_DB_ACQUIRE_TIMEOUT_SECONDS",
        default_value_t = 30_u64
    )]
    db_acquire_timeout_seconds: u64,
    #[arg(
        long,
        env = "SNAPSHOT_DB_STATEMENT_TIMEOUT_SECONDS",
        default_value_t = DEFAULT_SNAPSHOT_DB_STATEMENT_TIMEOUT_SECONDS
    )]
    db_statement_timeout_seconds: u64,
    #[arg(long, env = "S3_ENDPOINT")]
    s3_endpoint: Option<String>,
    #[arg(long, env = "S3_REGION")]
    s3_region: Option<String>,
    #[arg(long, env = "S3_BUCKET")]
    s3_bucket: Option<String>,
    #[arg(long, env = "S3_ACCESS_KEY_ID")]
    s3_access_key_id: Option<String>,
    #[arg(long, env = "S3_SECRET_ACCESS_KEY")]
    s3_secret_access_key: Option<String>,
    #[arg(long, env = "S3_SESSION_TOKEN")]
    s3_session_token: Option<String>,
    #[arg(long, env = "S3_PREFIX", default_value = "lca-results")]
    s3_prefix: String,
    #[arg(long)]
    snapshot_id: Option<Uuid>,
    #[arg(long, default_value_t = solver_worker::default_snapshot_process_states_arg())]
    process_states: String,
    #[arg(long)]
    include_user_id: Option<Uuid>,
    #[arg(long)]
    all_states: Option<bool>,
    #[arg(long)]
    include_user_state_codes: Option<String>,
    #[arg(long, default_value_t = false)]
    include_user_unassigned_only: bool,
    #[arg(long, default_value_t = false)]
    include_user_review_free_only: bool,
    #[arg(long)]
    data_scope: Option<String>,
    #[arg(long)]
    scope_manifest_json: Option<String>,
    #[arg(long)]
    scope_manifest_sha256: Option<String>,
    #[arg(long)]
    lcia_method_factor_source_json: Option<String>,
    #[arg(long)]
    lcia_factor_coverage_contract_json: Option<String>,
    #[arg(long, env = "LCIA_STATIC_CACHE_DIR")]
    lcia_static_cache_dir: Option<PathBuf>,
    #[arg(long, env = "LCIA_STATIC_CACHE_BASE_URL")]
    lcia_static_cache_base_url: Option<String>,
    #[arg(long = "root-process")]
    root_processes: Vec<RequestRootProcess>,
    #[arg(long, default_value_t = 0)]
    process_limit: usize,
    #[arg(long, default_value = "split_by_process_volume")]
    provider_rule: String,
    #[arg(long, default_value_t = false)]
    provider_rule_replay: bool,
    #[arg(long, default_value_t = false)]
    provider_rule_replay_only: bool,
    #[arg(
        long,
        default_value = "split_by_process_volume,strict_unique_provider,best_provider_strict,split_by_evidence,split_by_evidence_hybrid,split_equal"
    )]
    provider_rule_replay_rules: String,
    #[arg(long, default_value = "strict")]
    reference_normalization_mode: String,
    #[arg(long, default_value = "strict")]
    allocation_fraction_mode: String,
    #[arg(long, default_value_t = 0.999_999)]
    self_loop_cutoff: f64,
    #[arg(long, default_value_t = 1e-12)]
    singular_eps: f64,
    #[arg(long)]
    method_id: Option<Uuid>,
    #[arg(long)]
    method_version: Option<String>,
    #[arg(long)]
    no_lcia: bool,
    #[arg(long)]
    artifact_purpose: Option<String>,
    #[arg(long)]
    artifact_expires_in_seconds: Option<i64>,
    #[arg(long)]
    reuse_max_age_seconds: Option<i64>,
    #[arg(long)]
    review_submit_revision_checksum: Option<String>,
    #[arg(long, default_value = "reports/snapshot-coverage")]
    report_dir: PathBuf,
    #[arg(long, env = "SNAPSHOT_REPORT_MODE", default_value = "guarded")]
    snapshot_report_mode: String,
    #[arg(
        long,
        env = "SNAPSHOT_REPORT_RETENTION_DAYS",
        default_value_t = DEFAULT_LOCAL_SNAPSHOT_REPORT_RETENTION_DAYS
    )]
    snapshot_report_retention_days: u64,
    #[arg(
        long,
        env = "SNAPSHOT_REPORT_MAX_FILES",
        default_value_t = DEFAULT_LOCAL_SNAPSHOT_REPORT_MAX_FILES
    )]
    snapshot_report_max_files: usize,
    #[arg(
        long,
        env = "SNAPSHOT_REPORT_MIN_FREE_BYTES",
        default_value_t = DEFAULT_LOCAL_SNAPSHOT_REPORT_MIN_FREE_BYTES
    )]
    snapshot_report_min_free_bytes: u64,
}

#[derive(Debug, Clone)]
struct ProcessRow {
    id: Uuid,
    version: String,
    model_id: Option<Uuid>,
    user_id: Option<Uuid>,
    state_code: i32,
    team_id: Option<Uuid>,
    review_id: Option<Uuid>,
    modified_at: Option<DateTime<Utc>>,
    json: Value,
}

#[derive(Debug, Clone)]
struct FlowRow {
    id: Uuid,
    version: String,
    user_id: Option<Uuid>,
    state_code: i32,
    team_id: Option<Uuid>,
    review_id: Option<Uuid>,
    json: Value,
}

#[derive(Debug, Clone)]
struct MethodRow {
    id: Uuid,
    version: String,
    json: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
enum ExchangeDirection {
    Input,
    Output,
}

impl ExchangeDirection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Input => "Input",
            Self::Output => "Output",
        }
    }
}

fn parse_exchange_direction(value: Option<&str>) -> Option<ExchangeDirection> {
    match value.map(str::trim) {
        Some("Input") => Some(ExchangeDirection::Input),
        Some("Output") => Some(ExchangeDirection::Output),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct ParsedExchange {
    process_idx: i32,
    flow_id: Uuid,
    direction: Option<ExchangeDirection>,
    direction_label: String,
    internal_id: Option<String>,
    exchange_id: String,
    flow_version: String,
    is_reference_exchange: bool,
    amount: Option<f64>,
    allocation_state: AllocationFractionState,
    location: Option<String>,
}

#[derive(Debug, Clone)]
struct ProviderOutputCandidate {
    flow_id: Uuid,
    provider_idx: i32,
    output_exchange_internal_id: Option<String>,
    output_exchange_is_reference: bool,
    output_normalized_amount: Option<f64>,
    output_allocation_state: AllocationFractionState,
}

type ProviderMap = HashMap<Uuid, Vec<ProviderOutputCandidate>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderRule {
    StrictUniqueProvider,
    BestProviderStrict,
    SplitByProcessVolume,
    SplitByEvidenceStrict,
    SplitByEvidenceHybrid,
    SplitEqual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NormalizationMode {
    Strict,
    Lenient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AllocationMode {
    Strict,
    Lenient,
}

#[derive(Debug, Clone)]
struct ProcessMeta {
    process_idx: i32,
    process_id: Uuid,
    process_version: String,
    process_name: Option<String>,
    model_id: Option<Uuid>,
    location: Option<String>,
    reference_year: Option<i32>,
    annual_supply_or_production_volume: Option<f64>,
}

#[derive(Debug, Clone)]
struct ProviderCandidateScore {
    provider_idx: i32,
    provider_id: Uuid,
    geo_score: f64,
    time_score: f64,
    final_score: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SupplyRegionAnchor {
    source: CompiledProviderSupplyRegionSource,
    location: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ReferenceParseStats {
    missing_reference: i64,
    invalid_reference: i64,
    normalized_processes: i64,
}

#[derive(Debug, Clone, Copy, Default)]
struct AllocationParseStats {
    exchange_total: i64,
    fraction_present_count: i64,
    fraction_missing_count: i64,
    fraction_invalid_count: i64,
}

#[derive(Debug, Clone)]
struct MethodSelection {
    has_lcia: bool,
    method_id: Option<Uuid>,
    method_version: Option<String>,
    method_count: i64,
    factor_count: i64,
    source_evidence: Option<LcaMethodFactorSourceSnapshot>,
    rows: Vec<MethodRow>,
    static_bundle: Option<VerifiedStaticLciaBundle>,
}

#[derive(Debug, Clone)]
struct ImpactFactorSet {
    impact_id: Uuid,
    method_version: String,
    artifact_locator_id: Uuid,
    impact_key: String,
    impact_name: String,
    unit: String,
    factors_by_flow: HashMap<Uuid, f64>,
    factors_by_flow_direction: HashMap<(Uuid, ExchangeDirection), f64>,
}

#[derive(Debug)]
struct BuildOutput {
    data: ModelSparseData,
    coverage: SnapshotCoverageReport,
    snapshot_index: SnapshotIndexDocument,
    readiness: MatrixReadinessReport,
    compiled_graph: CompiledGraph,
    lcia_factor_coverage: Option<LciaFactorCoverageBuild>,
}

#[derive(Debug, Clone)]
struct CompiledScopeGraph {
    graph: CompiledGraph,
    lcia_exchange_observations: Vec<LciaExchangeObservation>,
}

#[derive(Debug, Clone)]
struct LciaExchangeObservation {
    flow_id: Uuid,
    flow_version: String,
    direction: Option<ExchangeDirection>,
    direction_label: String,
    exchange_id: String,
    amount: Option<f64>,
}

#[derive(Debug)]
struct LciaFactorCoverageBuild {
    counts: LciaFactorCoverageCounts,
    by_method: Vec<LciaMethodFactorCoverage>,
    records: tempfile::NamedTempFile,
    record_count: u64,
    artifact_byte_size: u64,
    artifact_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
struct ProviderRuleReplayRow {
    provider_rule: String,
    input_edges_total: i64,
    matched_unique_provider: i64,
    matched_multi_provider: i64,
    matched_multi_resolved: i64,
    matched_multi_unresolved: i64,
    matched_multi_fallback_equal: i64,
    unmatched_no_provider: i64,
    a_input_edges_written: i64,
    a_write_pct: f64,
    provider_present_resolved_pct: f64,
    resolved_strategy_counts: BTreeMap<String, i64>,
    unresolved_reason_counts: BTreeMap<String, i64>,
    volume_fallback_to_one_count: i64,
    geography_tier_counts: BTreeMap<String, i64>,
}

type ParsedProcessChunk = (
    ProcessMeta,
    Vec<ParsedExchange>,
    BTreeSet<Uuid>,
    ReferenceParseStats,
    AllocationParseStats,
);

#[derive(Debug, Clone, Serialize)]
struct SourceSnapshotSummary {
    process_count: i64,
    process_max_modified_at_utc: String,
    flow_count: i64,
    flow_max_modified_at_utc: String,
    lciamethod_count: i64,
    lciamethod_max_modified_at_utc: String,
}

#[derive(Debug, Clone)]
struct ReuseCandidate {
    snapshot_id: Uuid,
    artifact_url: String,
    coverage: SnapshotCoverageReport,
    process_count: i64,
    flow_count: i64,
    impact_count: i64,
    a_nnz: i64,
    b_nnz: i64,
    c_nnz: i64,
}

#[derive(Debug, Clone, Serialize)]
struct ResolvedRequestScopeSummary {
    selection_mode: SnapshotSelectionMode,
    scope_hash: String,
    roots: Vec<RequestRootProcess>,
    public_process_count: i64,
    private_process_count: i64,
    processes: Vec<ResolvedScopeProcess>,
}

#[derive(Debug, Clone)]
struct ResolvedProcessSelection {
    processes: Vec<ProcessRow>,
    scope_summary: ResolvedRequestScopeSummary,
}

#[derive(Debug, Clone, Default, Serialize)]
struct BuildTimingSec {
    reused_snapshot: bool,
    review_submit_baseline_reused: bool,
    review_submit_overlay_reused: bool,
    total_sec: f64,
    resolve_method_identity_sec: f64,
    compute_source_fingerprint_sec: f64,
    reuse_lookup_sec: f64,
    load_method_factors_sec: f64,
    build_sparse_payload_sec: f64,
    encode_artifact_sec: f64,
    upload_artifact_sec: f64,
    upload_snapshot_index_sec: f64,
    persist_metadata_sec: f64,
}

#[derive(Debug, Clone)]
struct MatchingDiagnosticsSummary {
    provider_decision_diagnostics: SnapshotProviderDecisionDiagnostics,
    candidate_summary: SnapshotCandidateSummary,
    resolution_summary: SnapshotResolutionSummary,
    geography_summary: SnapshotGeographySummary,
    volume_weight_summary: SnapshotVolumeWeightSummary,
    gap_summary: SnapshotGapSummary,
}

#[derive(Debug, Clone, Copy, Default)]
struct ProcessGapAccumulator {
    input_edges_total: i64,
    unmatched_no_provider: i64,
    a_input_edges_written: i64,
}

const GAP_TOP_K_LIMIT: usize = 20;

impl ProviderRule {
    fn as_str(self) -> &'static str {
        match self {
            Self::StrictUniqueProvider => "strict_unique_provider",
            Self::BestProviderStrict => "best_provider_strict",
            Self::SplitByProcessVolume => "split_by_process_volume",
            Self::SplitByEvidenceStrict => "split_by_evidence",
            Self::SplitByEvidenceHybrid => "split_by_evidence_hybrid",
            Self::SplitEqual => "split_equal",
        }
    }

    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "strict_unique_provider" => Ok(Self::StrictUniqueProvider),
            "best_provider_strict" => Ok(Self::BestProviderStrict),
            "split_by_process_volume" => Ok(Self::SplitByProcessVolume),
            "split_by_evidence" => Ok(Self::SplitByEvidenceStrict),
            "split_by_evidence_hybrid" => Ok(Self::SplitByEvidenceHybrid),
            "split_equal" => Ok(Self::SplitEqual),
            _ => Err(anyhow::anyhow!(
                "unsupported provider_rule={value}; expected one of: split_by_process_volume, strict_unique_provider, best_provider_strict, split_by_evidence, split_by_evidence_hybrid, split_equal"
            )),
        }
    }
}

impl NormalizationMode {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "strict" => Ok(Self::Strict),
            "lenient" => Ok(Self::Lenient),
            _ => Err(anyhow::anyhow!(
                "unsupported reference_normalization_mode={value}; expected strict or lenient"
            )),
        }
    }
}

impl AllocationMode {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "strict" => Ok(Self::Strict),
            "lenient" => Ok(Self::Lenient),
            _ => Err(anyhow::anyhow!(
                "unsupported allocation_fraction_mode={value}; expected strict or lenient"
            )),
        }
    }
}

fn snapshot_db_statement_timeout(seconds: u64) -> Option<Duration> {
    if seconds == 0 {
        None
    } else {
        Some(Duration::from_secs(seconds))
    }
}

fn snapshot_report_policy(cli: &Cli) -> anyhow::Result<LocalSnapshotReportPolicy> {
    validate_local_snapshot_report_policy(LocalSnapshotReportPolicy {
        retention_days: cli.snapshot_report_retention_days,
        max_files: cli.snapshot_report_max_files,
        min_free_bytes: cli.snapshot_report_min_free_bytes,
        mode: LocalSnapshotReportMode::parse(&cli.snapshot_report_mode)?,
    })
}

fn validate_versioned_scope_cli(
    cli: &Cli,
) -> anyhow::Result<Option<ValidatedPublicOwnerDraftScope>> {
    let has_versioned_field = cli.all_states.is_some()
        || cli.data_scope.is_some()
        || cli.scope_manifest_json.is_some()
        || cli.scope_manifest_sha256.is_some()
        || cli.lcia_method_factor_source_json.is_some()
        || cli.lcia_factor_coverage_contract_json.is_some()
        || cli.include_user_state_codes.is_some()
        || cli.include_user_unassigned_only
        || cli.include_user_review_free_only;
    if !has_versioned_field {
        return Ok(None);
    }

    let scope_manifest =
        parse_required_json_arg(cli.scope_manifest_json.as_deref(), "--scope-manifest-json")?;
    let method_source = parse_required_json_arg(
        cli.lcia_method_factor_source_json.as_deref(),
        "--lcia-method-factor-source-json",
    )?;
    let coverage_contract = parse_required_json_arg(
        cli.lcia_factor_coverage_contract_json.as_deref(),
        "--lcia-factor-coverage-contract-json",
    )?;
    validate_public_owner_draft_build_request(PublicOwnerDraftBuildRequest {
        all_states: cli.all_states,
        process_states: Some(&cli.process_states),
        include_user_id: cli.include_user_id,
        include_user_state_codes: cli.include_user_state_codes.as_deref(),
        include_user_unassigned_only: Some(cli.include_user_unassigned_only),
        include_user_review_free_only: Some(cli.include_user_review_free_only),
        data_scope: cli.data_scope.as_deref(),
        scope_manifest: Some(&scope_manifest),
        scope_manifest_sha256: cli.scope_manifest_sha256.as_deref(),
        lcia_method_factor_source: Some(&method_source),
        lcia_factor_coverage_contract: Some(&coverage_contract),
        no_lcia: Some(cli.no_lcia),
        requested_by: cli.include_user_id,
    })
    .map(Some)
}

fn parse_required_json_arg(value: Option<&str>, name: &str) -> anyhow::Result<Value> {
    let value = value.ok_or_else(|| anyhow::anyhow!("{name} is required for versioned scope"))?;
    serde_json::from_str(value).map_err(|error| anyhow::anyhow!("invalid {name}: {error}"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let total_started = Instant::now();
    let cli = Cli::parse();
    let versioned_scope = validate_versioned_scope_cli(&cli)?;
    let provider_rule = ProviderRule::parse(&cli.provider_rule)?;
    let reference_normalization_mode = NormalizationMode::parse(&cli.reference_normalization_mode)?;
    let allocation_mode = AllocationMode::parse(&cli.allocation_fraction_mode)?;
    let report_policy = snapshot_report_policy(&cli)?;

    let db_url = cli
        .database_url
        .as_deref()
        .or(cli.conn.as_deref())
        .ok_or_else(|| anyhow::anyhow!("missing DB connection: set DATABASE_URL or CONN"))?;
    let statement_timeout = snapshot_db_statement_timeout(cli.db_statement_timeout_seconds);
    if statement_timeout.is_none() {
        eprintln!(
            "[warn] SNAPSHOT_DB_STATEMENT_TIMEOUT_SECONDS=0 disables statement_timeout; use only for targeted manual recovery"
        );
    }
    let pool_options = WorkerDbPoolOptions::new(APP_SNAPSHOT_BUILDER)
        .max_connections(cli.db_max_connections.max(1))
        .acquire_timeout(Duration::from_secs(cli.db_acquire_timeout_seconds.max(1)))
        .statement_timeout(statement_timeout);
    println!(
        "[db_pool] application_name={} max_connections={} min_connections={} acquire_timeout_seconds={} idle_timeout_seconds={} max_lifetime_seconds={} statement_timeout_seconds={}",
        pool_options.application_name(),
        pool_options.max_connections_value(),
        pool_options.min_connections_value(),
        pool_options.acquire_timeout_value().as_secs(),
        pool_options.idle_timeout_value().as_secs(),
        pool_options.max_lifetime_value().as_secs(),
        pool_options
            .statement_timeout_value()
            .map_or(0, |duration| duration.as_secs())
    );
    let pool = pool_options.connect(db_url).await?;

    let requested_snapshot_id = cli.snapshot_id;
    let (all_states, state_codes, process_states_label) =
        parse_process_states(&cli.process_states)?;
    let request_roots = normalize_request_roots(&cli.root_processes);
    if !request_roots.is_empty() && cli.process_limit > 0 {
        return Err(anyhow::anyhow!(
            "--process-limit is not supported when --root-process is used"
        ));
    }
    let selection_mode = if request_roots.is_empty() {
        SnapshotSelectionMode::FilteredLibrary
    } else {
        SnapshotSelectionMode::RequestRootsClosure
    };
    let mut build_timing = BuildTimingSec::default();
    let method_started = Instant::now();
    let method = resolve_method_identity(&pool, &cli, versioned_scope.as_ref()).await?;
    build_timing.resolve_method_identity_sec = method_started.elapsed().as_secs_f64();
    let artifact_expires_in_seconds = positive_seconds(cli.artifact_expires_in_seconds);
    let reuse_max_age_seconds = positive_seconds(cli.reuse_max_age_seconds);
    let build_config = SnapshotBuildConfig {
        process_states: process_states_label.clone(),
        include_user_id: cli.include_user_id,
        data_scope: versioned_scope
            .as_ref()
            .map(|_| PUBLIC_PLUS_OWNER_DRAFT_SCOPE.to_owned()),
        scope_manifest_sha256: versioned_scope
            .as_ref()
            .map(|scope| scope.scope_manifest_sha256.clone()),
        lcia_method_factor_source: method.source_evidence.clone(),
        selection_mode,
        request_roots: request_roots.clone(),
        process_limit: i32::try_from(cli.process_limit)
            .map_err(|_| anyhow::anyhow!("process_limit overflow"))?,
        provider_rule: cli.provider_rule.clone(),
        provider_candidate_eligibility_mode: "reference_output_only".to_owned(),
        reference_normalization_mode: cli.reference_normalization_mode.clone(),
        allocation_fraction_mode: cli.allocation_fraction_mode.clone(),
        biosphere_sign_mode: "gross".to_owned(),
        self_loop_cutoff: cli.self_loop_cutoff,
        singular_eps: cli.singular_eps,
        has_lcia: method.has_lcia,
        artifact_purpose: cli.artifact_purpose.clone(),
        root_dependency_fingerprint: None,
        root_revision_checksum: cli.review_submit_revision_checksum.clone(),
        method_id: method.method_id,
        method_version: method.method_version.clone(),
    };
    let candidate_processes = fetch_processes(
        &pool,
        all_states,
        &state_codes,
        cli.include_user_id,
        versioned_scope.as_ref(),
    )
    .await?;
    let resolved_scope = resolve_process_selection(
        candidate_processes,
        all_states,
        &state_codes,
        cli.include_user_id,
        &request_roots,
        provider_rule,
        cli.process_limit,
    )?;
    let fingerprint_started = Instant::now();
    let (source_summary, source_fingerprint) = compute_source_fingerprint(
        &pool,
        &resolved_scope.processes,
        &build_config,
        versioned_scope.as_ref(),
        Some(&method),
    )
    .await?;
    build_timing.compute_source_fingerprint_sec = fingerprint_started.elapsed().as_secs_f64();

    if let Some(snapshot_id) = requested_snapshot_id {
        println!("[info] snapshot_id={snapshot_id} (requested)");
    } else {
        println!("[info] snapshot_id=auto");
    }
    println!("[info] process_states={process_states_label}");
    if let Some(include_user_id) = cli.include_user_id {
        println!("[info] include_user_id={include_user_id}");
    } else {
        println!("[info] include_user_id=none");
    }
    println!("[info] selection_mode={selection_mode}");
    println!(
        "[info] request_root_count={}",
        resolved_scope.scope_summary.roots.len()
    );
    println!(
        "[info] scope_hash={}",
        resolved_scope.scope_summary.scope_hash
    );
    println!("[info] process_limit={}", cli.process_limit);
    println!("[info] provider_rule={}", cli.provider_rule);
    println!(
        "[info] reference_normalization_mode={}",
        cli.reference_normalization_mode
    );
    println!(
        "[info] allocation_fraction_mode={}",
        cli.allocation_fraction_mode
    );
    println!("[info] biosphere_sign_mode=gross");
    println!("[info] self_loop_cutoff={}", cli.self_loop_cutoff);
    println!("[info] singular_eps={}", cli.singular_eps);
    if method.has_lcia {
        if let Some(method_id) = method.method_id {
            println!(
                "[info] lcia_method={}@{} factors={}",
                method_id,
                method.method_version.as_deref().unwrap_or_default(),
                method.factor_count
            );
        } else {
            println!(
                "[info] lcia_method=all methods={} factors={}",
                method.method_count, method.factor_count
            );
        }
    } else {
        println!("[info] lcia_method=disabled");
    }
    if let Some(purpose) = cli.artifact_purpose.as_deref() {
        println!("[info] artifact_purpose={purpose}");
    }
    if let Some(ttl_seconds) = artifact_expires_in_seconds {
        println!("[info] artifact_expires_in_seconds={ttl_seconds}");
    }
    if let Some(max_age_seconds) = reuse_max_age_seconds {
        println!("[info] reuse_max_age_seconds={max_age_seconds}");
    }
    println!(
        "[source] processes={} max_modified_at={} flows={} max_modified_at={} lciamethods={} max_modified_at={}",
        source_summary.process_count,
        source_summary.process_max_modified_at_utc,
        source_summary.flow_count,
        source_summary.flow_max_modified_at_utc,
        source_summary.lciamethod_count,
        source_summary.lciamethod_max_modified_at_utc
    );
    println!("[source] fingerprint={source_fingerprint}");

    if cli.provider_rule_replay || cli.provider_rule_replay_only {
        let replay_rules = parse_provider_rule_list(&cli.provider_rule_replay_rules)?;
        let replay_rows = run_provider_rule_replay(
            &pool,
            &resolved_scope.processes,
            cli.include_user_id,
            versioned_scope.as_ref(),
            &replay_rules,
            reference_normalization_mode,
            allocation_mode,
        )
        .await?;
        let replay_base = requested_snapshot_id.map_or_else(
            || resolved_scope.scope_summary.scope_hash.clone(),
            |id| id.to_string(),
        );
        let (replay_json_path, replay_md_path) = write_provider_rule_replay_report_files(
            &cli.report_dir,
            &replay_base,
            &build_config,
            &resolved_scope.scope_summary,
            &replay_rows,
        )?;
        println!("[provider_rule_replay_json] {}", replay_json_path.display());
        println!("[provider_rule_replay_md] {}", replay_md_path.display());
        for row in &replay_rows {
            println!(
                "[provider_rule_replay] rule={} a_write_pct={} provider_present_resolved_pct={} multi_unresolved={} fallback_equal={}",
                row.provider_rule,
                row.a_write_pct,
                row.provider_present_resolved_pct,
                row.matched_multi_unresolved,
                row.matched_multi_fallback_equal
            );
        }
        if cli.provider_rule_replay_only {
            return Ok(());
        }
    }

    let store = build_object_store(&cli)?;

    if is_review_submit_overlay_mode(&cli, &request_roots) {
        return run_review_submit_overlay_build(
            &pool,
            &store,
            &cli,
            requested_snapshot_id,
            total_started,
            all_states,
            &state_codes,
            cli.include_user_id,
            &request_roots,
            resolved_scope,
            build_config,
            method,
            provider_rule,
            reference_normalization_mode,
            allocation_mode,
            artifact_expires_in_seconds,
            reuse_max_age_seconds,
            report_policy,
        )
        .await;
    }

    let reuse_lookup_started = Instant::now();
    let reused_candidate =
        find_reusable_snapshot(&pool, &source_fingerprint, reuse_max_age_seconds).await?;
    build_timing.reuse_lookup_sec = reuse_lookup_started.elapsed().as_secs_f64();

    if let Some(reused) = reused_candidate {
        let snapshot_index_url = derive_snapshot_index_url(&reused.artifact_url);
        match store.download_object_url(&snapshot_index_url).await {
            Ok(index_bytes) => {
                validate_reusable_snapshot_index(
                    &index_bytes,
                    reused.snapshot_id,
                    versioned_scope.as_ref(),
                    &method,
                )?;
                let resolved_snapshot_id = reused.snapshot_id;

                build_timing.reused_snapshot = true;
                build_timing.total_sec = total_started.elapsed().as_secs_f64();
                write_report_files(
                    &cli.report_dir,
                    resolved_snapshot_id,
                    &build_config,
                    &resolved_scope.scope_summary,
                    &reused.coverage,
                    &reused.artifact_url,
                    &source_summary,
                    &source_fingerprint,
                    &build_timing,
                    report_policy,
                )?;
                println!(
                    "[reuse] matched existing ready snapshot={}",
                    reused.snapshot_id
                );
                if let Some(max_age_seconds) = reuse_max_age_seconds {
                    println!("[reuse] max_age_seconds={max_age_seconds}");
                }
                println!(
                    "[build_timing_sec] {}",
                    serde_json::to_string(&build_timing)?
                );
                println!("[resolved_snapshot_id] {resolved_snapshot_id}");
                println!("[done] snapshot ready: {resolved_snapshot_id}");
                println!("[artifact] {}", reused.artifact_url);
                println!("[snapshot_index] {snapshot_index_url}");
                println!(
                    "[matrix] process_count={} flow_count={} impact_count={} a_nnz={} b_nnz={} c_nnz={}",
                    reused.process_count,
                    reused.flow_count,
                    reused.impact_count,
                    reused.a_nnz,
                    reused.b_nnz,
                    reused.c_nnz
                );
                println!(
                    "[coverage] unique_match={} any_match={} singular_risk={}",
                    reused.coverage.matching.unique_provider_match_pct,
                    reused.coverage.matching.any_provider_match_pct,
                    reused.coverage.singular_risk.risk_level
                );
                return Ok(());
            }
            Err(error) => {
                println!(
                    "[reuse] skip snapshot={} because snapshot index sidecar is unavailable: {}",
                    reused.snapshot_id, error
                );
            }
        }
    }

    let factor_map_started = Instant::now();
    let impact_factor_sets = load_impact_factor_sets(&method)?;
    build_timing.load_method_factors_sec = factor_map_started.elapsed().as_secs_f64();

    let snapshot_id = requested_snapshot_id.unwrap_or_else(Uuid::new_v4);
    let build_started = Instant::now();
    let mut built = build_sparse_payload(
        &pool,
        snapshot_id,
        &method,
        resolved_scope.processes.clone(),
        cli.include_user_id,
        versioned_scope.as_ref(),
        provider_rule,
        reference_normalization_mode,
        allocation_mode,
        cli.self_loop_cutoff,
        cli.singular_eps,
        method.has_lcia,
        &impact_factor_sets,
    )
    .await?;
    build_timing.build_sparse_payload_sec = build_started.elapsed().as_secs_f64();

    let encode_started = Instant::now();
    let encoded = encode_snapshot_artifact(
        snapshot_id,
        build_config.clone(),
        built.coverage.clone(),
        &built.data,
    )?;
    build_timing.encode_artifact_sec = encode_started.elapsed().as_secs_f64();

    let upload_started = Instant::now();
    let artifact_url = store
        .upload_snapshot_artifact(
            snapshot_id,
            encoded.extension,
            encoded.content_type,
            encoded.bytes,
        )
        .await?;
    build_timing.upload_artifact_sec = upload_started.elapsed().as_secs_f64();

    attach_versioned_calculation_evidence(
        &store,
        snapshot_id,
        &mut built,
        versioned_scope.as_ref(),
        &method,
    )
    .await?;

    let snapshot_index_bytes = serde_json::to_vec(&built.snapshot_index)?;
    let upload_snapshot_index_started = Instant::now();
    let snapshot_index_url = store
        .upload_snapshot_index(snapshot_id, snapshot_index_bytes)
        .await?;
    build_timing.upload_snapshot_index_sec = upload_snapshot_index_started.elapsed().as_secs_f64();

    let persist_started = Instant::now();
    persist_snapshot_metadata(
        &pool,
        snapshot_id,
        &cli.provider_rule,
        all_states,
        &state_codes,
        cli.include_user_id,
        versioned_scope.as_ref(),
        &resolved_scope.scope_summary,
        &source_fingerprint,
        &method,
        &built,
        &artifact_url,
        &encoded.sha256,
        i64::try_from(encoded.byte_size).map_err(|_| anyhow::anyhow!("artifact too large"))?,
        encoded.format,
        cli.artifact_purpose.as_deref(),
        artifact_expires_in_seconds,
    )
    .await?;
    build_timing.persist_metadata_sec = persist_started.elapsed().as_secs_f64();
    build_timing.total_sec = total_started.elapsed().as_secs_f64();

    write_report_files(
        &cli.report_dir,
        snapshot_id,
        &build_config,
        &resolved_scope.scope_summary,
        &built.coverage,
        &artifact_url,
        &source_summary,
        &source_fingerprint,
        &build_timing,
        report_policy,
    )?;
    let readiness_path = write_matrix_readiness_report_file(
        &cli.report_dir,
        snapshot_id,
        &built.readiness,
        report_policy,
    )?;

    println!(
        "[build_timing_sec] {}",
        serde_json::to_string(&build_timing)?
    );
    println!("[resolved_snapshot_id] {snapshot_id}");
    println!("[done] snapshot ready: {snapshot_id}");
    println!("[artifact] {artifact_url}");
    println!("[snapshot_index] {snapshot_index_url}");
    println!(
        "[matrix] process_count={} flow_count={} a_nnz={} b_nnz={} c_nnz={}",
        built.data.process_count,
        built.data.flow_count,
        built.coverage.matrix_scale.a_nnz,
        built.coverage.matrix_scale.b_nnz,
        built.coverage.matrix_scale.c_nnz
    );
    println!(
        "[coverage] unique_match={} any_match={} singular_risk={}",
        built.coverage.matching.unique_provider_match_pct,
        built.coverage.matching.any_provider_match_pct,
        built.coverage.singular_risk.risk_level
    );
    if let Some(readiness_path) = readiness_path {
        println!("[matrix_readiness_report] {}", readiness_path.display());
    } else {
        println!("[matrix_readiness_report] skipped_local_report");
    }
    println!(
        "[matrix_readiness] status={:?} next_action={} blockers={} findings={}",
        built.readiness.status,
        built.readiness.next_action,
        built.readiness.blockers.len(),
        built.readiness.findings.len()
    );

    Ok(())
}

async fn attach_versioned_calculation_evidence(
    store: &ObjectStoreClient,
    snapshot_id: Uuid,
    built: &mut BuildOutput,
    versioned_scope: Option<&ValidatedPublicOwnerDraftScope>,
    method: &MethodSelection,
) -> anyhow::Result<()> {
    let Some(scope) = versioned_scope else {
        return Ok(());
    };
    let source = method.source_evidence.clone().ok_or_else(|| {
        anyhow::anyhow!("versioned scope build is missing LCIA method source evidence")
    })?;
    let coverage = built
        .lcia_factor_coverage
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("versioned scope build is missing LCIA factor coverage"))?;
    let gap_count = coverage
        .counts
        .unmatched
        .checked_add(coverage.counts.invalid)
        .and_then(|count| count.checked_add(coverage.counts.unsupported_direction))
        .ok_or_else(|| anyhow::anyhow!("LCIA factor coverage count overflow"))?;
    let record_count = coverage.record_count;
    if gap_count != record_count {
        return Err(anyhow::anyhow!(
            "LCIA gap count differs from uncharacterized record count"
        ));
    }
    let uncharacterized_evidence = if record_count == 0 {
        None
    } else {
        let artifact_byte_size = coverage.records.as_file().metadata()?.len();
        if artifact_byte_size != coverage.artifact_byte_size {
            return Err(anyhow::anyhow!(
                "LCIA gap evidence spool byte-size changed before upload"
            ));
        }
        let artifact_url = store
            .upload_snapshot_lcia_uncharacterized_evidence_file(
                snapshot_id,
                coverage.records.path(),
                artifact_byte_size,
            )
            .await?;
        Some(LciaUncharacterizedEvidenceArtifact {
            artifact_url,
            artifact_format: UNCHARACTERIZED_ARTIFACT_FORMAT.to_owned(),
            artifact_sha256: coverage.artifact_sha256.clone(),
            record_count,
        })
    };
    let evidence = LcaCalculationEvidence {
        schema_version: CALCULATION_EVIDENCE_SCHEMA_VERSION.to_owned(),
        scope_manifest_sha256: scope.scope_manifest_sha256.clone(),
        lcia_method_factor_source: source,
        lcia_factor_coverage: LciaFactorCoverageEvidence {
            schema_version: FACTOR_COVERAGE_EVIDENCE_SCHEMA_VERSION.to_owned(),
            source_snapshot_sha256: method
                .source_evidence
                .as_ref()
                .expect("source evidence checked above")
                .source_snapshot_sha256
                .clone(),
            method_manifest_sha256: method
                .source_evidence
                .as_ref()
                .expect("source evidence checked above")
                .method_manifest_sha256
                .clone(),
            factor_manifest_sha256: method
                .source_evidence
                .as_ref()
                .expect("source evidence checked above")
                .factor_manifest_sha256
                .clone(),
            method_identity_manifest_sha256: method
                .source_evidence
                .as_ref()
                .expect("source evidence checked above")
                .method_identity_manifest_sha256
                .clone(),
            count_unit: "exchange_method_pair".to_owned(),
            key_dimensions: ["method_id", "method_version", "flow_uuid", "direction"]
                .into_iter()
                .map(ToOwned::to_owned)
                .collect(),
            coverage_status: if gap_count == 0 {
                "complete".to_owned()
            } else {
                "incomplete_coverage".to_owned()
            },
            missing_factor_semantics: MISSING_FACTOR_SEMANTICS.to_owned(),
            counts: coverage.counts.clone(),
            by_method: coverage.by_method.clone(),
            uncharacterized_evidence,
        },
    };
    validate_calculation_evidence(&evidence)?;
    println!(
        "[calculation_evidence] {}",
        serde_json::to_string(&evidence)?
    );
    built.snapshot_index.calculation_evidence = Some(evidence);
    Ok(())
}

fn validate_reusable_snapshot_index(
    bytes: &[u8],
    snapshot_id: Uuid,
    versioned_scope: Option<&ValidatedPublicOwnerDraftScope>,
    method: &MethodSelection,
) -> anyhow::Result<()> {
    let index: SnapshotIndexDocument = serde_json::from_slice(bytes)?;
    if index.snapshot_id != snapshot_id {
        return Err(anyhow::anyhow!(
            "reusable snapshot index id mismatch: expected={snapshot_id} actual={}",
            index.snapshot_id
        ));
    }
    let Some(scope) = versioned_scope else {
        return Ok(());
    };
    let evidence = index
        .calculation_evidence
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("versioned reusable snapshot lacks calculation evidence"))?;
    validate_calculation_evidence(evidence)?;
    if evidence.scope_manifest_sha256 != scope.scope_manifest_sha256
        || method.source_evidence.as_ref() != Some(&evidence.lcia_method_factor_source)
    {
        return Err(anyhow::anyhow!(
            "versioned reusable snapshot calculation evidence drift"
        ));
    }
    Ok(())
}

fn build_object_store(cli: &Cli) -> anyhow::Result<ObjectStoreClient> {
    let endpoint = cli
        .s3_endpoint
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("missing S3_ENDPOINT"))?;
    let region = cli
        .s3_region
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("missing S3_REGION"))?;
    let bucket = cli
        .s3_bucket
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("missing S3_BUCKET"))?;
    let access_key_id = cli
        .s3_access_key_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("missing S3_ACCESS_KEY_ID"))?;
    let secret_access_key = cli
        .s3_secret_access_key
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("missing S3_SECRET_ACCESS_KEY"))?;

    ObjectStoreClient::new(
        endpoint,
        region,
        bucket,
        &cli.s3_prefix,
        access_key_id,
        secret_access_key,
        cli.s3_session_token.clone(),
    )
}

fn parse_process_states(input: &str) -> anyhow::Result<(bool, Vec<i32>, String)> {
    let trimmed = input.trim().replace(' ', "");
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("all") {
        return Ok((true, Vec::new(), "all".to_owned()));
    }

    let mut out = Vec::new();
    for token in trimmed.split(',') {
        let value: i32 = token
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid process state code: {token}"))?;
        out.push(value);
    }
    out.sort_unstable();
    out.dedup();
    let label = out.iter().map(i32::to_string).collect::<Vec<_>>().join(",");
    Ok((false, out, label))
}

fn derive_snapshot_index_url(artifact_url: &str) -> String {
    match artifact_url.rfind('/') {
        Some(idx) => format!("{}snapshot-index-v1.json", &artifact_url[..=idx]),
        None => format!("{artifact_url}/snapshot-index-v1.json"),
    }
}

fn positive_seconds(value: Option<i64>) -> Option<i64> {
    value.filter(|seconds| *seconds > 0)
}

fn artifact_expires_at_utc(ttl_seconds: Option<i64>) -> anyhow::Result<Option<String>> {
    let Some(ttl_seconds) = ttl_seconds else {
        return Ok(None);
    };
    let expires_at = Utc::now()
        .checked_add_signed(TimeDelta::seconds(ttl_seconds))
        .ok_or_else(|| anyhow::anyhow!("artifact expiration overflow: {ttl_seconds} seconds"))?;
    Ok(Some(
        expires_at.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
    ))
}

fn attach_artifact_lifecycle(
    process_filter: &mut Value,
    artifact_purpose: Option<&str>,
    artifact_expires_in_seconds: Option<i64>,
    artifact_expires_at_utc: Option<&str>,
) {
    if artifact_purpose.is_none()
        && artifact_expires_in_seconds.is_none()
        && artifact_expires_at_utc.is_none()
    {
        return;
    }

    let Some(root) = process_filter.as_object_mut() else {
        return;
    };
    let mut lifecycle = Map::new();
    if let Some(purpose) = artifact_purpose
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        lifecycle.insert("purpose".to_owned(), Value::String(purpose.to_owned()));
    }
    if let Some(ttl_seconds) = artifact_expires_in_seconds {
        lifecycle.insert("ttl_seconds".to_owned(), Value::from(ttl_seconds));
    }
    if let Some(expires_at) = artifact_expires_at_utc {
        lifecycle.insert(
            "expires_at_utc".to_owned(),
            Value::String(expires_at.to_owned()),
        );
    }
    if !lifecycle.is_empty() {
        root.insert("artifact_lifecycle".to_owned(), Value::Object(lifecycle));
    }
}

fn is_review_submit_overlay_mode(cli: &Cli, request_roots: &[RequestRootProcess]) -> bool {
    cli.artifact_purpose.as_deref() == Some(REVIEW_SUBMIT_OVERLAY_ARTIFACT_PURPOSE)
        && request_roots.len() == 1
}

#[allow(clippy::too_many_arguments)]
async fn run_review_submit_overlay_build(
    pool: &PgPool,
    store: &ObjectStoreClient,
    cli: &Cli,
    requested_snapshot_id: Option<Uuid>,
    total_started: Instant,
    all_states: bool,
    state_codes: &[i32],
    include_user_id: Option<Uuid>,
    request_roots: &[RequestRootProcess],
    resolved_scope: ResolvedProcessSelection,
    build_config: SnapshotBuildConfig,
    method: MethodSelection,
    provider_rule: ProviderRule,
    reference_normalization_mode: NormalizationMode,
    allocation_mode: AllocationMode,
    artifact_expires_in_seconds: Option<i64>,
    reuse_max_age_seconds: Option<i64>,
    report_policy: LocalSnapshotReportPolicy,
) -> anyhow::Result<()> {
    if method.has_lcia {
        return Err(anyhow::anyhow!(
            "review-submit overlay mode requires --no-lcia"
        ));
    }
    let Some(requested_snapshot_id) = requested_snapshot_id else {
        return Err(anyhow::anyhow!(
            "review-submit overlay mode requires --snapshot-id"
        ));
    };
    let root = request_roots
        .first()
        .ok_or_else(|| anyhow::anyhow!("review-submit overlay mode requires one root process"))?;
    let target_pos = resolved_scope
        .processes
        .iter()
        .position(|row| row.id == root.process_id && row.version.trim() == root.process_version);
    let Some(target_pos) = target_pos else {
        return Err(anyhow::anyhow!(
            "review-submit root process not found in resolved scope: {root}"
        ));
    };
    let target_row = resolved_scope.processes[target_pos].clone();
    let baseline_processes = resolved_scope
        .processes
        .iter()
        .enumerate()
        .filter_map(|(idx, row)| (idx != target_pos).then_some(row.clone()))
        .collect::<Vec<_>>();
    let baseline_scope_summary =
        review_submit_baseline_scope_summary(&resolved_scope.scope_summary, root)?;
    let root_dependency_fingerprint = review_submit_root_dependency_fingerprint(&target_row)?;
    let root_revision_checksum = match cli.review_submit_revision_checksum.clone() {
        Some(checksum) => checksum,
        None => stable_json_sha256(&target_row.json)?,
    };

    let mut baseline_config = build_config.clone();
    baseline_config.artifact_purpose = Some(REVIEW_SUBMIT_BASELINE_ARTIFACT_PURPOSE.to_owned());
    baseline_config.root_dependency_fingerprint = Some(root_dependency_fingerprint.clone());
    baseline_config.root_revision_checksum = None;
    let (baseline_source_summary, baseline_source_hash) = compute_source_fingerprint(
        pool,
        &baseline_processes,
        &baseline_config,
        None,
        Some(&method),
    )
    .await?;

    let mut overlay_config = build_config.clone();
    overlay_config.artifact_purpose = Some(REVIEW_SUBMIT_OVERLAY_ARTIFACT_PURPOSE.to_owned());
    overlay_config.root_dependency_fingerprint = Some(root_dependency_fingerprint);
    overlay_config.root_revision_checksum = Some(root_revision_checksum.clone());
    let overlay_source_hash =
        compute_review_submit_overlay_source_hash(&baseline_source_hash, &overlay_config)?;

    println!("[review_submit] mode=baseline_overlay");
    println!("[review_submit] baseline_source_fingerprint={baseline_source_hash}");
    println!("[review_submit] overlay_source_fingerprint={overlay_source_hash}");

    let mut build_timing = BuildTimingSec::default();
    let overlay_reuse_started = Instant::now();
    if let Some(reused_overlay) =
        find_reusable_snapshot(pool, &overlay_source_hash, reuse_max_age_seconds).await?
    {
        let snapshot_index_url = derive_snapshot_index_url(&reused_overlay.artifact_url);
        if store.download_object_url(&snapshot_index_url).await.is_ok() {
            build_timing.reused_snapshot = true;
            build_timing.review_submit_overlay_reused = true;
            build_timing.reuse_lookup_sec = overlay_reuse_started.elapsed().as_secs_f64();
            build_timing.total_sec = total_started.elapsed().as_secs_f64();
            write_report_files(
                &cli.report_dir,
                reused_overlay.snapshot_id,
                &overlay_config,
                &resolved_scope.scope_summary,
                &reused_overlay.coverage,
                &reused_overlay.artifact_url,
                &baseline_source_summary,
                &overlay_source_hash,
                &build_timing,
                report_policy,
            )?;
            println!(
                "[reuse] matched existing review-submit overlay snapshot={}",
                reused_overlay.snapshot_id
            );
            println!(
                "[build_timing_sec] {}",
                serde_json::to_string(&build_timing)?
            );
            println!("[resolved_snapshot_id] {}", reused_overlay.snapshot_id);
            println!("[done] snapshot ready: {}", reused_overlay.snapshot_id);
            println!("[artifact] {}", reused_overlay.artifact_url);
            println!("[snapshot_index] {snapshot_index_url}");
            println!(
                "[matrix] process_count={} flow_count={} impact_count={} a_nnz={} b_nnz={} c_nnz={}",
                reused_overlay.process_count,
                reused_overlay.flow_count,
                reused_overlay.impact_count,
                reused_overlay.a_nnz,
                reused_overlay.b_nnz,
                reused_overlay.c_nnz
            );
            return Ok(());
        }
    }
    build_timing.reuse_lookup_sec = overlay_reuse_started.elapsed().as_secs_f64();

    let baseline_started = Instant::now();
    let baseline_graph = load_or_build_review_submit_baseline(
        pool,
        store,
        cli,
        all_states,
        state_codes,
        include_user_id,
        &baseline_scope_summary,
        &baseline_processes,
        &baseline_config,
        &baseline_source_hash,
        &baseline_source_summary,
        &method,
        provider_rule,
        reference_normalization_mode,
        allocation_mode,
        reuse_max_age_seconds,
        &mut build_timing,
        report_policy,
    )
    .await?;
    build_timing.build_sparse_payload_sec += baseline_started.elapsed().as_secs_f64();

    let overlay_started = Instant::now();
    let overlay_graph = build_review_submit_overlay_graph(
        pool,
        &baseline_graph,
        &target_row,
        include_user_id,
        provider_rule,
        reference_normalization_mode,
        allocation_mode,
    )
    .await?;
    let built = assemble_sparse_payload(
        requested_snapshot_id,
        &method,
        &overlay_graph,
        cli.self_loop_cutoff,
        cli.singular_eps,
        false,
        &[],
        &[],
        false,
    )?;
    build_timing.build_sparse_payload_sec += overlay_started.elapsed().as_secs_f64();

    let artifact_url = persist_built_snapshot_artifact(
        pool,
        store,
        requested_snapshot_id,
        &cli.provider_rule,
        all_states,
        state_codes,
        include_user_id,
        &resolved_scope.scope_summary,
        &overlay_source_hash,
        &method,
        &built,
        &overlay_config,
        None,
        artifact_expires_in_seconds,
        &mut build_timing,
    )
    .await?;
    build_timing.total_sec = total_started.elapsed().as_secs_f64();

    let snapshot_index_url = derive_snapshot_index_url(&artifact_url);
    write_report_files(
        &cli.report_dir,
        requested_snapshot_id,
        &overlay_config,
        &resolved_scope.scope_summary,
        &built.coverage,
        &artifact_url,
        &baseline_source_summary,
        &overlay_source_hash,
        &build_timing,
        report_policy,
    )?;
    let readiness_path = write_matrix_readiness_report_file(
        &cli.report_dir,
        requested_snapshot_id,
        &built.readiness,
        report_policy,
    )?;

    println!(
        "[build_timing_sec] {}",
        serde_json::to_string(&build_timing)?
    );
    println!("[resolved_snapshot_id] {requested_snapshot_id}");
    println!("[done] snapshot ready: {requested_snapshot_id}");
    println!("[artifact] {artifact_url}");
    println!("[snapshot_index] {snapshot_index_url}");
    println!(
        "[matrix] process_count={} flow_count={} a_nnz={} b_nnz={} c_nnz={}",
        built.data.process_count,
        built.data.flow_count,
        built.coverage.matrix_scale.a_nnz,
        built.coverage.matrix_scale.b_nnz,
        built.coverage.matrix_scale.c_nnz
    );
    println!(
        "[coverage] unique_match={} any_match={} singular_risk={}",
        built.coverage.matching.unique_provider_match_pct,
        built.coverage.matching.any_provider_match_pct,
        built.coverage.singular_risk.risk_level
    );
    if let Some(readiness_path) = readiness_path {
        println!("[matrix_readiness_report] {}", readiness_path.display());
    } else {
        println!("[matrix_readiness_report] skipped_local_report");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn load_or_build_review_submit_baseline(
    pool: &PgPool,
    store: &ObjectStoreClient,
    cli: &Cli,
    all_states: bool,
    state_codes: &[i32],
    include_user_id: Option<Uuid>,
    scope_summary: &ResolvedRequestScopeSummary,
    baseline_processes: &[ProcessRow],
    baseline_config: &SnapshotBuildConfig,
    baseline_source_hash: &str,
    baseline_source_summary: &SourceSnapshotSummary,
    method: &MethodSelection,
    provider_rule: ProviderRule,
    reference_normalization_mode: NormalizationMode,
    allocation_mode: AllocationMode,
    reuse_max_age_seconds: Option<i64>,
    build_timing: &mut BuildTimingSec,
    report_policy: LocalSnapshotReportPolicy,
) -> anyhow::Result<CompiledGraph> {
    if let Some(reused) = find_reusable_snapshot_with_age_basis(
        pool,
        baseline_source_hash,
        reuse_max_age_seconds.or(Some(REVIEW_SUBMIT_BASELINE_TTL_SECONDS)),
        true,
    )
    .await?
    {
        match store.download_object_url(&reused.artifact_url).await {
            Ok(bytes) => {
                let decoded = decode_snapshot_artifact(&bytes)?;
                if let Some(graph) = decoded.compiled_graph {
                    touch_reused_snapshot_artifact(
                        pool,
                        reused.snapshot_id,
                        SNAPSHOT_ARTIFACT_FORMAT,
                        Some(REVIEW_SUBMIT_BASELINE_TTL_SECONDS),
                    )
                    .await?;
                    build_timing.review_submit_baseline_reused = true;
                    println!(
                        "[review_submit] baseline_snapshot_id={} reused=true",
                        reused.snapshot_id
                    );
                    return Ok(graph);
                }
                println!(
                    "[review_submit] skip baseline snapshot={} because compiled graph metadata is missing",
                    reused.snapshot_id
                );
            }
            Err(error) => {
                println!(
                    "[review_submit] skip baseline snapshot={} because artifact download failed: {}",
                    reused.snapshot_id, error
                );
            }
        }
    }

    if baseline_processes.is_empty() {
        println!("[review_submit] baseline_snapshot_id=none empty_dependency_scope=true");
        return Ok(empty_compiled_graph());
    }

    let baseline_snapshot_id = Uuid::new_v4();
    let built = build_sparse_payload(
        pool,
        baseline_snapshot_id,
        method,
        baseline_processes.to_vec(),
        include_user_id,
        None,
        provider_rule,
        reference_normalization_mode,
        allocation_mode,
        cli.self_loop_cutoff,
        cli.singular_eps,
        false,
        &[],
    )
    .await?;
    let artifact_url = persist_built_snapshot_artifact(
        pool,
        store,
        baseline_snapshot_id,
        &cli.provider_rule,
        all_states,
        state_codes,
        include_user_id,
        scope_summary,
        baseline_source_hash,
        method,
        &built,
        baseline_config,
        Some(built.compiled_graph.clone()),
        Some(REVIEW_SUBMIT_BASELINE_TTL_SECONDS),
        build_timing,
    )
    .await?;
    write_report_files(
        &cli.report_dir,
        baseline_snapshot_id,
        baseline_config,
        scope_summary,
        &built.coverage,
        &artifact_url,
        baseline_source_summary,
        baseline_source_hash,
        build_timing,
        report_policy,
    )?;
    println!("[review_submit] baseline_snapshot_id={baseline_snapshot_id} reused=false");
    Ok(built.compiled_graph)
}

#[allow(clippy::too_many_arguments)]
async fn persist_built_snapshot_artifact(
    pool: &PgPool,
    store: &ObjectStoreClient,
    snapshot_id: Uuid,
    provider_rule: &str,
    all_states: bool,
    state_codes: &[i32],
    include_user_id: Option<Uuid>,
    scope_summary: &ResolvedRequestScopeSummary,
    source_hash: &str,
    method: &MethodSelection,
    built: &BuildOutput,
    config: &SnapshotBuildConfig,
    compiled_graph: Option<CompiledGraph>,
    artifact_expires_in_seconds: Option<i64>,
    build_timing: &mut BuildTimingSec,
) -> anyhow::Result<String> {
    let encode_started = Instant::now();
    let encoded = encode_snapshot_artifact_with_graph(
        snapshot_id,
        config.clone(),
        built.coverage.clone(),
        &built.data,
        compiled_graph,
    )?;
    build_timing.encode_artifact_sec += encode_started.elapsed().as_secs_f64();

    let upload_started = Instant::now();
    let artifact_url = store
        .upload_snapshot_artifact(
            snapshot_id,
            encoded.extension,
            encoded.content_type,
            encoded.bytes,
        )
        .await?;
    build_timing.upload_artifact_sec += upload_started.elapsed().as_secs_f64();

    let snapshot_index_bytes = serde_json::to_vec(&built.snapshot_index)?;
    let upload_snapshot_index_started = Instant::now();
    store
        .upload_snapshot_index(snapshot_id, snapshot_index_bytes)
        .await?;
    build_timing.upload_snapshot_index_sec += upload_snapshot_index_started.elapsed().as_secs_f64();

    let persist_started = Instant::now();
    persist_snapshot_metadata(
        pool,
        snapshot_id,
        provider_rule,
        all_states,
        state_codes,
        include_user_id,
        None,
        scope_summary,
        source_hash,
        method,
        built,
        &artifact_url,
        &encoded.sha256,
        i64::try_from(encoded.byte_size).map_err(|_| anyhow::anyhow!("artifact too large"))?,
        encoded.format,
        config.artifact_purpose.as_deref(),
        artifact_expires_in_seconds,
    )
    .await?;
    build_timing.persist_metadata_sec += persist_started.elapsed().as_secs_f64();

    Ok(artifact_url)
}

fn empty_compiled_graph() -> CompiledGraph {
    CompiledGraph {
        processes: Vec::new(),
        flows: Vec::new(),
        provider_outputs: Vec::new(),
        provider_decisions: Vec::new(),
        technosphere_edges: Vec::new(),
        biosphere_edges: Vec::new(),
        reference_stats: CompiledReferenceStats::default(),
        allocation_stats: CompiledAllocationStats::default(),
        matching_stats: CompiledMatchingStats::default(),
    }
}

async fn build_review_submit_overlay_graph(
    pool: &PgPool,
    baseline_graph: &CompiledGraph,
    target_row: &ProcessRow,
    include_user_id: Option<Uuid>,
    provider_rule: ProviderRule,
    reference_normalization_mode: NormalizationMode,
    allocation_mode: AllocationMode,
) -> anyhow::Result<CompiledGraph> {
    let mut graph = baseline_graph.clone();
    let target_idx = i32::try_from(graph.processes.len())
        .map_err(|_| anyhow::anyhow!("overlay target index overflow"))?;
    let (target_meta, target_exchanges, target_flow_ids, target_reference, target_allocation) =
        parse_process_chunk(
            target_row,
            target_idx,
            reference_normalization_mode,
            allocation_mode,
        )?;
    let target_partition = classify_scope_partition(target_row, include_user_id);
    graph.processes.push(CompiledProcess {
        process_idx: target_meta.process_idx,
        process_id: target_meta.process_id,
        process_version: target_meta.process_version.clone(),
        process_name: target_meta.process_name.clone(),
        model_id: target_meta.model_id,
        location: target_meta.location.clone(),
        reference_year: target_meta.reference_year,
        annual_supply_or_production_volume: target_meta.annual_supply_or_production_volume,
        partition: target_partition,
    });
    graph.reference_stats.missing_reference += target_reference.missing_reference;
    graph.reference_stats.invalid_reference += target_reference.invalid_reference;
    graph.reference_stats.normalized_processes += target_reference.normalized_processes;
    graph.allocation_stats.exchange_total += target_allocation.exchange_total;
    graph.allocation_stats.fraction_present_count += target_allocation.fraction_present_count;
    graph.allocation_stats.fraction_missing_count += target_allocation.fraction_missing_count;
    graph.allocation_stats.fraction_invalid_count += target_allocation.fraction_invalid_count;

    let mut flow_idx_by_id = graph
        .flows
        .iter()
        .map(|flow| (flow.flow_id, flow.flow_idx))
        .collect::<HashMap<_, _>>();
    let missing_flow_ids = target_flow_ids
        .iter()
        .copied()
        .filter(|flow_id| !flow_idx_by_id.contains_key(flow_id))
        .collect::<BTreeSet<_>>();
    let flow_meta = fetch_flow_meta(pool, &missing_flow_ids, None).await?;
    for flow_id in missing_flow_ids {
        let flow_idx =
            i32::try_from(graph.flows.len()).map_err(|_| anyhow::anyhow!("flow idx overflow"))?;
        let kind = if flow_meta
            .get(&flow_id)
            .is_some_and(|meta| classify_flow_kind(&meta.json) == "elementary")
        {
            CompiledFlowKind::Elementary
        } else {
            CompiledFlowKind::Product
        };
        graph.flows.push(CompiledFlow {
            flow_idx,
            flow_id,
            kind,
        });
        flow_idx_by_id.insert(flow_id, flow_idx);
    }
    let elementary_flow_idx = graph
        .flows
        .iter()
        .filter_map(|flow| (flow.kind == CompiledFlowKind::Elementary).then_some(flow.flow_idx))
        .collect::<HashSet<_>>();

    for ex in &target_exchanges {
        if ex.direction == Some(ExchangeDirection::Output) {
            graph.provider_outputs.push(CompiledProviderOutput {
                flow_id: ex.flow_id,
                provider_idx: target_idx,
                output_exchange_internal_id: ex.internal_id.clone(),
                output_exchange_is_reference: ex.is_reference_exchange,
                output_normalized_amount: ex.amount,
                output_allocation_state: compiled_allocation_state(ex.allocation_state),
                eligibility: if ex.is_reference_exchange {
                    CompiledProviderCandidateEligibility::AcceptedReferenceOutput
                } else {
                    CompiledProviderCandidateEligibility::RejectedNonReferenceOutput
                },
            });
        }
    }

    let mut process_meta = graph
        .processes
        .iter()
        .map(process_meta_from_compiled)
        .collect::<Vec<_>>();
    process_meta.sort_by_key(|meta| meta.process_idx);
    let mut provider_map: ProviderMap = HashMap::new();
    for output in &graph.provider_outputs {
        provider_map
            .entry(output.flow_id)
            .or_default()
            .push(provider_output_candidate_from_compiled_output(output));
    }
    for providers in provider_map.values_mut() {
        sort_provider_output_candidates(providers, &process_meta);
    }
    graph.provider_outputs = provider_outputs_from_map(&provider_map);

    let target_process = compiled_process_for_idx(&graph.processes, target_idx)
        .ok_or_else(|| anyhow::anyhow!("missing overlay target process"))?
        .clone();
    for ex in &target_exchanges {
        if let Some(flow_idx) = flow_idx_by_id.get(&ex.flow_id).copied()
            && elementary_flow_idx.contains(&flow_idx)
            && let Some(amount) = ex.amount
        {
            let value = biosphere_gross_value(amount);
            if value.abs() > f64::EPSILON {
                graph.biosphere_edges.push(CompiledBiosphereEdge {
                    process_idx: target_idx,
                    flow_idx,
                    amount: value,
                    process_partition: target_partition,
                });
            }
        }

        if ex.direction != Some(ExchangeDirection::Input) {
            continue;
        }
        let Some(amount) = ex.amount else {
            continue;
        };
        let supply_region_anchor = supply_region_anchor_for_exchange(ex, &process_meta)?;
        graph.matching_stats.input_edges_total += 1;
        let provider_outputs = provider_map.get(&ex.flow_id);
        let eligible_providers = eligible_provider_indices(provider_outputs);
        let provider_candidates = provider_candidates_for_outputs(provider_outputs, &process_meta)?;
        let candidate_provider_count =
            i32::try_from(eligible_providers.len()).map_err(|_| anyhow::anyhow!("providers"))?;
        if candidate_provider_count == 1 {
            graph.matching_stats.matched_unique_provider += 1;
            graph.matching_stats.a_input_edges_written += 1;
            let provider_idx = *eligible_providers
                .first()
                .ok_or_else(|| anyhow::anyhow!("missing provider idx"))?;
            let provider_meta =
                process_meta_for_idx(&process_meta, provider_idx).ok_or_else(|| {
                    anyhow::anyhow!("missing provider process meta idx={provider_idx}")
                })?;
            graph.provider_decisions.push(CompiledProviderDecision {
                consumer_idx: target_idx,
                flow_id: ex.flow_id,
                candidate_provider_count,
                matched_provider_count: 1,
                candidates: provider_candidates,
                decision_kind: Some(CompiledProviderDecisionKind::UniqueProvider),
                resolution_strategy: Some(CompiledProviderResolutionStrategy::UniqueProvider),
                failure_reason: None,
                used_equal_fallback: false,
                volume_fallback_to_one_count: 0,
                geography_tier: Some(provider_geography_tier(
                    supply_region_anchor.location.as_deref(),
                    provider_meta.location.as_deref(),
                )),
                supply_region_source: Some(supply_region_anchor.source),
                supply_region_location: supply_region_anchor.location.clone(),
                exchange_location_present: ex.location.is_some(),
                allocations: vec![CompiledProviderAllocation {
                    provider_idx,
                    weight: 1.0,
                }],
            });
            let provider = compiled_process_for_idx(&graph.processes, provider_idx)
                .ok_or_else(|| anyhow::anyhow!("missing compiled provider idx={provider_idx}"))?;
            graph.technosphere_edges.push(CompiledTechnosphereEdge {
                provider_idx,
                consumer_idx: target_idx,
                flow_id: ex.flow_id,
                amount,
                provider_partition: provider.partition,
                consumer_partition: target_process.partition,
                partition: CompiledEdgePartition::from_partitions(
                    provider.partition,
                    target_process.partition,
                ),
            });
        } else if candidate_provider_count > 1 {
            graph.matching_stats.matched_multi_provider += 1;
            match resolve_multi_provider(provider_rule, ex, &eligible_providers, &process_meta)? {
                MultiProviderDecision::Resolved(mut resolution) => {
                    graph.matching_stats.matched_multi_resolved += 1;
                    if resolution.used_equal_fallback {
                        graph.matching_stats.matched_multi_fallback_equal += 1;
                    }
                    graph.matching_stats.a_input_edges_written += 1;
                    if resolution.geography_tier.is_none() {
                        resolution.geography_tier = best_geography_tier_for_allocations(
                            supply_region_anchor.location.as_deref(),
                            &resolution.allocations,
                            &process_meta,
                        )?;
                    }
                    let allocations = resolution
                        .allocations
                        .into_iter()
                        .map(|(provider_idx, weight)| CompiledProviderAllocation {
                            provider_idx,
                            weight,
                        })
                        .collect::<Vec<_>>();
                    graph.provider_decisions.push(CompiledProviderDecision {
                        consumer_idx: target_idx,
                        flow_id: ex.flow_id,
                        candidate_provider_count,
                        matched_provider_count: i32::try_from(allocations.len())
                            .map_err(|_| anyhow::anyhow!("provider allocation overflow"))?,
                        candidates: provider_candidates,
                        decision_kind: Some(CompiledProviderDecisionKind::MultiResolved),
                        resolution_strategy: Some(resolution.resolution_strategy),
                        failure_reason: None,
                        used_equal_fallback: resolution.used_equal_fallback,
                        volume_fallback_to_one_count: resolution.volume_fallback_to_one_count,
                        geography_tier: resolution.geography_tier,
                        supply_region_source: Some(supply_region_anchor.source),
                        supply_region_location: supply_region_anchor.location.clone(),
                        exchange_location_present: ex.location.is_some(),
                        allocations: allocations.clone(),
                    });
                    for allocation in allocations {
                        let weighted = amount * allocation.weight;
                        if weighted.abs() <= f64::EPSILON {
                            continue;
                        }
                        let provider =
                            compiled_process_for_idx(&graph.processes, allocation.provider_idx)
                                .ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "missing compiled provider idx={}",
                                        allocation.provider_idx
                                    )
                                })?;
                        graph.technosphere_edges.push(CompiledTechnosphereEdge {
                            provider_idx: allocation.provider_idx,
                            consumer_idx: target_idx,
                            flow_id: ex.flow_id,
                            amount: weighted,
                            provider_partition: provider.partition,
                            consumer_partition: target_process.partition,
                            partition: CompiledEdgePartition::from_partitions(
                                provider.partition,
                                target_process.partition,
                            ),
                        });
                    }
                }
                MultiProviderDecision::Unresolved(failure_reason) => {
                    graph.matching_stats.matched_multi_unresolved += 1;
                    graph.provider_decisions.push(CompiledProviderDecision {
                        consumer_idx: target_idx,
                        flow_id: ex.flow_id,
                        candidate_provider_count,
                        matched_provider_count: 0,
                        candidates: provider_candidates,
                        decision_kind: Some(CompiledProviderDecisionKind::MultiUnresolved),
                        resolution_strategy: None,
                        failure_reason: Some(failure_reason),
                        used_equal_fallback: false,
                        volume_fallback_to_one_count: 0,
                        geography_tier: None,
                        supply_region_source: Some(supply_region_anchor.source),
                        supply_region_location: supply_region_anchor.location.clone(),
                        exchange_location_present: ex.location.is_some(),
                        allocations: Vec::new(),
                    });
                }
            }
        } else {
            graph.matching_stats.unmatched_no_provider += 1;
            graph.provider_decisions.push(CompiledProviderDecision {
                consumer_idx: target_idx,
                flow_id: ex.flow_id,
                candidate_provider_count: 0,
                matched_provider_count: 0,
                candidates: Vec::new(),
                decision_kind: Some(CompiledProviderDecisionKind::NoProvider),
                resolution_strategy: None,
                failure_reason: Some(CompiledProviderFailureReason::NoProviderCandidates),
                used_equal_fallback: false,
                volume_fallback_to_one_count: 0,
                geography_tier: None,
                supply_region_source: Some(supply_region_anchor.source),
                supply_region_location: supply_region_anchor.location,
                exchange_location_present: ex.location.is_some(),
                allocations: Vec::new(),
            });
        }
    }

    Ok(graph)
}

fn process_meta_from_compiled(process: &CompiledProcess) -> ProcessMeta {
    ProcessMeta {
        process_idx: process.process_idx,
        process_id: process.process_id,
        process_version: process.process_version.clone(),
        process_name: process.process_name.clone(),
        model_id: process.model_id,
        location: process.location.clone(),
        reference_year: process.reference_year,
        annual_supply_or_production_volume: process.annual_supply_or_production_volume,
    }
}

fn review_submit_baseline_scope_summary(
    scope_summary: &ResolvedRequestScopeSummary,
    root: &RequestRootProcess,
) -> anyhow::Result<ResolvedRequestScopeSummary> {
    let processes = scope_summary
        .processes
        .iter()
        .filter(|process| {
            !(process.process_id == root.process_id
                && process.process_version.trim() == root.process_version)
        })
        .cloned()
        .collect::<Vec<_>>();
    let public_process_count = i64::try_from(
        processes
            .iter()
            .filter(|row| row.partition == ScopeProcessPartition::Public)
            .count(),
    )
    .map_err(|_| anyhow::anyhow!("public process count overflow"))?;
    let private_process_count = i64::try_from(
        processes
            .iter()
            .filter(|row| row.partition == ScopeProcessPartition::Private)
            .count(),
    )
    .map_err(|_| anyhow::anyhow!("private process count overflow"))?;

    Ok(ResolvedRequestScopeSummary {
        selection_mode: scope_summary.selection_mode,
        scope_hash: scope_summary.scope_hash.clone(),
        roots: scope_summary.roots.clone(),
        public_process_count,
        private_process_count,
        processes,
    })
}

fn review_submit_root_dependency_fingerprint(process: &ProcessRow) -> anyhow::Result<String> {
    let mut exchanges = process_exchange_items(&process.json)
        .into_iter()
        .filter_map(|exchange| {
            let direction = exchange
                .get("exchangeDirection")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())?;
            let flow_id = parse_uuid_at(exchange, &["referenceToFlowDataSet", "@refObjectId"])?;
            Some(serde_json::json!({
                "direction": direction,
                "flow_id": flow_id,
                "location": parse_exchange_location(exchange)
            }))
        })
        .collect::<Vec<_>>();
    exchanges.sort_by_key(std::string::ToString::to_string);
    let body = serde_json::json!({
        "schema": "review-submit-root-dependency-surface:v1",
        "process_id": process.id,
        "process_version": process.version,
        "model_id": process.model_id,
        "location": parse_process_location(&process.json),
        "exchanges": exchanges
    });
    stable_json_sha256(&body)
}

fn compute_review_submit_overlay_source_hash(
    baseline_source_hash: &str,
    config: &SnapshotBuildConfig,
) -> anyhow::Result<String> {
    let body = serde_json::json!({
        "schema": "review-submit-overlay-source:v1",
        "baseline_source_hash": baseline_source_hash,
        "config": config,
    });
    stable_json_sha256(&body)
}

fn stable_json_sha256(value: &Value) -> anyhow::Result<String> {
    let sorted = sorted_json(value);
    let bytes = serde_json::to_vec(&sorted)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(hex::encode(hasher.finalize()))
}

fn sorted_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(sorted_json).collect()),
        Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), sorted_json(value)))
                    .collect::<Map<_, _>>(),
            )
        }
        _ => value.clone(),
    }
}

async fn resolve_method_identity(
    pool: &PgPool,
    cli: &Cli,
    versioned_scope: Option<&ValidatedPublicOwnerDraftScope>,
) -> anyhow::Result<MethodSelection> {
    if cli.no_lcia {
        return Ok(MethodSelection {
            has_lcia: false,
            method_id: None,
            method_version: None,
            method_count: 0,
            factor_count: 0,
            source_evidence: None,
            rows: Vec::new(),
            static_bundle: None,
        });
    }

    if cli.method_id.is_some() && cli.method_version.is_none() {
        return Err(anyhow::anyhow!(
            "--method-version is required when --method-id is set"
        ));
    }
    if cli.method_id.is_none() && cli.method_version.is_some() {
        return Err(anyhow::anyhow!(
            "--method-version requires --method-id; omit both to use all methods"
        ));
    }

    if let Some(scope) = versioned_scope {
        if cli.method_id.is_some() || cli.method_version.is_some() {
            return Err(anyhow::anyhow!(
                "versioned static-cache builds must use the complete manifest method set"
            ));
        }
        let source = TrustedStaticCacheSource::new(
            cli.lcia_static_cache_dir.clone(),
            cli.lcia_static_cache_base_url.clone(),
        )?;
        let bundle =
            load_verified_static_lcia_bundle(&source, &scope.lcia_method_factor_source).await?;
        let method_count = i64::try_from(bundle.methods.len())
            .map_err(|_| anyhow::anyhow!("lciamethod count overflow"))?;
        let factor_count = bundle
            .factors_by_method
            .values()
            .try_fold(0_i64, |total, factors| {
                total.checked_add(i64::try_from(factors.len()).ok()?)
            })
            .ok_or_else(|| anyhow::anyhow!("lciamethod factor count overflow"))?;
        return Ok(MethodSelection {
            has_lcia: true,
            method_id: None,
            method_version: None,
            method_count,
            factor_count,
            source_evidence: Some(bundle.source_evidence.clone()),
            rows: Vec::new(),
            static_bundle: Some(bundle),
        });
    }

    let rows = fetch_selected_method_rows(pool, cli).await?;
    if rows.is_empty() {
        return Err(anyhow::anyhow!("no lciamethods found"));
    }
    if cli.method_id.is_some() && rows.len() != 1 {
        return Err(anyhow::anyhow!(
            "specific lciamethod selection resolved {} rows; expected exactly one",
            rows.len()
        ));
    }
    let method_count =
        i64::try_from(rows.len()).map_err(|_| anyhow::anyhow!("lciamethod count overflow"))?;
    let factor_count = rows.iter().try_fold(0_i64, |total, row| {
        let count = i64::try_from(method_factor_items(&row.json).len())
            .map_err(|_| anyhow::anyhow!("lciamethod factor count overflow"))?;
        total
            .checked_add(count)
            .ok_or_else(|| anyhow::anyhow!("lciamethod factor count overflow"))
    })?;
    let source_evidence = None;

    Ok(MethodSelection {
        has_lcia: true,
        method_id: cli.method_id,
        method_version: cli.method_version.clone(),
        method_count,
        factor_count,
        source_evidence,
        rows,
        static_bundle: None,
    })
}

async fn fetch_selected_method_rows(pool: &PgPool, cli: &Cli) -> anyhow::Result<Vec<MethodRow>> {
    let rows = match cli.method_id {
        Some(method_id) => {
            sqlx::query(
                r#"
                SELECT id, version, json
                FROM public.lciamethods
                WHERE id = $1 AND version = $2::bpchar
                "#,
            )
            .bind(method_id)
            .bind(cli.method_version.clone().unwrap_or_default())
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query(
                r#"
                SELECT DISTINCT ON (id)
                  id, version, json
                FROM public.lciamethods
                ORDER BY id, state_code DESC, modified_at DESC NULLS LAST, created_at DESC NULLS LAST
                "#,
            )
            .fetch_all(pool)
            .await?
        }
    };

    rows.into_iter()
        .map(|row| {
            Ok(MethodRow {
                id: row.try_get("id")?,
                version: row.try_get::<String, _>("version")?.trim().to_owned(),
                json: row.try_get("json")?,
            })
        })
        .collect()
}

fn parse_lang_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        }
        Value::Object(map) => {
            if let Some(text) = map.get("#text").and_then(Value::as_str) {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_owned());
                }
            }
            None
        }
        Value::Array(arr) => {
            let preferred = arr.iter().find(|entry| {
                entry
                    .get("@xml:lang")
                    .and_then(Value::as_str)
                    .is_some_and(|lang| lang.eq_ignore_ascii_case("en"))
            });
            if let Some(entry) = preferred
                && let Some(text) = parse_lang_text(entry)
            {
                return Some(text);
            }
            arr.iter().find_map(parse_lang_text)
        }
        _ => None,
    }
}

fn parse_string_path(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    parse_lang_text(current)
}

fn parse_lcia_method_name(method_json: &Value) -> Option<String> {
    parse_string_path(
        method_json,
        &[
            "LCIAMethodDataSet",
            "methodInformation",
            "dataSetInformation",
            "name",
            "baseName",
        ],
    )
    .or_else(|| {
        parse_string_path(
            method_json,
            &[
                "LCIAMethodDataSet",
                "methodInfo",
                "dataSetInfo",
                "name",
                "baseName",
            ],
        )
    })
}

fn parse_lcia_method_unit(method_json: &Value) -> Option<String> {
    parse_string_path(
        method_json,
        &[
            "LCIAMethodDataSet",
            "methodInformation",
            "quantitativeReference",
            "referenceToReferenceUnitGroup",
            "common:shortDescription",
        ],
    )
    .or_else(|| {
        parse_string_path(
            method_json,
            &[
                "LCIAMethodDataSet",
                "methodInfo",
                "quantitativeReference",
                "referenceToReferenceUnitGroup",
                "common:shortDescription",
            ],
        )
    })
}

fn load_impact_factor_sets(method: &MethodSelection) -> anyhow::Result<Vec<ImpactFactorSet>> {
    if !method.has_lcia {
        return Ok(Vec::new());
    }
    if let Some(bundle) = &method.static_bundle {
        let mut out = Vec::with_capacity(bundle.methods.len());
        for source_method in &bundle.methods {
            let mut factor_map = HashMap::new();
            let mut directional_factor_map = HashMap::new();
            for factor in bundle
                .factors_by_method
                .get(&source_method.method_id)
                .into_iter()
                .flatten()
            {
                let direction = match factor.direction {
                    StaticLciaDirection::Input => ExchangeDirection::Input,
                    StaticLciaDirection::Output => ExchangeDirection::Output,
                };
                accumulate_finite_factor(
                    &mut directional_factor_map,
                    (factor.flow_id, direction),
                    factor.value,
                    source_method.method_id,
                )?;
                if factor.value != 0.0 {
                    accumulate_finite_factor(
                        &mut factor_map,
                        factor.flow_id,
                        factor.value,
                        source_method.method_id,
                    )?;
                }
            }
            factor_map.retain(|_, value| *value != 0.0);
            out.push(ImpactFactorSet {
                impact_id: source_method.method_id,
                method_version: source_method.method_version.clone(),
                artifact_locator_id: source_method.artifact_locator_id,
                impact_key: format!("method:{}", source_method.method_id),
                impact_name: source_method.name.clone(),
                unit: source_method.unit.clone(),
                factors_by_flow: factor_map,
                factors_by_flow_direction: directional_factor_map,
            });
        }
        out.sort_unstable_by_key(|impact| impact.impact_id);
        return Ok(out);
    }
    if method.rows.is_empty() {
        return Err(anyhow::anyhow!("no lciamethods found for selected scope"));
    }

    let mut out = Vec::with_capacity(method.rows.len());
    for row in &method.rows {
        let mut factor_map: HashMap<Uuid, f64> = HashMap::new();
        let mut directional_factor_map: HashMap<(Uuid, ExchangeDirection), f64> = HashMap::new();
        for factor in method_factor_items(&row.json) {
            let Some(flow_id) = parse_uuid_at(factor, &["referenceToFlowDataSet", "@refObjectId"])
            else {
                continue;
            };
            let Some(value) = parse_number(
                factor
                    .get("meanValue")
                    .or_else(|| factor.get("meanAmount"))
                    .or_else(|| factor.get("resultingAmount")),
            ) else {
                continue;
            };
            if let Some(direction) =
                parse_exchange_direction(factor.get("exchangeDirection").and_then(Value::as_str))
            {
                accumulate_finite_factor(
                    &mut directional_factor_map,
                    (flow_id, direction),
                    value,
                    row.id,
                )?;
            }
            if value.abs() <= f64::EPSILON {
                continue;
            }
            accumulate_finite_factor(&mut factor_map, flow_id, value, row.id)?;
        }
        factor_map.retain(|_, value| value.abs() > f64::EPSILON);

        out.push(ImpactFactorSet {
            impact_id: row.id,
            method_version: row.version.clone(),
            artifact_locator_id: row.id,
            impact_key: format!("method:{}", row.id),
            impact_name: parse_lcia_method_name(&row.json)
                .unwrap_or_else(|| format!("LCIA Method {}", row.id)),
            unit: parse_lcia_method_unit(&row.json).unwrap_or_else(|| "unknown".to_owned()),
            factors_by_flow: factor_map,
            factors_by_flow_direction: directional_factor_map,
        });
    }
    out.sort_unstable_by_key(|impact| impact.impact_id);
    Ok(out)
}

fn accumulate_finite_factor<K>(
    factors: &mut HashMap<K, f64>,
    key: K,
    value: f64,
    method_id: Uuid,
) -> anyhow::Result<()>
where
    K: Eq + std::hash::Hash,
{
    if !value.is_finite() {
        return Err(anyhow::anyhow!(
            "LCIA factor for method {method_id} is non-finite"
        ));
    }
    let total = factors.get(&key).copied().unwrap_or_default() + value;
    if !total.is_finite() {
        return Err(anyhow::anyhow!(
            "LCIA factor aggregation overflow for method {method_id}"
        ));
    }
    factors.insert(key, total);
    Ok(())
}

fn retain_sparse_value(value: f64, preserve_sub_epsilon_values: bool) -> bool {
    if !value.is_finite() {
        false
    } else if preserve_sub_epsilon_values {
        value != 0.0
    } else {
        value.abs() > f64::EPSILON
    }
}

fn accumulate_biosphere_edge(
    b_map: &mut HashMap<(i32, i32), f64>,
    key: (i32, i32),
    amount: f64,
    preserve_sub_epsilon_values: bool,
) -> anyhow::Result<()> {
    if !retain_sparse_value(amount, preserve_sub_epsilon_values) {
        return Ok(());
    }
    let total = b_map.get(&key).copied().unwrap_or_default() + amount;
    if !total.is_finite() {
        return Err(anyhow::anyhow!(
            "biosphere edge aggregation overflow for flow_idx={} process_idx={}",
            key.0,
            key.1
        ));
    }
    b_map.insert(key, total);
    Ok(())
}

fn add_technosphere_edge(
    a_map: &mut HashMap<(i32, i32), f64>,
    provider_idx: i32,
    consumer_idx: i32,
    amount: f64,
) {
    if amount.abs() > f64::EPSILON {
        *a_map.entry((provider_idx, consumer_idx)).or_insert(0.0) += amount;
    }
}

fn provider_outputs_from_map(provider_map: &ProviderMap) -> Vec<CompiledProviderOutput> {
    let mut outputs = provider_map
        .values()
        .flat_map(|providers| {
            providers
                .iter()
                .map(|provider| CompiledProviderOutput {
                    flow_id: provider.flow_id,
                    provider_idx: provider.provider_idx,
                    output_exchange_internal_id: provider.output_exchange_internal_id.clone(),
                    output_exchange_is_reference: provider.output_exchange_is_reference,
                    output_normalized_amount: provider.output_normalized_amount,
                    output_allocation_state: compiled_allocation_state(
                        provider.output_allocation_state,
                    ),
                    eligibility: provider_candidate_eligibility(provider),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    outputs.sort_unstable_by_key(|output| {
        (
            output.flow_id,
            output.provider_idx,
            output.output_exchange_internal_id.clone(),
        )
    });
    outputs
}

fn provider_output_candidate_from_exchange(
    provider_idx: i32,
    flow_id: Uuid,
    exchange: &ParsedExchange,
) -> ProviderOutputCandidate {
    ProviderOutputCandidate {
        flow_id,
        provider_idx,
        output_exchange_internal_id: exchange.internal_id.clone(),
        output_exchange_is_reference: exchange.is_reference_exchange,
        output_normalized_amount: exchange.amount,
        output_allocation_state: exchange.allocation_state,
    }
}

fn provider_output_candidate_from_compiled_output(
    output: &CompiledProviderOutput,
) -> ProviderOutputCandidate {
    ProviderOutputCandidate {
        flow_id: output.flow_id,
        provider_idx: output.provider_idx,
        output_exchange_internal_id: output.output_exchange_internal_id.clone(),
        output_exchange_is_reference: output.output_exchange_is_reference
            || output.eligibility == CompiledProviderCandidateEligibility::AcceptedReferenceOutput,
        output_normalized_amount: output.output_normalized_amount,
        output_allocation_state: match output.output_allocation_state {
            CompiledProviderOutputAllocationState::Present => AllocationFractionState::Present,
            CompiledProviderOutputAllocationState::Missing
            | CompiledProviderOutputAllocationState::Unknown => AllocationFractionState::Missing,
            CompiledProviderOutputAllocationState::Invalid => AllocationFractionState::Invalid,
        },
    }
}

fn sort_provider_output_candidates(
    providers: &mut [ProviderOutputCandidate],
    process_meta: &[ProcessMeta],
) {
    providers.sort_by_key(|candidate| {
        (
            process_meta_for_idx(process_meta, candidate.provider_idx)
                .map_or(Uuid::nil(), |meta| meta.process_id),
            !candidate.output_exchange_is_reference,
            candidate.output_exchange_internal_id.clone(),
        )
    });
}

fn eligible_provider_indices(outputs: Option<&Vec<ProviderOutputCandidate>>) -> Vec<i32> {
    let Some(outputs) = outputs else {
        return Vec::new();
    };
    let mut providers = outputs
        .iter()
        .filter_map(|candidate| {
            candidate
                .output_exchange_is_reference
                .then_some(candidate.provider_idx)
        })
        .collect::<Vec<_>>();
    providers.dedup();
    providers
}

fn provider_candidate_eligibility(
    candidate: &ProviderOutputCandidate,
) -> CompiledProviderCandidateEligibility {
    if candidate.output_exchange_is_reference {
        CompiledProviderCandidateEligibility::AcceptedReferenceOutput
    } else {
        CompiledProviderCandidateEligibility::RejectedNonReferenceOutput
    }
}

fn compiled_allocation_state(
    state: AllocationFractionState,
) -> CompiledProviderOutputAllocationState {
    match state {
        AllocationFractionState::Present => CompiledProviderOutputAllocationState::Present,
        AllocationFractionState::Missing => CompiledProviderOutputAllocationState::Missing,
        AllocationFractionState::Invalid => CompiledProviderOutputAllocationState::Invalid,
    }
}

fn no_provider_failure_reason(
    outputs: Option<&Vec<ProviderOutputCandidate>>,
) -> CompiledProviderFailureReason {
    if outputs.is_some_and(|candidates| !candidates.is_empty()) {
        CompiledProviderFailureReason::RejectedNonReferenceOnly
    } else {
        CompiledProviderFailureReason::NoProviderCandidates
    }
}

async fn build_sparse_payload(
    pool: &PgPool,
    snapshot_id: Uuid,
    method: &MethodSelection,
    processes: Vec<ProcessRow>,
    include_user_id: Option<Uuid>,
    versioned_scope: Option<&ValidatedPublicOwnerDraftScope>,
    provider_rule: ProviderRule,
    reference_normalization_mode: NormalizationMode,
    allocation_mode: AllocationMode,
    self_loop_cutoff: f64,
    singular_eps: f64,
    has_lcia: bool,
    impact_factor_sets: &[ImpactFactorSet],
) -> anyhow::Result<BuildOutput> {
    if processes.is_empty() {
        return Err(anyhow::anyhow!("no processes matched filter"));
    }
    if has_lcia && impact_factor_sets.is_empty() {
        return Err(anyhow::anyhow!(
            "LCIA is enabled but no lciamethod factors were loaded"
        ));
    }
    let compiled_graph = compile_scope_graph(
        pool,
        processes,
        include_user_id,
        versioned_scope,
        provider_rule,
        reference_normalization_mode,
        allocation_mode,
        impact_factor_sets,
    )
    .await?;

    assemble_sparse_payload(
        snapshot_id,
        method,
        &compiled_graph.graph,
        self_loop_cutoff,
        singular_eps,
        has_lcia,
        impact_factor_sets,
        &compiled_graph.lcia_exchange_observations,
        versioned_scope.is_some(),
    )
}

fn parse_process_chunk(
    proc_row: &ProcessRow,
    process_idx: i32,
    reference_normalization_mode: NormalizationMode,
    allocation_mode: AllocationMode,
) -> anyhow::Result<ParsedProcessChunk> {
    let process_meta = ProcessMeta {
        process_idx,
        process_id: proc_row.id,
        process_version: proc_row.version.clone(),
        process_name: parse_process_name(&proc_row.json),
        model_id: proc_row.model_id,
        location: parse_process_location(&proc_row.json),
        reference_year: parse_process_reference_year(&proc_row.json),
        annual_supply_or_production_volume: parse_process_annual_supply_or_production_volume(
            &proc_row.json,
        ),
    };
    let mut local_exchanges = Vec::new();
    let mut local_flow_ids = BTreeSet::new();
    let mut local_allocation = AllocationParseStats::default();
    let exchange_items = process_exchange_items(&proc_row.json);
    let reference_internal_id = parse_reference_internal_id(&proc_row.json);
    let (reference_scale, local_reference) = resolve_reference_normalization(
        proc_row.id,
        &proc_row.json,
        &exchange_items,
        reference_normalization_mode,
    )?;

    for (exchange_index, ex) in exchange_items.iter().enumerate() {
        let direction_label = ex
            .get("exchangeDirection")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("unknown")
            .to_owned();
        let direction = parse_exchange_direction(Some(&direction_label));
        let Some(flow_id) = parse_uuid_at(ex, &["referenceToFlowDataSet", "@refObjectId"]) else {
            continue;
        };
        let internal_id = parse_exchange_internal_id(ex);
        let exchange_id = format!(
            "{}:{}:{}",
            proc_row.id,
            proc_row.version,
            internal_id
                .as_deref()
                .map_or_else(|| exchange_index.to_string(), ToOwned::to_owned)
        );
        let flow_version = ex
            .get("referenceToFlowDataSet")
            .and_then(|value| value.get("@version"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("unknown")
            .to_owned();
        local_allocation.exchange_total += 1;
        let (allocation_fraction, allocation_state) =
            resolve_allocation_fraction(ex, allocation_mode)?;
        match allocation_state {
            AllocationFractionState::Present => {
                local_allocation.fraction_present_count += 1;
            }
            AllocationFractionState::Missing => {
                local_allocation.fraction_missing_count += 1;
            }
            AllocationFractionState::Invalid => {
                local_allocation.fraction_invalid_count += 1;
            }
        }
        let amount = parse_number(
            ex.get("meanAmount")
                .or_else(|| ex.get("resultingAmount"))
                .or_else(|| ex.get("meanValue")),
        )
        .map(|raw| raw * reference_scale * allocation_fraction)
        .filter(|normalized| normalized.is_finite());

        local_exchanges.push(ParsedExchange {
            process_idx,
            flow_id,
            direction,
            direction_label,
            internal_id: internal_id.clone(),
            exchange_id,
            flow_version,
            is_reference_exchange: is_reference_internal_exchange(
                internal_id.as_deref(),
                reference_internal_id.as_deref(),
            ),
            amount,
            allocation_state,
            location: parse_exchange_location(ex),
        });
        local_flow_ids.insert(flow_id);
    }

    Ok((
        process_meta,
        local_exchanges,
        local_flow_ids,
        local_reference,
        local_allocation,
    ))
}

async fn compile_scope_graph(
    pool: &PgPool,
    processes: Vec<ProcessRow>,
    include_user_id: Option<Uuid>,
    versioned_scope: Option<&ValidatedPublicOwnerDraftScope>,
    provider_rule: ProviderRule,
    reference_normalization_mode: NormalizationMode,
    allocation_mode: AllocationMode,
    impact_factor_sets: &[ImpactFactorSet],
) -> anyhow::Result<CompiledScopeGraph> {
    let process_count_i32 =
        i32::try_from(processes.len()).map_err(|_| anyhow::anyhow!("process overflow"))?;
    let chunks = processes
        .par_iter()
        .enumerate()
        .map(|(idx, proc_row)| {
            let process_idx =
                i32::try_from(idx).map_err(|_| anyhow::anyhow!("process index overflow"))?;
            parse_process_chunk(
                proc_row,
                process_idx,
                reference_normalization_mode,
                allocation_mode,
            )
        })
        .collect::<Vec<_>>();

    let mut exchanges = Vec::<ParsedExchange>::new();
    let mut process_meta_by_idx = HashMap::<i32, ProcessMeta>::with_capacity(processes.len());
    let mut flow_candidates: BTreeSet<Uuid> = BTreeSet::new();
    let mut reference_stats = ReferenceParseStats::default();
    let mut allocation_stats = AllocationParseStats::default();
    for chunk in chunks {
        let (meta, chunk_exchanges, chunk_flow_ids, chunk_reference, chunk_allocation) = chunk?;
        process_meta_by_idx.insert(meta.process_idx, meta);
        exchanges.extend(chunk_exchanges);
        flow_candidates.extend(chunk_flow_ids);
        reference_stats.missing_reference += chunk_reference.missing_reference;
        reference_stats.invalid_reference += chunk_reference.invalid_reference;
        reference_stats.normalized_processes += chunk_reference.normalized_processes;
        allocation_stats.exchange_total += chunk_allocation.exchange_total;
        allocation_stats.fraction_present_count += chunk_allocation.fraction_present_count;
        allocation_stats.fraction_missing_count += chunk_allocation.fraction_missing_count;
        allocation_stats.fraction_invalid_count += chunk_allocation.fraction_invalid_count;
    }

    let mut process_meta = Vec::with_capacity(processes.len());
    for idx in 0..process_count_i32 {
        process_meta.push(
            process_meta_by_idx
                .remove(&idx)
                .ok_or_else(|| anyhow::anyhow!("missing process meta for idx={idx}"))?,
        );
    }

    let mut compiled_processes = Vec::with_capacity(process_meta.len());
    for meta in &process_meta {
        let row = processes
            .get(usize::try_from(meta.process_idx).map_err(|_| anyhow::anyhow!("negative idx"))?)
            .ok_or_else(|| anyhow::anyhow!("missing process row for idx={}", meta.process_idx))?;
        compiled_processes.push(CompiledProcess {
            process_idx: meta.process_idx,
            process_id: meta.process_id,
            process_version: meta.process_version.clone(),
            process_name: meta.process_name.clone(),
            model_id: meta.model_id,
            location: meta.location.clone(),
            reference_year: meta.reference_year,
            annual_supply_or_production_volume: meta.annual_supply_or_production_volume,
            partition: classify_scope_partition(row, include_user_id),
        });
    }

    let exchange_flow_candidates = flow_candidates.clone();
    for impact in impact_factor_sets {
        for flow_id in impact.factors_by_flow.keys() {
            flow_candidates.insert(*flow_id);
        }
    }

    let flow_meta = fetch_flow_meta(pool, &flow_candidates, versioned_scope).await?;
    if versioned_scope.is_some() {
        for flow_id in &exchange_flow_candidates {
            if !flow_meta.contains_key(flow_id) {
                return Err(anyhow::anyhow!(
                    "process exchange references flow outside exact visibility scope: {flow_id}"
                ));
            }
        }
    }
    let candidate_flow_ids = if versioned_scope.is_some() {
        let mut ids = flow_meta.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    } else {
        flow_candidates.into_iter().collect::<Vec<_>>()
    };
    let mut flows = Vec::with_capacity(candidate_flow_ids.len());
    let mut flow_idx_by_id = HashMap::with_capacity(candidate_flow_ids.len());
    let mut elementary_flow_idx = HashSet::new();
    for (idx, flow_id) in candidate_flow_ids.iter().enumerate() {
        let flow_index = i32::try_from(idx).map_err(|_| anyhow::anyhow!("flow idx overflow"))?;
        let kind = if flow_meta
            .get(flow_id)
            .is_some_and(|meta| classify_flow_kind(&meta.json) == "elementary")
        {
            CompiledFlowKind::Elementary
        } else {
            CompiledFlowKind::Product
        };
        if kind == CompiledFlowKind::Elementary {
            elementary_flow_idx.insert(flow_index);
        }
        flow_idx_by_id.insert(*flow_id, flow_index);
        flows.push(CompiledFlow {
            flow_idx: flow_index,
            flow_id: *flow_id,
            kind,
        });
    }

    let mut provider_map: ProviderMap = HashMap::new();
    for ex in &exchanges {
        if ex.direction == Some(ExchangeDirection::Output) {
            provider_map.entry(ex.flow_id).or_default().push(
                provider_output_candidate_from_exchange(ex.process_idx, ex.flow_id, ex),
            );
        }
    }
    for providers in provider_map.values_mut() {
        sort_provider_output_candidates(providers, &process_meta);
    }
    let provider_outputs = provider_outputs_from_map(&provider_map);

    let mut provider_decisions = Vec::<CompiledProviderDecision>::new();
    let mut technosphere_edges = Vec::<CompiledTechnosphereEdge>::new();
    let mut biosphere_edges = Vec::<CompiledBiosphereEdge>::new();
    let mut matching_stats = CompiledMatchingStats::default();

    for ex in &exchanges {
        if let Some(flow_idx) = flow_idx_by_id.get(&ex.flow_id).copied()
            && elementary_flow_idx.contains(&flow_idx)
            && let Some(amount) = ex.amount
        {
            let value = biosphere_gross_value(amount);
            if retain_sparse_value(value, versioned_scope.is_some()) {
                let process_partition =
                    compiled_process_for_idx(&compiled_processes, ex.process_idx)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "missing compiled process for biosphere idx={}",
                                ex.process_idx
                            )
                        })?
                        .partition;
                biosphere_edges.push(CompiledBiosphereEdge {
                    process_idx: ex.process_idx,
                    flow_idx,
                    amount: value,
                    process_partition,
                });
            }
        }

        if ex.direction != Some(ExchangeDirection::Input) {
            continue;
        }

        let Some(amount) = ex.amount else {
            continue;
        };
        let supply_region_anchor = supply_region_anchor_for_exchange(ex, &process_meta)?;
        matching_stats.input_edges_total += 1;
        let provider_outputs = provider_map.get(&ex.flow_id);
        let eligible_providers = eligible_provider_indices(provider_outputs);
        let provider_candidates = provider_candidates_for_outputs(provider_outputs, &process_meta)?;
        let candidate_provider_count =
            i32::try_from(eligible_providers.len()).map_err(|_| anyhow::anyhow!("providers"))?;
        if candidate_provider_count == 1 {
            matching_stats.matched_unique_provider += 1;
            matching_stats.a_input_edges_written += 1;
            let provider_idx = *eligible_providers
                .first()
                .ok_or_else(|| anyhow::anyhow!("missing provider idx"))?;
            let provider_meta =
                process_meta_for_idx(&process_meta, provider_idx).ok_or_else(|| {
                    anyhow::anyhow!("missing provider process meta idx={provider_idx}")
                })?;
            provider_decisions.push(CompiledProviderDecision {
                consumer_idx: ex.process_idx,
                flow_id: ex.flow_id,
                candidate_provider_count,
                matched_provider_count: 1,
                candidates: provider_candidates,
                decision_kind: Some(CompiledProviderDecisionKind::UniqueProvider),
                resolution_strategy: Some(CompiledProviderResolutionStrategy::UniqueProvider),
                failure_reason: None,
                used_equal_fallback: false,
                volume_fallback_to_one_count: 0,
                geography_tier: Some(provider_geography_tier(
                    supply_region_anchor.location.as_deref(),
                    provider_meta.location.as_deref(),
                )),
                supply_region_source: Some(supply_region_anchor.source),
                supply_region_location: supply_region_anchor.location.clone(),
                exchange_location_present: ex.location.is_some(),
                allocations: vec![CompiledProviderAllocation {
                    provider_idx,
                    weight: 1.0,
                }],
            });
            let provider = compiled_process_for_idx(&compiled_processes, provider_idx)
                .ok_or_else(|| anyhow::anyhow!("missing compiled provider idx={provider_idx}"))?;
            let consumer = compiled_process_for_idx(&compiled_processes, ex.process_idx)
                .ok_or_else(|| {
                    anyhow::anyhow!("missing compiled consumer idx={}", ex.process_idx)
                })?;
            technosphere_edges.push(CompiledTechnosphereEdge {
                provider_idx,
                consumer_idx: ex.process_idx,
                flow_id: ex.flow_id,
                amount,
                provider_partition: provider.partition,
                consumer_partition: consumer.partition,
                partition: CompiledEdgePartition::from_partitions(
                    provider.partition,
                    consumer.partition,
                ),
            });
        } else if candidate_provider_count > 1 {
            matching_stats.matched_multi_provider += 1;
            let resolution =
                resolve_multi_provider(provider_rule, ex, &eligible_providers, &process_meta)?;
            match resolution {
                MultiProviderDecision::Resolved(mut resolution) => {
                    matching_stats.matched_multi_resolved += 1;
                    if resolution.used_equal_fallback {
                        matching_stats.matched_multi_fallback_equal += 1;
                    }
                    matching_stats.a_input_edges_written += 1;
                    if resolution.geography_tier.is_none() {
                        resolution.geography_tier = best_geography_tier_for_allocations(
                            supply_region_anchor.location.as_deref(),
                            &resolution.allocations,
                            &process_meta,
                        )?;
                    }
                    let allocations = resolution
                        .allocations
                        .into_iter()
                        .map(|(provider_idx, weight)| CompiledProviderAllocation {
                            provider_idx,
                            weight,
                        })
                        .collect::<Vec<_>>();
                    provider_decisions.push(CompiledProviderDecision {
                        consumer_idx: ex.process_idx,
                        flow_id: ex.flow_id,
                        candidate_provider_count,
                        matched_provider_count: i32::try_from(allocations.len())
                            .map_err(|_| anyhow::anyhow!("provider allocation overflow"))?,
                        candidates: provider_candidates.clone(),
                        decision_kind: Some(CompiledProviderDecisionKind::MultiResolved),
                        resolution_strategy: Some(resolution.resolution_strategy),
                        failure_reason: None,
                        used_equal_fallback: resolution.used_equal_fallback,
                        volume_fallback_to_one_count: resolution.volume_fallback_to_one_count,
                        geography_tier: resolution.geography_tier,
                        supply_region_source: Some(supply_region_anchor.source),
                        supply_region_location: supply_region_anchor.location.clone(),
                        exchange_location_present: ex.location.is_some(),
                        allocations: allocations.clone(),
                    });
                    let consumer = compiled_process_for_idx(&compiled_processes, ex.process_idx)
                        .ok_or_else(|| {
                            anyhow::anyhow!("missing compiled consumer idx={}", ex.process_idx)
                        })?;
                    for allocation in &allocations {
                        let weighted = amount * allocation.weight;
                        if weighted.abs() <= f64::EPSILON {
                            continue;
                        }
                        let provider =
                            compiled_process_for_idx(&compiled_processes, allocation.provider_idx)
                                .ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "missing compiled provider idx={}",
                                        allocation.provider_idx
                                    )
                                })?;
                        technosphere_edges.push(CompiledTechnosphereEdge {
                            provider_idx: allocation.provider_idx,
                            consumer_idx: ex.process_idx,
                            flow_id: ex.flow_id,
                            amount: weighted,
                            provider_partition: provider.partition,
                            consumer_partition: consumer.partition,
                            partition: CompiledEdgePartition::from_partitions(
                                provider.partition,
                                consumer.partition,
                            ),
                        });
                    }
                }
                MultiProviderDecision::Unresolved(failure_reason) => {
                    matching_stats.matched_multi_unresolved += 1;
                    provider_decisions.push(CompiledProviderDecision {
                        consumer_idx: ex.process_idx,
                        flow_id: ex.flow_id,
                        candidate_provider_count,
                        matched_provider_count: 0,
                        candidates: provider_candidates,
                        decision_kind: Some(CompiledProviderDecisionKind::MultiUnresolved),
                        resolution_strategy: None,
                        failure_reason: Some(failure_reason),
                        used_equal_fallback: false,
                        volume_fallback_to_one_count: 0,
                        geography_tier: None,
                        supply_region_source: Some(supply_region_anchor.source),
                        supply_region_location: supply_region_anchor.location.clone(),
                        exchange_location_present: ex.location.is_some(),
                        allocations: Vec::new(),
                    });
                }
            }
        } else {
            matching_stats.unmatched_no_provider += 1;
            provider_decisions.push(CompiledProviderDecision {
                consumer_idx: ex.process_idx,
                flow_id: ex.flow_id,
                candidate_provider_count: 0,
                matched_provider_count: 0,
                candidates: Vec::new(),
                decision_kind: Some(CompiledProviderDecisionKind::NoProvider),
                resolution_strategy: None,
                failure_reason: Some(no_provider_failure_reason(provider_outputs)),
                used_equal_fallback: false,
                volume_fallback_to_one_count: 0,
                geography_tier: None,
                supply_region_source: Some(supply_region_anchor.source),
                supply_region_location: supply_region_anchor.location,
                exchange_location_present: ex.location.is_some(),
                allocations: Vec::new(),
            });
        }
    }

    let lcia_exchange_observations = exchanges
        .iter()
        .filter(|exchange| {
            flow_idx_by_id
                .get(&exchange.flow_id)
                .is_some_and(|flow_idx| elementary_flow_idx.contains(flow_idx))
        })
        .map(|exchange| LciaExchangeObservation {
            flow_id: exchange.flow_id,
            flow_version: exchange.flow_version.clone(),
            direction: exchange.direction,
            direction_label: exchange.direction_label.clone(),
            exchange_id: exchange.exchange_id.clone(),
            amount: exchange.amount,
        })
        .collect();

    Ok(CompiledScopeGraph {
        graph: CompiledGraph {
            processes: compiled_processes,
            flows,
            provider_outputs,
            provider_decisions,
            technosphere_edges,
            biosphere_edges,
            reference_stats: CompiledReferenceStats {
                missing_reference: reference_stats.missing_reference,
                invalid_reference: reference_stats.invalid_reference,
                normalized_processes: reference_stats.normalized_processes,
            },
            allocation_stats: CompiledAllocationStats {
                exchange_total: allocation_stats.exchange_total,
                fraction_present_count: allocation_stats.fraction_present_count,
                fraction_missing_count: allocation_stats.fraction_missing_count,
                fraction_invalid_count: allocation_stats.fraction_invalid_count,
            },
            matching_stats,
        },
        lcia_exchange_observations,
    })
}

fn assemble_sparse_payload(
    snapshot_id: Uuid,
    method: &MethodSelection,
    compiled_graph: &CompiledGraph,
    self_loop_cutoff: f64,
    singular_eps: f64,
    has_lcia: bool,
    impact_factor_sets: &[ImpactFactorSet],
    lcia_exchange_observations: &[LciaExchangeObservation],
    directional_lcia: bool,
) -> anyhow::Result<BuildOutput> {
    let process_count_i32 = i32::try_from(compiled_graph.processes.len())
        .map_err(|_| anyhow::anyhow!("process overflow"))?;
    let flow_count =
        i32::try_from(compiled_graph.flows.len()).map_err(|_| anyhow::anyhow!("flow overflow"))?;
    let mut flow_idx_by_id = HashMap::with_capacity(compiled_graph.flows.len());
    for flow in &compiled_graph.flows {
        flow_idx_by_id.insert(flow.flow_id, flow.flow_idx);
    }

    let mut a_map: HashMap<(i32, i32), f64> = HashMap::new();
    for edge in &compiled_graph.technosphere_edges {
        add_technosphere_edge(
            &mut a_map,
            edge.provider_idx,
            edge.consumer_idx,
            edge.amount,
        );
    }
    let mut b_map: HashMap<(i32, i32), f64> = HashMap::new();
    for edge in &compiled_graph.biosphere_edges {
        accumulate_biosphere_edge(
            &mut b_map,
            (edge.flow_idx, edge.process_idx),
            edge.amount,
            directional_lcia,
        )?;
    }

    a_map.retain(|_, value| value.abs() > f64::EPSILON);
    b_map.retain(|_, value| retain_sparse_value(*value, directional_lcia));

    let prefilter_diag_ge_cutoff = i64::try_from(
        a_map
            .iter()
            .filter(|((row, col), value)| row == col && value.abs() >= self_loop_cutoff)
            .count(),
    )
    .map_err(|_| anyhow::anyhow!("prefilter count overflow"))?;

    let mut technosphere_entries = Vec::new();
    technosphere_entries.reserve(a_map.len());
    for ((row, col), value) in a_map {
        if row == col && value.abs() >= self_loop_cutoff {
            continue;
        }
        technosphere_entries.push(SparseTriplet { row, col, value });
    }

    let mut diag_a = HashMap::<i32, f64>::new();
    for t in &technosphere_entries {
        if t.row == t.col {
            *diag_a.entry(t.row).or_insert(0.0) += t.value;
        }
    }

    let a_diag_ge_cutoff = i64::try_from(
        diag_a
            .values()
            .filter(|value| value.abs() >= self_loop_cutoff)
            .count(),
    )
    .map_err(|_| anyhow::anyhow!("diag count overflow"))?;

    let mut m_zero_diag_count: i64 = 0;
    let mut m_min_abs_diag = f64::INFINITY;
    for idx in 0..process_count_i32 {
        let a_diag = diag_a.get(&idx).copied().unwrap_or(0.0);
        let abs_m_diag = (1.0 - a_diag).abs();
        if abs_m_diag <= singular_eps {
            m_zero_diag_count += 1;
        }
        if abs_m_diag < m_min_abs_diag {
            m_min_abs_diag = abs_m_diag;
        }
    }
    if !m_min_abs_diag.is_finite() {
        m_min_abs_diag = 0.0;
    }

    let risk_level = if m_zero_diag_count > 0 {
        "high".to_owned()
    } else if prefilter_diag_ge_cutoff > 0 || a_diag_ge_cutoff > 0 {
        "medium".to_owned()
    } else {
        "low".to_owned()
    };

    let mut biosphere_entries = Vec::with_capacity(b_map.len());
    for ((row, col), value) in b_map {
        biosphere_entries.push(SparseTriplet { row, col, value });
    }

    let direction_by_flow = if directional_lcia {
        unique_supported_direction_by_flow(lcia_exchange_observations)
    } else {
        HashMap::new()
    };
    let mut characterization_factors = Vec::new();
    if has_lcia {
        for (impact_idx, impact) in impact_factor_sets.iter().enumerate() {
            let impact_row =
                i32::try_from(impact_idx).map_err(|_| anyhow::anyhow!("impact idx overflow"))?;
            let mut c_map = HashMap::<i32, f64>::new();
            if directional_lcia {
                for (flow_id, direction) in &direction_by_flow {
                    let Some(direction) = direction else {
                        continue;
                    };
                    let Some(cf_value) = impact
                        .factors_by_flow_direction
                        .get(&(*flow_id, *direction))
                    else {
                        continue;
                    };
                    if let Some(flow_idx) = flow_idx_by_id.get(flow_id).copied()
                        && retain_sparse_value(*cf_value, true)
                    {
                        accumulate_finite_factor(
                            &mut c_map,
                            flow_idx,
                            *cf_value,
                            impact.impact_id,
                        )?;
                    }
                }
            } else {
                for (flow_id, cf_value) in &impact.factors_by_flow {
                    if let Some(flow_idx) = flow_idx_by_id.get(flow_id).copied()
                        && cf_value.abs() > f64::EPSILON
                    {
                        accumulate_finite_factor(
                            &mut c_map,
                            flow_idx,
                            *cf_value,
                            impact.impact_id,
                        )?;
                    }
                }
            }
            c_map.retain(|_, value| retain_sparse_value(*value, directional_lcia));
            characterization_factors.reserve(c_map.len());
            for (col, value) in c_map {
                characterization_factors.push(SparseTriplet {
                    row: impact_row,
                    col,
                    value,
                });
            }
        }
    }

    let impact_count = if has_lcia {
        i32::try_from(impact_factor_sets.len()).map_err(|_| anyhow::anyhow!("impact overflow"))?
    } else {
        1_i32
    };
    let a_nnz = i64::try_from(technosphere_entries.len()).map_err(|_| anyhow::anyhow!("a nnz"))?;
    let b_nnz = i64::try_from(biosphere_entries.len()).map_err(|_| anyhow::anyhow!("b nnz"))?;
    let c_nnz =
        i64::try_from(characterization_factors.len()).map_err(|_| anyhow::anyhow!("c nnz"))?;
    let a_offdiag_nnz = i64::try_from(
        technosphere_entries
            .iter()
            .filter(|entry| entry.row != entry.col)
            .count(),
    )
    .map_err(|_| anyhow::anyhow!("a offdiag overflow"))?;
    let process_count_i64 = i64::from(process_count_i32);
    let m_nnz_estimated = a_offdiag_nnz + (process_count_i64 - m_zero_diag_count).max(0);
    let m_sparsity_estimated = if process_count_i64 == 0 {
        1.0
    } else {
        1.0 - (m_nnz_estimated as f64 / (process_count_i64 * process_count_i64) as f64)
    };

    let unique_provider_match_pct = pct(
        compiled_graph.matching_stats.matched_unique_provider,
        compiled_graph.matching_stats.input_edges_total,
    );
    let any_provider_match_pct = pct(
        compiled_graph.matching_stats.matched_unique_provider
            + compiled_graph.matching_stats.matched_multi_provider,
        compiled_graph.matching_stats.input_edges_total,
    );
    let a_write_pct = pct(
        compiled_graph.matching_stats.a_input_edges_written,
        compiled_graph.matching_stats.input_edges_total,
    );
    let provider_present_total = compiled_graph.matching_stats.matched_unique_provider
        + compiled_graph.matching_stats.matched_multi_provider;
    let provider_present_resolved_pct = pct(
        compiled_graph.matching_stats.a_input_edges_written,
        provider_present_total,
    );
    let allocation_fraction_present_pct = pct(
        compiled_graph.allocation_stats.fraction_present_count,
        compiled_graph.allocation_stats.exchange_total,
    );
    let matching_diagnostics = summarize_matching_diagnostics(compiled_graph);

    let coverage = SnapshotCoverageReport {
        schema_version: solver_worker::snapshot_artifacts::SNAPSHOT_COVERAGE_SCHEMA_VERSION
            .to_owned(),
        matching: SnapshotMatchingCoverage {
            input_edges_total: compiled_graph.matching_stats.input_edges_total,
            matched_unique_provider: compiled_graph.matching_stats.matched_unique_provider,
            matched_multi_provider: compiled_graph.matching_stats.matched_multi_provider,
            unmatched_no_provider: compiled_graph.matching_stats.unmatched_no_provider,
            matched_multi_resolved: compiled_graph.matching_stats.matched_multi_resolved,
            matched_multi_unresolved: compiled_graph.matching_stats.matched_multi_unresolved,
            matched_multi_fallback_equal: compiled_graph
                .matching_stats
                .matched_multi_fallback_equal,
            a_input_edges_written: compiled_graph.matching_stats.a_input_edges_written,
            a_write_pct,
            provider_present_resolved_pct,
            unique_provider_match_pct,
            any_provider_match_pct,
            provider_decision_diagnostics: matching_diagnostics.provider_decision_diagnostics,
            candidate_summary: matching_diagnostics.candidate_summary,
            resolution_summary: matching_diagnostics.resolution_summary,
            geography_summary: matching_diagnostics.geography_summary,
            volume_weight_summary: matching_diagnostics.volume_weight_summary,
            gap_summary: matching_diagnostics.gap_summary,
        },
        reference: SnapshotReferenceCoverage {
            process_total: process_count_i64,
            normalized_process_count: compiled_graph.reference_stats.normalized_processes,
            missing_reference_count: compiled_graph.reference_stats.missing_reference,
            invalid_reference_count: compiled_graph.reference_stats.invalid_reference,
        },
        allocation: SnapshotAllocationCoverage {
            exchange_total: compiled_graph.allocation_stats.exchange_total,
            allocation_fraction_present_pct,
            allocation_fraction_missing_count: compiled_graph
                .allocation_stats
                .fraction_missing_count,
            allocation_fraction_invalid_count: compiled_graph
                .allocation_stats
                .fraction_invalid_count,
        },
        singular_risk: SnapshotSingularRisk {
            risk_level,
            prefilter_diag_abs_ge_cutoff: prefilter_diag_ge_cutoff,
            postfilter_a_diag_abs_ge_cutoff: a_diag_ge_cutoff,
            m_zero_diagonal_count: m_zero_diag_count,
            m_min_abs_diagonal: m_min_abs_diag,
        },
        matrix_scale: SnapshotMatrixScale {
            process_count: process_count_i64,
            flow_count: i64::from(flow_count),
            impact_count: i64::from(impact_count),
            a_nnz,
            b_nnz,
            c_nnz,
            m_nnz_estimated,
            m_sparsity_estimated,
        },
    };

    let data = ModelSparseData {
        model_version: snapshot_id,
        process_count: process_count_i32,
        flow_count,
        impact_count,
        technosphere_entries,
        biosphere_entries,
        characterization_factors,
    };
    let process_map = compiled_graph
        .processes
        .iter()
        .map(|meta| SnapshotProcessMapEntry {
            process_id: meta.process_id,
            process_index: meta.process_idx,
            process_version: meta.process_version.clone(),
            process_name: meta.process_name.clone(),
            location: meta.location.clone(),
        })
        .collect::<Vec<_>>();
    let impact_map = build_snapshot_impact_map(snapshot_id, method, impact_factor_sets)?;

    let snapshot_index = SnapshotIndexDocument {
        version: 1,
        snapshot_id,
        process_count: process_count_i32,
        impact_count,
        process_map,
        impact_map,
        calculation_evidence: None,
    };
    let readiness = verify_matrix_readiness(&MatrixReadinessInput {
        schema_version: "matrix_readiness_input.v1".to_owned(),
        snapshot_id: Some(snapshot_id),
        config: None,
        coverage: coverage.clone(),
        payload: data.clone(),
        compiled_graph: Some(compiled_graph.clone()),
        policy: MatrixReadinessPolicy {
            require_lcia_factors: has_lcia,
            ..MatrixReadinessPolicy::default()
        },
    });

    let lcia_factor_coverage = directional_lcia
        .then(|| {
            build_lcia_factor_coverage(
                lcia_exchange_observations,
                impact_factor_sets,
                &direction_by_flow,
            )
        })
        .transpose()?;

    Ok(BuildOutput {
        data,
        coverage,
        snapshot_index,
        readiness,
        compiled_graph: compiled_graph.clone(),
        lcia_factor_coverage,
    })
}

fn unique_supported_direction_by_flow(
    observations: &[LciaExchangeObservation],
) -> HashMap<Uuid, Option<ExchangeDirection>> {
    let mut directions = HashMap::<Uuid, HashSet<ExchangeDirection>>::new();
    let mut unsupported = HashSet::<Uuid>::new();
    for observation in observations {
        if let Some(direction) = observation.direction {
            directions
                .entry(observation.flow_id)
                .or_default()
                .insert(direction);
        } else {
            unsupported.insert(observation.flow_id);
        }
    }
    observations
        .iter()
        .map(|observation| observation.flow_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .map(|flow_id| {
            let resolved = if unsupported.contains(&flow_id) {
                None
            } else {
                let values = directions.get(&flow_id);
                if values.is_some_and(|items| items.len() == 1) {
                    values.and_then(|items| items.iter().next().copied())
                } else {
                    None
                }
            };
            (flow_id, resolved)
        })
        .collect()
}

fn build_lcia_factor_coverage(
    observations: &[LciaExchangeObservation],
    impact_factor_sets: &[ImpactFactorSet],
    direction_by_flow: &HashMap<Uuid, Option<ExchangeDirection>>,
) -> anyhow::Result<LciaFactorCoverageBuild> {
    let mut counts = LciaFactorCoverageCounts::default();
    let mut by_method = Vec::with_capacity(impact_factor_sets.len());
    let mut records = tempfile::Builder::new()
        .prefix("lcia-uncharacterized-")
        .suffix(".jsonl")
        .tempfile()?;
    let mut record_count = 0_u64;
    let mut artifact_byte_size = 0_u64;
    let mut artifact_hasher = Sha256::new();
    let mut sorted_observations = observations.iter().collect::<Vec<_>>();
    sorted_observations.sort_by(|left, right| {
        left.flow_id
            .cmp(&right.flow_id)
            .then_with(|| left.direction_label.cmp(&right.direction_label))
            .then_with(|| left.exchange_id.cmp(&right.exchange_id))
    });
    let mut sorted_methods = impact_factor_sets.iter().collect::<Vec<_>>();
    sorted_methods.sort_by_key(|method| (method.impact_id, method.method_version.clone()));

    for method in sorted_methods {
        let mut method_counts = LciaFactorCoverageCounts::default();
        for observation in &sorted_observations {
            let amount = observation.amount.filter(|value| value.is_finite());
            let outcome = if observation.direction.is_none() {
                Some(("unsupported_direction", "unsupported_exchange_direction"))
            } else if direction_by_flow.get(&observation.flow_id) == Some(&None) {
                Some((
                    "unsupported_direction",
                    "ambiguous_elementary_flow_direction_axis",
                ))
            } else if amount.is_none() {
                Some(("invalid", "invalid_exchange_amount"))
            } else if observation.direction.is_some_and(|direction| {
                method
                    .factors_by_flow_direction
                    .contains_key(&(observation.flow_id, direction))
            }) {
                None
            } else {
                Some(("unmatched", "no_lcia_factor_for_flow_direction"))
            };

            match outcome {
                None => method_counts.matched = method_counts.matched.saturating_add(1),
                Some((kind, reason)) => {
                    match kind {
                        "unmatched" => {
                            method_counts.unmatched = method_counts.unmatched.saturating_add(1);
                        }
                        "invalid" => {
                            method_counts.invalid = method_counts.invalid.saturating_add(1);
                        }
                        "unsupported_direction" => {
                            method_counts.unsupported_direction =
                                method_counts.unsupported_direction.saturating_add(1);
                        }
                        _ => return Err(anyhow::anyhow!("unknown LCIA coverage outcome")),
                    }
                    let record = LciaUncharacterizedRecord {
                        method_id: method.impact_id,
                        method_version: method.method_version.clone(),
                        artifact_locator_id: method.artifact_locator_id,
                        flow_uuid: observation.flow_id,
                        flow_version: observation.flow_version.clone(),
                        direction: observation.direction.map_or_else(
                            || observation.direction_label.clone(),
                            |direction| direction.as_str().to_owned(),
                        ),
                        exchange_id: observation.exchange_id.clone(),
                        amount,
                        reason: reason.to_owned(),
                    };
                    let mut line = serde_json::to_vec(&record)?;
                    line.push(b'\n');
                    if record_count >= MAX_LCIA_GAP_EVIDENCE_RECORDS {
                        return Err(anyhow::anyhow!(
                            "LCIA gap evidence exceeds the {MAX_LCIA_GAP_EVIDENCE_RECORDS}-record fail-closed limit"
                        ));
                    }
                    artifact_byte_size = artifact_byte_size
                        .checked_add(u64::try_from(line.len())?)
                        .ok_or_else(|| anyhow::anyhow!("LCIA gap evidence byte-size overflow"))?;
                    if artifact_byte_size > MAX_LCIA_GAP_EVIDENCE_BYTES {
                        return Err(anyhow::anyhow!(
                            "LCIA gap evidence exceeds the {MAX_LCIA_GAP_EVIDENCE_BYTES}-byte fail-closed limit"
                        ));
                    }
                    records.write_all(&line)?;
                    artifact_hasher.update(&line);
                    record_count = record_count
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("LCIA gap record count overflow"))?;
                }
            }
        }
        counts.matched = counts
            .matched
            .checked_add(method_counts.matched)
            .ok_or_else(|| anyhow::anyhow!("LCIA matched count overflow"))?;
        counts.unmatched = counts
            .unmatched
            .checked_add(method_counts.unmatched)
            .ok_or_else(|| anyhow::anyhow!("LCIA unmatched count overflow"))?;
        counts.invalid = counts
            .invalid
            .checked_add(method_counts.invalid)
            .ok_or_else(|| anyhow::anyhow!("LCIA invalid count overflow"))?;
        counts.unsupported_direction = counts
            .unsupported_direction
            .checked_add(method_counts.unsupported_direction)
            .ok_or_else(|| anyhow::anyhow!("LCIA unsupported-direction count overflow"))?;
        by_method.push(LciaMethodFactorCoverage {
            method_id: method.impact_id,
            method_version: method.method_version.clone(),
            artifact_locator_id: method.artifact_locator_id,
            counts: method_counts,
        });
    }
    records.flush()?;
    Ok(LciaFactorCoverageBuild {
        counts,
        by_method,
        records,
        record_count,
        artifact_byte_size,
        artifact_sha256: hex::encode(artifact_hasher.finalize()),
    })
}

fn build_snapshot_impact_map(
    snapshot_id: Uuid,
    method: &MethodSelection,
    impact_factor_sets: &[ImpactFactorSet],
) -> anyhow::Result<Vec<SnapshotImpactMapEntry>> {
    if !method.has_lcia {
        return Ok(vec![SnapshotImpactMapEntry {
            impact_id: snapshot_id,
            impact_index: 0,
            impact_key: "lcia-disabled".to_owned(),
            impact_name: "LCIA disabled (placeholder impact)".to_owned(),
            unit: "unknown".to_owned(),
        }]);
    }
    if impact_factor_sets.is_empty() {
        return Err(anyhow::anyhow!(
            "LCIA is enabled but no impact factors were loaded"
        ));
    }

    let mut out = Vec::with_capacity(impact_factor_sets.len());
    for (impact_idx, impact) in impact_factor_sets.iter().enumerate() {
        out.push(SnapshotImpactMapEntry {
            impact_id: impact.impact_id,
            impact_index: i32::try_from(impact_idx)
                .map_err(|_| anyhow::anyhow!("impact index overflow"))?,
            impact_key: impact.impact_key.clone(),
            impact_name: impact.impact_name.clone(),
            unit: impact.unit.clone(),
        });
    }
    Ok(out)
}

#[derive(Debug, Clone)]
struct MultiProviderResolution {
    allocations: Vec<(i32, f64)>,
    resolution_strategy: CompiledProviderResolutionStrategy,
    used_equal_fallback: bool,
    volume_fallback_to_one_count: i32,
    geography_tier: Option<CompiledProviderGeographyTier>,
}

#[derive(Debug, Clone)]
enum MultiProviderDecision {
    Resolved(MultiProviderResolution),
    Unresolved(CompiledProviderFailureReason),
}

const AUTO_LINK_GEO_WEIGHT: f64 = 0.7;
const AUTO_LINK_TIME_WEIGHT: f64 = 0.3;
const AUTO_LINK_MIN_SCORE: f64 = 0.35;
const AUTO_LINK_TOP1_MIN_SCORE: f64 = 0.55;
const AUTO_LINK_TOP1_TOP2_MIN_RATIO: f64 = 1.2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AllocationFractionState {
    Present,
    Missing,
    Invalid,
}

fn resolve_reference_normalization(
    process_id: Uuid,
    process_json: &Value,
    exchanges: &[&Value],
    mode: NormalizationMode,
) -> anyhow::Result<(f64, ReferenceParseStats)> {
    let mut stats = ReferenceParseStats::default();
    let reference_internal_id = parse_reference_internal_id(process_json);

    let Some(reference_internal_id) = reference_internal_id.as_deref() else {
        stats.missing_reference = 1;
        return match mode {
            NormalizationMode::Strict => Err(anyhow::anyhow!(
                "missing quantitativeReference.referenceToReferenceFlow for process={process_id}"
            )),
            NormalizationMode::Lenient => Ok((1.0, stats)),
        };
    };

    let reference_exchange = exchanges.iter().copied().find(|exchange| {
        parse_exchange_internal_id(exchange).as_deref() == Some(reference_internal_id)
    });

    let Some(reference_exchange) = reference_exchange else {
        stats.invalid_reference = 1;
        return match mode {
            NormalizationMode::Strict => Err(anyhow::anyhow!(
                "referenceToReferenceFlow={} not found in exchanges for process={process_id}",
                reference_internal_id
            )),
            NormalizationMode::Lenient => Ok((1.0, stats)),
        };
    };

    let reference_amount = parse_number(
        reference_exchange
            .get("meanAmount")
            .or_else(|| reference_exchange.get("resultingAmount"))
            .or_else(|| reference_exchange.get("meanValue")),
    )
    .map(f64::abs)
    .filter(|value| *value > f64::EPSILON);
    let Some(reference_amount) = reference_amount else {
        stats.invalid_reference = 1;
        return match mode {
            NormalizationMode::Strict => Err(anyhow::anyhow!(
                "invalid reference amount for process={process_id} reference_internal_id={}",
                reference_internal_id
            )),
            NormalizationMode::Lenient => Ok((1.0, stats)),
        };
    };

    stats.normalized_processes = 1;
    Ok((1.0 / reference_amount, stats))
}

fn resolve_allocation_fraction(
    exchange_json: &Value,
    mode: AllocationMode,
) -> anyhow::Result<(f64, AllocationFractionState)> {
    let raw = exchange_json
        .get("allocations")
        .and_then(|v| v.get("allocation"))
        .and_then(|v| v.get("@allocatedFraction"));
    let Some(raw) = raw else {
        return match mode {
            AllocationMode::Strict => Err(anyhow::anyhow!(
                "missing allocations.allocation.@allocatedFraction"
            )),
            AllocationMode::Lenient => Ok((1.0, AllocationFractionState::Missing)),
        };
    };

    let parsed = match raw {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else if let Some(without_percent) = trimmed.strip_suffix('%') {
                without_percent
                    .trim()
                    .parse::<f64>()
                    .ok()
                    .map(|value| value / 100.0)
            } else {
                trimmed.parse::<f64>().ok().map(
                    |value| {
                        if value > 1.0 { value / 100.0 } else { value }
                    },
                )
            }
        }
        _ => None,
    };
    let fraction = parsed.filter(|value| value.is_finite() && *value >= 0.0 && *value <= 1.0);
    let Some(fraction) = fraction else {
        return match mode {
            AllocationMode::Strict => Err(anyhow::anyhow!(
                "invalid allocations.allocation.@allocatedFraction={}",
                raw
            )),
            AllocationMode::Lenient => Ok((1.0, AllocationFractionState::Invalid)),
        };
    };

    Ok((fraction, AllocationFractionState::Present))
}

fn resolve_multi_provider(
    provider_rule: ProviderRule,
    exchange: &ParsedExchange,
    providers: &[i32],
    process_meta: &[ProcessMeta],
) -> anyhow::Result<MultiProviderDecision> {
    if providers.is_empty() {
        return Ok(MultiProviderDecision::Unresolved(
            CompiledProviderFailureReason::NoProviderCandidates,
        ));
    }

    let split_equal = || -> MultiProviderResolution {
        let share = 1.0 / providers.len() as f64;
        MultiProviderResolution {
            allocations: providers.iter().map(|idx| (*idx, share)).collect(),
            resolution_strategy: CompiledProviderResolutionStrategy::SplitEqual,
            used_equal_fallback: false,
            volume_fallback_to_one_count: 0,
            geography_tier: None,
        }
    };

    let split_equal_fallback = || -> MultiProviderResolution {
        let mut resolution = split_equal();
        resolution.resolution_strategy = CompiledProviderResolutionStrategy::SplitEqualFallback;
        resolution.used_equal_fallback = true;
        resolution
    };

    let scored_candidates = |min_score: f64| -> anyhow::Result<Vec<ProviderCandidateScore>> {
        let mut scored = score_provider_candidates(
            exchange.process_idx,
            exchange.location.as_deref(),
            providers,
            process_meta,
        )?;
        scored.retain(|candidate| candidate.final_score >= min_score);
        Ok(scored)
    };

    match provider_rule {
        ProviderRule::StrictUniqueProvider => Ok(MultiProviderDecision::Unresolved(
            CompiledProviderFailureReason::RuleRequiresUniqueProvider,
        )),
        ProviderRule::SplitEqual => Ok(MultiProviderDecision::Resolved(split_equal())),
        ProviderRule::SplitByProcessVolume => {
            split_by_process_volume(exchange, providers, process_meta)
                .map(MultiProviderDecision::Resolved)
        }
        ProviderRule::BestProviderStrict => {
            let scored = scored_candidates(AUTO_LINK_MIN_SCORE)?;
            let Some(top1) = scored.first() else {
                return Ok(MultiProviderDecision::Unresolved(
                    CompiledProviderFailureReason::NoCandidateGeMinScore,
                ));
            };
            if top1.final_score < AUTO_LINK_TOP1_MIN_SCORE {
                return Ok(MultiProviderDecision::Unresolved(
                    CompiledProviderFailureReason::Top1BelowTop1MinScore,
                ));
            }
            if let Some(top2) = scored.get(1)
                && top2.final_score > f64::EPSILON
                && (top1.final_score / top2.final_score) < AUTO_LINK_TOP1_TOP2_MIN_RATIO
            {
                return Ok(MultiProviderDecision::Unresolved(
                    CompiledProviderFailureReason::Top1Top2RatioTooClose,
                ));
            }

            Ok(MultiProviderDecision::Resolved(MultiProviderResolution {
                allocations: vec![(top1.provider_idx, 1.0)],
                resolution_strategy: CompiledProviderResolutionStrategy::BestProviderStrict,
                used_equal_fallback: false,
                volume_fallback_to_one_count: 0,
                geography_tier: None,
            }))
        }
        ProviderRule::SplitByEvidenceStrict => {
            let scored = scored_candidates(AUTO_LINK_MIN_SCORE)?;
            if scored.is_empty() {
                return Ok(MultiProviderDecision::Unresolved(
                    CompiledProviderFailureReason::NoCandidateGeMinScore,
                ));
            }
            let score_sum = scored
                .iter()
                .map(|candidate| candidate.final_score)
                .sum::<f64>();
            if score_sum <= f64::EPSILON {
                return Ok(MultiProviderDecision::Unresolved(
                    CompiledProviderFailureReason::ScoreSumNonPositive,
                ));
            }
            Ok(MultiProviderDecision::Resolved(MultiProviderResolution {
                allocations: scored
                    .iter()
                    .map(|candidate| (candidate.provider_idx, candidate.final_score / score_sum))
                    .collect(),
                resolution_strategy: CompiledProviderResolutionStrategy::SplitByEvidence,
                used_equal_fallback: false,
                volume_fallback_to_one_count: 0,
                geography_tier: None,
            }))
        }
        ProviderRule::SplitByEvidenceHybrid => {
            let scored = scored_candidates(AUTO_LINK_MIN_SCORE)?;
            if scored.is_empty() {
                return Ok(MultiProviderDecision::Resolved(split_equal_fallback()));
            }
            let score_sum = scored
                .iter()
                .map(|candidate| candidate.final_score)
                .sum::<f64>();
            if score_sum <= f64::EPSILON {
                return Ok(MultiProviderDecision::Resolved(split_equal_fallback()));
            }
            Ok(MultiProviderDecision::Resolved(MultiProviderResolution {
                allocations: scored
                    .iter()
                    .map(|candidate| (candidate.provider_idx, candidate.final_score / score_sum))
                    .collect(),
                resolution_strategy: CompiledProviderResolutionStrategy::SplitByEvidence,
                used_equal_fallback: false,
                volume_fallback_to_one_count: 0,
                geography_tier: None,
            }))
        }
    }
}

fn split_by_process_volume(
    exchange: &ParsedExchange,
    providers: &[i32],
    process_meta: &[ProcessMeta],
) -> anyhow::Result<MultiProviderResolution> {
    let supply_region_anchor = supply_region_anchor_for_exchange(exchange, process_meta)?;
    let providers =
        same_model_preferred_provider_indices(exchange.process_idx, providers, process_meta)?;
    let mut tiered_providers = Vec::<(i32, CompiledProviderGeographyTier)>::new();
    for provider_idx in &providers {
        let provider = process_meta_for_idx(process_meta, *provider_idx)
            .ok_or_else(|| anyhow::anyhow!("missing provider process meta idx={provider_idx}"))?;
        tiered_providers.push((
            *provider_idx,
            provider_geography_tier(
                supply_region_anchor.location.as_deref(),
                provider.location.as_deref(),
            ),
        ));
    }

    let selected_tier = tiered_providers
        .iter()
        .map(|(_, tier)| *tier)
        .min_by_key(|tier| provider_geography_tier_rank(*tier))
        .ok_or_else(|| anyhow::anyhow!("provider candidates cannot be empty"))?;
    let selected_providers = tiered_providers
        .into_iter()
        .filter_map(|(provider_idx, tier)| (tier == selected_tier).then_some(provider_idx))
        .collect::<Vec<_>>();
    let mut raw_weights = Vec::<(i32, f64, bool)>::with_capacity(selected_providers.len());
    for provider_idx in selected_providers {
        let provider = process_meta_for_idx(process_meta, provider_idx)
            .ok_or_else(|| anyhow::anyhow!("missing provider process meta idx={provider_idx}"))?;
        let (raw_weight, used_fallback_to_one) = provider_volume_raw_weight(provider);
        raw_weights.push((provider_idx, raw_weight, used_fallback_to_one));
    }
    let weight_sum = raw_weights
        .iter()
        .map(|(_, raw_weight, _)| *raw_weight)
        .sum::<f64>();
    if weight_sum <= f64::EPSILON || !weight_sum.is_finite() {
        return Err(anyhow::anyhow!(
            "process volume provider weight sum must be positive"
        ));
    }
    let volume_fallback_to_one_count = i32::try_from(
        raw_weights
            .iter()
            .filter(|(_, _, used_fallback_to_one)| *used_fallback_to_one)
            .count(),
    )
    .map_err(|_| anyhow::anyhow!("volume fallback count overflow"))?;

    Ok(MultiProviderResolution {
        allocations: raw_weights
            .into_iter()
            .map(|(provider_idx, raw_weight, _)| (provider_idx, raw_weight / weight_sum))
            .collect(),
        resolution_strategy: CompiledProviderResolutionStrategy::SplitByProcessVolume,
        used_equal_fallback: false,
        volume_fallback_to_one_count,
        geography_tier: Some(selected_tier),
    })
}

fn provider_volume_raw_weight(provider: &ProcessMeta) -> (f64, bool) {
    match provider.annual_supply_or_production_volume {
        Some(value) if value.is_finite() && value > 0.0 => (value, false),
        _ => (1.0, true),
    }
}

fn same_model_preferred_provider_indices(
    consumer_idx: i32,
    providers: &[i32],
    process_meta: &[ProcessMeta],
) -> anyhow::Result<Vec<i32>> {
    let consumer = process_meta_for_idx(process_meta, consumer_idx)
        .ok_or_else(|| anyhow::anyhow!("missing consumer process meta idx={consumer_idx}"))?;
    let Some(consumer_model_id) = consumer.model_id else {
        return Ok(providers.to_vec());
    };

    let mut same_model = Vec::<i32>::new();
    for provider_idx in providers {
        let provider = process_meta_for_idx(process_meta, *provider_idx)
            .ok_or_else(|| anyhow::anyhow!("missing provider process meta idx={provider_idx}"))?;
        if provider.model_id == Some(consumer_model_id) {
            same_model.push(*provider_idx);
        }
    }

    if same_model.is_empty() {
        Ok(providers.to_vec())
    } else {
        Ok(same_model)
    }
}

fn score_provider_candidates(
    consumer_idx: i32,
    exchange_location: Option<&str>,
    providers: &[i32],
    process_meta: &[ProcessMeta],
) -> anyhow::Result<Vec<ProviderCandidateScore>> {
    let consumer = process_meta_for_idx(process_meta, consumer_idx)
        .ok_or_else(|| anyhow::anyhow!("missing consumer process meta idx={consumer_idx}"))?;
    let supply_region_anchor =
        resolve_supply_region_anchor(exchange_location, consumer.location.as_deref());
    let candidate_indices =
        same_model_preferred_provider_indices(consumer_idx, providers, process_meta)?;

    let mut scored = Vec::with_capacity(providers.len());
    for provider_idx in &candidate_indices {
        let provider = process_meta_for_idx(process_meta, *provider_idx)
            .ok_or_else(|| anyhow::anyhow!("missing provider process meta idx={provider_idx}"))?;
        let geo = geo_score(
            supply_region_anchor.location.as_deref(),
            provider.location.as_deref(),
        );
        let time = time_score(consumer.reference_year, provider.reference_year);
        let final_score = AUTO_LINK_GEO_WEIGHT * geo + AUTO_LINK_TIME_WEIGHT * time;
        scored.push(ProviderCandidateScore {
            provider_idx: *provider_idx,
            provider_id: provider.process_id,
            geo_score: geo,
            time_score: time,
            final_score,
        });
    }

    scored.sort_by(|left, right| {
        right
            .final_score
            .total_cmp(&left.final_score)
            .then_with(|| right.geo_score.total_cmp(&left.geo_score))
            .then_with(|| right.time_score.total_cmp(&left.time_score))
            .then_with(|| left.provider_id.cmp(&right.provider_id))
    });
    Ok(scored)
}

fn process_meta_for_idx(process_meta: &[ProcessMeta], process_idx: i32) -> Option<&ProcessMeta> {
    usize::try_from(process_idx)
        .ok()
        .and_then(|idx| process_meta.get(idx))
}

fn provider_candidates_for_outputs(
    providers: Option<&Vec<ProviderOutputCandidate>>,
    process_meta: &[ProcessMeta],
) -> anyhow::Result<Vec<CompiledProviderCandidate>> {
    let Some(providers) = providers else {
        return Ok(Vec::new());
    };
    providers
        .iter()
        .map(|provider| {
            let meta =
                process_meta_for_idx(process_meta, provider.provider_idx).ok_or_else(|| {
                    anyhow::anyhow!(
                        "missing provider process meta idx={}",
                        provider.provider_idx
                    )
                })?;
            Ok(CompiledProviderCandidate {
                provider_idx: provider.provider_idx,
                provider_id: meta.process_id,
                output_exchange_internal_id: provider.output_exchange_internal_id.clone(),
                output_exchange_is_reference: provider.output_exchange_is_reference,
                output_normalized_amount: provider.output_normalized_amount,
                output_allocation_state: compiled_allocation_state(
                    provider.output_allocation_state,
                ),
                eligibility: provider_candidate_eligibility(provider),
                process_name: meta.process_name.clone(),
                location: meta.location.clone(),
                reference_year: meta.reference_year,
                annual_supply_or_production_volume: meta.annual_supply_or_production_volume,
            })
        })
        .collect()
}

fn compiled_process_for_idx(
    processes: &[CompiledProcess],
    process_idx: i32,
) -> Option<&CompiledProcess> {
    usize::try_from(process_idx)
        .ok()
        .and_then(|idx| processes.get(idx))
}

#[derive(Debug, Clone)]
struct LocationDescriptor {
    canonical: Option<String>,
    country_code: Option<String>,
    region_group: Option<&'static str>,
    is_subnational: bool,
    is_global: bool,
}

fn geo_score(consumer_location: Option<&str>, provider_location: Option<&str>) -> f64 {
    let consumer = parse_location_descriptor(consumer_location);
    let provider = parse_location_descriptor(provider_location);
    if consumer.is_subnational
        && provider.is_subnational
        && consumer.canonical.is_some()
        && consumer.canonical == provider.canonical
    {
        return 1.0;
    }
    if consumer.country_code.is_some() && consumer.country_code == provider.country_code {
        return 0.85;
    }
    if consumer.region_group.is_some() && consumer.region_group == provider.region_group {
        return 0.6;
    }
    if provider.is_global {
        return 0.4;
    }
    0.1
}

fn provider_geography_tier(
    consumer_location: Option<&str>,
    provider_location: Option<&str>,
) -> CompiledProviderGeographyTier {
    let consumer = parse_location_descriptor(consumer_location);
    let provider = parse_location_descriptor(provider_location);
    if consumer.is_subnational
        && provider.is_subnational
        && consumer.canonical.is_some()
        && consumer.canonical == provider.canonical
    {
        return CompiledProviderGeographyTier::LocalSubnational;
    }
    if consumer.country_code.is_some() && consumer.country_code == provider.country_code {
        return CompiledProviderGeographyTier::SameCountry;
    }
    if !provider.is_global
        && consumer.region_group.is_some()
        && consumer.region_group == provider.region_group
    {
        return CompiledProviderGeographyTier::SameRegion;
    }
    if provider.is_global {
        return CompiledProviderGeographyTier::Global;
    }
    CompiledProviderGeographyTier::Other
}

fn provider_geography_tier_rank(tier: CompiledProviderGeographyTier) -> u8 {
    match tier {
        CompiledProviderGeographyTier::LocalSubnational => 0,
        CompiledProviderGeographyTier::SameCountry => 1,
        CompiledProviderGeographyTier::SameRegion => 2,
        CompiledProviderGeographyTier::Global => 3,
        CompiledProviderGeographyTier::Other => 4,
    }
}

fn best_geography_tier_for_allocations(
    supply_region_location: Option<&str>,
    allocations: &[(i32, f64)],
    process_meta: &[ProcessMeta],
) -> anyhow::Result<Option<CompiledProviderGeographyTier>> {
    let mut best_tier = None;
    for (provider_idx, _) in allocations {
        let provider = process_meta_for_idx(process_meta, *provider_idx)
            .ok_or_else(|| anyhow::anyhow!("missing provider process meta idx={provider_idx}"))?;
        let tier = provider_geography_tier(supply_region_location, provider.location.as_deref());
        let should_replace = match best_tier {
            Some(current) => {
                provider_geography_tier_rank(tier) < provider_geography_tier_rank(current)
            }
            None => true,
        };
        if should_replace {
            best_tier = Some(tier);
        }
    }
    Ok(best_tier)
}

fn time_score(consumer_year: Option<i32>, provider_year: Option<i32>) -> f64 {
    match (consumer_year, provider_year) {
        (Some(left), Some(right)) => {
            let diff = (left - right).abs();
            if diff <= 1 {
                1.0
            } else if diff <= 3 {
                0.85
            } else if diff <= 5 {
                0.65
            } else if diff <= 10 {
                0.4
            } else {
                0.2
            }
        }
        _ => 0.5,
    }
}

fn parse_location_descriptor(location: Option<&str>) -> LocationDescriptor {
    let canonical = location
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_uppercase().replace('_', "-"));
    let Some(canonical) = canonical else {
        return LocationDescriptor {
            canonical: None,
            country_code: None,
            region_group: None,
            is_subnational: false,
            is_global: false,
        };
    };

    let is_global = canonical == "GLO" || canonical == "ROW";
    let country_code = extract_country_code(&canonical);
    let is_subnational = canonical.contains('-') && country_code.is_some();
    let region_group = region_group_from_code(&canonical)
        .or_else(|| country_code.as_deref().and_then(region_group_from_code));
    LocationDescriptor {
        canonical: Some(canonical),
        country_code,
        region_group,
        is_subnational,
        is_global,
    }
}

fn extract_country_code(location: &str) -> Option<String> {
    if location == "GLO" {
        return None;
    }
    let token = location
        .split(['-', '_', ' '])
        .next()
        .map(str::trim)
        .unwrap_or_default();
    if token.len() == 2 && token.chars().all(|chr| chr.is_ascii_alphabetic()) {
        Some(token.to_owned())
    } else {
        None
    }
}

fn region_group_from_code(code: &str) -> Option<&'static str> {
    match code {
        "GLO" | "ROW" => Some("GLOBAL"),
        "RER" | "EU" | "EU27" | "EU28" | "EFTA" | "WEU" | "EEU" | "AT" | "BE" | "BG" | "CH"
        | "CY" | "CZ" | "DE" | "DK" | "EE" | "ES" | "FI" | "FR" | "GB" | "GR" | "HR" | "HU"
        | "IE" | "IS" | "IT" | "LI" | "LT" | "LU" | "LV" | "MT" | "NL" | "NO" | "PL" | "PT"
        | "RO" | "SE" | "SI" | "SK" => Some("EUROPE"),
        "APAC" | "RAS" | "SAS" | "EAS" | "OCE" | "CN" | "JP" | "KR" | "IN" | "AU" | "NZ" | "ID"
        | "TH" | "VN" | "MY" | "SG" | "PH" | "PK" | "BD" => Some("APAC"),
        "RNA" | "NAM" | "US" | "CA" | "MX" => Some("NORTH_AMERICA"),
        "RLA" | "LATAM" | "BR" | "AR" | "CL" | "CO" | "PE" | "UY" | "PY" | "BO" | "EC" | "VE"
        | "CR" | "GT" | "HN" | "NI" | "SV" | "PA" | "DO" | "CU" => Some("LATAM"),
        "RAF" | "AFR" | "ZA" | "EG" | "NG" | "KE" | "GH" | "DZ" | "MA" | "TN" | "ET" | "TZ"
        | "UG" => Some("AFRICA"),
        "RME" | "MEA" | "AE" | "SA" | "QA" | "KW" | "OM" | "BH" | "IL" | "TR" | "IR" | "IQ"
        | "JO" | "LB" => Some("MIDDLE_EAST"),
        _ => None,
    }
}

fn pct(numerator: i64, denominator: i64) -> f64 {
    if denominator <= 0 {
        0.0
    } else {
        ((numerator as f64 / denominator as f64) * 10000.0).round() / 100.0
    }
}

fn summarize_provider_decision_diagnostics(
    decisions: &[CompiledProviderDecision],
) -> SnapshotProviderDecisionDiagnostics {
    let mut resolved_strategy_counts = BTreeMap::<String, i64>::new();
    let mut unresolved_reason_counts = BTreeMap::<String, i64>::new();
    let mut candidate_eligibility_counts = BTreeMap::<String, i64>::new();
    let mut geography_tier_counts = BTreeMap::<String, i64>::new();
    let mut supply_region_source_counts = BTreeMap::<String, i64>::new();
    let mut rejected_non_reference_output_count = 0_i64;
    let mut volume_fallback_to_one_count = 0_i64;

    for decision in decisions {
        if let Some(strategy) = decision.resolution_strategy {
            *resolved_strategy_counts
                .entry(provider_resolution_strategy_label(strategy).to_owned())
                .or_insert(0) += 1;
        }
        if let Some(reason) = decision.failure_reason {
            *unresolved_reason_counts
                .entry(provider_failure_reason_label(reason).to_owned())
                .or_insert(0) += 1;
        }
        if let Some(tier) = decision.geography_tier {
            *geography_tier_counts
                .entry(provider_geography_tier_label(tier).to_owned())
                .or_insert(0) += 1;
        }
        if let Some(source) = decision.supply_region_source {
            *supply_region_source_counts
                .entry(provider_supply_region_source_label(source).to_owned())
                .or_insert(0) += 1;
        }
        for candidate in &decision.candidates {
            let eligibility =
                provider_candidate_eligibility_label(candidate.eligibility).to_owned();
            *candidate_eligibility_counts.entry(eligibility).or_insert(0) += 1;
            if candidate.eligibility
                == CompiledProviderCandidateEligibility::RejectedNonReferenceOutput
            {
                rejected_non_reference_output_count += 1;
            }
        }
        volume_fallback_to_one_count += i64::from(decision.volume_fallback_to_one_count);
    }

    SnapshotProviderDecisionDiagnostics {
        resolved_strategy_counts,
        unresolved_reason_counts,
        candidate_eligibility_counts,
        rejected_non_reference_output_count,
        volume_fallback_to_one_count,
        geography_tier_counts,
        supply_region_source_counts,
    }
}

fn summarize_matching_diagnostics(compiled_graph: &CompiledGraph) -> MatchingDiagnosticsSummary {
    let provider_decision_diagnostics =
        summarize_provider_decision_diagnostics(&compiled_graph.provider_decisions);
    let mut candidate_count_histogram = BTreeMap::<String, i64>::new();
    let mut tier_counts_by_strategy = BTreeMap::<String, BTreeMap<String, i64>>::new();
    let mut supply_region_source_counts_by_strategy =
        BTreeMap::<String, BTreeMap<String, i64>>::new();
    let mut requested_location_granularity_counts = BTreeMap::<String, i64>::new();
    let mut requested_location_granularity_counts_by_strategy =
        BTreeMap::<String, BTreeMap<String, i64>>::new();
    let mut exchange_location_present_count = 0_i64;
    let mut exchange_location_present_count_by_strategy = BTreeMap::<String, i64>::new();
    let mut unmatched_flow_counts = BTreeMap::<Uuid, i64>::new();
    let mut process_gap_counts = BTreeMap::<i32, ProcessGapAccumulator>::new();
    let mut volume_weight_summary = SnapshotVolumeWeightSummary::default();

    for decision in &compiled_graph.provider_decisions {
        *candidate_count_histogram
            .entry(candidate_count_bucket_label(decision.candidate_provider_count).to_owned())
            .or_insert(0) += 1;

        if decision.exchange_location_present {
            exchange_location_present_count += 1;
        }
        *requested_location_granularity_counts
            .entry(
                location_granularity_label(decision.supply_region_location.as_deref()).to_owned(),
            )
            .or_insert(0) += 1;

        if let Some(strategy) = decision.resolution_strategy {
            let strategy_label = provider_resolution_strategy_label(strategy).to_owned();
            if let Some(tier) = decision.geography_tier {
                let tier = provider_geography_tier_label(tier).to_owned();
                *tier_counts_by_strategy
                    .entry(strategy_label.clone())
                    .or_default()
                    .entry(tier)
                    .or_insert(0) += 1;
            }
            if let Some(source) = decision.supply_region_source {
                let source = provider_supply_region_source_label(source).to_owned();
                *supply_region_source_counts_by_strategy
                    .entry(strategy_label.clone())
                    .or_default()
                    .entry(source)
                    .or_insert(0) += 1;
            }
            if decision.exchange_location_present {
                *exchange_location_present_count_by_strategy
                    .entry(strategy_label.clone())
                    .or_insert(0) += 1;
            }
            let granularity =
                location_granularity_label(decision.supply_region_location.as_deref()).to_owned();
            *requested_location_granularity_counts_by_strategy
                .entry(strategy_label)
                .or_default()
                .entry(granularity)
                .or_insert(0) += 1;
        }

        if decision.resolution_strategy
            == Some(CompiledProviderResolutionStrategy::SplitByProcessVolume)
        {
            volume_weight_summary.decisions_total += 1;
            let candidate_count = i64::try_from(decision.allocations.len()).unwrap_or(i64::MAX);
            let fallback_count = i64::from(decision.volume_fallback_to_one_count);
            volume_weight_summary.candidate_total += candidate_count;
            volume_weight_summary.fallback_to_one_count += fallback_count;
            if fallback_count == 0 {
                volume_weight_summary.decisions_all_valid_count += 1;
            } else if fallback_count >= candidate_count {
                volume_weight_summary.decisions_all_missing_count += 1;
            } else {
                volume_weight_summary.decisions_partial_missing_count += 1;
            }
        }

        let process_gap = process_gap_counts.entry(decision.consumer_idx).or_default();
        process_gap.input_edges_total += 1;
        if decision.matched_provider_count > 0 {
            process_gap.a_input_edges_written += 1;
        }
        if decision.decision_kind == Some(CompiledProviderDecisionKind::NoProvider) {
            process_gap.unmatched_no_provider += 1;
            *unmatched_flow_counts.entry(decision.flow_id).or_insert(0) += 1;
        }
    }

    volume_weight_summary.valid_volume_count = (volume_weight_summary.candidate_total
        - volume_weight_summary.fallback_to_one_count)
        .max(0);

    MatchingDiagnosticsSummary {
        candidate_summary: SnapshotCandidateSummary {
            candidate_count_histogram,
        },
        resolution_summary: SnapshotResolutionSummary {
            resolved_strategy_counts: provider_decision_diagnostics
                .resolved_strategy_counts
                .clone(),
            unresolved_reason_counts: provider_decision_diagnostics
                .unresolved_reason_counts
                .clone(),
        },
        geography_summary: SnapshotGeographySummary {
            tier_counts: provider_decision_diagnostics.geography_tier_counts.clone(),
            tier_counts_by_strategy,
            supply_region_source_counts: provider_decision_diagnostics
                .supply_region_source_counts
                .clone(),
            supply_region_source_counts_by_strategy,
            exchange_location_present_count,
            exchange_location_present_count_by_strategy,
            requested_location_granularity_counts,
            requested_location_granularity_counts_by_strategy,
        },
        volume_weight_summary,
        gap_summary: SnapshotGapSummary {
            unmatched_top_flows: top_unmatched_flows(unmatched_flow_counts),
            process_gap_top: top_process_gaps(process_gap_counts, &compiled_graph.processes),
        },
        provider_decision_diagnostics,
    }
}

fn candidate_count_bucket_label(candidate_count: i32) -> &'static str {
    match candidate_count {
        i32::MIN..=0 => "zero",
        1 => "one",
        2..=5 => "two_to_five",
        6..=20 => "six_to_twenty",
        _ => "gt_twenty",
    }
}

fn location_granularity_label(location: Option<&str>) -> &'static str {
    let Some(location) = location.map(str::trim).filter(|value| !value.is_empty()) else {
        return "unspecified";
    };
    let descriptor = parse_location_descriptor(Some(location));
    if descriptor.is_global {
        "global"
    } else if descriptor.is_subnational {
        "subnational"
    } else if descriptor.country_code.is_some() {
        "country"
    } else if descriptor.region_group.is_some() {
        "region"
    } else {
        "unknown"
    }
}

fn top_unmatched_flows(
    unmatched_flow_counts: BTreeMap<Uuid, i64>,
) -> Vec<SnapshotUnmatchedFlowEntry> {
    let mut entries = unmatched_flow_counts
        .into_iter()
        .map(|(flow_id, count)| SnapshotUnmatchedFlowEntry {
            flow_id,
            flow_name: None,
            count,
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.flow_id.cmp(&right.flow_id))
    });
    entries.truncate(GAP_TOP_K_LIMIT);
    entries
}

fn top_process_gaps(
    process_gap_counts: BTreeMap<i32, ProcessGapAccumulator>,
    processes: &[CompiledProcess],
) -> Vec<SnapshotProcessGapEntry> {
    let mut entries = process_gap_counts
        .into_iter()
        .filter_map(|(process_idx, counts)| {
            if counts.unmatched_no_provider == 0 {
                return None;
            }
            let process = compiled_process_for_idx(processes, process_idx)?;
            Some(SnapshotProcessGapEntry {
                process_id: process.process_id,
                process_name: process.process_name.clone(),
                input_edges_total: counts.input_edges_total,
                unmatched_no_provider: counts.unmatched_no_provider,
                a_write_pct: pct(counts.a_input_edges_written, counts.input_edges_total),
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        right
            .unmatched_no_provider
            .cmp(&left.unmatched_no_provider)
            .then_with(|| right.input_edges_total.cmp(&left.input_edges_total))
            .then_with(|| left.process_id.cmp(&right.process_id))
    });
    entries.truncate(GAP_TOP_K_LIMIT);
    entries
}

fn provider_resolution_strategy_label(
    strategy: CompiledProviderResolutionStrategy,
) -> &'static str {
    match strategy {
        CompiledProviderResolutionStrategy::UniqueProvider => "unique_provider",
        CompiledProviderResolutionStrategy::BestProviderStrict => "best_provider_strict",
        CompiledProviderResolutionStrategy::SplitByEvidence => "split_by_evidence",
        CompiledProviderResolutionStrategy::SplitByProcessVolume => "split_by_process_volume",
        CompiledProviderResolutionStrategy::SplitEqual => "split_equal",
        CompiledProviderResolutionStrategy::SplitEqualFallback => "split_equal_fallback",
    }
}

fn provider_geography_tier_label(tier: CompiledProviderGeographyTier) -> &'static str {
    match tier {
        CompiledProviderGeographyTier::LocalSubnational => "local_subnational",
        CompiledProviderGeographyTier::SameCountry => "same_country",
        CompiledProviderGeographyTier::SameRegion => "same_region",
        CompiledProviderGeographyTier::Global => "global",
        CompiledProviderGeographyTier::Other => "other",
    }
}

fn provider_supply_region_source_label(source: CompiledProviderSupplyRegionSource) -> &'static str {
    match source {
        CompiledProviderSupplyRegionSource::ExchangeLocation => "exchange_location",
        CompiledProviderSupplyRegionSource::ConsumerProcessLocation => "consumer_process_location",
        CompiledProviderSupplyRegionSource::Unspecified => "unspecified",
    }
}

fn provider_failure_reason_label(reason: CompiledProviderFailureReason) -> &'static str {
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
}

fn provider_candidate_eligibility_label(
    eligibility: CompiledProviderCandidateEligibility,
) -> &'static str {
    match eligibility {
        CompiledProviderCandidateEligibility::Unknown => "unknown",
        CompiledProviderCandidateEligibility::AcceptedReferenceOutput => {
            "accepted_reference_output"
        }
        CompiledProviderCandidateEligibility::RejectedNonReferenceOutput => {
            "rejected_non_reference_output"
        }
    }
}

fn biosphere_gross_value(amount: f64) -> f64 {
    amount
}

fn normalize_request_roots(roots: &[RequestRootProcess]) -> Vec<RequestRootProcess> {
    let mut normalized = roots
        .iter()
        .map(|root| RequestRootProcess::new(root.process_id, root.process_version.clone()))
        .collect::<Vec<_>>();
    normalized.sort_unstable();
    normalized.dedup();
    normalized
}

fn compute_scope_hash(
    all_states: bool,
    state_codes: &[i32],
    include_user_id: Option<Uuid>,
    request_roots: &[RequestRootProcess],
    process_limit: usize,
    provider_rule: ProviderRule,
) -> anyhow::Result<String> {
    let selection_mode = if request_roots.is_empty() {
        SnapshotSelectionMode::FilteredLibrary
    } else {
        SnapshotSelectionMode::RequestRootsClosure
    };
    let body = serde_json::json!({
        "schema": "request-scope:v1",
        "selection_mode": selection_mode,
        "all_states": all_states,
        "process_states": state_codes,
        "include_user_id": include_user_id,
        "request_roots": request_roots,
        "process_limit": process_limit,
        "provider_rule": provider_rule.as_str(),
    });
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&body)?);
    Ok(hex::encode(hasher.finalize()))
}

fn classify_scope_partition(
    row: &ProcessRow,
    include_user_id: Option<Uuid>,
) -> ScopeProcessPartition {
    if row.state_code != 100 && include_user_id.is_some() && row.user_id == include_user_id {
        ScopeProcessPartition::Private
    } else {
        ScopeProcessPartition::Public
    }
}

fn resolve_process_selection(
    mut candidate_processes: Vec<ProcessRow>,
    all_states: bool,
    state_codes: &[i32],
    include_user_id: Option<Uuid>,
    request_roots: &[RequestRootProcess],
    provider_rule: ProviderRule,
    process_limit: usize,
) -> anyhow::Result<ResolvedProcessSelection> {
    if request_roots.is_empty() {
        if process_limit > 0 && candidate_processes.len() > process_limit {
            candidate_processes.truncate(process_limit);
        }
        let processes = candidate_processes
            .iter()
            .map(|row| ResolvedScopeProcess {
                process_id: row.id,
                process_version: row.version.clone(),
                partition: classify_scope_partition(row, include_user_id),
            })
            .collect::<Vec<_>>();
        let public_process_count = i64::try_from(
            processes
                .iter()
                .filter(|row| row.partition == ScopeProcessPartition::Public)
                .count(),
        )
        .map_err(|_| anyhow::anyhow!("public process count overflow"))?;
        let private_process_count = i64::try_from(
            processes
                .iter()
                .filter(|row| row.partition == ScopeProcessPartition::Private)
                .count(),
        )
        .map_err(|_| anyhow::anyhow!("private process count overflow"))?;
        let scope_hash = compute_scope_hash(
            all_states,
            state_codes,
            include_user_id,
            request_roots,
            process_limit,
            provider_rule,
        )?;
        return Ok(ResolvedProcessSelection {
            processes: candidate_processes,
            scope_summary: ResolvedRequestScopeSummary {
                selection_mode: SnapshotSelectionMode::FilteredLibrary,
                scope_hash,
                roots: Vec::new(),
                public_process_count,
                private_process_count,
                processes,
            },
        });
    }

    let processes = candidate_processes;
    let mut process_meta = Vec::with_capacity(processes.len());
    let mut input_exchanges_by_idx = Vec::<Vec<ParsedExchange>>::with_capacity(processes.len());
    let mut provider_sets: ProviderMap = HashMap::new();
    let mut process_lookup = HashMap::<(Uuid, String), i32>::with_capacity(processes.len());

    for (idx, proc_row) in processes.iter().enumerate() {
        let process_idx =
            i32::try_from(idx).map_err(|_| anyhow::anyhow!("process index overflow"))?;
        process_lookup.insert((proc_row.id, proc_row.version.clone()), process_idx);
        let reference_internal_id = parse_reference_internal_id(&proc_row.json);
        process_meta.push(ProcessMeta {
            process_idx,
            process_id: proc_row.id,
            process_version: proc_row.version.clone(),
            process_name: parse_process_name(&proc_row.json),
            model_id: proc_row.model_id,
            location: parse_process_location(&proc_row.json),
            reference_year: parse_process_reference_year(&proc_row.json),
            annual_supply_or_production_volume: parse_process_annual_supply_or_production_volume(
                &proc_row.json,
            ),
        });

        let mut input_exchanges = Vec::new();
        for exchange in process_exchange_items(&proc_row.json) {
            let direction = match exchange
                .get("exchangeDirection")
                .and_then(Value::as_str)
                .unwrap_or_default()
            {
                "Input" => Some(ExchangeDirection::Input),
                "Output" => Some(ExchangeDirection::Output),
                _ => None,
            };
            let Some(direction) = direction else {
                continue;
            };
            let Some(flow_id) =
                parse_uuid_at(exchange, &["referenceToFlowDataSet", "@refObjectId"])
            else {
                continue;
            };
            let internal_id = parse_exchange_internal_id(exchange);

            if direction == ExchangeDirection::Output {
                provider_sets.entry(flow_id).or_default().push(
                    provider_output_candidate_from_exchange(
                        process_idx,
                        flow_id,
                        &ParsedExchange {
                            process_idx,
                            flow_id,
                            direction: Some(direction),
                            direction_label: direction.as_str().to_owned(),
                            internal_id: internal_id.clone(),
                            exchange_id: internal_id
                                .clone()
                                .unwrap_or_else(|| format!("scope:{}:{}", proc_row.id, flow_id)),
                            flow_version: "unknown".to_owned(),
                            is_reference_exchange: is_reference_internal_exchange(
                                internal_id.as_deref(),
                                reference_internal_id.as_deref(),
                            ),
                            amount: None,
                            allocation_state: AllocationFractionState::Missing,
                            location: parse_exchange_location(exchange),
                        },
                    ),
                );
            } else {
                input_exchanges.push(ParsedExchange {
                    process_idx,
                    flow_id,
                    direction: Some(direction),
                    direction_label: direction.as_str().to_owned(),
                    internal_id: internal_id.clone(),
                    exchange_id: internal_id
                        .clone()
                        .unwrap_or_else(|| format!("scope:{}:{}", proc_row.id, flow_id)),
                    flow_version: "unknown".to_owned(),
                    is_reference_exchange: is_reference_internal_exchange(
                        internal_id.as_deref(),
                        reference_internal_id.as_deref(),
                    ),
                    amount: None,
                    allocation_state: AllocationFractionState::Missing,
                    location: parse_exchange_location(exchange),
                });
            }
        }
        input_exchanges_by_idx.push(input_exchanges);
    }

    let mut provider_map = provider_sets;
    for providers in provider_map.values_mut() {
        sort_provider_output_candidates(providers, &process_meta);
    }

    let normalized_roots = normalize_request_roots(request_roots);
    let mut selected = HashSet::<i32>::new();
    let mut queue = Vec::<i32>::new();
    for root in &normalized_roots {
        let key = (root.process_id, root.process_version.clone());
        let process_idx = process_lookup
            .get(&key)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("request root not found in candidate scope: {root}"))?;
        if selected.insert(process_idx) {
            queue.push(process_idx);
        }
    }

    let mut cursor = 0usize;
    while cursor < queue.len() {
        let current = queue[cursor];
        cursor += 1;
        let input_exchanges = input_exchanges_by_idx
            .get(usize::try_from(current).map_err(|_| anyhow::anyhow!("negative process idx"))?)
            .ok_or_else(|| anyhow::anyhow!("missing input exchanges for process idx={current}"))?;
        for exchange in input_exchanges {
            let provider_outputs = provider_map.get(&exchange.flow_id);
            let providers = eligible_provider_indices(provider_outputs);
            let provider_cnt = providers.len();
            let next_indices = if provider_cnt == 1 {
                providers
            } else if provider_cnt > 1 {
                match resolve_multi_provider(provider_rule, exchange, &providers, &process_meta)? {
                    MultiProviderDecision::Resolved(resolution) => resolution
                        .allocations
                        .into_iter()
                        .map(|(provider_idx, _weight)| provider_idx)
                        .collect::<Vec<_>>(),
                    MultiProviderDecision::Unresolved(_) => Vec::new(),
                }
            } else {
                Vec::new()
            };
            for provider_idx in next_indices {
                if selected.insert(provider_idx) {
                    queue.push(provider_idx);
                }
            }
        }
    }

    let mut selected_indices = selected.into_iter().collect::<Vec<_>>();
    selected_indices.sort_unstable();
    let mut resolved_processes = Vec::with_capacity(selected_indices.len());
    let mut selected_rows = Vec::with_capacity(selected_indices.len());
    for idx in selected_indices {
        let row = processes
            .get(usize::try_from(idx).map_err(|_| anyhow::anyhow!("negative process idx"))?)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("selected process idx out of bounds: {idx}"))?;
        resolved_processes.push(ResolvedScopeProcess {
            process_id: row.id,
            process_version: row.version.clone(),
            partition: classify_scope_partition(&row, include_user_id),
        });
        selected_rows.push(row);
    }
    let public_process_count = i64::try_from(
        resolved_processes
            .iter()
            .filter(|row| row.partition == ScopeProcessPartition::Public)
            .count(),
    )
    .map_err(|_| anyhow::anyhow!("public process count overflow"))?;
    let private_process_count = i64::try_from(
        resolved_processes
            .iter()
            .filter(|row| row.partition == ScopeProcessPartition::Private)
            .count(),
    )
    .map_err(|_| anyhow::anyhow!("private process count overflow"))?;
    let scope_hash = compute_scope_hash(
        all_states,
        state_codes,
        include_user_id,
        &normalized_roots,
        0,
        provider_rule,
    )?;

    Ok(ResolvedProcessSelection {
        processes: selected_rows,
        scope_summary: ResolvedRequestScopeSummary {
            selection_mode: SnapshotSelectionMode::RequestRootsClosure,
            scope_hash,
            roots: normalized_roots,
            public_process_count,
            private_process_count,
            processes: resolved_processes,
        },
    })
}

async fn compute_source_fingerprint(
    pool: &PgPool,
    selected_processes: &[ProcessRow],
    config: &SnapshotBuildConfig,
    versioned_scope: Option<&ValidatedPublicOwnerDraftScope>,
    method: Option<&MethodSelection>,
) -> anyhow::Result<(SourceSnapshotSummary, String)> {
    let (process_count, process_max_modified_at_utc) =
        summarize_selected_processes(selected_processes)?;
    let (flow_count, flow_max_modified_at_utc) =
        fetch_flow_source_summary(pool, versioned_scope).await?;
    let (lciamethod_count, lciamethod_max_modified_at_utc) = if config.has_lcia {
        if versioned_scope.is_some() {
            let method =
                method.ok_or_else(|| anyhow::anyhow!("missing selected LCIA method snapshot"))?;
            let evidence = method
                .source_evidence
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing static LCIA method source evidence"))?;
            (
                i64::try_from(evidence.method_count)
                    .map_err(|_| anyhow::anyhow!("LCIA method count overflow"))?,
                format!("static:{}", evidence.source_snapshot_sha256),
            )
        } else {
            fetch_lciamethod_source_summary(pool).await?
        }
    } else {
        (0, "disabled".to_owned())
    };

    let summary = SourceSnapshotSummary {
        process_count,
        process_max_modified_at_utc,
        flow_count,
        flow_max_modified_at_utc,
        lciamethod_count,
        lciamethod_max_modified_at_utc,
    };

    let body = serde_json::json!({
        "schema": "source-fingerprint:v1",
        "source": {
            "processes": {
                "count": summary.process_count,
                "max_modified_at_utc": summary.process_max_modified_at_utc,
            },
            "flows": {
                "count": summary.flow_count,
                "max_modified_at_utc": summary.flow_max_modified_at_utc,
            },
            "lciamethods": {
                "count": summary.lciamethod_count,
                "max_modified_at_utc": summary.lciamethod_max_modified_at_utc,
            }
        },
        "config": config,
    });

    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&body)?);
    let fingerprint = hex::encode(hasher.finalize());
    Ok((summary, fingerprint))
}

fn summarize_selected_processes(processes: &[ProcessRow]) -> anyhow::Result<(i64, String)> {
    let process_count =
        i64::try_from(processes.len()).map_err(|_| anyhow::anyhow!("process count overflow"))?;
    let max_modified_at = processes.iter().filter_map(|row| row.modified_at).max();
    Ok((process_count, format_modified_at_utc(max_modified_at)))
}

fn format_modified_at_utc(timestamp: Option<DateTime<Utc>>) -> String {
    timestamp.map_or_else(
        || "none".to_owned(),
        |value| value.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
    )
}

async fn fetch_flow_source_summary(
    pool: &PgPool,
    versioned_scope: Option<&ValidatedPublicOwnerDraftScope>,
) -> anyhow::Result<(i64, String)> {
    let row = if let Some(scope) = versioned_scope {
        sqlx::query(
            r#"
            SELECT
              COUNT(*)::bigint AS flow_count,
              COALESCE(
                to_char(MAX(modified_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                'none'
              ) AS flow_max_modified_at_utc
            FROM public.flows
            WHERE state_code = 100
               OR (
                 user_id = $1
                 AND state_code = 0
                 AND team_id IS NULL
                 AND review_id IS NULL
               )
            "#,
        )
        .bind(scope.actor_user_id)
        .fetch_one(pool)
        .await?
    } else {
        sqlx::query(
            r#"
        SELECT
          COUNT(*)::bigint AS flow_count,
          COALESCE(
            to_char(MAX(modified_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'none'
          ) AS flow_max_modified_at_utc
        FROM public.flows
        "#,
        )
        .fetch_one(pool)
        .await?
    };

    Ok((
        row.try_get::<i64, _>("flow_count")?,
        row.try_get::<String, _>("flow_max_modified_at_utc")?,
    ))
}

async fn fetch_lciamethod_source_summary(pool: &PgPool) -> anyhow::Result<(i64, String)> {
    let row = sqlx::query(
        r#"
        SELECT
          COUNT(*)::bigint AS lciamethod_count,
          COALESCE(
            to_char(MAX(modified_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'none'
          ) AS lciamethod_max_modified_at_utc
        FROM public.lciamethods
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok((
        row.try_get::<i64, _>("lciamethod_count")?,
        row.try_get::<String, _>("lciamethod_max_modified_at_utc")?,
    ))
}

async fn find_reusable_snapshot(
    pool: &PgPool,
    source_fingerprint: &str,
    reuse_max_age_seconds: Option<i64>,
) -> anyhow::Result<Option<ReuseCandidate>> {
    find_reusable_snapshot_with_age_basis(pool, source_fingerprint, reuse_max_age_seconds, false)
        .await
}

async fn find_reusable_snapshot_with_age_basis(
    pool: &PgPool,
    source_fingerprint: &str,
    reuse_max_age_seconds: Option<i64>,
    use_updated_at_for_age: bool,
) -> anyhow::Result<Option<ReuseCandidate>> {
    let row = sqlx::query(
        r#"
        SELECT
          s.id AS snapshot_id,
          a.artifact_url,
          a.coverage,
          a.process_count::bigint AS process_count,
          a.flow_count::bigint AS flow_count,
          a.impact_count::bigint AS impact_count,
          a.a_nnz,
          a.b_nnz,
          a.c_nnz
        FROM public.lca_network_snapshots s
        INNER JOIN public.lca_snapshot_artifacts a
          ON a.snapshot_id = s.id
        WHERE s.status = 'ready'
          AND a.status = 'ready'
          AND a.artifact_format = $2
          AND s.source_hash = $1
          AND (
            $3::bigint IS NULL
            OR (
              CASE
                WHEN $4::boolean THEN GREATEST(a.created_at, a.updated_at)
                ELSE a.created_at
              END
            ) >= now() - ($3::bigint * interval '1 second')
          )
        ORDER BY
          CASE
            WHEN $4::boolean THEN GREATEST(a.created_at, a.updated_at)
            ELSE a.created_at
          END DESC
        LIMIT 1
        "#,
    )
    .bind(source_fingerprint)
    .bind(SNAPSHOT_ARTIFACT_FORMAT)
    .bind(reuse_max_age_seconds)
    .bind(use_updated_at_for_age)
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let coverage_value = row.try_get::<Option<Value>, _>("coverage")?;
    let Some(coverage_value) = coverage_value else {
        return Ok(None);
    };
    let coverage: SnapshotCoverageReport = serde_json::from_value(coverage_value)?;

    Ok(Some(ReuseCandidate {
        snapshot_id: row.try_get::<Uuid, _>("snapshot_id")?,
        artifact_url: row.try_get::<String, _>("artifact_url")?,
        coverage,
        process_count: row.try_get::<i64, _>("process_count")?,
        flow_count: row.try_get::<i64, _>("flow_count")?,
        impact_count: row.try_get::<i64, _>("impact_count")?,
        a_nnz: row.try_get::<i64, _>("a_nnz")?,
        b_nnz: row.try_get::<i64, _>("b_nnz")?,
        c_nnz: row.try_get::<i64, _>("c_nnz")?,
    }))
}

async fn touch_reused_snapshot_artifact(
    pool: &PgPool,
    snapshot_id: Uuid,
    artifact_format: &str,
    refreshed_ttl_seconds: Option<i64>,
) -> anyhow::Result<()> {
    let refreshed_expires_at = artifact_expires_at_utc(refreshed_ttl_seconds)?;
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        UPDATE public.lca_snapshot_artifacts
        SET updated_at = NOW()
        WHERE snapshot_id = $1
          AND artifact_format = $2
        "#,
    )
    .bind(snapshot_id)
    .bind(artifact_format)
    .execute(&mut *tx)
    .await?;

    if let Some(expires_at) = refreshed_expires_at {
        sqlx::query(
            r#"
            UPDATE public.lca_network_snapshots
            SET
              updated_at = NOW(),
              process_filter = jsonb_set(
                process_filter,
                '{artifact_lifecycle,expires_at_utc}',
                to_jsonb($2::text),
                true
              )
            WHERE id = $1
            "#,
        )
        .bind(snapshot_id)
        .bind(expires_at)
        .execute(&mut *tx)
        .await?;
    } else {
        sqlx::query(
            r#"
            UPDATE public.lca_network_snapshots
            SET updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(snapshot_id)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn fetch_processes(
    pool: &PgPool,
    all_states: bool,
    state_codes: &[i32],
    include_user_id: Option<Uuid>,
    versioned_scope: Option<&ValidatedPublicOwnerDraftScope>,
) -> anyhow::Result<Vec<ProcessRow>> {
    let query_started = Instant::now();
    let rows = if let Some(scope) = versioned_scope {
        sqlx::query(
            r#"
            SELECT DISTINCT ON (id)
              id, version, model_id, user_id, state_code, team_id, review_id, modified_at, json
            FROM public.processes
            WHERE (
                state_code = 100
                OR (
                    user_id = $1
                    AND state_code = 0
                    AND team_id IS NULL
                    AND review_id IS NULL
                )
              )
              AND json ? 'processDataSet'
            ORDER BY id, version DESC, modified_at DESC NULLS LAST
            "#,
        )
        .bind(scope.actor_user_id)
        .fetch_all(pool)
        .await?
    } else if all_states {
        sqlx::query(
            r#"
            SELECT DISTINCT ON (id)
              id, version, model_id, user_id, state_code, team_id, review_id, modified_at, json
            FROM public.processes
            WHERE json ? 'processDataSet'
            ORDER BY id, version DESC
            "#,
        )
        .fetch_all(pool)
        .await?
    } else if let Some(user_id) = include_user_id {
        sqlx::query(
            r#"
            SELECT DISTINCT ON (id)
              id, version, model_id, user_id, state_code, team_id, review_id, modified_at, json
            FROM public.processes
            WHERE (state_code = ANY($1) OR user_id = $2)
              AND json ? 'processDataSet'
            ORDER BY id, version DESC
            "#,
        )
        .bind(state_codes)
        .bind(user_id)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT DISTINCT ON (id)
              id, version, model_id, user_id, state_code, team_id, review_id, modified_at, json
            FROM public.processes
            WHERE state_code = ANY($1)
              AND json ? 'processDataSet'
            ORDER BY id, version DESC
            "#,
        )
        .bind(state_codes)
        .fetch_all(pool)
        .await?
    };

    let elapsed = query_started.elapsed();
    let level = if elapsed >= SLOW_QUERY_LOG_THRESHOLD {
        "warn"
    } else {
        "info"
    };
    println!(
        "[query] level={level} name=snapshot.fetch_processes all_states={all_states} state_code_count={} include_user_id={} rows={} elapsed_seconds={:.3}",
        state_codes.len(),
        include_user_id.is_some(),
        rows.len(),
        elapsed.as_secs_f64()
    );

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let process = ProcessRow {
            id: row.try_get::<Uuid, _>("id")?,
            version: row.try_get::<String, _>("version")?.trim().to_owned(),
            model_id: row.try_get::<Option<Uuid>, _>("model_id")?,
            user_id: row.try_get::<Option<Uuid>, _>("user_id")?,
            state_code: row.try_get::<i32, _>("state_code")?,
            team_id: row.try_get::<Option<Uuid>, _>("team_id")?,
            review_id: row.try_get::<Option<Uuid>, _>("review_id")?,
            modified_at: row.try_get::<Option<DateTime<Utc>>, _>("modified_at")?,
            json: row.try_get::<Value, _>("json")?,
        };
        if let Some(scope) = versioned_scope {
            validate_process_row_visibility(&process, scope.actor_user_id)?;
        }
        out.push(process);
    }
    Ok(out)
}

fn validate_process_row_visibility(row: &ProcessRow, actor_user_id: Uuid) -> anyhow::Result<()> {
    if row.state_code == 100
        || (row.state_code == 0
            && row.user_id == Some(actor_user_id)
            && row.team_id.is_none()
            && row.review_id.is_none())
    {
        return Ok(());
    }
    Err(anyhow::anyhow!(
        "process visibility recheck failed: {}@{} owner={:?} state_code={} team_id={:?} review_id={:?}",
        row.id,
        row.version,
        row.user_id,
        row.state_code,
        row.team_id,
        row.review_id
    ))
}

async fn fetch_flow_meta(
    pool: &PgPool,
    flow_candidates: &BTreeSet<Uuid>,
    versioned_scope: Option<&ValidatedPublicOwnerDraftScope>,
) -> anyhow::Result<HashMap<Uuid, FlowRow>> {
    if flow_candidates.is_empty() {
        return Ok(HashMap::new());
    }
    let query_started = Instant::now();
    let candidate_ids = flow_candidates.iter().copied().collect::<Vec<_>>();
    let rows = if let Some(scope) = versioned_scope {
        sqlx::query(
            r#"
            SELECT DISTINCT ON (id)
              id, version, user_id, state_code, team_id, review_id, json
            FROM public.flows
            WHERE id = ANY($1)
              AND (
                state_code = 100
                OR (
                  user_id = $2
                  AND state_code = 0
                  AND team_id IS NULL
                  AND review_id IS NULL
                )
              )
            ORDER BY id, version DESC, modified_at DESC NULLS LAST, created_at DESC NULLS LAST
            "#,
        )
        .bind(&candidate_ids)
        .bind(scope.actor_user_id)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT DISTINCT ON (id)
              id, version, user_id, state_code, team_id, review_id, json
            FROM public.flows
            WHERE id = ANY($1)
            ORDER BY id, state_code DESC, modified_at DESC NULLS LAST, created_at DESC NULLS LAST
            "#,
        )
        .bind(&candidate_ids)
        .fetch_all(pool)
        .await?
    };

    let elapsed = query_started.elapsed();
    let missing_count = candidate_ids.len().saturating_sub(rows.len());
    let level = if elapsed >= SLOW_QUERY_LOG_THRESHOLD {
        "warn"
    } else {
        "info"
    };
    println!(
        "[query] level={level} name=snapshot.fetch_flow_meta candidate_count={} rows={} missing_count={} elapsed_seconds={:.3}",
        candidate_ids.len(),
        rows.len(),
        missing_count,
        elapsed.as_secs_f64()
    );

    let mut out = HashMap::<Uuid, FlowRow>::new();
    for row in rows {
        let flow = FlowRow {
            id: row.try_get("id")?,
            version: row.try_get::<String, _>("version")?.trim().to_owned(),
            user_id: row.try_get("user_id")?,
            state_code: row.try_get("state_code")?,
            team_id: row.try_get("team_id")?,
            review_id: row.try_get("review_id")?,
            json: row.try_get("json")?,
        };
        if let Some(scope) = versioned_scope {
            validate_flow_row_visibility(&flow, scope.actor_user_id)?;
        }
        out.insert(flow.id, flow);
    }
    Ok(out)
}

fn validate_flow_row_visibility(row: &FlowRow, actor_user_id: Uuid) -> anyhow::Result<()> {
    if row.state_code == 100
        || (row.state_code == 0
            && row.user_id == Some(actor_user_id)
            && row.team_id.is_none()
            && row.review_id.is_none())
    {
        return Ok(());
    }
    Err(anyhow::anyhow!(
        "flow visibility recheck failed: {}@{} owner={:?} state_code={} team_id={:?} review_id={:?}",
        row.id,
        row.version,
        row.user_id,
        row.state_code,
        row.team_id,
        row.review_id
    ))
}

fn process_exchange_items(process_json: &Value) -> Vec<&Value> {
    let Some(exchange) = process_json
        .get("processDataSet")
        .and_then(|v| v.get("exchanges"))
        .and_then(|v| v.get("exchange"))
    else {
        return Vec::new();
    };

    match exchange {
        Value::Array(arr) => arr.iter().collect(),
        Value::Object(_) => vec![exchange],
        _ => Vec::new(),
    }
}

fn method_factor_items(method_json: &Value) -> Vec<&Value> {
    let Some(factor) = method_json
        .get("LCIAMethodDataSet")
        .and_then(|v| v.get("characterisationFactors"))
        .and_then(|v| v.get("factor"))
    else {
        return Vec::new();
    };

    match factor {
        Value::Array(arr) => arr.iter().collect(),
        Value::Object(_) => vec![factor],
        _ => Vec::new(),
    }
}

fn parse_uuid_at(value: &Value, path: &[&str]) -> Option<Uuid> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str().and_then(|s| Uuid::parse_str(s).ok())
}

fn parse_reference_internal_id(process_json: &Value) -> Option<String> {
    process_json
        .get("processDataSet")
        .and_then(|v| v.get("processInformation"))
        .and_then(|v| v.get("quantitativeReference"))
        .and_then(|v| v.get("referenceToReferenceFlow"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn parse_exchange_internal_id(exchange_json: &Value) -> Option<String> {
    exchange_json
        .get("@dataSetInternalID")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn is_reference_internal_exchange(
    exchange_internal_id: Option<&str>,
    reference_internal_id: Option<&str>,
) -> bool {
    reference_internal_id.is_some_and(|reference| exchange_internal_id == Some(reference))
}

fn parse_number(value: Option<&Value>) -> Option<f64> {
    let number = match value {
        Some(Value::String(text)) => {
            let cleaned = text.replace(',', "");
            cleaned.parse::<f64>().ok()
        }
        Some(Value::Number(number)) => number.as_f64(),
        _ => None,
    }?;
    number.is_finite().then_some(number)
}

fn parse_exchange_location(exchange_json: &Value) -> Option<String> {
    exchange_json
        .get("location")
        .and_then(parse_location_string_value)
}

fn parse_location_string_value(value: &Value) -> Option<String> {
    match value {
        Value::Array(items) => items.iter().find_map(parse_location_string_value),
        Value::Object(_) => value
            .get("@location")
            .or_else(|| value.get("#text"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|location| !location.is_empty())
            .map(ToOwned::to_owned),
        Value::String(text) => {
            let trimmed = text.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        }
        _ => None,
    }
}

fn parse_process_location(process_json: &Value) -> Option<String> {
    process_json
        .get("processDataSet")
        .and_then(|v| v.get("processInformation"))
        .and_then(|v| v.get("geography"))
        .and_then(|v| v.get("locationOfOperationSupplyOrProduction"))
        .and_then(|v| v.get("@location"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn supply_region_anchor_for_exchange(
    exchange: &ParsedExchange,
    process_meta: &[ProcessMeta],
) -> anyhow::Result<SupplyRegionAnchor> {
    let consumer = process_meta_for_idx(process_meta, exchange.process_idx).ok_or_else(|| {
        anyhow::anyhow!("missing consumer process meta idx={}", exchange.process_idx)
    })?;
    Ok(resolve_supply_region_anchor(
        exchange.location.as_deref(),
        consumer.location.as_deref(),
    ))
}

fn resolve_supply_region_anchor(
    exchange_location: Option<&str>,
    consumer_location: Option<&str>,
) -> SupplyRegionAnchor {
    if let Some(location) = normalize_usable_supply_region_location(exchange_location) {
        return SupplyRegionAnchor {
            source: CompiledProviderSupplyRegionSource::ExchangeLocation,
            location: Some(location),
        };
    }
    if let Some(location) = normalize_usable_supply_region_location(consumer_location) {
        return SupplyRegionAnchor {
            source: CompiledProviderSupplyRegionSource::ConsumerProcessLocation,
            location: Some(location),
        };
    }
    SupplyRegionAnchor {
        source: CompiledProviderSupplyRegionSource::Unspecified,
        location: None,
    }
}

fn normalize_usable_supply_region_location(location: Option<&str>) -> Option<String> {
    let descriptor = parse_location_descriptor(location);
    location_descriptor_is_usable(&descriptor)
        .then_some(descriptor.canonical)
        .flatten()
}

fn location_descriptor_is_usable(descriptor: &LocationDescriptor) -> bool {
    descriptor.is_global || descriptor.country_code.is_some() || descriptor.region_group.is_some()
}

fn parse_process_name(process_json: &Value) -> Option<String> {
    process_json
        .get("processDataSet")
        .and_then(|v| v.get("processInformation"))
        .and_then(|v| v.get("dataSetInformation"))
        .and_then(|v| v.get("name"))
        .and_then(|v| v.get("baseName"))
        .and_then(|v| match v {
            Value::Array(items) => items.first(),
            Value::Object(_) => Some(v),
            _ => None,
        })
        .and_then(|v| v.get("#text").or(Some(v)))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn parse_process_reference_year(process_json: &Value) -> Option<i32> {
    let value = process_json
        .get("processDataSet")
        .and_then(|v| v.get("processInformation"))
        .and_then(|v| v.get("time"))
        .and_then(|v| v.get("common:referenceYear"))?;
    match value {
        Value::Number(number) => number.as_i64().and_then(|year| i32::try_from(year).ok()),
        Value::String(text) => text.trim().parse::<i32>().ok(),
        _ => None,
    }
}

fn parse_process_annual_supply_or_production_volume(process_json: &Value) -> Option<f64> {
    let value = process_json
        .get("processDataSet")
        .and_then(|v| v.get("modellingAndValidation"))
        .and_then(|v| v.get("dataSourcesTreatmentAndRepresentativeness"))
        .and_then(|v| v.get("annualSupplyOrProductionVolume"))?;
    parse_string_multi_lang_number_prefix(value)
}

fn parse_string_multi_lang_number_prefix(value: &Value) -> Option<f64> {
    match value {
        Value::Array(items) => items.iter().find_map(parse_string_multi_lang_number_prefix),
        Value::Object(_) => value
            .get("#text")
            .and_then(Value::as_str)
            .and_then(parse_positive_number_prefix),
        Value::String(text) => parse_positive_number_prefix(text),
        _ => None,
    }
}

fn parse_positive_number_prefix(text: &str) -> Option<f64> {
    let token = text.split_whitespace().next()?;
    let value = token.replace(',', "").parse::<f64>().ok()?;
    (value.is_finite() && value > 0.0).then_some(value)
}

fn classify_flow_kind(flow_json: &Value) -> &'static str {
    let Some(category) = flow_json
        .get("flowDataSet")
        .and_then(|v| v.get("flowInformation"))
        .and_then(|v| v.get("dataSetInformation"))
        .and_then(|v| v.get("classificationInformation"))
        .and_then(|v| v.get("common:elementaryFlowCategorization"))
        .and_then(|v| v.get("common:category"))
    else {
        return "product";
    };

    let category_text = match category {
        Value::Array(arr) => arr
            .first()
            .and_then(|v| v.get("#text"))
            .and_then(Value::as_str)
            .unwrap_or_default(),
        Value::Object(obj) => obj.get("#text").and_then(Value::as_str).unwrap_or_default(),
        Value::String(text) => text.as_str(),
        _ => "",
    };

    match category_text {
        "Emissions" | "Resources" | "Land use" => "elementary",
        _ => "product",
    }
}

#[allow(clippy::too_many_arguments)]
async fn persist_snapshot_metadata(
    pool: &PgPool,
    snapshot_id: Uuid,
    provider_rule: &str,
    all_states: bool,
    state_codes: &[i32],
    include_user_id: Option<Uuid>,
    versioned_scope: Option<&ValidatedPublicOwnerDraftScope>,
    scope_summary: &ResolvedRequestScopeSummary,
    source_hash: &str,
    method: &MethodSelection,
    built: &BuildOutput,
    artifact_url: &str,
    artifact_sha256: &str,
    artifact_byte_size: i64,
    artifact_format: &str,
    artifact_purpose: Option<&str>,
    artifact_expires_in_seconds: Option<i64>,
) -> anyhow::Result<()> {
    let artifact_expires_at_utc = artifact_expires_at_utc(artifact_expires_in_seconds)?;
    let mut process_filter = if let Some(versioned_scope) = versioned_scope {
        serde_json::json!({
            "all_states": false,
            "process_states": [100],
            "include_user_id": versioned_scope.actor_user_id,
            "include_user_state_codes": [0],
            "include_user_unassigned_only": true,
            "include_user_review_free_only": true,
            "data_scope": PUBLIC_PLUS_OWNER_DRAFT_SCOPE,
            "scope_manifest": versioned_scope.scope_manifest,
            "scope_manifest_sha256": versioned_scope.scope_manifest_sha256,
            "lcia_method_factor_source": versioned_scope.lcia_method_factor_source,
            "lcia_factor_coverage_contract": versioned_scope.lcia_factor_coverage_contract,
            "selection_mode": scope_summary.selection_mode,
            "request_roots": scope_summary.roots,
            "scope_hash": scope_summary.scope_hash,
            "resolved_scope": {
                "public_process_count": scope_summary.public_process_count,
                "private_process_count": scope_summary.private_process_count,
                "process_count": scope_summary.processes.len(),
            }
        })
    } else if all_states {
        serde_json::json!({
            "all_states": true,
            "selection_mode": scope_summary.selection_mode,
            "request_roots": scope_summary.roots,
            "scope_hash": scope_summary.scope_hash,
            "resolved_scope": {
                "public_process_count": scope_summary.public_process_count,
                "private_process_count": scope_summary.private_process_count,
                "process_count": scope_summary.processes.len(),
            }
        })
    } else if let Some(user_id) = include_user_id {
        serde_json::json!({
            "all_states": false,
            "process_states": state_codes,
            "include_user_id": user_id,
            "selection_mode": scope_summary.selection_mode,
            "request_roots": scope_summary.roots,
            "scope_hash": scope_summary.scope_hash,
            "resolved_scope": {
                "public_process_count": scope_summary.public_process_count,
                "private_process_count": scope_summary.private_process_count,
                "process_count": scope_summary.processes.len(),
            }
        })
    } else {
        serde_json::json!({
            "all_states": false,
            "process_states": state_codes,
            "selection_mode": scope_summary.selection_mode,
            "request_roots": scope_summary.roots,
            "scope_hash": scope_summary.scope_hash,
            "resolved_scope": {
                "public_process_count": scope_summary.public_process_count,
                "private_process_count": scope_summary.private_process_count,
                "process_count": scope_summary.processes.len(),
            }
        })
    };
    attach_artifact_lifecycle(
        &mut process_filter,
        artifact_purpose,
        artifact_expires_in_seconds,
        artifact_expires_at_utc.as_deref(),
    );

    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        INSERT INTO public.lca_network_snapshots (
            id,
            scope,
            process_filter,
            lcia_method_id,
            lcia_method_version,
            provider_matching_rule,
            source_hash,
            status,
            created_at,
            updated_at
        )
        VALUES ($1, 'full_library', $2::jsonb, $3, $4::bpchar, $5, $6, 'ready', NOW(), NOW())
        ON CONFLICT (id)
        DO UPDATE SET
            process_filter = EXCLUDED.process_filter,
            lcia_method_id = EXCLUDED.lcia_method_id,
            lcia_method_version = EXCLUDED.lcia_method_version,
            provider_matching_rule = EXCLUDED.provider_matching_rule,
            source_hash = EXCLUDED.source_hash,
            status = EXCLUDED.status,
            updated_at = NOW()
        "#,
    )
    .bind(snapshot_id)
    .bind(process_filter)
    .bind(method.method_id)
    .bind(method.method_version.clone())
    .bind(provider_rule)
    .bind(source_hash)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO public.lca_snapshot_artifacts (
            snapshot_id,
            artifact_url,
            artifact_sha256,
            artifact_byte_size,
            artifact_format,
            process_count,
            flow_count,
            impact_count,
            a_nnz,
            b_nnz,
            c_nnz,
            coverage,
            status,
            created_at,
            updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9, $10, $11,
            $12::jsonb, 'ready', NOW(), NOW()
        )
        ON CONFLICT (snapshot_id, artifact_format)
        DO UPDATE SET
            artifact_url = EXCLUDED.artifact_url,
            artifact_sha256 = EXCLUDED.artifact_sha256,
            artifact_byte_size = EXCLUDED.artifact_byte_size,
            process_count = EXCLUDED.process_count,
            flow_count = EXCLUDED.flow_count,
            impact_count = EXCLUDED.impact_count,
            a_nnz = EXCLUDED.a_nnz,
            b_nnz = EXCLUDED.b_nnz,
            c_nnz = EXCLUDED.c_nnz,
            coverage = EXCLUDED.coverage,
            status = EXCLUDED.status,
            updated_at = NOW()
        "#,
    )
    .bind(snapshot_id)
    .bind(artifact_url)
    .bind(artifact_sha256)
    .bind(artifact_byte_size)
    .bind(artifact_format)
    .bind(i64::from(built.data.process_count))
    .bind(i64::from(built.data.flow_count))
    .bind(i64::from(built.data.impact_count))
    .bind(built.coverage.matrix_scale.a_nnz)
    .bind(built.coverage.matrix_scale.b_nnz)
    .bind(built.coverage.matrix_scale.c_nnz)
    .bind(serde_json::to_value(&built.coverage)?)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

fn append_count_map(md: &mut String, label: &str, counts: &BTreeMap<String, i64>) {
    if counts.is_empty() {
        return;
    }
    md.push_str(&format!("- {label}:\n"));
    for (key, count) in counts {
        md.push_str(&format!("  - `{key}`: `{count}`\n"));
    }
}

fn append_nested_count_map(
    md: &mut String,
    label: &str,
    counts: &BTreeMap<String, BTreeMap<String, i64>>,
) {
    if counts.is_empty() {
        return;
    }
    md.push_str(&format!("- {label}:\n"));
    for (outer_key, inner_counts) in counts {
        md.push_str(&format!("  - `{outer_key}`:\n"));
        for (inner_key, count) in inner_counts {
            md.push_str(&format!("    - `{inner_key}`: `{count}`\n"));
        }
    }
}

fn append_unmatched_flow_top(md: &mut String, entries: &[SnapshotUnmatchedFlowEntry]) {
    if entries.is_empty() {
        return;
    }
    md.push_str("- unmatched_top_flows:\n");
    for entry in entries {
        if let Some(flow_name) = &entry.flow_name {
            md.push_str(&format!(
                "  - `{}` (`{}`): `{}`\n",
                flow_name, entry.flow_id, entry.count
            ));
        } else {
            md.push_str(&format!("  - `{}`: `{}`\n", entry.flow_id, entry.count));
        }
    }
}

fn append_process_gap_top(md: &mut String, entries: &[SnapshotProcessGapEntry]) {
    if entries.is_empty() {
        return;
    }
    md.push_str("- process_gap_top:\n");
    for entry in entries {
        let process_label = entry.process_name.as_deref().map_or_else(
            || entry.process_id.to_string(),
            |name| format!("{name} ({})", entry.process_id),
        );
        md.push_str(&format!(
            "  - `{}`: unmatched_no_provider=`{}`, input_edges_total=`{}`, a_write_pct=`{}`\n",
            process_label, entry.unmatched_no_provider, entry.input_edges_total, entry.a_write_pct
        ));
    }
}

fn write_report_files(
    report_dir: &Path,
    snapshot_id: Uuid,
    config: &SnapshotBuildConfig,
    scope_summary: &ResolvedRequestScopeSummary,
    coverage: &SnapshotCoverageReport,
    artifact_url: &str,
    source_summary: &SourceSnapshotSummary,
    source_fingerprint: &str,
    build_timing: &BuildTimingSec,
    report_policy: LocalSnapshotReportPolicy,
) -> anyhow::Result<()> {
    let json_path = report_dir.join(format!("{snapshot_id}.json"));
    let md_path = report_dir.join(format!("{snapshot_id}.md"));
    let generated_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let doc = serde_json::json!({
        "snapshot_id": snapshot_id,
        "generated_at_utc": generated_at,
        "config": config,
        "resolved_scope": scope_summary,
        "source": {
            "fingerprint": source_fingerprint,
            "summary": source_summary,
        },
        "build_timing_sec": build_timing,
        "coverage": coverage,
        "artifact": {
            "url": artifact_url,
        }
    });
    let json_bytes = serde_json::to_vec_pretty(&doc)?;

    let resolved_strategy_counts = if coverage
        .matching
        .resolution_summary
        .resolved_strategy_counts
        .is_empty()
    {
        &coverage
            .matching
            .provider_decision_diagnostics
            .resolved_strategy_counts
    } else {
        &coverage
            .matching
            .resolution_summary
            .resolved_strategy_counts
    };
    let unresolved_reason_counts = if coverage
        .matching
        .resolution_summary
        .unresolved_reason_counts
        .is_empty()
    {
        &coverage
            .matching
            .provider_decision_diagnostics
            .unresolved_reason_counts
    } else {
        &coverage
            .matching
            .resolution_summary
            .unresolved_reason_counts
    };
    let geography_tier_counts = if coverage.matching.geography_summary.tier_counts.is_empty() {
        &coverage
            .matching
            .provider_decision_diagnostics
            .geography_tier_counts
    } else {
        &coverage.matching.geography_summary.tier_counts
    };
    let supply_region_source_counts = if coverage
        .matching
        .geography_summary
        .supply_region_source_counts
        .is_empty()
    {
        &coverage
            .matching
            .provider_decision_diagnostics
            .supply_region_source_counts
    } else {
        &coverage
            .matching
            .geography_summary
            .supply_region_source_counts
    };
    let volume_fallback_to_one_count =
        if coverage.matching.volume_weight_summary.decisions_total > 0 {
            coverage
                .matching
                .volume_weight_summary
                .fallback_to_one_count
        } else {
            coverage
                .matching
                .provider_decision_diagnostics
                .volume_fallback_to_one_count
        };

    let mut md = String::new();
    md.push_str("# Snapshot Coverage Report\n\n");
    md.push_str(&format!("- snapshot_id: `{snapshot_id}`\n"));
    md.push_str(&format!("- generated_at_utc: `{generated_at}`\n"));
    md.push_str(&format!("- process_states: `{}`\n", config.process_states));
    md.push_str(&format!(
        "- include_user_id: `{}`\n",
        config
            .include_user_id
            .map_or_else(|| "none".to_owned(), |id| id.to_string())
    ));
    md.push_str(&format!("- selection_mode: `{}`\n", config.selection_mode));
    md.push_str(&format!(
        "- request_root_count: `{}`\n",
        config.request_roots.len()
    ));
    if !config.request_roots.is_empty() {
        let roots = config
            .request_roots
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        md.push_str(&format!("- request_roots: `{roots}`\n"));
    }
    md.push_str(&format!("- process_limit: `{}`\n", config.process_limit));
    md.push_str(&format!("- provider_rule: `{}`\n", config.provider_rule));
    md.push_str(&format!(
        "- reference_normalization_mode: `{}`\n",
        config.reference_normalization_mode
    ));
    md.push_str(&format!(
        "- allocation_fraction_mode: `{}`\n",
        config.allocation_fraction_mode
    ));
    md.push_str(&format!(
        "- biosphere_sign_mode: `{}`\n",
        config.biosphere_sign_mode
    ));
    md.push_str(&format!(
        "- self_loop_cutoff: `{}`\n",
        config.self_loop_cutoff
    ));
    md.push_str(&format!("- singular_eps: `{}`\n", config.singular_eps));
    md.push_str(&format!("- has_lcia: `{}`\n", config.has_lcia));
    let method_desc = if !config.has_lcia {
        "disabled".to_owned()
    } else if let Some(method_id) = config.method_id {
        format!(
            "{}@{}",
            method_id,
            config.method_version.as_deref().unwrap_or("unknown")
        )
    } else {
        "all_methods".to_owned()
    };
    md.push_str(&format!("- method: `{method_desc}`\n"));
    md.push_str(&format!("- scope_hash: `{}`\n", scope_summary.scope_hash));
    md.push_str(&format!("- source_fingerprint: `{source_fingerprint}`\n"));
    md.push_str(&format!("- artifact_url: `{artifact_url}`\n\n"));

    md.push_str("## Resolved Scope\n\n");
    md.push_str(&format!(
        "- public_process_count: `{}`\n",
        scope_summary.public_process_count
    ));
    md.push_str(&format!(
        "- private_process_count: `{}`\n",
        scope_summary.private_process_count
    ));
    md.push_str(&format!(
        "- resolved_process_count: `{}`\n\n",
        scope_summary.processes.len()
    ));

    md.push_str("## Build Timing (sec)\n\n");
    md.push_str(&format!(
        "- reused_snapshot: `{}`\n",
        build_timing.reused_snapshot
    ));
    md.push_str(&format!("- total_sec: `{}`\n", build_timing.total_sec));
    md.push_str(&format!(
        "- resolve_method_identity_sec: `{}`\n",
        build_timing.resolve_method_identity_sec
    ));
    md.push_str(&format!(
        "- compute_source_fingerprint_sec: `{}`\n",
        build_timing.compute_source_fingerprint_sec
    ));
    md.push_str(&format!(
        "- reuse_lookup_sec: `{}`\n",
        build_timing.reuse_lookup_sec
    ));
    md.push_str(&format!(
        "- load_method_factors_sec: `{}`\n",
        build_timing.load_method_factors_sec
    ));
    md.push_str(&format!(
        "- build_sparse_payload_sec: `{}`\n",
        build_timing.build_sparse_payload_sec
    ));
    md.push_str(&format!(
        "- encode_artifact_sec: `{}`\n",
        build_timing.encode_artifact_sec
    ));
    md.push_str(&format!(
        "- upload_artifact_sec: `{}`\n",
        build_timing.upload_artifact_sec
    ));
    md.push_str(&format!(
        "- upload_snapshot_index_sec: `{}`\n",
        build_timing.upload_snapshot_index_sec
    ));
    md.push_str(&format!(
        "- persist_metadata_sec: `{}`\n\n",
        build_timing.persist_metadata_sec
    ));

    md.push_str("## Source Summary\n\n");
    md.push_str(&format!(
        "- processes_count: `{}`\n",
        source_summary.process_count
    ));
    md.push_str(&format!(
        "- processes_max_modified_at_utc: `{}`\n",
        source_summary.process_max_modified_at_utc
    ));
    md.push_str(&format!("- flows_count: `{}`\n", source_summary.flow_count));
    md.push_str(&format!(
        "- flows_max_modified_at_utc: `{}`\n",
        source_summary.flow_max_modified_at_utc
    ));
    md.push_str(&format!(
        "- lciamethods_count: `{}`\n",
        source_summary.lciamethod_count
    ));
    md.push_str(&format!(
        "- lciamethods_max_modified_at_utc: `{}`\n\n",
        source_summary.lciamethod_max_modified_at_utc
    ));

    md.push_str("## Matching Coverage\n\n");
    md.push_str(&format!(
        "- input_edges_total: `{}`\n",
        coverage.matching.input_edges_total
    ));
    md.push_str(&format!(
        "- matched_unique_provider: `{}`\n",
        coverage.matching.matched_unique_provider
    ));
    md.push_str(&format!(
        "- matched_multi_provider: `{}`\n",
        coverage.matching.matched_multi_provider
    ));
    md.push_str(&format!(
        "- matched_multi_resolved: `{}`\n",
        coverage.matching.matched_multi_resolved
    ));
    md.push_str(&format!(
        "- matched_multi_unresolved: `{}`\n",
        coverage.matching.matched_multi_unresolved
    ));
    md.push_str(&format!(
        "- matched_multi_fallback_equal: `{}`\n",
        coverage.matching.matched_multi_fallback_equal
    ));
    md.push_str(&format!(
        "- unmatched_no_provider: `{}`\n",
        coverage.matching.unmatched_no_provider
    ));
    md.push_str(&format!(
        "- a_input_edges_written: `{}`\n",
        coverage.matching.a_input_edges_written
    ));
    md.push_str(&format!(
        "- a_write_pct: `{}`\n",
        coverage.matching.a_write_pct
    ));
    md.push_str(&format!(
        "- provider_present_resolved_pct: `{}`\n",
        coverage.matching.provider_present_resolved_pct
    ));
    md.push_str(&format!(
        "- unique_provider_match_pct: `{}`\n",
        coverage.matching.unique_provider_match_pct
    ));
    md.push_str(&format!(
        "- any_provider_match_pct: `{}`\n",
        coverage.matching.any_provider_match_pct
    ));
    md.push_str(&format!(
        "- coverage_schema_version: `{}`\n",
        coverage.schema_version
    ));
    md.push_str(&format!(
        "- volume_fallback_to_one_count: `{}`\n",
        volume_fallback_to_one_count
    ));
    append_count_map(
        &mut md,
        "candidate_count_histogram",
        &coverage
            .matching
            .candidate_summary
            .candidate_count_histogram,
    );
    append_count_map(
        &mut md,
        "resolved_strategy_counts",
        resolved_strategy_counts,
    );
    append_count_map(
        &mut md,
        "unresolved_reason_counts",
        unresolved_reason_counts,
    );
    append_count_map(&mut md, "geography_tier_counts", geography_tier_counts);
    append_nested_count_map(
        &mut md,
        "tier_counts_by_strategy",
        &coverage.matching.geography_summary.tier_counts_by_strategy,
    );
    append_count_map(
        &mut md,
        "supply_region_source_counts",
        supply_region_source_counts,
    );
    md.push_str(&format!(
        "- exchange_location_present_count: `{}`\n",
        coverage
            .matching
            .geography_summary
            .exchange_location_present_count
    ));
    append_count_map(
        &mut md,
        "requested_location_granularity_counts",
        &coverage
            .matching
            .geography_summary
            .requested_location_granularity_counts,
    );
    md.push_str(&format!(
        "- volume_weight_candidate_total: `{}`\n",
        coverage.matching.volume_weight_summary.candidate_total
    ));
    md.push_str(&format!(
        "- volume_weight_valid_volume_count: `{}`\n",
        coverage.matching.volume_weight_summary.valid_volume_count
    ));
    md.push_str(&format!(
        "- volume_weight_decisions_total: `{}`\n",
        coverage.matching.volume_weight_summary.decisions_total
    ));
    md.push_str(&format!(
        "- volume_weight_decisions_all_valid_count: `{}`\n",
        coverage
            .matching
            .volume_weight_summary
            .decisions_all_valid_count
    ));
    md.push_str(&format!(
        "- volume_weight_decisions_partial_missing_count: `{}`\n",
        coverage
            .matching
            .volume_weight_summary
            .decisions_partial_missing_count
    ));
    md.push_str(&format!(
        "- volume_weight_decisions_all_missing_count: `{}`\n",
        coverage
            .matching
            .volume_weight_summary
            .decisions_all_missing_count
    ));
    append_unmatched_flow_top(&mut md, &coverage.matching.gap_summary.unmatched_top_flows);
    append_process_gap_top(&mut md, &coverage.matching.gap_summary.process_gap_top);
    md.push('\n');

    md.push_str("## Reference Coverage\n\n");
    md.push_str(&format!(
        "- process_total: `{}`\n",
        coverage.reference.process_total
    ));
    md.push_str(&format!(
        "- normalized_process_count: `{}`\n",
        coverage.reference.normalized_process_count
    ));
    md.push_str(&format!(
        "- missing_reference_count: `{}`\n",
        coverage.reference.missing_reference_count
    ));
    md.push_str(&format!(
        "- invalid_reference_count: `{}`\n\n",
        coverage.reference.invalid_reference_count
    ));

    md.push_str("## Allocation Coverage\n\n");
    md.push_str(&format!(
        "- exchange_total: `{}`\n",
        coverage.allocation.exchange_total
    ));
    md.push_str(&format!(
        "- allocation_fraction_present_pct: `{}`\n",
        coverage.allocation.allocation_fraction_present_pct
    ));
    md.push_str(&format!(
        "- allocation_fraction_missing_count: `{}`\n",
        coverage.allocation.allocation_fraction_missing_count
    ));
    md.push_str(&format!(
        "- allocation_fraction_invalid_count: `{}`\n\n",
        coverage.allocation.allocation_fraction_invalid_count
    ));

    md.push_str("## Singular Risk\n\n");
    md.push_str(&format!(
        "- risk_level: `{}`\n",
        coverage.singular_risk.risk_level
    ));
    md.push_str(&format!(
        "- prefilter_diag_abs_ge_cutoff: `{}`\n",
        coverage.singular_risk.prefilter_diag_abs_ge_cutoff
    ));
    md.push_str(&format!(
        "- postfilter_a_diag_abs_ge_cutoff: `{}`\n",
        coverage.singular_risk.postfilter_a_diag_abs_ge_cutoff
    ));
    md.push_str(&format!(
        "- m_zero_diagonal_count: `{}`\n",
        coverage.singular_risk.m_zero_diagonal_count
    ));
    md.push_str(&format!(
        "- m_min_abs_diagonal: `{}`\n\n",
        coverage.singular_risk.m_min_abs_diagonal
    ));

    md.push_str("## Matrix Scale\n\n");
    md.push_str(&format!(
        "- process_count (n): `{}`\n",
        coverage.matrix_scale.process_count
    ));
    md.push_str(&format!(
        "- flow_count: `{}`\n",
        coverage.matrix_scale.flow_count
    ));
    md.push_str(&format!(
        "- impact_count: `{}`\n",
        coverage.matrix_scale.impact_count
    ));
    md.push_str(&format!("- a_nnz: `{}`\n", coverage.matrix_scale.a_nnz));
    md.push_str(&format!("- b_nnz: `{}`\n", coverage.matrix_scale.b_nnz));
    md.push_str(&format!("- c_nnz: `{}`\n", coverage.matrix_scale.c_nnz));
    md.push_str(&format!(
        "- m_nnz_estimated: `{}`\n",
        coverage.matrix_scale.m_nnz_estimated
    ));
    md.push_str(&format!(
        "- m_sparsity_estimated: `{}`\n",
        coverage.matrix_scale.m_sparsity_estimated
    ));

    let outcome = write_optional_local_report_files(
        report_dir,
        vec![(json_path, json_bytes), (md_path, md.into_bytes())],
        report_policy,
    )?;
    log_local_report_write_outcome("snapshot coverage", &outcome);
    Ok(())
}

fn write_matrix_readiness_report_file(
    report_dir: &Path,
    snapshot_id: Uuid,
    readiness: &MatrixReadinessReport,
    report_policy: LocalSnapshotReportPolicy,
) -> anyhow::Result<Option<PathBuf>> {
    let path = report_dir.join(format!("matrix-readiness-{snapshot_id}.json"));
    let outcome = write_optional_local_report_files(
        report_dir,
        vec![(path.clone(), serde_json::to_vec_pretty(readiness)?)],
        report_policy,
    )?;
    log_local_report_write_outcome("matrix readiness", &outcome);
    match outcome {
        LocalReportWriteOutcome::Written { .. } => Ok(Some(path)),
        LocalReportWriteOutcome::Skipped { .. } => Ok(None),
    }
}

fn log_local_report_write_outcome(label: &str, outcome: &LocalReportWriteOutcome) {
    match outcome {
        LocalReportWriteOutcome::Written { deleted_paths, .. } => {
            if !deleted_paths.is_empty() {
                eprintln!(
                    "[report_retention] label=\"{label}\" deleted_local_report_count={}",
                    deleted_paths.len()
                );
            }
        }
        LocalReportWriteOutcome::Skipped {
            reason,
            deleted_paths,
        } => {
            if !deleted_paths.is_empty() {
                eprintln!(
                    "[report_retention] label=\"{label}\" deleted_local_report_count={}",
                    deleted_paths.len()
                );
            }
            eprintln!("[report] skipped {label} local report write: {reason}");
        }
    }
}

fn parse_provider_rule_list(input: &str) -> anyhow::Result<Vec<ProviderRule>> {
    let mut out = Vec::<ProviderRule>::new();
    for token in input.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        let rule = ProviderRule::parse(trimmed)?;
        if !out.contains(&rule) {
            out.push(rule);
        }
    }
    if out.is_empty() {
        return Err(anyhow::anyhow!(
            "provider replay rules cannot be empty; pass at least one provider rule"
        ));
    }
    Ok(out)
}

async fn run_provider_rule_replay(
    pool: &PgPool,
    processes: &[ProcessRow],
    include_user_id: Option<Uuid>,
    versioned_scope: Option<&ValidatedPublicOwnerDraftScope>,
    rules: &[ProviderRule],
    reference_normalization_mode: NormalizationMode,
    allocation_mode: AllocationMode,
) -> anyhow::Result<Vec<ProviderRuleReplayRow>> {
    let mut out = Vec::with_capacity(rules.len());
    for rule in rules {
        let compiled_graph = compile_scope_graph(
            pool,
            processes.to_vec(),
            include_user_id,
            versioned_scope,
            *rule,
            reference_normalization_mode,
            allocation_mode,
            &[],
        )
        .await?;
        out.push(build_provider_rule_replay_row(*rule, &compiled_graph.graph));
    }
    Ok(out)
}

fn build_provider_rule_replay_row(
    provider_rule: ProviderRule,
    compiled_graph: &CompiledGraph,
) -> ProviderRuleReplayRow {
    let stats = compiled_graph.matching_stats;
    let diagnostics = summarize_provider_decision_diagnostics(&compiled_graph.provider_decisions);
    let provider_present_total = stats.matched_unique_provider + stats.matched_multi_provider;

    ProviderRuleReplayRow {
        provider_rule: provider_rule.as_str().to_owned(),
        input_edges_total: stats.input_edges_total,
        matched_unique_provider: stats.matched_unique_provider,
        matched_multi_provider: stats.matched_multi_provider,
        matched_multi_resolved: stats.matched_multi_resolved,
        matched_multi_unresolved: stats.matched_multi_unresolved,
        matched_multi_fallback_equal: stats.matched_multi_fallback_equal,
        unmatched_no_provider: stats.unmatched_no_provider,
        a_input_edges_written: stats.a_input_edges_written,
        a_write_pct: pct(stats.a_input_edges_written, stats.input_edges_total),
        provider_present_resolved_pct: pct(stats.a_input_edges_written, provider_present_total),
        resolved_strategy_counts: diagnostics.resolved_strategy_counts,
        unresolved_reason_counts: diagnostics.unresolved_reason_counts,
        volume_fallback_to_one_count: diagnostics.volume_fallback_to_one_count,
        geography_tier_counts: diagnostics.geography_tier_counts,
    }
}

fn write_provider_rule_replay_report_files(
    report_dir: &PathBuf,
    base_name: &str,
    config: &SnapshotBuildConfig,
    scope_summary: &ResolvedRequestScopeSummary,
    rows: &[ProviderRuleReplayRow],
) -> anyhow::Result<(PathBuf, PathBuf)> {
    fs::create_dir_all(report_dir)?;
    let json_path = report_dir.join(format!("provider-rule-replay-{base_name}.json"));
    let md_path = report_dir.join(format!("provider-rule-replay-{base_name}.md"));
    let generated_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let doc = serde_json::json!({
        "generated_at_utc": generated_at,
        "config": config,
        "resolved_scope": scope_summary,
        "results": rows,
    });
    fs::write(&json_path, serde_json::to_vec_pretty(&doc)?)?;

    let mut md = String::new();
    md.push_str("# Provider Rule Replay Report\n\n");
    md.push_str(&format!("- generated_at_utc: `{generated_at}`\n"));
    md.push_str(&format!("- base_name: `{base_name}`\n"));
    md.push_str(&format!("- selection_mode: `{}`\n", config.selection_mode));
    md.push_str(&format!("- process_states: `{}`\n", config.process_states));
    md.push_str(&format!(
        "- configured_provider_rule: `{}`\n",
        config.provider_rule
    ));
    md.push_str(&format!("- scope_hash: `{}`\n", scope_summary.scope_hash));
    md.push_str(&format!(
        "- resolved_process_count: `{}`\n\n",
        scope_summary.processes.len()
    ));

    md.push_str("## Replay Results\n\n");
    for row in rows {
        md.push_str(&format!("### `{}`\n\n", row.provider_rule));
        md.push_str(&format!(
            "- input_edges_total: `{}`\n",
            row.input_edges_total
        ));
        md.push_str(&format!(
            "- matched_unique_provider: `{}`\n",
            row.matched_unique_provider
        ));
        md.push_str(&format!(
            "- matched_multi_provider: `{}`\n",
            row.matched_multi_provider
        ));
        md.push_str(&format!(
            "- matched_multi_resolved: `{}`\n",
            row.matched_multi_resolved
        ));
        md.push_str(&format!(
            "- matched_multi_unresolved: `{}`\n",
            row.matched_multi_unresolved
        ));
        md.push_str(&format!(
            "- matched_multi_fallback_equal: `{}`\n",
            row.matched_multi_fallback_equal
        ));
        md.push_str(&format!(
            "- unmatched_no_provider: `{}`\n",
            row.unmatched_no_provider
        ));
        md.push_str(&format!(
            "- a_input_edges_written: `{}`\n",
            row.a_input_edges_written
        ));
        md.push_str(&format!("- a_write_pct: `{}`\n", row.a_write_pct));
        md.push_str(&format!(
            "- provider_present_resolved_pct: `{}`\n",
            row.provider_present_resolved_pct
        ));
        md.push_str(&format!(
            "- volume_fallback_to_one_count: `{}`\n",
            row.volume_fallback_to_one_count
        ));
        if !row.geography_tier_counts.is_empty() {
            md.push_str("- geography_tier_counts:\n");
            for (tier, count) in &row.geography_tier_counts {
                md.push_str(&format!("  - `{tier}`: `{count}`\n"));
            }
        }
        if !row.resolved_strategy_counts.is_empty() {
            md.push_str("- resolved_strategy_counts:\n");
            for (strategy, count) in &row.resolved_strategy_counts {
                md.push_str(&format!("  - `{strategy}`: `{count}`\n"));
            }
        }
        if !row.unresolved_reason_counts.is_empty() {
            md.push_str("- unresolved_reason_counts:\n");
            for (reason, count) in &row.unresolved_reason_counts {
                md.push_str(&format!("  - `{reason}`: `{count}`\n"));
            }
        }
        md.push('\n');
    }

    fs::write(&md_path, md)?;
    Ok((json_path, md_path))
}

#[cfg(test)]
mod tests {
    use super::{
        AllocationFractionState, AllocationMode, Cli,
        DEFAULT_SNAPSHOT_DB_STATEMENT_TIMEOUT_SECONDS, ExchangeDirection, FlowRow, ImpactFactorSet,
        LciaExchangeObservation, MethodSelection, MultiProviderDecision, NormalizationMode,
        ParsedExchange, ProcessMeta, ProcessRow, ProviderRule, accumulate_finite_factor,
        add_technosphere_edge, assemble_sparse_payload, attach_artifact_lifecycle,
        biosphere_gross_value, build_lcia_factor_coverage, build_review_submit_overlay_graph,
        candidate_count_bucket_label, compute_scope_hash, geo_score, location_granularity_label,
        no_provider_failure_reason, normalize_request_roots, parse_number,
        parse_process_annual_supply_or_production_volume, parse_process_states,
        parse_provider_rule_list, resolve_allocation_fraction, resolve_multi_provider,
        resolve_process_selection, resolve_reference_normalization,
        review_submit_root_dependency_fingerprint, snapshot_db_statement_timeout,
        summarize_matching_diagnostics, time_score, unique_supported_direction_by_flow,
        validate_flow_row_visibility, validate_process_row_visibility,
    };
    use chrono::Utc;
    use clap::Parser;
    use serde_json::json;
    use std::collections::HashMap;
    use uuid::Uuid;

    use solver_worker::compiled_graph::{
        CompiledAllocationStats, CompiledBiosphereEdge, CompiledFlow, CompiledFlowKind,
        CompiledGraph, CompiledMatchingStats, CompiledProcess, CompiledProviderAllocation,
        CompiledProviderCandidateEligibility, CompiledProviderDecision,
        CompiledProviderDecisionKind, CompiledProviderFailureReason, CompiledProviderGeographyTier,
        CompiledProviderOutput, CompiledProviderOutputAllocationState,
        CompiledProviderResolutionStrategy, CompiledProviderSupplyRegionSource,
        CompiledReferenceStats,
    };
    use solver_worker::graph_types::{RequestRootProcess, ScopeProcessPartition};
    use solver_worker::pgbouncer_sqlx::postgres::PgPoolOptions;

    fn assert_close(actual: f64, expected: f64) {
        let delta = (actual - expected).abs();
        assert!(
            delta <= 1e-12,
            "expected {expected}, got {actual}, delta={delta}"
        );
    }

    #[test]
    fn snapshot_db_statement_timeout_defaults_to_bounded_duration() {
        assert_eq!(
            snapshot_db_statement_timeout(DEFAULT_SNAPSHOT_DB_STATEMENT_TIMEOUT_SECONDS),
            Some(std::time::Duration::from_mins(15))
        );
    }

    #[test]
    fn snapshot_db_statement_timeout_allows_explicit_unbounded_recovery_mode() {
        assert_eq!(snapshot_db_statement_timeout(0), None);
    }

    #[test]
    fn technosphere_edge_is_provider_to_consumer() {
        let mut a_map: HashMap<(i32, i32), f64> = HashMap::new();
        add_technosphere_edge(&mut a_map, 10, 20, 0.4);

        assert_close(*a_map.get(&(10, 20)).expect("provider->consumer"), 0.4);
        assert!(!a_map.contains_key(&(20, 10)));
    }

    #[test]
    fn provider_rule_parse_supports_new_modes() {
        assert_eq!(
            ProviderRule::parse("strict_unique_provider").expect("parse"),
            ProviderRule::StrictUniqueProvider
        );
        assert_eq!(
            ProviderRule::parse("best_provider_strict").expect("parse"),
            ProviderRule::BestProviderStrict
        );
        assert_eq!(
            ProviderRule::parse("split_by_process_volume").expect("parse"),
            ProviderRule::SplitByProcessVolume
        );
        assert_eq!(
            ProviderRule::parse("split_by_evidence").expect("parse"),
            ProviderRule::SplitByEvidenceStrict
        );
        assert_eq!(
            ProviderRule::parse("split_by_evidence_hybrid").expect("parse"),
            ProviderRule::SplitByEvidenceHybrid
        );
        assert_eq!(
            ProviderRule::parse("split_equal").expect("parse"),
            ProviderRule::SplitEqual
        );
    }

    #[test]
    fn provider_rule_list_parses_and_deduplicates() {
        let parsed = parse_provider_rule_list(
            "strict_unique_provider, split_by_process_volume, split_by_evidence, split_by_evidence , split_equal",
        )
        .expect("parse");

        assert_eq!(
            parsed,
            vec![
                ProviderRule::StrictUniqueProvider,
                ProviderRule::SplitByProcessVolume,
                ProviderRule::SplitByEvidenceStrict,
                ProviderRule::SplitEqual,
            ]
        );
    }

    #[test]
    fn snapshot_builder_defaults_to_process_volume_provider_rule() {
        let cli = Cli::parse_from(["snapshot_builder"]);
        assert_eq!(cli.provider_rule, "split_by_process_volume");
    }

    #[test]
    fn snapshot_builder_accepts_review_submit_lifecycle_flags() {
        let cli = Cli::parse_from([
            "snapshot_builder",
            "--no-lcia",
            "--artifact-purpose",
            "review_submit_overlay",
            "--artifact-expires-in-seconds",
            "1209600",
            "--reuse-max-age-seconds",
            "1209600",
        ]);

        assert!(cli.no_lcia);
        assert_eq!(
            cli.artifact_purpose.as_deref(),
            Some("review_submit_overlay")
        );
        assert_eq!(cli.artifact_expires_in_seconds, Some(1_209_600));
        assert_eq!(cli.reuse_max_age_seconds, Some(1_209_600));
    }

    #[test]
    fn snapshot_builder_validates_exact_versioned_scope_cli_contract() {
        let actor = Uuid::new_v4();
        let manifest = solver_worker::calculation_evidence::expected_scope_manifest(actor);
        let manifest_hash = solver_worker::calculation_evidence::canonical_json_sha256(&manifest)
            .expect("manifest hash");
        let method_source =
            solver_worker::calculation_evidence::method_factor_source_contract_fixture();
        let coverage = solver_worker::calculation_evidence::expected_factor_coverage_contract();
        let cli = Cli::try_parse_from([
            "snapshot-builder",
            "--process-states",
            "100",
            "--include-user-id",
            &actor.to_string(),
            "--all-states",
            "false",
            "--include-user-state-codes",
            "0",
            "--include-user-unassigned-only",
            "--include-user-review-free-only",
            "--data-scope",
            "public_plus_owner_draft",
            "--scope-manifest-json",
            &manifest.to_string(),
            "--scope-manifest-sha256",
            &manifest_hash,
            "--lcia-method-factor-source-json",
            &method_source.to_string(),
            "--lcia-factor-coverage-contract-json",
            &coverage.to_string(),
        ])
        .expect("parse v2 cli");
        let validated = super::validate_versioned_scope_cli(&cli)
            .expect("validate v2 cli")
            .expect("versioned scope");
        assert_eq!(validated.actor_user_id, actor);
        assert_eq!(validated.scope_manifest_sha256, manifest_hash);
    }

    #[test]
    fn artifact_lifecycle_metadata_is_attached_to_process_filter() {
        let mut process_filter = json!({"all_states": false});

        attach_artifact_lifecycle(
            &mut process_filter,
            Some("review_submit_overlay"),
            Some(1_209_600),
            Some("2026-06-09T00:00:00.000000Z"),
        );

        assert_eq!(
            process_filter["artifact_lifecycle"],
            json!({
                "purpose": "review_submit_overlay",
                "ttl_seconds": 1_209_600,
                "expires_at_utc": "2026-06-09T00:00:00.000000Z"
            })
        );
    }

    #[test]
    fn snapshot_builder_review_submit_dependency_fingerprint_ignores_amount_changes() {
        let flow_id = fixed_flow_id("shared-flow");
        let mut left = ProcessRow {
            id: Uuid::new_v4(),
            version: "01.00.000".to_owned(),
            model_id: None,
            user_id: None,
            state_code: 100,
            team_id: None,
            review_id: None,
            modified_at: Some(Utc::now()),
            json: process_json(&[("Input", flow_id)]),
        };
        let mut right = left.clone();
        left.json["processDataSet"]["exchanges"]["exchange"][0]["meanAmount"] = json!(1.0);
        right.json["processDataSet"]["exchanges"]["exchange"][0]["meanAmount"] = json!(25.0);

        assert_eq!(
            review_submit_root_dependency_fingerprint(&left).expect("left fingerprint"),
            review_submit_root_dependency_fingerprint(&right).expect("right fingerprint")
        );
    }

    #[test]
    fn snapshot_builder_review_submit_dependency_fingerprint_changes_for_dependency_flow() {
        let mut left = ProcessRow {
            id: Uuid::new_v4(),
            version: "01.00.000".to_owned(),
            model_id: None,
            user_id: None,
            state_code: 100,
            team_id: None,
            review_id: None,
            modified_at: Some(Utc::now()),
            json: process_json(&[("Input", fixed_flow_id("left-flow"))]),
        };
        let mut right = left.clone();
        left.json["processDataSet"]["exchanges"]["exchange"][0]["meanAmount"] = json!(1.0);
        right.json = process_json(&[("Input", fixed_flow_id("right-flow"))]);

        assert_ne!(
            review_submit_root_dependency_fingerprint(&left).expect("left fingerprint"),
            review_submit_root_dependency_fingerprint(&right).expect("right fingerprint")
        );
    }

    #[tokio::test]
    async fn snapshot_builder_review_submit_overlay_adds_target_to_baseline_graph() {
        let provider_id = Uuid::new_v4();
        let target_id = Uuid::new_v4();
        let flow_id = fixed_flow_id("shared-flow");
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .expect("lazy pool");
        let baseline_graph = CompiledGraph {
            processes: vec![CompiledProcess {
                process_idx: 0,
                process_id: provider_id,
                process_version: "01.00.000".to_owned(),
                process_name: Some("provider".to_owned()),
                model_id: None,
                location: Some("CN".to_owned()),
                reference_year: Some(2026),
                annual_supply_or_production_volume: Some(10.0),
                partition: ScopeProcessPartition::Public,
            }],
            flows: vec![CompiledFlow {
                flow_idx: 0,
                flow_id,
                kind: CompiledFlowKind::Product,
            }],
            provider_outputs: vec![CompiledProviderOutput {
                flow_id,
                provider_idx: 0,
                output_exchange_internal_id: Some("1".to_owned()),
                output_exchange_is_reference: true,
                output_normalized_amount: Some(1.0),
                output_allocation_state: CompiledProviderOutputAllocationState::Present,
                eligibility: CompiledProviderCandidateEligibility::AcceptedReferenceOutput,
            }],
            provider_decisions: Vec::new(),
            technosphere_edges: Vec::new(),
            biosphere_edges: Vec::new(),
            reference_stats: CompiledReferenceStats {
                normalized_processes: 1,
                ..CompiledReferenceStats::default()
            },
            allocation_stats: CompiledAllocationStats::default(),
            matching_stats: CompiledMatchingStats::default(),
        };
        let target = ProcessRow {
            id: target_id,
            version: "01.00.000".to_owned(),
            model_id: None,
            user_id: None,
            state_code: 100,
            team_id: None,
            review_id: None,
            modified_at: Some(Utc::now()),
            json: process_json(&[("Input", flow_id)]),
        };

        let overlay_graph = build_review_submit_overlay_graph(
            &pool,
            &baseline_graph,
            &target,
            None,
            ProviderRule::SplitByProcessVolume,
            NormalizationMode::Lenient,
            AllocationMode::Lenient,
        )
        .await
        .expect("overlay graph");
        let method = MethodSelection {
            has_lcia: false,
            method_id: None,
            method_version: None,
            method_count: 0,
            factor_count: 0,
            source_evidence: None,
            rows: Vec::new(),
            static_bundle: None,
        };
        let built = assemble_sparse_payload(
            Uuid::new_v4(),
            &method,
            &overlay_graph,
            0.999_999,
            1e-12,
            false,
            &[],
            &[],
            false,
        )
        .expect("assemble overlay");

        assert_eq!(overlay_graph.processes.len(), 2);
        assert_eq!(overlay_graph.provider_decisions.len(), 1);
        assert_eq!(overlay_graph.technosphere_edges.len(), 1);
        assert_eq!(overlay_graph.technosphere_edges[0].provider_idx, 0);
        assert_eq!(overlay_graph.technosphere_edges[0].consumer_idx, 1);
        assert_eq!(built.data.process_count, 2);
        assert_eq!(built.coverage.matching.matched_unique_provider, 1);
    }

    #[test]
    fn versioned_directional_snapshot_preserves_nonzero_sub_epsilon_b_and_c_values() {
        let process_id = Uuid::new_v4();
        let flow_id = Uuid::new_v4();
        let method_id = Uuid::new_v4();
        let tiny_exchange = f64::EPSILON / 4.0;
        let tiny_factor: f64 = 7.006_49e-45;
        assert!(tiny_exchange.abs() < f64::EPSILON);
        assert!(tiny_factor.abs() < f64::EPSILON);

        let graph = CompiledGraph {
            processes: vec![CompiledProcess {
                process_idx: 0,
                process_id,
                process_version: "01.00.000".to_owned(),
                process_name: Some("tiny-value process".to_owned()),
                model_id: None,
                location: None,
                reference_year: None,
                annual_supply_or_production_volume: None,
                partition: ScopeProcessPartition::Public,
            }],
            flows: vec![CompiledFlow {
                flow_idx: 0,
                flow_id,
                kind: CompiledFlowKind::Elementary,
            }],
            provider_outputs: Vec::new(),
            provider_decisions: Vec::new(),
            technosphere_edges: Vec::new(),
            biosphere_edges: vec![
                CompiledBiosphereEdge {
                    process_idx: 0,
                    flow_idx: 0,
                    amount: tiny_exchange,
                    process_partition: ScopeProcessPartition::Public,
                },
                CompiledBiosphereEdge {
                    process_idx: 0,
                    flow_idx: 0,
                    amount: f64::INFINITY,
                    process_partition: ScopeProcessPartition::Public,
                },
            ],
            reference_stats: CompiledReferenceStats::default(),
            allocation_stats: CompiledAllocationStats::default(),
            matching_stats: CompiledMatchingStats::default(),
        };
        let factors = vec![ImpactFactorSet {
            impact_id: method_id,
            method_version: "01.00.000".to_owned(),
            artifact_locator_id: method_id,
            impact_key: format!("method:{method_id}"),
            impact_name: "Tiny factor method".to_owned(),
            unit: "kg".to_owned(),
            factors_by_flow: HashMap::from([(flow_id, tiny_factor)]),
            factors_by_flow_direction: HashMap::from([(
                (flow_id, ExchangeDirection::Output),
                tiny_factor,
            )]),
        }];
        let observations = vec![
            LciaExchangeObservation {
                flow_id,
                flow_version: "01.00.000".to_owned(),
                direction: Some(ExchangeDirection::Output),
                direction_label: "Output".to_owned(),
                exchange_id: "tiny-exchange".to_owned(),
                amount: Some(tiny_exchange),
            },
            LciaExchangeObservation {
                flow_id,
                flow_version: "01.00.000".to_owned(),
                direction: Some(ExchangeDirection::Output),
                direction_label: "Output".to_owned(),
                exchange_id: "nonfinite-exchange".to_owned(),
                amount: Some(f64::INFINITY),
            },
        ];
        let method = MethodSelection {
            has_lcia: true,
            method_id: None,
            method_version: None,
            method_count: 1,
            factor_count: 1,
            source_evidence: None,
            rows: Vec::new(),
            static_bundle: None,
        };

        let built = assemble_sparse_payload(
            Uuid::new_v4(),
            &method,
            &graph,
            0.999_999,
            1e-12,
            true,
            &factors,
            &observations,
            true,
        )
        .expect("assemble versioned directional snapshot");

        assert_eq!(built.data.biosphere_entries.len(), 1);
        assert_eq!(
            built.data.biosphere_entries[0].value.to_bits(),
            tiny_exchange.to_bits()
        );
        assert_eq!(built.data.characterization_factors.len(), 1);
        assert_eq!(
            built.data.characterization_factors[0].value.to_bits(),
            tiny_factor.to_bits()
        );
        assert_eq!(built.coverage.matrix_scale.b_nnz, 1);
        assert_eq!(built.coverage.matrix_scale.c_nnz, 1);
        let factor_coverage = built
            .lcia_factor_coverage
            .expect("versioned factor coverage");
        assert_eq!(factor_coverage.counts.matched, 1);
        assert_eq!(factor_coverage.counts.invalid, 1);
        assert_eq!(factor_coverage.record_count, 1);
    }

    #[test]
    fn biosphere_aggregation_rejects_finite_overflow() {
        let mut b_map = HashMap::new();
        super::accumulate_biosphere_edge(&mut b_map, (0, 0), f64::MAX, true)
            .expect("first finite edge");
        let error = super::accumulate_biosphere_edge(&mut b_map, (0, 0), f64::MAX, true)
            .expect_err("finite aggregation overflow must fail closed");
        assert!(error.to_string().contains("aggregation overflow"));
    }

    #[test]
    fn default_process_states_cover_100_through_199() {
        let default_states = solver_worker::default_snapshot_process_states_arg();
        let (all_states, parsed, label) =
            parse_process_states(default_states.as_str()).expect("parse default states");

        assert!(!all_states);
        assert_eq!(parsed.len(), 100);
        assert_eq!(parsed.first().copied(), Some(100));
        assert_eq!(parsed.last().copied(), Some(199));
        assert_eq!(label, default_states);
    }

    #[test]
    fn explicit_process_states_still_override_default_scope() {
        let (all_states, parsed, label) =
            parse_process_states("100,150,199").expect("parse explicit states");

        assert!(!all_states);
        assert_eq!(parsed, vec![100, 150, 199]);
        assert_eq!(label, "100,150,199");
    }

    #[test]
    fn normalize_request_roots_sorts_and_deduplicates() {
        let process_a =
            Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("process_a uuid");
        let process_b =
            Uuid::parse_str("00000000-0000-0000-0000-000000000002").expect("process_b uuid");
        let normalized = normalize_request_roots(&[
            RequestRootProcess::new(process_b, "02.00.000".to_owned()),
            RequestRootProcess::new(process_a, "01.00.000".to_owned()),
            RequestRootProcess::new(process_b, "02.00.000".to_owned()),
        ]);

        assert_eq!(normalized.len(), 2);
        assert_eq!(
            normalized,
            vec![
                RequestRootProcess::new(process_a, "01.00.000".to_owned()),
                RequestRootProcess::new(process_b, "02.00.000".to_owned())
            ]
        );
    }

    #[test]
    fn scope_hash_is_stable_for_root_order() {
        let root_a = RequestRootProcess::new(Uuid::new_v4(), "01.00.000".to_owned());
        let root_b = RequestRootProcess::new(Uuid::new_v4(), "02.00.000".to_owned());
        let user_id = Uuid::new_v4();
        let left = compute_scope_hash(
            false,
            &[100, 101],
            Some(user_id),
            &normalize_request_roots(&[root_b.clone(), root_a.clone()]),
            0,
            ProviderRule::StrictUniqueProvider,
        )
        .expect("left hash");
        let right = compute_scope_hash(
            false,
            &[100, 101],
            Some(user_id),
            &normalize_request_roots(&[root_a, root_b]),
            0,
            ProviderRule::StrictUniqueProvider,
        )
        .expect("right hash");

        assert_eq!(left, right);
    }

    #[test]
    fn geo_score_prefers_subnational_match() {
        assert_close(geo_score(Some("CN-BJ"), Some("CN-BJ")), 1.0);
        assert_close(geo_score(Some("CN-BJ"), Some("CN-SH")), 0.85);
        assert_close(geo_score(Some("CN"), Some("GLO")), 0.4);
    }

    #[test]
    fn request_roots_resolve_private_to_public_closure() {
        let private_user = Uuid::new_v4();
        let private_process_id = Uuid::new_v4();
        let public_process_id = Uuid::new_v4();
        let selected = resolve_process_selection(
            vec![
                ProcessRow {
                    id: public_process_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json(&[
                        ("Output", fixed_flow_id("public-output")),
                        ("Output", Uuid::new_v4()),
                    ]),
                },
                ProcessRow {
                    id: private_process_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: Some(private_user),
                    state_code: 0,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json(&[
                        ("Input", fixed_flow_id("public-output")),
                        ("Output", Uuid::new_v4()),
                    ]),
                },
            ],
            false,
            &[100],
            Some(private_user),
            &[RequestRootProcess::new(
                private_process_id,
                "01.00.000".to_owned(),
            )],
            ProviderRule::StrictUniqueProvider,
            0,
        )
        .expect("resolve scope");

        assert_eq!(selected.processes.len(), 2);
        assert_eq!(selected.scope_summary.public_process_count, 1);
        assert_eq!(selected.scope_summary.private_process_count, 1);
        assert_eq!(
            selected.scope_summary.processes[0].partition,
            ScopeProcessPartition::Public
        );
        assert_eq!(
            selected.scope_summary.processes[1].partition,
            ScopeProcessPartition::Private
        );
    }

    #[test]
    fn time_score_handles_missing_and_thresholds() {
        assert_close(time_score(None, Some(2020)), 0.5);
        assert_close(time_score(Some(2026), Some(2026)), 1.0);
        assert_close(time_score(Some(2026), Some(2024)), 0.85);
        assert_close(time_score(Some(2026), Some(2016)), 0.4);
        assert_close(time_score(Some(2026), Some(2010)), 0.2);
    }

    #[test]
    fn diagnostic_bucket_labels_are_stable() {
        assert_eq!(candidate_count_bucket_label(0), "zero");
        assert_eq!(candidate_count_bucket_label(1), "one");
        assert_eq!(candidate_count_bucket_label(3), "two_to_five");
        assert_eq!(candidate_count_bucket_label(12), "six_to_twenty");
        assert_eq!(candidate_count_bucket_label(21), "gt_twenty");

        assert_eq!(location_granularity_label(None), "unspecified");
        assert_eq!(location_granularity_label(Some("CN-BJ")), "subnational");
        assert_eq!(location_granularity_label(Some("CN")), "country");
        assert_eq!(location_granularity_label(Some("RER")), "region");
        assert_eq!(location_granularity_label(Some("GLO")), "global");
        assert_eq!(
            location_granularity_label(Some("not-a-location")),
            "unknown"
        );
    }

    #[test]
    fn matching_diagnostics_aggregate_v2_summary_groups() {
        let flow_without_provider = Uuid::from_u128(101);
        let process_with_gap = Uuid::from_u128(202);
        let compiled_graph = CompiledGraph {
            processes: vec![
                CompiledProcess {
                    process_idx: 0,
                    process_id: Uuid::from_u128(201),
                    process_version: "01.00.000".to_owned(),
                    process_name: Some("consumer with providers".to_owned()),
                    model_id: None,
                    location: Some("CN-BJ".to_owned()),
                    reference_year: Some(2026),
                    annual_supply_or_production_volume: None,
                    partition: ScopeProcessPartition::Public,
                },
                CompiledProcess {
                    process_idx: 1,
                    process_id: process_with_gap,
                    process_version: "01.00.000".to_owned(),
                    process_name: Some("consumer with gap".to_owned()),
                    model_id: None,
                    location: Some("CN".to_owned()),
                    reference_year: Some(2026),
                    annual_supply_or_production_volume: None,
                    partition: ScopeProcessPartition::Public,
                },
            ],
            flows: Vec::<CompiledFlow>::new(),
            provider_outputs: Vec::new(),
            provider_decisions: vec![
                CompiledProviderDecision {
                    consumer_idx: 0,
                    flow_id: Uuid::from_u128(301),
                    candidate_provider_count: 1,
                    matched_provider_count: 1,
                    candidates: Vec::new(),
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
                        provider_idx: 2,
                        weight: 1.0,
                    }],
                },
                CompiledProviderDecision {
                    consumer_idx: 0,
                    flow_id: Uuid::from_u128(302),
                    candidate_provider_count: 3,
                    matched_provider_count: 2,
                    candidates: Vec::new(),
                    decision_kind: Some(CompiledProviderDecisionKind::MultiResolved),
                    resolution_strategy: Some(
                        CompiledProviderResolutionStrategy::SplitByProcessVolume,
                    ),
                    failure_reason: None,
                    used_equal_fallback: false,
                    volume_fallback_to_one_count: 1,
                    geography_tier: Some(CompiledProviderGeographyTier::LocalSubnational),
                    supply_region_source: Some(
                        CompiledProviderSupplyRegionSource::ExchangeLocation,
                    ),
                    supply_region_location: Some("CN-BJ".to_owned()),
                    exchange_location_present: true,
                    allocations: vec![
                        CompiledProviderAllocation {
                            provider_idx: 3,
                            weight: 0.75,
                        },
                        CompiledProviderAllocation {
                            provider_idx: 4,
                            weight: 0.25,
                        },
                    ],
                },
                CompiledProviderDecision {
                    consumer_idx: 1,
                    flow_id: flow_without_provider,
                    candidate_provider_count: 0,
                    matched_provider_count: 0,
                    candidates: Vec::new(),
                    decision_kind: Some(CompiledProviderDecisionKind::NoProvider),
                    resolution_strategy: None,
                    failure_reason: Some(CompiledProviderFailureReason::NoProviderCandidates),
                    used_equal_fallback: false,
                    volume_fallback_to_one_count: 0,
                    geography_tier: None,
                    supply_region_source: Some(CompiledProviderSupplyRegionSource::Unspecified),
                    supply_region_location: None,
                    exchange_location_present: false,
                    allocations: Vec::new(),
                },
            ],
            technosphere_edges: Vec::new(),
            biosphere_edges: Vec::new(),
            reference_stats: CompiledReferenceStats::default(),
            allocation_stats: CompiledAllocationStats::default(),
            matching_stats: CompiledMatchingStats::default(),
        };

        let diagnostics = summarize_matching_diagnostics(&compiled_graph);

        assert_eq!(
            diagnostics
                .candidate_summary
                .candidate_count_histogram
                .get("zero"),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .candidate_summary
                .candidate_count_histogram
                .get("one"),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .candidate_summary
                .candidate_count_histogram
                .get("two_to_five"),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .requested_location_granularity_counts
                .get("subnational"),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .requested_location_granularity_counts
                .get("country"),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .requested_location_granularity_counts
                .get("unspecified"),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .exchange_location_present_count,
            1
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .tier_counts_by_strategy
                .get("unique_provider")
                .and_then(|counts| counts.get("same_country")),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .tier_counts_by_strategy
                .get("split_by_process_volume")
                .and_then(|counts| counts.get("local_subnational")),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .supply_region_source_counts_by_strategy
                .get("unique_provider")
                .and_then(|counts| counts.get("consumer_process_location")),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .supply_region_source_counts_by_strategy
                .get("split_by_process_volume")
                .and_then(|counts| counts.get("exchange_location")),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .supply_region_source_counts_by_strategy
                .get("unique_provider")
                .and_then(|counts| counts.get("unspecified")),
            None
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .exchange_location_present_count_by_strategy
                .get("split_by_process_volume"),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .exchange_location_present_count_by_strategy
                .get("unique_provider"),
            None
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .requested_location_granularity_counts_by_strategy
                .get("unique_provider")
                .and_then(|counts| counts.get("country")),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .geography_summary
                .requested_location_granularity_counts_by_strategy
                .get("split_by_process_volume")
                .and_then(|counts| counts.get("subnational")),
            Some(&1)
        );
        assert_eq!(diagnostics.volume_weight_summary.candidate_total, 2);
        assert_eq!(diagnostics.volume_weight_summary.valid_volume_count, 1);
        assert_eq!(diagnostics.volume_weight_summary.fallback_to_one_count, 1);
        assert_eq!(
            diagnostics
                .volume_weight_summary
                .decisions_partial_missing_count,
            1
        );
        assert_eq!(
            diagnostics.gap_summary.unmatched_top_flows[0].flow_id,
            flow_without_provider
        );
        assert_eq!(
            diagnostics.gap_summary.process_gap_top[0].process_id,
            process_with_gap
        );
    }

    fn process_json(exchanges: &[(&str, Uuid)]) -> serde_json::Value {
        process_json_with_metadata(exchanges, None, None)
    }

    fn process_json_without_quantitative_reference(
        exchanges: &[(&str, Uuid)],
        location: Option<&str>,
    ) -> serde_json::Value {
        let geography = location.map(|location| {
            json!({
                "locationOfOperationSupplyOrProduction": {
                    "@location": location
                }
            })
        });
        json!({
            "processDataSet": {
                "processInformation": {
                    "geography": geography
                },
                "exchanges": {
                    "exchange": exchanges.iter().enumerate().map(|(idx, (direction, flow_id))| {
                        json!({
                            "@dataSetInternalID": (idx + 1).to_string(),
                            "exchangeDirection": direction,
                            "referenceToFlowDataSet": {
                                "@refObjectId": flow_id
                            },
                            "meanAmount": 1.0,
                            "allocations": {
                                "allocation": {
                                    "@allocatedFraction": 1.0
                                }
                            }
                        })
                    }).collect::<Vec<_>>()
                }
            }
        })
    }

    fn process_json_with_metadata(
        exchanges: &[(&str, Uuid)],
        location: Option<&str>,
        annual_volume_text: Option<&str>,
    ) -> serde_json::Value {
        let exchanges: Vec<(&str, Uuid, Option<&str>)> = exchanges
            .iter()
            .map(|(direction, flow_id)| (*direction, *flow_id, None))
            .collect::<Vec<_>>();
        process_json_with_exchange_locations(&exchanges, location, annual_volume_text)
    }

    fn process_json_with_exchange_locations(
        exchanges: &[(&str, Uuid, Option<&str>)],
        location: Option<&str>,
        annual_volume_text: Option<&str>,
    ) -> serde_json::Value {
        let geography = location.map(|location| {
            json!({
                "locationOfOperationSupplyOrProduction": {
                    "@location": location
                }
            })
        });
        let annual_volume = annual_volume_text.map(|text| {
            json!({
                "#text": text
            })
        });
        json!({
            "processDataSet": {
                "processInformation": {
                    "quantitativeReference": {
                        "referenceToReferenceFlow": "1"
                    },
                    "geography": geography
                },
                "modellingAndValidation": {
                    "dataSourcesTreatmentAndRepresentativeness": {
                        "annualSupplyOrProductionVolume": annual_volume
                    }
                },
                "exchanges": {
                    "exchange": exchanges.iter().enumerate().map(|(idx, (direction, flow_id, exchange_location))| {
                        let mut exchange = json!({
                            "@dataSetInternalID": (idx + 1).to_string(),
                            "exchangeDirection": direction,
                            "referenceToFlowDataSet": {
                                "@refObjectId": flow_id
                            },
                            "meanAmount": 1.0,
                            "allocations": {
                                "allocation": {
                                    "@allocatedFraction": 1.0
                                }
                            }
                        });
                        if let Some(exchange_location) = exchange_location {
                            exchange["location"] = json!(exchange_location);
                        }
                        exchange
                    }).collect::<Vec<_>>()
                }
            }
        })
    }

    fn fixed_flow_id(label: &str) -> Uuid {
        let bytes = label.as_bytes();
        let mut raw = [0_u8; 16];
        for (idx, byte) in bytes.iter().copied().enumerate().take(16) {
            raw[idx] = byte;
        }
        Uuid::from_bytes(raw)
    }

    fn test_process_meta(
        process_idx: i32,
        location: Option<&str>,
        annual_volume: Option<f64>,
    ) -> ProcessMeta {
        ProcessMeta {
            process_idx,
            process_id: Uuid::from_u128(
                u128::try_from(process_idx).expect("nonnegative process idx") + 1,
            ),
            process_version: "01.00.000".to_owned(),
            process_name: None,
            model_id: None,
            location: location.map(ToOwned::to_owned),
            reference_year: Some(2026),
            annual_supply_or_production_volume: annual_volume,
        }
    }

    fn test_process_meta_with_model(
        process_idx: i32,
        model_id: Option<Uuid>,
        location: Option<&str>,
        annual_volume: Option<f64>,
    ) -> ProcessMeta {
        let mut meta = test_process_meta(process_idx, location, annual_volume);
        meta.model_id = model_id;
        meta
    }

    #[test]
    fn annual_supply_or_production_volume_parses_string_multilang_shapes() {
        let object_process = process_json_with_metadata(
            &[("Output", Uuid::new_v4())],
            Some("CN"),
            Some("1,234.5 kg reference flow"),
        );
        assert_close(
            parse_process_annual_supply_or_production_volume(&object_process).expect("parse"),
            1234.5,
        );

        let array_process = json!({
            "processDataSet": {
                "modellingAndValidation": {
                    "dataSourcesTreatmentAndRepresentativeness": {
                        "annualSupplyOrProductionVolume": [
                            { "#text": "" },
                            { "#text": "12.5 t reference flow" }
                        ]
                    }
                }
            }
        });
        assert_close(
            parse_process_annual_supply_or_production_volume(&array_process).expect("parse"),
            12.5,
        );
    }

    #[test]
    fn annual_supply_or_production_volume_ignores_invalid_or_non_positive_values() {
        for text in ["", "abc kg", "0 kg", "-5 kg", "NaN kg"] {
            let process =
                process_json_with_metadata(&[("Output", Uuid::new_v4())], Some("CN"), Some(text));
            assert!(
                parse_process_annual_supply_or_production_volume(&process).is_none(),
                "expected no parsed annual volume for {text:?}"
            );
        }
    }

    #[test]
    fn split_by_process_volume_uses_volume_within_local_tier() {
        let process_meta = vec![
            test_process_meta(0, Some("CN-BJ"), None),
            test_process_meta(1, Some("CN-BJ"), Some(10.0)),
            test_process_meta(2, Some("CN-BJ"), Some(30.0)),
            test_process_meta(3, Some("CN"), Some(1_000.0)),
            test_process_meta(4, Some("GLO"), Some(10_000.0)),
        ];
        let exchange = ParsedExchange {
            process_idx: 0,
            flow_id: Uuid::new_v4(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "test-exchange".to_owned(),
            flow_version: "01.00.000".to_owned(),
            internal_id: None,
            is_reference_exchange: false,
            amount: Some(1.0),
            allocation_state: AllocationFractionState::Present,
            location: None,
        };

        let resolution = resolve_multi_provider(
            ProviderRule::SplitByProcessVolume,
            &exchange,
            &[1, 2, 3, 4],
            &process_meta,
        )
        .expect("resolve");
        let MultiProviderDecision::Resolved(resolution) = resolution else {
            panic!("expected resolved decision");
        };

        assert_eq!(
            resolution.resolution_strategy,
            CompiledProviderResolutionStrategy::SplitByProcessVolume
        );
        assert_eq!(
            resolution.geography_tier,
            Some(CompiledProviderGeographyTier::LocalSubnational)
        );
        assert_eq!(resolution.volume_fallback_to_one_count, 0);
        assert_eq!(resolution.allocations.len(), 2);
        assert_eq!(resolution.allocations[0].0, 1);
        assert_close(resolution.allocations[0].1, 0.25);
        assert_eq!(resolution.allocations[1].0, 2);
        assert_close(resolution.allocations[1].1, 0.75);
    }

    #[test]
    fn split_by_process_volume_falls_back_to_one_for_missing_volume() {
        let process_meta = vec![
            test_process_meta(0, Some("CN-BJ"), None),
            test_process_meta(1, Some("CN-BJ"), Some(9.0)),
            test_process_meta(2, Some("CN-BJ"), None),
        ];
        let exchange = ParsedExchange {
            process_idx: 0,
            flow_id: Uuid::new_v4(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "test-exchange".to_owned(),
            flow_version: "01.00.000".to_owned(),
            internal_id: None,
            is_reference_exchange: false,
            amount: Some(1.0),
            allocation_state: AllocationFractionState::Present,
            location: None,
        };

        let resolution = resolve_multi_provider(
            ProviderRule::SplitByProcessVolume,
            &exchange,
            &[1, 2],
            &process_meta,
        )
        .expect("resolve");
        let MultiProviderDecision::Resolved(resolution) = resolution else {
            panic!("expected resolved decision");
        };

        assert_eq!(resolution.volume_fallback_to_one_count, 1);
        assert_eq!(resolution.allocations.len(), 2);
        assert_eq!(resolution.allocations[0].0, 1);
        assert_close(resolution.allocations[0].1, 0.9);
        assert_eq!(resolution.allocations[1].0, 2);
        assert_close(resolution.allocations[1].1, 0.1);
    }

    #[test]
    fn split_by_process_volume_uses_same_country_when_local_absent() {
        let process_meta = vec![
            test_process_meta(0, Some("CN-BJ"), None),
            test_process_meta(1, Some("CN"), Some(2.0)),
            test_process_meta(2, Some("CN-SH"), Some(6.0)),
            test_process_meta(3, Some("GLO"), Some(100.0)),
        ];
        let exchange = ParsedExchange {
            process_idx: 0,
            flow_id: Uuid::new_v4(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "test-exchange".to_owned(),
            flow_version: "01.00.000".to_owned(),
            internal_id: None,
            is_reference_exchange: false,
            amount: Some(1.0),
            allocation_state: AllocationFractionState::Present,
            location: None,
        };

        let resolution = resolve_multi_provider(
            ProviderRule::SplitByProcessVolume,
            &exchange,
            &[1, 2, 3],
            &process_meta,
        )
        .expect("resolve");
        let MultiProviderDecision::Resolved(resolution) = resolution else {
            panic!("expected resolved decision");
        };

        assert_eq!(
            resolution.geography_tier,
            Some(CompiledProviderGeographyTier::SameCountry)
        );
        assert_eq!(resolution.allocations.len(), 2);
        assert_eq!(resolution.allocations[0].0, 1);
        assert_close(resolution.allocations[0].1, 0.25);
        assert_eq!(resolution.allocations[1].0, 2);
        assert_close(resolution.allocations[1].1, 0.75);
    }

    #[test]
    fn split_by_process_volume_uses_exchange_location_before_consumer_location() {
        let process_meta = vec![
            test_process_meta(0, Some("CN-BJ"), None),
            test_process_meta(1, Some("CN-BJ"), Some(100.0)),
            test_process_meta(2, Some("GLO"), Some(1.0)),
        ];
        let exchange = ParsedExchange {
            process_idx: 0,
            flow_id: Uuid::new_v4(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "test-exchange".to_owned(),
            flow_version: "01.00.000".to_owned(),
            internal_id: None,
            is_reference_exchange: false,
            amount: Some(1.0),
            allocation_state: AllocationFractionState::Present,
            location: Some("GLO".to_owned()),
        };

        let resolution = resolve_multi_provider(
            ProviderRule::SplitByProcessVolume,
            &exchange,
            &[1, 2],
            &process_meta,
        )
        .expect("resolve");
        let MultiProviderDecision::Resolved(resolution) = resolution else {
            panic!("expected resolved decision");
        };

        assert_eq!(
            resolution.geography_tier,
            Some(CompiledProviderGeographyTier::Global)
        );
        assert_eq!(resolution.allocations, vec![(2, 1.0)]);
    }

    #[test]
    fn split_by_process_volume_prefers_same_model_candidates_before_geography_tier() {
        let consumer_model = Uuid::new_v4();
        let other_model = Uuid::new_v4();
        let process_meta = vec![
            test_process_meta_with_model(0, Some(consumer_model), Some("CN-BJ"), None),
            test_process_meta_with_model(1, Some(other_model), Some("CN-BJ"), Some(1_000.0)),
            test_process_meta_with_model(2, Some(consumer_model), Some("GLO"), Some(1.0)),
        ];
        let exchange = ParsedExchange {
            process_idx: 0,
            flow_id: Uuid::new_v4(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "test-exchange".to_owned(),
            flow_version: "01.00.000".to_owned(),
            internal_id: None,
            is_reference_exchange: false,
            amount: Some(1.0),
            allocation_state: AllocationFractionState::Present,
            location: None,
        };

        let resolution = resolve_multi_provider(
            ProviderRule::SplitByProcessVolume,
            &exchange,
            &[1, 2],
            &process_meta,
        )
        .expect("resolve");
        let MultiProviderDecision::Resolved(resolution) = resolution else {
            panic!("expected resolved decision");
        };

        assert_eq!(
            resolution.geography_tier,
            Some(CompiledProviderGeographyTier::Global)
        );
        assert_eq!(resolution.allocations, vec![(2, 1.0)]);
    }

    #[test]
    fn split_by_process_volume_uses_exchange_country_tier() {
        let process_meta = vec![
            test_process_meta(0, Some("CN-BJ"), None),
            test_process_meta(1, Some("CN-BJ"), Some(2.0)),
            test_process_meta(2, Some("CN"), Some(6.0)),
            test_process_meta(3, Some("GLO"), Some(100.0)),
        ];
        let exchange = ParsedExchange {
            process_idx: 0,
            flow_id: Uuid::new_v4(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "test-exchange".to_owned(),
            flow_version: "01.00.000".to_owned(),
            internal_id: None,
            is_reference_exchange: false,
            amount: Some(1.0),
            allocation_state: AllocationFractionState::Present,
            location: Some("CN".to_owned()),
        };

        let resolution = resolve_multi_provider(
            ProviderRule::SplitByProcessVolume,
            &exchange,
            &[1, 2, 3],
            &process_meta,
        )
        .expect("resolve");
        let MultiProviderDecision::Resolved(resolution) = resolution else {
            panic!("expected resolved decision");
        };

        assert_eq!(
            resolution.geography_tier,
            Some(CompiledProviderGeographyTier::SameCountry)
        );
        assert_eq!(resolution.allocations.len(), 2);
        assert_eq!(resolution.allocations[0].0, 1);
        assert_close(resolution.allocations[0].1, 0.25);
        assert_eq!(resolution.allocations[1].0, 2);
        assert_close(resolution.allocations[1].1, 0.75);
    }

    #[test]
    fn split_by_process_volume_falls_back_to_consumer_location_for_unusable_exchange_location() {
        let process_meta = vec![
            test_process_meta(0, Some("CN-BJ"), None),
            test_process_meta(1, Some("CN-BJ"), Some(1.0)),
            test_process_meta(2, Some("GLO"), Some(100.0)),
        ];
        let exchange = ParsedExchange {
            process_idx: 0,
            flow_id: Uuid::new_v4(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "test-exchange".to_owned(),
            flow_version: "01.00.000".to_owned(),
            internal_id: None,
            is_reference_exchange: false,
            amount: Some(1.0),
            allocation_state: AllocationFractionState::Present,
            location: Some("not-a-location".to_owned()),
        };

        let resolution = resolve_multi_provider(
            ProviderRule::SplitByProcessVolume,
            &exchange,
            &[1, 2],
            &process_meta,
        )
        .expect("resolve");
        let MultiProviderDecision::Resolved(resolution) = resolution else {
            panic!("expected resolved decision");
        };

        assert_eq!(
            resolution.geography_tier,
            Some(CompiledProviderGeographyTier::LocalSubnational)
        );
        assert_eq!(resolution.allocations, vec![(1, 1.0)]);
    }

    #[test]
    fn request_roots_closure_uses_process_volume_provider_resolution() {
        let root_process_id = Uuid::new_v4();
        let local_provider_id = Uuid::new_v4();
        let global_provider_id = Uuid::new_v4();
        let flow_id = fixed_flow_id("shared-flow");

        let selected = resolve_process_selection(
            vec![
                ProcessRow {
                    id: root_process_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json_with_metadata(&[("Input", flow_id)], Some("CN-BJ"), None),
                },
                ProcessRow {
                    id: local_provider_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json_with_metadata(
                        &[("Output", flow_id)],
                        Some("CN-BJ"),
                        Some("1 kg reference flow"),
                    ),
                },
                ProcessRow {
                    id: global_provider_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json_with_metadata(
                        &[("Output", flow_id)],
                        Some("GLO"),
                        Some("100000 kg reference flow"),
                    ),
                },
            ],
            false,
            &[100],
            None,
            &[RequestRootProcess::new(
                root_process_id,
                "01.00.000".to_owned(),
            )],
            ProviderRule::SplitByProcessVolume,
            0,
        )
        .expect("resolve scope");

        let selected_ids = selected
            .processes
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert_eq!(selected_ids, vec![root_process_id, local_provider_id]);
    }

    #[test]
    fn request_roots_closure_uses_exchange_location_provider_resolution() {
        let root_process_id = Uuid::new_v4();
        let local_provider_id = Uuid::new_v4();
        let global_provider_id = Uuid::new_v4();
        let flow_id = fixed_flow_id("shared-flow");

        let selected = resolve_process_selection(
            vec![
                ProcessRow {
                    id: root_process_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json_with_exchange_locations(
                        &[("Input", flow_id, Some("GLO"))],
                        Some("CN-BJ"),
                        None,
                    ),
                },
                ProcessRow {
                    id: local_provider_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json_with_metadata(
                        &[("Output", flow_id)],
                        Some("CN-BJ"),
                        Some("100000 kg reference flow"),
                    ),
                },
                ProcessRow {
                    id: global_provider_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json_with_metadata(
                        &[("Output", flow_id)],
                        Some("GLO"),
                        Some("1 kg reference flow"),
                    ),
                },
            ],
            false,
            &[100],
            None,
            &[RequestRootProcess::new(
                root_process_id,
                "01.00.000".to_owned(),
            )],
            ProviderRule::SplitByProcessVolume,
            0,
        )
        .expect("resolve scope");

        let selected_ids = selected
            .processes
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert_eq!(selected_ids, vec![root_process_id, global_provider_id]);
    }

    #[test]
    fn request_roots_closure_ignores_non_reference_output_provider() {
        let root_process_id = Uuid::new_v4();
        let local_non_reference_provider_id = Uuid::new_v4();
        let global_reference_provider_id = Uuid::new_v4();
        let demanded_flow_id = fixed_flow_id("demanded-flow");
        let local_reference_flow_id = fixed_flow_id("local-main");

        let selected = resolve_process_selection(
            vec![
                ProcessRow {
                    id: root_process_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json_with_metadata(
                        &[("Input", demanded_flow_id)],
                        Some("CN-BJ"),
                        None,
                    ),
                },
                ProcessRow {
                    id: local_non_reference_provider_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json_with_metadata(
                        &[
                            ("Output", local_reference_flow_id),
                            ("Output", demanded_flow_id),
                        ],
                        Some("CN-BJ"),
                        Some("100000 kg reference flow"),
                    ),
                },
                ProcessRow {
                    id: global_reference_provider_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json_with_metadata(
                        &[("Output", demanded_flow_id)],
                        Some("GLO"),
                        Some("1 kg reference flow"),
                    ),
                },
            ],
            false,
            &[100],
            None,
            &[RequestRootProcess::new(
                root_process_id,
                "01.00.000".to_owned(),
            )],
            ProviderRule::SplitByProcessVolume,
            0,
        )
        .expect("resolve scope");

        let selected_ids = selected
            .processes
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert_eq!(
            selected_ids,
            vec![root_process_id, global_reference_provider_id]
        );
    }

    #[test]
    fn request_roots_closure_requires_declared_reference_output_provider() {
        let root_process_id = Uuid::new_v4();
        let missing_reference_provider_id = Uuid::new_v4();
        let global_reference_provider_id = Uuid::new_v4();
        let demanded_flow_id = fixed_flow_id("demanded-flow");

        let selected = resolve_process_selection(
            vec![
                ProcessRow {
                    id: root_process_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json_with_metadata(
                        &[("Input", demanded_flow_id)],
                        Some("CN-BJ"),
                        None,
                    ),
                },
                ProcessRow {
                    id: missing_reference_provider_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json_without_quantitative_reference(
                        &[("Output", demanded_flow_id)],
                        Some("CN-BJ"),
                    ),
                },
                ProcessRow {
                    id: global_reference_provider_id,
                    version: "01.00.000".to_owned(),
                    model_id: None,
                    user_id: None,
                    state_code: 100,
                    team_id: None,
                    review_id: None,
                    modified_at: Some(Utc::now()),
                    json: process_json_with_metadata(
                        &[("Output", demanded_flow_id)],
                        Some("GLO"),
                        Some("1 kg reference flow"),
                    ),
                },
            ],
            false,
            &[100],
            None,
            &[RequestRootProcess::new(
                root_process_id,
                "01.00.000".to_owned(),
            )],
            ProviderRule::SplitByProcessVolume,
            0,
        )
        .expect("resolve scope");

        let selected_ids = selected
            .processes
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert_eq!(
            selected_ids,
            vec![root_process_id, global_reference_provider_id]
        );
    }

    #[test]
    fn no_provider_reason_distinguishes_rejected_non_reference_only() {
        let flow_id = fixed_flow_id("demanded-flow");
        let rejected = super::ProviderOutputCandidate {
            flow_id,
            provider_idx: 1,
            output_exchange_internal_id: Some("2".to_owned()),
            output_exchange_is_reference: false,
            output_normalized_amount: Some(100.0),
            output_allocation_state: AllocationFractionState::Present,
        };

        assert_eq!(
            no_provider_failure_reason(Some(&vec![rejected])),
            CompiledProviderFailureReason::RejectedNonReferenceOnly
        );
        assert_eq!(
            no_provider_failure_reason(None),
            CompiledProviderFailureReason::NoProviderCandidates
        );
    }

    #[test]
    fn best_provider_strict_selects_single_top_candidate() {
        let process_meta = vec![
            ProcessMeta {
                process_idx: 0,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: None,
                location: Some("CN-BJ".to_owned()),
                reference_year: Some(2026),
                annual_supply_or_production_volume: None,
            },
            ProcessMeta {
                process_idx: 1,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: None,
                location: Some("CN-BJ".to_owned()),
                reference_year: Some(2026),
                annual_supply_or_production_volume: None,
            },
            ProcessMeta {
                process_idx: 2,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: None,
                location: Some("GLO".to_owned()),
                reference_year: Some(2010),
                annual_supply_or_production_volume: None,
            },
        ];
        let exchange = ParsedExchange {
            process_idx: 0,
            flow_id: Uuid::new_v4(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "test-exchange".to_owned(),
            flow_version: "01.00.000".to_owned(),
            internal_id: None,
            is_reference_exchange: false,
            amount: Some(1.0),
            allocation_state: AllocationFractionState::Present,
            location: None,
        };

        let resolution = resolve_multi_provider(
            ProviderRule::BestProviderStrict,
            &exchange,
            &[1, 2],
            &process_meta,
        )
        .expect("resolve");
        let MultiProviderDecision::Resolved(resolution) = resolution else {
            panic!("expected resolved decision");
        };
        assert_eq!(resolution.allocations.len(), 1);
        assert_eq!(resolution.allocations[0].0, 1);
        assert_close(resolution.allocations[0].1, 1.0);
        assert_eq!(
            resolution.resolution_strategy,
            CompiledProviderResolutionStrategy::BestProviderStrict
        );
    }

    #[test]
    fn best_provider_strict_prefers_same_model_id_before_geo_time() {
        let model_consumer = Uuid::new_v4();
        let model_other = Uuid::new_v4();
        let process_meta = vec![
            ProcessMeta {
                process_idx: 0,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: Some(model_consumer),
                location: Some("CN-BJ".to_owned()),
                reference_year: Some(2026),
                annual_supply_or_production_volume: None,
            },
            ProcessMeta {
                process_idx: 1,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: Some(model_consumer),
                location: Some("CN".to_owned()),
                reference_year: Some(2024),
                annual_supply_or_production_volume: None,
            },
            ProcessMeta {
                process_idx: 2,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: Some(model_other),
                location: Some("CN-BJ".to_owned()),
                reference_year: Some(2026),
                annual_supply_or_production_volume: None,
            },
        ];
        let exchange = ParsedExchange {
            process_idx: 0,
            flow_id: Uuid::new_v4(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "test-exchange".to_owned(),
            flow_version: "01.00.000".to_owned(),
            internal_id: None,
            is_reference_exchange: false,
            amount: Some(1.0),
            allocation_state: AllocationFractionState::Present,
            location: None,
        };

        let resolution = resolve_multi_provider(
            ProviderRule::BestProviderStrict,
            &exchange,
            &[1, 2],
            &process_meta,
        )
        .expect("resolve");
        let MultiProviderDecision::Resolved(resolution) = resolution else {
            panic!("expected resolved decision");
        };
        assert_eq!(resolution.allocations.len(), 1);
        assert_eq!(resolution.allocations[0].0, 1);
        assert_close(resolution.allocations[0].1, 1.0);
    }

    #[test]
    fn strict_unique_provider_marks_rule_requires_unique_provider() {
        let process_meta = vec![
            ProcessMeta {
                process_idx: 0,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: None,
                location: Some("CN-BJ".to_owned()),
                reference_year: Some(2026),
                annual_supply_or_production_volume: None,
            },
            ProcessMeta {
                process_idx: 1,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: None,
                location: Some("CN-BJ".to_owned()),
                reference_year: Some(2026),
                annual_supply_or_production_volume: None,
            },
            ProcessMeta {
                process_idx: 2,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: None,
                location: Some("CN".to_owned()),
                reference_year: Some(2024),
                annual_supply_or_production_volume: None,
            },
        ];
        let exchange = ParsedExchange {
            process_idx: 0,
            flow_id: Uuid::new_v4(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "test-exchange".to_owned(),
            flow_version: "01.00.000".to_owned(),
            internal_id: None,
            is_reference_exchange: false,
            amount: Some(1.0),
            allocation_state: AllocationFractionState::Present,
            location: None,
        };

        let decision = resolve_multi_provider(
            ProviderRule::StrictUniqueProvider,
            &exchange,
            &[1, 2],
            &process_meta,
        )
        .expect("resolve");

        let MultiProviderDecision::Unresolved(reason) = decision else {
            panic!("expected unresolved decision");
        };
        assert_eq!(
            reason,
            CompiledProviderFailureReason::RuleRequiresUniqueProvider
        );
    }

    #[test]
    fn best_provider_strict_marks_ratio_too_close() {
        let process_meta = vec![
            ProcessMeta {
                process_idx: 0,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: None,
                location: Some("CN-BJ".to_owned()),
                reference_year: Some(2026),
                annual_supply_or_production_volume: None,
            },
            ProcessMeta {
                process_idx: 1,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: None,
                location: Some("CN-BJ".to_owned()),
                reference_year: Some(2026),
                annual_supply_or_production_volume: None,
            },
            ProcessMeta {
                process_idx: 2,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: None,
                location: Some("CN-BJ".to_owned()),
                reference_year: Some(2026),
                annual_supply_or_production_volume: None,
            },
        ];
        let exchange = ParsedExchange {
            process_idx: 0,
            flow_id: Uuid::new_v4(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "test-exchange".to_owned(),
            flow_version: "01.00.000".to_owned(),
            internal_id: None,
            is_reference_exchange: false,
            amount: Some(1.0),
            allocation_state: AllocationFractionState::Present,
            location: None,
        };

        let decision = resolve_multi_provider(
            ProviderRule::BestProviderStrict,
            &exchange,
            &[1, 2],
            &process_meta,
        )
        .expect("resolve");

        let MultiProviderDecision::Unresolved(reason) = decision else {
            panic!("expected unresolved decision");
        };
        assert_eq!(reason, CompiledProviderFailureReason::Top1Top2RatioTooClose);
    }

    #[test]
    fn best_provider_strict_marks_top1_below_min_score() {
        let process_meta = vec![
            ProcessMeta {
                process_idx: 0,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: None,
                location: Some("CN-BJ".to_owned()),
                reference_year: Some(2026),
                annual_supply_or_production_volume: None,
            },
            ProcessMeta {
                process_idx: 1,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: None,
                location: Some("GLO".to_owned()),
                reference_year: None,
                annual_supply_or_production_volume: None,
            },
            ProcessMeta {
                process_idx: 2,
                process_id: Uuid::new_v4(),
                process_version: "01.00.000".to_owned(),
                process_name: None,
                model_id: None,
                location: Some("US".to_owned()),
                reference_year: Some(2010),
                annual_supply_or_production_volume: None,
            },
        ];
        let exchange = ParsedExchange {
            process_idx: 0,
            flow_id: Uuid::new_v4(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "test-exchange".to_owned(),
            flow_version: "01.00.000".to_owned(),
            internal_id: None,
            is_reference_exchange: false,
            amount: Some(1.0),
            allocation_state: AllocationFractionState::Present,
            location: None,
        };

        let decision = resolve_multi_provider(
            ProviderRule::BestProviderStrict,
            &exchange,
            &[1, 2],
            &process_meta,
        )
        .expect("resolve");

        let MultiProviderDecision::Unresolved(reason) = decision else {
            panic!("expected unresolved decision");
        };
        assert_eq!(reason, CompiledProviderFailureReason::Top1BelowTop1MinScore);
    }

    #[test]
    fn allocation_fraction_parses_percent_and_numeric() {
        let exchange_percent = json!({
            "allocations": { "allocation": { "@allocatedFraction": "25%" } }
        });
        let exchange_numeric = json!({
            "allocations": { "allocation": { "@allocatedFraction": "25" } }
        });
        let (fraction_percent, _) =
            resolve_allocation_fraction(&exchange_percent, AllocationMode::Strict).expect("parse");
        let (fraction_numeric, _) =
            resolve_allocation_fraction(&exchange_numeric, AllocationMode::Strict).expect("parse");
        assert_close(fraction_percent, 0.25);
        assert_close(fraction_numeric, 0.25);
    }

    #[test]
    fn allocation_fraction_strict_fails_when_missing() {
        let exchange = json!({});
        let err =
            resolve_allocation_fraction(&exchange, AllocationMode::Strict).expect_err("expected");
        assert!(err.to_string().contains("missing"));
    }

    #[test]
    fn quantitative_reference_normalization_uses_reference_exchange() {
        let process_id = Uuid::new_v4();
        let process_json = json!({
            "processDataSet": {
                "processInformation": {
                    "quantitativeReference": {
                        "referenceToReferenceFlow": "2"
                    }
                }
            }
        });
        let exchanges = [
            json!({
                "@dataSetInternalID": "1",
                "meanAmount": "0.2"
            }),
            json!({
                "@dataSetInternalID": "2",
                "meanAmount": "0.5"
            }),
        ];
        let exchange_refs = exchanges.iter().collect::<Vec<_>>();
        let (scale, stats) = resolve_reference_normalization(
            process_id,
            &process_json,
            exchange_refs.as_slice(),
            NormalizationMode::Strict,
        )
        .expect("normalize");
        assert_close(scale, 2.0);
        assert_eq!(stats.normalized_processes, 1);
        assert_eq!(stats.missing_reference, 0);
        assert_eq!(stats.invalid_reference, 0);
    }

    #[test]
    fn biosphere_gross_value_preserves_input_sign() {
        assert_close(biosphere_gross_value(5.0), 5.0);
        assert_close(biosphere_gross_value(-5.0), -5.0);
        assert_close(biosphere_gross_value(0.0), 0.0);
    }

    #[test]
    fn exact_visibility_rechecks_reject_nonpublic_and_collaboration_drafts() {
        let actor = Uuid::new_v4();
        let foreign = Uuid::new_v4();
        let base = ProcessRow {
            id: Uuid::new_v4(),
            version: "01.00.000".to_owned(),
            model_id: None,
            user_id: Some(actor),
            state_code: 0,
            team_id: None,
            review_id: None,
            modified_at: None,
            json: json!({}),
        };
        validate_process_row_visibility(&base, actor).expect("owner draft");

        let mut row = base.clone();
        row.state_code = 100;
        row.user_id = Some(foreign);
        validate_process_row_visibility(&row, actor).expect("public state 100");
        row.state_code = 101;
        assert!(validate_process_row_visibility(&row, actor).is_err());
        row.state_code = 0;
        assert!(validate_process_row_visibility(&row, actor).is_err());
        row.user_id = Some(actor);
        row.state_code = 1;
        assert!(validate_process_row_visibility(&row, actor).is_err());
        row.state_code = 0;
        row.team_id = Some(Uuid::new_v4());
        assert!(validate_process_row_visibility(&row, actor).is_err());
        row.team_id = None;
        row.review_id = Some(Uuid::new_v4());
        assert!(validate_process_row_visibility(&row, actor).is_err());

        let flow = FlowRow {
            id: row.id,
            version: row.version.clone(),
            user_id: row.user_id,
            state_code: row.state_code,
            team_id: row.team_id,
            review_id: row.review_id,
            json: json!({}),
        };
        assert!(validate_flow_row_visibility(&flow, actor).is_err());
    }

    #[test]
    fn factor_coverage_matches_flow_and_direction_and_surfaces_gaps() {
        let flow_id = Uuid::new_v4();
        let factors = vec![ImpactFactorSet {
            impact_id: Uuid::new_v4(),
            method_version: "01.00.000".to_owned(),
            artifact_locator_id: Uuid::new_v4(),
            impact_key: "method:test".to_owned(),
            impact_name: "Test".to_owned(),
            unit: "kg".to_owned(),
            factors_by_flow: HashMap::from([(flow_id, 1.0)]),
            factors_by_flow_direction: HashMap::from([((flow_id, ExchangeDirection::Output), 1.0)]),
        }];
        let observations = vec![LciaExchangeObservation {
            flow_id,
            flow_version: "01.00.000".to_owned(),
            direction: Some(ExchangeDirection::Input),
            direction_label: "Input".to_owned(),
            exchange_id: "exchange-1".to_owned(),
            amount: Some(2.0),
        }];
        let directions = unique_supported_direction_by_flow(&observations);
        let coverage =
            build_lcia_factor_coverage(&observations, &factors, &directions).expect("coverage");
        assert_eq!(coverage.counts.matched, 0);
        assert_eq!(coverage.counts.unmatched, 1);
        let gap_jsonl = std::fs::read_to_string(coverage.records.path()).expect("read gaps");
        assert!(gap_jsonl.contains("no_lcia_factor_for_flow_direction"));

        let mut ambiguous = observations;
        ambiguous.push(LciaExchangeObservation {
            flow_id,
            flow_version: "01.00.000".to_owned(),
            direction: Some(ExchangeDirection::Output),
            direction_label: "Output".to_owned(),
            exchange_id: "exchange-2".to_owned(),
            amount: Some(1.0),
        });
        let directions = unique_supported_direction_by_flow(&ambiguous);
        let coverage = build_lcia_factor_coverage(&ambiguous, &factors, &directions)
            .expect("ambiguous coverage");
        assert_eq!(coverage.counts.unsupported_direction, 2);
        assert_eq!(coverage.record_count, 2);
    }

    #[test]
    fn factor_coverage_is_per_method_when_no_key_is_shared_by_all_methods() {
        let flow_a = Uuid::from_u128(1);
        let flow_b = Uuid::from_u128(2);
        let method_a = Uuid::from_u128(10);
        let method_b = Uuid::from_u128(20);
        let factors = vec![
            ImpactFactorSet {
                impact_id: method_b,
                method_version: "01.00.000".to_owned(),
                artifact_locator_id: method_b,
                impact_key: "method:b".to_owned(),
                impact_name: "B".to_owned(),
                unit: "kg".to_owned(),
                factors_by_flow: HashMap::from([(flow_b, 2.0)]),
                factors_by_flow_direction: HashMap::from([(
                    (flow_b, ExchangeDirection::Output),
                    2.0,
                )]),
            },
            ImpactFactorSet {
                impact_id: method_a,
                method_version: "01.00.000".to_owned(),
                artifact_locator_id: method_a,
                impact_key: "method:a".to_owned(),
                impact_name: "A".to_owned(),
                unit: "kg".to_owned(),
                factors_by_flow: HashMap::from([(flow_a, 1.0)]),
                factors_by_flow_direction: HashMap::from([(
                    (flow_a, ExchangeDirection::Output),
                    1.0,
                )]),
            },
        ];
        let observations = [flow_b, flow_a]
            .into_iter()
            .map(|flow_id| LciaExchangeObservation {
                flow_id,
                flow_version: "01.00.000".to_owned(),
                direction: Some(ExchangeDirection::Output),
                direction_label: "Output".to_owned(),
                exchange_id: format!("exchange-{flow_id}"),
                amount: Some(1.0),
            })
            .collect::<Vec<_>>();
        let directions = unique_supported_direction_by_flow(&observations);
        let coverage =
            build_lcia_factor_coverage(&observations, &factors, &directions).expect("coverage");
        assert_eq!(coverage.counts.matched, 2);
        assert_eq!(coverage.counts.unmatched, 2);
        assert_eq!(coverage.record_count, 2);
        assert_eq!(coverage.by_method[0].method_id, method_a);
        assert_eq!(coverage.by_method[0].counts.matched, 1);
        assert_eq!(coverage.by_method[0].counts.unmatched, 1);
        assert_eq!(coverage.by_method[1].method_id, method_b);
        let lines = std::fs::read_to_string(coverage.records.path()).expect("gaps");
        let records = lines
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("gap record"))
            .collect::<Vec<_>>();
        assert_eq!(
            std::fs::metadata(coverage.records.path())
                .expect("gap metadata")
                .len(),
            coverage.artifact_byte_size
        );
        assert_eq!(records[0]["method_id"], json!(method_a));
        assert_eq!(records[1]["method_id"], json!(method_b));
    }

    #[test]
    fn parse_number_rejects_nonfinite_values() {
        assert_eq!(parse_number(Some(&json!("NaN"))), None);
        assert_eq!(parse_number(Some(&json!("inf"))), None);
        assert_eq!(parse_number(Some(&json!("-Infinity"))), None);
        assert_eq!(parse_number(Some(&json!("1.25"))), Some(1.25));
    }

    #[test]
    fn normalized_exchange_overflow_becomes_invalid_before_matrix_assembly() {
        let reference_flow = Uuid::new_v4();
        let overflow_flow = Uuid::new_v4();
        let mut dataset = process_json(&[("Output", reference_flow), ("Input", overflow_flow)]);
        dataset["processDataSet"]["exchanges"]["exchange"][0]["meanAmount"] = json!(1e-15);
        dataset["processDataSet"]["exchanges"]["exchange"][1]["meanAmount"] = json!(f64::MAX);
        let row = ProcessRow {
            id: Uuid::new_v4(),
            version: "01.00.000".to_owned(),
            model_id: None,
            user_id: None,
            state_code: 100,
            team_id: None,
            review_id: None,
            modified_at: None,
            json: dataset,
        };

        let (_, exchanges, _, _, _) =
            super::parse_process_chunk(&row, 0, NormalizationMode::Strict, AllocationMode::Strict)
                .expect("parse process");

        assert_eq!(exchanges.len(), 2);
        assert!(exchanges[0].amount.is_some_and(f64::is_finite));
        assert_eq!(exchanges[1].amount, None);
    }

    #[test]
    fn factor_aggregation_rejects_finite_inputs_that_overflow() {
        let method_id = Uuid::new_v4();
        let mut factors = HashMap::new();
        accumulate_finite_factor(&mut factors, Uuid::nil(), f64::MAX, method_id)
            .expect("first finite factor");
        assert!(accumulate_finite_factor(&mut factors, Uuid::nil(), f64::MAX, method_id).is_err());
        assert!(factors[&Uuid::nil()].is_finite());
    }
}
