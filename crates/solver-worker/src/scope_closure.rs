//! Exact-version, non-fail-fast source-closure preflight for data-product builds.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    io::{BufRead, BufReader, Cursor, Read, Write},
    process::{Command, Stdio},
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::{
    calculation_evidence::RELEASE_METHOD_IDENTITIES,
    db::AppState,
    pgbouncer_sqlx::{self as sqlx, PgPool, Postgres, QueryBuilder, Row},
    worker_jobs::WorkerJobProgress,
};

pub const SCOPE_CLOSURE_JOB_KIND: &str = "lcia.scope_closure_check";
pub const SCOPE_CLOSURE_REQUEST_SCHEMA_VERSION: &str = "lcia.scope_closure_check.request.v1";
pub const SCOPE_CLOSURE_RESULT_SCHEMA_VERSION: &str = "lcia.scope_closure_check.result.v1";
pub const SCOPE_CLOSURE_SCANNER_VERSION: &str = "scope-closure-scanner.v1";
pub const TIDAS_BATCH_PROTOCOL: &str = "document-validation-batch.v1";
pub const TIDAS_BATCH_PROFILE: &str = "tidas-document-conformance.v1";
pub const REFERENCE_EDGE_SCHEMA_VERSION: &str = "tidas.reference-edge.v1";
pub const REFERENCE_ISSUE_SCHEMA_VERSION: &str = "tidas.reference-extraction-issue.v1";
const FETCH_BATCH_SIZE: usize = 96;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatasetCategory {
    #[serde(alias = "contact")]
    Contacts,
    #[serde(alias = "flowproperty")]
    Flowproperties,
    #[serde(alias = "flow")]
    Flows,
    #[serde(alias = "lciamethod")]
    Lciamethods,
    #[serde(alias = "lifecyclemodel")]
    Lifecyclemodels,
    #[serde(alias = "process")]
    Processes,
    #[serde(alias = "source")]
    Sources,
    #[serde(alias = "unitgroup")]
    Unitgroups,
}

impl DatasetCategory {
    #[must_use]
    pub const fn table_name(&self) -> &'static str {
        match self {
            Self::Contacts => "contacts",
            Self::Flowproperties => "flowproperties",
            Self::Flows => "flows",
            Self::Lciamethods => "lciamethods",
            Self::Lifecyclemodels => "lifecyclemodels",
            Self::Processes => "processes",
            Self::Sources => "sources",
            Self::Unitgroups => "unitgroups",
        }
    }

    fn from_reference_type(raw: &str) -> Option<Self> {
        match normalize_reference_type(raw).as_str() {
            "contact" | "contact data set" => Some(Self::Contacts),
            "flow" | "flow data set" => Some(Self::Flows),
            "flow property" | "flow property data set" => Some(Self::Flowproperties),
            "lcia method" | "lcia method data set" => Some(Self::Lciamethods),
            "life cycle model"
            | "life cycle model data set"
            | "lifecycle model"
            | "lifecycle model data set" => Some(Self::Lifecyclemodels),
            "process" | "process data set" => Some(Self::Processes),
            "source" | "source data set" => Some(Self::Sources),
            "unit group" | "unit group data set" => Some(Self::Unitgroups),
            _ => None,
        }
    }

    fn from_uri(raw: &str) -> Option<Self> {
        raw.split(['/', '\\'])
            .find_map(|part| match part.to_ascii_lowercase().as_str() {
                "contacts" | "contact" => Some(Self::Contacts),
                "flows" | "flow" => Some(Self::Flows),
                "flowproperties" | "flowproperty" | "flow-properties" => Some(Self::Flowproperties),
                "lciamethods" | "lciamethod" | "lcia-methods" => Some(Self::Lciamethods),
                "lifecyclemodels" | "lifecyclemodel" | "life-cycle-models" => {
                    Some(Self::Lifecyclemodels)
                }
                "processes" | "process" => Some(Self::Processes),
                "sources" | "source" => Some(Self::Sources),
                "unitgroups" | "unitgroup" | "unit-groups" => Some(Self::Unitgroups),
                _ => None,
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExactDatasetIdentity {
    pub category: DatasetCategory,
    pub id: Uuid,
    pub version: String,
}

impl ExactDatasetIdentity {
    #[must_use]
    pub fn document_key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.category.table_name(),
            self.id,
            self.version
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestedIdentity {
    pub id: Uuid,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeLinkPolicy {
    pub link_semantics_version: String,
    pub flow_identity_policy: String,
    pub allocation_semantics_version: String,
    pub technosphere_boundary_policy: String,
    pub provider_universe_policy: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestedScopeManifest {
    pub schema_version: String,
    pub coverage_mode: String,
    pub eligibility_predicate_version: String,
    #[serde(default)]
    pub processes: Vec<RequestedIdentity>,
    #[serde(default)]
    pub lcia_methods: Vec<RequestedIdentity>,
    pub version_resolution_policy: String,
    pub legacy_omitted_version_policy: String,
    pub certificate_freshness_policy: String,
    pub link_policy: ScopeLinkPolicy,
    #[serde(default)]
    pub process_manifest_hash: Option<String>,
}

impl RequestedScopeManifest {
    fn roots(&self) -> Vec<ExactDatasetIdentity> {
        let processes = self.processes.iter().map(|item| ExactDatasetIdentity {
            category: DatasetCategory::Processes,
            id: item.id,
            version: item.version.clone(),
        });
        let methods = self.lcia_methods.iter().map(|item| ExactDatasetIdentity {
            category: DatasetCategory::Lciamethods,
            id: item.id,
            version: item.version.clone(),
        });
        let mut roots = processes.chain(methods).collect::<Vec<_>>();
        roots.sort();
        roots.dedup();
        roots
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeClosureWorkerInput {
    pub closure_check_id: Uuid,
    pub scan_execution_id: Uuid,
    pub requested_scope: RequestedScopeManifest,
    pub requested_scope_hash: String,
    pub policy_fingerprint: String,
    pub data_snapshot_token: String,
    pub data_snapshot_manifest: Value,
    pub data_snapshot_manifest_hash: String,
    pub publication_epoch: i64,
    pub expected_validator_scanner_fingerprint: String,
    pub request_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataSnapshotManifest {
    pub schema_version: String,
    pub requested_scope: RequestedScopeManifest,
    pub current_public_release: CurrentPublicRelease,
    #[serde(default)]
    pub datasets: Vec<SnapshotDatasetEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentPublicRelease {
    pub publication_id: Uuid,
    pub release_run_id: Uuid,
    pub release_version: String,
    pub published_at: String,
    pub release_manifest_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotDatasetEntry {
    pub dataset_type: DatasetCategory,
    pub dataset_id: Uuid,
    pub dataset_version: String,
    pub role: String,
    #[serde(default)]
    pub source_process_id: Option<Uuid>,
    #[serde(default)]
    pub source_process_version: Option<String>,
    pub version_significant_hash: String,
    pub semantic_hash: String,
    pub canonical_content_hash: String,
}

impl SnapshotDatasetEntry {
    fn identity(&self) -> ExactDatasetIdentity {
        ExactDatasetIdentity {
            category: self.dataset_type,
            id: self.dataset_id,
            version: self.dataset_version.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClosureDocument {
    pub identity: ExactDatasetIdentity,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReferenceEdge {
    pub schema_version: String,
    pub document_key: String,
    pub source_category: String,
    pub target_category: String,
    pub target_uuid: String,
    pub requested_version_state: String,
    pub requested_version: Option<String>,
    pub requested_version_raw: Value,
    pub reference_role: String,
    pub json_path: String,
    pub raw_type: Value,
    pub uri: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedReference {
    pub source: ExactDatasetIdentity,
    pub target: ExactDatasetIdentity,
    pub json_path: String,
    pub reference_role: String,
    pub requested_version_state: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReferenceExtractionIssue {
    pub schema_version: String,
    pub issue_code: String,
    pub severity: String,
    pub document_key: String,
    pub source_category: String,
    pub json_path: String,
    pub reference_role: String,
    pub message: String,
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReferenceExtractionResult {
    pub schema_version: String,
    pub document_key: String,
    pub source_category: String,
    pub edges: Vec<ReferenceEdge>,
    pub issues: Vec<ReferenceExtractionIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClosureIssue {
    pub issue_key: String,
    pub severity: String,
    pub blocking: bool,
    pub issue_code: String,
    pub source: Option<ExactDatasetIdentity>,
    pub json_path: Option<String>,
    pub reference_role: Option<String>,
    pub requested_target_type: Option<String>,
    pub requested_target_id: Option<Uuid>,
    pub requested_target_version: Option<String>,
    pub message: String,
    pub suggested_action: Option<String>,
    pub occurrence_count: u32,
    #[serde(default)]
    pub occurrences: Vec<ClosureIssueOccurrence>,
    pub affected_roots: Vec<ExactDatasetIdentity>,
    pub affected_root_witness_paths: Vec<Vec<ExactDatasetIdentity>>,
    pub witness_path: Vec<ExactDatasetIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClosureIssueOccurrence {
    pub occurrence_key: String,
    pub source: Option<ExactDatasetIdentity>,
    pub json_path: Option<String>,
    pub reference_role: Option<String>,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeClosureScan {
    pub schema_version: String,
    pub complete: bool,
    pub roots: Vec<ExactDatasetIdentity>,
    pub documents: Vec<ClosureDocument>,
    pub edges: Vec<ReferenceEdge>,
    pub resolved_references: Vec<ResolvedReference>,
    pub omitted_version_resolutions: Vec<Value>,
    pub issues: Vec<ClosureIssue>,
    pub frontier: Vec<ExactDatasetIdentity>,
    pub provider_universe: Vec<ExactDatasetIdentity>,
}

impl ScopeClosureScan {
    #[must_use]
    pub fn blocker_codes(&self) -> Vec<String> {
        self.issues
            .iter()
            .filter(|issue| issue.blocking)
            .map(|issue| issue.issue_code.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

trait ScopeClosureProvider {
    async fn checkpoint(&self, _scanned: usize, _scheduled: usize) -> anyhow::Result<()> {
        Ok(())
    }

    async fn fetch_exact(
        &self,
        identities: &[ExactDatasetIdentity],
    ) -> anyhow::Result<ProviderFetchResult>;

    async fn resolve_omitted_version(
        &self,
        category: DatasetCategory,
        id: Uuid,
        policy: &str,
    ) -> anyhow::Result<OmittedVersionResolution>;
}

#[derive(Debug, Clone, Default)]
struct ProviderFetchResult {
    documents: Vec<ClosureDocument>,
    issues: Vec<ClosureIssue>,
    incomplete_identities: BTreeSet<ExactDatasetIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OmittedVersionResolution {
    selected: Option<ExactDatasetIdentity>,
    candidates: Vec<ExactDatasetIdentity>,
    policy: String,
}

pub struct PgScopeClosureProvider<'a> {
    pool: &'a PgPool,
    lease: Option<(Uuid, Uuid, i32)>,
    snapshot_universe: BTreeMap<ExactDatasetIdentity, SnapshotDatasetEntry>,
}

impl<'a> PgScopeClosureProvider<'a> {
    #[must_use]
    pub fn new(pool: &'a PgPool, snapshot: &DataSnapshotManifest) -> Self {
        Self {
            pool,
            lease: None,
            snapshot_universe: snapshot_dataset_universe(snapshot),
        }
    }

    #[must_use]
    pub fn new_leased(
        pool: &'a PgPool,
        snapshot: &DataSnapshotManifest,
        worker_job_id: Uuid,
        lease_token: Uuid,
        lease_seconds: i32,
    ) -> Self {
        Self {
            pool,
            lease: Some((worker_job_id, lease_token, lease_seconds)),
            snapshot_universe: snapshot_dataset_universe(snapshot),
        }
    }
}

fn snapshot_dataset_universe(
    snapshot: &DataSnapshotManifest,
) -> BTreeMap<ExactDatasetIdentity, SnapshotDatasetEntry> {
    snapshot
        .datasets
        .iter()
        .cloned()
        .map(|entry| (entry.identity(), entry))
        .collect()
}

impl ScopeClosureProvider for PgScopeClosureProvider<'_> {
    async fn checkpoint(&self, scanned: usize, scheduled: usize) -> anyhow::Result<()> {
        if let Some((worker_job_id, lease_token, lease_seconds)) = self.lease {
            crate::worker_jobs::heartbeat_worker_job(
                self.pool,
                worker_job_id,
                lease_token,
                "discover_reference_graph",
                0.18 + 0.22 * bounded_progress_ratio(scanned, scheduled),
                Some(json!({
                    "progressCounters": {
                        "scanned": scanned,
                        "total": scheduled,
                        "unit": "documents"
                    }
                })),
                lease_seconds,
            )
            .await?;
        }
        Ok(())
    }

    async fn fetch_exact(
        &self,
        identities: &[ExactDatasetIdentity],
    ) -> anyhow::Result<ProviderFetchResult> {
        // Read the exact requested identities, including identities absent from
        // the release allowlist.  We never accept a live-only row, but observing
        // it is required to distinguish an ineligible substitution (incomplete)
        // from a complete negative exact-version finding.
        let live_documents = fetch_exact_documents(self.pool, identities).await?;
        enforce_snapshot_boundary(identities, &self.snapshot_universe, live_documents)
    }

    async fn resolve_omitted_version(
        &self,
        category: DatasetCategory,
        id: Uuid,
        policy: &str,
    ) -> anyhow::Result<OmittedVersionResolution> {
        resolve_snapshot_omitted_version(&self.snapshot_universe, category, id, policy)
    }
}

fn bounded_progress_ratio(completed: usize, total: usize) -> f64 {
    let completed = u32::try_from(completed).unwrap_or(u32::MAX);
    let total = u32::try_from(total.max(1)).unwrap_or(u32::MAX);
    (f64::from(completed) / f64::from(total)).min(1.0)
}

fn enforce_snapshot_boundary(
    identities: &[ExactDatasetIdentity],
    snapshot_universe: &BTreeMap<ExactDatasetIdentity, SnapshotDatasetEntry>,
    live_documents: Vec<ClosureDocument>,
) -> anyhow::Result<ProviderFetchResult> {
    let live_by_identity = live_documents
        .into_iter()
        .map(|document| (document.identity.clone(), document))
        .collect::<BTreeMap<_, _>>();
    let mut result = ProviderFetchResult::default();
    for identity in identities {
        let Some(snapshot_entry) = snapshot_universe.get(identity) else {
            if live_by_identity.contains_key(identity) {
                result.issues.push(provider_boundary_issue(
                    "snapshot_dataset_not_allowed",
                    identity,
                    "A live exact dataset exists but is absent from the frozen public-release manifest.",
                    &json!({"snapshotAllowed": false, "liveOnly": true}),
                ));
                result.incomplete_identities.insert(identity.clone());
            }
            continue;
        };
        let Some(document) = live_by_identity.get(identity).cloned() else {
            result.issues.push(provider_boundary_issue(
                "snapshot_dataset_unavailable",
                identity,
                "The frozen public-release dataset is no longer readable from the source table.",
                &json!({"expectedCanonicalContentHash": snapshot_entry.canonical_content_hash}),
            ));
            result.incomplete_identities.insert(identity.clone());
            continue;
        };
        let actual_hash = canonical_json_sha256(&document.payload)?;
        if actual_hash != snapshot_entry.canonical_content_hash {
            result.issues.push(provider_boundary_issue(
                    "snapshot_source_drift",
                    identity,
                    "Live content no longer matches the canonical hash frozen in the public-release manifest.",
                    &json!({
                        "expectedCanonicalContentHash": snapshot_entry.canonical_content_hash,
                        "actualCanonicalContentHash": actual_hash,
                    }),
                ));
            result.incomplete_identities.insert(identity.clone());
            continue;
        }
        result.documents.push(document);
    }
    Ok(result)
}

fn resolve_snapshot_omitted_version(
    snapshot_universe: &BTreeMap<ExactDatasetIdentity, SnapshotDatasetEntry>,
    category: DatasetCategory,
    id: Uuid,
    policy: &str,
) -> anyhow::Result<OmittedVersionResolution> {
    if policy == "reject" {
        return Ok(OmittedVersionResolution {
            selected: None,
            candidates: Vec::new(),
            policy: policy.to_owned(),
        });
    }
    if policy != "latest_eligible" {
        return Err(anyhow::anyhow!(
            "unsupported legacy omitted-version policy: {policy}"
        ));
    }
    let mut candidates = snapshot_universe
        .keys()
        .filter(|identity| identity.category == category && identity.id == id)
        .cloned()
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.version.cmp(&right.version));
    Ok(OmittedVersionResolution {
        selected: candidates.last().cloned(),
        candidates,
        policy: policy.to_owned(),
    })
}

pub async fn load_scope_closure_worker_input(
    pool: &PgPool,
    closure_check_id: Uuid,
) -> anyhow::Result<ScopeClosureWorkerInput> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_check_get_worker_input($1) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_check_get_worker_input")?;
    let data = result
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("closure worker input RPC omitted data"))?;
    let input = serde_json::from_value::<ScopeClosureWorkerInput>(data)?;
    validate_worker_input(&input)?;
    validate_worker_input_hashes(pool, &input).await?;
    Ok(input)
}

#[allow(clippy::too_many_lines)]
pub fn validate_worker_input(input: &ScopeClosureWorkerInput) -> anyhow::Result<()> {
    let snapshot = parse_data_snapshot_manifest(&input.data_snapshot_manifest)?;
    if input.requested_scope.roots().is_empty() {
        return Err(anyhow::anyhow!(
            "requested closure scope has no exact roots"
        ));
    }
    if input.requested_scope.version_resolution_policy != "reference-version-resolution-v1" {
        return Err(anyhow::anyhow!(
            "scope closure requires versionResolutionPolicy=reference-version-resolution-v1"
        ));
    }
    if !matches!(
        input
            .requested_scope
            .link_policy
            .provider_universe_policy
            .as_str(),
        "scope_only" | "eligible_transitive_expansion-v1"
    ) {
        return Err(anyhow::anyhow!(
            "unsupported provider universe policy: {}",
            input.requested_scope.link_policy.provider_universe_policy
        ));
    }
    if !matches!(
        input.requested_scope.legacy_omitted_version_policy.as_str(),
        "reject" | "latest_eligible"
    ) {
        return Err(anyhow::anyhow!(
            "unsupported legacy omitted-version policy: {}",
            input.requested_scope.legacy_omitted_version_policy
        ));
    }
    for root in input.requested_scope.roots() {
        validate_version(root.version.as_str())?;
    }
    for (name, value) in [
        ("requestedScopeHash", input.requested_scope_hash.as_str()),
        ("policyFingerprint", input.policy_fingerprint.as_str()),
        ("dataSnapshotToken", input.data_snapshot_token.as_str()),
        (
            "expectedValidatorScannerFingerprint",
            input.expected_validator_scanner_fingerprint.as_str(),
        ),
        ("requestFingerprint", input.request_fingerprint.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(anyhow::anyhow!("closure worker input requires {name}"));
        }
    }
    if snapshot.schema_version != "lcia.scope-closure-data-snapshot.v2" {
        return Err(anyhow::anyhow!(
            "unsupported immutable data snapshot schema: {}",
            snapshot.schema_version
        ));
    }
    if snapshot.requested_scope != input.requested_scope {
        return Err(anyhow::anyhow!(
            "immutable data snapshot manifest differs from requested scope"
        ));
    }
    if input.data_snapshot_manifest_hash.trim().is_empty() || input.publication_epoch < 0 {
        return Err(anyhow::anyhow!("invalid immutable data snapshot metadata"));
    }
    if input.data_snapshot_manifest.get("requestedScope").is_none() {
        return Err(anyhow::anyhow!(
            "immutable data snapshot omits requestedScope"
        ));
    }
    if snapshot
        .current_public_release
        .release_manifest_hash
        .trim()
        .is_empty()
    {
        return Err(anyhow::anyhow!(
            "immutable data snapshot omits the public release manifest hash"
        ));
    }
    let mut identities = BTreeSet::new();
    for entry in &snapshot.datasets {
        validate_version(entry.dataset_version.as_str())?;
        if !identities.insert(entry.identity()) {
            return Err(anyhow::anyhow!(
                "immutable data snapshot contains a duplicate exact dataset identity"
            ));
        }
        for (name, hash) in [
            (
                "versionSignificantHash",
                entry.version_significant_hash.as_str(),
            ),
            ("semanticHash", entry.semantic_hash.as_str()),
            (
                "canonicalContentHash",
                entry.canonical_content_hash.as_str(),
            ),
        ] {
            if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(anyhow::anyhow!(
                    "immutable data snapshot dataset has invalid {name}"
                ));
            }
        }
    }
    let universe = snapshot_dataset_universe(&snapshot);
    for root in input.requested_scope.roots() {
        if let Some(entry) = universe.get(&root)
            && root.category == DatasetCategory::Processes
            && entry.role != "unit_process"
        {
            return Err(anyhow::anyhow!(
                "requested process root is not a unit_process in the frozen public release: {}",
                root.document_key()
            ));
        }
    }
    Ok(())
}

fn parse_data_snapshot_manifest(value: &Value) -> anyhow::Result<DataSnapshotManifest> {
    serde_json::from_value(value.clone())
        .map_err(|error| anyhow::anyhow!("invalid immutable data snapshot manifest: {error}"))
}

async fn validate_worker_input_hashes(
    pool: &PgPool,
    input: &ScopeClosureWorkerInput,
) -> anyhow::Result<()> {
    // These bindings originate from PostgreSQL `jsonb::text`, whose spacing is
    // deliberately part of the database hash contract.  Recompute with the
    // authoritative SQL helper instead of assuming Rust's byte encoding.
    let requested_scope = input
        .data_snapshot_manifest
        .get("requestedScope")
        .ok_or_else(|| anyhow::anyhow!("immutable data snapshot omits requestedScope"))?;
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.lcia_scope_closure_sha256($1::jsonb) AS requested_scope_hash,
               public.lcia_scope_closure_sha256($2::jsonb) AS snapshot_manifest_hash
        FROM _service_role
        ",
    )
    .bind(requested_scope)
    .bind(&input.data_snapshot_manifest)
    .fetch_one(pool)
    .await?;
    let requested_scope_hash = row.try_get::<String, _>("requested_scope_hash")?;
    let snapshot_manifest_hash = row.try_get::<String, _>("snapshot_manifest_hash")?;
    if requested_scope_hash != input.requested_scope_hash {
        return Err(anyhow::anyhow!("requested scope hash mismatch"));
    }
    if snapshot_manifest_hash != input.data_snapshot_manifest_hash
        || snapshot_manifest_hash != input.data_snapshot_token
    {
        return Err(anyhow::anyhow!(
            "immutable data snapshot token/hash mismatch"
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn collect_scope_closure<P: ScopeClosureProvider>(
    provider: &P,
    manifest: &RequestedScopeManifest,
) -> anyhow::Result<ScopeClosureScan> {
    let roots = manifest.roots();
    let root_set = roots.iter().cloned().collect::<BTreeSet<_>>();
    let mut queue = roots.iter().cloned().collect::<VecDeque<_>>();
    let mut scheduled = root_set.clone();
    let mut documents = BTreeMap::<ExactDatasetIdentity, ClosureDocument>::new();
    let mut graph = BTreeMap::<ExactDatasetIdentity, BTreeSet<ExactDatasetIdentity>>::new();
    let mut edges = Vec::new();
    let mut resolved_references = Vec::<ResolvedReference>::new();
    let mut omitted_version_resolutions = Vec::new();
    let mut raw_issues = Vec::<ClosureIssue>::new();
    let mut complete = true;

    while !queue.is_empty() {
        provider
            .checkpoint(documents.len(), scheduled.len())
            .await?;
        let batch = (0..FETCH_BATCH_SIZE)
            .filter_map(|_| queue.pop_front())
            .collect::<Vec<_>>();
        let fetched = provider.fetch_exact(&batch).await?;
        if !fetched.incomplete_identities.is_empty() {
            complete = false;
        }
        raw_issues.extend(fetched.issues);
        let incomplete_identities = fetched.incomplete_identities;
        let fetched_map = fetched
            .documents
            .into_iter()
            .map(|document| (document.identity.clone(), document))
            .collect::<BTreeMap<_, _>>();

        for requested in batch {
            let Some(document) = fetched_map.get(&requested).cloned() else {
                if !incomplete_identities.contains(&requested) {
                    let explicitly_requested = resolved_references.iter().any(|reference| {
                        reference.target == requested
                            && reference.requested_version_state == "explicit"
                    });
                    raw_issues.push(missing_dataset_issue(&requested, explicitly_requested));
                }
                continue;
            };
            let extraction = extract_references(
                document.identity.document_key().as_str(),
                document.identity.category,
                &document.payload,
            );
            raw_issues.extend(
                extraction
                    .issues
                    .iter()
                    .map(|issue| extraction_issue(&document.identity, issue)),
            );
            for edge in extraction.edges {
                let target_category = parse_category(edge.target_category.as_str())?;
                let target_id = Uuid::parse_str(edge.target_uuid.as_str()).ok();
                let target = match (
                    target_id,
                    edge.requested_version_state.as_str(),
                    edge.requested_version.as_deref(),
                ) {
                    (Some(id), "explicit", Some(version)) => Some(ExactDatasetIdentity {
                        category: target_category,
                        id,
                        version: normalize_exact_version(version)?,
                    }),
                    (Some(id), "omitted", _) => {
                        let resolution = provider
                            .resolve_omitted_version(
                                target_category,
                                id,
                                manifest.legacy_omitted_version_policy.as_str(),
                            )
                            .await?;
                        omitted_version_resolutions.push(json!({
                            "source": document.identity,
                            "jsonPath": edge.json_path,
                            "referenceRole": edge.reference_role,
                            "targetCategory": target_category,
                            "targetId": id,
                            "policy": resolution.policy,
                            "candidateUniverse": "frozen-public-release-manifest",
                            "candidates": resolution.candidates,
                            "selected": resolution.selected,
                        }));
                        resolution.selected
                    }
                    _ => None,
                };
                if edge.requested_version_state == "omitted" && target.is_none() {
                    raw_issues.push(omitted_version_issue(&document.identity, &edge, target_id));
                }
                if let Some(target) = target {
                    if target.category == DatasetCategory::Processes
                        && !root_set.contains(&target)
                        && manifest.link_policy.provider_universe_policy == "scope_only"
                    {
                        raw_issues.push(provider_outside_universe_issue(
                            &document.identity,
                            &target,
                            &edge,
                        ));
                        edges.push(edge);
                        continue;
                    }
                    graph
                        .entry(document.identity.clone())
                        .or_default()
                        .insert(target.clone());
                    resolved_references.push(ResolvedReference {
                        source: document.identity.clone(),
                        target: target.clone(),
                        json_path: edge.json_path.clone(),
                        reference_role: edge.reference_role.clone(),
                        requested_version_state: edge.requested_version_state.clone(),
                    });
                    if scheduled.insert(target.clone()) {
                        queue.push_back(target);
                    }
                }
                edges.push(edge);
            }
            documents.insert(document.identity.clone(), document);
        }
    }

    edges.sort_by_key(canonical_value);
    resolved_references.sort();
    omitted_version_resolutions.sort_by_key(canonical_value);
    attach_reference_occurrences(&mut raw_issues, &resolved_references);
    let mut issues = coalesce_issues(raw_issues);
    for issue in &mut issues {
        if let Some(source) = issue.source.as_ref() {
            let (affected, witnesses) = affected_roots_and_witness(&roots, &graph, source);
            issue.affected_roots = affected;
            issue.witness_path = witnesses.first().cloned().unwrap_or_default();
            issue.affected_root_witness_paths = witnesses;
        }
    }
    issues.sort_by_key(canonical_value);

    Ok(ScopeClosureScan {
        schema_version: "lcia.scope-closure-scan.v1".to_owned(),
        // A row absent from the frozen release is a complete negative finding.
        // An allowlisted row that is missing or hash-drifted is incomplete because
        // the claimed immutable source could not actually be observed.
        complete,
        roots,
        documents: documents.into_values().collect(),
        edges,
        resolved_references,
        omitted_version_resolutions,
        issues,
        frontier: Vec::new(),
        provider_universe: scheduled.into_iter().collect(),
    })
}

fn attach_reference_occurrences(issues: &mut [ClosureIssue], references: &[ResolvedReference]) {
    for issue in issues {
        let Some(target) = issue.source.as_ref().filter(|source| {
            issue.requested_target_id == Some(source.id)
                && issue.requested_target_version.as_deref() == Some(source.version.as_str())
        }) else {
            continue;
        };
        let mut occurrences = references
            .iter()
            .filter(|reference| &reference.target == target)
            .map(|reference| ClosureIssueOccurrence {
                occurrence_key: canonical_json_sha256(&json!({
                    "issueKey": issue.issue_key,
                    "source": reference.source,
                    "jsonPath": reference.json_path,
                    "referenceRole": reference.reference_role,
                }))
                .unwrap_or_else(|_| Uuid::new_v4().simple().to_string()),
                source: Some(reference.source.clone()),
                json_path: Some(reference.json_path.clone()),
                reference_role: Some(reference.reference_role.clone()),
                details: json!({
                    "requestedVersionState": reference.requested_version_state,
                    "target": reference.target,
                }),
            })
            .collect::<Vec<_>>();
        occurrences.sort_by(|left, right| left.occurrence_key.cmp(&right.occurrence_key));
        occurrences.dedup_by(|left, right| left.occurrence_key == right.occurrence_key);
        if !occurrences.is_empty() {
            issue.occurrence_count = u32::try_from(occurrences.len()).unwrap_or(u32::MAX);
            issue.reference_role = occurrences
                .first()
                .and_then(|occurrence| occurrence.reference_role.clone());
            issue.occurrences = occurrences;
        }
    }
}

fn affected_roots_and_witness(
    roots: &[ExactDatasetIdentity],
    graph: &BTreeMap<ExactDatasetIdentity, BTreeSet<ExactDatasetIdentity>>,
    target: &ExactDatasetIdentity,
) -> (Vec<ExactDatasetIdentity>, Vec<Vec<ExactDatasetIdentity>>) {
    let mut affected = Vec::new();
    let mut witnesses = Vec::new();
    for root in roots {
        let mut queue = VecDeque::from([(root.clone(), vec![root.clone()])]);
        let mut visited = BTreeSet::new();
        while let Some((node, path)) = queue.pop_front() {
            if !visited.insert(node.clone()) {
                continue;
            }
            if &node == target {
                affected.push(root.clone());
                witnesses.push(path);
                break;
            }
            if let Some(children) = graph.get(&node) {
                for child in children {
                    let mut next_path = path.clone();
                    next_path.push(child.clone());
                    queue.push_back((child.clone(), next_path));
                }
            }
        }
    }
    (affected, witnesses)
}

fn populate_affected_roots(scan: &mut ScopeClosureScan) {
    let mut graph = BTreeMap::<ExactDatasetIdentity, BTreeSet<ExactDatasetIdentity>>::new();
    for reference in &scan.resolved_references {
        graph
            .entry(reference.source.clone())
            .or_default()
            .insert(reference.target.clone());
    }
    for issue in &mut scan.issues {
        if let Some(source) = issue.source.as_ref() {
            let (affected, witnesses) = affected_roots_and_witness(&scan.roots, &graph, source);
            issue.affected_roots = affected;
            issue.witness_path = witnesses.first().cloned().unwrap_or_default();
            issue.affected_root_witness_paths = witnesses;
        }
    }
    scan.issues.sort_by_key(canonical_value);
}

#[must_use]
pub fn extract_references(
    document_key: &str,
    category: DatasetCategory,
    payload: &Value,
) -> ReferenceExtractionResult {
    let mut result = ReferenceExtractionResult {
        schema_version: "tidas.reference-extraction-result.v1".to_owned(),
        document_key: document_key.to_owned(),
        source_category: category.table_name().to_owned(),
        edges: Vec::new(),
        issues: Vec::new(),
    };
    walk_references(payload, "$", None, category, &mut result);
    result
}

fn walk_references(
    node: &Value,
    path: &str,
    parent_key: Option<&str>,
    source_category: DatasetCategory,
    result: &mut ReferenceExtractionResult,
) {
    match node {
        Value::Object(object) => {
            if looks_like_reference(object, parent_key) {
                extract_reference(object, path, parent_key, source_category, result);
            }
            for (key, value) in object {
                walk_references(
                    value,
                    format!("{path}.{key}").as_str(),
                    Some(key),
                    source_category,
                    result,
                );
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                walk_references(
                    item,
                    format!("{path}[{index}]").as_str(),
                    parent_key,
                    source_category,
                    result,
                );
            }
        }
        _ => {}
    }
}

fn looks_like_reference(object: &Map<String, Value>, parent_key: Option<&str>) -> bool {
    object.contains_key("@refObjectId")
        || object.contains_key("@uri")
        || parent_key.is_some_and(|key| key.to_ascii_lowercase().contains("referenceto"))
}

fn extract_reference(
    object: &Map<String, Value>,
    path: &str,
    parent_key: Option<&str>,
    source_category: DatasetCategory,
    result: &mut ReferenceExtractionResult,
) {
    let raw_type = object.get("@type").cloned().unwrap_or(Value::Null);
    let uri = object.get("@uri").cloned().unwrap_or(Value::Null);
    let target_category = raw_type
        .as_str()
        .and_then(DatasetCategory::from_reference_type)
        .or_else(|| uri.as_str().and_then(DatasetCategory::from_uri));
    let role = reference_role(source_category, path, parent_key, target_category.as_ref());

    if target_category.is_none() {
        result.issues.push(reference_issue(
            result,
            "reference_type_unresolved",
            path,
            role,
            "Reference target type cannot be resolved from @type or @uri.",
            json!({"raw_type": raw_type, "uri": uri}),
        ));
    }

    let Some(raw_id) = object
        .get("@refObjectId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        result.issues.push(reference_issue(
            result,
            "reference_object_id_missing",
            path,
            role,
            "Recognized reference is missing a non-empty @refObjectId.",
            json!({
                "raw_ref_object_id": object.get("@refObjectId").cloned().unwrap_or(Value::Null),
                "raw_type": raw_type,
                "uri": uri,
            }),
        ));
        return;
    };

    if Uuid::parse_str(raw_id).is_err() || raw_id.to_ascii_lowercase() != raw_id {
        result.issues.push(reference_issue(
            result,
            "reference_uuid_invalid",
            path,
            role,
            "Reference @refObjectId is not a canonical lowercase UUID.",
            json!({"raw_ref_object_id": raw_id}),
        ));
    }

    let raw_version = object.get("@version").cloned().unwrap_or(Value::Null);
    let (version_state, requested_version) = match &raw_version {
        Value::Null => ("omitted", None),
        Value::String(version) if validate_version(version).is_ok() => {
            ("explicit", Some(version.clone()))
        }
        value => {
            result.issues.push(reference_issue(
                result,
                "reference_version_invalid",
                path,
                role,
                "Reference @version must match NN.NN or NN.NN.NNN.",
                json!({"requested_version_raw": value}),
            ));
            ("invalid", value.as_str().map(str::to_owned))
        }
    };

    if let Some(target_category) = target_category {
        result.edges.push(ReferenceEdge {
            schema_version: REFERENCE_EDGE_SCHEMA_VERSION.to_owned(),
            document_key: result.document_key.clone(),
            source_category: source_category.table_name().to_owned(),
            target_category: target_category.table_name().to_owned(),
            target_uuid: raw_id.to_owned(),
            requested_version_state: version_state.to_owned(),
            requested_version,
            requested_version_raw: raw_version,
            reference_role: role.to_owned(),
            json_path: path.to_owned(),
            raw_type,
            uri,
        });
    }
}

fn reference_issue(
    result: &ReferenceExtractionResult,
    issue_code: &str,
    json_path: &str,
    reference_role: &str,
    message: &str,
    details: Value,
) -> ReferenceExtractionIssue {
    ReferenceExtractionIssue {
        schema_version: REFERENCE_ISSUE_SCHEMA_VERSION.to_owned(),
        issue_code: issue_code.to_owned(),
        severity: "error".to_owned(),
        document_key: result.document_key.clone(),
        source_category: result.source_category.clone(),
        json_path: json_path.to_owned(),
        reference_role: reference_role.to_owned(),
        message: message.to_owned(),
        details,
    }
}

fn reference_role<'a>(
    source_category: DatasetCategory,
    path: &str,
    parent_key: Option<&str>,
    target_category: Option<&DatasetCategory>,
) -> &'a str {
    let normalized_path = path.to_ascii_lowercase();
    let normalized_key = parent_key.unwrap_or_default().to_ascii_lowercase();
    if source_category == DatasetCategory::Processes
        && target_category == Some(&DatasetCategory::Flows)
        && normalized_path.contains("exchange")
        && normalized_key == "referencetoflowdataset"
    {
        "process_exchange_flow"
    } else if source_category == DatasetCategory::Lciamethods
        && target_category == Some(&DatasetCategory::Flows)
        && (normalized_path.contains("characterisation")
            || normalized_path.contains("characterization"))
    {
        "lcia_factor_flow"
    } else if source_category == DatasetCategory::Lifecyclemodels
        && target_category == Some(&DatasetCategory::Processes)
    {
        "lifecycle_model_process"
    } else {
        "support_document"
    }
}

fn normalize_reference_type(raw: &str) -> String {
    raw.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn validate_version(version: &str) -> anyhow::Result<()> {
    let components = version.split('.').collect::<Vec<_>>();
    let valid = matches!(components.as_slice(), [a, b] if a.len() == 2 && b.len() == 2 && a.chars().all(|c| c.is_ascii_digit()) && b.chars().all(|c| c.is_ascii_digit()))
        || matches!(components.as_slice(), [a, b, c] if a.len() == 2 && b.len() == 2 && c.len() == 3 && a.chars().all(|v| v.is_ascii_digit()) && b.chars().all(|v| v.is_ascii_digit()) && c.chars().all(|v| v.is_ascii_digit()));
    if valid {
        Ok(())
    } else {
        Err(anyhow::anyhow!("invalid exact TIDAS version: {version}"))
    }
}

fn normalize_exact_version(version: &str) -> anyhow::Result<String> {
    validate_version(version)?;
    if version.matches('.').count() == 1 {
        Ok(format!("{version}.000"))
    } else {
        Ok(version.to_owned())
    }
}

fn parse_category(value: &str) -> anyhow::Result<DatasetCategory> {
    match value {
        "contacts" => Ok(DatasetCategory::Contacts),
        "flowproperties" => Ok(DatasetCategory::Flowproperties),
        "flows" => Ok(DatasetCategory::Flows),
        "lciamethods" => Ok(DatasetCategory::Lciamethods),
        "lifecyclemodels" => Ok(DatasetCategory::Lifecyclemodels),
        "processes" => Ok(DatasetCategory::Processes),
        "sources" => Ok(DatasetCategory::Sources),
        "unitgroups" => Ok(DatasetCategory::Unitgroups),
        _ => Err(anyhow::anyhow!(
            "unsupported closure dataset category: {value}"
        )),
    }
}

fn extraction_issue(
    source: &ExactDatasetIdentity,
    issue: &ReferenceExtractionIssue,
) -> ClosureIssue {
    let issue_key = canonical_json_sha256(&json!({
        "code": issue.issue_code,
        "source": source,
        "path": issue.json_path,
        "role": issue.reference_role,
    }))
    .unwrap_or_else(|_| Uuid::new_v4().simple().to_string());
    ClosureIssue {
        issue_key: issue_key.clone(),
        severity: "blocker".to_owned(),
        blocking: true,
        issue_code: issue.issue_code.clone(),
        source: Some(source.clone()),
        json_path: Some(issue.json_path.clone()),
        reference_role: Some(issue.reference_role.clone()),
        requested_target_type: None,
        requested_target_id: None,
        requested_target_version: None,
        message: issue.message.clone(),
        suggested_action: Some(
            "Repair the source reference and rerun closure preflight.".to_owned(),
        ),
        occurrence_count: 1,
        occurrences: vec![ClosureIssueOccurrence {
            occurrence_key: format!("{issue_key}:0"),
            source: Some(source.clone()),
            json_path: Some(issue.json_path.clone()),
            reference_role: Some(issue.reference_role.clone()),
            details: issue.details.clone(),
        }],
        affected_roots: Vec::new(),
        affected_root_witness_paths: Vec::new(),
        witness_path: Vec::new(),
    }
}

fn missing_dataset_issue(
    target: &ExactDatasetIdentity,
    explicitly_requested: bool,
) -> ClosureIssue {
    let issue_code = if explicitly_requested {
        "reference_exact_version_missing"
    } else {
        "reference_target_missing"
    };
    let issue_key = canonical_json_sha256(&json!({"code": issue_code, "target": target}))
        .unwrap_or_else(|_| Uuid::new_v4().simple().to_string());
    ClosureIssue {
        issue_key: issue_key.clone(),
        severity: "blocker".to_owned(),
        blocking: true,
        issue_code: issue_code.to_owned(),
        source: Some(target.clone()),
        json_path: None,
        reference_role: None,
        requested_target_type: Some(target.category.table_name().to_owned()),
        requested_target_id: Some(target.id),
        requested_target_version: Some(target.version.clone()),
        message: format!(
            "Exact referenced dataset {} was not found.",
            target.document_key()
        ),
        suggested_action: Some("Publish or repair the exact referenced revision.".to_owned()),
        occurrence_count: 1,
        occurrences: vec![ClosureIssueOccurrence {
            occurrence_key: format!("{issue_key}:0"),
            source: Some(target.clone()),
            json_path: None,
            reference_role: None,
            details: json!({}),
        }],
        affected_roots: Vec::new(),
        affected_root_witness_paths: Vec::new(),
        witness_path: Vec::new(),
    }
}

fn provider_boundary_issue(
    code: &str,
    identity: &ExactDatasetIdentity,
    message: &str,
    details: &Value,
) -> ClosureIssue {
    let issue_key = canonical_json_sha256(&json!({
        "code": code,
        "identity": identity,
    }))
    .unwrap_or_else(|_| Uuid::new_v4().simple().to_string());
    let evidence = canonical_value(&details);
    ClosureIssue {
        issue_key: issue_key.clone(),
        severity: "blocker".to_owned(),
        blocking: true,
        issue_code: code.to_owned(),
        source: Some(identity.clone()),
        json_path: None,
        reference_role: None,
        requested_target_type: Some(identity.category.table_name().to_owned()),
        requested_target_id: Some(identity.id),
        requested_target_version: Some(identity.version.clone()),
        message: format!("{message} Evidence: {evidence}"),
        suggested_action: Some(
            "Recreate the closure request from a consistent published release snapshot.".to_owned(),
        ),
        occurrence_count: 1,
        occurrences: vec![ClosureIssueOccurrence {
            occurrence_key: format!("{issue_key}:0"),
            source: Some(identity.clone()),
            json_path: None,
            reference_role: None,
            details: details.clone(),
        }],
        affected_roots: Vec::new(),
        affected_root_witness_paths: Vec::new(),
        witness_path: Vec::new(),
    }
}

fn provider_outside_universe_issue(
    source: &ExactDatasetIdentity,
    target: &ExactDatasetIdentity,
    edge: &ReferenceEdge,
) -> ClosureIssue {
    let issue_key = canonical_json_sha256(&json!({
        "code": "provider_outside_scope_universe",
        "source": source,
        "path": edge.json_path,
        "target": target,
    }))
    .unwrap_or_else(|_| Uuid::new_v4().simple().to_string());
    ClosureIssue {
        issue_key: issue_key.clone(),
        severity: "blocker".to_owned(),
        blocking: true,
        issue_code: "provider_outside_scope_universe".to_owned(),
        source: Some(source.clone()),
        json_path: Some(edge.json_path.clone()),
        reference_role: Some(edge.reference_role.clone()),
        requested_target_type: Some(target.category.table_name().to_owned()),
        requested_target_id: Some(target.id),
        requested_target_version: Some(target.version.clone()),
        message: "Referenced process is outside the frozen scope-only provider universe."
            .to_owned(),
        suggested_action: Some(
            "Include the provider as a root or use the tracked transitive-expansion policy."
                .to_owned(),
        ),
        occurrence_count: 1,
        occurrences: vec![ClosureIssueOccurrence {
            occurrence_key: format!("{issue_key}:0"),
            source: Some(source.clone()),
            json_path: Some(edge.json_path.clone()),
            reference_role: Some(edge.reference_role.clone()),
            details: json!({"target": target}),
        }],
        affected_roots: Vec::new(),
        affected_root_witness_paths: Vec::new(),
        witness_path: Vec::new(),
    }
}

fn omitted_version_issue(
    source: &ExactDatasetIdentity,
    edge: &ReferenceEdge,
    target_id: Option<Uuid>,
) -> ClosureIssue {
    let issue_key = canonical_json_sha256(&json!({
        "code": "reference_version_omitted",
        "source": source,
        "path": edge.json_path,
        "target": edge.target_uuid,
    }))
    .unwrap_or_else(|_| Uuid::new_v4().simple().to_string());
    ClosureIssue {
        issue_key: issue_key.clone(),
        severity: "blocker".to_owned(),
        blocking: true,
        issue_code: "reference_version_omitted".to_owned(),
        source: Some(source.clone()),
        json_path: Some(edge.json_path.clone()),
        reference_role: Some(edge.reference_role.clone()),
        requested_target_type: Some(edge.target_category.clone()),
        requested_target_id: target_id,
        requested_target_version: None,
        message: "Reference omits @version and the selected policy did not resolve it.".to_owned(),
        suggested_action: Some("Bind the reference to an exact published version.".to_owned()),
        occurrence_count: 1,
        occurrences: vec![ClosureIssueOccurrence {
            occurrence_key: format!("{issue_key}:0"),
            source: Some(source.clone()),
            json_path: Some(edge.json_path.clone()),
            reference_role: Some(edge.reference_role.clone()),
            details: json!({"targetId": target_id}),
        }],
        affected_roots: Vec::new(),
        affected_root_witness_paths: Vec::new(),
        witness_path: Vec::new(),
    }
}

fn coalesce_issues(issues: Vec<ClosureIssue>) -> Vec<ClosureIssue> {
    let mut output = BTreeMap::<String, ClosureIssue>::new();
    for mut issue in issues {
        issue
            .occurrences
            .sort_by(|left, right| left.occurrence_key.cmp(&right.occurrence_key));
        issue
            .occurrences
            .dedup_by(|left, right| left.occurrence_key == right.occurrence_key);
        issue.occurrence_count = u32::try_from(issue.occurrences.len()).unwrap_or(u32::MAX);
        output
            .entry(issue.issue_key.clone())
            .and_modify(|existing| {
                existing.occurrences.extend(issue.occurrences.clone());
                existing
                    .occurrences
                    .sort_by(|left, right| left.occurrence_key.cmp(&right.occurrence_key));
                existing
                    .occurrences
                    .dedup_by(|left, right| left.occurrence_key == right.occurrence_key);
                existing.occurrence_count =
                    u32::try_from(existing.occurrences.len()).unwrap_or(u32::MAX);
            })
            .or_insert(issue);
    }
    output.into_values().collect()
}

async fn fetch_exact_documents(
    pool: &PgPool,
    identities: &[ExactDatasetIdentity],
) -> anyhow::Result<Vec<ClosureDocument>> {
    let mut grouped = BTreeMap::<DatasetCategory, Vec<&ExactDatasetIdentity>>::new();
    for identity in identities {
        grouped.entry(identity.category).or_default().push(identity);
    }
    let mut documents = Vec::new();
    for (category, group) in grouped {
        let table = category.table_name();
        let document_expression = if category == DatasetCategory::Lciamethods {
            "COALESCE(json, json_ordered::jsonb)"
        } else {
            "json_ordered::jsonb"
        };
        let read_keys = group
            .iter()
            .map(|identity| {
                let locator_id = if category == DatasetCategory::Lciamethods {
                    lcia_method_artifact_locator_id(identity)
                } else {
                    identity.id
                };
                (identity, locator_id)
            })
            .collect::<Vec<_>>();
        let mut builder = QueryBuilder::<Postgres>::new(format!(
            "SELECT id, btrim(version::text) AS version, {document_expression} AS document FROM public.{table} WHERE (id, btrim(version::text)) IN ("
        ));
        let mut separated = builder.separated(", ");
        for (identity, locator_id) in &read_keys {
            separated
                .push("(")
                .push_bind(*locator_id)
                .push(", ")
                .push_bind(identity.version.clone())
                .push(")");
        }
        separated.push_unseparated(") ORDER BY id, btrim(version::text)");
        let rows = builder.build().fetch_all(pool).await?;
        for row in rows {
            let locator_id = row.try_get::<Uuid, _>("id")?;
            let version = row.try_get::<String, _>("version")?;
            let requested = read_keys
                .iter()
                .find(|(identity, expected_locator)| {
                    *expected_locator == locator_id && identity.version == version
                })
                .map(|(identity, _)| (**identity).clone())
                .ok_or_else(|| anyhow::anyhow!("LCIA/source fetch returned an unexpected row"))?;
            documents.push(ClosureDocument {
                identity: requested,
                payload: row.try_get("document")?,
            });
        }
    }
    documents.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(documents)
}

fn lcia_method_artifact_locator_id(identity: &ExactDatasetIdentity) -> Uuid {
    RELEASE_METHOD_IDENTITIES
        .iter()
        .find(|(method_id, version, _)| {
            Uuid::parse_str(method_id) == Ok(identity.id) && *version == identity.version.as_str()
        })
        .and_then(|(_, _, locator_id)| Uuid::parse_str(locator_id).ok())
        .unwrap_or(identity.id)
}

pub fn canonical_json_sha256<T: Serialize>(value: &T) -> anyhow::Result<String> {
    let value = serde_json::to_value(value)?;
    let mut encoded = Vec::new();
    write_canonical_json(&value, &mut encoded)?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

fn canonical_value<T: Serialize>(value: &T) -> String {
    canonical_json_bytes(value)
        .map(|bytes| String::from_utf8_lossy(bytes.as_slice()).into_owned())
        .unwrap_or_default()
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    let value = serde_json::to_value(value)?;
    let mut output = Vec::new();
    write_canonical_json(&value, &mut output)?;
    Ok(output)
}

fn write_canonical_json(value: &Value, output: &mut Vec<u8>) -> anyhow::Result<()> {
    match value {
        Value::Object(object) => {
            output.push(b'{');
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            for (index, (key, item)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key)?;
                output.push(b':');
                write_canonical_json(item, output)?;
            }
            output.push(b'}');
        }
        Value::Array(items) => {
            output.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_json(item, output)?;
            }
            output.push(b']');
        }
        _ => serde_json::to_writer(output, value)?,
    }
    Ok(())
}

fn ensure_rpc_ok(result: &Value, name: &str) -> anyhow::Result<()> {
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(())
    } else {
        Err(anyhow::anyhow!("{name} returned non-ok result: {result}"))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeClosureEvidence {
    pub schema_version: String,
    pub source_fingerprint: String,
    pub resolution_map_hash: String,
    pub closure_bundle_hash: String,
    pub snapshot_id: Uuid,
    pub snapshot_hash: String,
    pub report_artifact_manifest_hash: String,
    pub evidence_hash: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeClosureExecutionResult {
    pub closure_check_id: Uuid,
    pub worker_job_id: Uuid,
    pub status: String,
    pub scan_completeness: String,
    pub certificate_hash: Option<String>,
    pub evidence: ScopeClosureEvidence,
    pub report_artifact_id: Uuid,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ScanExecutionClaim {
    Acquired,
    Busy,
    Completed { completed_check_id: Uuid },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactManifestEntry {
    artifact_type: String,
    file_name: String,
    content_type: String,
    byte_size: usize,
    checksum_sha256: String,
}

#[derive(Debug, Clone)]
struct PreparedArtifact {
    descriptor: ArtifactManifestEntry,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TidasBatchValidation {
    describe: Value,
    final_event: Value,
    issue_events: Vec<Value>,
}

/// Executes a leased closure job and atomically projects its terminal result.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn execute_scope_closure_job(
    state: &AppState,
    worker_job_id: Uuid,
    lease_token: Uuid,
    lease_seconds: i32,
    closure_check_id: Uuid,
    scan_execution_id: Uuid,
    data_snapshot_token: &str,
    request_fingerprint: &str,
) -> anyhow::Result<ScopeClosureExecutionResult> {
    let progress = WorkerJobProgress::new(&state.pool, worker_job_id, lease_token, lease_seconds);
    progress
        .heartbeat(
            "load_scope",
            0.08,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {"scanned": 0, "total": 1, "unit": "scope"},
            })),
        )
        .await?;
    let input = load_scope_closure_worker_input(&state.pool, closure_check_id).await?;
    let data_snapshot_manifest = parse_data_snapshot_manifest(&input.data_snapshot_manifest)?;
    if input.closure_check_id != closure_check_id {
        return Err(anyhow::anyhow!("closure worker input identity mismatch"));
    }
    if input.request_fingerprint != request_fingerprint {
        return Err(anyhow::anyhow!("closure request fingerprint mismatch"));
    }
    if input.scan_execution_id != scan_execution_id
        || input.data_snapshot_token != data_snapshot_token
    {
        return Err(anyhow::anyhow!("closure scan/snapshot envelope mismatch"));
    }
    if input.expected_validator_scanner_fingerprint != "scope-closure-validator-scanner.v1" {
        return Err(anyhow::anyhow!(
            "unsupported validator/scanner fingerprint: {}",
            input.expected_validator_scanner_fingerprint
        ));
    }
    let wait_started = std::time::Instant::now();
    let mut wait_backoff = std::time::Duration::from_secs(1);
    loop {
        match claim_scan_execution(&state.pool, scan_execution_id, worker_job_id, lease_token)
            .await?
        {
            ScanExecutionClaim::Acquired => break,
            ScanExecutionClaim::Completed { completed_check_id } => {
                return reuse_completed_scan_execution(
                    state,
                    closure_check_id,
                    worker_job_id,
                    lease_token,
                    completed_check_id,
                )
                .await;
            }
            ScanExecutionClaim::Busy => {
                if wait_started.elapsed() >= std::time::Duration::from_secs(3_600) {
                    return Err(anyhow::anyhow!("shared_scan_wait_timeout"));
                }
                progress
                    .heartbeat(
                        "waiting_for_shared_scan",
                        0.12,
                        Some(json!({
                            "closureCheckId": closure_check_id,
                            "scanExecutionId": scan_execution_id,
                            "progressCounters": {"scanned": 0, "total": 1, "unit": "sharedScan"},
                        })),
                    )
                    .await?;
                tokio::time::sleep(wait_backoff).await;
                wait_backoff = (wait_backoff * 2).min(std::time::Duration::from_secs(10));
            }
        }
    }

    progress
        .heartbeat(
            "discover_reference_graph",
            0.18,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "scanned": 0,
                    "total": input.requested_scope.roots().len(),
                    "unit": "documents"
                },
            })),
        )
        .await?;
    let provider = PgScopeClosureProvider::new_leased(
        &state.pool,
        &data_snapshot_manifest,
        worker_job_id,
        lease_token,
        lease_seconds,
    );
    let mut scan = collect_scope_closure(&provider, &input.requested_scope).await?;

    progress
        .heartbeat(
            "validate_documents",
            0.46,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "scanned": scan.documents.len(),
                    "total": scan.provider_universe.len(),
                    "unit": "documents"
                },
            })),
        )
        .await?;
    let validation =
        run_tidas_batch_validation_cached(&state.pool, worker_job_id, &scan.documents).await?;
    merge_tidas_validation_issues(&mut scan, &validation.issue_events);

    progress
        .heartbeat(
            "validate_reference_graph",
            0.62,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "scanned": scan.edges.len(),
                    "total": scan.edges.len(),
                    "unit": "references"
                },
            })),
        )
        .await?;
    scan.issues.sort_by_key(canonical_value);

    progress
        .heartbeat(
            "validate_link_readiness",
            0.72,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "scanned": scan.documents.len(),
                    "total": scan.documents.len(),
                    "unit": "datasets"
                },
            })),
        )
        .await?;
    let effective_scope = build_effective_scope_manifest(&input.requested_scope, &scan.documents);
    let source_fingerprint = source_fingerprint(&scan.documents)?;
    let resolution_map = build_resolution_map(&scan.edges, &scan.omitted_version_resolutions);
    let resolution_map_hash = canonical_json_sha256(&resolution_map)?;
    let closure_bundle = json!({
        "schemaVersion": "lcia.scope-closure-bundle.v1",
        "requestedScopeHash": input.requested_scope_hash,
        "policyFingerprint": input.policy_fingerprint,
        "dataSnapshotToken": input.data_snapshot_token,
        "validatorScannerFingerprint": input.expected_validator_scanner_fingerprint,
        "tidasValidation": validation,
        "scan": scan,
        "resolutionMap": resolution_map,
    });
    let closure_bundle_bytes = canonical_json_bytes(&closure_bundle)?;
    let closure_bundle_hash = sha256_hex(&closure_bundle_bytes);
    let snapshot_id = deterministic_uuid_from_hash(&closure_bundle_hash)?;
    let snapshot = json!({
        "schemaVersion": "lcia.scope-closure-snapshot.v1",
        "snapshotId": snapshot_id,
        "dataSnapshotToken": input.data_snapshot_token,
        "effectiveScope": effective_scope,
        "sourceFingerprint": source_fingerprint,
        "resolutionMapHash": resolution_map_hash,
        "closureBundleHash": closure_bundle_hash,
    });
    let snapshot_bytes = canonical_json_bytes(&snapshot)?;
    let snapshot_hash = sha256_hex(&snapshot_bytes);
    let issue_jsonl = build_issue_jsonl(&scan.issues)?;
    let xlsx_report = build_xlsx_report(closure_check_id, &scan.issues)?;

    let mut artifacts = vec![
        prepare_artifact(
            "closure_bundle",
            "closure-bundle-v1.json",
            "application/json",
            closure_bundle_bytes,
        ),
        prepare_artifact(
            "closure_snapshot",
            "closure-snapshot-v1.json",
            "application/json",
            snapshot_bytes,
        ),
        prepare_artifact(
            "closure_issues_jsonl",
            "closure-issues-v1.jsonl",
            "application/x-ndjson",
            issue_jsonl,
        ),
        prepare_artifact(
            "closure_report_xlsx",
            "closure-report-v1.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            xlsx_report,
        ),
    ];
    artifacts.sort_by(|left, right| {
        left.descriptor
            .artifact_type
            .cmp(&right.descriptor.artifact_type)
    });
    let artifact_manifest = artifacts
        .iter()
        .map(|artifact| artifact.descriptor.clone())
        .collect::<Vec<_>>();
    let content_artifact_manifest_hash = canonical_json_sha256(&artifact_manifest)?;

    progress
        .heartbeat(
            "validate_lcia_readiness",
            0.84,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "scanned": input.requested_scope.lcia_methods.len(),
                    "total": input.requested_scope.lcia_methods.len(),
                    "unit": "lciaMethods"
                },
            })),
        )
        .await?;
    let persisted = persist_closure_artifacts(
        state,
        worker_job_id,
        closure_check_id,
        &artifacts,
        content_artifact_manifest_hash.as_str(),
    )
    .await?;
    let report_artifact_id = persisted
        .get("closure_report_xlsx")
        .copied()
        .ok_or_else(|| anyhow::anyhow!("closure XLSX report artifact was not persisted"))?;
    let report_artifact_manifest_hash =
        report_artifact_manifest_hash(&state.pool, report_artifact_id).await?;
    let evidence_without_hash = json!({
        "schemaVersion": "lcia.scope-closure-evidence.v1",
        "sourceFingerprint": source_fingerprint,
        "resolutionMapHash": resolution_map_hash,
        "closureBundleHash": closure_bundle_hash,
        "snapshotId": snapshot_id,
        "snapshotHash": snapshot_hash,
    });
    let evidence_hash = canonical_json_sha256(&evidence_without_hash)?;
    let evidence = ScopeClosureEvidence {
        schema_version: "lcia.scope-closure-evidence.v1".to_owned(),
        source_fingerprint,
        resolution_map_hash,
        closure_bundle_hash,
        snapshot_id,
        snapshot_hash,
        report_artifact_manifest_hash,
        evidence_hash,
    };

    progress
        .heartbeat(
            "finalize_evidence",
            0.95,
            Some(json!({
                "closureCheckId": closure_check_id,
                "progressCounters": {
                    "scanned": artifacts.len(),
                    "total": artifacts.len(),
                    "unit": "artifacts"
                },
            })),
        )
        .await?;
    let blocker_codes = scan.blocker_codes();
    let status = if blocker_codes.is_empty() {
        "passed"
    } else {
        "blocked"
    };
    let scan_completeness = if scan.complete {
        "complete"
    } else {
        "incomplete"
    };
    let result_summary = json!({
        "schemaVersion": "lcia.scope-closure-summary.v1",
        "documentCount": scan.documents.len(),
        "referenceCount": scan.edges.len(),
        "issueCount": scan.issues.len(),
        "blockerCount": scan.issues.iter().filter(|issue| issue.blocking).count(),
        "evidenceHash": evidence.evidence_hash,
        "snapshotId": evidence.snapshot_id,
        "artifacts": persisted,
    });
    let rpc_result = record_scope_closure_result_v2(
        &state.pool,
        closure_check_id,
        worker_job_id,
        lease_token,
        status,
        scan_completeness,
        &effective_scope,
        &evidence,
        &result_summary,
        &scan.issues,
        &blocker_codes,
        report_artifact_id,
    )
    .await?;
    let certificate_hash = rpc_result
        .get("data")
        .and_then(|data| data.get("certificateHash"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok(ScopeClosureExecutionResult {
        closure_check_id,
        worker_job_id,
        status: status.to_owned(),
        scan_completeness: scan_completeness.to_owned(),
        certificate_hash,
        evidence,
        report_artifact_id,
        blocker_codes,
    })
}

async fn claim_scan_execution(
    pool: &PgPool,
    scan_execution_id: Uuid,
    worker_job_id: Uuid,
    lease_token: Uuid,
) -> anyhow::Result<ScanExecutionClaim> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_claim_scan_execution($1, $2, $3) AS result
        FROM _service_role
        ",
    )
    .bind(scan_execution_id)
    .bind(worker_job_id)
    .bind(lease_token)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_claim_scan_execution")?;
    parse_scan_execution_claim(result.get("data").unwrap_or(&Value::Null))
}

fn parse_scan_execution_claim(data: &Value) -> anyhow::Result<ScanExecutionClaim> {
    if data.get("acquired").and_then(Value::as_bool) == Some(true) {
        return Ok(ScanExecutionClaim::Acquired);
    }
    if data.get("completed").and_then(Value::as_bool) == Some(true) {
        let completed_check_id = data
            .get("completedCheckId")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("completed scan claim omitted completedCheckId"))?
            .parse()?;
        return Ok(ScanExecutionClaim::Completed { completed_check_id });
    }
    Ok(ScanExecutionClaim::Busy)
}

#[allow(clippy::too_many_lines)]
async fn reuse_completed_scan_execution(
    state: &AppState,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    lease_token: Uuid,
    completed_check_id: Uuid,
) -> anyhow::Result<ScopeClosureExecutionResult> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_reuse_completed_scan($1, $2, $3, $4) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .bind(worker_job_id)
    .bind(lease_token)
    .bind(completed_check_id)
    .fetch_one(&state.pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_reuse_completed_scan")?;
    let data = result
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("reuse completed scan RPC omitted data"))?;
    if data.get("reuseAvailable").and_then(Value::as_bool) != Some(true) {
        return Err(anyhow::anyhow!("completed scan is not reusable"));
    }
    let issues = load_reused_issues(&state.pool, completed_check_id).await?;
    let xlsx = build_xlsx_report(closure_check_id, &issues)?;
    let artifact = prepare_artifact(
        "closure_report_xlsx",
        "closure-report-v1.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        xlsx,
    );
    let content_manifest_hash = canonical_json_sha256(&vec![artifact.descriptor.clone()])?;
    let persisted = persist_closure_artifacts(
        state,
        worker_job_id,
        closure_check_id,
        std::slice::from_ref(&artifact),
        content_manifest_hash.as_str(),
    )
    .await?;
    let report_artifact_id = persisted
        .get("closure_report_xlsx")
        .copied()
        .ok_or_else(|| anyhow::anyhow!("reused scan report artifact was not persisted"))?;
    let report_hash = report_artifact_manifest_hash(&state.pool, report_artifact_id).await?;
    let result_summary = json!({
        "schemaVersion": "lcia.scope-closure-summary.v1",
        "issueCount": issues.len(),
        "blockerCount": issues.iter().filter(|issue| issue.blocking).count(),
        "evidenceHash": required_json_text(
            data.get("evidence")
                .ok_or_else(|| anyhow::anyhow!("reusable scan omitted evidence"))?,
            "evidenceHash",
        )?,
        "artifacts": persisted,
        "reusedFromCheckId": completed_check_id,
        "reportArtifactId": report_artifact_id,
        "reportArtifactManifestHash": report_hash,
    });
    let finalize = finalize_reused_scan_execution(
        &state.pool,
        closure_check_id,
        worker_job_id,
        lease_token,
        completed_check_id,
        report_artifact_id,
        &result_summary,
    )
    .await?;
    let evidence_json = data
        .get("evidence")
        .ok_or_else(|| anyhow::anyhow!("reusable scan omitted evidence"))?;
    let evidence = ScopeClosureEvidence {
        schema_version: "lcia.scope-closure-evidence.v1".to_owned(),
        source_fingerprint: required_json_text(evidence_json, "sourceFingerprint")?,
        resolution_map_hash: required_json_text(evidence_json, "resolutionMapHash")?,
        closure_bundle_hash: required_json_text(evidence_json, "closureBundleHash")?,
        snapshot_id: required_json_text(evidence_json, "snapshotId")?.parse()?,
        snapshot_hash: required_json_text(evidence_json, "snapshotHash")?,
        report_artifact_manifest_hash: report_hash,
        evidence_hash: required_json_text(evidence_json, "evidenceHash")?,
    };
    let status = required_json_text(&data, "status")?;
    let scan_completeness = required_json_text(&data, "scanCompleteness")?;
    let blocker_codes = data
        .get("blockerCodes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    Ok(ScopeClosureExecutionResult {
        closure_check_id,
        worker_job_id,
        status,
        scan_completeness,
        certificate_hash: finalize
            .get("data")
            .and_then(|item| item.get("certificateHash"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        evidence,
        report_artifact_id,
        blocker_codes,
    })
}

async fn finalize_reused_scan_execution(
    pool: &PgPool,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    lease_token: Uuid,
    completed_check_id: Uuid,
    report_artifact_id: Uuid,
    result_summary: &Value,
) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_finalize_reused_scan(
            $1, $2, $3, $4, $5, $6::jsonb
        ) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .bind(worker_job_id)
    .bind(lease_token)
    .bind(completed_check_id)
    .bind(report_artifact_id)
    .bind(result_summary)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_finalize_reused_scan")?;
    Ok(result)
}

fn required_json_text(value: &Value, key: &str) -> anyhow::Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("reusable scan omitted {key}"))
}

#[allow(clippy::too_many_lines)]
async fn load_reused_issues(
    pool: &PgPool,
    completed_check_id: Uuid,
) -> anyhow::Result<Vec<ClosureIssue>> {
    let rows = sqlx::query(
        r"
        SELECT issue_key, severity, blocking, issue_code,
               source_dataset_type, source_dataset_id, source_dataset_version,
               json_path, reference_role, requested_target_type,
               requested_target_id, requested_target_version, message,
               suggested_action, occurrence_count, affected_root_count,
               COALESCE((
                 SELECT jsonb_agg(jsonb_build_object(
                   'occurrenceKey', o.occurrence_key,
                   'sourceDatasetType', o.source_dataset_type,
                   'sourceDatasetId', o.source_dataset_id,
                   'sourceDatasetVersion', o.source_dataset_version,
                   'jsonPath', o.json_path,
                   'referenceRole', o.reference_role,
                   'details', o.details
                 ) ORDER BY o.occurrence_key)
                 FROM public.lcia_scope_closure_issue_occurrences o
                 WHERE o.closure_issue_id = i.id
               ), '[]'::jsonb) AS occurrences,
               COALESCE((
                 SELECT jsonb_agg(jsonb_build_object(
                   'datasetType', r.root_dataset_type,
                   'id', r.root_dataset_id,
                   'version', r.root_dataset_version,
                   'witnessPath', r.witness_path
                 ) ORDER BY r.root_dataset_type, r.root_dataset_id, r.root_dataset_version)
                 FROM public.lcia_scope_closure_issue_roots r
                 WHERE r.closure_issue_id = i.id
               ), '[]'::jsonb) AS affected_roots
        FROM public.lcia_scope_closure_issues i
        WHERE closure_check_id = $1
        ORDER BY issue_key
        ",
    )
    .bind(completed_check_id)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            let source_category = row
                .try_get::<Option<String>, _>("source_dataset_type")?
                .map(|category| parse_category(category.as_str()))
                .transpose()?;
            let source_id = row.try_get::<Option<Uuid>, _>("source_dataset_id")?;
            let source_version = row.try_get::<Option<String>, _>("source_dataset_version")?;
            let source = match (source_category, source_id, source_version) {
                (Some(category), Some(id), Some(version)) => Some(ExactDatasetIdentity {
                    category,
                    id,
                    version,
                }),
                _ => None,
            };
            let affected_roots_json = row.try_get::<Value, _>("affected_roots")?;
            let affected_roots = affected_roots_json
                .as_array()
                .into_iter()
                .flatten()
                .map(|root| {
                    Ok(ExactDatasetIdentity {
                        category: parse_category(
                            required_json_text(root, "datasetType")?.as_str(),
                        )?,
                        id: required_json_text(root, "id")?.parse()?,
                        version: required_json_text(root, "version")?,
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let affected_root_witness_paths: Vec<Vec<ExactDatasetIdentity>> = affected_roots_json
                .as_array()
                .into_iter()
                .flatten()
                .map(|root| {
                    root.get("witnessPath")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(|node| serde_json::from_value(node.clone()).ok())
                        .collect::<Vec<_>>()
                })
                .collect();
            let witness_path = affected_root_witness_paths
                .first()
                .cloned()
                .unwrap_or_default();
            let occurrences = row
                .try_get::<Value, _>("occurrences")?
                .as_array()
                .into_iter()
                .flatten()
                .map(|occurrence| {
                    let source_category = occurrence
                        .get("sourceDatasetType")
                        .and_then(Value::as_str)
                        .map(parse_category)
                        .transpose()?;
                    let source_id = occurrence
                        .get("sourceDatasetId")
                        .and_then(Value::as_str)
                        .map(Uuid::parse_str)
                        .transpose()?;
                    let source_version = occurrence
                        .get("sourceDatasetVersion")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    let source = match (source_category, source_id, source_version) {
                        (Some(category), Some(id), Some(version)) => Some(ExactDatasetIdentity {
                            category,
                            id,
                            version,
                        }),
                        _ => None,
                    };
                    Ok(ClosureIssueOccurrence {
                        occurrence_key: required_json_text(occurrence, "occurrenceKey")?,
                        source,
                        json_path: occurrence
                            .get("jsonPath")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        reference_role: occurrence
                            .get("referenceRole")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        details: occurrence
                            .get("details")
                            .cloned()
                            .unwrap_or_else(|| json!({})),
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(ClosureIssue {
                issue_key: row.try_get("issue_key")?,
                severity: row.try_get("severity")?,
                blocking: row.try_get("blocking")?,
                issue_code: row.try_get("issue_code")?,
                source,
                json_path: row.try_get("json_path")?,
                reference_role: row.try_get("reference_role")?,
                requested_target_type: row.try_get("requested_target_type")?,
                requested_target_id: row.try_get("requested_target_id")?,
                requested_target_version: row.try_get("requested_target_version")?,
                message: row.try_get("message")?,
                suggested_action: row.try_get("suggested_action")?,
                occurrence_count: u32::try_from(row.try_get::<i32, _>("occurrence_count")?.max(1))?,
                occurrences,
                affected_roots,
                affected_root_witness_paths,
                witness_path,
            })
        })
        .collect()
}

pub async fn record_scope_closure_failure(
    pool: &PgPool,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    lease_token: Uuid,
    _error: &str,
) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_fail_before_scan(
            $1, $2, $3, 'scope_closure_execution_failed'
        ) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .bind(worker_job_id)
    .bind(lease_token)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_fail_before_scan")?;
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
async fn record_scope_closure_result_v2(
    pool: &PgPool,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    lease_token: Uuid,
    status: &str,
    scan_completeness: &str,
    effective_scope: &RequestedScopeManifest,
    evidence: &ScopeClosureEvidence,
    result_summary: &Value,
    issues: &[ClosureIssue],
    blocker_codes: &[String],
    report_artifact_id: Uuid,
) -> anyhow::Result<Value> {
    let issues = issues.iter().map(issue_rpc_projection).collect::<Vec<_>>();
    record_scope_closure_result_v2_raw(
        pool,
        closure_check_id,
        worker_job_id,
        lease_token,
        status,
        scan_completeness,
        &serde_json::to_value(effective_scope)?,
        &serde_json::to_value(evidence)?,
        result_summary,
        &serde_json::to_value(issues)?,
        blocker_codes,
        Some(report_artifact_id),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn record_scope_closure_result_v2_raw(
    pool: &PgPool,
    closure_check_id: Uuid,
    worker_job_id: Uuid,
    lease_token: Uuid,
    status: &str,
    scan_completeness: &str,
    effective_scope: &Value,
    evidence: &Value,
    result_summary: &Value,
    issues: &Value,
    blocker_codes: &[String],
    report_artifact_id: Option<Uuid>,
) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_check_record_result_v2(
            $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb,
            $9::jsonb, $10::text[], $11
        ) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .bind(worker_job_id)
    .bind(lease_token)
    .bind(status)
    .bind(scan_completeness)
    .bind(effective_scope)
    .bind(evidence)
    .bind(result_summary)
    .bind(issues)
    .bind(blocker_codes)
    .bind(report_artifact_id)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_check_record_result_v2")?;
    Ok(result)
}

fn issue_rpc_projection(issue: &ClosureIssue) -> Value {
    let occurrences = issue
        .occurrences
        .iter()
        .map(|occurrence| {
            json!({
                "occurrenceKey": occurrence.occurrence_key,
                "sourceDatasetType": occurrence.source.as_ref().map(|item| item.category.table_name()),
                "sourceDatasetId": occurrence.source.as_ref().map(|item| item.id),
                "sourceDatasetVersion": occurrence.source.as_ref().map(|item| item.version.as_str()),
                "jsonPath": occurrence.json_path,
                "referenceRole": occurrence.reference_role,
                "details": occurrence.details,
            })
        })
        .collect::<Vec<_>>();
    let affected_roots = issue
        .affected_roots
        .iter()
        .enumerate()
        .map(|(index, root)| {
            json!({
                "datasetType": root.category.table_name(),
                "id": root.id,
                "version": root.version,
                "impactRole": "root",
                "witnessPath": issue.affected_root_witness_paths
                    .get(index)
                    .unwrap_or(&issue.witness_path),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "issueKey": issue.issue_key,
        "severity": issue.severity,
        "blocking": issue.blocking,
        "issueCode": issue.issue_code,
        "sourceDatasetType": issue.source.as_ref().map(|item| item.category.table_name()),
        "sourceDatasetId": issue.source.as_ref().map(|item| item.id),
        "sourceDatasetVersion": issue.source.as_ref().map(|item| item.version.as_str()),
        "jsonPath": issue.json_path,
        "referenceRole": issue.reference_role,
        "requestedTargetType": issue.requested_target_type,
        "requestedTargetId": issue.requested_target_id,
        "requestedTargetVersion": issue.requested_target_version,
        "message": issue.message,
        "suggestedAction": issue.suggested_action,
        "occurrenceCount": issue.occurrence_count,
        "affectedRootCount": issue.affected_roots.len(),
        "details": {"witnessPath": issue.witness_path},
        "occurrences": occurrences,
        "affectedRoots": affected_roots,
    })
}

fn build_effective_scope_manifest(
    requested: &RequestedScopeManifest,
    documents: &[ClosureDocument],
) -> RequestedScopeManifest {
    let mut effective = requested.clone();
    effective.processes = documents
        .iter()
        .filter(|document| document.identity.category == DatasetCategory::Processes)
        .map(|document| RequestedIdentity {
            id: document.identity.id,
            version: document.identity.version.clone(),
        })
        .collect();
    effective.lcia_methods = documents
        .iter()
        .filter(|document| document.identity.category == DatasetCategory::Lciamethods)
        .map(|document| RequestedIdentity {
            id: document.identity.id,
            version: document.identity.version.clone(),
        })
        .collect();
    effective.processes.sort_by(|left, right| {
        (left.id, left.version.as_str()).cmp(&(right.id, right.version.as_str()))
    });
    effective.lcia_methods.sort_by(|left, right| {
        (left.id, left.version.as_str()).cmp(&(right.id, right.version.as_str()))
    });
    effective.process_manifest_hash = canonical_json_sha256(&json!({
        "processes": effective.processes,
    }))
    .ok();
    effective
}

fn source_fingerprint(documents: &[ClosureDocument]) -> anyhow::Result<String> {
    let source = documents
        .iter()
        .map(|document| {
            Ok(json!({
                "identity": document.identity,
                "contentSha256": canonical_json_sha256(&document.payload)?,
            }))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    canonical_json_sha256(&source)
}

fn build_resolution_map(edges: &[ReferenceEdge], omitted_resolutions: &[Value]) -> Vec<Value> {
    let mut resolutions = edges
        .iter()
        .map(|edge| {
            json!({
                "kind": "reference-request",
                "source": edge.document_key,
                "jsonPath": edge.json_path,
                "role": edge.reference_role,
                "targetCategory": edge.target_category,
                "targetId": edge.target_uuid,
                "requestedVersionState": edge.requested_version_state,
                "requestedVersion": edge.requested_version,
            })
        })
        .collect::<Vec<_>>();
    resolutions.extend(omitted_resolutions.iter().map(|resolution| {
        json!({
            "kind": "omitted-version-resolution",
            "provenance": resolution,
        })
    }));
    resolutions.sort_by_key(canonical_value);
    resolutions
}

fn prepare_artifact(
    artifact_type: &str,
    file_name: &str,
    content_type: &str,
    bytes: Vec<u8>,
) -> PreparedArtifact {
    PreparedArtifact {
        descriptor: ArtifactManifestEntry {
            artifact_type: artifact_type.to_owned(),
            file_name: file_name.to_owned(),
            content_type: content_type.to_owned(),
            byte_size: bytes.len(),
            checksum_sha256: sha256_hex(&bytes),
        },
        bytes,
    }
}

async fn persist_closure_artifacts(
    state: &AppState,
    worker_job_id: Uuid,
    closure_check_id: Uuid,
    artifacts: &[PreparedArtifact],
    content_artifact_manifest_hash: &str,
) -> anyhow::Result<BTreeMap<String, Uuid>> {
    let write_set_id = Uuid::new_v4();
    let mut uploaded = Vec::<String>::new();
    let mut staged = Vec::<(&PreparedArtifact, String)>::new();
    for artifact in artifacts {
        let relative_key = format!(
            "scope-closure/{closure_check_id}/{write_set_id}/{}",
            artifact.descriptor.file_name
        );
        let object_key = state.object_store.prefixed_object_key(&relative_key)?;
        if let Err(error) = state
            .object_store
            .upload_object_key(
                object_key.as_str(),
                artifact.descriptor.content_type.as_str(),
                artifact.bytes.clone(),
            )
            .await
        {
            cleanup_uploaded_artifacts(state, &uploaded).await;
            return Err(error.context("failed to upload closure artifact write set"));
        }
        uploaded.push(object_key.clone());
        staged.push((artifact, object_key));
    }

    let mut transaction = match state.pool.begin().await {
        Ok(transaction) => transaction,
        Err(error) => {
            cleanup_uploaded_artifacts(state, &uploaded).await;
            return Err(anyhow::anyhow!(
                "failed to begin closure artifact metadata transaction: {error}"
            ));
        }
    };
    let mut persisted = BTreeMap::new();
    for (artifact, object_key) in &staged {
        let byte_size = i64::try_from(artifact.descriptor.byte_size)?;
        let row = match sqlx::query(
            r"
            INSERT INTO public.worker_job_artifacts (
                job_id, artifact_type, storage_path, content_type, byte_size,
                checksum_sha256, metadata, visibility
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, 'operator')
            RETURNING id
            ",
        )
        .bind(worker_job_id)
        .bind(artifact.descriptor.artifact_type.as_str())
        .bind(object_key)
        .bind(artifact.descriptor.content_type.as_str())
        .bind(byte_size)
        .bind(artifact.descriptor.checksum_sha256.as_str())
        .bind(json!({
            "schemaVersion": "lcia.scope-closure-artifact.v1",
            "closureCheckId": closure_check_id,
            "writeSetId": write_set_id,
            "fileName": artifact.descriptor.file_name,
            "contentArtifactManifestHash": content_artifact_manifest_hash,
        }))
        .fetch_one(&mut *transaction)
        .await
        {
            Ok(row) => row,
            Err(error) => {
                let _ = transaction.rollback().await;
                cleanup_uploaded_artifacts(state, &uploaded).await;
                return Err(anyhow::anyhow!(
                    "failed to persist closure artifact metadata write set: {error}"
                ));
            }
        };
        persisted.insert(
            artifact.descriptor.artifact_type.clone(),
            row.try_get::<Uuid, _>("id")?,
        );
    }
    if let Err(error) = transaction.commit().await {
        cleanup_uploaded_artifacts(state, &uploaded).await;
        return Err(anyhow::anyhow!(
            "failed to commit closure artifact metadata write set: {error}"
        ));
    }
    Ok(persisted)
}

async fn cleanup_uploaded_artifacts(state: &AppState, object_keys: &[String]) {
    for object_key in object_keys {
        if let Err(error) = state.object_store.delete_object_key(object_key).await {
            tracing::warn!(
                object_key,
                error = %error,
                "failed to compensate closure artifact upload"
            );
        }
    }
}

async fn report_artifact_manifest_hash(pool: &PgPool, artifact_id: Uuid) -> anyhow::Result<String> {
    let row = sqlx::query(
        r"
        SELECT public.lcia_scope_closure_sha256(jsonb_build_object(
            'artifactId', id,
            'bucket', storage_bucket,
            'objectPath', storage_path,
            'mediaType', content_type,
            'byteSize', byte_size,
            'checksumSha256', checksum_sha256
        )) AS manifest_hash
        FROM public.worker_job_artifacts
        WHERE id = $1
        ",
    )
    .bind(artifact_id)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get("manifest_hash")?)
}

fn build_issue_jsonl(issues: &[ClosureIssue]) -> anyhow::Result<Vec<u8>> {
    let mut output = Vec::new();
    for issue in issues {
        output.extend(canonical_json_bytes(issue)?);
        output.push(b'\n');
    }
    Ok(output)
}

#[allow(clippy::too_many_lines)]
async fn run_tidas_batch_validation_cached(
    pool: &PgPool,
    worker_job_id: Uuid,
    documents: &[ClosureDocument],
) -> anyhow::Result<TidasBatchValidation> {
    let describe_output = run_tidas_command(&["--describe", "--format", "json"])?;
    let describe: Value = serde_json::from_str(describe_output.trim())?;
    if !describe
        .get("protocols")
        .and_then(Value::as_array)
        .is_some_and(|protocols| protocols.iter().any(|item| item == TIDAS_BATCH_PROTOCOL))
    {
        return Err(anyhow::anyhow!(
            "installed tidas-tools does not support {TIDAS_BATCH_PROTOCOL}"
        ));
    }
    let cache_keys = documents
        .iter()
        .map(|document| document_validation_cache_key(document, &describe))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let cached = lookup_document_validation_evidence(pool, &cache_keys).await?;
    let cached_by_key = cached
        .into_iter()
        .map(|item| (document_evidence_key(&item), item))
        .collect::<BTreeMap<_, _>>();
    let mut issue_events = Vec::new();
    let mut missing = Vec::new();
    for (document, key) in documents.iter().zip(&cache_keys) {
        if let Some(hit) = cached_by_key.get(&document_evidence_key(key)) {
            let issue_artifact_ref = hit
                .get("issueArtifactRef")
                .ok_or_else(|| anyhow::anyhow!("cached TIDAS evidence omitted issueArtifactRef"))?;
            let expected_artifact_hash = hit
                .get("issueArtifactHash")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    anyhow::anyhow!("cached TIDAS evidence omitted issueArtifactHash")
                })?;
            if canonical_json_sha256(issue_artifact_ref)? != expected_artifact_hash {
                return Err(anyhow::anyhow!(
                    "cached TIDAS evidence issue artifact hash mismatch"
                ));
            }
            issue_events.extend(
                issue_artifact_ref
                    .get("issues")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
            );
        } else {
            missing.push(document.clone());
        }
    }

    let uncached = run_tidas_batch_validation(&missing, describe.clone())?;
    issue_events.extend(uncached.issue_events.clone());
    if !missing.is_empty() {
        let records = missing
            .iter()
            .map(|document| {
                let issues = uncached
                    .issue_events
                    .iter()
                    .filter(|event| {
                        event.get("document_key").and_then(Value::as_str)
                            == Some(document.identity.document_key().as_str())
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let mut record = document_validation_cache_key(document, &describe)?;
                let Value::Object(record) = &mut record else {
                    unreachable!("cache key is an object")
                };
                record.insert(
                    "status".to_owned(),
                    Value::String(
                        if issues.is_empty() {
                            "passed"
                        } else {
                            "failed"
                        }
                        .to_owned(),
                    ),
                );
                record.insert(
                    "summary".to_owned(),
                    json!({"issueCount": issues.len(), "completed": true}),
                );
                record.insert("issueArtifactRef".to_owned(), json!({"issues": issues}));
                record.insert(
                    "issueArtifactHash".to_owned(),
                    Value::String(canonical_json_sha256(
                        record.get("issueArtifactRef").unwrap(),
                    )?),
                );
                Ok(Value::Object(record.clone()))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        record_document_validation_evidence(pool, worker_job_id, &records).await?;
    }
    issue_events.sort_by_key(canonical_value);
    let final_event = json!({
        "type": "final",
        "schema_version": "tidas.validation-final-event.v1",
        "protocol": TIDAS_BATCH_PROTOCOL,
        "profile": TIDAS_BATCH_PROFILE,
        "completed": true,
        "summary": {
            "document_count": documents.len(),
            "issue_count": issue_events.len(),
            "cache_hit_count": documents.len() - missing.len(),
            "validated_count": missing.len(),
        },
        "fingerprints": describe,
    });
    Ok(TidasBatchValidation {
        describe,
        final_event,
        issue_events,
    })
}

fn run_tidas_batch_validation(
    documents: &[ClosureDocument],
    describe: Value,
) -> anyhow::Result<TidasBatchValidation> {
    if documents.is_empty() {
        return Ok(TidasBatchValidation {
            describe,
            final_event: json!({
                "type": "final",
                "completed": true,
                "summary": {"document_count": 0, "issue_count": 0},
            }),
            issue_events: Vec::new(),
        });
    }
    let temp = TempDir::new()?;
    let input_dir = temp.path().join("documents");
    fs::create_dir(&input_dir)?;
    let manifest_path = temp.path().join("manifest.jsonl");
    let mut manifest = Vec::new();
    for (index, document) in documents.iter().enumerate() {
        let file_name = format!("{index:08}.json");
        let document_bytes = canonical_json_bytes(&document.payload)?;
        fs::write(input_dir.join(&file_name), &document_bytes)?;
        manifest.extend(canonical_json_bytes(&json!({
            "document_key": document.identity.document_key(),
            "category": document.identity.category.table_name(),
            "relative_path": file_name,
            "content_sha256": sha256_hex(&document_bytes),
            "identity": {
                "dataset_type": document.identity.category.table_name(),
                "dataset_id": document.identity.id,
                "dataset_version": document.identity.version,
            },
        }))?);
        manifest.push(b'\n');
    }
    fs::write(&manifest_path, manifest)?;

    let input_dir_arg = input_dir.to_string_lossy().into_owned();
    let manifest_arg = manifest_path.to_string_lossy().into_owned();
    let events = run_tidas_batch_events(&[
        "--protocol",
        TIDAS_BATCH_PROTOCOL,
        "--profile",
        TIDAS_BATCH_PROFILE,
        "--input-dir",
        input_dir_arg.as_str(),
        "--input-manifest",
        manifest_arg.as_str(),
    ])?;
    let final_positions = events
        .iter()
        .enumerate()
        .filter(|(_, event)| event.get("type").and_then(Value::as_str) == Some("final"))
        .collect::<Vec<_>>();
    if final_positions.len() != 1 || final_positions[0].0 + 1 != events.len() {
        return Err(anyhow::anyhow!(
            "TIDAS batch validator must emit exactly one terminal final event"
        ));
    }
    let final_event = final_positions[0].1.clone();
    if final_event.get("completed").and_then(Value::as_bool) != Some(true)
        || final_event.get("protocol").and_then(Value::as_str) != Some(TIDAS_BATCH_PROTOCOL)
        || final_event.get("profile").and_then(Value::as_str) != Some(TIDAS_BATCH_PROFILE)
    {
        return Err(anyhow::anyhow!(
            "TIDAS batch validator final event does not match the requested protocol/profile"
        ));
    }
    let issue_events = events
        .into_iter()
        .filter(|event| event.get("type").and_then(Value::as_str) == Some("issue"))
        .collect::<Vec<_>>();
    validate_tidas_final_event(&final_event, &issue_events, documents.len())?;
    Ok(TidasBatchValidation {
        describe,
        final_event,
        issue_events,
    })
}

fn validate_tidas_final_event(
    final_event: &Value,
    issue_events: &[Value],
    document_count: usize,
) -> anyhow::Result<()> {
    let reported_documents = final_event
        .pointer("/summary/document_count")
        .and_then(Value::as_u64)
        .and_then(|count| usize::try_from(count).ok());
    let reported_issues = final_event
        .pointer("/summary/issue_count")
        .and_then(Value::as_u64)
        .and_then(|count| usize::try_from(count).ok());
    if reported_documents != Some(document_count) || reported_issues != Some(issue_events.len()) {
        return Err(anyhow::anyhow!(
            "TIDAS batch validator final summary does not match the observed stream"
        ));
    }
    let mut logical_stream = Vec::new();
    for issue in issue_events {
        logical_stream.extend(canonical_json_bytes(issue)?);
        logical_stream.push(b'\n');
    }
    if final_event
        .get("logical_issue_stream_sha256")
        .and_then(Value::as_str)
        != Some(sha256_hex(&logical_stream).as_str())
    {
        return Err(anyhow::anyhow!(
            "TIDAS batch validator logical issue stream hash mismatch"
        ));
    }
    Ok(())
}

fn tidas_command_candidates() -> impl Iterator<Item = (String, Vec<String>)> {
    std::env::var("TIDAS_VALIDATE_BIN")
        .ok()
        .into_iter()
        .map(|program| (program, Vec::<String>::new()))
        .chain([
            ("tidas-validate".to_owned(), Vec::new()),
            (
                "python3".to_owned(),
                vec!["-m".to_owned(), "tidas_tools.validate".to_owned()],
            ),
            (
                "python".to_owned(),
                vec!["-m".to_owned(), "tidas_tools.validate".to_owned()],
            ),
        ])
}

/// Read the validator JSONL stream one event at a time.  This keeps the pipe
/// bounded by the operating system and applies natural backpressure to a fast
/// validator instead of first buffering its entire output in memory.
fn run_tidas_batch_events(args: &[&str]) -> anyhow::Result<Vec<Value>> {
    let mut missing = Vec::new();
    for (program, prefix) in tidas_command_candidates() {
        let child = Command::new(&program)
            .args(prefix)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();
        let mut child = match child {
            Ok(child) => child,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(program);
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("TIDAS validator stdout was not captured"))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("TIDAS validator stderr was not captured"))?;
        let stderr_reader = std::thread::spawn(move || {
            let mut output = String::new();
            stderr.read_to_string(&mut output).map(|_| output)
        });
        let events = BufReader::new(stdout)
            .lines()
            .filter_map(|line| match line {
                Ok(line) if line.trim().is_empty() => None,
                other => Some(other),
            })
            .map(|line| {
                let line = line?;
                serde_json::from_str::<Value>(&line).map_err(std::io::Error::other)
            })
            .collect::<std::io::Result<Vec<_>>>();
        if events.is_err() {
            let _ = child.kill();
        }
        let status = child.wait()?;
        let stderr = stderr_reader
            .join()
            .map_err(|_| anyhow::anyhow!("TIDAS validator stderr reader panicked"))??;
        if !status.success() {
            return Err(anyhow::anyhow!(
                "TIDAS validator failed via {program}: {stderr}"
            ));
        }
        return Ok(events?);
    }
    Err(anyhow::anyhow!(
        "TIDAS validator is unavailable; tried {}",
        missing.join(", ")
    ))
}

fn document_validation_cache_key(
    document: &ClosureDocument,
    describe: &Value,
) -> anyhow::Result<Value> {
    let package_version = describe
        .pointer("/package/version")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("TIDAS validator describe omitted package version"))?;
    let engines = describe
        .get("engines")
        .ok_or_else(|| anyhow::anyhow!("TIDAS validator describe omitted engines"))?;
    let schema_lock = describe
        .get("tidas_schema_lock_sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("TIDAS validator describe omitted schema lock hash"))?;
    Ok(json!({
        "datasetType": document.identity.category.table_name(),
        "datasetId": document.identity.id,
        "datasetVersion": document.identity.version,
        "canonicalContentHash": canonical_json_sha256(&document.payload)?,
        "documentValidatorVersion": package_version,
        "documentValidationProfile": TIDAS_BATCH_PROFILE,
        "validationReportSchemaVersion": "tidas.validation-report.v1",
        "validatorEngineFingerprint": canonical_json_sha256(engines)?,
        "tidasSchemaLockSha256": schema_lock,
    }))
}

fn document_evidence_key(value: &Value) -> String {
    [
        "datasetType",
        "datasetId",
        "datasetVersion",
        "canonicalContentHash",
        "documentValidatorVersion",
        "documentValidationProfile",
        "validationReportSchemaVersion",
        "validatorEngineFingerprint",
        "tidasSchemaLockSha256",
    ]
    .iter()
    .map(|key| value.get(key).map(Value::to_string).unwrap_or_default())
    .collect::<Vec<_>>()
    .join("|")
}

async fn lookup_document_validation_evidence(
    pool: &PgPool,
    keys: &[Value],
) -> anyhow::Result<Vec<Value>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_document_validation_evidence_lookup($1::jsonb) AS result
        FROM _service_role
        ",
    )
    .bind(serde_json::to_value(keys)?)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_document_validation_evidence_lookup")?;
    Ok(result
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default())
}

async fn record_document_validation_evidence(
    pool: &PgPool,
    worker_job_id: Uuid,
    records: &[Value],
) -> anyhow::Result<()> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_document_validation_evidence_record($1::jsonb, $2) AS result
        FROM _service_role
        ",
    )
    .bind(serde_json::to_value(records)?)
    .bind(worker_job_id)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_document_validation_evidence_record")
}

fn run_tidas_command(args: &[&str]) -> anyhow::Result<String> {
    let mut missing = Vec::new();
    for (program, prefix) in tidas_command_candidates() {
        let output = Command::new(&program).args(prefix).args(args).output();
        match output {
            Ok(output) if output.status.success() => {
                return Ok(String::from_utf8(output.stdout)?);
            }
            Ok(output) => {
                return Err(anyhow::anyhow!(
                    "TIDAS validator failed via {program}: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => missing.push(program),
            Err(error) => return Err(error.into()),
        }
    }
    Err(anyhow::anyhow!(
        "TIDAS validator is unavailable; tried {}",
        missing.join(", ")
    ))
}

fn merge_tidas_validation_issues(scan: &mut ScopeClosureScan, events: &[Value]) {
    let documents = scan
        .documents
        .iter()
        .map(|document| (document.identity.document_key(), document.identity.clone()))
        .collect::<BTreeMap<_, _>>();
    for event in events {
        let document_key = event
            .get("document_key")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let source = documents.get(document_key).cloned();
        let issue = event.get("issue").cloned().unwrap_or_else(|| json!({}));
        let issue_code = issue
            .get("issue_code")
            .or_else(|| issue.get("code"))
            .and_then(Value::as_str)
            .unwrap_or("tidas_document_invalid");
        let location = issue
            .get("location")
            .or_else(|| issue.get("path"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let issue_key = canonical_json_sha256(&json!({
            "code": issue_code,
            "source": source,
            "path": location,
            "message": issue.get("message"),
        }))
        .unwrap_or_else(|_| Uuid::new_v4().simple().to_string());
        scan.issues.push(ClosureIssue {
            issue_key: issue_key.clone(),
            severity: "blocker".to_owned(),
            blocking: true,
            issue_code: format!("tidas_{issue_code}"),
            source: source.clone(),
            json_path: location.clone(),
            reference_role: None,
            requested_target_type: None,
            requested_target_id: None,
            requested_target_version: None,
            message: issue
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("TIDAS document validation failed")
                .to_owned(),
            suggested_action: Some(
                "Repair the schema-invalid document and rerun closure preflight.".to_owned(),
            ),
            occurrence_count: 1,
            occurrences: vec![ClosureIssueOccurrence {
                occurrence_key: format!("{issue_key}:0"),
                source: source.clone(),
                json_path: location.clone(),
                reference_role: None,
                details: issue,
            }],
            affected_roots: Vec::new(),
            affected_root_witness_paths: Vec::new(),
            witness_path: Vec::new(),
        });
    }
    scan.issues = coalesce_issues(std::mem::take(&mut scan.issues));
    populate_affected_roots(scan);
}

#[allow(clippy::too_many_lines)]
fn build_xlsx_report(closure_check_id: Uuid, issues: &[ClosureIssue]) -> anyhow::Result<Vec<u8>> {
    let cursor = Cursor::new(Vec::new());
    let mut zip = ZipWriter::new(cursor);
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    zip.start_file("[Content_Types].xml", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet3.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet4.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#)?;
    zip.start_file("_rels/.rels", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#)?;
    zip.start_file("xl/workbook.xml", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Summary" sheetId="1" r:id="rId1"/><sheet name="Closure Issues" sheetId="2" r:id="rId2"/><sheet name="Occurrences" sheetId="3" r:id="rId3"/><sheet name="Affected Datasets" sheetId="4" r:id="rId4"/></sheets></workbook>"#)?;
    zip.start_file("xl/_rels/workbook.xml.rels", options)?;
    zip.write_all(br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet3.xml"/><Relationship Id="rId4" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet4.xml"/></Relationships>"#)?;

    let blocker_count = issues.iter().filter(|issue| issue.blocking).count();
    let warning_count = issues.len().saturating_sub(blocker_count);
    let occurrence_count = issues
        .iter()
        .map(|issue| issue.occurrences.len())
        .sum::<usize>();
    let affected_dataset_count = issues
        .iter()
        .flat_map(|issue| issue.affected_roots.iter())
        .collect::<BTreeSet<_>>()
        .len();
    write_xlsx_worksheet(
        &mut zip,
        options,
        1,
        [
            vec!["Metric".to_owned(), "Value".to_owned()],
            vec!["Closure check ID".to_owned(), closure_check_id.to_string()],
            vec!["Issue count".to_owned(), issues.len().to_string()],
            vec!["Blocker count".to_owned(), blocker_count.to_string()],
            vec!["Warning count".to_owned(), warning_count.to_string()],
            vec!["Occurrence count".to_owned(), occurrence_count.to_string()],
            vec![
                "Affected dataset count".to_owned(),
                affected_dataset_count.to_string(),
            ],
        ],
    )?;

    let headers = [
        "Issue key",
        "Issue code",
        "Severity",
        "Message",
        "Source type",
        "Source id",
        "Source version",
        "JSON path",
        "Reference role",
        "Target type",
        "Target id",
        "Target version",
        "Occurrences",
        "Affected roots",
        "Suggested action",
    ];
    let mut issue_rows = vec![headers.iter().map(|value| (*value).to_owned()).collect()];
    for issue in issues {
        let source = issue.source.as_ref();
        issue_rows.push(vec![
            issue.issue_key.clone(),
            issue.issue_code.clone(),
            issue.severity.clone(),
            issue.message.clone(),
            source
                .map(|item| item.category.table_name().to_owned())
                .unwrap_or_default(),
            source.map(|item| item.id.to_string()).unwrap_or_default(),
            source.map(|item| item.version.clone()).unwrap_or_default(),
            issue.json_path.clone().unwrap_or_default(),
            issue.reference_role.clone().unwrap_or_default(),
            issue.requested_target_type.clone().unwrap_or_default(),
            issue
                .requested_target_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            issue.requested_target_version.clone().unwrap_or_default(),
            issue.occurrence_count.to_string(),
            issue.affected_roots.len().to_string(),
            issue.suggested_action.clone().unwrap_or_default(),
        ]);
    }
    write_xlsx_worksheet(&mut zip, options, 2, issue_rows)?;

    let mut occurrence_rows = vec![vec![
        "Issue key".to_owned(),
        "Occurrence key".to_owned(),
        "Source type".to_owned(),
        "Source id".to_owned(),
        "Source version".to_owned(),
        "JSON path".to_owned(),
        "Reference role".to_owned(),
        "Details".to_owned(),
    ]];
    for issue in issues {
        for occurrence in &issue.occurrences {
            let source = occurrence.source.as_ref();
            occurrence_rows.push(vec![
                issue.issue_key.clone(),
                occurrence.occurrence_key.clone(),
                source
                    .map(|item| item.category.table_name().to_owned())
                    .unwrap_or_default(),
                source.map(|item| item.id.to_string()).unwrap_or_default(),
                source.map(|item| item.version.clone()).unwrap_or_default(),
                occurrence.json_path.clone().unwrap_or_default(),
                occurrence.reference_role.clone().unwrap_or_default(),
                canonical_value(&occurrence.details),
            ]);
        }
    }
    write_xlsx_worksheet(&mut zip, options, 3, occurrence_rows)?;

    let mut affected_rows = vec![vec![
        "Issue key".to_owned(),
        "Dataset type".to_owned(),
        "Dataset id".to_owned(),
        "Dataset version".to_owned(),
        "Witness path".to_owned(),
    ]];
    for issue in issues {
        for (index, root) in issue.affected_roots.iter().enumerate() {
            let witness = issue
                .affected_root_witness_paths
                .get(index)
                .unwrap_or(&issue.witness_path);
            affected_rows.push(vec![
                issue.issue_key.clone(),
                root.category.table_name().to_owned(),
                root.id.to_string(),
                root.version.clone(),
                canonical_value(witness),
            ]);
        }
    }
    write_xlsx_worksheet(&mut zip, options, 4, affected_rows)?;
    Ok(zip.finish()?.into_inner())
}

fn write_xlsx_worksheet<I>(
    zip: &mut ZipWriter<Cursor<Vec<u8>>>,
    options: SimpleFileOptions,
    sheet_number: usize,
    rows: I,
) -> anyhow::Result<()>
where
    I: IntoIterator<Item = Vec<String>>,
{
    zip.start_file(format!("xl/worksheets/sheet{sheet_number}.xml"), options)?;
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>",
    );
    for (index, row) in rows.into_iter().enumerate() {
        append_xlsx_row(&mut xml, index + 1, row);
    }
    xml.push_str("</sheetData></worksheet>");
    zip.write_all(xml.as_bytes())?;
    Ok(())
}

fn append_xlsx_row<I>(xml: &mut String, row: usize, values: I)
where
    I: IntoIterator<Item = String>,
{
    xml.push_str(format!("<row r=\"{row}\">").as_str());
    for (column, value) in values.into_iter().enumerate() {
        let reference = format!("{}{}", xlsx_column_name(column), row);
        xml.push_str(
            format!(
                "<c r=\"{reference}\" t=\"inlineStr\"><is><t>{}</t></is></c>",
                xml_escape(value.as_str())
            )
            .as_str(),
        );
    }
    xml.push_str("</row>");
}

fn xlsx_column_name(mut index: usize) -> String {
    let mut output = String::new();
    loop {
        output.insert(
            0,
            char::from(b'A' + u8::try_from(index % 26).unwrap_or_default()),
        );
        if index < 26 {
            break;
        }
        index = index / 26 - 1;
    }
    output
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn deterministic_uuid_from_hash(hash: &str) -> anyhow::Result<Uuid> {
    let bytes = hex::decode(hash)?;
    let mut uuid_bytes = [0_u8; 16];
    uuid_bytes.copy_from_slice(
        bytes
            .get(..16)
            .ok_or_else(|| anyhow::anyhow!("closure bundle hash is too short"))?,
    );
    uuid_bytes[6] = (uuid_bytes[6] & 0x0f) | 0x50;
    uuid_bytes[8] = (uuid_bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(uuid_bytes))
}

/// Verifies that a package-build payload is bound to reusable frozen evidence.
pub async fn validate_package_closure_binding(
    pool: &PgPool,
    binding: &PackageClosureBinding<'_>,
) -> anyhow::Result<()> {
    let row = sqlx::query(
        r"
        SELECT c.status, c.scan_completeness, c.certificate_status,
               c.certificate_hash, c.effective_scope_hash,
               c.data_snapshot_token, c.source_fingerprint, c.resolution_map_hash,
               c.closure_bundle_hash, c.snapshot_id, c.snapshot_hash,
               c.report_artifact_manifest_hash, c.evidence_hash,
               c.requested_scope_manifest->>'certificateFreshnessPolicy' AS freshness_policy,
               EXISTS (
                 SELECT 1
                 FROM public.lcia_scope_closure_data_snapshots s
                 JOIN public.lca_release_publications p
                   ON p.is_current = true AND p.status = 'current'
                 JOIN public.lca_release_runs r ON r.id = p.release_run_id
                 WHERE s.data_snapshot_token = c.data_snapshot_token
                   AND s.root_manifest->'currentPublicRelease'->>'releaseRunId' = r.id::text
                   AND s.root_manifest->'currentPublicRelease'->>'releaseManifestHash' = r.release_manifest_hash
               ) AS current_release_matches
        FROM public.lcia_scope_closure_checks c
        WHERE c.id = $1
        ",
    )
    .bind(binding.closure_check_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("closure_check_not_found"))?;
    let certificate_status = row.try_get::<String, _>("certificate_status")?;
    let status = row.try_get::<String, _>("status")?;
    let scan_completeness = row.try_get::<String, _>("scan_completeness")?;
    let actual_certificate_hash = row.try_get::<Option<String>, _>("certificate_hash")?;
    let actual_scope_hash = row.try_get::<Option<String>, _>("effective_scope_hash")?;
    let actual_snapshot_token = row.try_get::<String, _>("data_snapshot_token")?;
    let freshness_policy = row
        .try_get::<Option<String>, _>("freshness_policy")?
        .unwrap_or_else(|| "frozen-artifact-reusable-v1".to_owned());
    let current_release_matches = row.try_get::<bool, _>("current_release_matches")?;
    let complete_evidence = [
        row.try_get::<Option<String>, _>("source_fingerprint")?,
        row.try_get::<Option<String>, _>("resolution_map_hash")?,
        row.try_get::<Option<String>, _>("closure_bundle_hash")?,
        row.try_get::<Option<Uuid>, _>("snapshot_id")?
            .map(|id| id.to_string()),
        row.try_get::<Option<String>, _>("snapshot_hash")?,
        row.try_get::<Option<String>, _>("report_artifact_manifest_hash")?,
        row.try_get::<Option<String>, _>("evidence_hash")?,
    ]
    .iter()
    .all(Option::is_some);
    if status != "passed"
        || scan_completeness != "complete"
        || certificate_status != "valid"
        || !freshness_policy_accepts_current_release(
            freshness_policy.as_str(),
            current_release_matches,
        )
        || actual_certificate_hash.as_deref() != Some(binding.closure_certificate_hash)
        || actual_scope_hash.as_deref() != Some(binding.effective_scope_hash)
        || actual_snapshot_token != binding.data_snapshot_token
        || row.try_get::<Option<Uuid>, _>("snapshot_id")? != Some(binding.snapshot_id)
        || row
            .try_get::<Option<String>, _>("snapshot_hash")?
            .as_deref()
            != Some(binding.snapshot_hash)
        || row
            .try_get::<Option<String>, _>("closure_bundle_hash")?
            .as_deref()
            != Some(binding.closure_bundle_hash)
        || row
            .try_get::<Option<String>, _>("report_artifact_manifest_hash")?
            .as_deref()
            != Some(binding.report_artifact_manifest_hash)
        || !complete_evidence
    {
        return Err(anyhow::anyhow!("closure_evidence_mismatch"));
    }
    Ok(())
}

fn freshness_policy_accepts_current_release(policy: &str, current_release_matches: bool) -> bool {
    match policy {
        "frozen-artifact-reusable-v1" => true,
        "current-membership-required-v1" => current_release_matches,
        _ => false,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PackageClosureBinding<'a> {
    pub closure_check_id: Uuid,
    pub closure_certificate_hash: &'a str,
    pub effective_scope_hash: &'a str,
    pub data_snapshot_token: &'a str,
    pub snapshot_id: Uuid,
    pub snapshot_hash: &'a str,
    pub closure_bundle_hash: &'a str,
    pub report_artifact_manifest_hash: &'a str,
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use serde_json::json;
    use zip::ZipArchive;

    use super::*;

    #[derive(Clone, Default)]
    struct FakeProvider {
        documents: BTreeMap<ExactDatasetIdentity, ClosureDocument>,
        fetches: Arc<Mutex<Vec<Vec<ExactDatasetIdentity>>>>,
        checkpoints: Arc<AtomicUsize>,
        fail_checkpoint: Option<usize>,
        reverse_fetch: bool,
        omitted_resolutions: BTreeMap<(DatasetCategory, Uuid), ExactDatasetIdentity>,
        omitted_calls: Arc<AtomicUsize>,
    }

    impl ScopeClosureProvider for FakeProvider {
        async fn checkpoint(&self, _scanned: usize, _scheduled: usize) -> anyhow::Result<()> {
            let call = self.checkpoints.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_checkpoint == Some(call) {
                return Err(anyhow::anyhow!("cancelled"));
            }
            Ok(())
        }

        async fn fetch_exact(
            &self,
            identities: &[ExactDatasetIdentity],
        ) -> anyhow::Result<ProviderFetchResult> {
            assert!(identities.len() <= FETCH_BATCH_SIZE);
            self.fetches.lock().unwrap().push(identities.to_vec());
            let mut output = identities
                .iter()
                .filter_map(|identity| self.documents.get(identity).cloned())
                .collect::<Vec<_>>();
            if self.reverse_fetch {
                output.reverse();
            }
            Ok(ProviderFetchResult {
                documents: output,
                ..ProviderFetchResult::default()
            })
        }

        async fn resolve_omitted_version(
            &self,
            category: DatasetCategory,
            id: Uuid,
            policy: &str,
        ) -> anyhow::Result<OmittedVersionResolution> {
            self.omitted_calls.fetch_add(1, Ordering::SeqCst);
            if policy == "reject" {
                return Ok(OmittedVersionResolution {
                    selected: None,
                    candidates: Vec::new(),
                    policy: policy.to_owned(),
                });
            }
            let selected = self.omitted_resolutions.get(&(category, id)).cloned();
            Ok(OmittedVersionResolution {
                candidates: selected.iter().cloned().collect(),
                selected,
                policy: policy.to_owned(),
            })
        }
    }

    fn id(value: &str) -> Uuid {
        value.parse().unwrap()
    }

    fn identity(category: DatasetCategory, value: &str) -> ExactDatasetIdentity {
        ExactDatasetIdentity {
            category,
            id: id(value),
            version: "01.00.000".to_owned(),
        }
    }

    fn manifest(processes: Vec<ExactDatasetIdentity>) -> RequestedScopeManifest {
        RequestedScopeManifest {
            schema_version: "lcia.scope-manifest.v1".to_owned(),
            coverage_mode: "subset".to_owned(),
            eligibility_predicate_version: "published-state-code-100-199:v1".to_owned(),
            processes: processes
                .into_iter()
                .map(|item| RequestedIdentity {
                    id: item.id,
                    version: item.version,
                })
                .collect(),
            lcia_methods: Vec::new(),
            version_resolution_policy: "reference-version-resolution-v1".to_owned(),
            legacy_omitted_version_policy: "reject".to_owned(),
            certificate_freshness_policy: "frozen-artifact-reusable-v1".to_owned(),
            link_policy: ScopeLinkPolicy {
                link_semantics_version: "signed-flow-balance-v1".to_owned(),
                flow_identity_policy: "exact-flow-version-reference-unit-v2".to_owned(),
                allocation_semantics_version: "tidas-reference-allocation-v3".to_owned(),
                technosphere_boundary_policy: "closed".to_owned(),
                provider_universe_policy: "scope_only".to_owned(),
            },
            process_manifest_hash: None,
        }
    }

    fn reference(category: &str, target: Uuid, version: Option<&str>) -> Value {
        let mut value = json!({
            "@type": format!("{category} data set"),
            "@refObjectId": target,
            "@uri": format!("../{category}/{target}.json"),
        });
        if let Some(version) = version {
            value["@version"] = json!(version);
        }
        value
    }

    fn snapshot_entry(identity: &ExactDatasetIdentity, payload: &Value) -> SnapshotDatasetEntry {
        SnapshotDatasetEntry {
            dataset_type: identity.category,
            dataset_id: identity.id,
            dataset_version: identity.version.clone(),
            role: "support".to_owned(),
            source_process_id: None,
            source_process_version: None,
            version_significant_hash: "1".repeat(64),
            semantic_hash: "2".repeat(64),
            canonical_content_hash: canonical_json_sha256(payload).unwrap(),
        }
    }

    #[test]
    fn closure_reads_reviewed_lcia_method_by_artifact_locator() {
        let method = ExactDatasetIdentity {
            category: DatasetCategory::Lciamethods,
            id: id("503699e0-eca9-4089-8bf8-e0f49c93e578"),
            version: "01.01.000".to_owned(),
        };
        assert_eq!(
            lcia_method_artifact_locator_id(&method),
            id("9ec743ea-6b00-400d-a53b-61547a3fc03c")
        );
    }

    #[test]
    fn reference_extraction_matches_tidas_tools_golden_fixture() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/reference_extraction_v1/golden.json"
        ))
        .unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let category = parse_category(case["category"].as_str().unwrap()).unwrap();
            let result = extract_references(
                case["document_key"].as_str().unwrap(),
                category,
                &case["payload"],
            );
            if let Some(expected) = case.get("expected") {
                assert_eq!(serde_json::to_value(result).unwrap(), *expected);
            } else {
                let targets = result
                    .edges
                    .iter()
                    .map(|edge| edge.target_uuid.as_str())
                    .collect::<Vec<_>>();
                let expected = case["expected_edge_targets"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(targets, expected);
            }
        }
    }

    #[tokio::test]
    async fn union_traversal_is_cycle_safe_shared_and_non_fail_fast() {
        let root_a = identity(
            DatasetCategory::Processes,
            "11111111-1111-1111-1111-111111111111",
        );
        let root_b = identity(
            DatasetCategory::Processes,
            "22222222-2222-2222-2222-222222222222",
        );
        let shared = identity(
            DatasetCategory::Sources,
            "33333333-3333-3333-3333-333333333333",
        );
        let missing = identity(
            DatasetCategory::Contacts,
            "44444444-4444-4444-4444-444444444444",
        );
        let documents = [
            ClosureDocument {
                identity: root_a.clone(),
                payload: json!({"references": [
                    reference("process", root_b.id, Some("01.00.000")),
                    reference("source", shared.id, Some("01.00.000")),
                ]}),
            },
            ClosureDocument {
                identity: root_b.clone(),
                payload: json!({"references": [
                    reference("process", root_a.id, Some("01.00.000")),
                    reference("source", shared.id, Some("01.00.000")),
                ]}),
            },
            ClosureDocument {
                identity: shared.clone(),
                payload: json!({"referenceToContact": reference(
                    "contact",
                    missing.id,
                    Some("01.00.000")
                )}),
            },
        ]
        .into_iter()
        .map(|document| (document.identity.clone(), document))
        .collect();
        let provider = FakeProvider {
            documents,
            ..FakeProvider::default()
        };

        let scan = collect_scope_closure(&provider, &manifest(vec![root_a, root_b]))
            .await
            .unwrap();

        assert!(scan.complete);
        assert_eq!(scan.documents.len(), 3);
        assert_eq!(scan.edges.len(), 5);
        assert_eq!(scan.issues.len(), 1);
        assert_eq!(scan.issues[0].issue_code, "reference_exact_version_missing");
        assert_eq!(scan.issues[0].affected_roots.len(), 2);
        let fetched = provider.fetches.lock().unwrap();
        assert_eq!(
            fetched
                .iter()
                .flatten()
                .filter(|item| *item == &shared)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn explicit_version_never_falls_back_and_omitted_version_keeps_provenance() {
        let root = identity(
            DatasetCategory::Processes,
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        );
        let target_id = id("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        let provider = FakeProvider {
            documents: [ClosureDocument {
                identity: root.clone(),
                payload: json!({"references": [
                    reference("source", target_id, Some("01.00.000")),
                    reference("source", target_id, None),
                ]}),
            }]
            .into_iter()
            .map(|document| (document.identity.clone(), document))
            .collect(),
            ..FakeProvider::default()
        };

        let scan = collect_scope_closure(&provider, &manifest(vec![root.clone()]))
            .await
            .unwrap();

        assert!(
            scan.issues
                .iter()
                .any(|issue| issue.issue_code == "reference_exact_version_missing")
        );
        let omitted = scan
            .issues
            .iter()
            .find(|issue| issue.issue_code == "reference_version_omitted")
            .unwrap();
        assert_eq!(omitted.source.as_ref(), Some(&root));
        assert_eq!(omitted.json_path.as_deref(), Some("$.references[1]"));
        assert_eq!(provider.omitted_calls.load(Ordering::SeqCst), 1);
        assert_eq!(scan.omitted_version_resolutions.len(), 1);
        assert_eq!(
            scan.omitted_version_resolutions[0]["candidateUniverse"],
            "frozen-public-release-manifest"
        );
    }

    #[test]
    fn frozen_snapshot_rejects_same_identity_with_live_content_drift() {
        let exact = identity(
            DatasetCategory::Processes,
            "abababab-abab-abab-abab-abababababab",
        );
        let frozen = json!({"name": "frozen"});
        let universe = [(exact.clone(), snapshot_entry(&exact, &frozen))]
            .into_iter()
            .collect();
        let result = enforce_snapshot_boundary(
            std::slice::from_ref(&exact),
            &universe,
            vec![ClosureDocument {
                identity: exact.clone(),
                payload: json!({"name": "mutated"}),
            }],
        )
        .unwrap();
        assert!(result.documents.is_empty());
        assert!(result.incomplete_identities.contains(&exact));
        assert_eq!(result.issues[0].issue_code, "snapshot_source_drift");
    }

    #[test]
    fn frozen_snapshot_rejects_live_dataset_absent_from_release_manifest() {
        let exact = identity(
            DatasetCategory::Sources,
            "acacacac-acac-acac-acac-acacacacacac",
        );
        let result = enforce_snapshot_boundary(
            std::slice::from_ref(&exact),
            &BTreeMap::new(),
            vec![ClosureDocument {
                identity: exact.clone(),
                payload: json!({"live": true}),
            }],
        )
        .unwrap();
        assert!(result.documents.is_empty());
        assert!(result.incomplete_identities.contains(&exact));
        assert_eq!(result.issues[0].issue_code, "snapshot_dataset_not_allowed");
    }

    #[test]
    fn omitted_version_winner_and_candidates_come_only_from_frozen_release() {
        let dataset_id = id("adadadad-adad-adad-adad-adadadadadad");
        let identities = ["01.00.000", "03.00.000", "02.00.000"]
            .into_iter()
            .map(|version| ExactDatasetIdentity {
                category: DatasetCategory::Sources,
                id: dataset_id,
                version: version.to_owned(),
            })
            .collect::<Vec<_>>();
        let universe = identities
            .iter()
            .map(|identity| {
                (
                    identity.clone(),
                    snapshot_entry(identity, &json!({"version": identity.version})),
                )
            })
            .collect();
        let resolution = resolve_snapshot_omitted_version(
            &universe,
            DatasetCategory::Sources,
            dataset_id,
            "latest_eligible",
        )
        .unwrap();
        assert_eq!(resolution.candidates.len(), 3);
        assert_eq!(
            resolution
                .selected
                .as_ref()
                .map(|item| item.version.as_str()),
            Some("03.00.000")
        );
        assert_eq!(resolution.policy, "latest_eligible");
    }

    #[test]
    fn tidas_final_event_must_close_the_exact_observed_issue_stream() {
        let issues = vec![json!({
            "type": "issue",
            "document_key": "sources:1:01.00.000",
            "issue": {"code": "invalid"},
        })];
        let mut logical_stream = canonical_json_bytes(&issues[0]).unwrap();
        logical_stream.push(b'\n');
        let final_event = json!({
            "type": "final",
            "protocol": TIDAS_BATCH_PROTOCOL,
            "profile": TIDAS_BATCH_PROFILE,
            "completed": true,
            "summary": {"document_count": 1, "issue_count": 1},
            "logical_issue_stream_sha256": sha256_hex(&logical_stream),
        });
        validate_tidas_final_event(&final_event, &issues, 1).unwrap();

        let mut drifted = final_event;
        drifted["logical_issue_stream_sha256"] = json!("0".repeat(64));
        assert!(validate_tidas_final_event(&drifted, &issues, 1).is_err());
    }

    #[tokio::test]
    async fn traversal_is_batched_bounded_and_cooperatively_cancelled() {
        let roots = (0..200_u128)
            .map(|index| ExactDatasetIdentity {
                category: DatasetCategory::Processes,
                id: Uuid::from_u128(index + 1),
                version: "01.00.000".to_owned(),
            })
            .collect::<Vec<_>>();
        let documents = roots
            .iter()
            .map(|item| {
                (
                    item.clone(),
                    ClosureDocument {
                        identity: item.clone(),
                        payload: json!({}),
                    },
                )
            })
            .collect();
        let provider = FakeProvider {
            documents,
            ..FakeProvider::default()
        };
        let scan = collect_scope_closure(&provider, &manifest(roots.clone()))
            .await
            .unwrap();
        assert_eq!(scan.documents.len(), 200);
        assert_eq!(provider.fetches.lock().unwrap().len(), 3);

        let cancelled = FakeProvider {
            documents: provider.documents,
            fail_checkpoint: Some(2),
            ..FakeProvider::default()
        };
        let error = collect_scope_closure(&cancelled, &manifest(roots))
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "cancelled");
    }

    #[tokio::test]
    async fn scan_and_artifact_hashes_are_deterministic_across_fetch_order() {
        let root = identity(
            DatasetCategory::Processes,
            "cccccccc-cccc-cccc-cccc-cccccccccccc",
        );
        let child = identity(
            DatasetCategory::Sources,
            "dddddddd-dddd-dddd-dddd-dddddddddddd",
        );
        let documents = [
            ClosureDocument {
                identity: root.clone(),
                payload: json!({"referenceToSource": reference(
                    "source",
                    child.id,
                    Some("01.00.000")
                )}),
            },
            ClosureDocument {
                identity: child,
                payload: json!({}),
            },
        ]
        .into_iter()
        .map(|document| (document.identity.clone(), document))
        .collect::<BTreeMap<_, _>>();
        let normal = FakeProvider {
            documents: documents.clone(),
            ..FakeProvider::default()
        };
        let reversed = FakeProvider {
            documents,
            reverse_fetch: true,
            ..FakeProvider::default()
        };
        let left = collect_scope_closure(&normal, &manifest(vec![root.clone()]))
            .await
            .unwrap();
        let right = collect_scope_closure(&reversed, &manifest(vec![root]))
            .await
            .unwrap();
        assert_eq!(
            canonical_json_sha256(&left).unwrap(),
            canonical_json_sha256(&right).unwrap()
        );
    }

    #[test]
    fn xlsx_report_is_valid_zip_and_tagged_to_current_run() {
        let closure_check_id = Uuid::new_v4();
        let bytes = build_xlsx_report(closure_check_id, &[]).unwrap();
        let mut archive = ZipArchive::new(Cursor::new(bytes)).unwrap();
        let mut workbook = String::new();
        std::io::Read::read_to_string(
            &mut archive.by_name("xl/workbook.xml").unwrap(),
            &mut workbook,
        )
        .unwrap();
        for name in [
            "Summary",
            "Closure Issues",
            "Occurrences",
            "Affected Datasets",
        ] {
            assert!(workbook.contains(format!("name=\"{name}\"").as_str()));
        }
        let mut worksheet = String::new();
        std::io::Read::read_to_string(
            &mut archive.by_name("xl/worksheets/sheet1.xml").unwrap(),
            &mut worksheet,
        )
        .unwrap();
        assert!(worksheet.contains(closure_check_id.to_string().as_str()));
        for sheet_number in 2..=4 {
            assert!(
                archive
                    .by_name(format!("xl/worksheets/sheet{sheet_number}.xml").as_str())
                    .is_ok()
            );
        }
    }

    #[test]
    fn short_exact_versions_are_normalized_without_changing_omitted_semantics() {
        assert_eq!(normalize_exact_version("01.02").unwrap(), "01.02.000");
        assert_eq!(normalize_exact_version("01.02.003").unwrap(), "01.02.003");
        assert!(normalize_exact_version("01").is_err());
    }

    #[test]
    fn frozen_artifact_freshness_does_not_require_current_membership() {
        assert!(freshness_policy_accepts_current_release(
            "frozen-artifact-reusable-v1",
            false
        ));
        assert!(!freshness_policy_accepts_current_release(
            "current-membership-required-v1",
            false
        ));
        assert!(freshness_policy_accepts_current_release(
            "current-membership-required-v1",
            true
        ));
        assert!(!freshness_policy_accepts_current_release("unknown", true));
    }

    #[test]
    fn coalesced_issue_preserves_each_reference_occurrence() {
        let target = identity(
            DatasetCategory::Flows,
            "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        );
        let mut process_issue = missing_dataset_issue(&target, true);
        process_issue.occurrences = vec![ClosureIssueOccurrence {
            occurrence_key: "process-exchange".to_owned(),
            source: Some(identity(
                DatasetCategory::Processes,
                "ffffffff-ffff-ffff-ffff-ffffffffffff",
            )),
            json_path: Some("$.exchanges[0]".to_owned()),
            reference_role: Some("exchange_flow".to_owned()),
            details: json!({}),
        }];
        let mut method_issue = process_issue.clone();
        method_issue.occurrences = vec![ClosureIssueOccurrence {
            occurrence_key: "lcia-factor".to_owned(),
            source: Some(identity(
                DatasetCategory::Lciamethods,
                "abababab-abab-abab-abab-abababababab",
            )),
            json_path: Some("$.factors[0]".to_owned()),
            reference_role: Some("lcia_factor_flow".to_owned()),
            details: json!({}),
        }];

        let issues = coalesce_issues(vec![process_issue, method_issue]);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].occurrence_count, 2);
        assert_eq!(
            issues[0]
                .occurrences
                .iter()
                .filter_map(|occurrence| occurrence.reference_role.as_deref())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["exchange_flow", "lcia_factor_flow"])
        );
        assert_eq!(
            issue_rpc_projection(&issues[0])["occurrences"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn scan_claim_parser_distinguishes_acquired_busy_and_completed() {
        assert_eq!(
            parse_scan_execution_claim(&json!({"acquired": true})).unwrap(),
            ScanExecutionClaim::Acquired
        );
        assert_eq!(
            parse_scan_execution_claim(&json!({"acquired": false, "completed": false})).unwrap(),
            ScanExecutionClaim::Busy
        );
        let completed_check_id = Uuid::new_v4();
        assert_eq!(
            parse_scan_execution_claim(&json!({
                "acquired": false,
                "completed": true,
                "completedCheckId": completed_check_id,
            }))
            .unwrap(),
            ScanExecutionClaim::Completed { completed_check_id }
        );
    }
}
