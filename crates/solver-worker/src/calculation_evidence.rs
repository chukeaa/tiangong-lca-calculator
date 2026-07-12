use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const PUBLIC_PLUS_OWNER_DRAFT_SCOPE: &str = "public_plus_owner_draft";
pub const SCOPE_MANIFEST_SCHEMA_VERSION: &str = "lca.data_scope.manifest.v1";
pub const SCOPE_PREDICATE_VERSION: &str = "public_state_100_or_authenticated_owner_state_0.v1";
pub const METHOD_SOURCE_REQUEST_SCHEMA_VERSION: &str = "lca.method_factor_source.request.v1";
pub const METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION: &str = "lca.method_factor_source.snapshot.v1";
pub const FACTOR_COVERAGE_CONTRACT_SCHEMA_VERSION: &str = "lcia.factor_coverage.contract.v1";
pub const FACTOR_COVERAGE_EVIDENCE_SCHEMA_VERSION: &str = "lcia.factor_coverage.v1";
pub const CALCULATION_EVIDENCE_SCHEMA_VERSION: &str = "lca.calculation_evidence.v1";
pub const UNCHARACTERIZED_ARTIFACT_FORMAT: &str = "lcia-uncharacterized-jsonl:v1";
pub const MISSING_FACTOR_SEMANTICS: &str = "incomplete_coverage_not_zero";

/// Raw versioned build fields passed by the Edge producer.
#[derive(Clone, Copy)]
pub struct PublicOwnerDraftBuildRequest<'a> {
    pub all_states: Option<bool>,
    pub process_states: Option<&'a str>,
    pub include_user_id: Option<Uuid>,
    pub include_user_state_codes: Option<&'a str>,
    pub include_user_unassigned_only: Option<bool>,
    pub include_user_review_free_only: Option<bool>,
    pub data_scope: Option<&'a str>,
    pub scope_manifest: Option<&'a Value>,
    pub scope_manifest_sha256: Option<&'a str>,
    pub lcia_method_factor_source: Option<&'a Value>,
    pub lcia_factor_coverage_contract: Option<&'a Value>,
    pub no_lcia: Option<bool>,
    pub requested_by: Option<Uuid>,
}

/// Canonical scope binding after all versioned request fields have been checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPublicOwnerDraftScope {
    pub actor_user_id: Uuid,
    pub scope_manifest: Value,
    pub scope_manifest_sha256: String,
    pub lcia_method_factor_source: Value,
    pub lcia_factor_coverage_contract: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LcaMethodFactorSourceSnapshot {
    pub schema_version: String,
    pub source_kind: String,
    pub relation: String,
    pub source_snapshot_sha256: String,
    pub method_manifest_sha256: String,
    pub factor_manifest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LciaFactorCoverageCounts {
    pub matched: u64,
    pub unmatched: u64,
    pub invalid: u64,
    pub unsupported_direction: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LciaUncharacterizedEvidenceArtifact {
    pub artifact_url: String,
    pub artifact_format: String,
    pub artifact_sha256: String,
    pub record_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LciaFactorCoverageEvidence {
    pub schema_version: String,
    pub coverage_status: String,
    pub missing_factor_semantics: String,
    pub counts: LciaFactorCoverageCounts,
    pub uncharacterized_evidence: Option<LciaUncharacterizedEvidenceArtifact>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LcaCalculationEvidence {
    pub schema_version: String,
    pub scope_manifest_sha256: String,
    pub lcia_method_factor_source: LcaMethodFactorSourceSnapshot,
    pub lcia_factor_coverage: LciaFactorCoverageEvidence,
}

/// One required row in the incomplete-coverage JSONL artifact.
#[derive(Debug, Clone, Serialize)]
pub struct LciaUncharacterizedRecord {
    pub elementary_flow_uuid: Uuid,
    pub flow_version: String,
    pub direction: String,
    pub exchange_id: String,
    pub amount: Option<f64>,
    pub reason: String,
}

#[must_use]
pub fn expected_scope_manifest(actor_user_id: Uuid) -> Value {
    serde_json::json!({
        "schema_version": SCOPE_MANIFEST_SCHEMA_VERSION,
        "scope": PUBLIC_PLUS_OWNER_DRAFT_SCOPE,
        "predicate_version": SCOPE_PREDICATE_VERSION,
        "actor": {
            "kind": "authenticated_user",
            "user_id": actor_user_id,
        },
        "applies_to": ["processes", "flows", "lciamethods"],
        "owner_draft_collaboration_guards": {
            "processes": {"team_id": {"is": null}, "review_id": {"is": null}},
            "flows": {"team_id": {"is": null}, "review_id": {"is": null}},
            "lciamethods": {"team_id": "not_applicable", "review_id": "not_applicable"},
        },
        "predicate": {
            "operator": "or",
            "clauses": [
                {"state_code": {"eq": 100}},
                {
                    "operator": "and",
                    "clauses": [
                        {"user_id": {"eq": actor_user_id}},
                        {"state_code": {"eq": 0}},
                    ],
                },
            ],
        },
    })
}

#[must_use]
pub fn expected_method_factor_source_contract() -> Value {
    serde_json::json!({
        "schema_version": METHOD_SOURCE_REQUEST_SCHEMA_VERSION,
        "source_kind": "database",
        "relation": "public.lciamethods",
        "visibility_binding": "scope_manifest",
        "evidence_schema_version": METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION,
        "snapshot_binding": {
            "required": true,
            "hash_algorithm": "sha256",
            "required_fields": [
                "source_snapshot_sha256",
                "method_manifest_sha256",
                "factor_manifest_sha256",
            ],
        },
    })
}

#[must_use]
pub fn expected_factor_coverage_contract() -> Value {
    serde_json::json!({
        "schema_version": FACTOR_COVERAGE_CONTRACT_SCHEMA_VERSION,
        "match_key": ["elementary_flow_uuid", "direction"],
        "required_counts": ["matched", "unmatched", "invalid", "unsupported_direction"],
        "required_uncharacterized_fields": [
            "elementary_flow_uuid",
            "flow_version",
            "direction",
            "exchange_id",
            "amount",
            "reason",
        ],
        "evidence_delivery": "artifact",
        "evidence_artifact_format": UNCHARACTERIZED_ARTIFACT_FORMAT,
        "incomplete_when_any": ["unmatched", "invalid", "unsupported_direction"],
        "status_field": "coverage_status",
        "complete_status": "complete",
        "incomplete_status": "incomplete_coverage",
        "missing_factor_semantics": MISSING_FACTOR_SEMANTICS,
    })
}

/// Validates the complete v2 producer contract. No legacy defaults are applied.
pub fn validate_public_owner_draft_build_request(
    request: PublicOwnerDraftBuildRequest<'_>,
) -> anyhow::Result<ValidatedPublicOwnerDraftScope> {
    if request.data_scope != Some(PUBLIC_PLUS_OWNER_DRAFT_SCOPE) {
        return Err(anyhow::anyhow!(
            "v2 build requires data_scope={PUBLIC_PLUS_OWNER_DRAFT_SCOPE}"
        ));
    }
    if request.all_states != Some(false) {
        return Err(anyhow::anyhow!("v2 build requires all_states=false"));
    }
    if parse_integer_list(request.process_states)? != vec![100] {
        return Err(anyhow::anyhow!(
            "v2 build requires process_states exactly 100"
        ));
    }
    if parse_integer_list(request.include_user_state_codes)? != vec![0] {
        return Err(anyhow::anyhow!(
            "v2 build requires include_user_state_codes exactly 0"
        ));
    }
    if request.include_user_unassigned_only != Some(true)
        || request.include_user_review_free_only != Some(true)
    {
        return Err(anyhow::anyhow!(
            "v2 build requires owner draft team_id/review_id null guards"
        ));
    }
    if request.no_lcia != Some(false) {
        return Err(anyhow::anyhow!("v2 build requires no_lcia=false"));
    }

    let actor_user_id = request
        .include_user_id
        .ok_or_else(|| anyhow::anyhow!("v2 build requires include_user_id"))?;
    if request.requested_by != Some(actor_user_id) {
        return Err(anyhow::anyhow!(
            "v2 build actor differs from authenticated requested_by"
        ));
    }

    let expected_manifest = expected_scope_manifest(actor_user_id);
    let actual_manifest = request
        .scope_manifest
        .ok_or_else(|| anyhow::anyhow!("v2 build requires scope_manifest"))?;
    if actual_manifest != &expected_manifest {
        return Err(anyhow::anyhow!(
            "v2 build scope_manifest differs from the frozen predicate"
        ));
    }
    let expected_manifest_sha256 = canonical_json_sha256(&expected_manifest)?;
    let actual_manifest_sha256 = normalize_sha256(
        request
            .scope_manifest_sha256
            .ok_or_else(|| anyhow::anyhow!("v2 build requires scope_manifest_sha256"))?,
    )?;
    if actual_manifest_sha256 != expected_manifest_sha256 {
        return Err(anyhow::anyhow!("v2 build scope_manifest hash drift"));
    }

    let expected_method_source = expected_method_factor_source_contract();
    if request.lcia_method_factor_source != Some(&expected_method_source) {
        return Err(anyhow::anyhow!(
            "v2 build LCIA method/factor source contract drift"
        ));
    }
    let expected_coverage = expected_factor_coverage_contract();
    if request.lcia_factor_coverage_contract != Some(&expected_coverage) {
        return Err(anyhow::anyhow!(
            "v2 build LCIA factor coverage contract drift"
        ));
    }

    Ok(ValidatedPublicOwnerDraftScope {
        actor_user_id,
        scope_manifest: expected_manifest,
        scope_manifest_sha256: expected_manifest_sha256,
        lcia_method_factor_source: expected_method_source,
        lcia_factor_coverage_contract: expected_coverage,
    })
}

pub fn validate_calculation_evidence(evidence: &LcaCalculationEvidence) -> anyhow::Result<()> {
    if evidence.schema_version != CALCULATION_EVIDENCE_SCHEMA_VERSION {
        return Err(anyhow::anyhow!("calculation evidence schema mismatch"));
    }
    normalize_sha256(&evidence.scope_manifest_sha256)?;
    let source = &evidence.lcia_method_factor_source;
    if source.schema_version != METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION
        || source.source_kind != "database"
        || source.relation != "public.lciamethods"
    {
        return Err(anyhow::anyhow!(
            "LCIA method/factor source snapshot invalid"
        ));
    }
    normalize_sha256(&source.source_snapshot_sha256)?;
    normalize_sha256(&source.method_manifest_sha256)?;
    normalize_sha256(&source.factor_manifest_sha256)?;

    let coverage = &evidence.lcia_factor_coverage;
    if coverage.schema_version != FACTOR_COVERAGE_EVIDENCE_SCHEMA_VERSION
        || coverage.missing_factor_semantics != MISSING_FACTOR_SEMANTICS
    {
        return Err(anyhow::anyhow!("LCIA factor coverage evidence invalid"));
    }
    let gap_count = coverage
        .counts
        .unmatched
        .checked_add(coverage.counts.invalid)
        .and_then(|count| count.checked_add(coverage.counts.unsupported_direction))
        .ok_or_else(|| anyhow::anyhow!("LCIA factor coverage count overflow"))?;
    match (gap_count, coverage.coverage_status.as_str()) {
        (0, "complete") => {
            if coverage.uncharacterized_evidence.is_some() {
                return Err(anyhow::anyhow!(
                    "complete LCIA factor coverage must not have a gap artifact"
                ));
            }
        }
        (1.., "incomplete_coverage") => {
            let artifact = coverage.uncharacterized_evidence.as_ref().ok_or_else(|| {
                anyhow::anyhow!("incomplete LCIA factor coverage requires a gap artifact")
            })?;
            if artifact.artifact_url.trim().is_empty()
                || artifact.artifact_format != UNCHARACTERIZED_ARTIFACT_FORMAT
                || artifact.record_count == 0
                || artifact.record_count != gap_count
            {
                return Err(anyhow::anyhow!(
                    "LCIA uncharacterized evidence artifact is inconsistent"
                ));
            }
            normalize_sha256(&artifact.artifact_sha256)?;
        }
        _ => {
            return Err(anyhow::anyhow!(
                "LCIA factor coverage status/count mismatch"
            ));
        }
    }
    Ok(())
}

/// Enforces exact solve binding and prevents scoped snapshots from falling back to v1.
pub fn validate_calculation_evidence_binding(
    snapshot_evidence: Option<&LcaCalculationEvidence>,
    request_binding: Option<&LcaCalculationEvidence>,
) -> anyhow::Result<Option<LcaCalculationEvidence>> {
    match (snapshot_evidence, request_binding) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(anyhow::anyhow!(
            "calculation evidence binding supplied for an unbound snapshot"
        )),
        (Some(_), None) => Err(anyhow::anyhow!(
            "scoped snapshot requires calculation_evidence_binding; v1 downgrade rejected"
        )),
        (Some(snapshot), Some(binding)) => {
            validate_calculation_evidence(snapshot)?;
            validate_calculation_evidence(binding)?;
            if snapshot != binding {
                return Err(anyhow::anyhow!(
                    "calculation evidence binding differs from snapshot evidence"
                ));
            }
            Ok(Some(snapshot.clone()))
        }
    }
}

pub fn encode_uncharacterized_jsonl(
    records: &[LciaUncharacterizedRecord],
) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for record in records {
        serde_json::to_writer(&mut bytes, record)?;
        bytes.push(b'\n');
    }
    Ok(bytes)
}

pub fn canonical_json_sha256(value: &Value) -> anyhow::Result<String> {
    let canonical = sorted_json(value);
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&canonical)?);
    Ok(hex::encode(hasher.finalize()))
}

#[must_use]
pub fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn parse_integer_list(value: Option<&str>) -> anyhow::Result<Vec<i32>> {
    let value = value.ok_or_else(|| anyhow::anyhow!("missing integer list"))?;
    let mut values = value
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::parse::<i32>)
        .collect::<Result<Vec<_>, _>>()?;
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

fn normalize_sha256(value: &str) -> anyhow::Result<String> {
    let value = value.trim().to_ascii_lowercase();
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow::anyhow!("invalid sha256 value"));
    }
    Ok(value)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_build_values(actor: Uuid) -> (Value, String, Value, Value) {
        let manifest = expected_scope_manifest(actor);
        let manifest_hash = canonical_json_sha256(&manifest).expect("manifest hash");
        (
            manifest,
            manifest_hash,
            expected_method_factor_source_contract(),
            expected_factor_coverage_contract(),
        )
    }

    #[test]
    fn validates_exact_public_owner_draft_build_contract() {
        let actor = Uuid::new_v4();
        let (manifest, hash, method_source, coverage) = valid_build_values(actor);
        let validated = validate_public_owner_draft_build_request(PublicOwnerDraftBuildRequest {
            all_states: Some(false),
            process_states: Some("100"),
            include_user_id: Some(actor),
            include_user_state_codes: Some("0"),
            include_user_unassigned_only: Some(true),
            include_user_review_free_only: Some(true),
            data_scope: Some(PUBLIC_PLUS_OWNER_DRAFT_SCOPE),
            scope_manifest: Some(&manifest),
            scope_manifest_sha256: Some(&hash),
            lcia_method_factor_source: Some(&method_source),
            lcia_factor_coverage_contract: Some(&coverage),
            no_lcia: Some(false),
            requested_by: Some(actor),
        })
        .expect("valid v2 contract");
        assert_eq!(validated.actor_user_id, actor);
        assert_eq!(validated.scope_manifest_sha256, hash);
    }

    #[test]
    fn scope_hash_matches_edge_canonical_json_implementation() {
        let actor = Uuid::parse_str("dab05739-1a42-421b-8170-3b77146d1d64").expect("actor");
        assert_eq!(
            canonical_json_sha256(&expected_scope_manifest(actor)).expect("scope hash"),
            "348b347f1bc962707aa69010b1e8e2e9f1cdfbc9eff2ca075d4bb625a4309f7d"
        );
    }

    #[test]
    fn rejects_actor_state_and_manifest_drift() {
        let actor = Uuid::new_v4();
        let (manifest, hash, method_source, coverage) = valid_build_values(actor);
        let other = Uuid::new_v4();
        let error = validate_public_owner_draft_build_request(PublicOwnerDraftBuildRequest {
            all_states: Some(false),
            process_states: Some("100,101"),
            include_user_id: Some(actor),
            include_user_state_codes: Some("0"),
            include_user_unassigned_only: Some(true),
            include_user_review_free_only: Some(true),
            data_scope: Some(PUBLIC_PLUS_OWNER_DRAFT_SCOPE),
            scope_manifest: Some(&manifest),
            scope_manifest_sha256: Some(&hash),
            lcia_method_factor_source: Some(&method_source),
            lcia_factor_coverage_contract: Some(&coverage),
            no_lcia: Some(false),
            requested_by: Some(other),
        })
        .expect_err("drift must fail");
        assert!(error.to_string().contains("process_states"));
    }

    fn complete_evidence(scope_hash: String) -> LcaCalculationEvidence {
        LcaCalculationEvidence {
            schema_version: CALCULATION_EVIDENCE_SCHEMA_VERSION.to_owned(),
            scope_manifest_sha256: scope_hash,
            lcia_method_factor_source: LcaMethodFactorSourceSnapshot {
                schema_version: METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION.to_owned(),
                source_kind: "database".to_owned(),
                relation: "public.lciamethods".to_owned(),
                source_snapshot_sha256: "a".repeat(64),
                method_manifest_sha256: "b".repeat(64),
                factor_manifest_sha256: "c".repeat(64),
            },
            lcia_factor_coverage: LciaFactorCoverageEvidence {
                schema_version: FACTOR_COVERAGE_EVIDENCE_SCHEMA_VERSION.to_owned(),
                coverage_status: "complete".to_owned(),
                missing_factor_semantics: MISSING_FACTOR_SEMANTICS.to_owned(),
                counts: LciaFactorCoverageCounts {
                    matched: 4,
                    ..LciaFactorCoverageCounts::default()
                },
                uncharacterized_evidence: None,
            },
        }
    }

    #[test]
    fn solve_binding_rejects_downgrade_and_source_drift() {
        let evidence = complete_evidence("d".repeat(64));
        validate_calculation_evidence_binding(Some(&evidence), Some(&evidence))
            .expect("exact binding");
        assert!(validate_calculation_evidence_binding(Some(&evidence), None).is_err());

        let mut drift = evidence.clone();
        drift.lcia_method_factor_source.factor_manifest_sha256 = "e".repeat(64);
        assert!(validate_calculation_evidence_binding(Some(&evidence), Some(&drift)).is_err());
    }

    #[test]
    fn incomplete_coverage_requires_exact_artifact_count() {
        let mut evidence = complete_evidence("d".repeat(64));
        evidence.lcia_factor_coverage.coverage_status = "incomplete_coverage".to_owned();
        evidence.lcia_factor_coverage.counts.unmatched = 2;
        evidence.lcia_factor_coverage.uncharacterized_evidence =
            Some(LciaUncharacterizedEvidenceArtifact {
                artifact_url: "https://example.invalid/gaps.jsonl".to_owned(),
                artifact_format: UNCHARACTERIZED_ARTIFACT_FORMAT.to_owned(),
                artifact_sha256: "f".repeat(64),
                record_count: 1,
            });
        assert!(validate_calculation_evidence(&evidence).is_err());
        evidence
            .lcia_factor_coverage
            .uncharacterized_evidence
            .as_mut()
            .expect("artifact")
            .record_count = 2;
        validate_calculation_evidence(&evidence).expect("consistent incomplete evidence");
    }
}
