use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const PUBLIC_PLUS_OWNER_DRAFT_SCOPE: &str = "public_plus_owner_draft";
pub const SCOPE_MANIFEST_SCHEMA_VERSION: &str = "lca.data_scope.manifest.v1";
pub const SCOPE_PREDICATE_VERSION: &str = "public_state_100_or_authenticated_owner_state_0.v1";
pub const METHOD_SOURCE_REQUEST_SCHEMA_VERSION: &str = "lca.method_factor_source.request.v2";
pub const METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION: &str = "lca.method_factor_source.snapshot.v2";
pub const STATIC_CACHE_BUNDLE_SCHEMA_VERSION: &str = "lcia.static_cache_bundle.v1";
pub const STATIC_CACHE_BUNDLE_MANIFEST_PATH: &str = "lciamethods/cache_manifest.json";
pub const FACTOR_COVERAGE_CONTRACT_SCHEMA_VERSION: &str = "lcia.method_factor_coverage.contract.v2";
pub const FACTOR_COVERAGE_EVIDENCE_SCHEMA_VERSION: &str = "lcia.method_factor_coverage.matrix.v1";
pub const CALCULATION_EVIDENCE_SCHEMA_VERSION: &str = "lca.calculation_evidence.v2";
pub const UNCHARACTERIZED_ARTIFACT_FORMAT: &str = "lcia-uncharacterized-jsonl:v2";
pub const MISSING_FACTOR_SEMANTICS: &str = "incomplete_coverage_not_zero";
pub const COVERAGE_COUNT_UNIT: &str = "exchange_method_pair";
pub const JSON_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;
pub const RELEASE_BUNDLE_VERSION: &str = "1.2.4";
pub const RELEASE_BUNDLE_MANIFEST_SHA256: &str =
    "e9b4e7f9a5125bb921efbffba9a4b50711f9ea982e22b500f35211884a0479c5";
pub const RELEASE_SOURCE_SNAPSHOT_SHA256: &str =
    "4efbe0b027969dc2b3b151a84422b3fb749bf1fc2d334c60d1fcf37bf7cc2c11";
pub const RELEASE_METHOD_MANIFEST_SHA256: &str =
    "801e886d2d02fc57c6815cfae2f33904139597c1665b55ee0f57bcacdd6be609";
pub const RELEASE_METHOD_IDENTITY_MANIFEST_SHA256: &str =
    "dedd7f932f8418a2babb0a9b3ac93c7c812bda4988f974859ac6981e855a0b19";
pub const RELEASE_FACTOR_MANIFEST_SHA256: &str =
    "40ffd33323c9882dbd0b0d9c99982bad1752e311062231bcf1f490ee96f92e96";
pub const RELEASE_METHOD_COUNT: u64 = 25;
pub const RELEASE_METHOD_IDENTITIES: [(&str, &str, &str); 25] = [
    (
        "01500b74-7ffb-463e-9bd4-72f17c2263ff",
        "01.00.000",
        "01500b74-7ffb-463e-9bd4-72f17c2263ff",
    ),
    (
        "05316e7a-b254-4bea-9cf0-6bf33eb5c630",
        "01.00.000",
        "05316e7a-b254-4bea-9cf0-6bf33eb5c630",
    ),
    (
        "14af9ca7-aa1d-4832-b1d9-ab05a06dcb12",
        "01.00.000",
        "14af9ca7-aa1d-4832-b1d9-ab05a06dcb12",
    ),
    (
        "2299222a-bbd8-474f-9d4f-4dd1f18aea7c",
        "01.01.000",
        "2299222a-bbd8-474f-9d4f-4dd1f18aea7c",
    ),
    (
        "503699e0-eca9-4089-8bf8-e0f49c93e578",
        "01.01.000",
        "9ec743ea-6b00-400d-a53b-61547a3fc03c",
    ),
    (
        "6209b35f-9447-40b5-b68c-a1099e3674a0",
        "01.00.000",
        "6209b35f-9447-40b5-b68c-a1099e3674a0",
    ),
    (
        "706261af-a357-4cc0-a50a-f3033fcbd556",
        "01.00.000",
        "706261af-a357-4cc0-a50a-f3033fcbd556",
    ),
    (
        "7cfdcfcf-b222-4b26-888a-a55f9fbf7ac8",
        "01.00.000",
        "7cfdcfcf-b222-4b26-888a-a55f9fbf7ac8",
    ),
    (
        "7fce5b3a-66b8-4ce1-91e8-a925aee1f186",
        "01.00.000",
        "7fce5b3a-66b8-4ce1-91e8-a925aee1f186",
    ),
    (
        "8c3141e9-1f15-43b5-bff2-182e49893a46",
        "01.00.000",
        "8c3141e9-1f15-43b5-bff2-182e49893a46",
    ),
    (
        "9d1d43a2-e1aa-4c16-acd4-3dd8a6a2fb85",
        "01.00.000",
        "9d1d43a2-e1aa-4c16-acd4-3dd8a6a2fb85",
    ),
    (
        "b2ad6110-c78d-11e6-9d9d-cec0c932ce01",
        "01.00.010",
        "b2ad6110-c78d-11e6-9d9d-cec0c932ce01",
    ),
    (
        "b2ad6494-c78d-11e6-9d9d-cec0c932ce01",
        "01.00.010",
        "b2ad6494-c78d-11e6-9d9d-cec0c932ce01",
    ),
    (
        "b2ad66ce-c78d-11e6-9d9d-cec0c932ce01",
        "03.00.014",
        "b2ad66ce-c78d-11e6-9d9d-cec0c932ce01",
    ),
    (
        "b2ad6890-c78d-11e6-9d9d-cec0c932ce01",
        "01.00.010",
        "b2ad6890-c78d-11e6-9d9d-cec0c932ce01",
    ),
    (
        "b53ec18f-7377-4ad3-86eb-cc3f4f276b2b",
        "01.00.010",
        "b53ec18f-7377-4ad3-86eb-cc3f4f276b2b",
    ),
    (
        "b5c602c6-def3-11e6-bf01-fe55135034f3",
        "02.00.011",
        "b5c602c6-def3-11e6-bf01-fe55135034f3",
    ),
    (
        "b5c610fe-def3-11e6-bf01-fe55135034f3",
        "02.01.000",
        "b5c610fe-def3-11e6-bf01-fe55135034f3",
    ),
    (
        "b5c611c6-def3-11e6-bf01-fe55135034f3",
        "01.04.000",
        "b5c611c6-def3-11e6-bf01-fe55135034f3",
    ),
    (
        "b5c614d2-def3-11e6-bf01-fe55135034f3",
        "01.02.009",
        "b5c614d2-def3-11e6-bf01-fe55135034f3",
    ),
    (
        "b5c619fa-def3-11e6-bf01-fe55135034f3",
        "02.00.010",
        "b5c619fa-def3-11e6-bf01-fe55135034f3",
    ),
    (
        "b5c629d6-def3-11e6-bf01-fe55135034f3",
        "02.00.012",
        "b5c629d6-def3-11e6-bf01-fe55135034f3",
    ),
    (
        "b5c632be-def3-11e6-bf01-fe55135034f3",
        "01.00.011",
        "b5c632be-def3-11e6-bf01-fe55135034f3",
    ),
    (
        "dacd48b5-4da5-49aa-aff4-cd5f5495c037",
        "01.00.000",
        "dacd48b5-4da5-49aa-aff4-cd5f5495c037",
    ),
    (
        "fd530f00-9325-424a-92ef-aaac67922fd9",
        "01.00.000",
        "fd530f00-9325-424a-92ef-aaac67922fd9",
    ),
];

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
    pub bundle_manifest_path: String,
    pub bundle_manifest_sha256: String,
    pub bundle_version: String,
    pub source_snapshot_sha256: String,
    pub method_manifest_sha256: String,
    pub factor_manifest_sha256: String,
    pub method_identity_manifest_sha256: String,
    pub method_count: u64,
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
pub struct LciaMethodFactorCoverage {
    pub method_id: Uuid,
    pub method_version: String,
    pub artifact_locator_id: Uuid,
    pub counts: LciaFactorCoverageCounts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LciaFactorCoverageEvidence {
    pub schema_version: String,
    pub source_snapshot_sha256: String,
    pub method_manifest_sha256: String,
    pub factor_manifest_sha256: String,
    pub method_identity_manifest_sha256: String,
    pub count_unit: String,
    pub key_dimensions: Vec<String>,
    pub coverage_status: String,
    pub missing_factor_semantics: String,
    pub counts: LciaFactorCoverageCounts,
    pub by_method: Vec<LciaMethodFactorCoverage>,
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
    pub method_id: Uuid,
    pub method_version: String,
    pub artifact_locator_id: Uuid,
    pub flow_uuid: Uuid,
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
        "applies_to": ["processes", "flows"],
        "owner_draft_collaboration_guards": {
            "processes": {"team_id": {"is": null}, "review_id": {"is": null}},
            "flows": {"team_id": {"is": null}, "review_id": {"is": null}},
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
pub fn method_factor_source_contract_fixture() -> Value {
    let methods = RELEASE_METHOD_IDENTITIES
        .iter()
        .map(|(method_id, method_version, artifact_locator_id)| {
            serde_json::json!({
                "method_id": method_id,
                "method_version": method_version,
                "artifact_locator_id": artifact_locator_id,
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "schema_version": METHOD_SOURCE_REQUEST_SCHEMA_VERSION,
        "source_kind": "static_cache_bundle",
        "bundle_manifest_path": STATIC_CACHE_BUNDLE_MANIFEST_PATH,
        "bundle_manifest_sha256": RELEASE_BUNDLE_MANIFEST_SHA256,
        "bundle_manifest": {
            "schema_version": STATIC_CACHE_BUNDLE_SCHEMA_VERSION,
            "source_kind": "static_cache_bundle",
            "bundle_version": RELEASE_BUNDLE_VERSION,
            "source_snapshot_sha256": RELEASE_SOURCE_SNAPSHOT_SHA256,
            "method_manifest_sha256": RELEASE_METHOD_MANIFEST_SHA256,
            "factor_manifest_sha256": RELEASE_FACTOR_MANIFEST_SHA256,
            "method_identity_manifest_sha256": RELEASE_METHOD_IDENTITY_MANIFEST_SHA256,
            "methods": methods,
        },
        "base_url_binding": "worker_trusted_configuration",
        "evidence_schema_version": METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION,
        "snapshot_binding": {
            "required": true,
            "hash_algorithm": "sha256",
            "required_fields": [
                "bundle_manifest_sha256",
                "bundle_version",
                "source_snapshot_sha256",
                "method_manifest_sha256",
                "factor_manifest_sha256",
                "method_identity_manifest_sha256",
                "method_count",
            ],
        },
    })
}

#[must_use]
pub fn expected_factor_coverage_contract() -> Value {
    serde_json::json!({
        "schema_version": FACTOR_COVERAGE_CONTRACT_SCHEMA_VERSION,
        "count_unit": COVERAGE_COUNT_UNIT,
        "require_non_empty_pair_matrix": true,
        "match_key": ["method_id", "method_version", "flow_uuid", "direction"],
        "required_counts": ["matched", "unmatched", "invalid", "unsupported_direction"],
        "required_uncharacterized_fields": [
            "method_id",
            "method_version",
            "artifact_locator_id",
            "flow_uuid",
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

#[allow(clippy::too_many_lines)]
pub fn validate_method_factor_source_request(value: &Value) -> anyhow::Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("LCIA method/factor source must be an object"))?;
    let required_top_level = [
        "schema_version",
        "source_kind",
        "bundle_manifest_path",
        "bundle_manifest_sha256",
        "bundle_manifest",
        "base_url_binding",
        "evidence_schema_version",
        "snapshot_binding",
    ];
    if object.len() != required_top_level.len()
        || required_top_level
            .iter()
            .any(|field| !object.contains_key(*field))
    {
        return Err(anyhow::anyhow!(
            "LCIA method/factor source wrapper fields differ from request.v2"
        ));
    }
    let string = |field: &str| object.get(field).and_then(Value::as_str);
    if string("schema_version") != Some(METHOD_SOURCE_REQUEST_SCHEMA_VERSION)
        || string("source_kind") != Some("static_cache_bundle")
        || string("bundle_manifest_path") != Some(STATIC_CACHE_BUNDLE_MANIFEST_PATH)
        || string("base_url_binding") != Some("worker_trusted_configuration")
        || string("evidence_schema_version") != Some(METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION)
    {
        return Err(anyhow::anyhow!(
            "LCIA method/factor source wrapper values differ from request.v2"
        ));
    }
    normalize_sha256(
        string("bundle_manifest_sha256")
            .ok_or_else(|| anyhow::anyhow!("LCIA bundle manifest SHA-256 is missing"))?,
    )?;
    let expected_snapshot_binding = serde_json::json!({
        "required": true,
        "hash_algorithm": "sha256",
        "required_fields": [
            "bundle_manifest_sha256",
            "bundle_version",
            "source_snapshot_sha256",
            "method_manifest_sha256",
            "factor_manifest_sha256",
            "method_identity_manifest_sha256",
            "method_count",
        ],
    });
    if object.get("snapshot_binding") != Some(&expected_snapshot_binding) {
        return Err(anyhow::anyhow!("LCIA snapshot binding contract drift"));
    }

    let manifest = object
        .get("bundle_manifest")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("LCIA bundle manifest must be an object"))?;
    let manifest_string = |field: &str| manifest.get(field).and_then(Value::as_str);
    if manifest_string("schema_version") != Some(STATIC_CACHE_BUNDLE_SCHEMA_VERSION)
        || manifest_string("source_kind") != Some("static_cache_bundle")
        || manifest_string("bundle_version").is_none_or(str::is_empty)
    {
        return Err(anyhow::anyhow!(
            "LCIA static cache bundle identity is invalid"
        ));
    }
    for field in [
        "source_snapshot_sha256",
        "method_manifest_sha256",
        "factor_manifest_sha256",
        "method_identity_manifest_sha256",
    ] {
        normalize_sha256(
            manifest_string(field)
                .ok_or_else(|| anyhow::anyhow!("LCIA bundle manifest missing {field}"))?,
        )?;
    }
    let methods = manifest
        .get("methods")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("LCIA bundle manifest methods must be an array"))?;
    let release_hashes_match = string("bundle_manifest_sha256")
        == Some(RELEASE_BUNDLE_MANIFEST_SHA256)
        && manifest_string("bundle_version") == Some(RELEASE_BUNDLE_VERSION)
        && manifest_string("source_snapshot_sha256") == Some(RELEASE_SOURCE_SNAPSHOT_SHA256)
        && manifest_string("method_manifest_sha256") == Some(RELEASE_METHOD_MANIFEST_SHA256)
        && manifest_string("factor_manifest_sha256") == Some(RELEASE_FACTOR_MANIFEST_SHA256)
        && manifest_string("method_identity_manifest_sha256")
            == Some(RELEASE_METHOD_IDENTITY_MANIFEST_SHA256)
        && u64::try_from(methods.len()) == Ok(RELEASE_METHOD_COUNT);
    if !release_hashes_match {
        return Err(anyhow::anyhow!(
            "LCIA method/factor source differs from the reviewed release bundle"
        ));
    }
    let mut identity_projection = methods
        .iter()
        .map(|method| {
            serde_json::json!({
                "method_id": method.get("method_id"),
                "method_version": method.get("method_version"),
                "artifact_locator_id": method.get("artifact_locator_id"),
            })
        })
        .collect::<Vec<_>>();
    identity_projection.sort_by_key(|identity| {
        (
            identity["method_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            identity["method_version"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        )
    });
    if canonical_json_sha256(&Value::Array(identity_projection))?
        != RELEASE_METHOD_IDENTITY_MANIFEST_SHA256
    {
        return Err(anyhow::anyhow!(
            "LCIA method/factor source method identities differ from the reviewed release"
        ));
    }
    Ok(())
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

    let method_source = request
        .lcia_method_factor_source
        .ok_or_else(|| anyhow::anyhow!("v2 build requires LCIA method/factor source"))?;
    validate_method_factor_source_request(method_source)?;
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
        lcia_method_factor_source: method_source.clone(),
        lcia_factor_coverage_contract: expected_coverage,
    })
}

#[allow(clippy::too_many_lines)]
pub fn validate_calculation_evidence(evidence: &LcaCalculationEvidence) -> anyhow::Result<()> {
    if evidence.schema_version != CALCULATION_EVIDENCE_SCHEMA_VERSION {
        return Err(anyhow::anyhow!("calculation evidence schema mismatch"));
    }
    normalize_sha256(&evidence.scope_manifest_sha256)?;
    let source = &evidence.lcia_method_factor_source;
    if source.schema_version != METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION
        || source.source_kind != "static_cache_bundle"
        || source.bundle_manifest_path != STATIC_CACHE_BUNDLE_MANIFEST_PATH
        || source.bundle_version != RELEASE_BUNDLE_VERSION
        || source.bundle_manifest_sha256 != RELEASE_BUNDLE_MANIFEST_SHA256
        || source.source_snapshot_sha256 != RELEASE_SOURCE_SNAPSHOT_SHA256
        || source.method_manifest_sha256 != RELEASE_METHOD_MANIFEST_SHA256
        || source.factor_manifest_sha256 != RELEASE_FACTOR_MANIFEST_SHA256
        || source.method_identity_manifest_sha256 != RELEASE_METHOD_IDENTITY_MANIFEST_SHA256
        || source.method_count != RELEASE_METHOD_COUNT
    {
        return Err(anyhow::anyhow!(
            "LCIA method/factor source snapshot invalid"
        ));
    }
    normalize_sha256(&source.source_snapshot_sha256)?;
    normalize_sha256(&source.method_manifest_sha256)?;
    normalize_sha256(&source.factor_manifest_sha256)?;
    normalize_sha256(&source.bundle_manifest_sha256)?;
    normalize_sha256(&source.method_identity_manifest_sha256)?;

    let coverage = &evidence.lcia_factor_coverage;
    if coverage.schema_version != FACTOR_COVERAGE_EVIDENCE_SCHEMA_VERSION
        || coverage.missing_factor_semantics != MISSING_FACTOR_SEMANTICS
        || coverage.count_unit != COVERAGE_COUNT_UNIT
        || coverage.key_dimensions != ["method_id", "method_version", "flow_uuid", "direction"]
        || coverage.source_snapshot_sha256 != source.source_snapshot_sha256
        || coverage.method_manifest_sha256 != source.method_manifest_sha256
        || coverage.factor_manifest_sha256 != source.factor_manifest_sha256
        || coverage.method_identity_manifest_sha256 != source.method_identity_manifest_sha256
    {
        return Err(anyhow::anyhow!("LCIA factor coverage evidence invalid"));
    }
    let by_method_total = coverage
        .by_method
        .iter()
        .try_fold(LciaFactorCoverageCounts::default(), |mut total, item| {
            total.matched = total.matched.checked_add(item.counts.matched)?;
            total.unmatched = total.unmatched.checked_add(item.counts.unmatched)?;
            total.invalid = total.invalid.checked_add(item.counts.invalid)?;
            total.unsupported_direction = total
                .unsupported_direction
                .checked_add(item.counts.unsupported_direction)?;
            Some(total)
        })
        .ok_or_else(|| anyhow::anyhow!("LCIA per-method coverage count overflow"))?;
    if coverage.by_method.len() != usize::try_from(source.method_count)?
        || by_method_total != coverage.counts
    {
        return Err(anyhow::anyhow!(
            "LCIA per-method coverage totals are inconsistent"
        ));
    }
    let mut expected_method_pair_count = None;
    for item in &coverage.by_method {
        let method_pair_count = item
            .counts
            .matched
            .checked_add(item.counts.unmatched)
            .and_then(|count| count.checked_add(item.counts.invalid))
            .and_then(|count| count.checked_add(item.counts.unsupported_direction))
            .ok_or_else(|| anyhow::anyhow!("LCIA method pair count overflow"))?;
        if expected_method_pair_count
            .replace(method_pair_count)
            .is_some_and(|expected| expected != method_pair_count)
        {
            return Err(anyhow::anyhow!(
                "LCIA per-method coverage matrix is truncated or uneven"
            ));
        }
        if method_pair_count > JSON_SAFE_INTEGER_MAX {
            return Err(anyhow::anyhow!(
                "LCIA method pair count exceeds the JSON safe-integer range"
            ));
        }
    }
    let mut method_keys = std::collections::HashSet::new();
    if coverage.by_method.iter().any(|item| {
        item.method_version.trim().is_empty()
            || !method_keys.insert((item.method_id, item.method_version.clone()))
    }) {
        return Err(anyhow::anyhow!(
            "LCIA per-method coverage identities are invalid"
        ));
    }
    let mut identities = coverage
        .by_method
        .iter()
        .map(|item| {
            serde_json::json!({
                "method_id": item.method_id,
                "method_version": item.method_version,
                "artifact_locator_id": item.artifact_locator_id,
            })
        })
        .collect::<Vec<_>>();
    identities.sort_by_key(|identity| {
        (
            identity["method_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            identity["method_version"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        )
    });
    if canonical_json_sha256(&Value::Array(identities))? != source.method_identity_manifest_sha256 {
        return Err(anyhow::anyhow!(
            "LCIA per-method coverage identities differ from source snapshot"
        ));
    }
    let gap_count = coverage
        .counts
        .unmatched
        .checked_add(coverage.counts.invalid)
        .and_then(|count| count.checked_add(coverage.counts.unsupported_direction))
        .ok_or_else(|| anyhow::anyhow!("LCIA factor coverage count overflow"))?;
    let pair_count = coverage
        .counts
        .matched
        .checked_add(gap_count)
        .ok_or_else(|| anyhow::anyhow!("LCIA factor coverage pair count overflow"))?;
    if pair_count == 0 || pair_count > JSON_SAFE_INTEGER_MAX {
        return Err(anyhow::anyhow!(
            "LCIA factor coverage pair count is empty or exceeds the JSON safe-integer range"
        ));
    }
    let expected_global_pair_count = expected_method_pair_count
        .ok_or_else(|| anyhow::anyhow!("LCIA per-method coverage matrix is empty"))?
        .checked_mul(source.method_count)
        .ok_or_else(|| anyhow::anyhow!("LCIA global pair count overflow"))?;
    if pair_count != expected_global_pair_count {
        return Err(anyhow::anyhow!(
            "LCIA global coverage count differs from method matrix cardinality"
        ));
    }
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

pub fn canonical_json_sha256(value: &Value) -> anyhow::Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(canonical_json_bytes(value)?);
    Ok(hex::encode(hasher.finalize()))
}

pub fn canonical_json_bytes(value: &Value) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(&sorted_json(value))?)
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
            method_factor_source_contract_fixture(),
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
            "621966942c1980dd8786b3ccfb0fda040fb77e5a842c9eb97a9e97e9c889841d"
        );
    }

    #[test]
    fn release_method_identity_projection_matches_pinned_digest() {
        let identities = RELEASE_METHOD_IDENTITIES
            .iter()
            .map(|(method_id, method_version, artifact_locator_id)| {
                serde_json::json!({
                    "method_id": method_id,
                    "method_version": method_version,
                    "artifact_locator_id": artifact_locator_id,
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            canonical_json_sha256(&Value::Array(identities)).expect("identity hash"),
            RELEASE_METHOD_IDENTITY_MANIFEST_SHA256
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
        let by_method = RELEASE_METHOD_IDENTITIES
            .iter()
            .map(
                |(method_id, method_version, artifact_locator_id)| LciaMethodFactorCoverage {
                    method_id: Uuid::parse_str(method_id).expect("method id"),
                    method_version: (*method_version).to_owned(),
                    artifact_locator_id: Uuid::parse_str(artifact_locator_id)
                        .expect("artifact locator id"),
                    counts: LciaFactorCoverageCounts {
                        matched: 4,
                        ..LciaFactorCoverageCounts::default()
                    },
                },
            )
            .collect::<Vec<_>>();
        LcaCalculationEvidence {
            schema_version: CALCULATION_EVIDENCE_SCHEMA_VERSION.to_owned(),
            scope_manifest_sha256: scope_hash,
            lcia_method_factor_source: LcaMethodFactorSourceSnapshot {
                schema_version: METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION.to_owned(),
                source_kind: "static_cache_bundle".to_owned(),
                bundle_manifest_path: STATIC_CACHE_BUNDLE_MANIFEST_PATH.to_owned(),
                bundle_manifest_sha256: RELEASE_BUNDLE_MANIFEST_SHA256.to_owned(),
                bundle_version: RELEASE_BUNDLE_VERSION.to_owned(),
                source_snapshot_sha256: RELEASE_SOURCE_SNAPSHOT_SHA256.to_owned(),
                method_manifest_sha256: RELEASE_METHOD_MANIFEST_SHA256.to_owned(),
                factor_manifest_sha256: RELEASE_FACTOR_MANIFEST_SHA256.to_owned(),
                method_identity_manifest_sha256: RELEASE_METHOD_IDENTITY_MANIFEST_SHA256.to_owned(),
                method_count: RELEASE_METHOD_COUNT,
            },
            lcia_factor_coverage: LciaFactorCoverageEvidence {
                schema_version: FACTOR_COVERAGE_EVIDENCE_SCHEMA_VERSION.to_owned(),
                source_snapshot_sha256: RELEASE_SOURCE_SNAPSHOT_SHA256.to_owned(),
                method_manifest_sha256: RELEASE_METHOD_MANIFEST_SHA256.to_owned(),
                factor_manifest_sha256: RELEASE_FACTOR_MANIFEST_SHA256.to_owned(),
                method_identity_manifest_sha256: RELEASE_METHOD_IDENTITY_MANIFEST_SHA256.to_owned(),
                count_unit: COVERAGE_COUNT_UNIT.to_owned(),
                key_dimensions: ["method_id", "method_version", "flow_uuid", "direction"]
                    .into_iter()
                    .map(ToOwned::to_owned)
                    .collect(),
                coverage_status: "complete".to_owned(),
                missing_factor_semantics: MISSING_FACTOR_SEMANTICS.to_owned(),
                counts: LciaFactorCoverageCounts {
                    matched: 4 * RELEASE_METHOD_COUNT,
                    ..LciaFactorCoverageCounts::default()
                },
                by_method,
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
        evidence.lcia_factor_coverage.counts.matched -= 2;
        evidence.lcia_factor_coverage.counts.unmatched = 2;
        evidence.lcia_factor_coverage.by_method[0].counts.matched -= 2;
        evidence.lcia_factor_coverage.by_method[0].counts.unmatched = 2;
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

    #[test]
    fn rejects_false_complete_zero_pair_coverage() {
        let mut evidence = complete_evidence("d".repeat(64));
        evidence.lcia_factor_coverage.counts.matched = 0;
        for method in &mut evidence.lcia_factor_coverage.by_method {
            method.counts.matched = 0;
        }
        assert!(validate_calculation_evidence(&evidence).is_err());
    }

    #[test]
    fn rejects_truncated_per_method_pair_matrix() {
        let mut evidence = complete_evidence("d".repeat(64));
        evidence.lcia_factor_coverage.counts.matched -= 1;
        evidence
            .lcia_factor_coverage
            .by_method
            .last_mut()
            .expect("last method")
            .counts
            .matched -= 1;
        assert!(validate_calculation_evidence(&evidence).is_err());
    }

    #[test]
    fn rejects_coverage_counts_above_json_safe_integer() {
        let mut evidence = complete_evidence("d".repeat(64));
        let unsafe_count = JSON_SAFE_INTEGER_MAX + 1;
        for method in &mut evidence.lcia_factor_coverage.by_method {
            method.counts.matched = unsafe_count;
        }
        evidence.lcia_factor_coverage.counts.matched =
            unsafe_count * evidence.lcia_method_factor_source.method_count;
        assert!(validate_calculation_evidence(&evidence).is_err());
    }
}
