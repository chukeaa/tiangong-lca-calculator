use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::{self, Read},
    path::{Component, Path, PathBuf},
};

use flate2::read::GzDecoder;
use serde::de::Error as _;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{DeserializeSeed, MapAccess, Visitor},
};
use serde_json::Value;
use sha2::Digest;
use uuid::Uuid;

use crate::calculation_evidence::{
    LcaMethodFactorSourceSnapshot, METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION,
    RELEASE_BUNDLE_MANIFEST_SHA256, RELEASE_BUNDLE_VERSION, RELEASE_FACTOR_MANIFEST_SHA256,
    RELEASE_METHOD_COUNT, RELEASE_METHOD_IDENTITY_MANIFEST_SHA256, RELEASE_METHOD_MANIFEST_SHA256,
    RELEASE_SOURCE_SNAPSHOT_SHA256, STATIC_CACHE_BUNDLE_MANIFEST_PATH,
    STATIC_CACHE_BUNDLE_SCHEMA_VERSION, canonical_json_bytes, canonical_json_sha256, sha256_bytes,
    validate_method_factor_source_request,
};

const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
const MAX_STATIC_ASSET_BYTES: u64 = 128 * 1024 * 1024;
const MAX_DECOMPRESSED_FACTOR_BYTES: u64 = 512 * 1024 * 1024;
const STATIC_CACHE_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticLciaBundleFile {
    pub path: String,
    pub media_type: String,
    pub byte_size: u64,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decompressed_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decompressed_byte_size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticLciaIdentityAliasEvidence {
    pub repository: String,
    pub commit: String,
    pub path: String,
    pub sha256: String,
    pub identity_field: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticLciaIdentityAlias {
    pub method_id: Uuid,
    pub method_version: String,
    pub artifact_locator_id: Uuid,
    pub status: String,
    pub evidence: StaticLciaIdentityAliasEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticLciaBundleFiles {
    pub list: StaticLciaBundleFile,
    pub factors: StaticLciaBundleFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticLciaMethodManifestEntry {
    pub method_id: Uuid,
    pub method_version: String,
    pub artifact_locator_id: Uuid,
    pub artifact_filename: String,
    pub factor_entry_count: u64,
    pub unique_flow_direction_key_count: u64,
    pub duplicate_entry_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticLciaBundleManifest {
    pub schema_version: String,
    pub source_kind: String,
    pub bundle_version: String,
    pub source_snapshot_sha256: String,
    pub method_manifest_sha256: String,
    pub method_identity_manifest_sha256: String,
    pub factor_manifest_sha256: String,
    pub hash_algorithm: String,
    pub canonicalization: String,
    pub method_membership_status: String,
    pub release_ready: bool,
    pub files: StaticLciaBundleFiles,
    #[serde(default)]
    pub identity_aliases: Vec<StaticLciaIdentityAlias>,
    pub methods: Vec<StaticLciaMethodManifestEntry>,
    pub source_snapshot_hash_input: Value,
    #[serde(default)]
    pub factor_index_summary: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct StaticLciaMethod {
    pub method_id: Uuid,
    pub method_version: String,
    pub artifact_locator_id: Uuid,
    pub artifact_filename: String,
    pub name: String,
    pub unit: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StaticLciaFactor {
    pub flow_id: Uuid,
    pub direction: StaticLciaDirection,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StaticLciaDirection {
    Input,
    Output,
}

#[derive(Debug, Clone)]
pub struct VerifiedStaticLciaBundle {
    pub source_evidence: LcaMethodFactorSourceSnapshot,
    pub methods: Vec<StaticLciaMethod>,
    pub factors_by_method: BTreeMap<Uuid, Vec<StaticLciaFactor>>,
}

type ParsedFactorIndex = (
    BTreeMap<Uuid, Vec<StaticLciaFactor>>,
    BTreeMap<Uuid, BTreeSet<(Uuid, StaticLciaDirection)>>,
    String,
);

#[derive(Debug, Clone)]
pub enum TrustedStaticCacheSource {
    Directory(PathBuf),
    BaseUrl(reqwest::Url),
}

impl TrustedStaticCacheSource {
    pub fn new(directory: Option<PathBuf>, base_url: Option<String>) -> anyhow::Result<Self> {
        match (directory, base_url) {
            (Some(directory), None) => Ok(Self::Directory(directory)),
            (None, Some(base_url)) => {
                let mut base_url = reqwest::Url::parse(base_url.trim())?;
                let loopback_http = base_url.scheme() == "http"
                    && base_url.host_str().is_some_and(|host| {
                        host.eq_ignore_ascii_case("localhost")
                            || host
                                .parse::<std::net::IpAddr>()
                                .is_ok_and(|address| address.is_loopback())
                    });
                if base_url.scheme() != "https" && !loopback_http {
                    return Err(anyhow::anyhow!(
                        "LCIA static cache base URL must use HTTPS (HTTP is loopback-test only)"
                    ));
                }
                if !base_url.path().ends_with('/') {
                    base_url.set_path(&format!("{}/", base_url.path()));
                }
                Ok(Self::BaseUrl(base_url))
            }
            (None, None) => Err(anyhow::anyhow!(
                "versioned LCIA build requires LCIA_STATIC_CACHE_DIR or LCIA_STATIC_CACHE_BASE_URL"
            )),
            (Some(_), Some(_)) => Err(anyhow::anyhow!(
                "LCIA_STATIC_CACHE_DIR and LCIA_STATIC_CACHE_BASE_URL are mutually exclusive"
            )),
        }
    }

    pub async fn read(
        &self,
        relative_path: &str,
        max_bytes: u64,
        expected_bytes: Option<u64>,
    ) -> anyhow::Result<Vec<u8>> {
        validate_relative_asset_path(relative_path)?;
        match self {
            Self::Directory(root) => {
                let path = root.join(relative_path);
                let byte_size = tokio::fs::metadata(&path).await?.len();
                validate_asset_size(relative_path, byte_size, max_bytes, expected_bytes)?;
                Ok(tokio::fs::read(path).await?)
            }
            Self::BaseUrl(base) => {
                let url = base.join(relative_path)?;
                if url.origin() != base.origin() || !url.as_str().starts_with(base.as_str()) {
                    return Err(anyhow::anyhow!(
                        "LCIA static cache asset escaped trusted base URL"
                    ));
                }
                let client = reqwest::Client::builder()
                    .timeout(STATIC_CACHE_FETCH_TIMEOUT)
                    .build()?;
                let mut response = client.get(url).send().await?;
                if !response.status().is_success() {
                    return Err(anyhow::anyhow!(
                        "LCIA static cache fetch failed with status {}",
                        response.status()
                    ));
                }
                if let Some(content_length) = response.content_length() {
                    validate_asset_size(relative_path, content_length, max_bytes, expected_bytes)?;
                }
                let capacity = usize::try_from(expected_bytes.unwrap_or(max_bytes).min(max_bytes))?;
                let mut bytes = Vec::with_capacity(capacity);
                while let Some(chunk) = response.chunk().await? {
                    let next_size = u64::try_from(bytes.len())?
                        .checked_add(u64::try_from(chunk.len())?)
                        .ok_or_else(|| anyhow::anyhow!("LCIA static asset byte-size overflow"))?;
                    if next_size > max_bytes || expected_bytes.is_some_and(|size| next_size > size)
                    {
                        return Err(anyhow::anyhow!(
                            "LCIA static cache asset exceeded its declared size"
                        ));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                validate_asset_size(
                    relative_path,
                    u64::try_from(bytes.len())?,
                    max_bytes,
                    expected_bytes,
                )?;
                Ok(bytes)
            }
        }
    }
}

pub async fn load_verified_static_lcia_bundle(
    source: &TrustedStaticCacheSource,
    request: &Value,
) -> anyhow::Result<VerifiedStaticLciaBundle> {
    validate_method_factor_source_request(request)?;
    validate_release_request_binding(request)?;
    let manifest_bytes = source
        .read(STATIC_CACHE_BUNDLE_MANIFEST_PATH, MAX_MANIFEST_BYTES, None)
        .await?;
    let manifest = verify_manifest_envelope(request, &manifest_bytes, true)?;
    let list_path = bundle_asset_path(&manifest.files.list.path)?;
    let factors_path = bundle_asset_path(&manifest.files.factors.path)?;
    let (list_bytes, factor_gzip_bytes) = tokio::try_join!(
        source.read(
            &list_path,
            MAX_STATIC_ASSET_BYTES,
            Some(manifest.files.list.byte_size),
        ),
        source.read(
            &factors_path,
            MAX_STATIC_ASSET_BYTES,
            Some(manifest.files.factors.byte_size),
        ),
    )?;
    verify_static_lcia_bundle(request, &manifest_bytes, &list_bytes, &factor_gzip_bytes)
}

fn validate_release_request_binding(request: &Value) -> anyhow::Result<()> {
    let request = request
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("LCIA method source request must be an object"))?;
    let manifest = request
        .get("bundle_manifest")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("LCIA bundle manifest must be an object"))?;
    let matches_release = request
        .get("bundle_manifest_sha256")
        .and_then(Value::as_str)
        == Some(RELEASE_BUNDLE_MANIFEST_SHA256)
        && manifest.get("bundle_version").and_then(Value::as_str) == Some(RELEASE_BUNDLE_VERSION)
        && manifest
            .get("source_snapshot_sha256")
            .and_then(Value::as_str)
            == Some(RELEASE_SOURCE_SNAPSHOT_SHA256)
        && manifest
            .get("method_manifest_sha256")
            .and_then(Value::as_str)
            == Some(RELEASE_METHOD_MANIFEST_SHA256)
        && manifest
            .get("method_identity_manifest_sha256")
            .and_then(Value::as_str)
            == Some(RELEASE_METHOD_IDENTITY_MANIFEST_SHA256)
        && manifest
            .get("factor_manifest_sha256")
            .and_then(Value::as_str)
            == Some(RELEASE_FACTOR_MANIFEST_SHA256)
        && manifest
            .get("methods")
            .and_then(Value::as_array)
            .is_some_and(|methods| u64::try_from(methods.len()) == Ok(RELEASE_METHOD_COUNT));
    if !matches_release {
        return Err(anyhow::anyhow!(
            "LCIA static cache request does not match the reviewed release bundle"
        ));
    }
    Ok(())
}

fn verify_manifest_envelope(
    request: &Value,
    manifest_bytes: &[u8],
    enforce_release_request: bool,
) -> anyhow::Result<StaticLciaBundleManifest> {
    if enforce_release_request {
        validate_method_factor_source_request(request)?;
    }
    let request_object = request.as_object().expect("validated request object");
    let expected_manifest_sha256 = request_object["bundle_manifest_sha256"]
        .as_str()
        .expect("validated manifest SHA-256");
    if sha256_bytes(manifest_bytes) != expected_manifest_sha256 {
        return Err(anyhow::anyhow!("LCIA bundle manifest raw SHA-256 mismatch"));
    }
    let manifest_value: Value = serde_json::from_slice(manifest_bytes)?;
    if request_object.get("bundle_manifest") != Some(&manifest_value) {
        return Err(anyhow::anyhow!(
            "LCIA fetched bundle manifest differs from embedded request manifest"
        ));
    }
    let manifest = serde_json::from_value(manifest_value)?;
    validate_manifest_identity(&manifest)?;
    Ok(manifest)
}

pub fn verify_static_lcia_bundle(
    request: &Value,
    manifest_bytes: &[u8],
    list_bytes: &[u8],
    factor_gzip_bytes: &[u8],
) -> anyhow::Result<VerifiedStaticLciaBundle> {
    verify_static_lcia_bundle_inner(request, manifest_bytes, list_bytes, factor_gzip_bytes, true)
}

#[allow(clippy::too_many_lines)]
fn verify_static_lcia_bundle_inner(
    request: &Value,
    manifest_bytes: &[u8],
    list_bytes: &[u8],
    factor_gzip_bytes: &[u8],
    enforce_release_request: bool,
) -> anyhow::Result<VerifiedStaticLciaBundle> {
    let manifest = verify_manifest_envelope(request, manifest_bytes, enforce_release_request)?;
    let request_object = request.as_object().expect("validated request object");
    let expected_manifest_sha256 = request_object["bundle_manifest_sha256"]
        .as_str()
        .expect("validated manifest SHA-256");
    verify_hash("LCIA method list", list_bytes, &manifest.files.list.sha256)?;
    verify_hash(
        "LCIA compressed factor index",
        factor_gzip_bytes,
        &manifest.files.factors.sha256,
    )?;
    validate_asset_size(
        "list.json",
        u64::try_from(list_bytes.len())?,
        MAX_STATIC_ASSET_BYTES,
        Some(manifest.files.list.byte_size),
    )?;
    validate_asset_size(
        "flow_factors.json.gz",
        u64::try_from(factor_gzip_bytes.len())?,
        MAX_STATIC_ASSET_BYTES,
        Some(manifest.files.factors.byte_size),
    )?;
    // The exact compressed bytes and the self-hashed source manifest bind the generator's
    // canonical digest. Parsing below independently verifies every normalized tuple/count.
    if manifest.files.factors.canonical_sha256.as_deref()
        != Some(manifest.factor_manifest_sha256.as_str())
    {
        return Err(anyhow::anyhow!(
            "LCIA canonical factor manifest SHA-256 mismatch"
        ));
    }

    let method_value = serde_json::to_value(&manifest.methods)?;
    if canonical_json_sha256(&method_value)? != manifest.method_manifest_sha256 {
        return Err(anyhow::anyhow!("LCIA method manifest SHA-256 mismatch"));
    }

    let list_value: Value = serde_json::from_slice(list_bytes)?;
    let list_files = list_value
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("LCIA method list files must be an array"))?;
    let mut list_by_locator = HashMap::<(Uuid, String), &Value>::new();
    for file in list_files {
        let locator = parse_uuid_field(file, "id")?;
        let version = required_string(file, "version")?.to_owned();
        if list_by_locator.insert((locator, version), file).is_some() {
            return Err(anyhow::anyhow!("duplicate LCIA method list identity"));
        }
    }

    let mut manifest_by_method = BTreeMap::new();
    let mut methods = Vec::with_capacity(manifest.methods.len());
    for entry in &manifest.methods {
        if entry.method_version.trim().is_empty()
            || entry.artifact_filename.trim().is_empty()
            || entry.factor_entry_count
                != entry
                    .unique_flow_direction_key_count
                    .checked_add(entry.duplicate_entry_count)
                    .ok_or_else(|| anyhow::anyhow!("LCIA factor count overflow"))?
        {
            return Err(anyhow::anyhow!("invalid LCIA method manifest entry"));
        }
        if manifest_by_method.insert(entry.method_id, entry).is_some() {
            return Err(anyhow::anyhow!("duplicate LCIA canonical method id"));
        }
        let list_file = list_by_locator
            .get(&(entry.artifact_locator_id, entry.method_version.clone()))
            .ok_or_else(|| anyhow::anyhow!("LCIA method locator is missing from list.json"))?;
        if required_string(list_file, "filename")? != entry.artifact_filename {
            return Err(anyhow::anyhow!(
                "LCIA method filename differs from list.json"
            ));
        }
        methods.push(StaticLciaMethod {
            method_id: entry.method_id,
            method_version: entry.method_version.clone(),
            artifact_locator_id: entry.artifact_locator_id,
            artifact_filename: entry.artifact_filename.clone(),
            name: localized_text(list_file.get("description"))
                .unwrap_or_else(|| format!("LCIA Method {}", entry.method_id)),
            unit: localized_text(
                list_file
                    .get("referenceQuantity")
                    .and_then(|value| value.get("common:shortDescription")),
            )
            .unwrap_or_else(|| "unknown".to_owned()),
        });
    }
    if list_by_locator.len() != methods.len() {
        return Err(anyhow::anyhow!(
            "LCIA method list contains unmanifested entries"
        ));
    }
    validate_identity_aliases(&manifest)?;

    let (factors_by_method, unique_keys, canonical_factor_sha256) = parse_factor_index_streaming(
        factor_gzip_bytes,
        &manifest_by_method,
        manifest
            .files
            .factors
            .decompressed_byte_size
            .ok_or_else(|| anyhow::anyhow!("LCIA factor decompressed byte size is missing"))?,
        manifest
            .files
            .factors
            .decompressed_sha256
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("LCIA factor decompressed SHA-256 is missing"))?,
    )?;
    if canonical_factor_sha256 != manifest.factor_manifest_sha256 {
        return Err(anyhow::anyhow!(
            "LCIA streamed canonical factor manifest SHA-256 mismatch"
        ));
    }

    for entry in &manifest.methods {
        let factor_count = u64::try_from(factors_by_method[&entry.method_id].len())?;
        let unique_count = u64::try_from(unique_keys[&entry.method_id].len())?;
        if factor_count != entry.factor_entry_count
            || unique_count != entry.unique_flow_direction_key_count
            || factor_count - unique_count != entry.duplicate_entry_count
        {
            return Err(anyhow::anyhow!(
                "LCIA factor cardinality differs for {}@{}",
                entry.method_id,
                entry.method_version
            ));
        }
    }

    methods.sort_by_key(|method| (method.method_id, method.method_version.clone()));
    let method_identity_manifest_sha256 = method_identity_manifest_sha256(&methods)?;
    if method_identity_manifest_sha256 != manifest.method_identity_manifest_sha256 {
        return Err(anyhow::anyhow!(
            "LCIA method identity manifest SHA-256 mismatch"
        ));
    }
    let method_count = u64::try_from(methods.len())?;
    Ok(VerifiedStaticLciaBundle {
        source_evidence: LcaMethodFactorSourceSnapshot {
            schema_version: METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION.to_owned(),
            source_kind: "static_cache_bundle".to_owned(),
            bundle_manifest_path: STATIC_CACHE_BUNDLE_MANIFEST_PATH.to_owned(),
            bundle_manifest_sha256: expected_manifest_sha256.to_owned(),
            bundle_version: manifest.bundle_version,
            source_snapshot_sha256: manifest.source_snapshot_sha256,
            method_manifest_sha256: manifest.method_manifest_sha256,
            factor_manifest_sha256: manifest.factor_manifest_sha256,
            method_identity_manifest_sha256,
            method_count,
        },
        methods,
        factors_by_method,
    })
}

pub fn method_identity_manifest_sha256(methods: &[StaticLciaMethod]) -> anyhow::Result<String> {
    let mut identities = methods
        .iter()
        .map(|method| {
            serde_json::json!({
                "method_id": method.method_id,
                "method_version": method.method_version,
                "artifact_locator_id": method.artifact_locator_id,
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
    canonical_json_sha256(&Value::Array(identities))
}

#[derive(Deserialize, Serialize)]
struct FactorIndexRecord {
    #[serde(rename = "@refObjectId")]
    flow_id: Uuid,
    #[serde(rename = "@version")]
    flow_version: String,
    #[serde(rename = "exchangeDirection")]
    direction: String,
    factor: OneOrMany<FactorIndexItem>,
}

#[derive(Deserialize, Serialize)]
struct FactorIndexItem {
    key: Uuid,
    value: Value,
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    fn into_vec(self) -> Vec<T> {
        match self {
            Self::One(value) => vec![value],
            Self::Many(values) => values,
        }
    }
}

struct FactorIndexAccumulator<'a> {
    allowed_methods: &'a BTreeMap<Uuid, &'a StaticLciaMethodManifestEntry>,
    factors_by_method: BTreeMap<Uuid, Vec<StaticLciaFactor>>,
    unique_keys: BTreeMap<Uuid, BTreeSet<(Uuid, StaticLciaDirection)>>,
    canonical_records: BTreeMap<String, Vec<u8>>,
    canonical_record_bytes: u64,
}

struct FactorIndexSeed<'a, 'b> {
    accumulator: &'a mut FactorIndexAccumulator<'b>,
}

impl<'de> DeserializeSeed<'de> for FactorIndexSeed<'_, '_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(FactorIndexVisitor {
            accumulator: self.accumulator,
        })
    }
}

struct FactorIndexVisitor<'a, 'b> {
    accumulator: &'a mut FactorIndexAccumulator<'b>,
}

impl<'de> Visitor<'de> for FactorIndexVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an LCIA flow-direction factor map")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some((index_key, record)) = map.next_entry::<String, FactorIndexRecord>()? {
            if record.flow_version.trim().is_empty() {
                return Err(A::Error::custom("LCIA factor flow version is empty"));
            }
            let canonical_record =
                canonical_json_bytes(&serde_json::to_value(&record).map_err(A::Error::custom)?)
                    .map_err(A::Error::custom)?;
            self.accumulator.canonical_record_bytes = self
                .accumulator
                .canonical_record_bytes
                .checked_add(u64::try_from(canonical_record.len()).map_err(A::Error::custom)?)
                .ok_or_else(|| A::Error::custom("LCIA canonical factor byte-size overflow"))?;
            if self.accumulator.canonical_record_bytes > MAX_DECOMPRESSED_FACTOR_BYTES
                || self
                    .accumulator
                    .canonical_records
                    .insert(index_key.clone(), canonical_record)
                    .is_some()
            {
                return Err(A::Error::custom(
                    "LCIA canonical factor records exceed size or contain duplicates",
                ));
            }
            let direction = parse_direction(&record.direction).map_err(A::Error::custom)?;
            let expected_key = format!(
                "{}:{}",
                record.flow_id,
                match direction {
                    StaticLciaDirection::Input => "INPUT",
                    StaticLciaDirection::Output => "OUTPUT",
                }
            );
            if index_key != expected_key {
                return Err(A::Error::custom("LCIA factor index key/record mismatch"));
            }
            let factors = record.factor.into_vec();
            if factors.is_empty() {
                return Err(A::Error::custom("LCIA factor index record is empty"));
            }
            for factor in factors {
                if !self.accumulator.allowed_methods.contains_key(&factor.key) {
                    return Err(A::Error::custom(format!(
                        "LCIA factor references an unmanifested method {}",
                        factor.key
                    )));
                }
                let value = parse_finite_number(Some(&factor.value)).map_err(A::Error::custom)?;
                self.accumulator
                    .factors_by_method
                    .get_mut(&factor.key)
                    .expect("manifested method factor vector")
                    .push(StaticLciaFactor {
                        flow_id: record.flow_id,
                        direction,
                        value,
                    });
                self.accumulator
                    .unique_keys
                    .get_mut(&factor.key)
                    .expect("manifested method key set")
                    .insert((record.flow_id, direction));
            }
        }
        Ok(())
    }
}

struct CappedHashingReader<R> {
    inner: R,
    byte_count: u64,
    byte_limit: u64,
    hasher: sha2::Sha256,
}

impl<R> CappedHashingReader<R> {
    fn new(inner: R, byte_limit: u64) -> Self {
        Self {
            inner,
            byte_count: 0,
            byte_limit,
            hasher: sha2::Sha256::new(),
        }
    }

    fn digest(&self) -> String {
        hex::encode(self.hasher.clone().finalize())
    }
}

impl<R: Read> Read for CappedHashingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let remaining_with_probe = self
            .byte_limit
            .saturating_sub(self.byte_count)
            .saturating_add(1);
        let max_read = usize::try_from(remaining_with_probe)
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let read = self.inner.read(&mut buffer[..max_read])?;
        let next_count = self
            .byte_count
            .checked_add(u64::try_from(read).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("LCIA decompressed byte-size overflow"))?;
        if next_count > self.byte_limit {
            return Err(io::Error::other(
                "LCIA decompressed factor index exceeded its declared size",
            ));
        }
        self.byte_count = next_count;
        self.hasher.update(&buffer[..read]);
        Ok(read)
    }
}

fn parse_factor_index_streaming(
    gzip_bytes: &[u8],
    allowed_methods: &BTreeMap<Uuid, &StaticLciaMethodManifestEntry>,
    decompressed_byte_size: u64,
    decompressed_sha256: &str,
) -> anyhow::Result<ParsedFactorIndex> {
    if decompressed_byte_size > MAX_DECOMPRESSED_FACTOR_BYTES {
        return Err(anyhow::anyhow!(
            "LCIA decompressed factor index exceeds the hard size cap"
        ));
    }
    validate_sha256(decompressed_sha256)?;
    let decoder = GzDecoder::new(gzip_bytes);
    let mut reader = CappedHashingReader::new(decoder, decompressed_byte_size);
    let mut accumulator = FactorIndexAccumulator {
        allowed_methods,
        factors_by_method: allowed_methods
            .keys()
            .map(|method_id| (*method_id, Vec::new()))
            .collect(),
        unique_keys: allowed_methods
            .keys()
            .map(|method_id| (*method_id, BTreeSet::new()))
            .collect(),
        canonical_records: BTreeMap::new(),
        canonical_record_bytes: 0,
    };
    {
        let mut deserializer = serde_json::Deserializer::from_reader(&mut reader);
        FactorIndexSeed {
            accumulator: &mut accumulator,
        }
        .deserialize(&mut deserializer)?;
        deserializer.end()?;
    }
    if reader.byte_count != decompressed_byte_size || reader.digest() != decompressed_sha256 {
        return Err(anyhow::anyhow!(
            "LCIA decompressed factor index size/SHA-256 mismatch"
        ));
    }
    let mut canonical_hasher = sha2::Sha256::new();
    canonical_hasher.update(b"{");
    for (index, (key, record)) in accumulator.canonical_records.iter().enumerate() {
        if index > 0 {
            canonical_hasher.update(b",");
        }
        canonical_hasher.update(serde_json::to_vec(key)?);
        canonical_hasher.update(b":");
        canonical_hasher.update(record);
    }
    canonical_hasher.update(b"}");
    Ok((
        accumulator.factors_by_method,
        accumulator.unique_keys,
        hex::encode(canonical_hasher.finalize()),
    ))
}

fn validate_manifest_identity(manifest: &StaticLciaBundleManifest) -> anyhow::Result<()> {
    if manifest.schema_version != STATIC_CACHE_BUNDLE_SCHEMA_VERSION
        || manifest.source_kind != "static_cache_bundle"
        || manifest.bundle_version.trim().is_empty()
        || manifest.hash_algorithm != "sha256"
        || manifest.canonicalization != "sorted_object_keys_preserve_array_order.v1"
        || manifest.method_membership_status != "consistent_with_verified_aliases"
        || !manifest.release_ready
        || manifest.methods.is_empty()
        || manifest.files.list.path != "list.json"
        || manifest.files.list.media_type != "application/json"
        || manifest.files.factors.path != "flow_factors.json.gz"
        || manifest.files.factors.media_type != "application/gzip"
        || manifest.files.list.byte_size == 0
        || manifest.files.factors.byte_size == 0
        || manifest.files.factors.decompressed_byte_size == Some(0)
    {
        return Err(anyhow::anyhow!(
            "LCIA static cache manifest is not release-ready"
        ));
    }
    for hash in [
        &manifest.source_snapshot_sha256,
        &manifest.method_manifest_sha256,
        &manifest.method_identity_manifest_sha256,
        &manifest.factor_manifest_sha256,
        &manifest.files.list.sha256,
        &manifest.files.factors.sha256,
    ] {
        validate_sha256(hash)?;
    }
    let expected_hash_input = serde_json::json!({
        "schema_version": manifest.schema_version,
        "source_kind": manifest.source_kind,
        "bundle_version": manifest.bundle_version,
        "method_manifest_sha256": manifest.method_manifest_sha256,
        "method_identity_manifest_sha256": manifest.method_identity_manifest_sha256,
        "factor_manifest_sha256": manifest.factor_manifest_sha256,
        "files": manifest.files,
    });
    if manifest.source_snapshot_hash_input != expected_hash_input
        || canonical_json_sha256(&manifest.source_snapshot_hash_input)?
            != manifest.source_snapshot_sha256
    {
        return Err(anyhow::anyhow!(
            "LCIA source snapshot hash input/digest mismatch"
        ));
    }
    if manifest.files.list.byte_size > MAX_STATIC_ASSET_BYTES
        || manifest.files.factors.byte_size > MAX_STATIC_ASSET_BYTES
        || manifest
            .files
            .factors
            .decompressed_byte_size
            .is_none_or(|size| size > MAX_DECOMPRESSED_FACTOR_BYTES)
    {
        return Err(anyhow::anyhow!(
            "LCIA static cache manifest exceeds size policy"
        ));
    }
    Ok(())
}

fn validate_identity_aliases(manifest: &StaticLciaBundleManifest) -> anyhow::Result<()> {
    let required = manifest
        .methods
        .iter()
        .filter(|method| method.method_id != method.artifact_locator_id)
        .map(|method| {
            (
                method.method_id,
                method.method_version.clone(),
                method.artifact_locator_id,
            )
        })
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for alias in &manifest.identity_aliases {
        let identity = (
            alias.method_id,
            alias.method_version.clone(),
            alias.artifact_locator_id,
        );
        let expected_path = format!(
            "tiangong_lca_data/lciamethods/{}.xml",
            alias.artifact_locator_id
        );
        if !required.contains(&identity)
            || !actual.insert(identity)
            || alias.status != "verified"
            || alias.evidence.repository != "tiangong-lca/data"
            || alias.evidence.commit.len() != 40
            || !alias
                .evidence
                .commit
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || alias.evidence.path != expected_path
            || validate_sha256(&alias.evidence.sha256).is_err()
            || alias.evidence.identity_field
                != "LCIAMethodDataSet.LCIAMethodInformation.dataSetInformation.common:UUID"
        {
            return Err(anyhow::anyhow!(
                "LCIA method identity alias evidence is invalid"
            ));
        }
    }
    if actual != required {
        return Err(anyhow::anyhow!(
            "LCIA method identity aliases do not exactly cover locator mismatches"
        ));
    }
    Ok(())
}

fn validate_asset_size(
    label: &str,
    actual_bytes: u64,
    max_bytes: u64,
    expected_bytes: Option<u64>,
) -> anyhow::Result<()> {
    if actual_bytes > max_bytes || expected_bytes.is_some_and(|expected| actual_bytes != expected) {
        return Err(anyhow::anyhow!(
            "LCIA static cache asset size mismatch for {label}: actual={actual_bytes} expected={expected_bytes:?} max={max_bytes}"
        ));
    }
    Ok(())
}

fn bundle_asset_path(filename: &str) -> anyhow::Result<String> {
    validate_relative_asset_path(filename)?;
    if filename.contains('/') {
        return Err(anyhow::anyhow!(
            "LCIA bundle file path must be relative to the manifest directory"
        ));
    }
    let parent = Path::new(STATIC_CACHE_BUNDLE_MANIFEST_PATH)
        .parent()
        .expect("manifest has parent");
    Ok(parent.join(filename).to_string_lossy().into_owned())
}

fn validate_relative_asset_path(path: &str) -> anyhow::Result<()> {
    let candidate = Path::new(path);
    if path.trim().is_empty()
        || candidate.is_absolute()
        || candidate
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(anyhow::anyhow!("untrusted LCIA static cache asset path"));
    }
    Ok(())
}

fn verify_hash(label: &str, bytes: &[u8], expected: &str) -> anyhow::Result<()> {
    validate_sha256(expected)?;
    if sha256_bytes(bytes) != expected {
        return Err(anyhow::anyhow!("{label} SHA-256 mismatch"));
    }
    Ok(())
}

fn validate_sha256(value: &str) -> anyhow::Result<()> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(anyhow::anyhow!("invalid SHA-256"))
    }
}

fn required_string<'a>(value: &'a Value, field: &str) -> anyhow::Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("LCIA field {field} must be a non-empty string"))
}

fn parse_uuid_field(value: &Value, field: &str) -> anyhow::Result<Uuid> {
    Ok(Uuid::parse_str(required_string(value, field)?)?)
}

fn parse_direction(value: &str) -> anyhow::Result<StaticLciaDirection> {
    match value.trim().to_ascii_uppercase().as_str() {
        "INPUT" => Ok(StaticLciaDirection::Input),
        "OUTPUT" => Ok(StaticLciaDirection::Output),
        _ => Err(anyhow::anyhow!("unsupported LCIA factor direction")),
    }
}

fn parse_finite_number(value: Option<&Value>) -> anyhow::Result<f64> {
    let number = match value {
        Some(Value::String(value)) => value.trim().replace(',', "").parse::<f64>()?,
        Some(Value::Number(value)) => value
            .as_f64()
            .ok_or_else(|| anyhow::anyhow!("LCIA factor is not representable as f64"))?,
        _ => return Err(anyhow::anyhow!("LCIA factor value is missing or invalid")),
    };
    if !number.is_finite() {
        return Err(anyhow::anyhow!("LCIA factor value must be finite"));
    }
    Ok(number)
}

fn localized_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.trim().to_owned()).filter(|value| !value.is_empty()),
        Value::Object(value) => value
            .get("#text")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        Value::Array(values) => values
            .iter()
            .find(|value| {
                value
                    .get("@xml:lang")
                    .and_then(Value::as_str)
                    .is_some_and(|language| language.eq_ignore_ascii_case("en"))
            })
            .and_then(|value| localized_text(Some(value)))
            .or_else(|| values.iter().find_map(|value| localized_text(Some(value)))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use flate2::{Compression, write::GzEncoder};
    use serde_json::json;

    use super::*;
    use crate::calculation_evidence::{
        METHOD_SOURCE_REQUEST_SCHEMA_VERSION, METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION,
    };

    struct Fixture {
        request: Value,
        manifest_bytes: Vec<u8>,
        list_bytes: Vec<u8>,
        factor_gzip_bytes: Vec<u8>,
    }

    #[allow(clippy::too_many_lines)]
    fn fixture(factor_value: &str) -> Fixture {
        let method_a = Uuid::parse_str("01500b74-7ffb-463e-9bd4-72f17c2263ff").expect("method a");
        let method_b = Uuid::parse_str("503699e0-eca9-4089-8bf8-e0f49c93e578").expect("method b");
        let locator_b = Uuid::parse_str("9ec743ea-6b00-400d-a53b-61547a3fc03c").expect("locator b");
        let flow_a = Uuid::parse_str("47921e60-2827-4b38-95f6-8152c6f03f8c").expect("flow a");
        let flow_b = Uuid::parse_str("29059f2a-6556-11dd-ad8b-0800200c9a66").expect("flow b");

        let list = json!({
            "metadata": {"totalFiles": 2},
            "files": [
                {
                    "filename": format!("{method_a}_01.00.000.json.gz"),
                    "id": method_a,
                    "version": "01.00.000",
                    "description": [{"@xml:lang": "en", "#text": "Method A"}],
                    "referenceQuantity": {
                        "common:shortDescription": {"@xml:lang": "en", "#text": "kg A"}
                    }
                },
                {
                    "filename": format!("{locator_b}_01.01.000.json.gz"),
                    "id": locator_b,
                    "version": "01.01.000",
                    "description": [{"@xml:lang": "en", "#text": "Method B"}],
                    "referenceQuantity": {
                        "common:shortDescription": {"@xml:lang": "en", "#text": "kg B"}
                    }
                }
            ]
        });
        let list_bytes = serde_json::to_vec_pretty(&list).expect("list bytes");
        let factors = json!({
            format!("{flow_a}:OUTPUT"): {
                "@refObjectId": flow_a,
                "@version": "01.00.000",
                "exchangeDirection": "OUTPUT",
                "factor": [
                    {"key": method_a, "value": factor_value},
                    {"key": method_a, "value": "2.0"}
                ]
            },
            format!("{flow_b}:INPUT"): {
                "@refObjectId": flow_b,
                "@version": "01.00.000",
                "exchangeDirection": "INPUT",
                "factor": [{"key": method_b, "value": "3.0"}]
            }
        });
        let factor_bytes = serde_json::to_vec_pretty(&factors).expect("factor bytes");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&factor_bytes).expect("compress factors");
        let factor_gzip_bytes = encoder.finish().expect("finish gzip");
        let factor_manifest_sha256 = canonical_json_sha256(&factors).expect("factor hash");
        let methods = vec![
            StaticLciaMethodManifestEntry {
                method_id: method_a,
                method_version: "01.00.000".to_owned(),
                artifact_locator_id: method_a,
                artifact_filename: format!("{method_a}_01.00.000.json.gz"),
                factor_entry_count: 2,
                unique_flow_direction_key_count: 1,
                duplicate_entry_count: 1,
            },
            StaticLciaMethodManifestEntry {
                method_id: method_b,
                method_version: "01.01.000".to_owned(),
                artifact_locator_id: locator_b,
                artifact_filename: format!("{locator_b}_01.01.000.json.gz"),
                factor_entry_count: 1,
                unique_flow_direction_key_count: 1,
                duplicate_entry_count: 0,
            },
        ];
        let method_manifest_sha256 =
            canonical_json_sha256(&serde_json::to_value(&methods).expect("methods value"))
                .expect("method hash");
        let identities = vec![
            StaticLciaMethod {
                method_id: method_a,
                method_version: "01.00.000".to_owned(),
                artifact_locator_id: method_a,
                artifact_filename: String::new(),
                name: String::new(),
                unit: String::new(),
            },
            StaticLciaMethod {
                method_id: method_b,
                method_version: "01.01.000".to_owned(),
                artifact_locator_id: locator_b,
                artifact_filename: String::new(),
                name: String::new(),
                unit: String::new(),
            },
        ];
        let method_identity_manifest_sha256 =
            method_identity_manifest_sha256(&identities).expect("identity hash");
        let files = json!({
            "list": {
                "path": "list.json",
                "media_type": "application/json",
                "byte_size": list_bytes.len(),
                "sha256": sha256_bytes(&list_bytes),
            },
            "factors": {
                "path": "flow_factors.json.gz",
                "media_type": "application/gzip",
                "byte_size": factor_gzip_bytes.len(),
                "decompressed_byte_size": factor_bytes.len(),
                "sha256": sha256_bytes(&factor_gzip_bytes),
                "decompressed_sha256": sha256_bytes(&factor_bytes),
                "canonical_sha256": factor_manifest_sha256,
            }
        });
        let source_snapshot_hash_input = json!({
            "schema_version": STATIC_CACHE_BUNDLE_SCHEMA_VERSION,
            "source_kind": "static_cache_bundle",
            "bundle_version": "test-1",
            "method_manifest_sha256": method_manifest_sha256,
            "method_identity_manifest_sha256": method_identity_manifest_sha256,
            "factor_manifest_sha256": factor_manifest_sha256,
            "files": files,
        });
        let source_snapshot_sha256 =
            canonical_json_sha256(&source_snapshot_hash_input).expect("source hash");
        let manifest = json!({
            "schema_version": STATIC_CACHE_BUNDLE_SCHEMA_VERSION,
            "source_kind": "static_cache_bundle",
            "bundle_version": "test-1",
            "source_snapshot_sha256": source_snapshot_sha256,
            "source_snapshot_hash_input": source_snapshot_hash_input,
            "method_manifest_sha256": method_manifest_sha256,
            "method_identity_manifest_sha256": method_identity_manifest_sha256,
            "factor_manifest_sha256": factor_manifest_sha256,
            "hash_algorithm": "sha256",
            "canonicalization": "sorted_object_keys_preserve_array_order.v1",
            "method_membership_status": "consistent_with_verified_aliases",
            "release_ready": true,
            "files": files,
            "identity_aliases": [{
                "method_id": method_b,
                "method_version": "01.01.000",
                "artifact_locator_id": locator_b,
                "status": "verified",
                "evidence": {
                    "repository": "tiangong-lca/data",
                    "commit": "1".repeat(40),
                    "path": format!("tiangong_lca_data/lciamethods/{locator_b}.xml"),
                    "sha256": "2".repeat(64),
                    "identity_field": "LCIAMethodDataSet.LCIAMethodInformation.dataSetInformation.common:UUID",
                }
            }],
            "methods": methods,
        });
        let manifest_bytes = serde_json::to_vec_pretty(&manifest).expect("manifest bytes");
        let request = json!({
            "schema_version": METHOD_SOURCE_REQUEST_SCHEMA_VERSION,
            "source_kind": "static_cache_bundle",
            "bundle_manifest_path": STATIC_CACHE_BUNDLE_MANIFEST_PATH,
            "bundle_manifest_sha256": sha256_bytes(&manifest_bytes),
            "bundle_manifest": manifest,
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
                ]
            }
        });
        Fixture {
            request,
            manifest_bytes,
            list_bytes,
            factor_gzip_bytes,
        }
    }

    #[test]
    fn verifies_streamed_bundle_with_exact_alias_and_cardinality() {
        let fixture = fixture("1.0");
        let verified = verify_static_lcia_bundle_inner(
            &fixture.request,
            &fixture.manifest_bytes,
            &fixture.list_bytes,
            &fixture.factor_gzip_bytes,
            false,
        )
        .expect("verified bundle");
        assert_eq!(verified.methods.len(), 2);
        assert_eq!(verified.source_evidence.method_count, 2);
        assert_eq!(
            verified
                .factors_by_method
                .values()
                .map(Vec::len)
                .sum::<usize>(),
            3
        );
    }

    #[test]
    fn rejects_nonfinite_factor_even_when_all_declared_hashes_match() {
        let fixture = fixture("NaN");
        let error = verify_static_lcia_bundle_inner(
            &fixture.request,
            &fixture.manifest_bytes,
            &fixture.list_bytes,
            &fixture.factor_gzip_bytes,
            false,
        )
        .expect_err("nonfinite factor must fail");
        assert!(error.to_string().contains("finite"));
    }

    #[test]
    fn rejects_raw_manifest_drift_before_loading_assets() {
        let fixture = fixture("1.0");
        let mut drift = fixture.manifest_bytes.clone();
        drift.push(b' ');
        let error = verify_static_lcia_bundle_inner(
            &fixture.request,
            &drift,
            &fixture.list_bytes,
            &fixture.factor_gzip_bytes,
            false,
        )
        .expect_err("raw manifest drift must fail");
        assert!(error.to_string().contains("raw SHA-256"));
    }

    #[test]
    fn rejects_missing_required_identity_alias() {
        let mut fixture = fixture("1.0");
        let manifest = fixture
            .request
            .get_mut("bundle_manifest")
            .expect("manifest");
        manifest["identity_aliases"] = json!([]);
        fixture.manifest_bytes = serde_json::to_vec_pretty(manifest).expect("manifest bytes");
        fixture.request["bundle_manifest_sha256"] = json!(sha256_bytes(&fixture.manifest_bytes));
        let error = verify_static_lcia_bundle_inner(
            &fixture.request,
            &fixture.manifest_bytes,
            &fixture.list_bytes,
            &fixture.factor_gzip_bytes,
            false,
        )
        .expect_err("missing alias must fail");
        assert!(error.to_string().contains("aliases"));
    }

    #[test]
    fn caps_streamed_decompression_at_declared_size() {
        let fixture = fixture("1.0");
        let manifest: StaticLciaBundleManifest =
            serde_json::from_slice(&fixture.manifest_bytes).expect("manifest");
        let allowed = manifest
            .methods
            .iter()
            .map(|method| (method.method_id, method))
            .collect::<BTreeMap<_, _>>();
        let declared = manifest.files.factors.decompressed_byte_size.expect("size");
        let error = parse_factor_index_streaming(
            &fixture.factor_gzip_bytes,
            &allowed,
            declared - 1,
            manifest
                .files
                .factors
                .decompressed_sha256
                .as_deref()
                .expect("hash"),
        )
        .expect_err("decompression overrun must fail");
        assert!(error.to_string().contains("declared size"));
    }

    #[test]
    fn rejects_non_loopback_plain_http_base_url() {
        assert!(
            TrustedStaticCacheSource::new(None, Some("http://example.com/assets/".to_owned()))
                .is_err()
        );
        assert!(
            TrustedStaticCacheSource::new(None, Some("http://127.0.0.1:8080/".to_owned())).is_ok()
        );
    }

    #[test]
    #[ignore = "requires LCIA_STATIC_CACHE_RELEASE_DIR pointing at Next public assets"]
    fn verifies_reviewed_release_bundle_bytes() {
        let root = PathBuf::from(
            std::env::var("LCIA_STATIC_CACHE_RELEASE_DIR").expect("LCIA_STATIC_CACHE_RELEASE_DIR"),
        );
        let manifest_bytes =
            std::fs::read(root.join(STATIC_CACHE_BUNDLE_MANIFEST_PATH)).expect("manifest");
        assert_eq!(
            sha256_bytes(&manifest_bytes),
            RELEASE_BUNDLE_MANIFEST_SHA256
        );
        let manifest: Value = serde_json::from_slice(&manifest_bytes).expect("manifest json");
        let request = json!({
            "schema_version": METHOD_SOURCE_REQUEST_SCHEMA_VERSION,
            "source_kind": "static_cache_bundle",
            "bundle_manifest_path": STATIC_CACHE_BUNDLE_MANIFEST_PATH,
            "bundle_manifest_sha256": RELEASE_BUNDLE_MANIFEST_SHA256,
            "bundle_manifest": manifest,
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
                ]
            }
        });
        validate_release_request_binding(&request).expect("release binding");
        let verified = verify_static_lcia_bundle(
            &request,
            &manifest_bytes,
            &std::fs::read(root.join("lciamethods/list.json")).expect("list"),
            &std::fs::read(root.join("lciamethods/flow_factors.json.gz")).expect("factors"),
        )
        .expect("verified reviewed release bundle");
        assert_eq!(
            u64::try_from(verified.methods.len()).expect("method count"),
            RELEASE_METHOD_COUNT
        );
        assert_eq!(
            verified.source_evidence.source_snapshot_sha256,
            RELEASE_SOURCE_SNAPSHOT_SHA256
        );
    }
}
