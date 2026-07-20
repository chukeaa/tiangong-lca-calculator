use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};

use flate2::{Compression, GzBuilder, write::GzEncoder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use solver_core::SolveResult;
use tempfile::TempDir;
use uuid::Uuid;

use crate::{
    calculation_evidence::{
        RELEASE_BUNDLE_MANIFEST_SHA256, RELEASE_BUNDLE_VERSION, RELEASE_FACTOR_MANIFEST_SHA256,
        RELEASE_METHOD_COUNT, RELEASE_METHOD_IDENTITIES, RELEASE_METHOD_IDENTITY_MANIFEST_SHA256,
        RELEASE_METHOD_MANIFEST_SHA256, RELEASE_SOURCE_SNAPSHOT_SHA256,
        canonical_json_bytes as canonical_value_json_bytes,
    },
    compiled_graph::{
        CompiledExchangeDirection, CompiledReleaseEvidence, CompiledReleaseInventoryExchange,
        CompiledReleaseSourceDataset, CompiledReleaseSourceDatasetRole,
        CompiledReleaseSourceDatasetType,
    },
    snapshot_artifacts::{SnapshotBuildConfig, SnapshotCoverageReport},
    snapshot_index::{SnapshotImpactMapEntry, SnapshotIndexDocument},
    storage::ObjectStoreClient,
};

pub const CALCULATION_BUNDLE_FORMAT: &str = "tiangong.calculation-bundle.v2";
pub const CALCULATION_BUNDLE_MANIFEST_CONTENT_TYPE: &str = "application/json";
pub const CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT: usize = 256;
const CALCULATION_BUNDLE_GZIP_CONTENT_TYPE: &str = "application/gzip";
const CALCULATION_CONTRACT_VERSION: &str = "1.0.0";
const GZIP_LEVEL: u32 = 6;

fn calculation_solver_contract(config: &SnapshotBuildConfig) -> Value {
    json!({
        "engineVersion": env!("CARGO_PKG_VERSION"),
        "numericalPolicy": {
            "equation": "M=I-A; Mx=y",
            "backend": "umfpack",
            "unitDemandAmount": 1,
        },
        "providerPolicy": { "rule": config.provider_rule },
        "allocationPolicy": {
            "semanticsVersion": config.allocation_semantics_version,
            "mode": config.allocation_fraction_mode,
        },
        "linkPolicy": {
            "semanticsVersion": config.link_semantics_version,
            "candidateEligibility": config.provider_candidate_eligibility_mode,
            "technosphereBoundary": config.technosphere_boundary_policy,
            "flowIdentity": config.flow_identity_policy,
        },
        "zeroPolicy": {
            "directionalLci": "retain_finite_nonzero",
            "lcia": "retain_finite_including_zero",
        },
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalculationBundleArtifact {
    pub kind: String,
    pub path: String,
    pub schema_version: String,
    pub media_type: String,
    pub compression: String,
    pub sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uncompressed_sha256: Option<String>,
    pub byte_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uncompressed_byte_size: Option<u64>,
    pub record_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_process_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_process_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub derived: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalculationBundleManifest {
    pub schema_version: String,
    pub calculation_contract_version: String,
    pub calculation_id: Uuid,
    pub bundle_content_hash: String,
    pub scope: CalculationBundleScope,
    pub snapshot: CalculationBundleSnapshot,
    pub solver: Value,
    pub method_set: Value,
    pub artifacts: Vec<CalculationBundleArtifact>,
    pub calculation_evidence: Value,
    pub hashes: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalculationBundleScope {
    pub coverage_mode: String,
    pub process_count: usize,
    pub selection_manifest_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalculationBundleSnapshot {
    pub id: Uuid,
    pub sha256: String,
    pub process_count: usize,
    pub flow_count: usize,
    pub impact_count: usize,
}

#[derive(Debug)]
pub struct LocalCalculationBundleArtifact {
    pub metadata: CalculationBundleArtifact,
    pub local_path: PathBuf,
}

#[derive(Debug)]
pub struct BuiltCalculationBundle {
    _directory: TempDir,
    pub calculation_id: Uuid,
    pub bundle_content_hash: String,
    pub manifest_sha256: String,
    pub manifest_byte_size: u64,
    pub manifest_path: PathBuf,
    pub manifest: CalculationBundleManifest,
    pub artifacts: Vec<LocalCalculationBundleArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalculationBundleArtifactRef {
    pub schema_version: String,
    pub calculation_id: Uuid,
    pub bundle_content_hash: String,
    pub manifest_url: String,
    pub manifest_sha256: String,
    pub manifest_byte_size: u64,
    pub artifact_count: usize,
}

pub async fn upload_built_calculation_bundle(
    store: &ObjectStoreClient,
    bundle: &BuiltCalculationBundle,
) -> anyhow::Result<CalculationBundleArtifactRef> {
    let relative_prefix = format!(
        "calculation-bundles/{}/{}",
        bundle.calculation_id, bundle.bundle_content_hash
    );
    for artifact in &bundle.artifacts {
        let relative_key = format!("{relative_prefix}/{}", artifact.metadata.path);
        let key = store.prefixed_object_key(&relative_key)?;
        let storage_content_type = calculation_bundle_storage_content_type(&artifact.metadata)?;
        store
            .upload_object_key_file(
                &key,
                storage_content_type,
                &artifact.local_path,
                artifact.metadata.byte_size,
            )
            .await?;
    }

    let manifest_relative_key = format!("{relative_prefix}/calculation-bundle.json");
    let manifest_key = store.prefixed_object_key(&manifest_relative_key)?;
    let uploaded = store
        .upload_object_key_file(
            &manifest_key,
            CALCULATION_BUNDLE_MANIFEST_CONTENT_TYPE,
            &bundle.manifest_path,
            bundle.manifest_byte_size,
        )
        .await?;
    Ok(CalculationBundleArtifactRef {
        schema_version: CALCULATION_BUNDLE_FORMAT.to_owned(),
        calculation_id: bundle.calculation_id,
        bundle_content_hash: bundle.bundle_content_hash.clone(),
        manifest_url: uploaded.object_url,
        manifest_sha256: bundle.manifest_sha256.clone(),
        manifest_byte_size: bundle.manifest_byte_size,
        artifact_count: bundle.artifacts.len(),
    })
}

fn calculation_bundle_storage_content_type(
    artifact: &CalculationBundleArtifact,
) -> anyhow::Result<&str> {
    match artifact.compression.as_str() {
        "gzip" => Ok(CALCULATION_BUNDLE_GZIP_CONTENT_TYPE),
        "none" => Ok(artifact.media_type.as_str()),
        compression => Err(anyhow::anyhow!(
            "unsupported Calculation Bundle artifact compression: {compression}"
        )),
    }
}

#[derive(Debug)]
pub struct CalculationBundleWriter {
    directory: TempDir,
    calculation_id: Uuid,
    snapshot_id: Uuid,
    snapshot_sha256: String,
    snapshot_flow_count: usize,
    config: SnapshotBuildConfig,
    coverage: SnapshotCoverageReport,
    calculation_evidence: Value,
    processes: Vec<ReleaseProcessRecord>,
    impacts: Vec<ReleaseImpact>,
    biosphere_by_process: Vec<Vec<CompiledReleaseInventoryExchange>>,
    artifacts: Vec<LocalCalculationBundleArtifact>,
    completed_result_chunks: BTreeSet<usize>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseProcessRecord {
    process_index: usize,
    root_process: GlobalReference,
    quantitative_reference: QuantitativeReference,
}

#[derive(Debug, Clone, Serialize)]
struct GlobalReference {
    id: Uuid,
    version: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct QuantitativeReference {
    exchange_internal_id: String,
    flow: GlobalReference,
    direction: &'static str,
    reference_unit: String,
    mean_amount: f64,
}

#[derive(Debug, Clone)]
struct ReleaseImpact {
    index: usize,
    id: Uuid,
    version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct InventoryKey {
    flow_id: Uuid,
    flow_version: String,
    direction: CompiledExchangeDirection,
    unit: String,
    location: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct InventoryRecord<'a> {
    process_index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    exchange_internal_id: Option<&'a str>,
    flow: GlobalReference,
    direction: CompiledExchangeDirection,
    unit: &'a str,
    location: Option<&'a str>,
    mean_amount: f64,
    allocation_target_internal_id: &'a str,
    allocation_fraction: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OwnedInventoryRecord {
    process_index: usize,
    flow: GlobalReference,
    direction: CompiledExchangeDirection,
    unit: String,
    location: Option<String>,
    mean_amount: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LciaRecord {
    process_index: usize,
    method: GlobalReference,
    mean_amount: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TechnosphereRecord<'a> {
    dependent_process_index: usize,
    residual_exchange_internal_id: &'a str,
    balancing_process_index: usize,
    balancing_reference_exchange_internal_id: &'a str,
    residual_coefficient: f64,
    reference_coefficient: f64,
    routing_weight: f64,
    activity_requirement: f64,
    flow: GlobalReference,
    location: Option<&'a str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceClosureRecord<'a> {
    schema_version: &'static str,
    dataset_type: &'static str,
    role: &'static str,
    uuid: Uuid,
    version: &'a str,
    path: String,
    sha256: &'a str,
    document: &'a Value,
}

struct DeterministicGzipNdjsonWriter {
    encoder: GzEncoder<File>,
    plain_hasher: Sha256,
    plain_byte_size: u64,
    record_count: u64,
    path: PathBuf,
}

impl DeterministicGzipNdjsonWriter {
    fn create(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(path)?;
        let encoder = GzBuilder::new()
            .mtime(0)
            .write(file, Compression::new(GZIP_LEVEL));
        Ok(Self {
            encoder,
            plain_hasher: Sha256::new(),
            plain_byte_size: 0,
            record_count: 0,
            path: path.to_owned(),
        })
    }

    fn write<T: Serialize>(&mut self, value: &T) -> anyhow::Result<()> {
        let bytes = canonical_json_bytes(value)?;
        self.encoder.write_all(bytes.as_slice())?;
        self.encoder.write_all(b"\n")?;
        self.plain_hasher.update(bytes.as_slice());
        self.plain_hasher.update(b"\n");
        self.plain_byte_size = self
            .plain_byte_size
            .checked_add(u64::try_from(bytes.len() + 1)?)
            .ok_or_else(|| anyhow::anyhow!("Calculation Bundle uncompressed byte size overflow"))?;
        self.record_count = self
            .record_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Calculation Bundle record count overflow"))?;
        Ok(())
    }

    fn finish(self) -> anyhow::Result<FinishedNdjson> {
        let Self {
            encoder,
            plain_hasher,
            plain_byte_size,
            record_count,
            path,
        } = self;
        let file = encoder.finish()?;
        file.sync_all()?;
        let byte_size = file.metadata()?.len();
        Ok(FinishedNdjson {
            sha256: sha256_file(&path)?,
            path,
            uncompressed_sha256: hex::encode(plain_hasher.finalize()),
            byte_size,
            uncompressed_byte_size: plain_byte_size,
            record_count,
        })
    }
}

struct FinishedNdjson {
    path: PathBuf,
    sha256: String,
    uncompressed_sha256: String,
    byte_size: u64,
    uncompressed_byte_size: u64,
    record_count: u64,
}

impl CalculationBundleWriter {
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        calculation_id: Uuid,
        snapshot_id: Uuid,
        snapshot_sha256: String,
        snapshot_flow_count: usize,
        config: SnapshotBuildConfig,
        coverage: SnapshotCoverageReport,
        snapshot_index: &SnapshotIndexDocument,
        release_evidence: &CompiledReleaseEvidence,
    ) -> anyhow::Result<Self> {
        validate_sha256(&snapshot_sha256, "snapshot.sha256")?;
        let process_count = usize::try_from(snapshot_index.process_count)
            .map_err(|_| anyhow::anyhow!("negative snapshot process count"))?;
        if process_count == 0 || release_evidence.processes.len() != process_count {
            return Err(anyhow::anyhow!(
                "Calculation Bundle process evidence mismatch: snapshot={process_count} evidence={}",
                release_evidence.processes.len()
            ));
        }
        validate_source_datasets(release_evidence)?;

        let mut processes = release_evidence
            .processes
            .iter()
            .map(|process| {
                let process_index = usize::try_from(process.process_idx)
                    .map_err(|_| anyhow::anyhow!("negative release process index"))?;
                validate_version(&process.process_version, "process.version")?;
                validate_version(
                    &process.quantitative_reference_flow_version,
                    "quantitativeReference.flow.version",
                )?;
                require_nonempty(
                    &process.quantitative_reference_exchange_internal_id,
                    "quantitativeReference.exchangeInternalId",
                )?;
                require_nonempty(
                    &process.reference_unit,
                    "quantitativeReference.referenceUnit",
                )?;
                ensure_finite_nonzero(
                    process.normalized_mean_amount,
                    "quantitativeReference.meanAmount",
                )?;
                Ok(ReleaseProcessRecord {
                    process_index,
                    root_process: GlobalReference {
                        id: process.process_id,
                        version: process.process_version.clone(),
                    },
                    quantitative_reference: QuantitativeReference {
                        exchange_internal_id: process
                            .quantitative_reference_exchange_internal_id
                            .clone(),
                        flow: GlobalReference {
                            id: process.quantitative_reference_flow_id,
                            version: process.quantitative_reference_flow_version.clone(),
                        },
                        direction: "Output",
                        reference_unit: process.reference_unit.clone(),
                        mean_amount: process.normalized_mean_amount,
                    },
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        processes.sort_unstable_by_key(|process| process.process_index);
        for (expected, process) in processes.iter().enumerate() {
            if process.process_index != expected {
                return Err(anyhow::anyhow!(
                    "Calculation Bundle process index gap: expected={expected} got={}",
                    process.process_index
                ));
            }
        }

        let impacts = validate_impacts(&snapshot_index.impact_map)?;
        let calculation_evidence = snapshot_index
            .calculation_evidence
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?
            .unwrap_or_else(|| {
                json!({
                    "schemaVersion": "lca.calculation-evidence.legacy-snapshot.v1",
                    "snapshotCoverage": coverage,
                })
            });

        let mut biosphere_by_process = vec![Vec::new(); process_count];
        for exchange in &release_evidence.biosphere_edges {
            validate_inventory_exchange(exchange, process_count)?;
            let process_index = usize::try_from(exchange.process_idx)?;
            biosphere_by_process[process_index].push(exchange.clone());
        }
        for exchanges in &mut biosphere_by_process {
            exchanges.sort_by(inventory_exchange_order);
        }

        let mut writer = Self {
            directory: tempfile::Builder::new()
                .prefix("tiangong-calculation-bundle-")
                .tempdir()?,
            calculation_id,
            snapshot_id,
            snapshot_sha256,
            snapshot_flow_count,
            config,
            coverage,
            calculation_evidence,
            processes,
            impacts,
            biosphere_by_process,
            artifacts: Vec::new(),
            completed_result_chunks: BTreeSet::new(),
        };
        writer.write_static_artifacts(release_evidence)?;
        Ok(writer)
    }

    #[allow(clippy::too_many_lines)]
    pub fn write_result_chunk(
        &mut self,
        first_process_index: usize,
        items: &[SolveResult],
    ) -> anyhow::Result<()> {
        if !first_process_index.is_multiple_of(CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT) {
            return Err(anyhow::anyhow!(
                "Calculation Bundle result chunk must start on a 256-process boundary"
            ));
        }
        if items.is_empty() || items.len() > CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT {
            return Err(anyhow::anyhow!(
                "Calculation Bundle result chunk size is invalid"
            ));
        }
        let end = first_process_index
            .checked_add(items.len())
            .ok_or_else(|| anyhow::anyhow!("Calculation Bundle result chunk index overflow"))?;
        if end > self.processes.len() {
            return Err(anyhow::anyhow!(
                "Calculation Bundle result chunk exceeds process axis"
            ));
        }
        if !self.completed_result_chunks.insert(first_process_index) {
            return Err(anyhow::anyhow!(
                "Calculation Bundle result chunk already written"
            ));
        }

        let chunk_number = first_process_index / CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT;
        let last_process_index = end - 1;
        let lci_path = format!("results/lci-{chunk_number:06}.ndjson.gz");
        let lcia_path = format!("results/lcia-{chunk_number:06}.ndjson.gz");
        let mut lci_writer =
            DeterministicGzipNdjsonWriter::create(&self.directory.path().join(lci_path.as_str()))?;
        let mut lcia_writer =
            DeterministicGzipNdjsonWriter::create(&self.directory.path().join(lcia_path.as_str()))?;

        for (offset, item) in items.iter().enumerate() {
            let process_index = first_process_index + offset;
            let x = item.x.as_ref().ok_or_else(|| {
                anyhow::anyhow!("Calculation Bundle solve item[{process_index}] is missing x")
            })?;
            if x.len() != self.processes.len() {
                return Err(anyhow::anyhow!(
                    "Calculation Bundle x axis mismatch for process {process_index}: expected={} got={}",
                    self.processes.len(),
                    x.len()
                ));
            }
            let h = item.h.as_ref().ok_or_else(|| {
                anyhow::anyhow!("Calculation Bundle solve item[{process_index}] is missing h")
            })?;
            if h.len() != self.impacts.len() {
                return Err(anyhow::anyhow!(
                    "Calculation Bundle h axis mismatch for process {process_index}: expected={} got={}",
                    self.impacts.len(),
                    h.len()
                ));
            }

            let mut inventory = BTreeMap::<InventoryKey, f64>::new();
            for (source_process_index, scale) in x.iter().copied().enumerate() {
                ensure_finite(scale, "x")?;
                if scale == 0.0 {
                    continue;
                }
                for exchange in &self.biosphere_by_process[source_process_index] {
                    let contribution = exchange.normalized_mean_amount * scale;
                    ensure_finite(contribution, "directional LCI contribution")?;
                    let key = InventoryKey {
                        flow_id: exchange.flow_id,
                        flow_version: exchange.flow_version.clone(),
                        direction: exchange.direction,
                        unit: exchange.unit.clone(),
                        location: exchange.location.clone(),
                    };
                    let value = inventory.entry(key).or_insert(0.0);
                    *value += contribution;
                    ensure_finite(*value, "directional LCI aggregate")?;
                }
            }
            for (key, mean_amount) in inventory {
                if mean_amount == 0.0 {
                    continue;
                }
                lci_writer.write(&OwnedInventoryRecord {
                    process_index,
                    flow: GlobalReference {
                        id: key.flow_id,
                        version: key.flow_version,
                    },
                    direction: key.direction,
                    unit: key.unit,
                    location: key.location,
                    mean_amount,
                })?;
            }

            for impact in &self.impacts {
                let mean_amount = h[impact.index];
                ensure_finite(mean_amount, "LCIA result")?;
                lcia_writer.write(&LciaRecord {
                    process_index,
                    method: GlobalReference {
                        id: impact.id,
                        version: impact.version.clone(),
                    },
                    mean_amount,
                })?;
            }
        }

        self.push_finished_ndjson(
            "lci",
            "tiangong.calculation-bundle.lci.v1",
            lci_path,
            first_process_index,
            last_process_index,
            lci_writer.finish()?,
        );
        self.push_finished_ndjson(
            "lcia",
            "tiangong.calculation-bundle.lcia.v1",
            lcia_path,
            first_process_index,
            last_process_index,
            lcia_writer.finish()?,
        );
        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<BuiltCalculationBundle> {
        let expected_chunk_starts = (0..self.processes.len())
            .step_by(CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT)
            .collect::<BTreeSet<_>>();
        if self.completed_result_chunks != expected_chunk_starts {
            return Err(anyhow::anyhow!(
                "Calculation Bundle result chunks incomplete: expected={expected_chunk_starts:?} got={:?}",
                self.completed_result_chunks
            ));
        }

        self.write_coverage_artifact()?;
        self.artifacts
            .sort_by(|left, right| left.metadata.path.cmp(&right.metadata.path));
        let selection_manifest_hash = canonical_sha256(&json!({
            "schemaVersion": "tiangong.calculation-bundle.selection.v1",
            "processes": self.processes,
        }))?;
        let coverage_mode = if self.config.request_roots.is_empty() {
            "global_eligible"
        } else {
            "subset"
        };
        let process_count = self.processes.len();
        let impact_count = self.impacts.len();
        let mut manifest = CalculationBundleManifest {
            schema_version: CALCULATION_BUNDLE_FORMAT.to_owned(),
            calculation_contract_version: CALCULATION_CONTRACT_VERSION.to_owned(),
            calculation_id: self.calculation_id,
            bundle_content_hash: "0".repeat(64),
            scope: CalculationBundleScope {
                coverage_mode: coverage_mode.to_owned(),
                process_count,
                selection_manifest_hash,
            },
            snapshot: CalculationBundleSnapshot {
                id: self.snapshot_id,
                sha256: self.snapshot_sha256,
                process_count,
                flow_count: self.snapshot_flow_count,
                impact_count,
            },
            solver: calculation_solver_contract(&self.config),
            method_set: json!({
                "schemaVersion": "lcia.static_cache_bundle.v1",
                "bundleVersion": RELEASE_BUNDLE_VERSION,
                "methodCount": RELEASE_METHOD_COUNT,
                "rawManifestSha256": RELEASE_BUNDLE_MANIFEST_SHA256,
                "sourceSnapshotSha256": RELEASE_SOURCE_SNAPSHOT_SHA256,
                "methodManifestSha256": RELEASE_METHOD_MANIFEST_SHA256,
                "methodIdentityManifestSha256": RELEASE_METHOD_IDENTITY_MANIFEST_SHA256,
                "factorManifestSha256": RELEASE_FACTOR_MANIFEST_SHA256,
            }),
            artifacts: self
                .artifacts
                .iter()
                .map(|artifact| artifact.metadata.clone())
                .collect(),
            calculation_evidence: self.calculation_evidence,
            hashes: json!({
                "algorithm": "sha256",
                "canonicalJson": "RFC8785/JCS",
                "gzip": { "level": GZIP_LEVEL, "mtime": 0 },
            }),
        };
        manifest.bundle_content_hash = bundle_content_hash(&manifest)?;
        let manifest_bytes = canonical_json_bytes(&manifest)?;
        let manifest_sha256 = sha256_bytes(manifest_bytes.as_slice());
        let manifest_byte_size = u64::try_from(manifest_bytes.len())?;
        let manifest_path = self.directory.path().join("calculation-bundle.json");
        std::fs::write(&manifest_path, manifest_bytes)?;

        Ok(BuiltCalculationBundle {
            _directory: self.directory,
            calculation_id: self.calculation_id,
            bundle_content_hash: manifest.bundle_content_hash.clone(),
            manifest_sha256,
            manifest_byte_size,
            manifest_path,
            manifest,
            artifacts: self.artifacts,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn write_static_artifacts(
        &mut self,
        release_evidence: &CompiledReleaseEvidence,
    ) -> anyhow::Result<()> {
        self.write_source_closure_artifact(&release_evidence.source_datasets)?;
        for first_process_index in
            (0..self.processes.len()).step_by(CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT)
        {
            let end = (first_process_index + CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT)
                .min(self.processes.len());
            let last_process_index = end - 1;
            let chunk_number = first_process_index / CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT;

            let process_path = format!("axes/processes-{chunk_number:06}.ndjson.gz");
            let mut process_writer = DeterministicGzipNdjsonWriter::create(
                &self.directory.path().join(process_path.as_str()),
            )?;
            for process in &self.processes[first_process_index..end] {
                process_writer.write(process)?;
            }
            self.push_finished_ndjson(
                "process_axis",
                "tiangong.calculation-bundle.process-axis.v1",
                process_path,
                first_process_index,
                last_process_index,
                process_writer.finish()?,
            );

            let inventory_path = format!("axes/inventory-{chunk_number:06}.ndjson.gz");
            let mut inventory_writer = DeterministicGzipNdjsonWriter::create(
                &self.directory.path().join(inventory_path.as_str()),
            )?;
            let mut inventory = release_evidence
                .inventory_exchanges
                .iter()
                .filter(|exchange| {
                    usize::try_from(exchange.process_idx)
                        .is_ok_and(|index| index >= first_process_index && index < end)
                })
                .collect::<Vec<_>>();
            inventory.sort_by(|left, right| inventory_exchange_order(left, right));
            for exchange in inventory {
                validate_inventory_exchange(exchange, self.processes.len())?;
                inventory_writer.write(&inventory_record(exchange)?)?;
            }
            self.push_finished_ndjson(
                "inventory_axis",
                "tiangong.calculation-bundle.inventory-axis.v1",
                inventory_path,
                first_process_index,
                last_process_index,
                inventory_writer.finish()?,
            );

            let biosphere_path = format!("graph/biosphere-{chunk_number:06}.ndjson.gz");
            let mut biosphere_writer = DeterministicGzipNdjsonWriter::create(
                &self.directory.path().join(biosphere_path.as_str()),
            )?;
            let mut biosphere = release_evidence
                .biosphere_edges
                .iter()
                .filter(|exchange| {
                    usize::try_from(exchange.process_idx)
                        .is_ok_and(|index| index >= first_process_index && index < end)
                })
                .collect::<Vec<_>>();
            biosphere.sort_by(|left, right| inventory_exchange_order(left, right));
            for exchange in biosphere {
                validate_inventory_exchange(exchange, self.processes.len())?;
                biosphere_writer.write(&inventory_record(exchange)?)?;
            }
            self.push_finished_ndjson(
                "biosphere_edges",
                "tiangong.calculation-bundle.biosphere-edges.v1",
                biosphere_path,
                first_process_index,
                last_process_index,
                biosphere_writer.finish()?,
            );

            let technosphere_path = format!("graph/technosphere-{chunk_number:06}.ndjson.gz");
            let mut technosphere_writer = DeterministicGzipNdjsonWriter::create(
                &self.directory.path().join(technosphere_path.as_str()),
            )?;
            let mut technosphere = release_evidence
                .technosphere_edges
                .iter()
                .filter(|edge| {
                    usize::try_from(edge.dependent_process_idx)
                        .is_ok_and(|index| index >= first_process_index && index < end)
                })
                .collect::<Vec<_>>();
            technosphere.sort_by(|left, right| {
                left.dependent_process_idx
                    .cmp(&right.dependent_process_idx)
                    .then_with(|| {
                        left.residual_exchange_internal_id
                            .cmp(&right.residual_exchange_internal_id)
                    })
                    .then_with(|| left.balancing_process_idx.cmp(&right.balancing_process_idx))
            });
            for edge in technosphere {
                validate_version(&edge.flow_version, "technosphere.flow.version")?;
                require_nonempty(
                    &edge.residual_exchange_internal_id,
                    "technosphere.residualExchangeInternalId",
                )?;
                require_nonempty(
                    &edge.balancing_reference_exchange_internal_id,
                    "technosphere.balancingReferenceExchangeInternalId",
                )?;
                ensure_finite(
                    edge.residual_coefficient,
                    "technosphere.residualCoefficient",
                )?;
                ensure_finite(
                    edge.reference_coefficient,
                    "technosphere.referenceCoefficient",
                )?;
                ensure_finite(edge.routing_weight, "technosphere.routingWeight")?;
                ensure_finite(
                    edge.activity_requirement,
                    "technosphere.activityRequirement",
                )?;
                let dependent_process_index = usize::try_from(edge.dependent_process_idx)?;
                let balancing_process_index = usize::try_from(edge.balancing_process_idx)?;
                if dependent_process_index >= self.processes.len()
                    || balancing_process_index >= self.processes.len()
                {
                    return Err(anyhow::anyhow!(
                        "technosphere edge process index is outside process axis"
                    ));
                }
                technosphere_writer.write(&TechnosphereRecord {
                    dependent_process_index,
                    residual_exchange_internal_id: &edge.residual_exchange_internal_id,
                    balancing_process_index,
                    balancing_reference_exchange_internal_id: &edge
                        .balancing_reference_exchange_internal_id,
                    residual_coefficient: edge.residual_coefficient,
                    reference_coefficient: edge.reference_coefficient,
                    routing_weight: edge.routing_weight,
                    activity_requirement: edge.activity_requirement,
                    flow: GlobalReference {
                        id: edge.flow_id,
                        version: edge.flow_version.clone(),
                    },
                    location: edge.location.as_deref(),
                })?;
            }
            self.push_finished_ndjson(
                "technosphere_edges",
                "tiangong.calculation-bundle.technosphere-edges.v2",
                technosphere_path,
                first_process_index,
                last_process_index,
                technosphere_writer.finish()?,
            );
        }
        Ok(())
    }

    fn write_source_closure_artifact(
        &mut self,
        source_datasets: &[CompiledReleaseSourceDataset],
    ) -> anyhow::Result<()> {
        let relative_path = "source/source-closure.ndjson.gz";
        let mut writer =
            DeterministicGzipNdjsonWriter::create(&self.directory.path().join(relative_path))?;
        let mut datasets = source_datasets.iter().collect::<Vec<_>>();
        datasets.sort_by(|left, right| source_dataset_order(left, right));
        for dataset in datasets {
            validate_version(&dataset.dataset_version, "sourceClosure.dataset.version")?;
            validate_sha256(&dataset.document_sha256, "sourceClosure.dataset.sha256")?;
            let canonical_document = canonical_value_json_bytes(&dataset.document)?;
            if sha256_bytes(canonical_document.as_slice()) != dataset.document_sha256 {
                return Err(anyhow::anyhow!(
                    "Calculation Bundle source closure document hash drift for {}:{}@{}",
                    dataset.dataset_type.as_str(),
                    dataset.dataset_id,
                    dataset.dataset_version
                ));
            }
            writer.write(&SourceClosureRecord {
                schema_version: "tiangong.source-closure.dataset.v1",
                dataset_type: dataset.dataset_type.as_str(),
                role: dataset.role.as_str(),
                uuid: dataset.dataset_id,
                version: &dataset.dataset_version,
                path: source_dataset_path(dataset),
                sha256: &dataset.document_sha256,
                document: &dataset.document,
            })?;
        }
        let finished = writer.finish()?;
        self.artifacts.push(LocalCalculationBundleArtifact {
            metadata: CalculationBundleArtifact {
                kind: "source_closure".to_owned(),
                path: relative_path.to_owned(),
                schema_version: "tiangong.source-closure.bundle.v1".to_owned(),
                media_type: "application/x-ndjson".to_owned(),
                compression: "gzip".to_owned(),
                sha256: finished.sha256,
                uncompressed_sha256: Some(finished.uncompressed_sha256),
                byte_size: finished.byte_size,
                uncompressed_byte_size: Some(finished.uncompressed_byte_size),
                record_count: finished.record_count,
                first_process_index: None,
                last_process_index: None,
                derived: Some(false),
            },
            local_path: finished.path,
        });
        Ok(())
    }

    fn write_coverage_artifact(&mut self) -> anyhow::Result<()> {
        let relative_path = "evidence/coverage.json";
        let local_path = self.directory.path().join(relative_path);
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let complete = self.coverage.matching.unmatched_no_provider == 0
            && self.coverage.reference.missing_reference_count == 0
            && self.coverage.reference.invalid_reference_count == 0;
        let body = canonical_json_bytes(&json!({
            "schemaVersion": "tiangong.calculation-bundle.coverage.v1",
            "complete": complete,
            "processCount": self.processes.len(),
            "snapshotCoverage": self.coverage,
            "calculationEvidence": self.calculation_evidence,
        }))?;
        std::fs::write(&local_path, &body)?;
        self.artifacts.push(LocalCalculationBundleArtifact {
            metadata: CalculationBundleArtifact {
                kind: "coverage".to_owned(),
                path: relative_path.to_owned(),
                schema_version: "tiangong.calculation-bundle.coverage.v1".to_owned(),
                media_type: "application/json".to_owned(),
                compression: "none".to_owned(),
                sha256: sha256_bytes(body.as_slice()),
                uncompressed_sha256: None,
                byte_size: u64::try_from(body.len())?,
                uncompressed_byte_size: None,
                record_count: 1,
                first_process_index: None,
                last_process_index: None,
                derived: None,
            },
            local_path,
        });
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn push_finished_ndjson(
        &mut self,
        kind: &str,
        schema_version: &str,
        relative_path: String,
        first_process_index: usize,
        last_process_index: usize,
        finished: FinishedNdjson,
    ) {
        self.artifacts.push(LocalCalculationBundleArtifact {
            metadata: CalculationBundleArtifact {
                kind: kind.to_owned(),
                path: relative_path,
                schema_version: schema_version.to_owned(),
                media_type: "application/x-ndjson".to_owned(),
                compression: "gzip".to_owned(),
                sha256: finished.sha256,
                uncompressed_sha256: Some(finished.uncompressed_sha256),
                byte_size: finished.byte_size,
                uncompressed_byte_size: Some(finished.uncompressed_byte_size),
                record_count: finished.record_count,
                first_process_index: Some(first_process_index),
                last_process_index: Some(last_process_index),
                derived: None,
            },
            local_path: finished.path,
        });
    }
}

fn validate_impacts(items: &[SnapshotImpactMapEntry]) -> anyhow::Result<Vec<ReleaseImpact>> {
    if items.len() != usize::try_from(RELEASE_METHOD_COUNT)? {
        return Err(anyhow::anyhow!(
            "Calculation Bundle requires the reviewed {RELEASE_METHOD_COUNT}-method set; got {}",
            items.len()
        ));
    }
    let mut impacts = items
        .iter()
        .map(|impact| {
            let index = usize::try_from(impact.impact_index)
                .map_err(|_| anyhow::anyhow!("negative impact index"))?;
            let version = impact
                .impact_version
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("Calculation Bundle impact version is missing"))?;
            validate_version(version, "impact.version")?;
            Ok(ReleaseImpact {
                index,
                id: impact.impact_id,
                version: version.to_owned(),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    impacts.sort_unstable_by_key(|impact| impact.index);
    for (expected, impact) in impacts.iter().enumerate() {
        if impact.index != expected {
            return Err(anyhow::anyhow!(
                "Calculation Bundle impact index gap: expected={expected} got={}",
                impact.index
            ));
        }
    }
    let actual = impacts
        .iter()
        .map(|impact| (impact.id.to_string(), impact.version.clone()))
        .collect::<BTreeSet<_>>();
    let expected = RELEASE_METHOD_IDENTITIES
        .iter()
        .map(|(method_id, version, _)| ((*method_id).to_owned(), (*version).to_owned()))
        .collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(anyhow::anyhow!(
            "Calculation Bundle impact identities do not match the reviewed method set"
        ));
    }
    Ok(impacts)
}

fn validate_inventory_exchange(
    exchange: &CompiledReleaseInventoryExchange,
    process_count: usize,
) -> anyhow::Result<()> {
    let process_index = usize::try_from(exchange.process_idx)
        .map_err(|_| anyhow::anyhow!("negative inventory process index"))?;
    if process_index >= process_count {
        return Err(anyhow::anyhow!(
            "inventory process index is outside process axis"
        ));
    }
    validate_version(&exchange.flow_version, "inventory.flow.version")?;
    require_nonempty(&exchange.unit, "inventory.unit")?;
    require_nonempty(
        &exchange.allocation_target_internal_id,
        "inventory.allocationTargetInternalId",
    )?;
    ensure_finite(exchange.allocation_fraction, "inventory.allocationFraction")?;
    ensure_finite(exchange.normalized_mean_amount, "inventory.meanAmount")
}

fn inventory_record(
    exchange: &CompiledReleaseInventoryExchange,
) -> anyhow::Result<InventoryRecord<'_>> {
    Ok(InventoryRecord {
        process_index: usize::try_from(exchange.process_idx)?,
        exchange_internal_id: exchange.exchange_internal_id.as_deref(),
        flow: GlobalReference {
            id: exchange.flow_id,
            version: exchange.flow_version.clone(),
        },
        direction: exchange.direction,
        unit: exchange.unit.as_str(),
        location: exchange.location.as_deref(),
        mean_amount: exchange.normalized_mean_amount,
        allocation_target_internal_id: exchange.allocation_target_internal_id.as_str(),
        allocation_fraction: exchange.allocation_fraction,
    })
}

fn inventory_exchange_order(
    left: &CompiledReleaseInventoryExchange,
    right: &CompiledReleaseInventoryExchange,
) -> std::cmp::Ordering {
    left.process_idx
        .cmp(&right.process_idx)
        .then_with(|| left.direction.cmp(&right.direction))
        .then_with(|| left.flow_id.cmp(&right.flow_id))
        .then_with(|| left.flow_version.cmp(&right.flow_version))
        .then_with(|| left.unit.cmp(&right.unit))
        .then_with(|| left.location.cmp(&right.location))
        .then_with(|| left.exchange_internal_id.cmp(&right.exchange_internal_id))
}

fn source_dataset_order(
    left: &CompiledReleaseSourceDataset,
    right: &CompiledReleaseSourceDataset,
) -> std::cmp::Ordering {
    left.dataset_type
        .cmp(&right.dataset_type)
        .then_with(|| left.dataset_id.cmp(&right.dataset_id))
        .then_with(|| left.dataset_version.cmp(&right.dataset_version))
}

fn source_dataset_path(dataset: &CompiledReleaseSourceDataset) -> String {
    format!(
        "{}/{}_{}.json",
        dataset.dataset_type.directory(),
        dataset.dataset_id,
        dataset.dataset_version
    )
}

fn validate_source_datasets(release_evidence: &CompiledReleaseEvidence) -> anyhow::Result<()> {
    if release_evidence.source_datasets.is_empty() {
        return Err(anyhow::anyhow!(
            "snapshot lacks frozen source closure evidence; rebuild the snapshot"
        ));
    }
    let expected_processes = release_evidence
        .processes
        .iter()
        .map(|process| (process.process_id, process.process_version.clone()))
        .collect::<BTreeSet<_>>();
    let mut observed_processes = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for dataset in &release_evidence.source_datasets {
        validate_version(&dataset.dataset_version, "sourceClosure.dataset.version")?;
        validate_sha256(&dataset.document_sha256, "sourceClosure.dataset.sha256")?;
        if !dataset.document.is_object() {
            return Err(anyhow::anyhow!(
                "source closure document must be an object for {}:{}@{}",
                dataset.dataset_type.as_str(),
                dataset.dataset_id,
                dataset.dataset_version
            ));
        }
        let document_id = dataset
            .dataset_type
            .document_uuid(&dataset.document)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "source closure document is missing canonical common:UUID for {}:{}@{}",
                    dataset.dataset_type.as_str(),
                    dataset.dataset_id,
                    dataset.dataset_version
                )
            })?;
        let document_id = Uuid::parse_str(document_id).map_err(|_| {
            anyhow::anyhow!(
                "source closure document has invalid common:UUID for {}:{}@{}",
                dataset.dataset_type.as_str(),
                dataset.dataset_id,
                dataset.dataset_version
            )
        })?;
        if document_id != dataset.dataset_id {
            return Err(anyhow::anyhow!(
                "source closure document identity mismatch for {}:{}@{}: document={document_id}",
                dataset.dataset_type.as_str(),
                dataset.dataset_id,
                dataset.dataset_version
            ));
        }
        if !keys.insert((
            dataset.dataset_type,
            dataset.dataset_id,
            dataset.dataset_version.clone(),
        )) {
            return Err(anyhow::anyhow!(
                "duplicate source closure dataset {}:{}@{}",
                dataset.dataset_type.as_str(),
                dataset.dataset_id,
                dataset.dataset_version
            ));
        }
        match (dataset.dataset_type, dataset.role) {
            (
                CompiledReleaseSourceDatasetType::Process,
                CompiledReleaseSourceDatasetRole::UnitProcess,
            ) => {
                observed_processes.insert((dataset.dataset_id, dataset.dataset_version.clone()));
            }
            (CompiledReleaseSourceDatasetType::Process, _) => {
                return Err(anyhow::anyhow!(
                    "source closure Process must have unit_process role"
                ));
            }
            (_, CompiledReleaseSourceDatasetRole::Support) => {}
            (_, CompiledReleaseSourceDatasetRole::UnitProcess) => {
                return Err(anyhow::anyhow!(
                    "only source closure Process documents may have unit_process role"
                ));
            }
        }
    }
    if observed_processes != expected_processes {
        return Err(anyhow::anyhow!(
            "source closure Process identities differ from the Calculation Bundle process axis"
        ));
    }
    Ok(())
}

fn bundle_content_hash(manifest: &CalculationBundleManifest) -> anyhow::Result<String> {
    let mut value = serde_json::to_value(manifest)?;
    value
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("Calculation Bundle manifest must be an object"))?
        .remove("bundleContentHash");
    canonical_sha256(&value)
}

fn canonical_sha256<T: Serialize>(value: &T) -> anyhow::Result<String> {
    Ok(sha256_bytes(canonical_json_bytes(value)?.as_slice()))
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    let parsed: Value = serde_json::from_slice(bytes.as_slice())?;
    reject_non_finite_json(&parsed)?;
    Ok(serde_json::to_vec(&parsed)?)
}

fn reject_non_finite_json(value: &Value) -> anyhow::Result<()> {
    match value {
        Value::Array(items) => {
            for item in items {
                reject_non_finite_json(item)?;
            }
        }
        Value::Object(items) => {
            for item in items.values() {
                reject_non_finite_json(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn validate_sha256(value: &str, field: &str) -> anyhow::Result<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "{field} must be a lowercase SHA-256 digest"
        ))
    }
}

fn validate_version(value: &str, field: &str) -> anyhow::Result<()> {
    let bytes = value.as_bytes();
    if bytes.len() == 9
        && bytes[2] == b'.'
        && bytes[5] == b'.'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 2 || index == 5 || byte.is_ascii_digit())
    {
        Ok(())
    } else {
        Err(anyhow::anyhow!("{field} is not an ILCD version: {value}"))
    }
}

fn require_nonempty(value: &str, field: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() {
        Err(anyhow::anyhow!("{field} must not be empty"))
    } else {
        Ok(())
    }
}

fn ensure_finite(value: f64, field: &str) -> anyhow::Result<()> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("{field} must be finite"))
    }
}

fn ensure_finite_nonzero(value: f64, field: &str) -> anyhow::Result<()> {
    ensure_finite(value, field)?;
    if value == 0.0 {
        Err(anyhow::anyhow!("{field} must be non-zero"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use flate2::read::GzDecoder;
    use solver_core::FactorizationState;

    use super::*;
    use crate::{
        compiled_graph::{CompiledReleaseProcess, CompiledReleaseTechnosphereEdge},
        snapshot_index::{SnapshotImpactMapEntry, SnapshotProcessMapEntry},
    };

    #[allow(clippy::too_many_lines)]
    fn fixture_writer() -> CalculationBundleWriter {
        let process_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let flow_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let elementary_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
        let snapshot_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
        let config: SnapshotBuildConfig = serde_json::from_value(json!({
            "process_states": "100",
            "selection_mode": "filtered_library",
            "request_roots": [],
            "process_limit": 0,
            "provider_rule": "split_by_process_volume",
            "provider_candidate_eligibility_mode": "reference_output_only",
            "reference_normalization_mode": "strict",
            "allocation_fraction_mode": "strict",
            "allocation_semantics_version": "tidas-quantitative-reference-v2",
            "biosphere_sign_mode": "gross",
            "self_loop_cutoff": 0.999_999,
            "singular_eps": 1e-12,
            "has_lcia": true,
            "method_id": null,
            "method_version": null
        }))
        .unwrap();
        let coverage: SnapshotCoverageReport = serde_json::from_value(json!({
            "schema_version": "snapshot_coverage.v2",
            "matching": {
                "input_edges_total": 0,
                "matched_unique_provider": 0,
                "matched_multi_provider": 0,
                "unmatched_no_provider": 0,
                "unique_provider_match_pct": 100.0,
                "any_provider_match_pct": 100.0
            },
            "reference": {
                "process_total": 1,
                "normalized_process_count": 1,
                "missing_reference_count": 0,
                "invalid_reference_count": 0
            },
            "allocation": {
                "exchange_total": 1,
                "allocation_fraction_present_pct": 100.0,
                "allocation_fraction_missing_count": 0,
                "allocation_fraction_invalid_count": 0
            },
            "singular_risk": {
                "risk_level": "low",
                "prefilter_diag_abs_ge_cutoff": 0,
                "postfilter_a_diag_abs_ge_cutoff": 0,
                "m_zero_diagonal_count": 0,
                "m_min_abs_diagonal": 1.0
            },
            "matrix_scale": {
                "process_count": 1,
                "flow_count": 1,
                "impact_count": 25,
                "a_nnz": 0,
                "b_nnz": 1,
                "c_nnz": 25,
                "m_nnz_estimated": 1,
                "m_sparsity_estimated": 1.0
            }
        }))
        .unwrap();
        let impact_map = RELEASE_METHOD_IDENTITIES
            .iter()
            .enumerate()
            .map(|(index, (method_id, version, _))| SnapshotImpactMapEntry {
                impact_id: Uuid::parse_str(method_id).unwrap(),
                impact_index: i32::try_from(index).unwrap(),
                impact_version: Some((*version).to_owned()),
                impact_key: format!("method:{index}"),
                impact_name: format!("Method {index}"),
                unit: "kg".to_owned(),
            })
            .collect();
        let index = SnapshotIndexDocument {
            version: 1,
            snapshot_id,
            process_count: 1,
            impact_count: 25,
            process_map: vec![SnapshotProcessMapEntry {
                process_id,
                process_index: 0,
                process_version: "01.00.000".to_owned(),
                process_name: None,
                location: None,
            }],
            impact_map,
            calculation_evidence: None,
        };
        let source_document = json!({
            "processDataSet": {
                "processInformation": {
                    "dataSetInformation": { "common:UUID": process_id.to_string() }
                }
            }
        });
        let source_document_sha256 = sha256_bytes(
            canonical_value_json_bytes(&source_document)
                .unwrap()
                .as_slice(),
        );
        let evidence = CompiledReleaseEvidence {
            processes: vec![CompiledReleaseProcess {
                process_idx: 0,
                process_id,
                process_version: "01.00.000".to_owned(),
                quantitative_reference_exchange_internal_id: "0".to_owned(),
                quantitative_reference_flow_id: flow_id,
                quantitative_reference_flow_version: "01.00.000".to_owned(),
                reference_unit: "kg".to_owned(),
                normalized_mean_amount: 1.0,
                reference_direction: Some(CompiledExchangeDirection::Output),
                raw_reference_amount: Some(1.0),
                signed_raw_reference_coefficient: Some(1.0),
                normalized_reference_coefficient: Some(1.0),
            }],
            inventory_exchanges: vec![CompiledReleaseInventoryExchange {
                process_idx: 0,
                exchange_internal_id: Some("1".to_owned()),
                flow_id: elementary_id,
                flow_version: "01.00.000".to_owned(),
                direction: CompiledExchangeDirection::Output,
                unit: "kg".to_owned(),
                location: Some("GLO".to_owned()),
                normalized_mean_amount: 0.25,
                allocation_target_internal_id: "0".to_owned(),
                allocation_fraction: 1.0,
                signed_normalized_coefficient: Some(0.25),
            }],
            technosphere_edges: vec![CompiledReleaseTechnosphereEdge {
                dependent_process_idx: 0,
                residual_exchange_internal_id: "input-1".to_owned(),
                balancing_process_idx: 0,
                balancing_reference_exchange_internal_id: "0".to_owned(),
                residual_coefficient: -2.0,
                reference_coefficient: 1.0,
                routing_weight: 0.25,
                activity_requirement: 0.5,
                flow_id,
                flow_version: "01.00.000".to_owned(),
                location: Some("GLO".to_owned()),
            }],
            biosphere_edges: vec![CompiledReleaseInventoryExchange {
                process_idx: 0,
                exchange_internal_id: Some("1".to_owned()),
                flow_id: elementary_id,
                flow_version: "01.00.000".to_owned(),
                direction: CompiledExchangeDirection::Output,
                unit: "kg".to_owned(),
                location: Some("GLO".to_owned()),
                normalized_mean_amount: 0.25,
                allocation_target_internal_id: "0".to_owned(),
                allocation_fraction: 1.0,
                signed_normalized_coefficient: Some(0.25),
            }],
            source_datasets: vec![CompiledReleaseSourceDataset {
                dataset_type: CompiledReleaseSourceDatasetType::Process,
                role: CompiledReleaseSourceDatasetRole::UnitProcess,
                dataset_id: process_id,
                dataset_version: "01.00.000".to_owned(),
                document_sha256: source_document_sha256,
                document: source_document,
            }],
        };
        CalculationBundleWriter::new(
            Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").unwrap(),
            snapshot_id,
            "1".repeat(64),
            1,
            config,
            coverage,
            &index,
            &evidence,
        )
        .unwrap()
    }

    #[test]
    fn writes_directional_lci_lcia_and_stable_manifest() {
        let mut writer = fixture_writer();
        writer
            .write_result_chunk(
                0,
                &[SolveResult {
                    x: Some(vec![2.0]),
                    g: None,
                    h: Some((0..25).map(f64::from).collect()),
                    factorization_state: FactorizationState::Ready,
                }],
            )
            .unwrap();
        let built = writer.finish().unwrap();
        assert_eq!(built.manifest.schema_version, CALCULATION_BUNDLE_FORMAT);
        assert_eq!(built.manifest.artifacts.len(), 8);
        assert_eq!(built.bundle_content_hash.len(), 64);

        let lci = built
            .artifacts
            .iter()
            .find(|artifact| artifact.metadata.kind == "lci")
            .unwrap();
        let mut decoder = GzDecoder::new(File::open(&lci.local_path).unwrap());
        let mut body = String::new();
        decoder.read_to_string(&mut body).unwrap();
        assert!(body.contains("\"meanAmount\":0.5"));
        assert!(body.contains("\"direction\":\"Output\""));

        let technosphere = built
            .artifacts
            .iter()
            .find(|artifact| artifact.metadata.kind == "technosphere_edges")
            .unwrap();
        let mut decoder = GzDecoder::new(File::open(&technosphere.local_path).unwrap());
        let mut body = String::new();
        decoder.read_to_string(&mut body).unwrap();
        assert!(body.contains("\"residualExchangeInternalId\":\"input-1\""));
        assert!(body.contains("\"balancingReferenceExchangeInternalId\":\"0\""));
        assert!(body.contains("\"routingWeight\":0.25"));
        assert!(body.contains("\"activityRequirement\":0.5"));

        let inventory = built
            .artifacts
            .iter()
            .find(|artifact| artifact.metadata.kind == "inventory_axis")
            .unwrap();
        let mut decoder = GzDecoder::new(File::open(&inventory.local_path).unwrap());
        let mut body = String::new();
        decoder.read_to_string(&mut body).unwrap();
        assert!(body.contains("\"allocationTargetInternalId\":\"0\""));
        assert!(body.contains("\"allocationFraction\":1.0"));

        let source_closure = built
            .artifacts
            .iter()
            .find(|artifact| artifact.metadata.kind == "source_closure")
            .unwrap();
        let mut decoder = GzDecoder::new(File::open(&source_closure.local_path).unwrap());
        let mut body = String::new();
        decoder.read_to_string(&mut body).unwrap();
        assert!(body.contains("\"schemaVersion\":\"tiangong.source-closure.dataset.v1\""));
        assert!(body.contains("\"datasetType\":\"process\""));
        assert!(body.contains("\"role\":\"unit_process\""));
        assert!(body.contains(
            "\"path\":\"processes/11111111-1111-4111-8111-111111111111_01.00.000.json\""
        ));
    }

    #[test]
    fn rejects_missing_or_duplicate_result_chunks() {
        let writer = fixture_writer();
        assert!(writer.finish().is_err());

        let mut writer = fixture_writer();
        let result = SolveResult {
            x: Some(vec![1.0]),
            g: None,
            h: Some(vec![0.0; 25]),
            factorization_state: FactorizationState::Ready,
        };
        writer
            .write_result_chunk(0, std::slice::from_ref(&result))
            .unwrap();
        assert!(writer.write_result_chunk(0, &[result]).is_err());
    }

    #[test]
    fn identical_inputs_produce_identical_bundle_and_gzip_hashes() {
        let solve = || SolveResult {
            x: Some(vec![1.0]),
            g: None,
            h: Some(vec![0.0; 25]),
            factorization_state: FactorizationState::Ready,
        };
        let build = || {
            let mut writer = fixture_writer();
            writer.write_result_chunk(0, &[solve()]).unwrap();
            writer.finish().unwrap()
        };
        let first = build();
        let second = build();
        assert_eq!(first.bundle_content_hash, second.bundle_content_hash);
        assert_eq!(first.manifest_sha256, second.manifest_sha256);
        assert_eq!(
            first
                .manifest
                .artifacts
                .iter()
                .map(|artifact| (&artifact.path, &artifact.sha256))
                .collect::<Vec<_>>(),
            second
                .manifest
                .artifacts
                .iter()
                .map(|artifact| (&artifact.path, &artifact.sha256))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn compressed_artifacts_use_gzip_storage_content_type() {
        let mut writer = fixture_writer();
        writer
            .write_result_chunk(
                0,
                &[SolveResult {
                    x: Some(vec![1.0]),
                    g: None,
                    h: Some(vec![0.0; 25]),
                    factorization_state: FactorizationState::Ready,
                }],
            )
            .unwrap();
        let built = writer.finish().unwrap();
        let lci = built
            .manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "lci")
            .unwrap();
        assert_eq!(lci.media_type, "application/x-ndjson");
        assert_eq!(lci.compression, "gzip");
        assert_eq!(
            calculation_bundle_storage_content_type(lci).unwrap(),
            CALCULATION_BUNDLE_GZIP_CONTENT_TYPE
        );

        let coverage = built
            .manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "coverage")
            .unwrap();
        assert_eq!(
            calculation_bundle_storage_content_type(coverage).unwrap(),
            "application/json"
        );
    }
}
