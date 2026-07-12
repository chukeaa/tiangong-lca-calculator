use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::calculation_evidence::LcaCalculationEvidence;

/// Snapshot-level business index sidecar document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotIndexDocument {
    pub version: u8,
    pub snapshot_id: Uuid,
    pub process_count: i32,
    pub impact_count: i32,
    pub process_map: Vec<SnapshotProcessMapEntry>,
    pub impact_map: Vec<SnapshotImpactMapEntry>,
    /// Exact scope, method-source, and factor-coverage proof for versioned snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calculation_evidence: Option<LcaCalculationEvidence>,
}

/// Process mapping entry inside `snapshot-index-v1.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotProcessMapEntry {
    pub process_id: Uuid,
    pub process_index: i32,
    pub process_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// Impact mapping entry inside `snapshot-index-v1.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotImpactMapEntry {
    pub impact_id: Uuid,
    pub impact_index: i32,
    pub impact_key: String,
    pub impact_name: String,
    pub unit: String,
}

/// Derives the snapshot-index sidecar URL from a snapshot artifact URL.
#[must_use]
pub fn derive_snapshot_index_url(artifact_url: &str) -> String {
    match artifact_url.rfind('/') {
        Some(idx) => format!("{}snapshot-index-v1.json", &artifact_url[..=idx]),
        None => format!("{artifact_url}/snapshot-index-v1.json"),
    }
}
