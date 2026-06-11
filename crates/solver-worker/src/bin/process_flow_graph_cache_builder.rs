#![allow(
    clippy::approx_constant,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::similar_names,
    clippy::struct_field_names,
    clippy::too_many_lines
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use chrono::Utc;
use clap::Parser;
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use solver_worker::{
    pgbouncer_sqlx::{self as sqlx, Row, postgres::PgPoolOptions},
    storage::ObjectStoreClient,
};

#[path = "process_flow_graph_cache_builder/geo_regions.rs"]
mod process_flow_graph_geo_regions;

use process_flow_graph_geo_regions::{CHINA_REGION_SHAPES, GeoRegionShape, WORLD_REGION_SHAPES};

const BASIC_FLOW_TYPE: &str = "Elementary flow";
const DEFAULT_CACHE_PREFIX: &str = "national-carbon/process-flow-graph/v1";
const DEFAULT_PAGE_SIZE: i64 = 500;
const MAX_PAGE_SIZE: i64 = 1000;
const PUBLISHED_STATE_CODE: i32 = 100;
const ACTIVE_MANIFEST_SCHEMA_VERSION: &str = "process_flow_graph_manifest_v1";
const BUILD_SCHEMA_VERSION: &str = "process_flow_graph_v2";
const GEO_MAP_VIEW_SCHEMA_VERSION: &str = "process_flow_graph_geo_map_view_v2";
const EDGE_BINARY_MAGIC: &[u8; 8] = b"PFGEDG1\0";
const CSR_BINARY_MAGIC: &[u8; 8] = b"PFGCSR1\0";
const LAYOUT_BINARY_MAGIC: &[u8; 8] = b"PFGLAY1\0";
const BINARY_FORMAT_VERSION: u32 = 1;
const U32_NONE: u32 = u32::MAX;
const SPHERE_RADIUS: f32 = 310.0;
const GOLDEN_ANGLE: f32 = 2.399_963_1;
const EXPANDED_TOPOLOGY_ANCHOR_LIMIT: usize = 96;
const EXPANDED_TOPOLOGY_ITERATIONS: usize = 72;
const EXPANDED_TOPOLOGY_TARGET_WIDTH: f32 = 1900.0;
const EXPANDED_TOPOLOGY_TARGET_HEIGHT: f32 = 1250.0;
const EXPANDED_UNIFORM_OUTLINE_BINS: usize = 144;
const EXPANDED_UNIFORM_OUTLINE_QUANTILE: f32 = 0.985;
const WORLD_MAP_WIDTH: f32 = 1120.0;
const WORLD_MAP_HEIGHT: f32 = 640.0;
const CHINA_MAP_WIDTH: f32 = 1100.0;
const CHINA_MAP_HEIGHT: f32 = 720.0;
const GEO_LOCATION_RULE_VERSION: &str = "geo-map-location-v1";
const GEO_EXCLUDED_LOCATION_EXAMPLE_LIMIT: usize = 16;

#[derive(Debug, Parser)]
#[command(name = "process-flow-graph-cache-builder")]
struct Cli {
    /// `PostgreSQL` URL (preferred env: `DATABASE_URL`, fallback: `CONN`).
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    /// `PostgreSQL` URL fallback used by this project in local `.env`.
    #[arg(long, env = "CONN")]
    conn: Option<String>,
    /// S3-compatible endpoint for process-flow graph cache objects.
    #[arg(long, env = "S3_ENDPOINT")]
    s3_endpoint: Option<String>,
    /// S3 region.
    #[arg(long, env = "S3_REGION")]
    s3_region: Option<String>,
    /// S3 bucket fallback.
    #[arg(long, env = "S3_BUCKET")]
    s3_bucket: Option<String>,
    /// Dedicated process-flow graph cache bucket.
    #[arg(long, env = "PROCESS_FLOW_GRAPH_CACHE_BUCKET")]
    cache_bucket: Option<String>,
    /// S3 access key id.
    #[arg(long, env = "S3_ACCESS_KEY_ID")]
    s3_access_key_id: Option<String>,
    /// S3 access key id compatibility alias.
    #[arg(long, env = "S3_ACCESS_KEY")]
    s3_access_key: Option<String>,
    /// S3 secret access key.
    #[arg(long, env = "S3_SECRET_ACCESS_KEY")]
    s3_secret_access_key: Option<String>,
    /// S3 secret access key compatibility alias.
    #[arg(long, env = "S3_SECRET_KEY")]
    s3_secret_key: Option<String>,
    /// Optional S3 session token.
    #[arg(long, env = "S3_SESSION_TOKEN")]
    s3_session_token: Option<String>,
    /// Cache key prefix.
    #[arg(
        long,
        env = "PROCESS_FLOW_GRAPH_CACHE_PREFIX",
        default_value = DEFAULT_CACHE_PREFIX
    )]
    cache_prefix: String,
    /// Optional explicit build id.
    #[arg(long)]
    build_id: Option<String>,
    /// Limit eligible flow nodes for canary runs.
    #[arg(long)]
    limit_flows: Option<usize>,
    /// Limit connected process nodes for canary runs.
    #[arg(long)]
    limit_processes: Option<usize>,
    /// Limit exchange edges for canary runs.
    #[arg(long)]
    max_edges: Option<usize>,
    /// DB page size for source table scans.
    #[arg(long, default_value_t = DEFAULT_PAGE_SIZE)]
    page_size: i64,
    /// Optional source row limit per source table for local canary runs.
    #[arg(long)]
    source_row_limit: Option<usize>,
    /// Optional directory with classification dictionary `*.json.gz` files.
    #[arg(long, env = "PROCESS_FLOW_GRAPH_CLASSIFICATION_DIR")]
    classification_dir: Option<PathBuf>,
    /// Execute uploads. Omit for dry-run only.
    #[arg(long)]
    execute: bool,
}

#[derive(Debug, Clone)]
struct DatasetRow {
    id: String,
    json: Value,
    modified_at: Option<String>,
    version: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphNode {
    category: String,
    category_display_path: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    category_path: Vec<CategoryPathItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category_system: Option<String>,
    cluster_id_level1: String,
    cluster_id_level3: String,
    cluster_label_level1: String,
    cluster_label_level3: String,
    degree: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    flow_type: Option<String>,
    id: String,
    kind: NodeKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,
    name: String,
    object_id: String,
    version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference_year: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    type_of_data_set: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CategoryPathItem {
    id: String,
    level: u32,
    name: String,
    zh_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum NodeKind {
    Flow,
    Process,
}

#[derive(Debug, Clone)]
struct GraphEdge {
    data_derivation_type_status_idx: Option<u32>,
    direction: ExchangeDirection,
    edge_index: u32,
    exchange_internal_id: Option<u32>,
    exchange_location_idx: Option<u32>,
    flow_index: u32,
    mean_amount: Option<f64>,
    process_index: u32,
    quantitative_reference: bool,
    resulting_amount: Option<f64>,
    source_index: u32,
    target_index: u32,
    unit_idx: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum ExchangeDirection {
    Input,
    Output,
}

impl ExchangeDirection {
    const fn as_u8(self) -> u8 {
        match self {
            Self::Input => 0,
            Self::Output => 1,
        }
    }
}

#[derive(Debug, Clone)]
struct FlowMetadata {
    category: String,
    category_display_path: String,
    category_path: Vec<CategoryPathItem>,
    category_system: Option<String>,
    cluster_id_level1: String,
    cluster_id_level3: String,
    cluster_label_level1: String,
    cluster_label_level3: String,
    flow_type: String,
    id: String,
    location: Option<String>,
    name: String,
    version: String,
}

#[derive(Debug, Clone)]
struct ProcessMetadata {
    category: String,
    category_display_path: String,
    category_path: Vec<CategoryPathItem>,
    category_system: Option<String>,
    cluster_id_level1: String,
    cluster_id_level3: String,
    cluster_label_level1: String,
    cluster_label_level3: String,
    id: String,
    location: Option<String>,
    name: String,
    reference_exchange_internal_id: Option<u32>,
    reference_flow_id: Option<String>,
    reference_year: Option<String>,
    type_of_data_set: Option<String>,
    version: String,
}

#[derive(Debug, Clone)]
struct ProcessExchange {
    data_derivation_type_status: Option<String>,
    exchange_direction: ExchangeDirection,
    exchange_internal_id: Option<u32>,
    exchange_location: Option<String>,
    flow_id: String,
    flow_version: Option<String>,
    mean_amount: Option<f64>,
    quantitative_reference: bool,
    resulting_amount: Option<f64>,
    unit: Option<String>,
}

#[derive(Debug, Clone)]
struct ClassificationMetadata {
    category: String,
    category_display_path: String,
    category_path: Vec<CategoryPathItem>,
    category_system: Option<String>,
}

impl ClassificationMetadata {
    fn fallback(label: &str) -> Self {
        let trimmed_label = label.trim();
        let name = if trimmed_label.is_empty() {
            "uncategorized"
        } else {
            trimmed_label
        };
        Self {
            category: name.to_owned(),
            category_display_path: name.to_owned(),
            category_path: vec![CategoryPathItem {
                id: slug_from_text(name),
                level: 1,
                name: name.to_owned(),
                zh_name: name.to_owned(),
            }],
            category_system: None,
        }
    }

    fn from_category_path(
        category_system: Option<String>,
        category_path: Vec<CategoryPathItem>,
    ) -> Self {
        let category = category_path
            .iter()
            .map(|item| item.name.clone())
            .collect::<Vec<_>>()
            .join(" / ");
        let category_display_path = category_path
            .iter()
            .map(|item| item.zh_name.clone())
            .collect::<Vec<_>>()
            .join(" / ");

        Self {
            category,
            category_display_path,
            category_path,
            category_system,
        }
    }
}

#[derive(Debug, Clone)]
struct RawClassification {
    category_path: Vec<RawClassificationItem>,
    category_system: Option<String>,
}

#[derive(Debug, Clone)]
struct RawClassificationItem {
    id: Option<String>,
    level: u32,
    name: String,
    zh_name: Option<String>,
}

#[derive(Debug, Clone)]
struct CatalogClassificationPath {
    category_path: Vec<CategoryPathItem>,
    category_system: String,
}

#[derive(Debug, Default)]
struct ClassificationCatalog {
    by_leaf_id: BTreeMap<String, CatalogClassificationPath>,
    by_name_path: BTreeMap<String, CatalogClassificationPath>,
}

#[derive(Debug, Default)]
struct ClassificationCatalogBuilder {
    entries: BTreeMap<String, ClassificationCatalogEntry>,
}

#[derive(Debug, Clone)]
struct ClassificationCatalogEntry {
    category_system: String,
    data_type: String,
    ids: Vec<String>,
    names: Option<Vec<String>>,
    zh_names: Option<Vec<String>>,
}

impl ClassificationCatalog {
    fn load(classification_dir: Option<&Path>) -> anyhow::Result<Self> {
        let Some(classification_dir) = classification_dir else {
            return Ok(Self::default());
        };
        if !classification_dir.exists() {
            return Err(anyhow::anyhow!(
                "classification directory does not exist: {}",
                classification_dir.display()
            ));
        }

        let mut builder = ClassificationCatalogBuilder::default();
        for entry in fs::read_dir(classification_dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !file_name.ends_with(".json.gz") {
                continue;
            }

            let Some(category_system) = category_system_from_file_name(file_name) else {
                continue;
            };
            let is_zh = file_name.contains("_zh.");
            load_classification_catalog_file(&path, category_system, is_zh, &mut builder)?;
        }

        Ok(builder.finish())
    }

    fn entry_count(&self) -> usize {
        self.by_name_path.len()
    }

    fn resolve(
        &self,
        raw: &RawClassification,
        expected_data_type: &str,
    ) -> Option<ClassificationMetadata> {
        let expected_data_type = canonical_data_type(expected_data_type);
        let category_system = raw
            .category_system
            .as_deref()
            .and_then(canonical_category_system);
        let leaf_id = raw.category_path.last().and_then(|item| item.id.as_deref());
        let names = raw
            .category_path
            .iter()
            .map(|item| item.name.clone())
            .collect::<Vec<_>>();
        let resolve_name_path = || {
            if names.is_empty() {
                return None;
            }
            let normalized_path = normalized_category_path(&names);
            [
                classification_lookup_key(category_system, &expected_data_type, &normalized_path),
                classification_lookup_key(None, &expected_data_type, &normalized_path),
                classification_lookup_key(category_system, "", &normalized_path),
                classification_lookup_key(None, "", &normalized_path),
            ]
            .into_iter()
            .find_map(|key| {
                self.by_name_path.get(&key).map(|path| {
                    ClassificationMetadata::from_category_path(
                        Some(path.category_system.clone()),
                        path.category_path.clone(),
                    )
                })
            })
        };

        if category_system.is_none()
            && let Some(classification) = resolve_name_path()
        {
            return Some(classification);
        }

        if let Some(leaf_id) = leaf_id {
            let id_keys = [
                classification_lookup_key(category_system, &expected_data_type, leaf_id),
                classification_lookup_key(category_system, "", leaf_id),
                classification_lookup_key(None, &expected_data_type, leaf_id),
                classification_lookup_key(None, "", leaf_id),
            ];
            for key in id_keys {
                if let Some(path) = self.by_leaf_id.get(&key) {
                    return Some(ClassificationMetadata::from_category_path(
                        Some(path.category_system.clone()),
                        path.category_path.clone(),
                    ));
                }
            }
        }

        resolve_name_path()
    }
}

impl ClassificationCatalogBuilder {
    fn add_path(
        &mut self,
        category_system: &str,
        data_type: &str,
        is_zh: bool,
        category_path: &[CategoryPathItem],
    ) {
        if category_path.is_empty() {
            return;
        }

        let ids = category_path
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>();
        let key = classification_entry_key(category_system, data_type, &ids);
        let entry = self
            .entries
            .entry(key)
            .or_insert_with(|| ClassificationCatalogEntry {
                category_system: category_system.to_owned(),
                data_type: data_type.to_owned(),
                ids: ids.clone(),
                names: None,
                zh_names: None,
            });
        let names = category_path
            .iter()
            .map(|item| item.name.clone())
            .collect::<Vec<_>>();
        if is_zh {
            entry.zh_names = Some(names);
        } else {
            entry.names = Some(names);
        }
    }

    fn finish(self) -> ClassificationCatalog {
        let mut catalog = ClassificationCatalog::default();

        for entry in self.entries.into_values() {
            let Some(names) = entry.names.as_ref().or(entry.zh_names.as_ref()) else {
                continue;
            };
            let zh_names = entry.zh_names.as_ref().unwrap_or(names);
            let category_path = entry
                .ids
                .iter()
                .enumerate()
                .map(|(index, id)| CategoryPathItem {
                    id: id.clone(),
                    level: u32::try_from(index + 1).unwrap_or(u32::MAX),
                    name: names.get(index).cloned().unwrap_or_else(|| id.clone()),
                    zh_name: zh_names
                        .get(index)
                        .or_else(|| names.get(index))
                        .cloned()
                        .unwrap_or_else(|| id.clone()),
                })
                .collect::<Vec<_>>();
            let catalog_path = CatalogClassificationPath {
                category_path,
                category_system: entry.category_system.clone(),
            };
            let Some(leaf_id) = entry.ids.last() else {
                continue;
            };

            insert_catalog_path(
                &mut catalog.by_leaf_id,
                classification_lookup_key(Some(&entry.category_system), &entry.data_type, leaf_id),
                catalog_path.clone(),
            );
            insert_catalog_path(
                &mut catalog.by_leaf_id,
                classification_lookup_key(None, &entry.data_type, leaf_id),
                catalog_path.clone(),
            );

            let normalized_path = normalized_category_path(names);
            insert_catalog_path(
                &mut catalog.by_name_path,
                classification_lookup_key(
                    Some(&entry.category_system),
                    &entry.data_type,
                    &normalized_path,
                ),
                catalog_path.clone(),
            );
            insert_catalog_path(
                &mut catalog.by_name_path,
                classification_lookup_key(None, &entry.data_type, &normalized_path),
                catalog_path,
            );
        }

        catalog
    }
}

fn load_classification_catalog_file(
    path: &Path,
    category_system: &str,
    is_zh: bool,
    builder: &mut ClassificationCatalogBuilder,
) -> anyhow::Result<()> {
    let file = fs::File::open(path)?;
    let mut decoder = GzDecoder::new(file);
    let mut text = String::new();
    decoder.read_to_string(&mut text)?;
    let root: Value = serde_json::from_str(&text)?;
    let Some(category_system_root) = root.get("CategorySystem") else {
        return Ok(());
    };
    let Some(categories) = category_system_root.get("categories") else {
        return Ok(());
    };

    for category_group in as_array(categories) {
        let data_type = category_group
            .get("@dataType")
            .and_then(normalize_value)
            .map_or_else(|| "Flow".to_owned(), |value| canonical_data_type(&value));
        let Some(category_nodes) = category_group.get("category") else {
            continue;
        };
        for category_node in as_array(category_nodes) {
            collect_classification_catalog_path(
                category_node,
                category_system,
                &data_type,
                is_zh,
                &[],
                builder,
            );
        }
    }

    Ok(())
}

fn collect_classification_catalog_path(
    node: &Value,
    category_system: &str,
    data_type: &str,
    is_zh: bool,
    parent_path: &[CategoryPathItem],
    builder: &mut ClassificationCatalogBuilder,
) {
    let Some(name) = node
        .get("@name")
        .and_then(normalize_value)
        .or_else(|| localized_text(node))
    else {
        return;
    };
    let id = node
        .get("@id")
        .and_then(normalize_value)
        .unwrap_or_else(|| slug_from_text(&name));
    let mut category_path = parent_path.to_vec();
    category_path.push(CategoryPathItem {
        id,
        level: u32::try_from(category_path.len() + 1).unwrap_or(u32::MAX),
        name,
        zh_name: String::new(),
    });
    builder.add_path(category_system, data_type, is_zh, &category_path);

    if let Some(children) = node.get("category") {
        for child in as_array(children) {
            collect_classification_catalog_path(
                child,
                category_system,
                data_type,
                is_zh,
                &category_path,
                builder,
            );
        }
    }
}

fn insert_catalog_path(
    map: &mut BTreeMap<String, CatalogClassificationPath>,
    key: String,
    path: CatalogClassificationPath,
) {
    map.entry(key).or_insert(path);
}

fn category_system_from_file_name(file_name: &str) -> Option<&'static str> {
    if file_name.starts_with("CPCClassification") {
        Some("CPC")
    } else if file_name.starts_with("ISICClassification") {
        Some("ISIC")
    } else if file_name.starts_with("ILCD") {
        Some("ILCD")
    } else {
        None
    }
}

fn canonical_category_system(value: &str) -> Option<&'static str> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.contains("cpc") {
        Some("CPC")
    } else if normalized.contains("isic") {
        Some("ISIC")
    } else if normalized.contains("ilcd") {
        Some("ILCD")
    } else {
        None
    }
}

fn canonical_data_type(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "flow" | "流" => "Flow".to_owned(),
        "process" | "过程" | "生命周期模型" => "Process".to_owned(),
        "lciamethod" | "生命周期影响评估方法" => "LCIAMethod".to_owned(),
        "flowproperty" | "流属性" => "FlowProperty".to_owned(),
        "unitgroup" | "单位组" => "UnitGroup".to_owned(),
        "contact" | "联系信息" => "Contact".to_owned(),
        "source" | "来源" => "Source".to_owned(),
        "" => "Unspecified".to_owned(),
        _ => value.trim().to_owned(),
    }
}

fn classification_entry_key(category_system: &str, data_type: &str, ids: &[String]) -> String {
    format!(
        "{}\u{1e}{}\u{1e}{}",
        category_system,
        data_type,
        ids.join("\u{1f}")
    )
}

fn classification_lookup_key(
    category_system: Option<&str>,
    data_type: &str,
    lookup_value: &str,
) -> String {
    format!(
        "{}\u{1e}{}\u{1e}{}",
        category_system.unwrap_or(""),
        data_type,
        lookup_value
    )
}

fn normalized_category_path(parts: &[String]) -> String {
    parts
        .iter()
        .map(|part| normalized_category_lookup_part(part))
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

fn normalized_category_lookup_part(part: &str) -> String {
    part.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_lowercase()
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct BuildStats {
    edge_count: usize,
    flow_count: usize,
    max_degree: u32,
    process_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct BuildSummary {
    build_id: String,
    bucket: String,
    dry_run: bool,
    geo_maps: Vec<GeoMapSummary>,
    prefix: String,
    stats: BuildStats,
    uploaded_objects: usize,
    source_rows: SourceRows,
    source_watermarks: SourceWatermarks,
}

#[derive(Debug, Clone, Serialize)]
struct SourceRows {
    flows: usize,
    processes: usize,
}

#[derive(Debug, Clone, Serialize)]
struct SourceWatermarks {
    flows: Option<String>,
    processes: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchFlow {
    degree: u32,
    flow_type: Option<String>,
    id: String,
    name: String,
    version: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeLookup {
    edge_by_id_format: &'static str,
    flow_by_id: BTreeMap<String, u32>,
    node_by_id: BTreeMap<String, u32>,
    process_by_id: BTreeMap<String, u32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct DictionaryPayload {
    binary_formats: Value,
    build_id: String,
    data_derivation_type_statuses: Vec<String>,
    exchange_locations: Vec<String>,
    schema_version: &'static str,
    units: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct StringDictionary {
    index_by_value: BTreeMap<String, u32>,
    values: Vec<String>,
}

impl StringDictionary {
    fn intern(&mut self, value: Option<String>) -> Option<u32> {
        let value = value.map(|item| item.trim().to_owned())?;
        if value.is_empty() {
            return None;
        }
        if let Some(index) = self.index_by_value.get(&value) {
            return Some(*index);
        }
        let index = u32::try_from(self.values.len()).ok()?;
        self.values.push(value.clone());
        self.index_by_value.insert(value, index);
        Some(index)
    }
}

#[derive(Debug, Clone, Default)]
struct Dictionaries {
    data_derivation_type_statuses: StringDictionary,
    exchange_locations: StringDictionary,
    units: StringDictionary,
}

#[derive(Debug, Clone)]
struct ProcessFlowGraph {
    adjacency_edge_indices: Vec<u32>,
    adjacency_offsets: Vec<u32>,
    dictionaries: Dictionaries,
    edges: Vec<GraphEdge>,
    expanded_layout: Vec<[f32; 3]>,
    flow_by_id: BTreeMap<String, u32>,
    geo_maps: Vec<GeoMapBuild>,
    nodes: Vec<GraphNode>,
    node_by_id: BTreeMap<String, u32>,
    process_by_id: BTreeMap<String, u32>,
    search_flows: Vec<SearchFlow>,
    sphere_layout: Vec<[f32; 3]>,
    stats: BuildStats,
}

#[derive(Debug, Clone)]
struct EncodedObject {
    byte_size: usize,
    content_type: &'static str,
    path: String,
    sha256: String,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct LayoutBounds {
    height: f32,
    max_x: f32,
    max_y: f32,
    min_x: f32,
    min_y: f32,
    width: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum GeoMapScope {
    China,
    World,
}

impl GeoMapScope {
    const fn as_str(self) -> &'static str {
        match self {
            Self::China => "china",
            Self::World => "world",
        }
    }

    const fn frame(self) -> (f32, f32) {
        match self {
            Self::China => (CHINA_MAP_WIDTH, CHINA_MAP_HEIGHT),
            Self::World => (WORLD_MAP_WIDTH, WORLD_MAP_HEIGHT),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeoMapPath {
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
    id: String,
    label: String,
    path: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeoMapBackground {
    height: f32,
    paths: Vec<GeoMapPath>,
    scope: GeoMapScope,
    width: f32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessLink {
    direction: ExchangeDirection,
    exchange_id: String,
    flow_id: String,
    id: String,
    process_id: String,
    source: String,
    target: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeoMapSummary {
    edge_count: usize,
    excluded_location_count: usize,
    node_count: usize,
    process_link_count: usize,
    region_count: usize,
    scope: GeoMapScope,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeoMapDiagnostics {
    excluded_location_count: usize,
    excluded_location_examples: Vec<String>,
    location_rule_version: &'static str,
    region_count: usize,
}

#[derive(Debug, Clone)]
struct GeoMapBuild {
    adjacency: BTreeMap<String, Vec<String>>,
    background: GeoMapBackground,
    diagnostics: GeoMapDiagnostics,
    layout: Vec<[f32; 3]>,
    nodes: Vec<GraphNode>,
    process_links: Vec<ProcessLink>,
    scope: GeoMapScope,
    search_flows: Vec<SearchFlow>,
    stats: BuildStats,
    visible_edge_indices: Vec<usize>,
}

#[derive(Debug, Clone)]
struct GeoAnchor {
    key: String,
    samples: &'static [[f32; 2]],
    x: f32,
    y: f32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let page_size = cli.page_size.clamp(1, MAX_PAGE_SIZE);
    let build_id = cli.build_id.clone().unwrap_or_else(default_build_id);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET default_transaction_read_only = on")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(resolve_database_url(&cli)?)
        .await?;

    eprintln!("[process-flow-graph] reading published non-basic flows from database");
    let flow_rows =
        fetch_all_rows_read_only(&pool, "flows", page_size, cli.source_row_limit).await?;
    eprintln!("[process-flow-graph] reading published processes from database");
    let process_rows =
        fetch_all_rows_read_only(&pool, "processes", page_size, cli.source_row_limit).await?;
    eprintln!(
        "[process-flow-graph] source rows loaded: flows={} processes={}",
        flow_rows.len(),
        process_rows.len()
    );

    let classification_catalog = ClassificationCatalog::load(cli.classification_dir.as_deref())?;
    if classification_catalog.entry_count() > 0 {
        eprintln!(
            "[process-flow-graph] classification catalog loaded: entries={}",
            classification_catalog.entry_count()
        );
    }

    let graph = build_graph(&flow_rows, &process_rows, &cli, &classification_catalog)?;
    eprintln!(
        "[process-flow-graph] graph built: flows={} processes={} edges={}",
        graph.stats.flow_count, graph.stats.process_count, graph.stats.edge_count
    );
    let summary = publish_graph(
        &cli,
        &build_id,
        &graph,
        SourceRows {
            flows: flow_rows.len(),
            processes: process_rows.len(),
        },
        SourceWatermarks {
            flows: max_modified_at(&flow_rows),
            processes: max_modified_at(&process_rows),
        },
    )
    .await?;

    println!("{}", serde_json::to_string_pretty(&summary)?);
    println!(
        "[summary] dry_run={} buildId={} sourceFlows={} sourceProcesses={} flows={} processes={} edges={} uploadedObjects={} status=ok",
        summary.dry_run,
        summary.build_id,
        summary.source_rows.flows,
        summary.source_rows.processes,
        summary.stats.flow_count,
        summary.stats.process_count,
        summary.stats.edge_count,
        summary.uploaded_objects
    );
    Ok(())
}

fn default_build_id() -> String {
    format!(
        "process-flow-graph-{}",
        Utc::now().to_rfc3339().replace(['+', ':', '.'], "-")
    )
}

fn max_modified_at(rows: &[DatasetRow]) -> Option<String> {
    rows.iter()
        .filter_map(|row| row.modified_at.as_deref())
        .max()
        .map(str::to_owned)
}

fn resolve_database_url(cli: &Cli) -> anyhow::Result<&str> {
    cli.database_url
        .as_deref()
        .or(cli.conn.as_deref())
        .ok_or_else(|| anyhow::anyhow!("missing DB connection: set DATABASE_URL or CONN"))
}

fn required<'a>(value: Option<&'a str>, name: &str) -> anyhow::Result<&'a str> {
    value.ok_or_else(|| anyhow::anyhow!("missing {name}"))
}

fn resolve_bucket(cli: &Cli) -> anyhow::Result<&str> {
    required(
        cli.cache_bucket.as_deref().or(cli.s3_bucket.as_deref()),
        "PROCESS_FLOW_GRAPH_CACHE_BUCKET or S3_BUCKET",
    )
}

fn resolve_access_key(cli: &Cli) -> anyhow::Result<&str> {
    required(
        cli.s3_access_key_id
            .as_deref()
            .or(cli.s3_access_key.as_deref()),
        "S3_ACCESS_KEY_ID or S3_ACCESS_KEY",
    )
}

fn resolve_secret_key(cli: &Cli) -> anyhow::Result<&str> {
    required(
        cli.s3_secret_access_key
            .as_deref()
            .or(cli.s3_secret_key.as_deref()),
        "S3_SECRET_ACCESS_KEY or S3_SECRET_KEY",
    )
}

fn build_object_store(cli: &Cli) -> anyhow::Result<ObjectStoreClient> {
    ObjectStoreClient::new(
        required(cli.s3_endpoint.as_deref(), "S3_ENDPOINT")?,
        required(cli.s3_region.as_deref(), "S3_REGION")?,
        resolve_bucket(cli)?,
        "",
        resolve_access_key(cli)?,
        resolve_secret_key(cli)?,
        cli.s3_session_token.clone(),
    )
}

async fn fetch_all_rows(
    pool: &sqlx::PgPool,
    table: &str,
    page_size: i64,
    source_row_limit: Option<usize>,
) -> anyhow::Result<Vec<DatasetRow>> {
    let mut rows = Vec::new();
    let mut last_id: Option<String> = None;
    let mut last_version: Option<String> = None;

    loop {
        if source_row_limit.is_some_and(|limit| rows.len() >= limit) {
            break;
        }
        let remaining_limit = source_row_limit
            .and_then(|limit| i64::try_from(limit.saturating_sub(rows.len())).ok())
            .unwrap_or(page_size);
        let query_limit = page_size.min(remaining_limit.max(1));
        eprintln!(
            "[process-flow-graph] fetching {table} rows after={} limit={query_limit}",
            last_id.as_deref().unwrap_or("start")
        );
        let page_rows = fetch_rows_page_read_only(
            pool,
            table,
            query_limit,
            last_id.as_deref(),
            last_version.as_deref(),
        )
        .await?;

        let page_len = page_rows.len();
        eprintln!(
            "[process-flow-graph] fetched {table} page rows={} total={}",
            page_len,
            rows.len() + page_len
        );
        if let Some(last_row) = page_rows.last() {
            last_id = Some(last_row.id.clone());
            last_version = Some(last_row.version.clone());
        }
        rows.extend(page_rows);

        if i64::try_from(page_len).unwrap_or_default() < query_limit {
            break;
        }
    }

    Ok(rows)
}

async fn fetch_all_rows_read_only(
    pool: &sqlx::PgPool,
    table: &str,
    page_size: i64,
    source_row_limit: Option<usize>,
) -> anyhow::Result<Vec<DatasetRow>> {
    fetch_all_rows(pool, table, page_size, source_row_limit).await
}

async fn fetch_rows_page_read_only(
    pool: &sqlx::PgPool,
    table: &str,
    query_limit: i64,
    last_id: Option<&str>,
    last_version: Option<&str>,
) -> anyhow::Result<Vec<DatasetRow>> {
    let flow_type_filter = if table == "flows" {
        "AND COALESCE(\
            json_ordered::jsonb #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}', \
            json_ordered::jsonb #>> '{flowDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}', \
            json_ordered::jsonb #>> '{flow_data_set,modellingAndValidation,LCIMethod,typeOfDataSet}', \
            json_ordered::jsonb #>> '{flow_data_set,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}', \
            json::jsonb #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}', \
            json::jsonb #>> '{flowDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}', \
            json::jsonb #>> '{flow_data_set,modellingAndValidation,LCIMethod,typeOfDataSet}', \
            json::jsonb #>> '{flow_data_set,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' \
         ) IS NOT NULL \
         AND COALESCE(\
            json_ordered::jsonb #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}', \
            json_ordered::jsonb #>> '{flowDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}', \
            json_ordered::jsonb #>> '{flow_data_set,modellingAndValidation,LCIMethod,typeOfDataSet}', \
            json_ordered::jsonb #>> '{flow_data_set,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}', \
            json::jsonb #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}', \
            json::jsonb #>> '{flowDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}', \
            json::jsonb #>> '{flow_data_set,modellingAndValidation,LCIMethod,typeOfDataSet}', \
            json::jsonb #>> '{flow_data_set,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' \
         ) <> 'Elementary flow'"
    } else {
        ""
    };
    let query = format!(
        "SELECT id::text AS id, version, COALESCE(json_ordered::jsonb, json::jsonb) AS json, modified_at::text AS modified_at \
         FROM public.{table} \
         WHERE state_code = $1 \
           {flow_type_filter} \
           AND ($3::text IS NULL OR (id::text, version) > ($3::text, $4::text)) \
         ORDER BY id::text ASC, version ASC \
         LIMIT $2"
    );
    let mut attempts = 0_u8;
    loop {
        attempts += 1;
        match fetch_rows_page_read_only_once(pool, &query, query_limit, last_id, last_version).await
        {
            Ok(rows) => return Ok(rows),
            Err(error) if attempts < 3 => {
                eprintln!(
                    "[process-flow-graph] retrying {table} page after transient read error: {error}"
                );
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn fetch_rows_page_read_only_once(
    pool: &sqlx::PgPool,
    query: &str,
    query_limit: i64,
    last_id: Option<&str>,
    last_version: Option<&str>,
) -> anyhow::Result<Vec<DatasetRow>> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await?;
    let page = sqlx::query(query)
        .bind(PUBLISHED_STATE_CODE)
        .bind(query_limit)
        .bind(last_id)
        .bind(last_version.unwrap_or(""))
        .fetch_all(&mut *tx)
        .await?;
    let mut rows = Vec::with_capacity(page.len());
    for row in page {
        rows.push(DatasetRow {
            id: row.try_get("id")?,
            json: row.try_get("json")?,
            modified_at: row.try_get("modified_at")?,
            version: row.try_get("version")?,
        });
    }
    tx.commit().await?;
    Ok(rows)
}

fn build_graph(
    flow_rows: &[DatasetRow],
    process_rows: &[DatasetRow],
    cli: &Cli,
    classification_catalog: &ClassificationCatalog,
) -> anyhow::Result<ProcessFlowGraph> {
    let mut flow_by_version = BTreeMap::<String, FlowMetadata>::new();
    let mut latest_flow_by_id = BTreeMap::<String, FlowMetadata>::new();

    for row in flow_rows {
        let Some(flow_meta) = parse_flow_row(row, classification_catalog) else {
            continue;
        };
        if cli.limit_flows.is_some_and(|limit| {
            latest_flow_by_id.len() >= limit && !latest_flow_by_id.contains_key(&flow_meta.id)
        }) {
            continue;
        }
        flow_by_version.insert(
            flow_version_key(&flow_meta.id, &flow_meta.version),
            flow_meta.clone(),
        );
        let replace_latest = latest_flow_by_id
            .get(&flow_meta.id)
            .is_none_or(|current| flow_meta.version > current.version);
        if replace_latest {
            latest_flow_by_id.insert(flow_meta.id.clone(), flow_meta);
        }
    }

    if flow_by_version.is_empty() {
        return Err(anyhow::anyhow!("no eligible non-basic flows found"));
    }

    let mut graph = GraphBuilder::new();
    let mut latest_flow_index_by_id = BTreeMap::<String, u32>::new();
    let mut flow_index_by_version = BTreeMap::<String, u32>::new();

    for flow_meta in flow_by_version.values() {
        let node_index = graph.add_flow_node(flow_meta)?;
        flow_index_by_version.insert(
            flow_version_key(&flow_meta.id, &flow_meta.version),
            node_index,
        );
    }
    for flow_meta in latest_flow_by_id.values() {
        if let Some(index) =
            flow_index_by_version.get(&flow_version_key(&flow_meta.id, &flow_meta.version))
        {
            latest_flow_index_by_id.insert(flow_meta.id.clone(), *index);
        }
    }

    for row in process_rows {
        if cli
            .limit_processes
            .is_some_and(|limit| graph.process_by_id.len() >= limit)
        {
            break;
        }
        if cli
            .max_edges
            .is_some_and(|limit| graph.edges.len() >= limit)
        {
            break;
        }
        let Some(process_meta) = parse_process_metadata(row, classification_catalog) else {
            continue;
        };
        let mut process_index: Option<u32> = None;

        for exchange in parse_process_exchanges(row, &process_meta) {
            if cli
                .max_edges
                .is_some_and(|limit| graph.edges.len() >= limit)
            {
                break;
            }
            let Some(flow_index) = resolve_flow_index(
                &exchange.flow_id,
                exchange.flow_version.as_deref(),
                &flow_index_by_version,
                &latest_flow_index_by_id,
            ) else {
                continue;
            };
            let resolved_process_index = if let Some(index) = process_index {
                index
            } else {
                let index = graph.add_process_node(&process_meta)?;
                process_index = Some(index);
                index
            };
            graph.add_edge(resolved_process_index, flow_index, exchange)?;
        }
    }

    graph.finish()
}

struct GraphBuilder {
    dictionaries: Dictionaries,
    edges: Vec<GraphEdge>,
    flow_by_id: BTreeMap<String, u32>,
    node_by_id: BTreeMap<String, u32>,
    nodes: Vec<GraphNode>,
    process_by_id: BTreeMap<String, u32>,
}

impl GraphBuilder {
    fn new() -> Self {
        Self {
            dictionaries: Dictionaries::default(),
            edges: Vec::new(),
            flow_by_id: BTreeMap::new(),
            node_by_id: BTreeMap::new(),
            nodes: Vec::new(),
            process_by_id: BTreeMap::new(),
        }
    }

    fn add_flow_node(&mut self, flow: &FlowMetadata) -> anyhow::Result<u32> {
        let node_id = flow_node_id(flow);
        if let Some(index) = self.node_by_id.get(&node_id) {
            return Ok(*index);
        }
        let index = u32::try_from(self.nodes.len())
            .map_err(|_| anyhow::anyhow!("node count exceeds u32"))?;
        self.nodes.push(GraphNode {
            category: flow.category.clone(),
            category_display_path: flow.category_display_path.clone(),
            category_path: flow.category_path.clone(),
            category_system: flow.category_system.clone(),
            cluster_id_level1: flow.cluster_id_level1.clone(),
            cluster_id_level3: flow.cluster_id_level3.clone(),
            cluster_label_level1: flow.cluster_label_level1.clone(),
            cluster_label_level3: flow.cluster_label_level3.clone(),
            degree: 0,
            flow_type: Some(flow.flow_type.clone()),
            id: node_id.clone(),
            kind: NodeKind::Flow,
            location: flow.location.clone(),
            name: flow.name.clone(),
            object_id: flow.id.clone(),
            version: flow.version.clone(),
            reference_year: None,
            type_of_data_set: None,
        });
        self.node_by_id.insert(node_id.clone(), index);
        self.flow_by_id.insert(node_id, index);
        Ok(index)
    }

    fn add_process_node(&mut self, process: &ProcessMetadata) -> anyhow::Result<u32> {
        let node_id = process_node_id(process);
        if let Some(index) = self.node_by_id.get(&node_id) {
            return Ok(*index);
        }
        let index = u32::try_from(self.nodes.len())
            .map_err(|_| anyhow::anyhow!("node count exceeds u32"))?;
        self.nodes.push(GraphNode {
            category: process.category.clone(),
            category_display_path: process.category_display_path.clone(),
            category_path: process.category_path.clone(),
            category_system: process.category_system.clone(),
            cluster_id_level1: process.cluster_id_level1.clone(),
            cluster_id_level3: process.cluster_id_level3.clone(),
            cluster_label_level1: process.cluster_label_level1.clone(),
            cluster_label_level3: process.cluster_label_level3.clone(),
            degree: 0,
            flow_type: None,
            id: node_id.clone(),
            kind: NodeKind::Process,
            location: process.location.clone(),
            name: process.name.clone(),
            object_id: process.id.clone(),
            version: process.version.clone(),
            reference_year: process.reference_year.clone(),
            type_of_data_set: process.type_of_data_set.clone(),
        });
        self.node_by_id.insert(node_id.clone(), index);
        self.process_by_id.insert(node_id, index);
        Ok(index)
    }

    fn add_edge(
        &mut self,
        process_index: u32,
        flow_index: u32,
        exchange: ProcessExchange,
    ) -> anyhow::Result<()> {
        let edge_index = u32::try_from(self.edges.len())
            .map_err(|_| anyhow::anyhow!("edge count exceeds u32"))?;
        let (source_index, target_index) = match exchange.exchange_direction {
            ExchangeDirection::Input => (flow_index, process_index),
            ExchangeDirection::Output => (process_index, flow_index),
        };
        let data_derivation_type_status_idx = self
            .dictionaries
            .data_derivation_type_statuses
            .intern(exchange.data_derivation_type_status);
        let exchange_location_idx = self
            .dictionaries
            .exchange_locations
            .intern(exchange.exchange_location);
        let unit_idx = self.dictionaries.units.intern(exchange.unit);

        self.edges.push(GraphEdge {
            data_derivation_type_status_idx,
            direction: exchange.exchange_direction,
            edge_index,
            exchange_internal_id: exchange.exchange_internal_id,
            exchange_location_idx,
            flow_index,
            mean_amount: exchange.mean_amount,
            process_index,
            quantitative_reference: exchange.quantitative_reference,
            resulting_amount: exchange.resulting_amount,
            source_index,
            target_index,
            unit_idx,
        });
        Ok(())
    }

    fn finish(mut self) -> anyhow::Result<ProcessFlowGraph> {
        let mut adjacency = vec![Vec::<u32>::new(); self.nodes.len()];
        let mut degrees = vec![0_u32; self.nodes.len()];
        for edge in &self.edges {
            let source = usize::try_from(edge.source_index)?;
            let target = usize::try_from(edge.target_index)?;
            adjacency[source].push(edge.edge_index);
            adjacency[target].push(edge.edge_index);
            degrees[source] = degrees[source].saturating_add(1);
            degrees[target] = degrees[target].saturating_add(1);
        }
        for (node, degree) in self.nodes.iter_mut().zip(degrees.iter().copied()) {
            node.degree = degree;
        }
        let max_degree = degrees.iter().copied().max().unwrap_or_default();
        let stats = BuildStats {
            edge_count: self.edges.len(),
            flow_count: self.flow_by_id.len(),
            max_degree,
            process_count: self.process_by_id.len(),
        };
        let (adjacency_offsets, adjacency_edge_indices) = build_csr(adjacency)?;
        let sphere_layout = create_sphere_layout(&self.nodes);
        let expanded_layout = create_expanded_layout(&self.nodes, &self.edges);
        let search_flows = build_search_flows(&self.nodes);

        let mut graph = ProcessFlowGraph {
            adjacency_edge_indices,
            adjacency_offsets,
            dictionaries: self.dictionaries,
            edges: self.edges,
            expanded_layout,
            flow_by_id: self.flow_by_id,
            geo_maps: Vec::new(),
            nodes: self.nodes,
            node_by_id: self.node_by_id,
            process_by_id: self.process_by_id,
            search_flows,
            sphere_layout,
            stats,
        };
        graph.geo_maps = create_geo_map_builds(&graph)?;

        Ok(graph)
    }
}

fn build_csr(adjacency: Vec<Vec<u32>>) -> anyhow::Result<(Vec<u32>, Vec<u32>)> {
    let mut offsets = Vec::with_capacity(adjacency.len() + 1);
    let mut edge_indices = Vec::new();
    offsets.push(0);
    for mut edges in adjacency {
        edges.sort_unstable();
        edges.dedup();
        edge_indices.extend(edges);
        offsets.push(
            u32::try_from(edge_indices.len())
                .map_err(|_| anyhow::anyhow!("adjacency edge reference count exceeds u32"))?,
        );
    }
    Ok((offsets, edge_indices))
}

fn build_search_flows(nodes: &[GraphNode]) -> Vec<SearchFlow> {
    let mut flows = nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Flow)
        .map(|node| SearchFlow {
            degree: node.degree,
            flow_type: node.flow_type.clone(),
            id: node.id.clone(),
            name: node.name.clone(),
            version: node.version.clone(),
        })
        .collect::<Vec<_>>();
    flows.sort_by(|left, right| {
        right
            .degree
            .cmp(&left.degree)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.id.cmp(&right.id))
    });
    flows
}

fn resolve_flow_index(
    flow_id: &str,
    flow_version: Option<&str>,
    flow_index_by_version: &BTreeMap<String, u32>,
    latest_flow_index_by_id: &BTreeMap<String, u32>,
) -> Option<u32> {
    flow_version
        .and_then(|version| {
            flow_index_by_version
                .get(&flow_version_key(flow_id, version))
                .copied()
        })
        .or_else(|| latest_flow_index_by_id.get(flow_id).copied())
}

async fn publish_graph(
    cli: &Cli,
    build_id: &str,
    graph: &ProcessFlowGraph,
    source_rows: SourceRows,
    source_watermarks: SourceWatermarks,
) -> anyhow::Result<BuildSummary> {
    let prefix = normalize_prefix(&cli.cache_prefix);
    let bucket = resolve_bucket(cli)?.to_owned();
    let dry_run = !cli.execute;
    let store = if dry_run {
        None
    } else {
        Some(build_object_store(cli)?)
    };
    let generated_at = Utc::now().to_rfc3339();
    let mut objects = encode_graph_objects(&prefix, build_id, graph)?;
    let build_manifest = build_manifest_object(
        &prefix,
        build_id,
        graph,
        &objects,
        &generated_at,
        &source_watermarks,
    )?;
    objects.push(build_manifest);
    let active_manifest = active_manifest_object(&prefix, build_id, &generated_at)?;
    objects.push(active_manifest);

    for object in &objects {
        if let Some(store) = store.as_ref() {
            store
                .upload_object_key(&object.path, object.content_type, object.bytes.clone())
                .await?;
        }
    }

    Ok(BuildSummary {
        build_id: build_id.to_owned(),
        bucket,
        dry_run,
        geo_maps: graph
            .geo_maps
            .iter()
            .map(|geo_map| GeoMapSummary {
                edge_count: geo_map.stats.edge_count,
                excluded_location_count: geo_map.diagnostics.excluded_location_count,
                node_count: geo_map.nodes.len(),
                process_link_count: geo_map.process_links.len(),
                region_count: geo_map.diagnostics.region_count,
                scope: geo_map.scope,
            })
            .collect(),
        prefix,
        stats: graph.stats.clone(),
        uploaded_objects: if dry_run { 0 } else { objects.len() },
        source_rows,
        source_watermarks,
    })
}

fn encode_graph_objects(
    prefix: &str,
    build_id: &str,
    graph: &ProcessFlowGraph,
) -> anyhow::Result<Vec<EncodedObject>> {
    let build_prefix = format!("{prefix}/builds/{build_id}");
    let nodes_payload = json!({
        "schemaVersion": BUILD_SCHEMA_VERSION,
        "buildId": build_id,
        "nodes": graph.nodes,
    });
    let dictionary_payload = DictionaryPayload {
        binary_formats: json!({
            "edges": {
                "format": "little-endian",
                "magic": "PFGEDG1",
                "version": BINARY_FORMAT_VERSION,
                "recordFields": [
                    "sourceIndex:u32",
                    "targetIndex:u32",
                    "flowIndex:u32",
                    "processIndex:u32",
                    "direction:u8",
                    "quantitativeReference:u8",
                    "reserved:u16",
                    "meanAmount:f64",
                    "resultingAmount:f64",
                    "dataDerivationTypeStatusIndex:u32",
                    "exchangeLocationIndex:u32",
                    "unitIndex:u32",
                    "exchangeInternalId:u32"
                ]
            },
            "adjacency": {
                "format": "little-endian",
                "magic": "PFGCSR1",
                "version": BINARY_FORMAT_VERSION,
                "arrays": ["offsets:u32[nodeCount+1]", "edgeIndices:u32[edgeReferenceCount]"]
            },
            "layout": {
                "format": "little-endian",
                "magic": "PFGLAY1",
                "version": BINARY_FORMAT_VERSION,
                "arrays": ["xyz:f32[nodeCount*3]"]
            }
        }),
        build_id: build_id.to_owned(),
        data_derivation_type_statuses: graph
            .dictionaries
            .data_derivation_type_statuses
            .values
            .clone(),
        exchange_locations: graph.dictionaries.exchange_locations.values.clone(),
        schema_version: BUILD_SCHEMA_VERSION,
        units: graph.dictionaries.units.values.clone(),
    };
    let lookup_payload = NodeLookup {
        edge_by_id_format: "exchange:{edgeIndex}",
        flow_by_id: graph.flow_by_id.clone(),
        node_by_id: graph.node_by_id.clone(),
        process_by_id: graph.process_by_id.clone(),
    };

    let mut objects = vec![
        encoded_gzip_json(
            format!("{build_prefix}/graph/nodes.json.gz"),
            &nodes_payload,
        )?,
        encoded_gzip_binary(
            format!("{build_prefix}/graph/edges.bin.gz"),
            &encode_edges(graph)?,
        )?,
        encoded_gzip_binary(
            format!("{build_prefix}/graph/adjacency.csr.bin.gz"),
            &encode_adjacency(graph)?,
        )?,
        encoded_gzip_json(
            format!("{build_prefix}/graph/dictionaries.json.gz"),
            &dictionary_payload,
        )?,
        encoded_gzip_binary(
            format!("{build_prefix}/layout/sphere3d.f32.bin.gz"),
            &encode_layout(graph.nodes.len(), &graph.sphere_layout)?,
        )?,
        encoded_gzip_binary(
            format!("{build_prefix}/layout/expanded2d.f32.bin.gz"),
            &encode_layout(graph.nodes.len(), &graph.expanded_layout)?,
        )?,
        encoded_gzip_json(
            format!("{build_prefix}/layout/clusters.json.gz"),
            &cluster_payload(build_id, &graph.nodes),
        )?,
        encoded_gzip_json(
            format!("{build_prefix}/layout/clusters-level1.json.gz"),
            &cluster_level_payload(build_id, &graph.nodes, ClusterLevel::Level1),
        )?,
        encoded_gzip_json(
            format!("{build_prefix}/layout/clusters-level3.json.gz"),
            &cluster_level_payload(build_id, &graph.nodes, ClusterLevel::Level3),
        )?,
        encoded_gzip_json(
            format!("{build_prefix}/indexes/search-flows.json.gz"),
            &json!({
                "schemaVersion": BUILD_SCHEMA_VERSION,
                "buildId": build_id,
                "searchFlows": graph.search_flows,
            }),
        )?,
        encoded_gzip_json(
            format!("{build_prefix}/indexes/node-lookup.json.gz"),
            &lookup_payload,
        )?,
    ];

    for geo_map in &graph.geo_maps {
        objects.extend(encode_geo_map_objects(
            &build_prefix,
            build_id,
            graph,
            geo_map,
        )?);
    }

    Ok(objects)
}

fn build_manifest_object(
    prefix: &str,
    build_id: &str,
    graph: &ProcessFlowGraph,
    objects: &[EncodedObject],
    generated_at: &str,
    source_watermarks: &SourceWatermarks,
) -> anyhow::Result<EncodedObject> {
    let build_prefix = format!("{prefix}/builds/{build_id}/");
    let mut files = Map::new();
    for object in objects {
        let relative_path = object
            .path
            .strip_prefix(&build_prefix)
            .unwrap_or(object.path.as_str());
        files.insert(
            file_key(relative_path).to_owned(),
            json!({
                "path": relative_path,
                "byteSize": object.byte_size,
                "sha256": object.sha256,
                "contentType": object.content_type,
            }),
        );
    }
    let payload = json!({
        "schemaVersion": BUILD_SCHEMA_VERSION,
        "buildId": build_id,
        "generatedAt": generated_at,
        "dataAsOf": generated_at,
        "sourceWatermarks": source_watermarks,
        "stats": graph.stats,
        "files": files,
    });
    encoded_json(
        format!("{prefix}/builds/{build_id}/manifest.json"),
        &payload,
    )
}

fn active_manifest_object(
    prefix: &str,
    build_id: &str,
    generated_at: &str,
) -> anyhow::Result<EncodedObject> {
    encoded_json(
        format!("{prefix}/manifest.json"),
        &json!({
            "schemaVersion": ACTIVE_MANIFEST_SCHEMA_VERSION,
            "activeBuildId": build_id,
            "buildManifestPath": format!("builds/{build_id}/manifest.json"),
            "generatedAt": generated_at,
        }),
    )
}

fn file_key(relative_path: &str) -> &'static str {
    match relative_path {
        "graph/nodes.json.gz" => "nodes",
        "graph/edges.bin.gz" => "edges",
        "graph/adjacency.csr.bin.gz" => "adjacency",
        "graph/dictionaries.json.gz" => "dictionaries",
        "layout/sphere3d.f32.bin.gz" => "sphere3d",
        "layout/expanded2d.f32.bin.gz" => "expanded2d",
        "layout/clusters.json.gz" => "clusters",
        "layout/clusters-level1.json.gz" => "clustersLevel1",
        "layout/clusters-level3.json.gz" => "clustersLevel3",
        "geo-map/world/view.json.gz" => "geoMapWorldView",
        "geo-map/world/edges.bin.gz" => "geoMapWorldEdges",
        "geo-map/world/adjacency.csr.bin.gz" => "geoMapWorldAdjacency",
        "geo-map/world/layout.f32.bin.gz" => "geoMapWorldLayout",
        "geo-map/china/view.json.gz" => "geoMapChinaView",
        "geo-map/china/edges.bin.gz" => "geoMapChinaEdges",
        "geo-map/china/adjacency.csr.bin.gz" => "geoMapChinaAdjacency",
        "geo-map/china/layout.f32.bin.gz" => "geoMapChinaLayout",
        "indexes/search-flows.json.gz" => "searchFlows",
        "indexes/node-lookup.json.gz" => "nodeLookup",
        _ => "unknown",
    }
}

fn encoded_json<T>(path: String, payload: &T) -> anyhow::Result<EncodedObject>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(payload)?;
    Ok(encoded_object(path, "application/json", bytes))
}

fn encoded_gzip_json<T>(path: String, payload: &T) -> anyhow::Result<EncodedObject>
where
    T: Serialize,
{
    let json_bytes = serde_json::to_vec(payload)?;
    let bytes = gzip_bytes(&json_bytes)?;
    Ok(encoded_object(path, "application/gzip", bytes))
}

fn encoded_gzip_binary(path: String, bytes: &[u8]) -> anyhow::Result<EncodedObject> {
    Ok(encoded_object(path, "application/gzip", gzip_bytes(bytes)?))
}

fn encoded_object(path: String, content_type: &'static str, bytes: Vec<u8>) -> EncodedObject {
    let byte_size = bytes.len();
    EncodedObject {
        byte_size,
        content_type,
        path,
        sha256: sha256_hex(&bytes),
        bytes,
    }
}

fn gzip_bytes(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn encode_edges(graph: &ProcessFlowGraph) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(16 + graph.edges.len() * 52);
    bytes.extend_from_slice(EDGE_BINARY_MAGIC);
    push_u32(&mut bytes, BINARY_FORMAT_VERSION);
    push_u32(&mut bytes, u32::try_from(graph.edges.len())?);
    for edge in &graph.edges {
        push_u32(&mut bytes, edge.source_index);
        push_u32(&mut bytes, edge.target_index);
        push_u32(&mut bytes, edge.flow_index);
        push_u32(&mut bytes, edge.process_index);
        bytes.push(edge.direction.as_u8());
        bytes.push(u8::from(edge.quantitative_reference));
        push_u16(&mut bytes, 0);
        push_f64(&mut bytes, edge.mean_amount.unwrap_or(f64::NAN));
        push_f64(&mut bytes, edge.resulting_amount.unwrap_or(f64::NAN));
        push_u32(
            &mut bytes,
            edge.data_derivation_type_status_idx.unwrap_or(U32_NONE),
        );
        push_u32(&mut bytes, edge.exchange_location_idx.unwrap_or(U32_NONE));
        push_u32(&mut bytes, edge.unit_idx.unwrap_or(U32_NONE));
        push_u32(&mut bytes, edge.exchange_internal_id.unwrap_or(U32_NONE));
    }
    Ok(bytes)
}

fn encode_adjacency(graph: &ProcessFlowGraph) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(
        20 + (graph.adjacency_offsets.len() + graph.adjacency_edge_indices.len()) * 4,
    );
    bytes.extend_from_slice(CSR_BINARY_MAGIC);
    push_u32(&mut bytes, BINARY_FORMAT_VERSION);
    push_u32(&mut bytes, u32::try_from(graph.nodes.len())?);
    push_u32(
        &mut bytes,
        u32::try_from(graph.adjacency_edge_indices.len())?,
    );
    for value in &graph.adjacency_offsets {
        push_u32(&mut bytes, *value);
    }
    for value in &graph.adjacency_edge_indices {
        push_u32(&mut bytes, *value);
    }
    Ok(bytes)
}

fn encode_layout(node_count: usize, layout: &[[f32; 3]]) -> anyhow::Result<Vec<u8>> {
    if node_count != layout.len() {
        return Err(anyhow::anyhow!("layout length mismatch"));
    }
    let mut bytes = Vec::with_capacity(16 + layout.len() * 12);
    bytes.extend_from_slice(LAYOUT_BINARY_MAGIC);
    push_u32(&mut bytes, BINARY_FORMAT_VERSION);
    push_u32(&mut bytes, u32::try_from(node_count)?);
    for [x, y, z] in layout {
        push_f32(&mut bytes, *x);
        push_f32(&mut bytes, *y);
        push_f32(&mut bytes, *z);
    }
    Ok(bytes)
}

fn push_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_f32(bytes: &mut Vec<u8>, value: f32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_f64(bytes: &mut Vec<u8>, value: f64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClusterLevel {
    Level1,
    Level3,
}

fn cluster_fields(node: &GraphNode, level: ClusterLevel) -> (&str, &str) {
    match level {
        ClusterLevel::Level1 => (&node.cluster_id_level1, &node.cluster_label_level1),
        ClusterLevel::Level3 => (&node.cluster_id_level3, &node.cluster_label_level3),
    }
}

fn collect_clusters(nodes: &[GraphNode], level: ClusterLevel) -> Vec<Value> {
    let mut clusters = BTreeMap::<String, (String, usize)>::new();
    for node in nodes {
        let (id, label) = cluster_fields(node, level);
        let entry = clusters
            .entry(id.to_owned())
            .or_insert_with(|| (label.to_owned(), 0));
        entry.1 += 1;
    }
    clusters
        .into_iter()
        .map(|(id, (label, count))| {
            json!({
                "id": id,
                "label": label,
                "count": count,
            })
        })
        .collect()
}

fn cluster_level_payload(build_id: &str, nodes: &[GraphNode], level: ClusterLevel) -> Value {
    let payload_key = match level {
        ClusterLevel::Level1 => "clustersLevel1",
        ClusterLevel::Level3 => "clustersLevel3",
    };
    json!({
        "schemaVersion": BUILD_SCHEMA_VERSION,
        "buildId": build_id,
        payload_key: collect_clusters(nodes, level),
    })
}

fn cluster_payload(build_id: &str, nodes: &[GraphNode]) -> Value {
    let clusters_level1 = collect_clusters(nodes, ClusterLevel::Level1);
    let clusters_level3 = collect_clusters(nodes, ClusterLevel::Level3);
    json!({
        "schemaVersion": BUILD_SCHEMA_VERSION,
        "buildId": build_id,
        "clusters": clusters_level1.clone(),
        "clustersLevel1": clusters_level1,
        "clustersLevel3": clusters_level3,
    })
}

fn encode_geo_map_objects(
    build_prefix: &str,
    build_id: &str,
    graph: &ProcessFlowGraph,
    geo_map: &GeoMapBuild,
) -> anyhow::Result<Vec<EncodedObject>> {
    let scope = geo_map.scope.as_str();
    let units = graph.dictionaries.units.values.clone();
    let view_payload = json!({
        "schemaVersion": GEO_MAP_VIEW_SCHEMA_VERSION,
        "buildId": build_id,
        "scope": geo_map.scope,
        "background": geo_map.background,
        "geoMapFrame": {
            "width": geo_map.background.width,
            "height": geo_map.background.height,
        },
        "nodes": geo_map.nodes,
        "processLinks": geo_map.process_links,
        "adjacency": geo_map.adjacency,
        "adjacencyIncludesProcessLinks": true,
        "clustersLevel1": collect_clusters(&geo_map.nodes, ClusterLevel::Level1),
        "clustersLevel3": collect_clusters(&geo_map.nodes, ClusterLevel::Level3),
        "diagnostics": geo_map.diagnostics,
        "searchFlows": geo_map.search_flows,
        "stats": geo_map.stats,
        "units": units,
    });

    Ok(vec![
        encoded_gzip_json(
            format!("{build_prefix}/geo-map/{scope}/view.json.gz"),
            &view_payload,
        )?,
        encoded_gzip_binary(
            format!("{build_prefix}/geo-map/{scope}/edges.bin.gz"),
            &encode_geo_edges(graph, geo_map)?,
        )?,
        encoded_gzip_binary(
            format!("{build_prefix}/geo-map/{scope}/adjacency.csr.bin.gz"),
            &encode_geo_adjacency(geo_map)?,
        )?,
        encoded_gzip_binary(
            format!("{build_prefix}/geo-map/{scope}/layout.f32.bin.gz"),
            &encode_layout(geo_map.nodes.len(), &geo_map.layout)?,
        )?,
    ])
}

fn encode_geo_edges(graph: &ProcessFlowGraph, geo_map: &GeoMapBuild) -> anyhow::Result<Vec<u8>> {
    let node_index_by_id = geo_map
        .nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (node.id.as_str(), u32::try_from(index)))
        .map(|(id, index)| index.map(|index| (id, index)))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let mut bytes = Vec::with_capacity(16 + geo_map.visible_edge_indices.len() * 52);
    bytes.extend_from_slice(EDGE_BINARY_MAGIC);
    push_u32(&mut bytes, BINARY_FORMAT_VERSION);
    push_u32(
        &mut bytes,
        u32::try_from(geo_map.visible_edge_indices.len())?,
    );

    for edge_index in &geo_map.visible_edge_indices {
        let edge = &graph.edges[*edge_index];
        let source_id = &graph.nodes[usize::try_from(edge.source_index)?].id;
        let target_id = &graph.nodes[usize::try_from(edge.target_index)?].id;
        let flow_id = &graph.nodes[usize::try_from(edge.flow_index)?].id;
        let process_id = &graph.nodes[usize::try_from(edge.process_index)?].id;
        let source_index = *node_index_by_id
            .get(source_id.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing geo source node: {source_id}"))?;
        let target_index = *node_index_by_id
            .get(target_id.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing geo target node: {target_id}"))?;
        let flow_index = *node_index_by_id
            .get(flow_id.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing geo flow node: {flow_id}"))?;
        let process_index = *node_index_by_id
            .get(process_id.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing geo process node: {process_id}"))?;

        push_u32(&mut bytes, source_index);
        push_u32(&mut bytes, target_index);
        push_u32(&mut bytes, flow_index);
        push_u32(&mut bytes, process_index);
        bytes.push(edge.direction.as_u8());
        bytes.push(u8::from(edge.quantitative_reference));
        push_u16(&mut bytes, 0);
        push_f64(&mut bytes, edge.mean_amount.unwrap_or(f64::NAN));
        push_f64(&mut bytes, edge.resulting_amount.unwrap_or(f64::NAN));
        push_u32(
            &mut bytes,
            edge.data_derivation_type_status_idx.unwrap_or(U32_NONE),
        );
        push_u32(&mut bytes, edge.exchange_location_idx.unwrap_or(U32_NONE));
        push_u32(&mut bytes, edge.unit_idx.unwrap_or(U32_NONE));
        push_u32(&mut bytes, edge.exchange_internal_id.unwrap_or(U32_NONE));
    }

    Ok(bytes)
}

fn encode_geo_adjacency(geo_map: &GeoMapBuild) -> anyhow::Result<Vec<u8>> {
    let visible_edge_ids = geo_map
        .visible_edge_indices
        .iter()
        .enumerate()
        .map(|(index, edge_index)| (format!("exchange:{edge_index}"), u32::try_from(index)))
        .map(|(id, index)| index.map(|index| (id, index)))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let mut offsets = Vec::with_capacity(geo_map.nodes.len() + 1);
    let mut edge_references = Vec::<u32>::new();
    offsets.push(0_u32);

    for node in &geo_map.nodes {
        for edge_id in geo_map
            .adjacency
            .get(&node.id)
            .into_iter()
            .flat_map(|edges| edges.iter())
        {
            if let Some(edge_index) = visible_edge_ids.get(edge_id) {
                edge_references.push(*edge_index);
            }
        }
        offsets.push(u32::try_from(edge_references.len())?);
    }

    let mut bytes = Vec::with_capacity(20 + (offsets.len() + edge_references.len()) * 4);
    bytes.extend_from_slice(CSR_BINARY_MAGIC);
    push_u32(&mut bytes, BINARY_FORMAT_VERSION);
    push_u32(&mut bytes, u32::try_from(geo_map.nodes.len())?);
    push_u32(&mut bytes, u32::try_from(edge_references.len())?);
    for value in offsets {
        push_u32(&mut bytes, value);
    }
    for value in edge_references {
        push_u32(&mut bytes, value);
    }

    Ok(bytes)
}

fn create_geo_map_builds(graph: &ProcessFlowGraph) -> anyhow::Result<Vec<GeoMapBuild>> {
    Ok(vec![
        create_geo_map_build(graph, GeoMapScope::World)?,
        create_geo_map_build(graph, GeoMapScope::China)?,
    ])
}

fn create_geo_map_build(
    graph: &ProcessFlowGraph,
    scope: GeoMapScope,
) -> anyhow::Result<GeoMapBuild> {
    let mut anchor_by_node_id = BTreeMap::<String, GeoAnchor>::new();
    let mut excluded_location_count = 0_usize;
    let mut excluded_location_examples = Vec::<String>::new();
    let mut region_keys = BTreeSet::<String>::new();
    let mut visible_node_indices = Vec::<usize>::new();

    for (index, node) in graph.nodes.iter().enumerate() {
        if let Some(anchor) = resolve_geo_anchor(node, scope) {
            region_keys.insert(anchor.key.clone());
            anchor_by_node_id.insert(node.id.clone(), anchor);
            visible_node_indices.push(index);
        } else {
            excluded_location_count += 1;
            record_excluded_location_example(node, &mut excluded_location_examples);
        }
    }

    let visible_node_ids = visible_node_indices
        .iter()
        .map(|index| graph.nodes[*index].id.clone())
        .collect::<BTreeSet<_>>();
    let nodes = visible_node_indices
        .iter()
        .map(|index| graph.nodes[*index].clone())
        .collect::<Vec<_>>();
    let visible_edge_indices = graph
        .edges
        .iter()
        .enumerate()
        .filter_map(|(index, edge)| {
            let source_id = &graph.nodes[usize::try_from(edge.source_index).ok()?].id;
            let target_id = &graph.nodes[usize::try_from(edge.target_index).ok()?].id;
            (visible_node_ids.contains(source_id) && visible_node_ids.contains(target_id))
                .then_some(index)
        })
        .collect::<Vec<_>>();
    let process_links = build_geo_process_links(graph, scope, &visible_node_ids);
    let adjacency = build_geo_adjacency(graph, &nodes, &visible_edge_indices, &process_links)?;
    let layout = create_geo_layout(&nodes, &anchor_by_node_id, scope);
    let search_flows = graph
        .search_flows
        .iter()
        .filter(|flow| visible_node_ids.contains(&flow.id))
        .cloned()
        .collect::<Vec<_>>();
    let edge_count = visible_edge_indices.len() + process_links.len();
    let stats = BuildStats {
        edge_count,
        flow_count: nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Flow)
            .count(),
        max_degree: nodes
            .iter()
            .map(|node| node.degree)
            .max()
            .unwrap_or_default(),
        process_count: nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Process)
            .count(),
    };
    let diagnostics = GeoMapDiagnostics {
        excluded_location_count,
        excluded_location_examples,
        location_rule_version: GEO_LOCATION_RULE_VERSION,
        region_count: region_keys.len(),
    };

    Ok(GeoMapBuild {
        adjacency,
        background: create_geo_background(scope),
        diagnostics,
        layout,
        nodes,
        process_links,
        scope,
        search_flows,
        stats,
        visible_edge_indices,
    })
}

fn record_excluded_location_example(node: &GraphNode, examples: &mut Vec<String>) {
    if examples.len() >= GEO_EXCLUDED_LOCATION_EXAMPLE_LIMIT {
        return;
    }

    let location = node
        .location
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("<missing>");

    if examples.iter().any(|example| example == location) {
        return;
    }

    examples.push(location.to_owned());
}

fn build_geo_adjacency(
    graph: &ProcessFlowGraph,
    nodes: &[GraphNode],
    visible_edge_indices: &[usize],
    process_links: &[ProcessLink],
) -> anyhow::Result<BTreeMap<String, Vec<String>>> {
    let mut adjacency = nodes
        .iter()
        .map(|node| (node.id.clone(), Vec::<String>::new()))
        .collect::<BTreeMap<_, _>>();

    for edge_index in visible_edge_indices {
        let edge = &graph.edges[*edge_index];
        let source_id = &graph.nodes[usize::try_from(edge.source_index)?].id;
        let target_id = &graph.nodes[usize::try_from(edge.target_index)?].id;
        let edge_id = format!("exchange:{edge_index}");
        adjacency
            .entry(source_id.clone())
            .or_default()
            .push(edge_id.clone());
        adjacency
            .entry(target_id.clone())
            .or_default()
            .push(edge_id);
    }

    for link in process_links {
        adjacency
            .entry(link.source.clone())
            .or_default()
            .push(link.id.clone());
        adjacency
            .entry(link.target.clone())
            .or_default()
            .push(link.id.clone());
    }

    for edge_ids in adjacency.values_mut() {
        edge_ids.sort();
        edge_ids.dedup();
    }

    Ok(adjacency)
}

fn create_geo_layout(
    nodes: &[GraphNode],
    anchor_by_node_id: &BTreeMap<String, GeoAnchor>,
    scope: GeoMapScope,
) -> Vec<[f32; 3]> {
    let mut node_ids_by_anchor = BTreeMap::<String, Vec<String>>::new();
    for node in nodes {
        if let Some(anchor) = anchor_by_node_id.get(&node.id) {
            node_ids_by_anchor
                .entry(anchor.key.clone())
                .or_default()
                .push(node.id.clone());
        }
    }

    for node_ids in node_ids_by_anchor.values_mut() {
        node_ids.sort();
    }

    let (frame_width, frame_height) = scope.frame();

    nodes
        .iter()
        .map(|node| {
            let Some(anchor) = anchor_by_node_id.get(&node.id) else {
                return [0.0, 0.0, get_geo_layout_z(node)];
            };
            let anchor_node_ids = node_ids_by_anchor
                .get(&anchor.key)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let anchor_count = anchor_node_ids.len().max(1);
            let rank = anchor_node_ids
                .binary_search(&node.id)
                .unwrap_or_default()
                .min(anchor_count - 1);
            let point = distribute_geo_anchor(
                anchor,
                &node.id,
                rank,
                anchor_count,
                frame_width,
                frame_height,
            );
            [point[0], point[1], get_geo_layout_z(node)]
        })
        .collect()
}

fn distribute_geo_anchor(
    anchor: &GeoAnchor,
    node_id: &str,
    index: usize,
    count: usize,
    frame_width: f32,
    frame_height: f32,
) -> [f32; 2] {
    if !anchor.samples.is_empty() {
        let sample_index = select_geo_anchor_sample(anchor, node_id, index, count);
        let [x, y] = anchor.samples[sample_index];
        return [
            x.clamp(-frame_width / 2.0, frame_width / 2.0),
            y.clamp(-frame_height / 2.0, frame_height / 2.0),
        ];
    }

    [anchor.x, anchor.y]
}

fn select_geo_anchor_sample(
    anchor: &GeoAnchor,
    node_id: &str,
    index: usize,
    count: usize,
) -> usize {
    let sample_count = anchor.samples.len();
    if sample_count <= 1 {
        return 0;
    }

    let hash_key = format!("{}:{node_id}", anchor.key);
    if count == 0 || count > sample_count {
        return (hash_unit(&hash_key, 3331) * sample_count as f32)
            .floor()
            .min((sample_count - 1) as f32) as usize;
    }

    let bucket_start = index.saturating_mul(sample_count) / count;
    let bucket_end = ((index + 1).saturating_mul(sample_count)).div_ceil(count);
    let bucket_end = bucket_end.clamp(bucket_start + 1, sample_count);
    let bucket_len = bucket_end - bucket_start;
    let bucket_offset = (hash_unit(&hash_key, 3337) * bucket_len as f32)
        .floor()
        .min((bucket_len - 1) as f32) as usize;

    bucket_start + bucket_offset
}

fn get_geo_layout_z(node: &GraphNode) -> f32 {
    if node.kind == NodeKind::Process {
        0.6
    } else {
        0.0
    }
}

fn build_geo_process_links(
    graph: &ProcessFlowGraph,
    scope: GeoMapScope,
    visible_node_ids: &BTreeSet<String>,
) -> Vec<ProcessLink> {
    let visible_process_ids = graph
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Process && visible_node_ids.contains(&node.id))
        .map(|node| node.id.clone())
        .collect::<BTreeSet<_>>();
    let mut edges_by_flow = BTreeMap::<u32, Vec<&GraphEdge>>::new();
    for edge in &graph.edges {
        edges_by_flow.entry(edge.flow_index).or_default().push(edge);
    }

    let mut links = Vec::<ProcessLink>::new();
    let mut seen = BTreeSet::<String>::new();
    for (flow_index, flow_edges) in edges_by_flow {
        let Ok(flow_node_index) = usize::try_from(flow_index) else {
            continue;
        };
        let Some(flow_node) = graph.nodes.get(flow_node_index) else {
            continue;
        };
        let mut providers = BTreeSet::<String>::new();
        let mut consumers = BTreeSet::<String>::new();

        for edge in flow_edges {
            let Ok(process_index) = usize::try_from(edge.process_index) else {
                continue;
            };
            let Some(process_node) = graph.nodes.get(process_index) else {
                continue;
            };
            if !visible_process_ids.contains(&process_node.id) {
                continue;
            }
            match edge.direction {
                ExchangeDirection::Input => {
                    consumers.insert(process_node.id.clone());
                }
                ExchangeDirection::Output => {
                    providers.insert(process_node.id.clone());
                }
            }
        }

        for source_process_id in &providers {
            for target_process_id in &consumers {
                if source_process_id == target_process_id {
                    continue;
                }
                let id = create_process_link_id(
                    scope,
                    &flow_node.id,
                    source_process_id,
                    target_process_id,
                );
                if !seen.insert(id.clone()) {
                    continue;
                }
                links.push(ProcessLink {
                    direction: ExchangeDirection::Output,
                    exchange_id: id.clone(),
                    flow_id: flow_node.id.clone(),
                    id,
                    process_id: target_process_id.clone(),
                    source: source_process_id.clone(),
                    target: target_process_id.clone(),
                });
            }
        }
    }

    links
}

fn create_process_link_id(
    scope: GeoMapScope,
    flow_id: &str,
    source_process_id: &str,
    target_process_id: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(scope.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(flow_id.as_bytes());
    hasher.update([0]);
    hasher.update(source_process_id.as_bytes());
    hasher.update([0]);
    hasher.update(target_process_id.as_bytes());
    let digest = hex::encode(hasher.finalize());

    format!("process-link:{}:{}", scope.as_str(), &digest[..20])
}

fn resolve_geo_anchor(node: &GraphNode, scope: GeoMapScope) -> Option<GeoAnchor> {
    let location = node.location.as_deref()?.trim().to_ascii_uppercase();
    if location.is_empty() || location == "NULL" || location == "-" {
        return None;
    }

    match scope {
        GeoMapScope::China => resolve_china_anchor(&location),
        GeoMapScope::World => resolve_world_anchor(&location),
    }
}

fn resolve_world_anchor(location: &str) -> Option<GeoAnchor> {
    let region_key = resolve_world_region_key(location)?;
    let shape = find_geo_region_shape(WORLD_REGION_SHAPES, &region_key)?;

    Some(GeoAnchor {
        key: format!("world:{}", shape.key),
        samples: shape.samples,
        x: shape.x,
        y: shape.y,
    })
}

fn resolve_china_anchor(location: &str) -> Option<GeoAnchor> {
    let province_code = resolve_china_province_code(location)?;
    let shape = find_geo_region_shape(CHINA_REGION_SHAPES, province_code)?;

    Some(GeoAnchor {
        key: format!("china:{}", shape.key),
        samples: shape.samples,
        x: shape.x,
        y: shape.y,
    })
}

fn resolve_world_region_key(location: &str) -> Option<String> {
    if location == "CN" || location.starts_with("CN-") {
        return Some("CN".to_owned());
    }

    let code = location.split('-').next().unwrap_or(location);
    if code.len() != 2 || !code.chars().all(|ch| ch.is_ascii_uppercase()) {
        return None;
    }
    if is_world_excluded_region_code(code) {
        return None;
    }

    let normalized_code = match code {
        "UK" => "GB",
        _ => code,
    };

    find_geo_region_shape(WORLD_REGION_SHAPES, normalized_code).map(|shape| shape.key.to_owned())
}

fn resolve_china_province_code(location: &str) -> Option<&str> {
    if !location.starts_with("CN-") {
        return None;
    }

    let province_code = location.split('-').nth(1)?;
    find_geo_region_shape(CHINA_REGION_SHAPES, province_code).map(|shape| shape.key)
}

fn find_geo_region_shape(
    shapes: &'static [GeoRegionShape],
    key: &str,
) -> Option<&'static GeoRegionShape> {
    shapes.iter().find(|shape| shape.key == key)
}

fn is_world_excluded_region_code(code: &str) -> bool {
    matches!(
        code,
        "AFR"
            | "CENTREL"
            | "CIS"
            | "CPA"
            | "EAS"
            | "EC-CC"
            | "EEU"
            | "EU+EFTA+UK"
            | "EU-15"
            | "EU-25"
            | "EU-25&CC"
            | "EU-25&CC&AC"
            | "EU-27"
            | "EU-AC"
            | "EU-NMC"
            | "FSU"
            | "GLO"
            | "MEA"
            | "NORDEL"
            | "OCE"
            | "PAO"
            | "PAS"
            | "RAF"
            | "RAM"
            | "RAS"
            | "RER"
            | "RLA"
            | "RME"
            | "RNA"
            | "RNE"
            | "SAS"
            | "UCTE"
            | "WEU"
    )
}

fn create_geo_background(scope: GeoMapScope) -> GeoMapBackground {
    let (width, height) = scope.frame();
    let label = match scope {
        GeoMapScope::China => "China map frame",
        GeoMapScope::World => "World map frame",
    };

    GeoMapBackground {
        height,
        paths: vec![GeoMapPath {
            code: None,
            id: format!("{}-frame", scope.as_str()),
            label: label.to_owned(),
            path: format!("M0 0H{width}V{height}H0Z"),
        }],
        scope,
        width,
    }
}

fn create_sphere_layout(nodes: &[GraphNode]) -> Vec<[f32; 3]> {
    let count = nodes.len().max(1) as f32;
    nodes
        .iter()
        .enumerate()
        .map(|(index, node)| {
            let i = index as f32;
            let z = 1.0 - (2.0 * (i + 0.5) / count);
            let radius = (1.0 - z * z).sqrt();
            let theta = i * GOLDEN_ANGLE + hash_unit(&node.id, 17) * 0.12;
            let node_radius = SPHERE_RADIUS
                + if node.kind == NodeKind::Process {
                    7.0
                } else {
                    0.0
                };
            [
                theta.cos() * radius * node_radius,
                theta.sin() * radius * node_radius,
                z * node_radius,
            ]
        })
        .collect()
}

fn create_expanded_layout(nodes: &[GraphNode], edges: &[GraphEdge]) -> Vec<[f32; 3]> {
    let (edges_by_flow, edges_by_process) = build_layout_edge_indexes(nodes.len(), edges);
    let topology_layout =
        create_topology_expanded_layout(nodes, edges, &edges_by_flow, &edges_by_process);
    let topology_bounds = summarize_layout(&topology_layout);
    let uniform_layout = create_uniform_silhouette_layout(&topology_layout, nodes);

    fit_layout_to_bounds(&uniform_layout, topology_bounds)
}

fn build_layout_edge_indexes(
    node_count: usize,
    edges: &[GraphEdge],
) -> (Vec<Vec<usize>>, Vec<Vec<usize>>) {
    let mut edges_by_flow = vec![Vec::<usize>::new(); node_count];
    let mut edges_by_process = vec![Vec::<usize>::new(); node_count];

    for (edge_index, edge) in edges.iter().enumerate() {
        if let Ok(flow_index) = usize::try_from(edge.flow_index)
            && flow_index < node_count
        {
            edges_by_flow[flow_index].push(edge_index);
        }
        if let Ok(process_index) = usize::try_from(edge.process_index)
            && process_index < node_count
        {
            edges_by_process[process_index].push(edge_index);
        }
    }

    (edges_by_flow, edges_by_process)
}

fn create_topology_expanded_layout(
    nodes: &[GraphNode],
    edges: &[GraphEdge],
    edges_by_flow: &[Vec<usize>],
    edges_by_process: &[Vec<usize>],
) -> Vec<[f32; 3]> {
    let mut positions = vec![[0.0_f32, 0.0_f32]; nodes.len()];
    let mut flow_indexes = Vec::<usize>::new();
    let mut process_indexes = Vec::<usize>::new();

    for (index, node) in nodes.iter().enumerate() {
        match node.kind {
            NodeKind::Flow => flow_indexes.push(index),
            NodeKind::Process => process_indexes.push(index),
        }
    }

    let anchor_indexes = create_topology_anchors(nodes, &flow_indexes);
    let anchor_set = anchor_indexes.iter().copied().collect::<BTreeSet<_>>();
    place_topology_anchors(&mut positions, nodes, &anchor_indexes);
    initialize_topology_processes(
        &anchor_set,
        edges,
        edges_by_process,
        nodes,
        &mut positions,
        &process_indexes,
    );
    initialize_topology_flows(
        &anchor_set,
        edges,
        edges_by_flow,
        nodes,
        &mut positions,
        &flow_indexes,
    );

    let mut working_positions = positions;
    let mut target_positions = working_positions.clone();

    for iteration in 0..EXPANDED_TOPOLOGY_ITERATIONS {
        target_positions.clone_from(&working_positions);
        relax_topology_processes(
            edges,
            edges_by_process,
            nodes,
            &working_positions,
            &process_indexes,
            &mut target_positions,
        );
        relax_topology_flows(
            &anchor_set,
            edges,
            edges_by_flow,
            &flow_indexes,
            nodes,
            &working_positions,
            &mut target_positions,
        );
        place_topology_anchors(&mut target_positions, nodes, &anchor_indexes);
        std::mem::swap(&mut working_positions, &mut target_positions);

        if iteration % 8 == 7 {
            apply_density_pressure(&anchor_set, iteration, nodes, &mut working_positions);
            place_topology_anchors(&mut working_positions, nodes, &anchor_indexes);
        }
    }

    normalize_topology_layout(&working_positions, nodes)
}

fn create_topology_anchors(nodes: &[GraphNode], flow_indexes: &[usize]) -> Vec<usize> {
    let mut anchor_indexes = flow_indexes.to_vec();
    anchor_indexes.sort_by(|left, right| {
        nodes[*right]
            .degree
            .cmp(&nodes[*left].degree)
            .then_with(|| nodes[*left].id.cmp(&nodes[*right].id))
    });
    anchor_indexes.truncate(EXPANDED_TOPOLOGY_ANCHOR_LIMIT.min(anchor_indexes.len()));
    anchor_indexes
}

fn place_topology_anchors(
    positions: &mut [[f32; 2]],
    nodes: &[GraphNode],
    anchor_indexes: &[usize],
) {
    let anchor_count = anchor_indexes.len().max(1) as f32;

    for (rank, node_index) in anchor_indexes.iter().copied().enumerate() {
        let node = &nodes[node_index];
        let rank_progress = ((rank as f32 + 0.5) / anchor_count).sqrt();
        let angle = rank as f32 * GOLDEN_ANGLE + (hash_unit(&node.id, 709) - 0.5) * 0.22;
        let radius_x = EXPANDED_TOPOLOGY_TARGET_WIDTH * (0.18 + rank_progress * 0.34);
        let radius_y = EXPANDED_TOPOLOGY_TARGET_HEIGHT * (0.18 + rank_progress * 0.34);
        positions[node_index] = [angle.cos() * radius_x, angle.sin() * radius_y];
    }
}

fn initialize_topology_processes(
    anchor_set: &BTreeSet<usize>,
    edges: &[GraphEdge],
    edges_by_process: &[Vec<usize>],
    nodes: &[GraphNode],
    positions: &mut [[f32; 2]],
    process_indexes: &[usize],
) {
    for process_index in process_indexes {
        let incident_edges = edges_by_process
            .get(*process_index)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut total_weight = 0.0_f32;
        let mut x = 0.0_f32;
        let mut y = 0.0_f32;

        for edge_index in incident_edges {
            let Some(flow_index) = layout_flow_index_from_edge(edges, *edge_index) else {
                continue;
            };
            if !anchor_set.contains(&flow_index) {
                continue;
            }
            let [anchor_x, anchor_y] = positions[flow_index];
            let weight = 1.2 + (get_node_degree(nodes, flow_index) as f32).ln_1p() * 0.12;
            x += anchor_x * weight;
            y += anchor_y * weight;
            total_weight += weight;
        }

        if total_weight <= 0.0 {
            positions[*process_index] = get_seed_position(&nodes[*process_index].id, 811);
            continue;
        }

        x /= total_weight;
        y /= total_weight;
        let orbit_angle = hash_unit(&nodes[*process_index].id, 821) * std::f32::consts::TAU;
        let orbit_radius = 24.0
            + (get_node_degree(nodes, *process_index) as f32)
                .sqrt()
                .mul_add(13.0, 0.0)
                .min(120.0)
            + hash_unit(&nodes[*process_index].id, 823) * 36.0;
        positions[*process_index] = [
            x + orbit_angle.cos() * orbit_radius,
            y + orbit_angle.sin() * orbit_radius * 0.72,
        ];
    }
}

fn initialize_topology_flows(
    anchor_set: &BTreeSet<usize>,
    edges: &[GraphEdge],
    edges_by_flow: &[Vec<usize>],
    nodes: &[GraphNode],
    positions: &mut [[f32; 2]],
    flow_indexes: &[usize],
) {
    for flow_index in flow_indexes {
        if anchor_set.contains(flow_index) {
            continue;
        }
        let incident_edges = edges_by_flow
            .get(*flow_index)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut total_weight = 0.0_f32;
        let mut x = 0.0_f32;
        let mut y = 0.0_f32;

        for edge_index in incident_edges {
            let Some(process_index) = layout_process_index_from_edge(edges, *edge_index) else {
                continue;
            };
            let [process_x, process_y] = positions[process_index];
            let weight = 1.0 / (get_node_degree(nodes, process_index) as f32).powf(0.28);
            x += process_x * weight;
            y += process_y * weight;
            total_weight += weight;
        }

        if total_weight <= 0.0 {
            positions[*flow_index] = get_seed_position(&nodes[*flow_index].id, 829);
            continue;
        }

        x /= total_weight;
        y /= total_weight;
        let orbit_angle = hash_unit(&nodes[*flow_index].id, 831) * std::f32::consts::TAU;
        let orbit_radius = 18.0
            + (get_node_degree(nodes, *flow_index) as f32)
                .sqrt()
                .mul_add(9.0, 0.0)
                .min(95.0)
            + hash_unit(&nodes[*flow_index].id, 833) * 26.0;
        positions[*flow_index] = [
            x + orbit_angle.cos() * orbit_radius,
            y + orbit_angle.sin() * orbit_radius * 0.76,
        ];
    }
}

fn relax_topology_processes(
    edges: &[GraphEdge],
    edges_by_process: &[Vec<usize>],
    nodes: &[GraphNode],
    positions: &[[f32; 2]],
    process_indexes: &[usize],
    target_positions: &mut [[f32; 2]],
) {
    for process_index in process_indexes {
        let incident_edges = edges_by_process
            .get(*process_index)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if incident_edges.is_empty() {
            continue;
        }
        let mut total_weight = 0.0_f32;
        let mut x = 0.0_f32;
        let mut y = 0.0_f32;

        for edge_index in incident_edges {
            let Some(flow_index) = layout_flow_index_from_edge(edges, *edge_index) else {
                continue;
            };
            let [flow_x, flow_y] = positions[flow_index];
            let weight = 1.0 / (get_node_degree(nodes, flow_index) as f32).powf(0.48);
            x += flow_x * weight;
            y += flow_y * weight;
            total_weight += weight;
        }

        if total_weight <= 0.0 {
            continue;
        }
        let blend = 0.34;
        target_positions[*process_index] = [
            positions[*process_index][0] * (1.0 - blend) + (x / total_weight) * blend,
            positions[*process_index][1] * (1.0 - blend) + (y / total_weight) * blend,
        ];
    }
}

fn relax_topology_flows(
    anchor_set: &BTreeSet<usize>,
    edges: &[GraphEdge],
    edges_by_flow: &[Vec<usize>],
    flow_indexes: &[usize],
    nodes: &[GraphNode],
    positions: &[[f32; 2]],
    target_positions: &mut [[f32; 2]],
) {
    for flow_index in flow_indexes {
        if anchor_set.contains(flow_index) {
            continue;
        }
        let incident_edges = edges_by_flow
            .get(*flow_index)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if incident_edges.is_empty() {
            continue;
        }
        let mut total_weight = 0.0_f32;
        let mut x = 0.0_f32;
        let mut y = 0.0_f32;

        for edge_index in incident_edges {
            let Some(process_index) = layout_process_index_from_edge(edges, *edge_index) else {
                continue;
            };
            let [process_x, process_y] = positions[process_index];
            let weight = 1.0 / (get_node_degree(nodes, process_index) as f32).powf(0.24);
            x += process_x * weight;
            y += process_y * weight;
            total_weight += weight;
        }

        if total_weight <= 0.0 {
            continue;
        }
        let degree = get_node_degree(nodes, *flow_index);
        let blend = if degree > 180 {
            0.08
        } else if degree > 72 {
            0.14
        } else {
            0.28
        };
        target_positions[*flow_index] = [
            positions[*flow_index][0] * (1.0 - blend) + (x / total_weight) * blend,
            positions[*flow_index][1] * (1.0 - blend) + (y / total_weight) * blend,
        ];
    }
}

fn apply_density_pressure(
    fixed_indexes: &BTreeSet<usize>,
    iteration: usize,
    nodes: &[GraphNode],
    positions: &mut [[f32; 2]],
) {
    let cell_size = 68.0_f32;
    let max_comfortable_count = 18_usize;
    let mut cells = BTreeMap::<(i32, i32), (usize, f32, f32)>::new();

    for [x, y] in positions.iter().copied() {
        let cell_x = (x / cell_size).floor() as i32;
        let cell_y = (y / cell_size).floor() as i32;
        let cell = cells.entry((cell_x, cell_y)).or_default();
        cell.0 += 1;
        cell.1 += x;
        cell.2 += y;
    }

    for cell in cells.values_mut() {
        let count = cell.0.max(1) as f32;
        cell.1 /= count;
        cell.2 /= count;
    }

    for (index, position) in positions.iter_mut().enumerate() {
        if fixed_indexes.contains(&index) {
            continue;
        }
        let cell_x = (position[0] / cell_size).floor() as i32;
        let cell_y = (position[1] / cell_size).floor() as i32;
        let Some((count, center_x, center_y)) = cells.get(&(cell_x, cell_y)).copied() else {
            continue;
        };
        if count <= max_comfortable_count {
            continue;
        }

        let mut dx = position[0] - center_x;
        let mut dy = position[1] - center_y;
        let length = dx.hypot(dy);
        if length < 0.001 {
            let angle =
                hash_unit(&format!("{}:{iteration}", nodes[index].id), 839) * std::f32::consts::TAU;
            dx = angle.cos();
            dy = angle.sin();
        } else {
            dx /= length;
            dy /= length;
        }

        let pressure =
            ((count - max_comfortable_count) as f32 / max_comfortable_count as f32).clamp(0.0, 2.4);
        let node_scale = if nodes[index].kind == NodeKind::Process {
            12.0
        } else {
            9.0
        };
        position[0] += dx * pressure * node_scale;
        position[1] += dy * pressure * node_scale;
    }
}

fn normalize_topology_layout(positions: &[[f32; 2]], nodes: &[GraphNode]) -> Vec<[f32; 3]> {
    let mut min_x = f32::INFINITY;
    let mut max_x = f32::NEG_INFINITY;
    let mut min_y = f32::INFINITY;
    let mut max_y = f32::NEG_INFINITY;

    for [x, y] in positions {
        min_x = min_x.min(*x);
        max_x = max_x.max(*x);
        min_y = min_y.min(*y);
        max_y = max_y.max(*y);
    }

    let width = (max_x - min_x).max(1.0);
    let height = (max_y - min_y).max(1.0);
    let center_x = f32::midpoint(min_x, max_x);
    let center_y = f32::midpoint(min_y, max_y);
    let scale =
        (EXPANDED_TOPOLOGY_TARGET_WIDTH / width).min(EXPANDED_TOPOLOGY_TARGET_HEIGHT / height);

    positions
        .iter()
        .zip(nodes)
        .map(|([x, y], node)| {
            [
                (*x - center_x) * scale,
                (*y - center_y) * scale,
                get_expanded_layout_z(node),
            ]
        })
        .collect()
}

fn create_uniform_silhouette_layout(
    base_layout: &[[f32; 3]],
    nodes: &[GraphNode],
) -> Vec<[f32; 3]> {
    let [center_x, center_y] = get_layout_center2(base_layout);
    let outline_radii = build_smoothed_outline_radii(base_layout);
    let node_count = nodes.len().max(1) as f32;
    let mut target_points = (0..nodes.len())
        .map(|rank| {
            let sample = rank as f32 + 0.5;
            let angle = sample * GOLDEN_ANGLE;
            let radial_progress = (sample / node_count).sqrt();
            let outline_radius = get_outline_radius_at(&outline_radii, angle);
            let radius = outline_radius * radial_progress.clamp(0.006, 0.996);

            [
                center_x + angle.cos() * radius,
                center_y + angle.sin() * radius,
            ]
        })
        .collect::<Vec<_>>();
    let target_bounds = summarize_points2(&target_points);
    target_points.sort_by(|left, right| {
        spatial_sort_key(*left, target_bounds)
            .cmp(&spatial_sort_key(*right, target_bounds))
            .then_with(|| left[0].total_cmp(&right[0]))
            .then_with(|| left[1].total_cmp(&right[1]))
    });

    let mut ordered_nodes = nodes.iter().enumerate().collect::<Vec<_>>();
    ordered_nodes.sort_by(|(left_index, left), (right_index, right)| {
        left.cluster_id_level3
            .cmp(&right.cluster_id_level3)
            .then_with(|| right.degree.cmp(&left.degree))
            .then_with(|| node_kind_order(left.kind).cmp(&node_kind_order(right.kind)))
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| nodes[*left_index].id.cmp(&nodes[*right_index].id))
    });

    let mut layout = vec![[0.0_f32, 0.0_f32, 0.0_f32]; nodes.len()];
    for (rank, (node_index, node)) in ordered_nodes.into_iter().enumerate() {
        let [x, y] = target_points
            .get(rank)
            .copied()
            .unwrap_or([center_x, center_y]);
        let jitter_angle = hash_unit(&node.id, 1211) * std::f32::consts::TAU;
        let jitter_radius = (hash_unit(&node.id, 1217) - 0.5) * 1.8;

        layout[node_index] = [
            x + jitter_angle.cos() * jitter_radius,
            y + jitter_angle.sin() * jitter_radius,
            get_expanded_layout_z(node),
        ];
    }

    layout
}

fn node_kind_order(kind: NodeKind) -> u8 {
    match kind {
        NodeKind::Process => 0,
        NodeKind::Flow => 1,
    }
}

fn summarize_points2(points: &[[f32; 2]]) -> LayoutBounds {
    let mut min_x = f32::INFINITY;
    let mut max_x = f32::NEG_INFINITY;
    let mut min_y = f32::INFINITY;
    let mut max_y = f32::NEG_INFINITY;

    for [x, y] in points {
        min_x = min_x.min(*x);
        max_x = max_x.max(*x);
        min_y = min_y.min(*y);
        max_y = max_y.max(*y);
    }

    LayoutBounds {
        height: (max_y - min_y).max(1.0),
        max_x,
        max_y,
        min_x,
        min_y,
        width: (max_x - min_x).max(1.0),
    }
}

fn spatial_sort_key([x, y]: [f32; 2], bounds: LayoutBounds) -> u64 {
    let nx = ((x - bounds.min_x) / bounds.width.max(1.0)).clamp(0.0, 1.0);
    let ny = ((y - bounds.min_y) / bounds.height.max(1.0)).clamp(0.0, 1.0);
    let gx = (nx * 1023.0).round() as u32;
    let gy = (ny * 1023.0).round() as u32;

    morton2(gx, gy)
}

fn morton2(x: u32, y: u32) -> u64 {
    let mut key = 0_u64;
    for bit in 0..10 {
        key |= u64::from((x >> bit) & 1) << (bit * 2);
        key |= u64::from((y >> bit) & 1) << (bit * 2 + 1);
    }
    key
}

fn build_smoothed_outline_radii(layout: &[[f32; 3]]) -> Vec<f32> {
    let [center_x, center_y] = get_layout_center2(layout);
    let mut bins = vec![Vec::<f32>::new(); EXPANDED_UNIFORM_OUTLINE_BINS];
    let mut max_radius = 1.0_f32;

    for [x, y, _] in layout {
        let dx = *x - center_x;
        let dy = *y - center_y;
        let radius = dx.hypot(dy);
        let angle = dy.atan2(dx);
        let bin_index = ((((angle + std::f32::consts::TAU) % std::f32::consts::TAU)
            / std::f32::consts::TAU)
            * EXPANDED_UNIFORM_OUTLINE_BINS as f32)
            .floor() as usize
            % EXPANDED_UNIFORM_OUTLINE_BINS;
        bins[bin_index].push(radius);
        max_radius = max_radius.max(radius);
    }

    let mut raw_radii = bins
        .iter()
        .map(|values| quantile(values, EXPANDED_UNIFORM_OUTLINE_QUANTILE))
        .collect::<Vec<_>>();

    for index in 0..raw_radii.len() {
        if raw_radii[index] > 0.0 {
            continue;
        }
        let mut radius = 0.0_f32;
        for distance in 1..raw_radii.len() {
            let left = raw_radii[(index + raw_radii.len() - distance) % raw_radii.len()];
            let right = raw_radii[(index + distance) % raw_radii.len()];
            if left > 0.0 || right > 0.0 {
                radius = left.max(right);
                break;
            }
        }
        raw_radii[index] = if radius > 0.0 { radius } else { max_radius };
    }

    (0..raw_radii.len())
        .map(|index| {
            let mut weighted_radius = 0.0_f32;
            let mut total_weight = 0.0_f32;
            for delta in [-3_i8, -2, -1, 0, 1, 2, 3] {
                let neighbor_index = circular_index(index, raw_radii.len(), delta);
                let weight = match delta.abs() {
                    0 => 4.0,
                    1 => 3.0,
                    2 => 2.0,
                    _ => 1.0,
                };
                weighted_radius += raw_radii[neighbor_index] * weight;
                total_weight += weight;
            }
            weighted_radius / total_weight
        })
        .collect()
}

fn fit_layout_to_bounds(layout: &[[f32; 3]], target_bounds: LayoutBounds) -> Vec<[f32; 3]> {
    let source_bounds = summarize_layout(layout);
    let source_center_x = f32::midpoint(source_bounds.min_x, source_bounds.max_x);
    let source_center_y = f32::midpoint(source_bounds.min_y, source_bounds.max_y);
    let target_center_x = f32::midpoint(target_bounds.min_x, target_bounds.max_x);
    let target_center_y = f32::midpoint(target_bounds.min_y, target_bounds.max_y);
    let scale_x = target_bounds.width / source_bounds.width.max(1.0);
    let scale_y = target_bounds.height / source_bounds.height.max(1.0);

    layout
        .iter()
        .map(|[x, y, z]| {
            [
                target_center_x + (*x - source_center_x) * scale_x,
                target_center_y + (*y - source_center_y) * scale_y,
                *z,
            ]
        })
        .collect()
}

fn summarize_layout(layout: &[[f32; 3]]) -> LayoutBounds {
    let mut min_x = f32::INFINITY;
    let mut max_x = f32::NEG_INFINITY;
    let mut min_y = f32::INFINITY;
    let mut max_y = f32::NEG_INFINITY;

    for [x, y, _] in layout {
        min_x = min_x.min(*x);
        max_x = max_x.max(*x);
        min_y = min_y.min(*y);
        max_y = max_y.max(*y);
    }

    LayoutBounds {
        height: (max_y - min_y).max(1.0),
        max_x,
        max_y,
        min_x,
        min_y,
        width: (max_x - min_x).max(1.0),
    }
}

fn get_layout_center2(layout: &[[f32; 3]]) -> [f32; 2] {
    if layout.is_empty() {
        return [0.0, 0.0];
    }

    let mut x = 0.0_f32;
    let mut y = 0.0_f32;
    for [node_x, node_y, _] in layout {
        x += *node_x;
        y += *node_y;
    }

    [x / layout.len() as f32, y / layout.len() as f32]
}

fn get_outline_radius_at(outline_radii: &[f32], angle: f32) -> f32 {
    let bin_count = outline_radii.len().max(1);
    let normalized_angle = (angle + std::f32::consts::TAU) % std::f32::consts::TAU;
    let position = (normalized_angle / std::f32::consts::TAU) * bin_count as f32;
    let left_index = position.floor() as usize % bin_count;
    let right_index = (left_index + 1) % bin_count;
    let progress = position - position.floor();

    outline_radii[left_index] * (1.0 - progress) + outline_radii[right_index] * progress
}

fn quantile(values: &[f32], ratio: f32) -> f32 {
    if values.is_empty() {
        return 0.0;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(f32::total_cmp);
    let index = (((sorted.len() - 1) as f32) * ratio).floor() as usize;

    sorted[index.min(sorted.len() - 1)]
}

fn circular_index(index: usize, len: usize, delta: i8) -> usize {
    debug_assert!(len > 0);
    match delta {
        -3 => (index + len - (3 % len)) % len,
        -2 => (index + len - (2 % len)) % len,
        -1 => (index + len - 1) % len,
        0 => index % len,
        1 => (index + 1) % len,
        2 => (index + 2) % len,
        3 => (index + 3) % len,
        _ => unreachable!("outline smoothing only requests neighbors from -3 to 3"),
    }
}

fn get_seed_position(id: &str, salt: u64) -> [f32; 2] {
    let angle = hash_unit(id, salt) * std::f32::consts::TAU;
    let radius = hash_unit(id, salt + 1).sqrt();

    [
        angle.cos() * radius * EXPANDED_TOPOLOGY_TARGET_WIDTH * 0.36,
        angle.sin() * radius * EXPANDED_TOPOLOGY_TARGET_HEIGHT * 0.36,
    ]
}

fn get_node_degree(nodes: &[GraphNode], index: usize) -> u32 {
    nodes.get(index).map_or(1, |node| node.degree.max(1))
}

fn get_expanded_layout_z(node: &GraphNode) -> f32 {
    let degree = node.degree.max(1) as f32;
    if node.kind == NodeKind::Process {
        26.0 + (degree.ln_1p() * 1.6).clamp(0.0, 14.0)
    } else {
        9.0 + (degree.ln_1p() * 1.1).clamp(0.0, 10.0)
    }
}

fn layout_flow_index_from_edge(edges: &[GraphEdge], edge_index: usize) -> Option<usize> {
    edges
        .get(edge_index)
        .and_then(|edge| usize::try_from(edge.flow_index).ok())
}

fn layout_process_index_from_edge(edges: &[GraphEdge], edge_index: usize) -> Option<usize> {
    edges
        .get(edge_index)
        .and_then(|edge| usize::try_from(edge.process_index).ok())
}

fn hash_unit(value: &str, salt: u64) -> f32 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ salt;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let lower = (hash & 0xffff_ffff) as u32;
    lower as f32 / u32::MAX as f32
}

fn normalize_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_owned()
}

fn flow_version_key(flow_id: &str, flow_version: &str) -> String {
    format!("{flow_id}:{flow_version}")
}

fn flow_node_id(flow: &FlowMetadata) -> String {
    format!("flow:{}@{}", flow.id, flow.version)
}

fn process_node_id(process: &ProcessMetadata) -> String {
    format!("process:{}@{}", process.id, process.version)
}

fn parse_flow_row(
    row: &DatasetRow,
    classification_catalog: &ClassificationCatalog,
) -> Option<FlowMetadata> {
    let root = preferred_json(row);
    let data_set =
        pick_record(root, &["flowDataSet"]).or_else(|| pick_record(root, &["flow_data_set"]))?;
    let data_set_info = pick_record(data_set, &["flowInformation", "dataSetInformation"]);
    let flow_type = pick_value(
        data_set,
        &["modellingAndValidation", "LCIMethod", "typeOfDataSet"],
    )
    .or_else(|| {
        pick_value(
            data_set,
            &[
                "modellingAndValidation",
                "LCIMethodAndAllocation",
                "typeOfDataSet",
            ],
        )
    })
    .and_then(normalize_value)?;

    if flow_type == BASIC_FLOW_TYPE {
        return None;
    }

    let classification = data_set_info
        .and_then(|info| extract_classification(info, "Flow", classification_catalog))
        .unwrap_or_else(|| ClassificationMetadata::fallback(&flow_type));
    let classification_levels = classification_levels(&classification);

    Some(FlowMetadata {
        category: classification.category.clone(),
        category_display_path: classification.category_display_path.clone(),
        category_path: classification.category_path.clone(),
        category_system: classification.category_system.clone(),
        cluster_id_level1: cluster_id_from_category_level(&classification_levels, 1),
        cluster_id_level3: cluster_id_from_category_level(&classification_levels, 3),
        cluster_label_level1: cluster_label_from_category_level(&classification_levels, 1),
        cluster_label_level3: cluster_label_from_category_level(&classification_levels, 3),
        flow_type,
        id: row.id.clone(),
        location: None,
        name: data_set_info
            .and_then(|info| pick_value(info, &["name", "baseName"]))
            .and_then(localized_text)
            .unwrap_or_else(|| row.id.clone()),
        version: extract_data_set_version(data_set, &row.version),
    })
}

fn parse_process_metadata(
    row: &DatasetRow,
    classification_catalog: &ClassificationCatalog,
) -> Option<ProcessMetadata> {
    let root = preferred_json(row);
    let data_set = pick_record(root, &["processDataSet"])
        .or_else(|| pick_record(root, &["process_data_set"]))?;
    let process_info = pick_record(data_set, &["processInformation"]);
    let data_set_info = process_info.and_then(|info| pick_record(info, &["dataSetInformation"]));
    let reference_flow = process_info
        .and_then(|info| pick_record(info, &["quantitativeReference", "referenceToReferenceFlow"]));
    let classification = data_set_info
        .and_then(|info| extract_classification(info, "Process", classification_catalog))
        .unwrap_or_else(|| ClassificationMetadata::fallback("process"));
    let classification_levels = classification_levels(&classification);

    Some(ProcessMetadata {
        category: classification.category.clone(),
        category_display_path: classification.category_display_path.clone(),
        category_path: classification.category_path.clone(),
        category_system: classification.category_system.clone(),
        cluster_id_level1: cluster_id_from_category_level(&classification_levels, 1),
        cluster_id_level3: cluster_id_from_category_level(&classification_levels, 3),
        cluster_label_level1: cluster_label_from_category_level(&classification_levels, 1),
        cluster_label_level3: cluster_label_from_category_level(&classification_levels, 3),
        id: row.id.clone(),
        location: process_info
            .and_then(|info| {
                pick_value(
                    info,
                    &[
                        "geography",
                        "locationOfOperationSupplyOrProduction",
                        "@location",
                    ],
                )
            })
            .and_then(normalize_value)
            .or_else(|| {
                process_info
                    .and_then(|info| {
                        pick_value(
                            info,
                            &[
                                "geography",
                                "locationOfOperationSupplyOrProduction",
                                "descriptionOfRestrictions",
                            ],
                        )
                    })
                    .and_then(localized_text)
            }),
        name: data_set_info
            .and_then(|info| pick_value(info, &["name", "baseName"]))
            .and_then(localized_text)
            .unwrap_or_else(|| row.id.clone()),
        reference_exchange_internal_id: reference_flow.and_then(normalize_u32),
        reference_flow_id: reference_flow
            .and_then(|value| value.get("@refObjectId"))
            .and_then(normalize_value),
        reference_year: process_info
            .and_then(|info| pick_value(info, &["time", "common:referenceYear"]))
            .and_then(normalize_value),
        type_of_data_set: pick_value(
            data_set,
            &["modellingAndValidation", "LCIMethod", "typeOfDataSet"],
        )
        .or_else(|| {
            pick_value(
                data_set,
                &[
                    "modellingAndValidation",
                    "LCIMethodAndAllocation",
                    "typeOfDataSet",
                ],
            )
        })
        .and_then(normalize_value),
        version: extract_data_set_version(data_set, &row.version),
    })
}

fn parse_process_exchanges(row: &DatasetRow, process: &ProcessMetadata) -> Vec<ProcessExchange> {
    let Some(data_set) = pick_record(preferred_json(row), &["processDataSet"])
        .or_else(|| pick_record(preferred_json(row), &["process_data_set"]))
    else {
        return Vec::new();
    };
    let Some(exchanges) = pick_value(data_set, &["exchanges", "exchange"]) else {
        return Vec::new();
    };

    as_array(exchanges)
        .filter_map(|exchange| {
            let reference = exchange.get("referenceToFlowDataSet")?;
            let flow_id = reference.get("@refObjectId").and_then(normalize_value)?;
            let exchange_internal_id = exchange.get("@dataSetInternalID").and_then(normalize_u32);
            let quantitative_reference = exchange
                .get("quantitativeReference")
                .and_then(normalize_value)
                .is_some_and(|value| value.eq_ignore_ascii_case("true"))
                || process.reference_flow_id.as_deref() == Some(flow_id.as_str())
                || process.reference_exchange_internal_id == exchange_internal_id;
            Some(ProcessExchange {
                data_derivation_type_status: exchange
                    .get("dataDerivationTypeStatus")
                    .and_then(normalize_value),
                exchange_direction: normalize_exchange_direction(
                    exchange.get("exchangeDirection"),
                    quantitative_reference,
                ),
                exchange_internal_id,
                exchange_location: exchange.get("location").and_then(normalize_value),
                flow_id,
                flow_version: reference.get("@version").and_then(normalize_value),
                mean_amount: exchange
                    .get("meanAmount")
                    .or_else(|| exchange.get("meanValue"))
                    .and_then(normalize_f64),
                quantitative_reference,
                resulting_amount: exchange.get("resultingAmount").and_then(normalize_f64),
                unit: reference
                    .get("common:shortDescription")
                    .or_else(|| reference.get("shortDescription"))
                    .and_then(localized_text)
                    .and_then(|text| extract_unit_hint(&text)),
            })
        })
        .collect()
}

fn preferred_json(row: &DatasetRow) -> &Value {
    &row.json
}

fn pick_record<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    pick_value(value, keys).filter(|item| item.is_object())
}

fn pick_value<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for key in keys {
        current = current.get(*key)?;
    }
    Some(current)
}

fn as_array(value: &Value) -> Box<dyn Iterator<Item = &Value> + '_> {
    match value {
        Value::Array(items) => Box::new(items.iter()),
        Value::Null => Box::new(std::iter::empty()),
        other => Box::new(std::iter::once(other)),
    }
}

fn normalize_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        }
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn normalize_u32(value: &Value) -> Option<u32> {
    match value {
        Value::Number(number) => number.as_u64().and_then(|item| u32::try_from(item).ok()),
        Value::String(text) => text.trim().parse::<u32>().ok(),
        _ => None,
    }
}

fn normalize_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn localized_text(value: &Value) -> Option<String> {
    if let Some(direct) = normalize_value(value) {
        return Some(direct);
    }
    if let Some(text) = value.get("#text").and_then(normalize_value) {
        return Some(text);
    }

    let items = match value {
        Value::Array(items) => items.as_slice(),
        _ => return None,
    };
    items
        .iter()
        .find(|item| item.get("@xml:lang").and_then(normalize_value).as_deref() == Some("zh"))
        .or_else(|| {
            items.iter().find(|item| {
                item.get("@xml:lang").and_then(normalize_value).as_deref() == Some("en")
            })
        })
        .or_else(|| items.first())
        .and_then(|item| item.get("#text"))
        .and_then(normalize_value)
}

fn localized_text_for_lang(value: &Value, lang: &str) -> Option<String> {
    if value
        .get("@xml:lang")
        .and_then(normalize_value)
        .is_some_and(|value_lang| value_lang.eq_ignore_ascii_case(lang))
    {
        return value
            .get("#text")
            .and_then(normalize_value)
            .or_else(|| normalize_value(value));
    }

    let items = match value {
        Value::Array(items) => items.as_slice(),
        _ => return None,
    };
    items
        .iter()
        .find(|item| {
            item.get("@xml:lang")
                .and_then(normalize_value)
                .is_some_and(|value_lang| value_lang.eq_ignore_ascii_case(lang))
        })
        .and_then(|item| {
            item.get("#text")
                .and_then(normalize_value)
                .or_else(|| normalize_value(item))
        })
}

fn extract_classification(
    info: &Value,
    expected_data_type: &str,
    classification_catalog: &ClassificationCatalog,
) -> Option<ClassificationMetadata> {
    let raw_classification = extract_raw_classification(info)?;
    classification_catalog
        .resolve(&raw_classification, expected_data_type)
        .or_else(|| {
            let category_path = raw_classification
                .category_path
                .iter()
                .enumerate()
                .map(|(index, item)| CategoryPathItem {
                    id: item
                        .id
                        .clone()
                        .unwrap_or_else(|| slug_from_text(&item.name)),
                    level: if item.level == 0 {
                        u32::try_from(index + 1).unwrap_or(u32::MAX)
                    } else {
                        item.level
                    },
                    name: item.name.clone(),
                    zh_name: item.zh_name.clone().unwrap_or_else(|| item.name.clone()),
                })
                .collect::<Vec<_>>();
            (!category_path.is_empty()).then(|| {
                ClassificationMetadata::from_category_path(
                    raw_classification
                        .category_system
                        .as_deref()
                        .and_then(canonical_category_system)
                        .map(str::to_owned),
                    category_path,
                )
            })
        })
}

fn extract_raw_classification(info: &Value) -> Option<RawClassification> {
    let classification_info = info.get("classificationInformation")?;
    let (classification_container, classification_items) =
        pick_classification_items(classification_info)?;
    let category_path = as_array(classification_items)
        .enumerate()
        .filter_map(|(index, item)| extract_raw_classification_item(item, index))
        .collect::<Vec<_>>();

    (!category_path.is_empty()).then(|| RawClassification {
        category_path,
        category_system: extract_classification_system(classification_container),
    })
}

fn pick_classification_items(classification_info: &Value) -> Option<(&Value, &Value)> {
    let classification_container = pick_record(classification_info, &["common:classification"])
        .or_else(|| pick_record(classification_info, &["classification"]));
    if let Some(classification_container) = classification_container
        && let Some(classification_items) = classification_container
            .get("common:class")
            .or_else(|| classification_container.get("class"))
    {
        return Some((classification_container, classification_items));
    }

    let elementary_container = pick_record(
        classification_info,
        &["common:elementaryFlowCategorization"],
    )
    .or_else(|| pick_record(classification_info, &["elementaryFlowCategorization"]));
    if let Some(elementary_container) = elementary_container
        && let Some(classification_items) = elementary_container
            .get("common:category")
            .or_else(|| elementary_container.get("category"))
    {
        return Some((elementary_container, classification_items));
    }

    None
}

fn extract_classification_system(classification_container: &Value) -> Option<String> {
    [
        "@name",
        "@classes",
        "@classificationSystem",
        "classificationSystem",
        "common:classificationSystem",
    ]
    .into_iter()
    .find_map(|key| classification_container.get(key).and_then(normalize_value))
}

fn extract_raw_classification_item(value: &Value, index: usize) -> Option<RawClassificationItem> {
    let name = localized_text_for_lang(value, "en").or_else(|| localized_text(value))?;
    let zh_name = localized_text_for_lang(value, "zh");
    let id = [
        "@classId",
        "@id",
        "@categoryId",
        "@catId",
        "@code",
        "classId",
        "id",
    ]
    .into_iter()
    .find_map(|key| value.get(key).and_then(normalize_value));
    let level = ["@level", "level", "@categoryLevel", "categoryLevel"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(normalize_u32))
        .unwrap_or_else(|| u32::try_from(index + 1).unwrap_or(u32::MAX));

    Some(RawClassificationItem {
        id,
        level,
        name,
        zh_name,
    })
}

fn extract_data_set_version(data_set: &Value, fallback: &str) -> String {
    pick_value(
        data_set,
        &[
            "administrativeInformation",
            "publicationAndOwnership",
            "common:dataSetVersion",
        ],
    )
    .and_then(normalize_value)
    .unwrap_or_else(|| fallback.to_owned())
}

fn normalize_exchange_direction(
    value: Option<&Value>,
    quantitative_reference: bool,
) -> ExchangeDirection {
    let raw = value.and_then(normalize_value).unwrap_or_default();
    let lower = raw.to_ascii_lowercase();
    if lower.contains("output") {
        ExchangeDirection::Output
    } else if lower.contains("input") {
        ExchangeDirection::Input
    } else if quantitative_reference {
        ExchangeDirection::Output
    } else {
        ExchangeDirection::Input
    }
}

fn classification_levels(classification: &ClassificationMetadata) -> Vec<String> {
    let mut levels = classification
        .category_path
        .iter()
        .map(|item| item.name.trim())
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();

    if levels.is_empty() {
        levels = classification
            .category
            .split(" / ")
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
    }

    if levels.is_empty() {
        vec!["uncategorized".to_owned()]
    } else {
        levels
    }
}

fn cluster_label_from_category_level(levels: &[String], level: usize) -> String {
    let take_count = level.clamp(1, levels.len());
    levels
        .iter()
        .take(take_count)
        .cloned()
        .collect::<Vec<_>>()
        .join(" / ")
}

fn cluster_id_from_category_level(levels: &[String], level: usize) -> String {
    slug_from_text(&cluster_label_from_category_level(levels, level))
}

fn slug_from_text(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase();
    let mut slug = String::new();
    for ch in normalized.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug = slug.trim_matches('-').to_owned();
    if slug.is_empty() {
        "uncategorized".to_owned()
    } else {
        slug
    }
}

fn extract_unit_hint(text: &str) -> Option<String> {
    let start = text.find('(')?;
    let end = text[start + 1..].find(')')? + start + 1;
    let segment = &text[start + 1..end];
    segment
        .split(',')
        .nth(1)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use flate2::{Compression, read::GzDecoder, write::GzEncoder};
    use serde_json::json;
    use std::{
        collections::BTreeSet,
        fs,
        io::{Read, Write},
        path::Path,
    };

    use super::DatasetRow;
    use super::{
        CHINA_REGION_SHAPES, CategoryPathItem, ClassificationCatalog, ClassificationCatalogBuilder,
        Cli, ExchangeDirection, GEO_LOCATION_RULE_VERSION, GeoMapScope, WORLD_REGION_SHAPES,
        build_graph, cluster_payload, encoded_gzip_json, find_geo_region_shape,
        normalize_exchange_direction, parse_flow_row, resolve_china_province_code,
        resolve_world_region_key,
    };

    fn test_cli() -> Cli {
        Cli {
            database_url: None,
            conn: None,
            s3_endpoint: None,
            s3_region: None,
            s3_bucket: None,
            cache_bucket: Some("bucket".to_owned()),
            s3_access_key_id: None,
            s3_access_key: None,
            s3_secret_access_key: None,
            s3_secret_key: None,
            s3_session_token: None,
            cache_prefix: "national-carbon/process-flow-graph/v1".to_owned(),
            build_id: Some("test".to_owned()),
            limit_flows: None,
            limit_processes: None,
            max_edges: None,
            page_size: 500,
            source_row_limit: None,
            classification_dir: None,
            execute: false,
        }
    }

    fn flow_row(id: &str, name: &str, flow_type: &str) -> DatasetRow {
        DatasetRow {
            id: id.to_owned(),
            version: "01.00.000".to_owned(),
            modified_at: None,
            json: json!({
                "flowDataSet": {
                    "flowInformation": {
                        "dataSetInformation": {
                            "name": {"baseName": [{"@xml:lang": "zh", "#text": name}]}
                        }
                    },
                    "modellingAndValidation": {
                        "LCIMethod": {"typeOfDataSet": flow_type}
                    },
                    "administrativeInformation": {
                        "publicationAndOwnership": {"common:dataSetVersion": "01.00.000"}
                    }
                }
            }),
        }
    }

    fn flow_row_with_category(
        id: &str,
        name: &str,
        flow_type: &str,
        categories: &[&str],
    ) -> DatasetRow {
        let mut row = flow_row(id, name, flow_type);
        row.json["flowDataSet"]["flowInformation"]["dataSetInformation"]["classificationInformation"] = json!({
            "common:classification": {
                "common:class": categories
                    .iter()
                    .map(|category| json!({"@xml:lang": "en", "#text": category}))
                    .collect::<Vec<_>>()
            }
        });
        row
    }

    fn test_classification_catalog() -> ClassificationCatalog {
        let mut builder = ClassificationCatalogBuilder::default();
        let english_path = vec![
            CategoryPathItem {
                id: "0".to_owned(),
                level: 1,
                name: "Ores and minerals".to_owned(),
                zh_name: String::new(),
            },
            CategoryPathItem {
                id: "04".to_owned(),
                level: 2,
                name: "Stone, sand and clay".to_owned(),
                zh_name: String::new(),
            },
            CategoryPathItem {
                id: "041".to_owned(),
                level: 3,
                name: "Limestone".to_owned(),
                zh_name: String::new(),
            },
        ];
        let chinese_path = vec![
            CategoryPathItem {
                id: "0".to_owned(),
                level: 1,
                name: "矿石与矿物".to_owned(),
                zh_name: String::new(),
            },
            CategoryPathItem {
                id: "04".to_owned(),
                level: 2,
                name: "石料、砂和黏土".to_owned(),
                zh_name: String::new(),
            },
            CategoryPathItem {
                id: "041".to_owned(),
                level: 3,
                name: "石灰石".to_owned(),
                zh_name: String::new(),
            },
        ];
        builder.add_path("CPC", "Flow", false, &english_path);
        builder.add_path("CPC", "Flow", true, &chinese_path);
        builder.finish()
    }

    fn write_gzip_json(path: &Path, value: &serde_json::Value) {
        let file = fs::File::create(path).expect("create gz json");
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder
            .write_all(
                serde_json::to_string(value)
                    .expect("serialize json")
                    .as_bytes(),
            )
            .expect("write gz json");
        encoder.finish().expect("finish gz json");
    }

    fn process_row() -> DatasetRow {
        DatasetRow {
            id: "process-a".to_owned(),
            version: "01.00.000".to_owned(),
            modified_at: None,
            json: json!({
                "processDataSet": {
                    "processInformation": {
                        "dataSetInformation": {
                            "name": {"baseName": [{"@xml:lang": "zh", "#text": "过程 A"}]}
                        },
                        "quantitativeReference": {
                            "referenceToReferenceFlow": "1"
                        }
                    },
                    "administrativeInformation": {
                        "publicationAndOwnership": {"common:dataSetVersion": "01.00.000"}
                    },
                    "exchanges": {
                        "exchange": [
                            {
                                "@dataSetInternalID": "1",
                                "referenceToFlowDataSet": {
                                    "@refObjectId": "flow-a",
                                    "@version": "01.00.000"
                                },
                                "exchangeDirection": "Output",
                                "meanAmount": "1",
                                "resultingAmount": "1",
                                "dataDerivationTypeStatus": "Measured"
                            },
                            {
                                "@dataSetInternalID": "2",
                                "referenceToFlowDataSet": {
                                    "@refObjectId": "flow-b",
                                    "@version": "01.00.000"
                                },
                                "exchangeDirection": "Output",
                                "meanAmount": "0.5",
                                "resultingAmount": "0.5",
                                "dataDerivationTypeStatus": "Calculated"
                            },
                            {
                                "@dataSetInternalID": "3",
                                "referenceToFlowDataSet": {
                                    "@refObjectId": "elementary",
                                    "@version": "01.00.000"
                                },
                                "exchangeDirection": "Input",
                                "meanAmount": "2",
                                "resultingAmount": "2",
                                "dataDerivationTypeStatus": "Measured"
                            }
                        ]
                    }
                }
            }),
        }
    }

    fn process_row_for_flow(
        id: &str,
        location: &str,
        flow_id: &str,
        direction: &str,
    ) -> DatasetRow {
        DatasetRow {
            id: id.to_owned(),
            version: "01.00.000".to_owned(),
            modified_at: None,
            json: json!({
                "processDataSet": {
                    "processInformation": {
                        "dataSetInformation": {
                            "name": {"baseName": [{"@xml:lang": "zh", "#text": id}]},
                            "classificationInformation": {
                                "common:classification": {
                                    "common:class": [
                                        {"@xml:lang": "en", "#text": "Manufacturing"},
                                        {"@xml:lang": "en", "#text": "Power"},
                                        {"@xml:lang": "en", "#text": "Solar"}
                                    ]
                                }
                            }
                        },
                        "geography": {
                            "locationOfOperationSupplyOrProduction": {
                                "@location": location
                            }
                        }
                    },
                    "administrativeInformation": {
                        "publicationAndOwnership": {"common:dataSetVersion": "01.00.000"}
                    },
                    "exchanges": {
                        "exchange": [
                            {
                                "@dataSetInternalID": "1",
                                "referenceToFlowDataSet": {
                                    "@refObjectId": flow_id,
                                    "@version": "01.00.000"
                                },
                                "exchangeDirection": direction,
                                "meanAmount": "1",
                                "resultingAmount": "1"
                            }
                        ]
                    }
                }
            }),
        }
    }

    #[test]
    fn output_flow_process_preserves_other_non_basic_outputs() {
        let flows = vec![
            flow_row("flow-a", "Flow A", "Product flow"),
            flow_row("flow-b", "Flow B", "Waste flow"),
            flow_row("elementary", "Elementary", "Elementary flow"),
        ];
        let processes = vec![process_row()];
        let graph = build_graph(
            &flows,
            &processes,
            &test_cli(),
            &ClassificationCatalog::default(),
        )
        .expect("graph");

        assert_eq!(graph.stats.process_count, 1);
        assert_eq!(graph.stats.edge_count, 2);
        assert!(graph.node_by_id.contains_key("flow:flow-a@01.00.000"));
        assert!(graph.node_by_id.contains_key("flow:flow-b@01.00.000"));
        assert!(!graph.node_by_id.contains_key("flow:elementary@01.00.000"));
        assert!(
            graph
                .edges
                .iter()
                .all(|edge| edge.direction == ExchangeDirection::Output)
        );
    }

    #[test]
    fn output_exchange_direction_is_stable() {
        assert_eq!(
            normalize_exchange_direction(Some(&json!("Output")), false),
            ExchangeDirection::Output
        );
        assert_eq!(
            normalize_exchange_direction(None, true),
            ExchangeDirection::Output
        );
        assert_eq!(
            normalize_exchange_direction(None, false),
            ExchangeDirection::Input
        );
    }

    #[test]
    fn nodes_emit_level1_and_level3_cluster_contract() {
        let flows = vec![flow_row_with_category(
            "flow-a",
            "Flow A",
            "Product flow",
            &["Energy", "Electricity", "Solar"],
        )];
        let processes = vec![process_row_for_flow(
            "process-a",
            "CN-GD",
            "flow-a",
            "Input",
        )];
        let graph = build_graph(
            &flows,
            &processes,
            &test_cli(),
            &ClassificationCatalog::default(),
        )
        .expect("graph");
        let flow_node = graph
            .nodes
            .iter()
            .find(|node| node.id == "flow:flow-a@01.00.000")
            .expect("flow node");

        assert_eq!(flow_node.cluster_id_level1, "energy");
        assert_eq!(flow_node.cluster_id_level3, "energy-electricity-solar");
        assert_eq!(flow_node.cluster_label_level1, "Energy");
        assert_eq!(
            flow_node.cluster_label_level3,
            "Energy / Electricity / Solar"
        );

        let payload = cluster_payload("test", &graph.nodes);
        assert_eq!(payload["schemaVersion"], "process_flow_graph_v2");
        assert!(payload["clustersLevel1"].as_array().expect("l1").len() >= 2);
        assert!(payload["clustersLevel3"].as_array().expect("l3").len() >= 2);
    }

    #[test]
    fn structured_category_path_preserves_slashes_inside_names() {
        let flows = vec![flow_row_with_category(
            "flow-a",
            "Flow A",
            "Product flow",
            &["Agriculture", "Swine / pigs", "Swine / pigs"],
        )];
        let processes = vec![process_row_for_flow(
            "process-a",
            "CN-GD",
            "flow-a",
            "Input",
        )];
        let graph = build_graph(
            &flows,
            &processes,
            &test_cli(),
            &ClassificationCatalog::default(),
        )
        .expect("graph");
        let flow_node = graph
            .nodes
            .iter()
            .find(|node| node.id == "flow:flow-a@01.00.000")
            .expect("flow node");

        assert_eq!(
            flow_node
                .category_path
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Agriculture", "Swine / pigs", "Swine / pigs"]
        );
        assert_eq!(flow_node.category_path.len(), 3);
        assert_eq!(
            flow_node.cluster_label_level3,
            "Agriculture / Swine / pigs / Swine / pigs"
        );
    }

    #[test]
    fn classification_catalog_localizes_actual_node_paths() {
        let catalog = test_classification_catalog();
        let row = flow_row_with_category(
            "flow-a",
            "Flow A",
            "Product flow",
            &["Ores and minerals", "Stone, sand and clay", "Limestone"],
        );
        let flow = parse_flow_row(&row, &catalog).expect("flow");

        assert_eq!(flow.category_system.as_deref(), Some("CPC"));
        assert_eq!(
            flow.category,
            "Ores and minerals / Stone, sand and clay / Limestone"
        );
        assert_eq!(
            flow.category_display_path,
            "矿石与矿物 / 石料、砂和黏土 / 石灰石"
        );
        assert_eq!(
            flow.category_path
                .iter()
                .map(|item| item.zh_name.as_str())
                .collect::<Vec<_>>(),
            vec!["矿石与矿物", "石料、砂和黏土", "石灰石"]
        );
    }

    #[test]
    fn classification_catalog_loads_gzip_dictionary_files() {
        let temp_dir = std::env::temp_dir().join(format!(
            "process-flow-classification-catalog-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).expect("temp dir");
        write_gzip_json(
            &temp_dir.join("CPCClassification.min.json.gz"),
            &json!({
                "CategorySystem": {
                    "categories": {
                        "@dataType": "Flow",
                        "category": [{
                            "@id": "0",
                            "@name": "Ores and minerals",
                            "category": [{
                                "@id": "04",
                                "@name": "Stone, sand and clay"
                            }]
                        }]
                    }
                }
            }),
        );
        write_gzip_json(
            &temp_dir.join("CPCClassification_zh.min.json.gz"),
            &json!({
                "CategorySystem": {
                    "categories": {
                        "@dataType": "Flow",
                        "category": [{
                            "@id": "0",
                            "@name": "矿石与矿物",
                            "category": [{
                                "@id": "04",
                                "@name": "石料、砂和黏土"
                            }]
                        }]
                    }
                }
            }),
        );

        let catalog = ClassificationCatalog::load(Some(&temp_dir)).expect("catalog");
        let row = flow_row_with_category(
            "flow-a",
            "Flow A",
            "Product flow",
            &["Ores and minerals", "Stone, sand and clay"],
        );
        let flow = parse_flow_row(&row, &catalog).expect("flow");

        assert_eq!(flow.category_system.as_deref(), Some("CPC"));
        assert_eq!(flow.category_display_path, "矿石与矿物 / 石料、砂和黏土");

        fs::remove_dir_all(&temp_dir).expect("cleanup temp dir");
    }

    #[test]
    fn geo_map_cache_builds_china_scope_with_process_links() {
        let flows = vec![flow_row("flow-a", "Flow A", "Product flow")];
        let processes = vec![
            process_row_for_flow("provider", "CN-GD", "flow-a", "Output"),
            process_row_for_flow("consumer", "CN-GD", "flow-a", "Input"),
        ];
        let graph = build_graph(
            &flows,
            &processes,
            &test_cli(),
            &ClassificationCatalog::default(),
        )
        .expect("graph");
        let china = graph
            .geo_maps
            .iter()
            .find(|geo_map| geo_map.scope == GeoMapScope::China)
            .expect("china geo map");

        assert_eq!(china.nodes.len(), 2);
        assert_eq!(china.visible_edge_indices.len(), 0);
        assert_eq!(china.process_links.len(), 1);
        assert_eq!(china.stats.edge_count, 1);
        assert!(
            china
                .background
                .paths
                .iter()
                .all(|path| !path.path.is_empty())
        );
        assert!(
            china
                .layout
                .iter()
                .all(|position| position.iter().all(|value| value.is_finite()))
        );
        assert!(
            china
                .adjacency
                .values()
                .filter(|edges| edges
                    .iter()
                    .any(|edge| edge.starts_with("process-link:china:")))
                .count()
                >= 2
        );
    }

    #[test]
    fn geo_location_code_resolution_matches_map_coverage() {
        for code in ["AD", "GE", "GF", "GP", "MQ", "PG", "RE", "YT"] {
            assert_eq!(resolve_world_region_key(code).as_deref(), Some(code));
        }
        assert_eq!(resolve_world_region_key("UK").as_deref(), Some("GB"));
        assert_eq!(resolve_world_region_key("CN-GD-SZX").as_deref(), Some("CN"));
        assert!(
            find_geo_region_shape(WORLD_REGION_SHAPES, "CN")
                .expect("CN world shape")
                .samples
                .len()
                >= 1_800
        );

        for code in ["BV", "EU-27", "GI", "GLO", "RER", "UM"] {
            assert!(resolve_world_region_key(code).is_none());
        }

        assert_eq!(resolve_china_province_code("CN-GD"), Some("GD"));
        assert_eq!(resolve_china_province_code("CN-GD-SZX"), Some("GD"));
        assert_eq!(resolve_china_province_code("CN-HK"), Some("HK"));
        assert!(
            find_geo_region_shape(CHINA_REGION_SHAPES, "GD")
                .expect("GD province shape")
                .samples
                .len()
                >= 300
        );
        assert!(resolve_china_province_code("CN").is_none());
        assert!(resolve_china_province_code("HK").is_none());
        assert!(resolve_china_province_code("CN-XX").is_none());
    }

    #[test]
    fn geo_map_location_rules_filter_nodes_by_scope() {
        let flows = vec![flow_row("flow-a", "Flow A", "Product flow")];
        let processes = vec![
            process_row_for_flow("cn-country", "CN", "flow-a", "Output"),
            process_row_for_flow("cn-province", "CN-GD-SZX", "flow-a", "Input"),
            process_row_for_flow("gb-alias", "UK", "flow-a", "Output"),
            process_row_for_flow("global", "GLO", "flow-a", "Output"),
            process_row_for_flow("null-location", "NULL", "flow-a", "Output"),
            process_row_for_flow("unknown", "ZZ", "flow-a", "Output"),
        ];
        let graph = build_graph(
            &flows,
            &processes,
            &test_cli(),
            &ClassificationCatalog::default(),
        )
        .expect("graph");
        let world = graph
            .geo_maps
            .iter()
            .find(|geo_map| geo_map.scope == GeoMapScope::World)
            .expect("world geo map");
        let world_node_ids = world
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<BTreeSet<_>>();

        assert!(world_node_ids.contains("process:cn-country@01.00.000"));
        assert!(world_node_ids.contains("process:cn-province@01.00.000"));
        assert!(world_node_ids.contains("process:gb-alias@01.00.000"));
        assert!(!world_node_ids.contains("process:global@01.00.000"));
        assert!(!world_node_ids.contains("process:null-location@01.00.000"));
        assert!(!world_node_ids.contains("process:unknown@01.00.000"));
        assert_eq!(
            world.diagnostics.location_rule_version,
            GEO_LOCATION_RULE_VERSION
        );
        assert!(world.diagnostics.excluded_location_count >= 3);

        let china = graph
            .geo_maps
            .iter()
            .find(|geo_map| geo_map.scope == GeoMapScope::China)
            .expect("china geo map");
        let china_node_ids = china
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<BTreeSet<_>>();

        assert!(china_node_ids.contains("process:cn-province@01.00.000"));
        assert!(!china_node_ids.contains("process:cn-country@01.00.000"));
        assert!(!china_node_ids.contains("process:gb-alias@01.00.000"));
        assert!(!china_node_ids.contains("process:global@01.00.000"));
        assert!(!china_node_ids.contains("process:null-location@01.00.000"));
        assert!(!china_node_ids.contains("process:unknown@01.00.000"));
        assert_eq!(
            china.diagnostics.location_rule_version,
            GEO_LOCATION_RULE_VERSION
        );
        assert!(china.diagnostics.excluded_location_count >= 5);
    }

    #[test]
    fn geo_map_layout_spreads_nodes_inside_same_region() {
        let flows = vec![flow_row("flow-a", "Flow A", "Product flow")];
        let processes = (0..12)
            .map(|index| {
                process_row_for_flow(
                    &format!("gd-process-{index:02}"),
                    "CN-GD",
                    "flow-a",
                    if index % 2 == 0 { "Output" } else { "Input" },
                )
            })
            .collect::<Vec<_>>();
        let graph = build_graph(
            &flows,
            &processes,
            &test_cli(),
            &ClassificationCatalog::default(),
        )
        .expect("graph");
        let china = graph
            .geo_maps
            .iter()
            .find(|geo_map| geo_map.scope == GeoMapScope::China)
            .expect("china geo map");
        let (frame_width, frame_height) = GeoMapScope::China.frame();
        let unique_points = china
            .layout
            .iter()
            .map(|position| {
                assert!(position[0] >= -frame_width / 2.0);
                assert!(position[0] <= frame_width / 2.0);
                assert!(position[1] >= -frame_height / 2.0);
                assert!(position[1] <= frame_height / 2.0);
                assert!(position.iter().all(|value| value.is_finite()));
                (
                    (position[0] * 10.0).round() as i32,
                    (position[1] * 10.0).round() as i32,
                )
            })
            .collect::<BTreeSet<_>>();
        let gd_sample_points = find_geo_region_shape(CHINA_REGION_SHAPES, "GD")
            .expect("GD province shape")
            .samples
            .iter()
            .map(|[x, y]| ((*x * 10.0).round() as i32, (*y * 10.0).round() as i32))
            .collect::<BTreeSet<_>>();

        assert_eq!(china.nodes.len(), 12);
        assert!(unique_points.len() >= 10);
        assert!(
            unique_points
                .iter()
                .all(|point| gd_sample_points.contains(point))
        );
    }

    #[test]
    fn expanded_layout_is_finite_and_fitted_to_overview_bounds() {
        let flows = vec![
            flow_row("flow-a", "Flow A", "Product flow"),
            flow_row("flow-b", "Flow B", "Waste flow"),
            flow_row("elementary", "Elementary", "Elementary flow"),
        ];
        let processes = vec![process_row()];
        let graph = build_graph(
            &flows,
            &processes,
            &test_cli(),
            &ClassificationCatalog::default(),
        )
        .expect("graph");
        let bounds = super::summarize_layout(&graph.expanded_layout);

        assert_eq!(graph.expanded_layout.len(), graph.nodes.len());
        assert!(bounds.width > 0.0);
        assert!(bounds.height > 0.0);
        assert!(bounds.width <= super::EXPANDED_TOPOLOGY_TARGET_WIDTH + 0.01);
        assert!(bounds.height <= super::EXPANDED_TOPOLOGY_TARGET_HEIGHT + 0.01);
        assert!(
            graph
                .expanded_layout
                .iter()
                .all(|position| position.iter().all(|value| value.is_finite()))
        );
    }

    #[test]
    fn gzip_json_round_trips() {
        let object = encoded_gzip_json("graph/test.json.gz".to_owned(), &json!({"ok": true}))
            .expect("encode");
        let mut decoder = GzDecoder::new(object.bytes.as_slice());
        let mut decoded = String::new();
        decoder.read_to_string(&mut decoded).expect("decode");
        assert_eq!(decoded, "{\"ok\":true}");
    }
}
