#![allow(
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
    io::Write,
};

use chrono::Utc;
use clap::Parser;
use flate2::{Compression, write::GzEncoder};
use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use solver_worker::{
    pgbouncer_sqlx::{self as sqlx, Row, postgres::PgPoolOptions},
    storage::ObjectStoreClient,
};

const BASIC_FLOW_TYPE: &str = "Elementary flow";
const DEFAULT_CACHE_PREFIX: &str = "national-carbon/process-flow-graph/v1";
const DEFAULT_PAGE_SIZE: i64 = 500;
const MAX_PAGE_SIZE: i64 = 1000;
const PUBLISHED_STATE_CODE: i32 = 100;
const ACTIVE_MANIFEST_SCHEMA_VERSION: &str = "process_flow_graph_manifest_v1";
const BUILD_SCHEMA_VERSION: &str = "process_flow_graph_v1";
const EDGE_BINARY_MAGIC: &[u8; 8] = b"PFGEDG1\0";
const CSR_BINARY_MAGIC: &[u8; 8] = b"PFGCSR1\0";
const LAYOUT_BINARY_MAGIC: &[u8; 8] = b"PFGLAY1\0";
const BINARY_FORMAT_VERSION: u32 = 1;
const U32_NONE: u32 = u32::MAX;
const SPHERE_RADIUS: f32 = 310.0;
const GOLDEN_ANGLE: f32 = 2.399_963_1;

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
    cluster_id: String,
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
    cluster_id: String,
    flow_type: String,
    id: String,
    location: Option<String>,
    name: String,
    version: String,
}

#[derive(Debug, Clone)]
struct ProcessMetadata {
    category: String,
    cluster_id: String,
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

    let graph = build_graph(&flow_rows, &process_rows, &cli)?;
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
        Utc::now().to_rfc3339().replace([':', '.'], "-")
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
) -> anyhow::Result<ProcessFlowGraph> {
    let mut flow_by_version = BTreeMap::<String, FlowMetadata>::new();
    let mut latest_flow_by_id = BTreeMap::<String, FlowMetadata>::new();

    for row in flow_rows {
        let Some(flow_meta) = parse_flow_row(row) else {
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
        let Some(process_meta) = parse_process_metadata(row) else {
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
            cluster_id: flow.cluster_id.clone(),
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
            cluster_id: process.cluster_id.clone(),
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
        let expanded_layout = create_expanded_layout(&self.nodes);
        let search_flows = build_search_flows(&self.nodes);

        Ok(ProcessFlowGraph {
            adjacency_edge_indices,
            adjacency_offsets,
            dictionaries: self.dictionaries,
            edges: self.edges,
            expanded_layout,
            flow_by_id: self.flow_by_id,
            nodes: self.nodes,
            node_by_id: self.node_by_id,
            process_by_id: self.process_by_id,
            search_flows,
            sphere_layout,
            stats,
        })
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

    Ok(vec![
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
    ])
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

fn cluster_payload(build_id: &str, nodes: &[GraphNode]) -> Value {
    let mut clusters = BTreeMap::<String, Value>::new();
    for node in nodes {
        clusters.entry(node.cluster_id.clone()).or_insert_with(|| {
            json!({
                "id": node.cluster_id,
                "label": node.category,
            })
        });
    }
    json!({
        "schemaVersion": BUILD_SCHEMA_VERSION,
        "buildId": build_id,
        "clusters": clusters.into_values().collect::<Vec<_>>(),
    })
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

fn create_expanded_layout(nodes: &[GraphNode]) -> Vec<[f32; 3]> {
    let clusters = cluster_order(nodes);
    let columns = (clusters.len() as f32).sqrt().ceil().max(1.0) as usize;
    let spacing_x = 320.0_f32;
    let spacing_y = 230.0_f32;
    let mut cluster_centers = BTreeMap::<String, [f32; 2]>::new();
    for (index, cluster_id) in clusters.iter().enumerate() {
        let column = index % columns;
        let row = index / columns;
        let x = (column as f32 - (columns as f32 - 1.0) / 2.0) * spacing_x;
        let y = (row as f32 - 0.5) * spacing_y;
        cluster_centers.insert(cluster_id.clone(), [x, y]);
    }
    let mut cluster_counts = BTreeMap::<String, usize>::new();
    nodes
        .iter()
        .map(|node| {
            let count = cluster_counts.entry(node.cluster_id.clone()).or_default();
            let local_index = *count;
            *count += 1;
            let center = cluster_centers
                .get(&node.cluster_id)
                .copied()
                .unwrap_or([0.0, 0.0]);
            let ring = 22.0
                + (local_index % 11) as f32 * 11.5
                + if node.kind == NodeKind::Process {
                    22.0
                } else {
                    0.0
                };
            let theta = local_index as f32 * GOLDEN_ANGLE;
            let jitter_x = (hash_unit(&node.id, 29) - 0.5) * 24.0;
            let jitter_y = (hash_unit(&node.id, 31) - 0.5) * 24.0;
            [
                center[0] + theta.cos() * ring + jitter_x,
                center[1] + theta.sin() * ring + jitter_y,
                if node.kind == NodeKind::Process {
                    24.0
                } else {
                    8.0
                },
            ]
        })
        .collect()
}

fn cluster_order(nodes: &[GraphNode]) -> Vec<String> {
    let mut seen = BTreeSet::<String>::new();
    for node in nodes {
        seen.insert(node.cluster_id.clone());
    }
    seen.into_iter().collect()
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

fn parse_flow_row(row: &DatasetRow) -> Option<FlowMetadata> {
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

    let category = data_set_info
        .and_then(extract_classification)
        .unwrap_or_else(|| flow_type.clone());

    Some(FlowMetadata {
        category: category.clone(),
        cluster_id: cluster_id_from_category(&category),
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

fn parse_process_metadata(row: &DatasetRow) -> Option<ProcessMetadata> {
    let root = preferred_json(row);
    let data_set = pick_record(root, &["processDataSet"])
        .or_else(|| pick_record(root, &["process_data_set"]))?;
    let process_info = pick_record(data_set, &["processInformation"]);
    let data_set_info = process_info.and_then(|info| pick_record(info, &["dataSetInformation"]));
    let reference_flow = process_info
        .and_then(|info| pick_record(info, &["quantitativeReference", "referenceToReferenceFlow"]));
    let category = data_set_info
        .and_then(extract_classification)
        .unwrap_or_else(|| "process".to_owned());

    Some(ProcessMetadata {
        category: category.clone(),
        cluster_id: cluster_id_from_category(&category),
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

fn extract_classification(info: &Value) -> Option<String> {
    let classification_info = info.get("classificationInformation")?;
    let classification = pick_value(
        classification_info,
        &["common:classification", "common:class"],
    )
    .or_else(|| pick_value(classification_info, &["classification", "class"]))
    .or_else(|| {
        pick_value(
            classification_info,
            &["common:elementaryFlowCategorization", "common:category"],
        )
    })?;
    let labels = as_array(classification)
        .filter_map(localized_text)
        .collect::<Vec<_>>();

    (!labels.is_empty()).then(|| labels.join(" / "))
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

fn cluster_id_from_category(category: &str) -> String {
    let head = category
        .split('/')
        .next()
        .unwrap_or(category)
        .trim()
        .to_ascii_lowercase();
    let mut slug = String::new();
    for ch in head.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }
    slug.trim_matches('-').to_owned()
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
    use flate2::read::GzDecoder;
    use serde_json::json;
    use std::io::Read;

    use super::DatasetRow;
    use super::{
        Cli, ExchangeDirection, build_graph, encoded_gzip_json, normalize_exchange_direction,
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

    #[test]
    fn output_flow_process_preserves_other_non_basic_outputs() {
        let flows = vec![
            flow_row("flow-a", "Flow A", "Product flow"),
            flow_row("flow-b", "Flow B", "Waste flow"),
            flow_row("elementary", "Elementary", "Elementary flow"),
        ];
        let processes = vec![process_row()];
        let graph = build_graph(&flows, &processes, &test_cli()).expect("graph");

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
    fn gzip_json_round_trips() {
        let object = encoded_gzip_json("graph/test.json.gz".to_owned(), &json!({"ok": true}))
            .expect("encode");
        let mut decoder = GzDecoder::new(object.bytes.as_slice());
        let mut decoded = String::new();
        decoder.read_to_string(&mut decoded).expect("decode");
        assert_eq!(decoded, "{\"ok\":true}");
    }
}
