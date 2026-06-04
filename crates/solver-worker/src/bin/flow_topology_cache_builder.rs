#![allow(
    clippy::similar_names,
    clippy::struct_field_names,
    clippy::too_many_lines
)]

use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;
use clap::Parser;
use serde::Serialize;
use serde_json::{Value, json};
use solver_worker::{
    pgbouncer_sqlx::{self as sqlx, Row, postgres::PgPoolOptions},
    storage::ObjectStoreClient,
};

const BASIC_FLOW_TYPE: &str = "Elementary flow";
const DEFAULT_CACHE_PREFIX: &str = "national-carbon/flow-topology/v1";
const DEFAULT_PAGE_SIZE: i64 = 500;
const MAX_PAGE_SIZE: i64 = 1000;
const PUBLISHED_STATE_CODE: i32 = 100;
const SCHEMA_VERSION: &str = "flow_process_topology_v1";

#[derive(Debug, Parser)]
#[command(name = "flow-topology-cache-builder")]
struct Cli {
    /// `PostgreSQL` URL (preferred env: `DATABASE_URL`, fallback: `CONN`).
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    /// `PostgreSQL` URL fallback used by this project in local `.env`.
    #[arg(long, env = "CONN")]
    conn: Option<String>,
    /// S3-compatible endpoint for topology cache objects.
    #[arg(long, env = "S3_ENDPOINT")]
    s3_endpoint: Option<String>,
    /// S3 region.
    #[arg(long, env = "S3_REGION")]
    s3_region: Option<String>,
    /// S3 bucket fallback.
    #[arg(long, env = "S3_BUCKET")]
    s3_bucket: Option<String>,
    /// Dedicated topology cache bucket.
    #[arg(long, env = "FLOW_TOPOLOGY_CACHE_BUCKET")]
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
        env = "FLOW_TOPOLOGY_CACHE_PREFIX",
        default_value = DEFAULT_CACHE_PREFIX
    )]
    cache_prefix: String,
    /// Optional explicit build id.
    #[arg(long)]
    build_id: Option<String>,
    /// Limit generated flow snapshots for canary runs.
    #[arg(long)]
    limit_flows: Option<usize>,
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
struct FlowTopologyFlow {
    #[serde(rename = "flowType")]
    flow_type: String,
    id: String,
    name: String,
    version: String,
}

#[derive(Debug, Clone, Serialize)]
struct FlowTopologyNode {
    #[serde(skip_serializing_if = "Option::is_none")]
    classification: Option<String>,
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "referenceYear")]
    reference_year: Option<String>,
    #[serde(rename = "type")]
    node_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none", rename = "typeOfDataSet")]
    type_of_data_set: Option<String>,
    version: String,
}

#[derive(Debug, Clone, Serialize)]
struct FlowTopologyEdge {
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "dataDerivationTypeStatus"
    )]
    data_derivation_type_status: Option<String>,
    #[serde(rename = "exchangeDirection")]
    exchange_direction: ExchangeDirection,
    id: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "meanAmount")]
    mean_amount: Option<String>,
    #[serde(rename = "quantitativeReference")]
    quantitative_reference: bool,
    relation: Relation,
    #[serde(skip_serializing_if = "Option::is_none", rename = "resultingAmount")]
    resulting_amount: Option<String>,
    source: String,
    target: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Relation {
    Provider,
    Consumer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum ExchangeDirection {
    Input,
    Output,
}

#[derive(Debug, Clone, Serialize)]
struct FlowTopologySnapshot {
    #[serde(rename = "buildId")]
    build_id: String,
    #[serde(rename = "dataAsOf")]
    data_as_of: String,
    edges: Vec<FlowTopologyEdge>,
    flow: FlowTopologyFlow,
    nodes: Vec<FlowTopologyNode>,
    #[serde(rename = "schemaVersion")]
    schema_version: &'static str,
    stats: FlowTopologyStats,
}

#[derive(Debug, Clone, Default, Serialize)]
struct FlowTopologyStats {
    consumers: usize,
    #[serde(rename = "processCount")]
    process_count: usize,
    providers: usize,
}

#[derive(Debug, Clone)]
struct FlowMetadata {
    flow: FlowTopologyFlow,
    modified_at: Option<String>,
}

#[derive(Debug, Clone)]
struct ProcessMetadata {
    classification: Option<String>,
    id: String,
    location: Option<String>,
    name: String,
    reference_flow_id: Option<String>,
    reference_year: Option<String>,
    type_of_data_set: Option<String>,
    version: String,
}

#[derive(Debug, Clone)]
struct ProcessExchange {
    data_derivation_type_status: Option<String>,
    exchange_direction: ExchangeDirection,
    flow_id: String,
    flow_version: Option<String>,
    mean_amount: Option<String>,
    quantitative_reference: bool,
    resulting_amount: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct BuildSummary {
    #[serde(rename = "buildId")]
    build_id: String,
    bucket: String,
    dry_run: bool,
    prefix: String,
    #[serde(rename = "snapshotCount")]
    snapshot_count: usize,
    #[serde(rename = "uploadedObjects")]
    uploaded_objects: usize,
    #[serde(rename = "sourceRows")]
    source_rows: SourceRows,
}

#[derive(Debug, Clone, Serialize)]
struct SourceRows {
    flows: usize,
    processes: usize,
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

    eprintln!("[flow-topology] reading published flows from database");
    let flow_rows =
        fetch_all_rows_read_only(&pool, "flows", page_size, cli.source_row_limit).await?;
    eprintln!("[flow-topology] reading published processes from database");
    let process_rows =
        fetch_all_rows_read_only(&pool, "processes", page_size, cli.source_row_limit).await?;
    eprintln!(
        "[flow-topology] source rows loaded: flows={} processes={}",
        flow_rows.len(),
        process_rows.len()
    );
    let snapshots = build_snapshots(&flow_rows, &process_rows, &build_id, cli.limit_flows);
    eprintln!(
        "[flow-topology] built topology snapshots: {}",
        snapshots.len()
    );
    let summary = publish_snapshots(
        &cli,
        &snapshots,
        &build_id,
        SourceRows {
            flows: flow_rows.len(),
            processes: process_rows.len(),
        },
    )
    .await?;

    println!("{}", serde_json::to_string_pretty(&summary)?);
    println!(
        "[summary] dry_run={} buildId={} sourceFlows={} sourceProcesses={} snapshots={} uploadedObjects={} status=ok",
        summary.dry_run,
        summary.build_id,
        summary.source_rows.flows,
        summary.source_rows.processes,
        summary.snapshot_count,
        summary.uploaded_objects
    );
    Ok(())
}

fn default_build_id() -> String {
    format!(
        "flow-topology-{}",
        Utc::now().to_rfc3339().replace([':', '.'], "-")
    )
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
        "FLOW_TOPOLOGY_CACHE_BUCKET or S3_BUCKET",
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
            "[flow-topology] fetching {table} rows after={} limit={query_limit}",
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
            "[flow-topology] fetched {table} page rows={} total={}",
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
                    "[flow-topology] retrying {table} page after transient read error: {error}"
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

fn build_snapshots(
    flow_rows: &[DatasetRow],
    process_rows: &[DatasetRow],
    build_id: &str,
    limit_flows: Option<usize>,
) -> Vec<FlowTopologySnapshot> {
    let data_as_of = Utc::now().to_rfc3339();
    let mut flow_by_version = BTreeMap::<String, FlowMetadata>::new();
    let mut latest_flow_by_id = BTreeMap::<String, FlowMetadata>::new();

    for row in flow_rows {
        let Some(flow_meta) = parse_flow_row(row) else {
            continue;
        };
        flow_by_version.insert(
            format!("{}:{}", flow_meta.flow.id, flow_meta.flow.version),
            flow_meta.clone(),
        );
        let replace_latest = latest_flow_by_id
            .get(&flow_meta.flow.id)
            .is_none_or(|current| flow_meta.flow.version > current.flow.version);
        if replace_latest {
            latest_flow_by_id.insert(flow_meta.flow.id.clone(), flow_meta);
        }
    }

    for flow_meta in latest_flow_by_id.values() {
        flow_by_version.insert(flow_meta.flow.id.clone(), flow_meta.clone());
    }

    let mut snapshots = BTreeMap::<String, FlowTopologySnapshot>::new();
    for row in process_rows {
        let Some(process_meta) = parse_process_metadata(row) else {
            continue;
        };

        for exchange in parse_process_exchanges(row, &process_meta) {
            let Some(flow_meta) =
                flow_lookup_keys(&exchange.flow_id, exchange.flow_version.as_deref())
                    .iter()
                    .find_map(|key| flow_by_version.get(key))
            else {
                continue;
            };
            let snapshot_key = format!("{}:{}", flow_meta.flow.id, flow_meta.flow.version);
            if limit_flows.is_some_and(|limit| snapshots.len() >= limit)
                && !snapshots.contains_key(&snapshot_key)
            {
                continue;
            }

            let snapshot = snapshots
                .entry(snapshot_key)
                .or_insert_with(|| new_snapshot(flow_meta, build_id, &data_as_of));
            let process_id = process_node_id(&process_meta);
            if !snapshot.nodes.iter().any(|node| node.id == process_id) {
                snapshot.nodes.push(FlowTopologyNode {
                    classification: process_meta.classification.clone(),
                    id: process_id.clone(),
                    location: process_meta.location.clone(),
                    name: process_meta.name.clone(),
                    reference_year: process_meta.reference_year.clone(),
                    node_type: "process",
                    type_of_data_set: process_meta.type_of_data_set.clone(),
                    version: process_meta.version.clone(),
                });
            }

            let relation = if exchange.exchange_direction == ExchangeDirection::Output {
                Relation::Provider
            } else {
                Relation::Consumer
            };
            let flow_id = flow_node_id(&flow_meta.flow);
            let edge_index = snapshot.edges.len();
            snapshot.edges.push(FlowTopologyEdge {
                data_derivation_type_status: exchange.data_derivation_type_status,
                exchange_direction: exchange.exchange_direction,
                id: format!(
                    "edge:{}:{}:{}",
                    process_meta.id, process_meta.version, edge_index
                ),
                mean_amount: exchange.mean_amount,
                quantitative_reference: exchange.quantitative_reference,
                relation,
                resulting_amount: exchange.resulting_amount,
                source: if relation == Relation::Provider {
                    process_id.clone()
                } else {
                    flow_id.clone()
                },
                target: if relation == Relation::Provider {
                    flow_id
                } else {
                    process_id
                },
            });
        }
    }

    snapshots
        .into_values()
        .filter(|snapshot| !snapshot.edges.is_empty())
        .map(finalize_stats)
        .collect()
}

fn new_snapshot(
    flow_meta: &FlowMetadata,
    build_id: &str,
    data_as_of: &str,
) -> FlowTopologySnapshot {
    FlowTopologySnapshot {
        build_id: build_id.to_owned(),
        data_as_of: flow_meta
            .modified_at
            .clone()
            .unwrap_or_else(|| data_as_of.to_owned()),
        edges: Vec::new(),
        flow: flow_meta.flow.clone(),
        nodes: vec![FlowTopologyNode {
            classification: None,
            id: flow_node_id(&flow_meta.flow),
            location: None,
            name: flow_meta.flow.name.clone(),
            reference_year: None,
            node_type: "flow",
            type_of_data_set: None,
            version: flow_meta.flow.version.clone(),
        }],
        schema_version: SCHEMA_VERSION,
        stats: FlowTopologyStats::default(),
    }
}

fn finalize_stats(mut snapshot: FlowTopologySnapshot) -> FlowTopologySnapshot {
    let providers = snapshot
        .edges
        .iter()
        .filter(|edge| edge.relation == Relation::Provider)
        .map(|edge| edge.source.as_str())
        .collect::<BTreeSet<_>>();
    let consumers = snapshot
        .edges
        .iter()
        .filter(|edge| edge.relation == Relation::Consumer)
        .map(|edge| edge.target.as_str())
        .collect::<BTreeSet<_>>();
    snapshot.stats = FlowTopologyStats {
        consumers: consumers.len(),
        process_count: snapshot
            .nodes
            .iter()
            .filter(|node| node.node_type == "process")
            .count(),
        providers: providers.len(),
    };
    snapshot
}

async fn publish_snapshots(
    cli: &Cli,
    snapshots: &[FlowTopologySnapshot],
    build_id: &str,
    source_rows: SourceRows,
) -> anyhow::Result<BuildSummary> {
    let prefix = normalize_prefix(&cli.cache_prefix);
    let bucket = resolve_bucket(cli)?.to_owned();
    let dry_run = !cli.execute;
    let store = if dry_run {
        None
    } else {
        Some(build_object_store(cli)?)
    };
    let mut uploaded_objects = 0_usize;

    for snapshot in snapshots {
        let topology_path = topology_object_path(&prefix, build_id, &snapshot.flow);
        let latest_path = latest_pointer_path(&prefix, build_id, &snapshot.flow.id);
        upload_json(store.as_ref(), &topology_path, snapshot).await?;
        upload_json(
            store.as_ref(),
            &latest_path,
            &json!({
                "buildId": build_id,
                "flow": snapshot.flow,
                "schemaVersion": SCHEMA_VERSION,
                "topologyPath": topology_path.strip_prefix(format!("{prefix}/").as_str()).unwrap_or(topology_path.as_str()),
            }),
        )
        .await?;
        uploaded_objects += 2;
    }

    upload_json(
        store.as_ref(),
        &format!("{prefix}/manifest.json"),
        &json!({
            "activeBuildId": build_id,
            "bucket": bucket,
            "generatedAt": Utc::now().to_rfc3339(),
            "objectCount": snapshots.len(),
            "schemaVersion": SCHEMA_VERSION,
        }),
    )
    .await?;
    uploaded_objects += 1;

    Ok(BuildSummary {
        build_id: build_id.to_owned(),
        bucket,
        dry_run,
        prefix,
        snapshot_count: snapshots.len(),
        uploaded_objects: if dry_run { 0 } else { uploaded_objects },
        source_rows,
    })
}

async fn upload_json<T>(
    store: Option<&ObjectStoreClient>,
    object_path: &str,
    payload: &T,
) -> anyhow::Result<()>
where
    T: Serialize,
{
    if let Some(store) = store {
        store
            .upload_object_key(
                object_path,
                "application/json",
                serde_json::to_vec(payload)?,
            )
            .await?;
    }
    Ok(())
}

fn normalize_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_owned()
}

fn topology_object_path(prefix: &str, build_id: &str, flow: &FlowTopologyFlow) -> String {
    format!(
        "{prefix}/builds/{build_id}/by-flow/{}/{}/{}/topology.json",
        hash_prefix(&flow.id),
        flow.id,
        flow.version
    )
}

fn latest_pointer_path(prefix: &str, build_id: &str, flow_id: &str) -> String {
    format!(
        "{prefix}/builds/{build_id}/by-flow/{}/{flow_id}/latest.json",
        hash_prefix(flow_id)
    )
}

fn hash_prefix(flow_id: &str) -> String {
    let normalized = flow_id.replace('-', "").to_ascii_lowercase();
    let prefix = normalized.chars().take(2).collect::<String>();
    if prefix.is_empty() {
        "00".to_owned()
    } else {
        prefix
    }
}

fn flow_node_id(flow: &FlowTopologyFlow) -> String {
    format!("flow:{}@{}", flow.id, flow.version)
}

fn process_node_id(process: &ProcessMetadata) -> String {
    format!("process:{}@{}", process.id, process.version)
}

fn flow_lookup_keys(flow_id: &str, flow_version: Option<&str>) -> Vec<String> {
    flow_version.map_or_else(
        || vec![flow_id.to_owned()],
        |version| vec![format!("{flow_id}:{version}"), flow_id.to_owned()],
    )
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

    Some(FlowMetadata {
        flow: FlowTopologyFlow {
            flow_type,
            id: row.id.clone(),
            name: data_set_info
                .and_then(|info| pick_value(info, &["name", "baseName"]))
                .and_then(localized_text)
                .unwrap_or_else(|| row.id.clone()),
            version: extract_data_set_version(data_set, &row.version),
        },
        modified_at: row.modified_at.clone(),
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

    Some(ProcessMetadata {
        classification: data_set_info.and_then(extract_classification),
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
            let quantitative_reference = exchange
                .get("quantitativeReference")
                .and_then(normalize_value)
                .is_some_and(|value| value.eq_ignore_ascii_case("true"))
                || process.reference_flow_id.as_deref() == Some(flow_id.as_str());
            Some(ProcessExchange {
                data_derivation_type_status: exchange
                    .get("dataDerivationTypeStatus")
                    .and_then(normalize_value),
                exchange_direction: normalize_exchange_direction(
                    exchange.get("exchangeDirection"),
                    quantitative_reference,
                ),
                flow_id,
                flow_version: reference.get("@version").and_then(normalize_value),
                mean_amount: exchange
                    .get("meanAmount")
                    .or_else(|| exchange.get("meanValue"))
                    .and_then(normalize_value),
                quantitative_reference,
                resulting_amount: exchange.get("resultingAmount").and_then(normalize_value),
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

#[cfg(test)]
mod tests {
    use super::{ExchangeDirection, hash_prefix, normalize_exchange_direction};
    use serde_json::json;

    #[test]
    fn hash_prefix_matches_frontend_contract() {
        assert_eq!(hash_prefix("11111111-2222"), "11");
        assert_eq!(hash_prefix(""), "00");
    }

    #[test]
    fn output_exchange_is_provider_direction() {
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
}
