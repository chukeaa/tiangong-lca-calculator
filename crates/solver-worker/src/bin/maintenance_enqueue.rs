use chrono::{DateTime, Utc};
use clap::{Parser, ValueEnum};
use serde_json::{Map, Value, json};
use solver_worker::{
    db_pool::{APP_MAINTENANCE_ENQUEUE, WorkerDbPoolOptions},
    pgbouncer_sqlx::Row,
};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum MaintenanceJobKind {
    #[value(name = "snapshot-gc")]
    Snapshot,
    #[value(name = "result-gc")]
    Result,
    #[value(name = "package-artifact-gc")]
    PackageArtifact,
    #[value(name = "process-flow-graph-cache")]
    ProcessFlowGraphCache,
}

impl MaintenanceJobKind {
    const fn job_kind(self) -> &'static str {
        match self {
            Self::Snapshot => "lca.snapshot_gc",
            Self::Result => "lca.result_gc",
            Self::PackageArtifact => "tidas.package_artifact_gc",
            Self::ProcessFlowGraphCache => "national_carbon.process_flow_graph_cache_build",
        }
    }

    const fn payload_schema_version(self) -> &'static str {
        match self {
            Self::Snapshot => "lca.snapshot_gc.request.v1",
            Self::Result => "lca.result_gc.request.v1",
            Self::PackageArtifact => "tidas.package_artifact_gc.request.v1",
            Self::ProcessFlowGraphCache => {
                "national_carbon.process_flow_graph_cache_build.request.v1"
            }
        }
    }
}

#[derive(Debug, Clone, Parser)]
#[command(name = "maintenance-enqueue")]
struct Cli {
    /// Maintenance job kind to enqueue.
    #[arg(value_enum)]
    job_kind: MaintenanceJobKind,
    /// `PostgreSQL` URL (preferred env: `DATABASE_URL`, fallback: `CONN`).
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    /// `PostgreSQL` URL fallback used by this project in local `.env`.
    #[arg(long, env = "CONN")]
    conn: Option<String>,
    /// Deployment environment used in payload and concurrency keys.
    #[arg(long, env = "MAINTENANCE_JOB_ENVIRONMENT", default_value = "default")]
    environment: String,
    /// Enqueue destructive execute mode. Default is dry-run.
    #[arg(long, env = "MAINTENANCE_JOB_EXECUTE", default_value_t = false)]
    execute: bool,
    /// Optional requester UUID for operator-initiated jobs.
    #[arg(long, env = "MAINTENANCE_JOB_REQUESTED_BY")]
    requested_by: Option<Uuid>,
    /// Worker job requester type.
    #[arg(long, env = "MAINTENANCE_JOB_REQUESTER_TYPE", default_value = "system")]
    requester_type: String,
    /// Worker job visibility. Keep maintenance jobs out of the normal user task center.
    #[arg(long, env = "MAINTENANCE_JOB_VISIBILITY", default_value = "operator")]
    visibility: String,
    /// Optional explicit idempotency key.
    #[arg(long, env = "MAINTENANCE_JOB_IDEMPOTENCY_KEY")]
    idempotency_key: Option<String>,
    /// Optional explicit concurrency key.
    #[arg(long, env = "MAINTENANCE_JOB_CONCURRENCY_KEY")]
    concurrency_key: Option<String>,
    /// Optional queue key; defaults to the environment name.
    #[arg(long, env = "MAINTENANCE_JOB_QUEUE_KEY")]
    queue_key: Option<String>,
    /// Worker job priority.
    #[arg(long, env = "MAINTENANCE_JOB_PRIORITY", default_value_t = 0_i32)]
    priority: i32,
    /// Worker job max attempts. Defaults to 1 for destructive execute, 3 for dry-run.
    #[arg(long, env = "MAINTENANCE_JOB_MAX_ATTEMPTS")]
    max_attempts: Option<i32>,

    #[arg(long)]
    snapshot_retention_days: Option<i64>,
    #[arg(long)]
    orphan_retention_days: Option<i64>,
    #[arg(long)]
    max_snapshots: Option<i64>,
    #[arg(long)]
    max_orphan_dirs: Option<i64>,
    #[arg(long)]
    max_bytes: Option<i64>,
    #[arg(long)]
    batch_size: Option<i64>,
    #[arg(long)]
    max_batches: Option<i64>,
    #[arg(long)]
    job_retention_days: Option<i64>,
    #[arg(long)]
    request_cache_retention_days: Option<i64>,
    #[arg(long)]
    build_id: Option<String>,
    #[arg(long)]
    limit_flows: Option<i64>,
    #[arg(long)]
    limit_processes: Option<i64>,
    #[arg(long)]
    max_edges: Option<i64>,
    #[arg(long)]
    source_row_limit: Option<i64>,
    #[arg(long)]
    page_size: Option<i64>,
    #[arg(long)]
    cache_prefix: Option<String>,
    #[arg(long)]
    cache_bucket: Option<String>,
}

impl Cli {
    fn resolved_database_url(&self) -> anyhow::Result<&str> {
        self.database_url
            .as_deref()
            .or(self.conn.as_deref())
            .ok_or_else(|| anyhow::anyhow!("missing DB connection: set DATABASE_URL or CONN"))
    }

    fn environment(&self) -> String {
        let trimmed = self.environment.trim();
        if trimmed.is_empty() {
            "default".to_owned()
        } else {
            trimmed.to_owned()
        }
    }

    fn requester_type(&self) -> String {
        self.requester_type.trim().to_ascii_lowercase()
    }

    fn visibility(&self) -> String {
        self.visibility.trim().to_ascii_lowercase()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let pool = WorkerDbPoolOptions::new(APP_MAINTENANCE_ENQUEUE)
        .max_connections(1)
        .connect(cli.resolved_database_url()?)
        .await?;

    let result = enqueue_maintenance_job(&pool, &cli, Utc::now()).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn enqueue_maintenance_job(
    pool: &solver_worker::pgbouncer_sqlx::PgPool,
    cli: &Cli,
    now: DateTime<Utc>,
) -> anyhow::Result<Value> {
    let environment = cli.environment();
    let mode = if cli.execute { "execute" } else { "dry_run" };
    let payload = maintenance_payload(cli, &environment);
    let idempotency_key = cli.idempotency_key.clone().unwrap_or_else(|| {
        default_idempotency_key(cli.job_kind.job_kind(), &environment, mode, now)
    });
    let concurrency_key = cli
        .concurrency_key
        .clone()
        .unwrap_or_else(|| default_concurrency_key(cli.job_kind.job_kind(), &environment, mode));
    let queue_key = cli.queue_key.clone().unwrap_or(environment);
    let max_attempts = cli.max_attempts.unwrap_or(if cli.execute { 1 } else { 3 });

    let row = solver_worker::pgbouncer_sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.worker_enqueue_job(
          p_job_kind => $1,
          p_payload_json => $2::jsonb,
          p_payload_schema_version => $3,
          p_requested_by => $4,
          p_requester_type => $5,
          p_idempotency_key => $6,
          p_concurrency_key => $7,
          p_priority => $8,
          p_queue_key => $9,
          p_visibility => $10,
          p_max_attempts => $11
        ) AS result
        FROM _service_role
        ",
    )
    .bind(cli.job_kind.job_kind())
    .bind(payload)
    .bind(cli.job_kind.payload_schema_version())
    .bind(cli.requested_by)
    .bind(cli.requester_type())
    .bind(idempotency_key)
    .bind(concurrency_key)
    .bind(cli.priority)
    .bind(queue_key)
    .bind(cli.visibility())
    .bind(max_attempts)
    .fetch_one(pool)
    .await?;

    let result = row.try_get::<Value, _>("result")?;
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(result)
    } else {
        Err(anyhow::anyhow!(
            "worker_enqueue_job returned non-ok result: {result}"
        ))
    }
}

fn maintenance_payload(cli: &Cli, environment: &str) -> Value {
    let mut payload = Map::new();
    payload.insert("environment".to_owned(), json!(environment));
    payload.insert("execute".to_owned(), json!(cli.execute));

    match cli.job_kind {
        MaintenanceJobKind::Snapshot => {
            insert_i64(
                &mut payload,
                "snapshotRetentionDays",
                cli.snapshot_retention_days,
            );
            insert_i64(
                &mut payload,
                "orphanRetentionDays",
                cli.orphan_retention_days,
            );
            insert_i64(&mut payload, "maxSnapshots", cli.max_snapshots);
            insert_i64(&mut payload, "maxOrphanDirs", cli.max_orphan_dirs);
            insert_i64(&mut payload, "maxBytes", cli.max_bytes);
            insert_i64(&mut payload, "batchSize", cli.batch_size);
        }
        MaintenanceJobKind::Result => {
            insert_i64(&mut payload, "batchSize", cli.batch_size);
            insert_i64(&mut payload, "maxBatches", cli.max_batches);
        }
        MaintenanceJobKind::PackageArtifact => {
            insert_i64(&mut payload, "batchSize", cli.batch_size);
            insert_i64(&mut payload, "maxBatches", cli.max_batches);
            insert_i64(&mut payload, "jobRetentionDays", cli.job_retention_days);
            insert_i64(
                &mut payload,
                "requestCacheRetentionDays",
                cli.request_cache_retention_days,
            );
        }
        MaintenanceJobKind::ProcessFlowGraphCache => {
            insert_string(&mut payload, "buildId", cli.build_id.as_deref());
            insert_i64(&mut payload, "limitFlows", cli.limit_flows);
            insert_i64(&mut payload, "limitProcesses", cli.limit_processes);
            insert_i64(&mut payload, "maxEdges", cli.max_edges);
            insert_i64(&mut payload, "sourceRowLimit", cli.source_row_limit);
            insert_i64(&mut payload, "pageSize", cli.page_size);
            insert_string(&mut payload, "cachePrefix", cli.cache_prefix.as_deref());
            insert_string(&mut payload, "cacheBucket", cli.cache_bucket.as_deref());
        }
    }

    Value::Object(payload)
}

fn insert_i64(payload: &mut Map<String, Value>, key: &str, value: Option<i64>) {
    if let Some(value) = value {
        payload.insert(key.to_owned(), json!(value));
    }
}

fn insert_string(payload: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        payload.insert(key.to_owned(), json!(value));
    }
}

fn default_idempotency_key(
    job_kind: &str,
    environment: &str,
    mode: &str,
    now: DateTime<Utc>,
) -> String {
    if mode == "execute" {
        format!(
            "{job_kind}:{environment}:{mode}:{}",
            now.format("%Y%m%dT%H%M%SZ")
        )
    } else {
        format!("{job_kind}:{environment}:{mode}:{}", now.format("%Y-%m-%d"))
    }
}

fn default_concurrency_key(job_kind: &str, environment: &str, mode: &str) -> String {
    format!("{job_kind}:{environment}:{mode}")
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::{
        Cli, MaintenanceJobKind, default_concurrency_key, default_idempotency_key,
        maintenance_payload,
    };

    fn base_cli(job_kind: MaintenanceJobKind) -> Cli {
        Cli {
            job_kind,
            database_url: None,
            conn: None,
            environment: "main".to_owned(),
            execute: false,
            requested_by: None,
            requester_type: "system".to_owned(),
            visibility: "operator".to_owned(),
            idempotency_key: None,
            concurrency_key: None,
            queue_key: None,
            priority: 0,
            max_attempts: None,
            snapshot_retention_days: None,
            orphan_retention_days: None,
            max_snapshots: None,
            max_orphan_dirs: None,
            max_bytes: None,
            batch_size: None,
            max_batches: None,
            job_retention_days: None,
            request_cache_retention_days: None,
            build_id: None,
            limit_flows: None,
            limit_processes: None,
            max_edges: None,
            source_row_limit: None,
            page_size: None,
            cache_prefix: None,
            cache_bucket: None,
        }
    }

    #[test]
    fn builds_snapshot_gc_payload() {
        let mut cli = base_cli(MaintenanceJobKind::Snapshot);
        cli.snapshot_retention_days = Some(30);
        cli.orphan_retention_days = Some(14);
        cli.batch_size = Some(25);

        assert_eq!(
            maintenance_payload(&cli, "main"),
            json!({
                "environment": "main",
                "execute": false,
                "snapshotRetentionDays": 30,
                "orphanRetentionDays": 14,
                "batchSize": 25
            })
        );
    }

    #[test]
    fn builds_package_execute_payload() {
        let mut cli = base_cli(MaintenanceJobKind::PackageArtifact);
        cli.execute = true;
        cli.batch_size = Some(100);
        cli.job_retention_days = Some(30);
        cli.request_cache_retention_days = Some(7);

        assert_eq!(
            maintenance_payload(&cli, "main"),
            json!({
                "environment": "main",
                "execute": true,
                "batchSize": 100,
                "jobRetentionDays": 30,
                "requestCacheRetentionDays": 7
            })
        );
    }

    #[test]
    fn default_keys_split_dry_run_and_execute() {
        let now = chrono::Utc
            .with_ymd_and_hms(2026, 5, 31, 12, 34, 56)
            .unwrap();

        assert_eq!(
            default_idempotency_key("lca.snapshot_gc", "main", "dry_run", now),
            "lca.snapshot_gc:main:dry_run:2026-05-31"
        );
        assert_eq!(
            default_idempotency_key("lca.snapshot_gc", "main", "execute", now),
            "lca.snapshot_gc:main:execute:20260531T123456Z"
        );
        assert_eq!(
            default_concurrency_key("lca.snapshot_gc", "main", "execute"),
            "lca.snapshot_gc:main:execute"
        );
    }

    #[test]
    fn builds_process_flow_graph_cache_payload() {
        let mut cli = base_cli(MaintenanceJobKind::ProcessFlowGraphCache);
        cli.build_id = Some("process-flow-graph-test".to_owned());
        cli.limit_flows = Some(10);
        cli.limit_processes = Some(20);
        cli.max_edges = Some(100);
        cli.source_row_limit = Some(250);
        cli.page_size = Some(250);
        cli.cache_prefix = Some("national-carbon/process-flow-graph/v1".to_owned());

        assert_eq!(
            maintenance_payload(&cli, "dev"),
            json!({
                "environment": "dev",
                "execute": false,
                "buildId": "process-flow-graph-test",
                "limitFlows": 10,
                "limitProcesses": 20,
                "maxEdges": 100,
                "sourceRowLimit": 250,
                "pageSize": 250,
                "cachePrefix": "national-carbon/process-flow-graph/v1"
            })
        );
    }
}
