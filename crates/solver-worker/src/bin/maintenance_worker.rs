use std::{path::PathBuf, process::Command, time::Duration};

use clap::Parser;
use serde_json::{Map, Value, json};
use solver_worker::{
    pgbouncer_sqlx::{self as sqlx, Row, postgres::PgPoolOptions},
    worker_jobs::{
        WorkerJob, WorkerJobResult, claim_worker_jobs, heartbeat_worker_job,
        record_worker_job_result,
    },
};
use tokio::time::sleep;
use tracing::{error, info, instrument};
use uuid::Uuid;

const MAINTENANCE_WORKER_QUEUE: &str = "maintenance";
const SNAPSHOT_GC_JOB_KIND: &str = "lca.snapshot_gc";
const RESULT_GC_JOB_KIND: &str = "lca.result_gc";
const PACKAGE_ARTIFACT_GC_JOB_KIND: &str = "tidas.package_artifact_gc";
const SNAPSHOT_GC_PAYLOAD_SCHEMA_VERSION: &str = "lca.snapshot_gc.request.v1";
const RESULT_GC_PAYLOAD_SCHEMA_VERSION: &str = "lca.result_gc.request.v1";
const PACKAGE_ARTIFACT_GC_PAYLOAD_SCHEMA_VERSION: &str = "tidas.package_artifact_gc.request.v1";
const SNAPSHOT_GC_RESULT_SCHEMA_VERSION: &str = "lca.snapshot_gc.result.v1";
const RESULT_GC_RESULT_SCHEMA_VERSION: &str = "lca.result_gc.result.v1";
const PACKAGE_ARTIFACT_GC_RESULT_SCHEMA_VERSION: &str = "tidas.package_artifact_gc.result.v1";

#[derive(Debug, Clone, Parser)]
#[command(name = "maintenance-worker")]
struct Cli {
    /// `PostgreSQL` URL (preferred env: `DATABASE_URL`, fallback: `CONN`).
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    /// `PostgreSQL` URL fallback used by this project in local `.env`.
    #[arg(long, env = "CONN")]
    conn: Option<String>,
    /// Stable worker id recorded on claimed maintenance `worker_jobs` rows.
    #[arg(long, env = "MAINTENANCE_WORKER_ID")]
    worker_id: Option<String>,
    /// Number of maintenance `worker_jobs` rows to claim per poll.
    #[arg(
        long,
        env = "MAINTENANCE_WORKER_JOBS_CLAIM_LIMIT",
        default_value_t = 1_i32
    )]
    claim_limit: i32,
    /// Lease seconds used when claiming or heartbeating maintenance `worker_jobs` rows.
    #[arg(
        long,
        env = "MAINTENANCE_WORKER_JOBS_LEASE_SECONDS",
        default_value_t = 3_600_i32
    )]
    lease_seconds: i32,
    /// Poll interval for maintenance worker (ms).
    #[arg(long, env = "MAINTENANCE_WORKER_POLL_MS", default_value_t = 5_000_u64)]
    poll_ms: u64,
    /// Maximum number of DB connections held by this worker.
    #[arg(
        long,
        env = "MAINTENANCE_WORKER_DB_MAX_CONNECTIONS",
        default_value_t = 2_u32
    )]
    db_max_connections: u32,
}

impl Cli {
    fn resolved_database_url(&self) -> anyhow::Result<&str> {
        self.database_url
            .as_deref()
            .or(self.conn.as_deref())
            .ok_or_else(|| anyhow::anyhow!("missing DB connection: set DATABASE_URL or CONN"))
    }

    fn worker_id(&self) -> String {
        self.worker_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map_or_else(
                || format!("maintenance-worker-{}", std::process::id()),
                str::to_owned,
            )
    }

    fn claim_limit(&self) -> i32 {
        self.claim_limit.clamp(1, 50)
    }

    fn lease_seconds(&self) -> i32 {
        self.lease_seconds.clamp(1, 86_400)
    }

    const fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaintenanceCommand {
    job_kind: String,
    binary_name: &'static str,
    binary_path: PathBuf,
    args: Vec<String>,
    execute: bool,
    payload_schema_version: &'static str,
    result_schema_version: &'static str,
}

#[derive(Debug, Clone)]
struct MaintenanceCommandOutput {
    success: bool,
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let pool = PgPoolOptions::new()
        .max_connections(cli.db_max_connections.max(1))
        .connect(cli.resolved_database_url()?)
        .await?;

    run_maintenance_worker_loop(
        &pool,
        cli.worker_id(),
        cli.claim_limit(),
        cli.lease_seconds(),
        cli.poll_interval(),
    )
    .await
}

#[instrument(skip(pool))]
async fn run_maintenance_worker_loop(
    pool: &sqlx::PgPool,
    worker_id: String,
    claim_limit: i32,
    lease_seconds: i32,
    poll_interval: Duration,
) -> anyhow::Result<()> {
    loop {
        match claim_worker_jobs(
            pool,
            MAINTENANCE_WORKER_QUEUE,
            &worker_id,
            claim_limit,
            lease_seconds,
        )
        .await
        {
            Ok(jobs) if jobs.is_empty() => {
                sleep(poll_interval).await;
            }
            Ok(jobs) => {
                for job in jobs {
                    process_maintenance_worker_job(pool, job, lease_seconds).await;
                }
            }
            Err(err) => {
                error!(error = %err, "maintenance worker_jobs claim error");
                sleep(poll_interval).await;
            }
        }
    }
}

async fn process_maintenance_worker_job(pool: &sqlx::PgPool, job: WorkerJob, lease_seconds: i32) {
    let command = match maintenance_command_for_job(&job) {
        Ok(command) => command,
        Err(err) => {
            record_invalid_maintenance_job(pool, &job, &err.to_string()).await;
            return;
        }
    };

    if let Err(err) = heartbeat_worker_job(
        pool,
        job.id,
        job.lease_token,
        command.binary_name,
        0.05,
        Some(json!({
            "jobKind": command.job_kind,
            "binary": command.binary_name,
            "args": command.args,
            "execute": command.execute,
        })),
        lease_seconds,
    )
    .await
    {
        error!(error = %err, worker_job_id = %job.id, "failed to heartbeat maintenance worker job before execution");
        return;
    }

    match run_maintenance_command(command.clone()).await {
        Ok(output) if output.success => {
            record_maintenance_success(pool, &job, &command, output).await;
        }
        Ok(output) => {
            record_maintenance_failure(pool, &job, &command, output).await;
        }
        Err(err) => {
            let output = MaintenanceCommandOutput {
                success: false,
                code: None,
                stdout: String::new(),
                stderr: err.to_string(),
            };
            record_maintenance_failure(pool, &job, &command, output).await;
        }
    }
}

async fn run_maintenance_command(
    command: MaintenanceCommand,
) -> anyhow::Result<MaintenanceCommandOutput> {
    tokio::task::spawn_blocking(move || {
        let output = Command::new(&command.binary_path)
            .args(&command.args)
            .output()?;
        Ok::<_, anyhow::Error>(MaintenanceCommandOutput {
            success: output.status.success(),
            code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    })
    .await?
}

async fn record_invalid_maintenance_job(pool: &sqlx::PgPool, job: &WorkerJob, err_message: &str) {
    let result = WorkerJobResult::failed(
        "invalid_maintenance_worker_job_payload",
        err_message.to_owned(),
        json!({
            "workerJobId": job.id,
            "jobKind": job.job_kind,
            "payloadSchemaVersion": job.payload_schema_version,
        }),
        Some(json!({"error": err_message})),
        None,
    );
    if let Err(record_err) = record_worker_job_result(pool, job.id, job.lease_token, result).await {
        error!(error = %record_err, worker_job_id = %job.id, "failed to record invalid maintenance worker job");
    }
}

async fn record_maintenance_success(
    pool: &sqlx::PgPool,
    job: &WorkerJob,
    command: &MaintenanceCommand,
    output: MaintenanceCommandOutput,
) {
    let summary = parse_summary_line(&output.stdout);
    let (result_ref, report_artifact_error) =
        maintenance_report_artifact_ref(pool, job, command, &output, &summary).await;
    let result = WorkerJobResult {
        status: "completed".to_owned(),
        result_json: Some(json!({
            "jobKind": command.job_kind,
            "binary": command.binary_name,
            "args": command.args,
            "execute": command.execute,
            "exitCode": output.code,
            "summary": summary,
        })),
        result_schema_version: Some(command.result_schema_version.to_owned()),
        result_ref,
        diagnostics: Some(maintenance_diagnostics(&output, report_artifact_error)),
        error_code: None,
        error_message: None,
        error_details: None,
        blocker_codes: Vec::new(),
        resolution_scope: None,
        retryable: None,
    };
    if let Err(record_err) = record_worker_job_result(pool, job.id, job.lease_token, result).await {
        error!(error = %record_err, worker_job_id = %job.id, "failed to record maintenance worker success");
    } else {
        info!(worker_job_id = %job.id, job_kind = %command.job_kind, "maintenance worker job completed");
    }
}

async fn record_maintenance_failure(
    pool: &sqlx::PgPool,
    job: &WorkerJob,
    command: &MaintenanceCommand,
    output: MaintenanceCommandOutput,
) {
    let retryable = !command.execute;
    let summary = parse_summary_line(&output.stdout);
    let (result_ref, report_artifact_error) =
        maintenance_report_artifact_ref(pool, job, command, &output, &summary).await;
    let message = tail_lines(
        if output.stderr.trim().is_empty() {
            &output.stdout
        } else {
            &output.stderr
        },
        20,
    )
    .join("\n");
    let result = WorkerJobResult {
        status: "failed".to_owned(),
        result_json: Some(json!({
            "jobKind": command.job_kind,
            "binary": command.binary_name,
            "args": command.args,
            "execute": command.execute,
            "exitCode": output.code,
            "summary": summary,
        })),
        result_schema_version: Some(command.result_schema_version.to_owned()),
        result_ref,
        diagnostics: Some(maintenance_diagnostics(&output, report_artifact_error)),
        error_code: Some("maintenance_worker_job_failed".to_owned()),
        error_message: Some(if message.is_empty() {
            "maintenance worker job failed".to_owned()
        } else {
            message
        }),
        error_details: Some(json!({
            "workerJobId": job.id,
            "jobKind": command.job_kind,
            "binary": command.binary_name,
            "args": command.args,
            "exitCode": output.code,
        })),
        blocker_codes: Vec::new(),
        resolution_scope: None,
        retryable: Some(retryable),
    };
    if let Err(record_err) = record_worker_job_result(pool, job.id, job.lease_token, result).await {
        error!(error = %record_err, worker_job_id = %job.id, "failed to record maintenance worker failure");
    }
}

async fn maintenance_report_artifact_ref(
    pool: &sqlx::PgPool,
    job: &WorkerJob,
    command: &MaintenanceCommand,
    output: &MaintenanceCommandOutput,
    summary: &Value,
) -> (Option<Value>, Option<String>) {
    match insert_maintenance_report_artifact(pool, job, command, output, summary).await {
        Ok(result_ref) => (Some(result_ref), None),
        Err(err) => {
            error!(error = %err, worker_job_id = %job.id, "failed to insert maintenance worker report artifact");
            (None, Some(err.to_string()))
        }
    }
}

async fn insert_maintenance_report_artifact(
    pool: &sqlx::PgPool,
    job: &WorkerJob,
    command: &MaintenanceCommand,
    output: &MaintenanceCommandOutput,
    summary: &Value,
) -> anyhow::Result<Value> {
    let metadata = json!({
        "jobKind": command.job_kind,
        "binary": command.binary_name,
        "args": command.args,
        "execute": command.execute,
        "exitCode": output.code,
        "success": output.success,
        "summary": summary,
        "stdoutTail": tail_lines(&output.stdout, 200),
        "stderrTail": tail_lines(&output.stderr, 200),
    });

    let row = sqlx::query(
        r"
        INSERT INTO public.worker_job_artifacts (
            job_id,
            artifact_type,
            content_type,
            metadata,
            visibility
        )
        VALUES ($1, 'maintenance_gc_report', 'application/json', $2::jsonb, 'operator')
        RETURNING id
        ",
    )
    .bind(job.id)
    .bind(metadata)
    .fetch_one(pool)
    .await?;

    let artifact_id = row.try_get::<Uuid, _>("id")?;
    Ok(json!({
        "artifactId": artifact_id,
        "artifactType": "maintenance_gc_report",
        "visibility": "operator",
    }))
}

fn maintenance_diagnostics(
    output: &MaintenanceCommandOutput,
    report_artifact_error: Option<String>,
) -> Value {
    let mut diagnostics = Map::new();
    diagnostics.insert(
        "stdoutTail".to_owned(),
        json!(tail_lines(&output.stdout, 100)),
    );
    diagnostics.insert(
        "stderrTail".to_owned(),
        json!(tail_lines(&output.stderr, 100)),
    );
    if let Some(error) = report_artifact_error {
        diagnostics.insert("reportArtifactError".to_owned(), json!(error));
    }
    Value::Object(diagnostics)
}

fn maintenance_command_for_job(job: &WorkerJob) -> anyhow::Result<MaintenanceCommand> {
    if job.worker_queue != MAINTENANCE_WORKER_QUEUE {
        return Err(anyhow::anyhow!(
            "unsupported worker queue for maintenance job: {}",
            job.worker_queue
        ));
    }

    let payload = payload_object(&job.payload)?;
    let execute = payload_bool(payload, &["execute"]).unwrap_or(false)
        && !payload_bool(payload, &["dryRun", "dry_run"]).unwrap_or(false);

    let (binary_name, payload_schema_version, result_schema_version, args) =
        match job.job_kind.as_str() {
            SNAPSHOT_GC_JOB_KIND => (
                "snapshot_gc",
                SNAPSHOT_GC_PAYLOAD_SCHEMA_VERSION,
                SNAPSHOT_GC_RESULT_SCHEMA_VERSION,
                snapshot_gc_args(payload, execute),
            ),
            RESULT_GC_JOB_KIND => (
                "result_gc",
                RESULT_GC_PAYLOAD_SCHEMA_VERSION,
                RESULT_GC_RESULT_SCHEMA_VERSION,
                result_gc_args(payload, execute),
            ),
            PACKAGE_ARTIFACT_GC_JOB_KIND => (
                "package_gc",
                PACKAGE_ARTIFACT_GC_PAYLOAD_SCHEMA_VERSION,
                PACKAGE_ARTIFACT_GC_RESULT_SCHEMA_VERSION,
                package_gc_args(payload, execute),
            ),
            _ => {
                return Err(anyhow::anyhow!(
                    "unsupported maintenance worker job kind: {}",
                    job.job_kind
                ));
            }
        };

    if job.payload_schema_version != payload_schema_version {
        return Err(anyhow::anyhow!(
            "unsupported maintenance payload schema for {}: {}",
            job.job_kind,
            job.payload_schema_version
        ));
    }

    Ok(MaintenanceCommand {
        job_kind: job.job_kind.clone(),
        binary_name,
        binary_path: resolve_maintenance_binary(binary_name),
        args,
        execute,
        payload_schema_version,
        result_schema_version,
    })
}

fn snapshot_gc_args(payload: &Map<String, Value>, execute: bool) -> Vec<String> {
    let mut args = Vec::new();
    push_i64_arg(
        &mut args,
        "--snapshot-retention-days",
        payload,
        &["snapshotRetentionDays", "snapshot_retention_days"],
    );
    push_i64_arg(
        &mut args,
        "--orphan-retention-days",
        payload,
        &["orphanRetentionDays", "orphan_retention_days"],
    );
    push_i64_arg(
        &mut args,
        "--max-snapshots",
        payload,
        &["maxSnapshots", "max_snapshots"],
    );
    push_i64_arg(
        &mut args,
        "--max-orphan-dirs",
        payload,
        &["maxOrphanDirs", "max_orphan_dirs"],
    );
    push_i64_arg(
        &mut args,
        "--max-bytes",
        payload,
        &["maxBytes", "max_bytes"],
    );
    push_i64_arg(
        &mut args,
        "--batch-size",
        payload,
        &["batchSize", "batch_size"],
    );
    if execute {
        args.push("--execute".to_owned());
    }
    args
}

fn result_gc_args(payload: &Map<String, Value>, execute: bool) -> Vec<String> {
    let mut args = Vec::new();
    push_i64_arg(
        &mut args,
        "--batch-size",
        payload,
        &["batchSize", "batch_size"],
    );
    push_i64_arg(
        &mut args,
        "--max-batches",
        payload,
        &["maxBatches", "max_batches"],
    );
    if !execute {
        args.push("--dry-run".to_owned());
    }
    args
}

fn package_gc_args(payload: &Map<String, Value>, execute: bool) -> Vec<String> {
    let mut args = Vec::new();
    push_i64_arg(
        &mut args,
        "--batch-size",
        payload,
        &["batchSize", "batch_size"],
    );
    push_i64_arg(
        &mut args,
        "--max-batches",
        payload,
        &["maxBatches", "max_batches"],
    );
    push_i64_arg(
        &mut args,
        "--job-retention-days",
        payload,
        &["jobRetentionDays", "job_retention_days"],
    );
    push_i64_arg(
        &mut args,
        "--request-cache-retention-days",
        payload,
        &["requestCacheRetentionDays", "request_cache_retention_days"],
    );
    if execute {
        args.push("--execute".to_owned());
    }
    args
}

fn payload_object(value: &Value) -> anyhow::Result<&Map<String, Value>> {
    let Value::Object(payload) = value else {
        return Err(anyhow::anyhow!(
            "maintenance worker job payload must be an object"
        ));
    };
    Ok(payload)
}

fn payload_bool(payload: &Map<String, Value>, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .find_map(|key| payload.get(*key).and_then(Value::as_bool))
}

fn payload_i64(payload: &Map<String, Value>, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .find_map(|key| payload.get(*key).and_then(Value::as_i64))
}

fn push_i64_arg(args: &mut Vec<String>, flag: &str, payload: &Map<String, Value>, keys: &[&str]) {
    if let Some(value) = payload_i64(payload, keys) {
        args.push(flag.to_owned());
        args.push(value.to_string());
    }
}

fn resolve_maintenance_binary(binary_name: &str) -> PathBuf {
    if let Ok(path) = std::env::var(format!(
        "MAINTENANCE_WORKER_{}_BIN",
        binary_name.to_ascii_uppercase()
    )) && !path.trim().is_empty()
    {
        return PathBuf::from(path);
    }

    if let Ok(bin_dir) = std::env::var("MAINTENANCE_WORKER_BIN_DIR")
        && !bin_dir.trim().is_empty()
    {
        return PathBuf::from(bin_dir).join(binary_name);
    }

    if let Ok(current_exe) = std::env::current_exe()
        && let Some(parent) = current_exe.parent()
    {
        let sibling = parent.join(binary_name);
        if sibling.exists() {
            return sibling;
        }
    }

    PathBuf::from(binary_name)
}

fn parse_summary_line(stdout: &str) -> Value {
    let Some(line) = stdout
        .lines()
        .rev()
        .find(|line| line.trim_start().starts_with("[summary]"))
    else {
        return json!({});
    };
    let summary = line.trim_start().trim_start_matches("[summary]").trim();
    let mut result = Map::new();
    for part in summary.split_whitespace() {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        result.insert(key.to_owned(), parse_summary_value(value));
    }
    Value::Object(result)
}

fn parse_summary_value(value: &str) -> Value {
    match value {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => value
            .parse::<i64>()
            .map_or_else(|_| Value::String(value.to_owned()), |parsed| json!(parsed)),
    }
}

fn tail_lines(value: &str, max_lines: usize) -> Vec<String> {
    let lines = value.lines().map(str::to_owned).collect::<Vec<String>>();
    let start = lines.len().saturating_sub(max_lines);
    lines.into_iter().skip(start).collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        PACKAGE_ARTIFACT_GC_JOB_KIND, PACKAGE_ARTIFACT_GC_PAYLOAD_SCHEMA_VERSION,
        RESULT_GC_JOB_KIND, RESULT_GC_PAYLOAD_SCHEMA_VERSION, SNAPSHOT_GC_JOB_KIND,
        SNAPSHOT_GC_PAYLOAD_SCHEMA_VERSION, maintenance_command_for_job, parse_summary_line,
    };
    use solver_worker::worker_jobs::WorkerJob;
    use uuid::Uuid;

    fn worker_job(
        job_kind: &str,
        payload_schema_version: &str,
        payload: serde_json::Value,
    ) -> WorkerJob {
        WorkerJob {
            id: Uuid::new_v4(),
            job_kind: job_kind.to_owned(),
            worker_queue: "maintenance".to_owned(),
            payload_schema_version: payload_schema_version.to_owned(),
            payload,
            requested_by: None,
            lease_token: Uuid::new_v4(),
            attempt_count: 1,
        }
    }

    #[test]
    fn maps_snapshot_gc_payload_to_safe_dry_run_args() {
        let job = worker_job(
            SNAPSHOT_GC_JOB_KIND,
            SNAPSHOT_GC_PAYLOAD_SCHEMA_VERSION,
            json!({
                "snapshotRetentionDays": 30,
                "orphanRetentionDays": 14,
                "batchSize": 25
            }),
        );

        let command = maintenance_command_for_job(&job).expect("command");

        assert_eq!(command.binary_name, "snapshot_gc");
        assert!(!command.execute);
        assert_eq!(
            command.args,
            vec![
                "--snapshot-retention-days",
                "30",
                "--orphan-retention-days",
                "14",
                "--batch-size",
                "25"
            ]
        );
    }

    #[test]
    fn maps_result_gc_dry_run_to_existing_cli_flag() {
        let job = worker_job(
            RESULT_GC_JOB_KIND,
            RESULT_GC_PAYLOAD_SCHEMA_VERSION,
            json!({
                "batchSize": 10,
                "maxBatches": 1
            }),
        );

        let command = maintenance_command_for_job(&job).expect("command");

        assert_eq!(command.binary_name, "result_gc");
        assert!(!command.execute);
        assert_eq!(
            command.args,
            vec!["--batch-size", "10", "--max-batches", "1", "--dry-run"]
        );
    }

    #[test]
    fn maps_package_gc_execute_to_existing_cli_flag() {
        let job = worker_job(
            PACKAGE_ARTIFACT_GC_JOB_KIND,
            PACKAGE_ARTIFACT_GC_PAYLOAD_SCHEMA_VERSION,
            json!({
                "execute": true,
                "batchSize": 100,
                "jobRetentionDays": 30,
                "requestCacheRetentionDays": 7
            }),
        );

        let command = maintenance_command_for_job(&job).expect("command");

        assert_eq!(command.binary_name, "package_gc");
        assert!(command.execute);
        assert_eq!(
            command.args,
            vec![
                "--batch-size",
                "100",
                "--job-retention-days",
                "30",
                "--request-cache-retention-days",
                "7",
                "--execute"
            ]
        );
    }

    #[test]
    fn parses_summary_line_values() {
        let summary = parse_summary_line(
            "[info] ignored\n[summary] dry_run=true batches=2 candidates=42 status=ok\n",
        );

        assert_eq!(summary["dry_run"], true);
        assert_eq!(summary["batches"], 2);
        assert_eq!(summary["candidates"], 42);
        assert_eq!(summary["status"], "ok");
    }
}
