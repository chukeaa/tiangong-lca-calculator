use std::sync::Arc;

use clap::{Parser, ValueEnum};
use serde_json::{Map, Value, json};
use solver_worker::{
    config::AppConfig,
    db::{AppState, archive_queue_message, read_one_queue_message},
    db_pool::{APP_PACKAGE_WORKER, APP_PACKAGE_WORKER_QUEUE},
    package_db::{
        PackageJobContinuation, extract_package_job_id, extract_package_job_id_from_raw_payload,
        handle_package_job_payload, handle_package_job_payload_once,
        is_retryable_package_job_error, mark_package_request_cache_failed,
        reschedule_retryable_package_job, update_package_job_status,
    },
    package_execution::clear_runtime_export_traversal_cache,
    package_retention::refresh_import_source_retention,
    package_types::{
        PACKAGE_EXPORT_PAYLOAD_SCHEMA_VERSION, PACKAGE_EXPORT_RESULT_SCHEMA_VERSION,
        PACKAGE_EXPORT_WORKER_JOB_KIND, PACKAGE_IMPORT_PAYLOAD_SCHEMA_VERSION,
        PACKAGE_IMPORT_RESULT_SCHEMA_VERSION, PACKAGE_IMPORT_WORKER_JOB_KIND, PACKAGE_QUEUE_NAME,
        PACKAGE_WORKER_QUEUE, PackageJobPayload,
    },
    pgbouncer_sqlx::{self as sqlx, Row},
    storage::ObjectStoreUploadError,
    worker_jobs::{
        WorkerJob, WorkerJobResult, claim_worker_jobs, heartbeat_worker_job,
        record_worker_job_result,
    },
};
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Parser)]
#[command(name = "package-worker")]
struct PackageWorkerCli {
    #[command(flatten)]
    app: AppConfig,
    /// Package worker queue backend override.
    #[arg(long, env = "PACKAGE_QUEUE_BACKEND", default_value = "worker-jobs")]
    package_queue_backend: PackageQueueBackend,
    /// Stable worker id recorded on claimed package `worker_jobs` rows.
    #[arg(long, env = "PACKAGE_WORKER_ID")]
    package_worker_id: Option<String>,
    /// Number of package `worker_jobs` rows to claim per poll.
    #[arg(long, env = "PACKAGE_WORKER_JOBS_CLAIM_LIMIT", default_value_t = 1_i32)]
    package_worker_jobs_claim_limit: i32,
    /// Lease seconds used when claiming or heartbeating package `worker_jobs` rows.
    #[arg(
        long,
        env = "PACKAGE_WORKER_JOBS_LEASE_SECONDS",
        default_value_t = 900_i32
    )]
    package_worker_jobs_lease_seconds: i32,
}

/// Queue backend used by package worker mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PackageQueueBackend {
    /// Legacy `pgmq` queue payloads.
    Pgmq,
    /// Unified `public.worker_jobs` queue payloads.
    WorkerJobs,
}

impl PackageWorkerCli {
    fn worker_id(&self) -> String {
        self.package_worker_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map_or_else(
                || format!("package-worker-{}", std::process::id()),
                str::to_owned,
            )
    }

    fn worker_jobs_claim_limit(&self) -> i32 {
        self.package_worker_jobs_claim_limit.clamp(1, 50)
    }

    fn worker_jobs_lease_seconds(&self) -> i32 {
        self.package_worker_jobs_lease_seconds.clamp(1, 86_400)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = PackageWorkerCli::parse();
    let state = Arc::new(
        AppState::new_with_application_names(
            &cli.app,
            APP_PACKAGE_WORKER,
            APP_PACKAGE_WORKER_QUEUE,
        )
        .await?,
    );

    match cli.package_queue_backend {
        PackageQueueBackend::Pgmq => {
            cli.app
                .require_legacy_job_table_backend_allowed("package pgmq backend")?;
            let queue_name = resolve_queue_name(&cli.app.pgmq_queue);

            if cli.app.pgmq_queue == "lca_jobs" {
                info!(
                    queue = %queue_name,
                    "using package queue default instead of solver-worker queue default"
                );
            }

            run_package_worker_loop(
                state,
                queue_name,
                cli.app.worker_vt_seconds,
                cli.app.poll_interval(),
            )
            .await
        }
        PackageQueueBackend::WorkerJobs => {
            run_package_worker_jobs_loop(
                state,
                cli.worker_id(),
                cli.worker_jobs_claim_limit(),
                cli.worker_jobs_lease_seconds(),
                cli.app.poll_interval(),
            )
            .await
        }
    }
}

#[instrument(skip(state))]
#[allow(clippy::too_many_lines)]
async fn run_package_worker_loop(
    state: Arc<AppState>,
    queue_name: String,
    vt_seconds: i32,
    poll_interval: std::time::Duration,
) -> anyhow::Result<()> {
    loop {
        match read_one_queue_message(&state.queue_pool, &queue_name, vt_seconds).await {
            Ok(Some(message)) => {
                let parsed = serde_json::from_value::<PackageJobPayload>(message.payload.clone());
                match parsed {
                    Ok(payload) => {
                        if let Err(err) = handle_package_job_payload(&state, payload.clone()).await
                        {
                            error!(error = %err, "package job execution failed");
                            let job_id = extract_package_job_id(&payload);
                            let err_message = err.to_string();
                            let mut rescheduled = false;
                            clear_runtime_export_traversal_cache(job_id);
                            if is_retryable_package_job_error(&err) {
                                match reschedule_retryable_package_job(
                                    &state.pool,
                                    &payload,
                                    &err_message,
                                )
                                .await
                                {
                                    Ok(true) => {
                                        rescheduled = true;
                                    }
                                    Ok(false) => {
                                        warn!(
                                            job_id = %job_id,
                                            error = %err_message,
                                            "package job retry budget exhausted"
                                        );
                                    }
                                    Err(retry_err) => {
                                        warn!(
                                            job_id = %job_id,
                                            error = %retry_err,
                                            original_error = %err_message,
                                            "failed to reschedule retryable package job"
                                        );
                                    }
                                }
                            }
                            if !rescheduled {
                                let diagnostics =
                                    build_package_job_failure_diagnostics(&payload, &err);
                                let cache_error_code =
                                    package_request_cache_error_code(&err).to_owned();
                                let cache_error_message =
                                    package_request_cache_error_message(&payload, &err);
                                let _ = update_package_job_status(
                                    &state.pool,
                                    job_id,
                                    "failed",
                                    diagnostics,
                                )
                                .await;
                                let _ = mark_package_request_cache_failed(
                                    &state.pool,
                                    job_id,
                                    cache_error_code.as_str(),
                                    cache_error_message.as_str(),
                                )
                                .await;
                                if let PackageJobPayload::ImportPackage {
                                    source_artifact_id, ..
                                } = payload
                                    && let Err(err) = refresh_import_source_retention(
                                        &state.pool,
                                        source_artifact_id,
                                    )
                                    .await
                                {
                                    warn!(
                                        job_id = %job_id,
                                        source_artifact_id = %source_artifact_id,
                                        error = %err,
                                        "failed to refresh import source retention after failed import job"
                                    );
                                }
                            }
                        } else {
                            info!("package job completed");
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, "invalid package job payload");
                        if let Some(job_id) =
                            extract_package_job_id_from_raw_payload(&message.payload)
                        {
                            let err_message = format!("invalid package job payload: {err}");
                            let _ = update_package_job_status(
                                &state.pool,
                                job_id,
                                "failed",
                                json!({"error": err_message}),
                            )
                            .await;
                            let _ = mark_package_request_cache_failed(
                                &state.pool,
                                job_id,
                                "invalid_job_payload",
                                &err_message,
                            )
                            .await;
                        }
                    }
                }

                if let Err(err) =
                    archive_queue_message(&state.queue_pool, &queue_name, message.msg_id).await
                {
                    error!(error = %err, msg_id = message.msg_id, "failed to archive queue message");
                }
            }
            Ok(None) => {
                sleep(poll_interval).await;
            }
            Err(err) => {
                error!(error = %err, "package queue read error");
                sleep(poll_interval).await;
            }
        }
    }
}

#[instrument(skip(state))]
async fn run_package_worker_jobs_loop(
    state: Arc<AppState>,
    worker_id: String,
    claim_limit: i32,
    lease_seconds: i32,
    poll_interval: std::time::Duration,
) -> anyhow::Result<()> {
    loop {
        match claim_worker_jobs(
            &state.pool,
            PACKAGE_WORKER_QUEUE,
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
                    process_package_worker_job(&state, job, lease_seconds, poll_interval).await;
                }
            }
            Err(err) => {
                error!(error = %err, "package worker_jobs claim error");
                sleep(poll_interval).await;
            }
        }
    }
}

async fn process_package_worker_job(
    state: &AppState,
    job: WorkerJob,
    lease_seconds: i32,
    poll_interval: std::time::Duration,
) {
    let mut payload = match package_worker_job_payload(&job) {
        Ok(payload) => payload,
        Err(err) => {
            record_invalid_package_worker_job_payload(state, &job, &err.to_string()).await;
            return;
        }
    };

    if heartbeat_package_worker_job(state, &job, &payload, lease_seconds, None, 0.05)
        .await
        .is_err()
    {
        return;
    }

    loop {
        match handle_package_job_payload_once(state, payload.clone()).await {
            Ok(PackageJobContinuation::Complete) => {
                record_package_worker_job_success(state, &job, &payload).await;
                return;
            }
            Ok(PackageJobContinuation::Continue {
                next_payload,
                diagnostics,
            }) => {
                let progress = package_progress_from_diagnostics(&diagnostics, 0.25);
                if heartbeat_package_worker_job(
                    state,
                    &job,
                    &next_payload,
                    lease_seconds,
                    Some(diagnostics),
                    progress,
                )
                .await
                .is_err()
                {
                    return;
                }
                payload = next_payload;
                sleep(std::cmp::min(
                    poll_interval,
                    std::time::Duration::from_millis(500),
                ))
                .await;
            }
            Err(err) => {
                record_package_worker_job_failure(state, &job, &payload, &err).await;
                return;
            }
        }
    }
}

async fn heartbeat_package_worker_job(
    state: &AppState,
    job: &WorkerJob,
    payload: &PackageJobPayload,
    lease_seconds: i32,
    diagnostics: Option<Value>,
    progress: f64,
) -> anyhow::Result<()> {
    let package_job_id = extract_package_job_id(payload);
    let phase = package_payload_type_name(payload);
    if let Err(err) = heartbeat_worker_job(
        &state.pool,
        job.id,
        job.lease_token,
        phase,
        progress,
        diagnostics.or_else(|| {
            Some(json!({
                "packageJobId": package_job_id,
                "payloadType": phase,
            }))
        }),
        lease_seconds,
    )
    .await
    {
        error!(
            error = %err,
            worker_job_id = %job.id,
            package_job_id = %package_job_id,
            "failed to heartbeat package worker_jobs row"
        );
        return Err(err);
    }

    Ok(())
}

async fn record_invalid_package_worker_job_payload(
    state: &AppState,
    job: &WorkerJob,
    err_message: &str,
) {
    error!(
        error = %err_message,
        worker_job_id = %job.id,
        job_kind = %job.job_kind,
        "invalid package worker_jobs payload"
    );
    let result = WorkerJobResult::failed(
        "invalid_package_worker_job_payload",
        err_message.to_owned(),
        json!({
            "workerJobId": job.id,
            "jobKind": job.job_kind,
            "payloadSchemaVersion": job.payload_schema_version,
        }),
        Some(json!({"error": err_message})),
        None,
    );
    if let Err(record_err) =
        record_worker_job_result(&state.pool, job.id, job.lease_token, result).await
    {
        error!(error = %record_err, worker_job_id = %job.id, "failed to record invalid package worker_jobs payload");
    }
}

async fn record_package_worker_job_failure(
    state: &AppState,
    job: &WorkerJob,
    payload: &PackageJobPayload,
    err: &anyhow::Error,
) {
    let package_job_id = extract_package_job_id(payload);
    let err_message = err.to_string();
    let diagnostics = build_package_job_failure_diagnostics(payload, err);
    let cache_error_code = package_request_cache_error_code(err).to_owned();
    let cache_error_message = package_request_cache_error_message(payload, err);
    clear_runtime_export_traversal_cache(package_job_id);

    let _ =
        update_package_job_status(&state.pool, package_job_id, "failed", diagnostics.clone()).await;
    let _ = mark_package_request_cache_failed(
        &state.pool,
        package_job_id,
        cache_error_code.as_str(),
        cache_error_message.as_str(),
    )
    .await;
    if let Err(err) = link_package_worker_job_domain_refs(&state.pool, job.id, package_job_id).await
    {
        warn!(
            error = %err,
            worker_job_id = %job.id,
            package_job_id = %package_job_id,
            "failed to link package domain rows to worker_jobs"
        );
    }
    if let PackageJobPayload::ImportPackage {
        source_artifact_id, ..
    } = payload
        && let Err(err) = refresh_import_source_retention(&state.pool, *source_artifact_id).await
    {
        warn!(
            job_id = %package_job_id,
            source_artifact_id = %source_artifact_id,
            error = %err,
            "failed to refresh import source retention after failed package worker_jobs job"
        );
    }

    let retryable = is_retryable_package_job_error(err);
    let result = WorkerJobResult {
        status: "failed".to_owned(),
        result_json: None,
        result_schema_version: None,
        result_ref: Some(package_worker_result_ref(job.id, package_job_id)),
        diagnostics: Some(diagnostics),
        error_code: Some(cache_error_code),
        error_message: Some(cache_error_message),
        error_details: Some(json!({
            "workerJobId": job.id,
            "packageJobId": package_job_id,
            "payloadType": package_payload_type_name(payload),
            "error": err_message,
        })),
        blocker_codes: Vec::new(),
        resolution_scope: None,
        retryable: Some(retryable),
    };
    if let Err(record_err) =
        record_worker_job_result(&state.pool, job.id, job.lease_token, result).await
    {
        error!(error = %record_err, worker_job_id = %job.id, "failed to record package worker_jobs failure");
    }
}

async fn record_package_worker_job_success(
    state: &AppState,
    job: &WorkerJob,
    payload: &PackageJobPayload,
) {
    let package_job_id = extract_package_job_id(payload);
    match build_package_worker_job_result(state, job.id, payload).await {
        Ok(result) => {
            if let Err(err) =
                record_worker_job_result(&state.pool, job.id, job.lease_token, result).await
            {
                error!(error = %err, worker_job_id = %job.id, package_job_id = %package_job_id, "failed to record package worker_jobs success");
            } else {
                info!(worker_job_id = %job.id, package_job_id = %package_job_id, "package worker_jobs job completed");
            }
        }
        Err(err) => {
            let err_message = err.to_string();
            let result = WorkerJobResult::failed(
                "package_worker_job_projection_failed",
                err_message.clone(),
                json!({
                    "workerJobId": job.id,
                    "packageJobId": package_job_id,
                    "payloadType": package_payload_type_name(payload),
                }),
                Some(json!({"error": err_message})),
                None,
            );
            if let Err(record_err) =
                record_worker_job_result(&state.pool, job.id, job.lease_token, result).await
            {
                error!(error = %record_err, worker_job_id = %job.id, "failed to record package worker_jobs projection failure");
            }
        }
    }
}

async fn build_package_worker_job_result(
    state: &AppState,
    worker_job_id: Uuid,
    payload: &PackageJobPayload,
) -> anyhow::Result<WorkerJobResult> {
    let package_job_id = extract_package_job_id(payload);
    link_package_worker_job_domain_refs(&state.pool, worker_job_id, package_job_id).await?;
    let package_job = fetch_package_job_projection(&state.pool, package_job_id).await?;
    let artifacts = fetch_package_artifact_projection(&state.pool, package_job_id).await?;
    let result_json = json!({
        "workerJobId": worker_job_id,
        "packageJobId": package_job_id,
        "payloadType": package_payload_type_name(payload),
        "packageJobStatus": package_job.get("status").cloned().unwrap_or(Value::Null),
        "artifacts": artifacts,
    });

    Ok(WorkerJobResult {
        status: "completed".to_owned(),
        result_json: Some(result_json),
        result_schema_version: Some(package_result_schema_version(payload).to_owned()),
        result_ref: Some(package_worker_result_ref(worker_job_id, package_job_id)),
        diagnostics: Some(json!({
            "packageJob": package_job,
        })),
        error_code: None,
        error_message: None,
        error_details: None,
        blocker_codes: Vec::new(),
        resolution_scope: None,
        retryable: None,
    })
}

async fn link_package_worker_job_domain_refs(
    pool: &sqlx::PgPool,
    worker_job_id: Uuid,
    package_job_id: Uuid,
) -> anyhow::Result<()> {
    execute_optional_worker_job_ref_update(
        pool,
        r"
        UPDATE public.lca_package_jobs
           SET worker_job_id = $1
         WHERE id = $2
        ",
        worker_job_id,
        package_job_id,
    )
    .await?;
    execute_optional_worker_job_ref_update(
        pool,
        r"
        UPDATE public.lca_package_artifacts
           SET worker_job_id = $1
         WHERE job_id = $2
        ",
        worker_job_id,
        package_job_id,
    )
    .await?;
    execute_optional_worker_job_ref_update(
        pool,
        r"
        UPDATE public.lca_package_export_items
           SET worker_job_id = $1
         WHERE job_id = $2
        ",
        worker_job_id,
        package_job_id,
    )
    .await?;
    execute_optional_worker_job_ref_update(
        pool,
        r"
        UPDATE public.lca_package_request_cache
           SET worker_job_id = $1
         WHERE job_id = $2
        ",
        worker_job_id,
        package_job_id,
    )
    .await?;

    Ok(())
}

async fn execute_optional_worker_job_ref_update(
    pool: &sqlx::PgPool,
    statement: &str,
    worker_job_id: Uuid,
    compat_job_id: Uuid,
) -> anyhow::Result<()> {
    let result = sqlx::query(statement)
        .bind(worker_job_id)
        .bind(compat_job_id)
        .execute(pool)
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(err) if is_undefined_table(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn package_worker_result_ref(worker_job_id: Uuid, package_job_id: Uuid) -> Value {
    json!({
        "domainSource": "worker_jobs",
        "workerJobId": worker_job_id,
        "packageJobId": package_job_id,
    })
}

async fn fetch_package_job_projection(pool: &sqlx::PgPool, job_id: Uuid) -> anyhow::Result<Value> {
    let result = sqlx::query(
        r"
        SELECT status, job_type, scope, root_count, diagnostics
        FROM public.lca_package_jobs
        WHERE id = $1
        ",
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await;

    let row = match result {
        Ok(row) => row,
        Err(err) if is_undefined_table(&err) => {
            return Ok(json!({
                "id": job_id,
                "missing": true,
                "legacyTableMissing": true,
            }));
        }
        Err(err) => return Err(err.into()),
    };

    Ok(row.map_or_else(
        || json!({"id": job_id, "missing": true}),
        |row| {
            json!({
                "id": job_id,
                "status": row.try_get::<String, _>("status").ok(),
                "jobType": row.try_get::<String, _>("job_type").ok(),
                "scope": row.try_get::<String, _>("scope").ok(),
                "rootCount": row.try_get::<i32, _>("root_count").ok(),
                "diagnostics": row.try_get::<Value, _>("diagnostics").ok(),
            })
        },
    ))
}

fn is_undefined_table(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().as_deref() == Some("42P01"),
        _ => false,
    }
}

async fn fetch_package_artifact_projection(
    pool: &sqlx::PgPool,
    job_id: Uuid,
) -> anyhow::Result<Value> {
    let rows = sqlx::query(
        r"
        SELECT id, artifact_kind, status, artifact_format, content_type, artifact_byte_size, artifact_url
        FROM public.lca_package_artifacts
        WHERE job_id = $1
          AND status <> 'deleted'
        ORDER BY created_at DESC
        ",
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;

    Ok(Value::Array(
        rows.into_iter()
            .map(|row| {
                json!({
                    "id": row.try_get::<Uuid, _>("id").ok(),
                    "artifactKind": row.try_get::<String, _>("artifact_kind").ok(),
                    "status": row.try_get::<String, _>("status").ok(),
                    "artifactFormat": row.try_get::<String, _>("artifact_format").ok(),
                    "contentType": row.try_get::<String, _>("content_type").ok(),
                    "artifactByteSize": row.try_get::<i64, _>("artifact_byte_size").ok(),
                    "artifactUrl": row.try_get::<String, _>("artifact_url").ok(),
                })
            })
            .collect(),
    ))
}

fn package_worker_job_payload(job: &WorkerJob) -> anyhow::Result<PackageJobPayload> {
    if job.worker_queue != PACKAGE_WORKER_QUEUE {
        return Err(anyhow::anyhow!(
            "unsupported worker queue for package job: {}",
            job.worker_queue
        ));
    }

    let expected_schema = package_payload_schema_version_for_job_kind(&job.job_kind)
        .ok_or_else(|| anyhow::anyhow!("unsupported package worker job kind: {}", job.job_kind))?;
    if job.payload_schema_version != expected_schema {
        return Err(anyhow::anyhow!(
            "unsupported package payload schema for {}: {}",
            job.job_kind,
            job.payload_schema_version
        ));
    }

    let mut payload = normalize_package_worker_payload_object(job.payload.clone())?;
    if !payload.contains_key("requested_by")
        && let Some(requested_by) = job.requested_by
    {
        payload.insert("requested_by".to_owned(), json!(requested_by));
    }
    if !payload.contains_key("type") {
        payload.insert(
            "type".to_owned(),
            Value::String(
                package_payload_type_for_job_kind(&job.job_kind)
                    .ok_or_else(|| {
                        anyhow::anyhow!("unsupported package worker job kind: {}", job.job_kind)
                    })?
                    .to_owned(),
            ),
        );
    }

    Ok(serde_json::from_value(Value::Object(payload))?)
}

fn normalize_package_worker_payload_object(value: Value) -> anyhow::Result<Map<String, Value>> {
    let Value::Object(mut payload) = value else {
        return Err(anyhow::anyhow!(
            "package worker job payload must be an object"
        ));
    };

    copy_alias(&mut payload, "jobId", "job_id");
    copy_alias(&mut payload, "packageJobId", "job_id");
    copy_alias(&mut payload, "requestedBy", "requested_by");
    copy_alias(&mut payload, "sourceArtifactId", "source_artifact_id");
    normalize_package_roots(&mut payload);

    Ok(payload)
}

fn normalize_package_roots(payload: &mut Map<String, Value>) {
    let Some(Value::Array(roots)) = payload.get_mut("roots") else {
        return;
    };
    for root in roots {
        let Value::Object(root_obj) = root else {
            continue;
        };
        copy_alias(root_obj, "rootTable", "table");
        copy_alias(root_obj, "tableName", "table");
        copy_alias(root_obj, "datasetId", "id");
        copy_alias(root_obj, "datasetVersion", "version");
    }
}

fn copy_alias(payload: &mut Map<String, Value>, alias: &str, canonical: &str) {
    if !payload.contains_key(canonical)
        && let Some(value) = payload.get(alias).cloned()
    {
        payload.insert(canonical.to_owned(), value);
    }
}

fn package_payload_schema_version_for_job_kind(job_kind: &str) -> Option<&'static str> {
    match job_kind {
        PACKAGE_EXPORT_WORKER_JOB_KIND => Some(PACKAGE_EXPORT_PAYLOAD_SCHEMA_VERSION),
        PACKAGE_IMPORT_WORKER_JOB_KIND => Some(PACKAGE_IMPORT_PAYLOAD_SCHEMA_VERSION),
        _ => None,
    }
}

fn package_payload_type_for_job_kind(job_kind: &str) -> Option<&'static str> {
    match job_kind {
        PACKAGE_EXPORT_WORKER_JOB_KIND => Some("export_package"),
        PACKAGE_IMPORT_WORKER_JOB_KIND => Some("import_package"),
        _ => None,
    }
}

fn package_payload_type_name(payload: &PackageJobPayload) -> &'static str {
    match payload {
        PackageJobPayload::ExportPackage { .. } => "export_package",
        PackageJobPayload::ImportPackage { .. } => "import_package",
    }
}

fn package_result_schema_version(payload: &PackageJobPayload) -> &'static str {
    match payload {
        PackageJobPayload::ExportPackage { .. } => PACKAGE_EXPORT_RESULT_SCHEMA_VERSION,
        PackageJobPayload::ImportPackage { .. } => PACKAGE_IMPORT_RESULT_SCHEMA_VERSION,
    }
}

fn package_progress_from_diagnostics(diagnostics: &Value, fallback: f64) -> f64 {
    let Some(progress) = diagnostics.get("progress") else {
        return fallback;
    };
    let total = progress
        .get("total_items")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    if total <= 0.0 {
        return fallback;
    }
    let processed = progress
        .get("processed_items")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    (processed / total).clamp(0.05, 0.95)
}

fn resolve_queue_name(requested: &str) -> String {
    if requested == "lca_jobs" {
        PACKAGE_QUEUE_NAME.to_owned()
    } else {
        requested.to_owned()
    }
}

fn build_package_job_failure_diagnostics(
    payload: &PackageJobPayload,
    err: &anyhow::Error,
) -> serde_json::Value {
    let phase = match payload {
        PackageJobPayload::ExportPackage { .. } => "export_package",
        PackageJobPayload::ImportPackage { .. } => "import_package",
    };
    let message = package_request_cache_error_message(payload, err);
    if let Some(upload_err) = find_object_store_upload_error(err) {
        return json!({
            "phase": phase,
            "stage": upload_err.stage,
            "result": "failed",
            "error_code": upload_err.error_code(),
            "message": message,
            "error": err.to_string(),
            "upload_mode": upload_err.upload_mode,
            "artifact_byte_size": upload_err.object_byte_size,
            "max_upload_bytes": upload_err.max_upload_bytes,
            "http_status": upload_err.status_code,
            "storage_error_code": upload_err.s3_error_code,
            "part_number": upload_err.part_number,
            "part_count": upload_err.part_count,
            "is_oversize": upload_err.is_oversize(),
        });
    }

    json!({
        "phase": phase,
        "stage": phase,
        "result": "failed",
        "error_code": "job_execution_failed",
        "message": message,
        "error": err.to_string(),
    })
}

fn package_request_cache_error_code(err: &anyhow::Error) -> &'static str {
    find_object_store_upload_error(err)
        .map_or("job_execution_failed", ObjectStoreUploadError::error_code)
}

fn package_request_cache_error_message(payload: &PackageJobPayload, err: &anyhow::Error) -> String {
    if let Some(upload_err) = find_object_store_upload_error(err)
        && upload_err.is_oversize()
    {
        let operation = match payload {
            PackageJobPayload::ExportPackage { .. } => "export package",
            PackageJobPayload::ImportPackage { .. } => "package artifact",
        };
        let max_hint = upload_err
            .max_upload_bytes
            .map(|size| format!(" max_upload_bytes={size}."))
            .unwrap_or_default();
        return format!(
            "The {operation} exceeded the object storage upload size limit; upload mode={}, stage={}, artifact_byte_size={}.{}",
            upload_err.upload_mode,
            upload_err.stage,
            upload_err
                .object_byte_size
                .map_or_else(|| "unknown".to_owned(), |size| size.to_string()),
            max_hint
        );
    }

    err.to_string()
}

fn find_object_store_upload_error(err: &anyhow::Error) -> Option<&ObjectStoreUploadError> {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<ObjectStoreUploadError>())
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use clap::Parser;
    use reqwest::StatusCode;
    use serde_json::json;
    use solver_worker::package_types::{
        PACKAGE_EXPORT_PAYLOAD_SCHEMA_VERSION, PACKAGE_EXPORT_WORKER_JOB_KIND,
        PACKAGE_IMPORT_PAYLOAD_SCHEMA_VERSION, PACKAGE_IMPORT_WORKER_JOB_KIND, PackageExportScope,
        PackageJobPayload, PackageRootTable,
    };
    use solver_worker::storage::ObjectStoreUploadError;
    use solver_worker::worker_jobs::WorkerJob;
    use uuid::Uuid;

    use super::{
        PackageQueueBackend, PackageWorkerCli, build_package_job_failure_diagnostics,
        package_request_cache_error_code, package_request_cache_error_message,
        package_worker_job_payload, package_worker_result_ref,
    };

    fn worker_job(
        job_kind: &str,
        payload_schema_version: &str,
        payload: serde_json::Value,
    ) -> WorkerJob {
        WorkerJob {
            id: Uuid::new_v4(),
            job_kind: job_kind.to_owned(),
            worker_queue: "package".to_owned(),
            payload_schema_version: payload_schema_version.to_owned(),
            payload,
            requested_by: Some(Uuid::new_v4()),
            lease_token: Uuid::new_v4(),
            attempt_count: 1,
        }
    }

    fn export_payload() -> PackageJobPayload {
        PackageJobPayload::ExportPackage {
            job_id: Uuid::nil(),
            requested_by: Uuid::nil(),
            scope: PackageExportScope::OpenData,
            roots: Vec::new(),
        }
    }

    #[test]
    fn package_worker_defaults_to_worker_jobs_backend() {
        let cli = PackageWorkerCli::parse_from([
            "package-worker",
            "--database-url",
            "postgres://example.local/app",
        ]);

        assert_eq!(cli.package_queue_backend, PackageQueueBackend::WorkerJobs);
    }

    #[test]
    fn package_pgmq_backend_requires_global_legacy_opt_in() {
        let cli = PackageWorkerCli::parse_from([
            "package-worker",
            "--database-url",
            "postgres://example.local/app",
            "--package-queue-backend",
            "pgmq",
        ]);

        assert_eq!(cli.package_queue_backend, PackageQueueBackend::Pgmq);
        assert!(
            cli.app
                .require_legacy_job_table_backend_allowed("package pgmq backend")
                .unwrap_err()
                .to_string()
                .contains("ALLOW_LEGACY_JOB_TABLE_BACKEND=true")
        );

        let allowed = PackageWorkerCli::parse_from([
            "package-worker",
            "--database-url",
            "postgres://example.local/app",
            "--package-queue-backend",
            "pgmq",
            "--allow-legacy-job-table-backend",
        ]);

        allowed
            .app
            .require_legacy_job_table_backend_allowed("package pgmq backend")
            .expect("legacy backend opt-in should allow package pgmq");
    }

    #[test]
    fn package_worker_result_ref_points_to_worker_jobs_domain_source() {
        let worker_job_id = Uuid::new_v4();
        let package_job_id = Uuid::new_v4();

        let result_ref = package_worker_result_ref(worker_job_id, package_job_id);

        assert_eq!(
            result_ref,
            json!({
                "domainSource": "worker_jobs",
                "workerJobId": worker_job_id,
                "packageJobId": package_job_id,
            })
        );
    }

    #[test]
    fn maps_worker_jobs_export_payload_with_camel_case_aliases() {
        let package_job_id = Uuid::new_v4();
        let requested_by = Uuid::new_v4();
        let root_id = Uuid::new_v4();
        let job = worker_job(
            PACKAGE_EXPORT_WORKER_JOB_KIND,
            PACKAGE_EXPORT_PAYLOAD_SCHEMA_VERSION,
            json!({
                "packageJobId": package_job_id,
                "requestedBy": requested_by,
                "scope": "selected_roots",
                "roots": [{
                    "tableName": "processes",
                    "datasetId": root_id,
                    "datasetVersion": "01.00.000"
                }]
            }),
        );

        let payload = package_worker_job_payload(&job).expect("payload");

        match payload {
            PackageJobPayload::ExportPackage {
                job_id,
                requested_by: parsed_requested_by,
                scope,
                roots,
            } => {
                assert_eq!(job_id, package_job_id);
                assert_eq!(parsed_requested_by, requested_by);
                assert_eq!(scope, PackageExportScope::SelectedRoots);
                assert_eq!(roots[0].table, PackageRootTable::Processes);
                assert_eq!(roots[0].id, root_id);
                assert_eq!(roots[0].version, "01.00.000");
            }
            other @ PackageJobPayload::ImportPackage { .. } => {
                panic!("unexpected payload: {other:?}");
            }
        }
    }

    #[test]
    fn maps_worker_jobs_import_payload_with_camel_case_aliases() {
        let package_job_id = Uuid::new_v4();
        let requested_by = Uuid::new_v4();
        let source_artifact_id = Uuid::new_v4();
        let mut job = worker_job(
            PACKAGE_IMPORT_WORKER_JOB_KIND,
            PACKAGE_IMPORT_PAYLOAD_SCHEMA_VERSION,
            json!({
                "jobId": package_job_id,
                "sourceArtifactId": source_artifact_id
            }),
        );
        job.requested_by = Some(requested_by);

        let payload = package_worker_job_payload(&job).expect("payload");

        match payload {
            PackageJobPayload::ImportPackage {
                job_id,
                requested_by: parsed_requested_by,
                source_artifact_id: parsed_source_artifact_id,
            } => {
                assert_eq!(job_id, package_job_id);
                assert_eq!(parsed_requested_by, requested_by);
                assert_eq!(parsed_source_artifact_id, source_artifact_id);
            }
            other @ PackageJobPayload::ExportPackage { .. } => {
                panic!("unexpected payload: {other:?}");
            }
        }
    }

    #[test]
    fn rejects_wrong_package_worker_jobs_schema() {
        let job = worker_job(
            PACKAGE_EXPORT_WORKER_JOB_KIND,
            PACKAGE_IMPORT_PAYLOAD_SCHEMA_VERSION,
            json!({
                "job_id": Uuid::new_v4(),
                "requested_by": Uuid::new_v4(),
                "scope": "open_data"
            }),
        );

        let err = package_worker_job_payload(&job).expect_err("schema mismatch");
        assert!(
            err.to_string()
                .contains("unsupported package payload schema")
        );
    }

    #[test]
    fn package_failure_diagnostics_surface_structured_upload_fields() {
        let payload = export_payload();
        let err = Error::new(ObjectStoreUploadError {
            stage: "upload_object",
            upload_mode: "single_put",
            status_code: Some(StatusCode::PAYLOAD_TOO_LARGE.as_u16()),
            s3_error_code: Some("EntityTooLarge".to_owned()),
            object_byte_size: Some(12),
            max_upload_bytes: None,
            part_number: None,
            part_count: None,
            message: "object upload failed".to_owned(),
        });

        let diagnostics = build_package_job_failure_diagnostics(&payload, &err);

        assert_eq!(diagnostics["error_code"], "artifact_too_large");
        assert_eq!(diagnostics["upload_mode"], "single_put");
        assert_eq!(diagnostics["artifact_byte_size"], 12);
        assert_eq!(diagnostics["is_oversize"], true);
    }

    #[test]
    fn package_request_cache_error_message_humanizes_oversize_uploads() {
        let payload = export_payload();
        let err = Error::new(ObjectStoreUploadError {
            stage: "upload_object",
            upload_mode: "single_put",
            status_code: Some(StatusCode::PAYLOAD_TOO_LARGE.as_u16()),
            s3_error_code: Some("EntityTooLarge".to_owned()),
            object_byte_size: Some(34),
            max_upload_bytes: Some(128),
            part_number: None,
            part_count: None,
            message: "object upload failed".to_owned(),
        });

        assert_eq!(package_request_cache_error_code(&err), "artifact_too_large");
        let message = package_request_cache_error_message(&payload, &err);
        assert!(message.contains("upload size limit"));
        assert!(message.contains("max_upload_bytes=128"));
    }
}
