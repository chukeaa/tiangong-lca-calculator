use std::{
    future::Future,
    io::ErrorKind,
    path::PathBuf,
    process::Command,
    time::{Duration, Instant},
};

use crate::pgbouncer_sqlx::{self as sqlx, PgPool, Postgres, Row, Transaction};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use solver_core::{
    ModelSparseData, NumericOptions, PrepareResult, SolveBatchResult, SolveComputationTiming,
    SolveOptions, SolveResult, SolverService, SparseTriplet,
};
use tokio::time::sleep;
use tracing::{info, instrument, warn};
use uuid::Uuid;

use crate::{
    artifacts::{
        EncodedArtifact, encode_contribution_path_artifact, encode_solve_all_unit_query_artifact,
        encode_solve_batch_artifact, encode_solve_one_artifact,
    },
    calculation_bundle::{
        CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT, CalculationBundleArtifactRef,
        CalculationBundleWriter, upload_built_calculation_bundle,
    },
    calculation_evidence::{
        LcaCalculationEvidence, validate_calculation_evidence,
        validate_calculation_evidence_binding,
    },
    config::AppConfig,
    contribution_path::{ContributionPathArtifact, analyze_contribution_path},
    db_pool::{APP_SOLVER_WORKER, APP_SOLVER_WORKER_QUEUE, WorkerDbPoolOptions},
    graph_types::RequestRootProcess,
    snapshot_artifacts::{
        DecodedSnapshotArtifact, ScopeClosureSnapshotBinding, decode_snapshot_artifact,
    },
    snapshot_index::{SnapshotIndexDocument, derive_snapshot_index_url},
    storage::ObjectStoreClient,
    types::{JobPayload, SolveOptionsPayload},
};

/// Queue message from pgmq.read.
#[derive(Debug, Clone)]
pub struct QueueMessage {
    /// pgmq message id.
    pub msg_id: i64,
    /// Raw payload.
    pub payload: Value,
}

/// App state shared by worker and HTTP server.
#[derive(Debug)]
pub struct AppState {
    /// Main DB pool for compute, package, snapshot, and result persistence queries.
    pub pool: PgPool,
    /// Queue-only DB pool for pgmq read/archive operations.
    pub queue_pool: PgPool,
    /// Core solver service.
    pub solver: SolverService,
    /// Object storage for result/snapshot artifacts.
    pub object_store: ObjectStoreClient,
    /// Maximum number of concurrent `build_snapshot` jobs across worker instances.
    pub build_snapshot_max_concurrency: u32,
    /// Poll interval while waiting for a `build_snapshot` concurrency slot.
    pub build_snapshot_lock_poll_interval: Duration,
}

const DEFAULT_ALL_UNIT_BATCH_SIZE: usize = 128;
const MAX_ALL_UNIT_BATCH_SIZE: usize = 2_048;
const BUILD_SNAPSHOT_ADVISORY_LOCK_BASE: i64 = 0x5447_4c43_4253_4e50;
const ACQUIRE_BUILD_SNAPSHOT_WORKER_JOBS_SLOT_SQL: &str = r"
WITH _service_role AS (
    SELECT set_config('request.jwt.claim.role', 'service_role', true)
),
_lock AS (
    SELECT pg_advisory_xact_lock($5)
    FROM _service_role
),
active_builds AS (
    SELECT count(active.id)::integer AS active_build_count
    FROM _lock
    LEFT JOIN public.worker_jobs AS active
      ON active.worker_runtime = 'calculator'
     AND active.worker_queue = 'solver'
     AND active.job_kind = 'lca.build_snapshot'
     AND active.status = 'running'
     AND active.phase = 'build_snapshot'
     AND active.lease_expires_at >= NOW()
     AND active.id <> $1
),
updated AS (
    UPDATE public.worker_jobs AS job
       SET phase = 'build_snapshot',
           progress = GREATEST(COALESCE(job.progress, 0), 0.10),
           diagnostics = COALESCE(job.diagnostics, '{}'::jsonb) || $6::jsonb,
           heartbeat_at = NOW(),
           lease_expires_at = NOW() + ($4::integer * interval '1 second'),
           updated_at = NOW()
      FROM active_builds
     WHERE job.id = $1
       AND job.lease_token is not distinct from $2
       AND job.status = 'running'
       AND job.lease_expires_at >= NOW()
       AND active_builds.active_build_count < $3
     RETURNING active_builds.active_build_count
)
SELECT active_build_count
FROM updated
";
const REVIEW_SUBMIT_SNAPSHOT_ARTIFACT_PURPOSE: &str = "review_submit_overlay";
const REVIEW_SUBMIT_SNAPSHOT_TTL_SECONDS: i64 = 14 * 24 * 60 * 60;

fn pgmq_queue_name_literal(queue_name: &str) -> anyhow::Result<String> {
    if queue_name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        Ok(format!("'{queue_name}'"))
    } else {
        Err(anyhow::anyhow!("invalid pgmq queue name: {queue_name}"))
    }
}

struct BuildSnapshotLockGuard {
    tx: Option<Transaction<'static, Postgres>>,
    key: Option<i64>,
    slot: Option<u32>,
    wait_sec: f64,
    max_concurrency: u32,
    acquired_at: Instant,
    strategy: &'static str,
    worker_lease: Option<BuildSnapshotWorkerLease>,
}

#[derive(Debug, Clone)]
pub(crate) struct BuildSnapshotWorkerLease {
    pub(crate) worker_job_id: Uuid,
    pub(crate) lease_token: Uuid,
    pub(crate) lease_seconds: i32,
}

impl BuildSnapshotLockGuard {
    fn diagnostics(&self) -> Value {
        let mut diagnostics = serde_json::json!({
            "enabled": true,
            "strategy": self.strategy,
            "max_concurrency": self.max_concurrency,
            "wait_sec": self.wait_sec,
            "hold_sec": self.acquired_at.elapsed().as_secs_f64(),
        });
        if let Some(payload) = diagnostics.as_object_mut() {
            if let Some(key) = self.key {
                payload.insert("lock_key".to_owned(), serde_json::json!(key));
            }
            if let Some(slot) = self.slot {
                payload.insert("slot".to_owned(), serde_json::json!(slot));
            }
            if let Some(lease) = &self.worker_lease {
                payload.insert(
                    "worker_job_id".to_owned(),
                    serde_json::json!(lease.worker_job_id),
                );
                payload.insert(
                    "lease_token".to_owned(),
                    serde_json::json!(lease.lease_token),
                );
                payload.insert(
                    "lease_seconds".to_owned(),
                    serde_json::json!(lease.lease_seconds),
                );
            }
        }
        diagnostics
    }

    async fn release(mut self) -> anyhow::Result<()> {
        let Some(tx) = self.tx.take() else {
            return Ok(());
        };

        let hold_sec = self.acquired_at.elapsed().as_secs_f64();
        tx.commit().await?;
        info!(
            lock_key = ?self.key,
            slot = ?self.slot,
            max_concurrency = self.max_concurrency,
            wait_sec = self.wait_sec,
            hold_sec,
            release_path = "explicit",
            "released build_snapshot transaction advisory lock"
        );
        Ok(())
    }
}

impl Drop for BuildSnapshotLockGuard {
    fn drop(&mut self) {
        if self.tx.is_some() {
            let hold_sec = self.acquired_at.elapsed().as_secs_f64();
            warn!(
                lock_key = ?self.key,
                slot = ?self.slot,
                max_concurrency = self.max_concurrency,
                wait_sec = self.wait_sec,
                hold_sec,
                release_path = "drop",
                "build_snapshot transaction advisory lock guard dropped before explicit release"
            );
        }
    }
}

fn build_snapshot_heartbeat_interval(lease_seconds: i32) -> Duration {
    let lease_seconds = lease_seconds.clamp(1, 86_400);
    Duration::from_secs(u64::from((lease_seconds / 3).clamp(1, 60).cast_unsigned()))
}

fn acquire_build_snapshot_worker_jobs_slot_sql() -> &'static str {
    ACQUIRE_BUILD_SNAPSHOT_WORKER_JOBS_SLOT_SQL
}

impl AppState {
    /// Creates app state with DB pool and required object storage.
    pub async fn new(config: &AppConfig) -> anyhow::Result<Self> {
        Self::new_with_application_names(config, APP_SOLVER_WORKER, APP_SOLVER_WORKER_QUEUE).await
    }

    /// Creates app state with explicit DB application names for the main and queue pools.
    pub async fn new_with_application_names(
        config: &AppConfig,
        application_name: &str,
        queue_application_name: &str,
    ) -> anyhow::Result<Self> {
        let pool = connect_pool(
            application_name,
            config.resolved_database_url()?,
            config.db_max_connections(),
            config.db_min_connections(),
            config.db_acquire_timeout(),
        )
        .await?;

        let queue_pool = if config.has_explicit_queue_database_url() {
            connect_pool(
                queue_application_name,
                config.resolved_queue_database_url()?,
                config.queue_db_max_connections(),
                config.queue_db_min_connections(),
                config.queue_db_acquire_timeout(),
            )
            .await?
        } else {
            pool.clone()
        };

        let endpoint = config
            .s3_endpoint
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("missing S3_ENDPOINT: result persistence is S3-only"))?;
        let region = config
            .s3_region
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("missing S3_REGION: result persistence is S3-only"))?;
        let bucket = config
            .s3_bucket
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("missing S3_BUCKET: result persistence is S3-only"))?;
        let access_key_id = config.s3_access_key_id.as_deref().ok_or_else(|| {
            anyhow::anyhow!("missing S3_ACCESS_KEY_ID: result persistence is S3-only")
        })?;
        let secret_access_key = config.s3_secret_access_key.as_deref().ok_or_else(|| {
            anyhow::anyhow!("missing S3_SECRET_ACCESS_KEY: result persistence is S3-only")
        })?;
        let object_store = ObjectStoreClient::new_with_upload_limit(
            endpoint,
            region,
            bucket,
            &config.s3_prefix,
            access_key_id,
            secret_access_key,
            config.s3_session_token.clone(),
            config.s3_max_upload_bytes(),
        )?;

        Ok(Self {
            pool,
            queue_pool,
            solver: SolverService::new(),
            object_store,
            build_snapshot_max_concurrency: config.build_snapshot_max_concurrency(),
            build_snapshot_lock_poll_interval: config.build_snapshot_lock_poll_interval(),
        })
    }
}

async fn connect_pool(
    application_name: &str,
    database_url: &str,
    max_connections: u32,
    min_connections: u32,
    acquire_timeout: Duration,
) -> anyhow::Result<PgPool> {
    WorkerDbPoolOptions::new(application_name)
        .max_connections(max_connections)
        .min_connections(min_connections)
        .acquire_timeout(acquire_timeout)
        .connect(database_url)
        .await
}

async fn acquire_build_snapshot_lock(
    pool: &PgPool,
    max_concurrency: u32,
    poll_interval: Duration,
) -> anyhow::Result<BuildSnapshotLockGuard> {
    let started = Instant::now();
    let max_concurrency = max_concurrency.max(1);
    let poll_interval = poll_interval.max(Duration::from_millis(100));

    loop {
        for slot in 0..max_concurrency {
            let key = BUILD_SNAPSHOT_ADVISORY_LOCK_BASE + i64::from(slot);
            let mut tx = pool.begin().await?;
            let acquired = sqlx::query_scalar::<bool>("SELECT pg_try_advisory_xact_lock($1)")
                .bind(key)
                .fetch_one(&mut *tx)
                .await?;
            if acquired {
                let wait_sec = started.elapsed().as_secs_f64();
                info!(
                    lock_key = key,
                    slot,
                    max_concurrency,
                    wait_sec,
                    "acquired build_snapshot transaction advisory lock"
                );
                return Ok(BuildSnapshotLockGuard {
                    tx: Some(tx),
                    key: Some(key),
                    slot: Some(slot),
                    wait_sec,
                    max_concurrency,
                    acquired_at: Instant::now(),
                    strategy: "postgres_transaction_advisory_lock",
                    worker_lease: None,
                });
            }
            tx.rollback().await?;
        }

        sleep(poll_interval).await;
    }
}

async fn acquire_build_snapshot_worker_jobs_slot(
    pool: &PgPool,
    max_concurrency: u32,
    poll_interval: Duration,
    lease: BuildSnapshotWorkerLease,
) -> anyhow::Result<BuildSnapshotLockGuard> {
    let started = Instant::now();
    let max_concurrency = max_concurrency.max(1);
    let poll_interval = poll_interval.max(Duration::from_millis(100));
    let lease_seconds = lease.lease_seconds.clamp(1, 86_400);

    loop {
        let wait_sec = started.elapsed().as_secs_f64();
        let diagnostics = serde_json::json!({
            "build_snapshot_lock": {
                "enabled": true,
                "strategy": "worker_jobs_phase_lease",
                "max_concurrency": max_concurrency,
                "wait_sec": wait_sec,
                "waiting": false,
            }
        });
        let row = sqlx::query(acquire_build_snapshot_worker_jobs_slot_sql())
            .bind(lease.worker_job_id)
            .bind(lease.lease_token)
            .bind(i32::try_from(max_concurrency).unwrap_or(i32::MAX))
            .bind(lease_seconds)
            .bind(BUILD_SNAPSHOT_ADVISORY_LOCK_BASE)
            .bind(diagnostics)
            .fetch_optional(pool)
            .await?;

        if let Some(row) = row {
            let active_build_count = row.try_get::<i32, _>("active_build_count")?;
            info!(
                worker_job_id = %lease.worker_job_id,
                max_concurrency,
                active_build_count,
                wait_sec,
                "acquired build_snapshot worker_jobs lease slot"
            );
            return Ok(BuildSnapshotLockGuard {
                tx: None,
                key: Some(BUILD_SNAPSHOT_ADVISORY_LOCK_BASE),
                slot: Some(active_build_count.max(0).cast_unsigned()),
                wait_sec,
                max_concurrency,
                acquired_at: Instant::now(),
                strategy: "worker_jobs_phase_lease",
                worker_lease: Some(lease),
            });
        }

        crate::worker_jobs::heartbeat_worker_job(
            pool,
            lease.worker_job_id,
            lease.lease_token,
            "waiting_for_build_snapshot_lock",
            0.05,
            Some(serde_json::json!({
                "build_snapshot_lock": {
                    "enabled": true,
                    "strategy": "worker_jobs_phase_lease",
                    "max_concurrency": max_concurrency,
                    "wait_sec": wait_sec,
                    "waiting": true,
                }
            })),
            lease_seconds,
        )
        .await?;
        sleep(poll_interval).await;
    }
}

/// Reads one message from pgmq queue.
#[instrument(skip(pool))]
pub async fn read_one_queue_message(
    pool: &PgPool,
    queue_name: &str,
    vt_seconds: i32,
) -> anyhow::Result<Option<QueueMessage>> {
    let queue_name = pgmq_queue_name_literal(queue_name)?;
    let rows = sqlx::raw_sql(&format!(
        r"
        SELECT msg_id, message
        FROM pgmq.read({queue_name}, {vt_seconds}, 1)
        LIMIT 1
        "
    ))
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .next()
        .map(|r| {
            Ok(QueueMessage {
                msg_id: r.try_get::<i64, _>("msg_id")?,
                payload: r.try_get::<Value, _>("message")?,
            })
        })
        .transpose()
}

/// Archives processed message.
#[instrument(skip(pool))]
pub async fn archive_queue_message(
    pool: &PgPool,
    queue_name: &str,
    msg_id: i64,
) -> anyhow::Result<()> {
    let queue_name = pgmq_queue_name_literal(queue_name)?;
    let _ = sqlx::raw_sql(&format!("SELECT pgmq.archive({queue_name}, {msg_id})"))
        .execute(pool)
        .await?;
    Ok(())
}

/// Updates `lca_jobs` status and diagnostics.
#[instrument(skip(pool, diagnostics))]
pub async fn update_job_status(
    pool: &PgPool,
    job_id: Uuid,
    status: &str,
    diagnostics: Value,
) -> anyhow::Result<f64> {
    let db_write_started = Instant::now();
    let update_result = sqlx::query(
        r"
        UPDATE lca_jobs
        SET status = $2,
            diagnostics = $3::jsonb,
            updated_at = NOW(),
            started_at = CASE WHEN $2 = 'running' AND started_at IS NULL THEN NOW() ELSE started_at END,
            finished_at = CASE WHEN $2 IN ('completed','failed') AND finished_at IS NULL THEN NOW() ELSE finished_at END
        WHERE id = $1
        ",
    )
    .bind(job_id)
    .bind(status)
    .bind(diagnostics.clone())
    .execute(pool)
    .await;
    let db_write_sec = db_write_started.elapsed().as_secs_f64();
    match update_result {
        Ok(_) => {}
        Err(err) if is_undefined_table(&err) => {
            warn!(
                job_id = %job_id,
                status,
                "skipping legacy lca_jobs status update because the table is not present"
            );
            return Ok(db_write_sec);
        }
        Err(err) => return Err(err.into()),
    }

    let diagnostics_with_timing =
        merge_job_status_update_timing(diagnostics.clone(), status, db_write_sec);
    if diagnostics_with_timing != diagnostics {
        set_job_diagnostics(pool, job_id, diagnostics_with_timing).await?;
    }

    Ok(db_write_sec)
}

#[derive(Debug, Default)]
struct ResultInsert {
    diagnostics: Value,
    artifact_url: String,
    artifact_sha256: String,
    artifact_byte_size: i64,
    artifact_format: String,
}

/// Inserts one `lca_results` row.
#[instrument(skip(pool, data))]
async fn insert_result(
    pool: &PgPool,
    job_id: Uuid,
    snapshot_id: Uuid,
    data: ResultInsert,
) -> anyhow::Result<Uuid> {
    let row = sqlx::query(
        r"
        INSERT INTO lca_results (
            job_id,
            snapshot_id,
            diagnostics,
            artifact_url,
            artifact_sha256,
            artifact_byte_size,
            artifact_format,
            created_at
        )
        VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, NOW())
        RETURNING id
        ",
    )
    .bind(job_id)
    .bind(snapshot_id)
    .bind(data.diagnostics)
    .bind(data.artifact_url)
    .bind(data.artifact_sha256)
    .bind(data.artifact_byte_size)
    .bind(data.artifact_format)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get::<Uuid, _>("id")?)
}

#[instrument(skip(pool, diagnostics))]
async fn update_result_diagnostics(
    pool: &PgPool,
    result_id: Uuid,
    diagnostics: Value,
) -> anyhow::Result<()> {
    let _ = sqlx::query(
        r"
        UPDATE lca_results
        SET diagnostics = $2::jsonb
        WHERE id = $1
        ",
    )
    .bind(result_id)
    .bind(diagnostics)
    .execute(pool)
    .await?;
    Ok(())
}

#[instrument(skip(pool, diagnostics))]
async fn set_job_diagnostics(
    pool: &PgPool,
    job_id: Uuid,
    diagnostics: Value,
) -> anyhow::Result<()> {
    let result = sqlx::query(
        r"
        UPDATE lca_jobs
        SET diagnostics = $2::jsonb
        WHERE id = $1
        ",
    )
    .bind(job_id)
    .bind(diagnostics)
    .execute(pool)
    .await;
    match result {
        Ok(_) => Ok(()),
        Err(err) if is_undefined_table(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Marks request cache row as running for a given job.
#[instrument(skip(pool))]
pub async fn mark_result_cache_running(pool: &PgPool, job_id: Uuid) -> anyhow::Result<()> {
    let result = sqlx::query(
        r"
        UPDATE lca_result_cache
        SET status = 'running',
            updated_at = NOW(),
            last_accessed_at = NOW()
        WHERE job_id = $1
        ",
    )
    .bind(job_id)
    .execute(pool)
    .await;

    match result {
        Ok(_rows) => Ok(()),
        Err(err) if is_undefined_table(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Marks request cache row as ready and stores result id for a given job.
#[instrument(skip(pool))]
pub async fn mark_result_cache_ready(
    pool: &PgPool,
    job_id: Uuid,
    result_id: Uuid,
) -> anyhow::Result<()> {
    let result = sqlx::query(
        r"
        UPDATE lca_result_cache
        SET status = 'ready',
            result_id = $2,
            error_code = NULL,
            error_message = NULL,
            updated_at = NOW(),
            last_accessed_at = NOW()
        WHERE job_id = $1
        ",
    )
    .bind(job_id)
    .bind(result_id)
    .execute(pool)
    .await;

    match result {
        Ok(_rows) => Ok(()),
        Err(err) if is_undefined_table(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Marks request cache row as failed for a given job.
#[instrument(skip(pool))]
pub async fn mark_result_cache_failed(
    pool: &PgPool,
    job_id: Uuid,
    error_code: &str,
    error_message: &str,
) -> anyhow::Result<()> {
    let result = sqlx::query(
        r"
        UPDATE lca_result_cache
        SET status = 'failed',
            error_code = $2,
            error_message = $3,
            updated_at = NOW(),
            last_accessed_at = NOW()
        WHERE job_id = $1
        ",
    )
    .bind(job_id)
    .bind(error_code)
    .bind(error_message)
    .execute(pool)
    .await;

    match result {
        Ok(_rows) => Ok(()),
        Err(err) if is_undefined_table(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Returns latest result id for a given job.
#[instrument(skip(pool))]
pub async fn latest_result_id_for_job(pool: &PgPool, job_id: Uuid) -> anyhow::Result<Option<Uuid>> {
    let row = match sqlx::query(
        r"
        SELECT id
        FROM lca_results
        WHERE job_id = $1
        ORDER BY created_at DESC
        LIMIT 1
        ",
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await
    {
        Ok(row) => row,
        Err(err) if is_undefined_table(&err) => return Ok(None),
        Err(err) => return Err(err.into()),
    };

    row.map(|r| r.try_get::<Uuid, _>("id"))
        .transpose()
        .map_err(Into::into)
}

fn merge_job_status_update_timing(
    mut diagnostics: Value,
    status: &str,
    db_write_sec: f64,
) -> Value {
    let Value::Object(ref mut root) = diagnostics else {
        return diagnostics;
    };

    let timing_value = root
        .entry("job_status_update_timing_sec".to_owned())
        .or_insert_with(|| Value::Object(Map::new()));
    if !timing_value.is_object() {
        *timing_value = Value::Object(Map::new());
    }

    let Some(timing) = timing_value.as_object_mut() else {
        return diagnostics;
    };

    timing.insert(format!("{status}_db_write_sec"), Value::from(db_write_sec));
    timing.insert("last_status".to_owned(), Value::String(status.to_owned()));
    timing.insert("last_db_write_sec".to_owned(), Value::from(db_write_sec));

    diagnostics
}

#[derive(Debug, Clone)]
struct SnapshotArtifactMeta {
    url: String,
    format: String,
    sha256: String,
}

/// Loads sparse snapshot data from snapshot artifact first, then falls back to `lca_*` tables.
#[instrument(skip(state))]
pub async fn fetch_snapshot_sparse_data(
    state: &AppState,
    snapshot_id: Uuid,
) -> anyhow::Result<ModelSparseData> {
    let mut artifact_error = None;
    if let Some(meta) = fetch_snapshot_artifact_meta(&state.pool, snapshot_id).await? {
        match fetch_snapshot_payload_from_artifact(state, snapshot_id, &meta).await {
            Ok(payload) => return Ok(payload),
            Err(err) => {
                warn!(
                    snapshot_id = %snapshot_id,
                    artifact_format = %meta.format,
                    error = %err,
                    "failed to load snapshot artifact, falling back to table-backed sparse data"
                );
                artifact_error = Some(err);
            }
        }
    }

    match fetch_snapshot_sparse_data_from_tables(&state.pool, snapshot_id).await {
        Ok(data) => Ok(data),
        Err(err) => {
            if let Some(sqlx_err) = err.downcast_ref::<sqlx::Error>()
                && is_undefined_table(sqlx_err)
            {
                return Err(missing_legacy_tables_sparse_data_error(
                    snapshot_id,
                    artifact_error.as_ref(),
                ));
            }
            Err(err)
        }
    }
}

fn missing_legacy_tables_sparse_data_error(
    snapshot_id: Uuid,
    artifact_error: Option<&anyhow::Error>,
) -> anyhow::Error {
    if let Some(artifact_error) = artifact_error {
        anyhow::anyhow!(
            "snapshot {snapshot_id} has no readable artifact and legacy lca_* matrix tables are missing; original artifact read/decode error: {artifact_error:#}"
        )
    } else {
        anyhow::anyhow!(
            "snapshot {snapshot_id} has no readable artifact and legacy lca_* matrix tables are missing"
        )
    }
}

#[instrument(skip(pool))]
async fn fetch_snapshot_sparse_data_from_tables(
    pool: &PgPool,
    snapshot_id: Uuid,
) -> anyhow::Result<ModelSparseData> {
    let process_count = fetch_process_count(pool, snapshot_id).await?;
    let flow_count = fetch_flow_count(pool, snapshot_id).await?;
    let impact_count = fetch_impact_count(pool, snapshot_id).await?;

    let technosphere_entries = fetch_triplets(
        pool,
        snapshot_id,
        r#"
        SELECT "row" AS row_idx, "col" AS col_idx, value
        FROM lca_technosphere_entries
        WHERE snapshot_id = $1
        "#,
    )
    .await?;

    let biosphere_entries = fetch_triplets(
        pool,
        snapshot_id,
        r#"
        SELECT "row" AS row_idx, "col" AS col_idx, value
        FROM lca_biosphere_entries
        WHERE snapshot_id = $1
        "#,
    )
    .await?;

    let characterization_factors = fetch_triplets(
        pool,
        snapshot_id,
        r#"
        SELECT "row" AS row_idx, "col" AS col_idx, value
        FROM lca_characterization_factors
        WHERE snapshot_id = $1
        "#,
    )
    .await?;

    Ok(ModelSparseData {
        model_version: snapshot_id,
        process_count,
        flow_count,
        impact_count,
        technosphere_entries,
        biosphere_entries,
        characterization_factors,
    })
}

async fn fetch_snapshot_artifact_meta(
    pool: &PgPool,
    snapshot_id: Uuid,
) -> anyhow::Result<Option<SnapshotArtifactMeta>> {
    let row = match sqlx::query(
        r"
        SELECT artifact_url, artifact_format, artifact_sha256
        FROM lca_snapshot_artifacts
        WHERE snapshot_id = $1
          AND status = 'ready'
        ORDER BY created_at DESC
        LIMIT 1
        ",
    )
    .bind(snapshot_id)
    .fetch_optional(pool)
    .await
    {
        Ok(row) => row,
        Err(err) if is_undefined_table(&err) => return Ok(None),
        Err(err) => return Err(err.into()),
    };

    row.map(|r| {
        Ok(SnapshotArtifactMeta {
            url: r.try_get::<String, _>("artifact_url")?,
            format: r.try_get::<String, _>("artifact_format")?,
            sha256: r.try_get::<String, _>("artifact_sha256")?,
        })
    })
    .transpose()
}

async fn fetch_snapshot_payload_from_artifact(
    state: &AppState,
    snapshot_id: Uuid,
    meta: &SnapshotArtifactMeta,
) -> anyhow::Result<ModelSparseData> {
    Ok(
        fetch_decoded_snapshot_artifact_from_meta(state, snapshot_id, meta)
            .await?
            .payload,
    )
}

async fn fetch_decoded_snapshot_artifact_from_meta(
    state: &AppState,
    snapshot_id: Uuid,
    meta: &SnapshotArtifactMeta,
) -> anyhow::Result<DecodedSnapshotArtifact> {
    let bytes = state.object_store.download_object_url(&meta.url).await?;
    let observed_sha256 = hex::encode(Sha256::digest(bytes.as_slice()));
    if observed_sha256 != meta.sha256 {
        return Err(anyhow::anyhow!(
            "snapshot artifact hash mismatch: snapshot={snapshot_id} expected={} observed={observed_sha256}",
            meta.sha256
        ));
    }

    let decoded = decode_snapshot_artifact(bytes.as_slice())?;
    if decoded.snapshot_id != snapshot_id {
        return Err(anyhow::anyhow!(
            "artifact snapshot mismatch: expected={} got={}",
            snapshot_id,
            decoded.snapshot_id
        ));
    }

    Ok(decoded)
}

pub(crate) async fn fetch_decoded_snapshot_artifact(
    state: &AppState,
    snapshot_id: Uuid,
) -> anyhow::Result<DecodedSnapshotArtifact> {
    let meta = fetch_snapshot_artifact_meta(&state.pool, snapshot_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("snapshot {snapshot_id} has no ready artifact"))?;
    fetch_decoded_snapshot_artifact_from_meta(state, snapshot_id, &meta).await
}

pub(crate) async fn fetch_snapshot_index_document(
    state: &AppState,
    snapshot_id: Uuid,
) -> anyhow::Result<SnapshotIndexDocument> {
    let meta = fetch_snapshot_artifact_meta(&state.pool, snapshot_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("snapshot {snapshot_id} has no ready artifact"))?;
    let snapshot_index_url = derive_snapshot_index_url(&meta.url);
    let bytes = state
        .object_store
        .download_object_url(&snapshot_index_url)
        .await?;
    let decoded: SnapshotIndexDocument = serde_json::from_slice(bytes.as_slice())?;
    if decoded.snapshot_id != snapshot_id {
        return Err(anyhow::anyhow!(
            "snapshot index mismatch: expected={} got={}",
            snapshot_id,
            decoded.snapshot_id
        ));
    }
    Ok(decoded)
}

async fn resolve_snapshot_calculation_evidence(
    state: &AppState,
    snapshot_id: Uuid,
    request_binding: Option<&LcaCalculationEvidence>,
) -> anyhow::Result<Option<LcaCalculationEvidence>> {
    let snapshot_index = fetch_snapshot_index_document(state, snapshot_id).await?;
    validate_calculation_evidence_binding(
        snapshot_index.calculation_evidence.as_ref(),
        request_binding,
    )
}

fn is_undefined_table(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().as_deref() == Some("42P01"),
        _ => false,
    }
}

/// Executes one queue payload end-to-end.
#[instrument(skip(state))]
pub async fn handle_job_payload(state: &AppState, payload: JobPayload) -> anyhow::Result<()> {
    Box::pin(handle_job_payload_with_worker_lease(state, payload, None)).await
}

pub(crate) async fn handle_worker_jobs_job_payload(
    state: &AppState,
    payload: JobPayload,
    worker_job_id: Uuid,
    lease_token: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<()> {
    Box::pin(handle_job_payload_with_worker_lease(
        state,
        payload,
        Some(BuildSnapshotWorkerLease {
            worker_job_id,
            lease_token,
            lease_seconds,
        }),
    ))
    .await
}

#[allow(clippy::too_many_lines)]
async fn handle_job_payload_with_worker_lease(
    state: &AppState,
    payload: JobPayload,
    build_snapshot_worker_lease: Option<BuildSnapshotWorkerLease>,
) -> anyhow::Result<()> {
    match payload {
        JobPayload::PrepareFactorization {
            job_id,
            snapshot_id,
            print_level,
        } => {
            let running_db_write_sec = update_job_status(
                &state.pool,
                job_id,
                "running",
                serde_json::json!({"phase": "prepare"}),
            )
            .await?;

            let data = fetch_snapshot_sparse_data(state, snapshot_id).await?;
            let prepared = state.solver.prepare(
                &data,
                NumericOptions {
                    print_level: print_level.unwrap_or(0.0),
                },
            )?;

            let ready_diag = merge_job_status_update_timing(
                serde_json::to_value(prepared)?,
                "running",
                running_db_write_sec,
            );
            let _ = update_job_status(&state.pool, job_id, "ready", ready_diag).await?;
        }
        JobPayload::SolveOne {
            job_id,
            snapshot_id,
            rhs,
            solve,
            print_level,
            calculation_evidence_binding,
        } => {
            let running_db_write_sec = update_job_status(
                &state.pool,
                job_id,
                "running",
                serde_json::json!({"phase": "solve_one"}),
            )
            .await?;

            if let Err(err) = mark_result_cache_running(&state.pool, job_id).await {
                warn!(
                    error = %err,
                    job_id = %job_id,
                    "failed to mark result cache running"
                );
            }

            let level = print_level.unwrap_or(0.0);
            let calculation_evidence = resolve_snapshot_calculation_evidence(
                state,
                snapshot_id,
                calculation_evidence_binding.as_ref(),
            )
            .await?;
            ensure_prepared(state, snapshot_id, level).await?;
            let timed = state.solver.solve_one_timed(
                snapshot_id,
                NumericOptions { print_level: level },
                &rhs,
                to_core_solve_options(solve),
            )?;
            let solved = timed.result;

            let result_diag = persist_solve_one_result(
                state,
                job_id,
                snapshot_id,
                &solved,
                &timed.timing,
                calculation_evidence.clone(),
            )
            .await?;
            let completed_diag = merge_job_status_update_timing(
                serde_json::json!({
                    "result": "stored",
                    "storage": result_diag,
                    "calculation_evidence": calculation_evidence,
                }),
                "running",
                running_db_write_sec,
            );
            let _ = update_job_status(&state.pool, job_id, "completed", completed_diag).await?;

            if let Some(result_id) = latest_result_id_for_job(&state.pool, job_id).await?
                && let Err(err) = mark_result_cache_ready(&state.pool, job_id, result_id).await
            {
                warn!(
                    error = %err,
                    job_id = %job_id,
                    result_id = %result_id,
                    "failed to mark result cache ready"
                );
            }
        }
        JobPayload::SolveBatch {
            job_id,
            snapshot_id,
            rhs_batch,
            solve,
            print_level,
            calculation_evidence_binding,
        } => {
            let running_db_write_sec = update_job_status(
                &state.pool,
                job_id,
                "running",
                serde_json::json!({"phase": "solve_batch"}),
            )
            .await?;

            if let Err(err) = mark_result_cache_running(&state.pool, job_id).await {
                warn!(
                    error = %err,
                    job_id = %job_id,
                    "failed to mark result cache running"
                );
            }

            let level = print_level.unwrap_or(0.0);
            let calculation_evidence = resolve_snapshot_calculation_evidence(
                state,
                snapshot_id,
                calculation_evidence_binding.as_ref(),
            )
            .await?;
            ensure_prepared(state, snapshot_id, level).await?;
            let solved = state.solver.solve_batch(
                snapshot_id,
                NumericOptions { print_level: level },
                &rhs_batch,
                to_core_solve_options(solve),
            )?;

            let result_diag = persist_solve_batch_result(
                state,
                job_id,
                snapshot_id,
                &solved,
                "solve_batch",
                calculation_evidence.clone(),
                None,
            )
            .await?;
            let completed_diag = merge_job_status_update_timing(
                serde_json::json!({
                    "result": "stored",
                    "storage": result_diag,
                    "calculation_evidence": calculation_evidence,
                }),
                "running",
                running_db_write_sec,
            );
            let _ = update_job_status(&state.pool, job_id, "completed", completed_diag).await?;

            if let Some(result_id) = latest_result_id_for_job(&state.pool, job_id).await?
                && let Err(err) = mark_result_cache_ready(&state.pool, job_id, result_id).await
            {
                warn!(
                    error = %err,
                    job_id = %job_id,
                    result_id = %result_id,
                    "failed to mark result cache ready"
                );
            }
        }
        JobPayload::SolveAllUnit {
            job_id,
            snapshot_id,
            solve,
            unit_batch_size,
            print_level,
            calculation_evidence_binding,
        } => {
            let running_db_write_sec = update_job_status(
                &state.pool,
                job_id,
                "running",
                serde_json::json!({"phase": "solve_all_unit"}),
            )
            .await?;

            if let Err(err) = mark_result_cache_running(&state.pool, job_id).await {
                warn!(
                    error = %err,
                    job_id = %job_id,
                    "failed to mark result cache running"
                );
            }

            let level = print_level.unwrap_or(0.0);
            let calculation_evidence = resolve_snapshot_calculation_evidence(
                state,
                snapshot_id,
                calculation_evidence_binding.as_ref(),
            )
            .await?;
            ensure_prepared(state, snapshot_id, level).await?;
            let process_count = fetch_snapshot_process_count(&state.pool, snapshot_id).await?;
            let n = usize::try_from(process_count)
                .map_err(|_| anyhow::anyhow!("process count overflow: {process_count}"))?;
            if n == 0 {
                return Err(anyhow::anyhow!(
                    "solve_all_unit requires non-zero process count"
                ));
            }
            let batch_size = normalize_all_unit_batch_size(unit_batch_size, n);
            let _ = resolve_solve_all_unit_options(solve)?;
            let (solved, calculation_bundle) = solve_all_unit_with_calculation_bundle(
                state,
                job_id,
                snapshot_id,
                n,
                batch_size,
                level,
            )
            .await?;
            let query_artifact_meta =
                persist_solve_all_unit_query_artifact(state, job_id, snapshot_id, &solved)
                    .await
                    .map_err(|err| {
                        warn!(
                            error = %err,
                            job_id = %job_id,
                            snapshot_id = %snapshot_id,
                            "failed to persist solve_all_unit query sidecar artifact"
                        );
                        err
                    })
                    .ok();
            let result_diag = persist_solve_batch_result(
                state,
                job_id,
                snapshot_id,
                &solved,
                "solve_all_unit",
                calculation_evidence.clone(),
                Some(calculation_bundle.clone()),
            )
            .await?;
            let completed_diag = merge_job_status_update_timing(
                serde_json::json!({
                    "result": "stored",
                    "storage": result_diag,
                    "solve_all_unit": {
                        "process_count": n,
                        "unit_batch_size": batch_size,
                    },
                    "calculation_bundle": calculation_bundle,
                    "calculation_evidence": calculation_evidence,
                }),
                "running",
                running_db_write_sec,
            );
            let _ = update_job_status(&state.pool, job_id, "completed", completed_diag).await?;

            if let Some(result_id) = latest_result_id_for_job(&state.pool, job_id).await? {
                if let Err(err) = mark_result_cache_ready(&state.pool, job_id, result_id).await {
                    warn!(
                        error = %err,
                        job_id = %job_id,
                        result_id = %result_id,
                        "failed to mark result cache ready"
                    );
                }

                if let Some(meta) = query_artifact_meta
                    && let Err(err) = upsert_latest_all_unit_result(
                        &state.pool,
                        snapshot_id,
                        job_id,
                        result_id,
                        &meta,
                    )
                    .await
                {
                    warn!(
                        error = %err,
                        job_id = %job_id,
                        snapshot_id = %snapshot_id,
                        result_id = %result_id,
                        "failed to upsert lca_latest_all_unit_results"
                    );
                }
            }
        }
        JobPayload::AnalyzeContributionPath {
            job_id,
            snapshot_id,
            process_id,
            process_index,
            impact_id,
            impact_index,
            amount,
            options,
            print_level,
            calculation_evidence_binding,
        } => {
            let running_db_write_sec = update_job_status(
                &state.pool,
                job_id,
                "running",
                serde_json::json!({"phase": "analyze_contribution_path"}),
            )
            .await?;

            if let Err(err) = mark_result_cache_running(&state.pool, job_id).await {
                warn!(
                    error = %err,
                    job_id = %job_id,
                    "failed to mark result cache running"
                );
            }

            if !amount.is_finite() || amount == 0.0 {
                return Err(anyhow::anyhow!(
                    "analyze_contribution_path requires finite non-zero amount"
                ));
            }

            let level = print_level.unwrap_or(0.0);
            let calculation_evidence = resolve_snapshot_calculation_evidence(
                state,
                snapshot_id,
                calculation_evidence_binding.as_ref(),
            )
            .await?;
            ensure_prepared(state, snapshot_id, level).await?;
            let snapshot_data = fetch_snapshot_sparse_data(state, snapshot_id).await?;
            let snapshot_index = fetch_snapshot_index_document(state, snapshot_id).await?;
            let process_count = usize::try_from(snapshot_data.process_count)
                .map_err(|_| anyhow::anyhow!("process count overflow"))?;
            let root_process_index = usize::try_from(process_index)
                .map_err(|_| anyhow::anyhow!("process_index overflow: {process_index}"))?;
            if root_process_index >= process_count {
                return Err(anyhow::anyhow!(
                    "process_index out of range: process_index={process_index} process_count={process_count}"
                ));
            }
            let impact_count = usize::try_from(snapshot_data.impact_count)
                .map_err(|_| anyhow::anyhow!("impact count overflow"))?;
            let target_impact_index = usize::try_from(impact_index)
                .map_err(|_| anyhow::anyhow!("impact_index overflow: {impact_index}"))?;
            if target_impact_index >= impact_count {
                return Err(anyhow::anyhow!(
                    "impact_index out of range: impact_index={impact_index} impact_count={impact_count}"
                ));
            }

            let rhs = build_single_rhs(process_count, root_process_index, amount);
            let timed = state.solver.solve_one_timed(
                snapshot_id,
                NumericOptions { print_level: level },
                &rhs,
                SolveOptions {
                    return_x: true,
                    return_g: true,
                    return_h: true,
                },
            )?;
            let analysis = analyze_contribution_path(
                snapshot_id,
                job_id,
                process_id,
                impact_id,
                process_index,
                impact_index,
                amount,
                options,
                &snapshot_index,
                &snapshot_data,
                &timed.result,
            )?;

            let result_diag = persist_contribution_path_result(
                state,
                job_id,
                snapshot_id,
                &analysis,
                calculation_evidence.clone(),
            )
            .await?;
            let completed_diag = merge_job_status_update_timing(
                serde_json::json!({
                    "result": "stored",
                    "storage": result_diag,
                    "contribution_path": {
                        "process_id": process_id,
                        "impact_id": impact_id,
                        "amount": amount,
                        "summary": analysis.summary,
                    },
                    "calculation_evidence": calculation_evidence,
                }),
                "running",
                running_db_write_sec,
            );
            let _ = update_job_status(&state.pool, job_id, "completed", completed_diag).await?;

            if let Some(result_id) = latest_result_id_for_job(&state.pool, job_id).await?
                && let Err(err) = mark_result_cache_ready(&state.pool, job_id, result_id).await
            {
                warn!(
                    error = %err,
                    job_id = %job_id,
                    result_id = %result_id,
                    "failed to mark result cache ready"
                );
            }
        }
        JobPayload::InvalidateFactorization {
            job_id,
            snapshot_id,
        } => {
            let invalidated = state.solver.invalidate(snapshot_id);
            let _ = update_job_status(
                &state.pool,
                job_id,
                "completed",
                serde_json::json!({"invalidated": invalidated}),
            )
            .await?;
        }
        JobPayload::RebuildFactorization {
            job_id,
            snapshot_id,
            print_level,
        } => {
            let _ = state.solver.invalidate(snapshot_id);
            let data = fetch_snapshot_sparse_data(state, snapshot_id).await?;
            let prepared: PrepareResult = state.solver.prepare(
                &data,
                NumericOptions {
                    print_level: print_level.unwrap_or(0.0),
                },
            )?;
            let _ = update_job_status(
                &state.pool,
                job_id,
                "ready",
                serde_json::to_value(prepared)?,
            )
            .await?;
        }
        JobPayload::BuildSnapshot {
            job_id,
            snapshot_id,
            scope,
            all_states,
            process_states,
            include_user_id,
            include_user_state_codes,
            include_user_unassigned_only,
            include_user_review_free_only,
            data_scope,
            scope_manifest,
            scope_manifest_sha256,
            lcia_method_factor_source,
            lcia_factor_coverage_contract,
            request_roots,
            provider_rule,
            reference_normalization_mode,
            allocation_fraction_mode,
            process_limit,
            self_loop_cutoff,
            singular_eps,
            method_id,
            method_version,
            no_lcia,
        } => {
            if data_scope.is_some() && build_snapshot_worker_lease.is_none() {
                return Err(anyhow::anyhow!(
                    "versioned snapshot scope requires authenticated worker_jobs context"
                ));
            }
            let lock_strategy = if build_snapshot_worker_lease.is_some() {
                "worker_jobs_phase_lease"
            } else {
                "postgres_transaction_advisory_lock"
            };
            let running_db_write_sec = update_job_status(
                &state.pool,
                job_id,
                "running",
                serde_json::json!({
                    "phase": "build_snapshot",
                    "snapshot_id": snapshot_id,
                    "build_snapshot_lock": {
                        "enabled": true,
                        "strategy": lock_strategy,
                        "max_concurrency": state.build_snapshot_max_concurrency,
                        "waiting": true,
                    },
                }),
            )
            .await?;

            let lock_guard = match build_snapshot_worker_lease.clone() {
                Some(lease) => {
                    acquire_build_snapshot_worker_jobs_slot(
                        &state.pool,
                        state.build_snapshot_max_concurrency,
                        state.build_snapshot_lock_poll_interval,
                        lease,
                    )
                    .await?
                }
                None => {
                    acquire_build_snapshot_lock(
                        &state.pool,
                        state.build_snapshot_max_concurrency,
                        state.build_snapshot_lock_poll_interval,
                    )
                    .await?
                }
            };
            let mut build_snapshot_lock = lock_guard.diagnostics();
            if let Some(lock_payload) = build_snapshot_lock.as_object_mut() {
                lock_payload.insert("waiting".to_owned(), Value::Bool(false));
            }
            let _lock_running_db_write_sec = update_job_status(
                &state.pool,
                job_id,
                "running",
                serde_json::json!({
                    "phase": "build_snapshot",
                    "snapshot_id": snapshot_id,
                    "build_snapshot_lock": build_snapshot_lock,
                }),
            )
            .await?;

            let versioned_builder_args = data_scope
                .as_ref()
                .map(|data_scope| {
                    Ok::<_, anyhow::Error>(VersionedSnapshotBuilderArgs {
                        all_states: all_states
                            .ok_or_else(|| anyhow::anyhow!("versioned build missing all_states"))?,
                        include_user_state_codes: include_user_state_codes.clone().ok_or_else(
                            || anyhow::anyhow!("versioned build missing include_user_state_codes"),
                        )?,
                        include_user_unassigned_only: include_user_unassigned_only
                            .ok_or_else(|| anyhow::anyhow!("versioned build missing team guard"))?,
                        include_user_review_free_only: include_user_review_free_only.ok_or_else(
                            || anyhow::anyhow!("versioned build missing review guard"),
                        )?,
                        data_scope: data_scope.clone(),
                        scope_manifest: scope_manifest.clone().ok_or_else(|| {
                            anyhow::anyhow!("versioned build missing scope_manifest")
                        })?,
                        scope_manifest_sha256: scope_manifest_sha256.clone().ok_or_else(|| {
                            anyhow::anyhow!("versioned build missing scope_manifest_sha256")
                        })?,
                        lcia_method_factor_source: lcia_method_factor_source.clone().ok_or_else(
                            || anyhow::anyhow!("versioned build missing method source contract"),
                        )?,
                        lcia_factor_coverage_contract: lcia_factor_coverage_contract
                            .clone()
                            .ok_or_else(|| {
                                anyhow::anyhow!("versioned build missing factor coverage contract")
                            })?,
                    })
                })
                .transpose()?;
            let build_future = run_snapshot_builder_job(
                snapshot_id,
                process_states.as_deref(),
                include_user_id,
                request_roots.as_deref(),
                provider_rule.as_deref(),
                reference_normalization_mode.as_deref(),
                allocation_fraction_mode.as_deref(),
                process_limit,
                self_loop_cutoff,
                singular_eps,
                method_id,
                method_version.as_deref(),
                None,
                None,
                None,
                None,
                versioned_builder_args.as_ref(),
                None,
                no_lcia.unwrap_or(false),
            );
            let executed_result = match build_snapshot_worker_lease.as_ref() {
                Some(lease) => {
                    run_snapshot_builder_job_with_worker_heartbeat(
                        &state.pool,
                        lease,
                        build_snapshot_lock.clone(),
                        build_future,
                    )
                    .await
                }
                None => build_future.await,
            };
            let mut completed_build_snapshot_lock = lock_guard.diagnostics();
            if let Some(lock_payload) = completed_build_snapshot_lock.as_object_mut() {
                lock_payload.insert("waiting".to_owned(), Value::Bool(false));
            }
            let release_result = lock_guard.release().await;
            let executed = match executed_result {
                Ok(executed) => {
                    if let Err(err) = release_result {
                        return Err(anyhow::anyhow!(
                            "failed to release build_snapshot advisory lock: {err}"
                        ));
                    }
                    executed
                }
                Err(err) => {
                    if let Err(release_err) = release_result {
                        warn!(
                            error = %release_err,
                            "failed to release build_snapshot advisory lock after builder failure"
                        );
                    }
                    return Err(err);
                }
            };
            let build_snapshot_lock = completed_build_snapshot_lock;

            let resolved_snapshot_id = executed.resolved_snapshot_id;
            let build_timing_sec = executed.build_timing_sec.clone();
            let calculation_evidence = if let Some(versioned) = &versioned_builder_args {
                let index = fetch_snapshot_index_document(state, resolved_snapshot_id).await?;
                let evidence = index.calculation_evidence.ok_or_else(|| {
                    anyhow::anyhow!("versioned snapshot build produced no calculation evidence")
                })?;
                validate_calculation_evidence(&evidence)?;
                if evidence.scope_manifest_sha256 != versioned.scope_manifest_sha256 {
                    return Err(anyhow::anyhow!(
                        "versioned snapshot build produced scope evidence drift"
                    ));
                }
                Some(evidence)
            } else {
                None
            };
            if let Some(lease) = &build_snapshot_worker_lease {
                crate::worker_jobs::heartbeat_worker_job(
                    &state.pool,
                    lease.worker_job_id,
                    lease.lease_token,
                    "build_snapshot",
                    0.90,
                    Some(serde_json::json!({
                        "build_snapshot_lock": build_snapshot_lock.clone(),
                        "publishing": true,
                        "build_snapshot_result": {
                            "requested_snapshot_id": snapshot_id,
                            "resolved_snapshot_id": resolved_snapshot_id,
                            "calculation_evidence": calculation_evidence.clone(),
                        },
                    })),
                    lease.lease_seconds.clamp(1, 86_400),
                )
                .await
                .map_err(|err| {
                    anyhow::anyhow!(
                        "build_snapshot worker_jobs lease heartbeat failed before publish: {err}"
                    )
                })?;
            }
            if resolved_snapshot_id != snapshot_id {
                set_job_snapshot_id(&state.pool, job_id, resolved_snapshot_id).await?;
            }

            let source_hash = fetch_snapshot_source_hash(&state.pool, resolved_snapshot_id).await?;
            if let Some(scope_value) = scope.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                upsert_active_snapshot(
                    &state.pool,
                    scope_value,
                    resolved_snapshot_id,
                    source_hash.as_deref(),
                    include_user_id,
                    job_id,
                )
                .await?;
            }

            let mut completed_payload = serde_json::json!({
                "phase": "build_snapshot",
                "requested_snapshot_id": snapshot_id,
                "snapshot_id": resolved_snapshot_id,
                "builder": executed,
                "build_snapshot_lock": build_snapshot_lock,
                "source_hash": source_hash,
                "calculation_evidence": calculation_evidence,
            });
            if let (Some(build_timing_sec), Some(payload)) =
                (build_timing_sec, completed_payload.as_object_mut())
            {
                payload.insert("build_timing_sec".to_owned(), build_timing_sec);
            }
            let completed_diag =
                merge_job_status_update_timing(completed_payload, "running", running_db_write_sec);
            let _ = update_job_status(&state.pool, job_id, "completed", completed_diag).await?;
        }
        JobPayload::LciaResultPackageBuild { .. } => {
            return Err(anyhow::anyhow!(
                "lcia_result package build execution requires worker_jobs context"
            ));
        }
        JobPayload::ScopeClosureCheck { .. } => {
            return Err(anyhow::anyhow!(
                "scope closure execution requires worker_jobs context"
            ));
        }
    }

    Ok(())
}

/// Ensures factorization exists in cache.
pub async fn ensure_prepared(
    state: &AppState,
    snapshot_id: Uuid,
    print_level: f64,
) -> anyhow::Result<()> {
    if state
        .solver
        .factorization_status(snapshot_id, NumericOptions { print_level })
        .is_none()
    {
        let data = fetch_snapshot_sparse_data(state, snapshot_id).await?;
        let _ = state
            .solver
            .prepare(&data, NumericOptions { print_level })?;
    }
    Ok(())
}

async fn persist_solve_one_result(
    state: &AppState,
    job_id: Uuid,
    snapshot_id: Uuid,
    solved: &SolveResult,
    timing: &SolveComputationTiming,
    calculation_evidence: Option<LcaCalculationEvidence>,
) -> anyhow::Result<Value> {
    let timing_json = serde_json::to_value(timing)?;
    let encode_started = Instant::now();
    let encoded = encode_solve_one_artifact(snapshot_id, job_id, solved)?;
    let encode_artifact_sec = encode_started.elapsed().as_secs_f64();

    persist_result_artifact(
        state,
        job_id,
        snapshot_id,
        PersistArtifactInput {
            suffix: "solve_one",
            encoded,
            compute_timing: Some(timing_json),
            encode_artifact_sec,
            calculation_evidence,
            calculation_bundle: None,
        },
    )
    .await
}

async fn persist_solve_batch_result(
    state: &AppState,
    job_id: Uuid,
    snapshot_id: Uuid,
    solved: &SolveBatchResult,
    suffix: &'static str,
    calculation_evidence: Option<LcaCalculationEvidence>,
    calculation_bundle: Option<CalculationBundleArtifactRef>,
) -> anyhow::Result<Value> {
    let encode_started = Instant::now();
    let encoded = encode_solve_batch_artifact(snapshot_id, job_id, solved)?;
    let encode_artifact_sec = encode_started.elapsed().as_secs_f64();

    persist_result_artifact(
        state,
        job_id,
        snapshot_id,
        PersistArtifactInput {
            suffix,
            encoded,
            compute_timing: None,
            encode_artifact_sec,
            calculation_evidence,
            calculation_bundle,
        },
    )
    .await
}

async fn solve_all_unit_with_calculation_bundle(
    state: &AppState,
    job_id: Uuid,
    snapshot_id: Uuid,
    process_count: usize,
    solve_batch_size: usize,
    print_level: f64,
) -> anyhow::Result<(SolveBatchResult, CalculationBundleArtifactRef)> {
    let snapshot_meta = fetch_snapshot_artifact_meta(&state.pool, snapshot_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("snapshot {snapshot_id} has no ready artifact"))?;
    let decoded =
        fetch_decoded_snapshot_artifact_from_meta(state, snapshot_id, &snapshot_meta).await?;
    let release_evidence = decoded
        .compiled_graph
        .as_ref()
        .and_then(|graph| graph.release_evidence.clone())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "snapshot {snapshot_id} lacks exact Calculation Bundle release evidence; rebuild the snapshot"
            )
        })?;
    let snapshot_index = fetch_snapshot_index_document(state, snapshot_id).await?;
    if usize::try_from(decoded.payload.process_count)? != process_count {
        return Err(anyhow::anyhow!(
            "snapshot process count drift while building Calculation Bundle"
        ));
    }
    let mut bundle_writer = CalculationBundleWriter::new(
        job_id,
        snapshot_id,
        snapshot_meta.sha256,
        usize::try_from(decoded.payload.flow_count)?,
        decoded.config,
        decoded.coverage,
        &snapshot_index,
        &release_evidence,
    )?;

    let mut legacy_items = Vec::with_capacity(process_count);
    let internal_options = SolveOptions {
        return_x: true,
        return_g: false,
        return_h: true,
    };
    for artifact_start in (0..process_count).step_by(CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT) {
        let artifact_end =
            (artifact_start + CALCULATION_BUNDLE_CHUNK_PROCESS_COUNT).min(process_count);
        let mut artifact_items = Vec::with_capacity(artifact_end - artifact_start);
        for solve_start in (artifact_start..artifact_end).step_by(solve_batch_size) {
            let solve_end = (solve_start + solve_batch_size).min(artifact_end);
            let rhs_batch = build_all_unit_rhs_batch(process_count, solve_start, solve_end);
            let partial = state.solver.solve_batch(
                snapshot_id,
                NumericOptions { print_level },
                rhs_batch.as_slice(),
                internal_options,
            )?;
            artifact_items.extend(partial.items);
        }
        bundle_writer.write_result_chunk(artifact_start, artifact_items.as_slice())?;
        legacy_items.extend(artifact_items.into_iter().map(|item| SolveResult {
            x: None,
            g: None,
            h: item.h,
            factorization_state: item.factorization_state,
        }));
    }
    let built = bundle_writer.finish()?;
    let bundle_ref = upload_built_calculation_bundle(&state.object_store, &built).await?;
    Ok((
        SolveBatchResult {
            items: legacy_items,
        },
        bundle_ref,
    ))
}

async fn persist_solve_all_unit_query_artifact(
    state: &AppState,
    job_id: Uuid,
    snapshot_id: Uuid,
    solved: &SolveBatchResult,
) -> anyhow::Result<QueryArtifactMeta> {
    let encoded = encode_solve_all_unit_query_artifact(snapshot_id, job_id, solved)?;
    let artifact_len = i64::try_from(encoded.bytes.len())
        .map_err(|_| anyhow::anyhow!("query artifact size overflow"))?;
    let artifact_url = state
        .object_store
        .upload_result(
            snapshot_id,
            job_id,
            "solve_all_unit_query",
            encoded.extension,
            encoded.content_type,
            encoded.bytes,
        )
        .await?;

    Ok(QueryArtifactMeta {
        url: artifact_url,
        sha256: encoded.sha256,
        byte_size: artifact_len,
        format: encoded.format.to_owned(),
    })
}

async fn persist_contribution_path_result(
    state: &AppState,
    job_id: Uuid,
    snapshot_id: Uuid,
    analysis: &ContributionPathArtifact,
    calculation_evidence: Option<LcaCalculationEvidence>,
) -> anyhow::Result<Value> {
    let encoded = encode_contribution_path_artifact(analysis)?;
    persist_result_artifact(
        state,
        job_id,
        snapshot_id,
        PersistArtifactInput {
            suffix: "contribution_path",
            encoded,
            compute_timing: None,
            encode_artifact_sec: 0.0,
            calculation_evidence,
            calculation_bundle: None,
        },
    )
    .await
}

async fn upsert_latest_all_unit_result(
    pool: &PgPool,
    snapshot_id: Uuid,
    job_id: Uuid,
    result_id: Uuid,
    query_artifact: &QueryArtifactMeta,
) -> anyhow::Result<Uuid> {
    let row = sqlx::query(
        r"
        INSERT INTO public.lca_latest_all_unit_results (
            snapshot_id,
            job_id,
            result_id,
            query_artifact_url,
            query_artifact_sha256,
            query_artifact_byte_size,
            query_artifact_format,
            status,
            computed_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'ready', NOW(), NOW())
        ON CONFLICT (snapshot_id)
        DO UPDATE SET
            job_id = EXCLUDED.job_id,
            result_id = EXCLUDED.result_id,
            query_artifact_url = EXCLUDED.query_artifact_url,
            query_artifact_sha256 = EXCLUDED.query_artifact_sha256,
            query_artifact_byte_size = EXCLUDED.query_artifact_byte_size,
            query_artifact_format = EXCLUDED.query_artifact_format,
            status = EXCLUDED.status,
            computed_at = EXCLUDED.computed_at,
            updated_at = NOW()
        RETURNING id
        ",
    )
    .bind(snapshot_id)
    .bind(job_id)
    .bind(result_id)
    .bind(query_artifact.url.as_str())
    .bind(query_artifact.sha256.as_str())
    .bind(query_artifact.byte_size)
    .bind(query_artifact.format.as_str())
    .fetch_one(pool)
    .await?;
    Ok(row.try_get::<Uuid, _>("id")?)
}

struct PersistArtifactInput {
    suffix: &'static str,
    encoded: EncodedArtifact,
    compute_timing: Option<Value>,
    encode_artifact_sec: f64,
    calculation_evidence: Option<LcaCalculationEvidence>,
    calculation_bundle: Option<CalculationBundleArtifactRef>,
}

struct ArtifactMeta {
    format: String,
    sha256: String,
    encoded_len: usize,
    artifact_len: i64,
}

#[derive(Debug, Clone)]
struct QueryArtifactMeta {
    url: String,
    sha256: String,
    byte_size: i64,
    format: String,
}

#[derive(Debug, Clone)]
struct LciaResultPackageReadyInput {
    build_worker_job_id: Uuid,
    package_version: String,
    snapshot_id: Uuid,
    result_id: Uuid,
    latest_all_unit_result_id: Option<Uuid>,
    result_artifact_ref: Value,
    query_artifact_ref: Value,
    artifact_manifest: Value,
    available_impact_categories: Value,
    default_impact_category: Option<String>,
    package_result_hash: Option<String>,
    audit: Value,
}

#[derive(Debug, Clone)]
struct LciaResultPackageArtifacts {
    result_id: Uuid,
    latest_all_unit_result_id: Uuid,
    result_diag: Value,
    query_artifact_meta: QueryArtifactMeta,
    calculation_bundle: CalculationBundleArtifactRef,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct SnapshotBuilderExecution {
    pub(crate) requested_snapshot_id: Uuid,
    pub(crate) resolved_snapshot_id: Uuid,
    #[serde(skip_serializing)]
    pub(crate) build_timing_sec: Option<Value>,
    pub(crate) command: Vec<String>,
    pub(crate) exit_code: i32,
    pub(crate) stdout_tail: String,
    pub(crate) stderr_tail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scope_closure_discovery: Option<Value>,
}

#[derive(Debug, Clone)]
struct BuilderCommandCandidate {
    program: String,
    args: Vec<String>,
    current_dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct VersionedSnapshotBuilderArgs {
    all_states: bool,
    include_user_state_codes: String,
    include_user_unassigned_only: bool,
    include_user_review_free_only: bool,
    data_scope: String,
    scope_manifest: Value,
    scope_manifest_sha256: String,
    lcia_method_factor_source: Value,
    lcia_factor_coverage_contract: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct ScopeClosureSnapshotBuilderArgs {
    pub(crate) mode: ScopeClosureSnapshotBuilderMode,
    pub(crate) binding: Value,
    pub(crate) data_snapshot: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopeClosureSnapshotBuilderMode {
    Discovery,
    Build,
}

impl ScopeClosureSnapshotBuilderMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Discovery => "discovery",
            Self::Build => "build",
        }
    }
}

#[derive(Clone)]
struct PersistTimingContext {
    compute_timing: Option<Value>,
    encode_artifact_sec: f64,
    upload_artifact_sec: f64,
    calculation_evidence: Option<LcaCalculationEvidence>,
    calculation_bundle: Option<CalculationBundleArtifactRef>,
}

pub(crate) async fn run_review_submit_gate_snapshot_builder(
    state: &AppState,
    snapshot_id: Uuid,
    include_user_id: Uuid,
    request_roots: &[crate::graph_types::RequestRootProcess],
    revision_checksum: &str,
) -> anyhow::Result<SnapshotBuilderExecution> {
    let lock_guard = acquire_build_snapshot_lock(
        &state.pool,
        state.build_snapshot_max_concurrency,
        state.build_snapshot_lock_poll_interval,
    )
    .await?;
    let executed_result = run_snapshot_builder_job(
        snapshot_id,
        None,
        Some(include_user_id),
        Some(request_roots),
        Some("split_by_process_volume"),
        Some("lenient"),
        Some("lenient"),
        None,
        None,
        None,
        None,
        None,
        Some(REVIEW_SUBMIT_SNAPSHOT_ARTIFACT_PURPOSE),
        Some(REVIEW_SUBMIT_SNAPSHOT_TTL_SECONDS),
        Some(REVIEW_SUBMIT_SNAPSHOT_TTL_SECONDS),
        Some(revision_checksum),
        None,
        None,
        true,
    )
    .await;
    let release_result = lock_guard.release().await;

    match executed_result {
        Ok(executed) => {
            if let Err(err) = release_result {
                return Err(anyhow::anyhow!(
                    "failed to release build_snapshot advisory lock: {err}"
                ));
            }
            Ok(executed)
        }
        Err(err) => {
            if let Err(release_err) = release_result {
                warn!(
                    error = %release_err,
                    "failed to release build_snapshot advisory lock after review-submit snapshot builder failure"
                );
            }
            Err(err)
        }
    }
}

pub(crate) async fn run_scope_closure_snapshot_builder(
    state: &AppState,
    requested_snapshot_id: Uuid,
    request_roots: &[RequestRootProcess],
    scope_closure: &ScopeClosureSnapshotBuilderArgs,
) -> anyhow::Result<SnapshotBuilderExecution> {
    let process_states = crate::default_snapshot_process_states_arg();
    let lock_guard = acquire_build_snapshot_lock(
        &state.pool,
        state.build_snapshot_max_concurrency,
        state.build_snapshot_lock_poll_interval,
    )
    .await?;
    let executed_result = run_snapshot_builder_job(
        requested_snapshot_id,
        Some(process_states.as_str()),
        None,
        Some(request_roots),
        Some("split_by_process_volume"),
        Some("strict"),
        Some("strict"),
        None,
        None,
        None,
        None,
        None,
        Some("scope_closure_preflight"),
        None,
        None,
        None,
        None,
        Some(scope_closure),
        false,
    )
    .await;
    let release_result = lock_guard.release().await;
    match executed_result {
        Ok(executed) => {
            release_result.map_err(|error| {
                anyhow::anyhow!(
                    "failed to release build_snapshot lock after scope closure build: {error}"
                )
            })?;
            Ok(executed)
        }
        Err(error) => {
            if let Err(release_error) = release_result {
                warn!(
                    error = %release_error,
                    "failed to release build_snapshot lock after scope closure failure"
                );
            }
            Err(error)
        }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn handle_lcia_result_package_build_worker_job(
    state: &AppState,
    build_worker_job_id: Uuid,
    payload: &JobPayload,
) -> anyhow::Result<Value> {
    let JobPayload::LciaResultPackageBuild {
        build_id,
        input_manifest,
        input_manifest_hash,
        default_impact_category,
        closure_check_id,
        closure_certificate_hash,
        effective_scope_hash,
        data_snapshot_token,
        snapshot_id: closure_snapshot_id,
        snapshot_hash: closure_snapshot_hash,
        closure_bundle_artifact_id,
        closure_bundle_hash,
        report_artifact_manifest_hash,
        snapshot_artifact_id,
        snapshot_index_sha256,
        snapshot_build_contract_hash,
        ..
    } = payload
    else {
        return Err(anyhow::anyhow!(
            "expected lcia_result_package_build payload"
        ));
    };

    let result_job_id = *build_id;
    let snapshot_execution_mode = package_snapshot_execution_mode(*closure_check_id);
    let request_roots = lcia_result_package_request_roots(
        input_manifest,
        snapshot_execution_mode == PackageSnapshotExecutionMode::LegacyLiveBuild,
    )?;
    let (snapshot_id, snapshot_source) = if snapshot_execution_mode
        == PackageSnapshotExecutionMode::CertifiedReuse
    {
        let snapshot_id = closure_snapshot_id
            .ok_or_else(|| anyhow::anyhow!("certified package payload omitted snapshot_id"))?;
        let snapshot_hash = closure_snapshot_hash
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("certified package payload omitted snapshot_hash"))?;
        let effective_scope_hash = effective_scope_hash.as_deref().ok_or_else(|| {
            anyhow::anyhow!("certified package payload omitted effective_scope_hash")
        })?;
        let data_snapshot_token = data_snapshot_token.as_deref().ok_or_else(|| {
            anyhow::anyhow!("certified package payload omitted data_snapshot_token")
        })?;
        let closure_bundle_hash = closure_bundle_hash.as_deref().ok_or_else(|| {
            anyhow::anyhow!("certified package payload omitted closure_bundle_hash")
        })?;
        let closure_bundle_artifact_id = closure_bundle_artifact_id.ok_or_else(|| {
            anyhow::anyhow!("certified package payload omitted closure_bundle_artifact_id")
        })?;
        let closure_check_id = closure_check_id
            .ok_or_else(|| anyhow::anyhow!("certified package payload omitted closure_check_id"))?;
        let snapshot_artifact_id = snapshot_artifact_id.ok_or_else(|| {
            anyhow::anyhow!("certified package payload omitted snapshot_artifact_id")
        })?;
        let snapshot_index_sha256 = snapshot_index_sha256.as_deref().ok_or_else(|| {
            anyhow::anyhow!("certified package payload omitted snapshot_index_sha256")
        })?;
        let snapshot_build_contract_hash =
            snapshot_build_contract_hash.as_deref().ok_or_else(|| {
                anyhow::anyhow!("certified package payload omitted snapshot_build_contract_hash")
            })?;
        prepare_certified_package_snapshot(
            state,
            closure_check_id,
            snapshot_id,
            snapshot_hash,
            effective_scope_hash,
            data_snapshot_token,
            closure_bundle_hash,
            closure_bundle_artifact_id,
            snapshot_artifact_id,
            snapshot_index_sha256,
            snapshot_build_contract_hash,
            request_roots.as_slice(),
        )
        .await?;
        (
            snapshot_id,
            serde_json::json!({
                "mode": "certified_snapshot_reuse_v1",
                "snapshotId": snapshot_id,
                "snapshotHash": snapshot_hash,
                "liveSnapshotBuilderInvoked": false,
            }),
        )
    } else {
        let (executed, build_snapshot_lock) =
            run_lcia_result_package_snapshot_builder(state, *build_id, request_roots.as_slice())
                .await?;
        (
            executed.resolved_snapshot_id,
            serde_json::json!({
                "mode": "legacy_live_snapshot_build_v1",
                "snapshotBuilder": executed,
                "buildSnapshotLock": build_snapshot_lock,
                "liveSnapshotBuilderInvoked": true,
            }),
        )
    };
    let artifacts =
        persist_lcia_result_package_all_unit_artifacts(state, result_job_id, snapshot_id).await?;
    link_lcia_result_package_worker_job_domain_refs(
        &state.pool,
        build_worker_job_id,
        result_job_id,
    )
    .await?;

    let artifact_manifest = serde_json::json!({
        "artifactManifestVersion": "lcia-result-package-worker.v1",
        "inputManifestHash": input_manifest_hash,
        "snapshotSource": snapshot_source,
        "resultDiagnostics": artifacts.result_diag.clone(),
        "queryArtifact": {
            "artifactUrl": artifacts.query_artifact_meta.url.clone(),
            "artifactSha256": artifacts.query_artifact_meta.sha256.clone(),
            "artifactByteSize": artifacts.query_artifact_meta.byte_size,
            "artifactFormat": artifacts.query_artifact_meta.format.clone(),
        },
        "calculationBundle": artifacts.calculation_bundle.clone(),
        "scopeClosureEvidence": closure_check_id.map(|closure_check_id| serde_json::json!({
            "closureCheckId": closure_check_id,
            "certificateHash": closure_certificate_hash,
            "effectiveScopeHash": effective_scope_hash,
            "dataSnapshotToken": data_snapshot_token,
            "snapshotId": closure_snapshot_id,
            "snapshotHash": closure_snapshot_hash,
            "closureBundleHash": closure_bundle_hash,
            "closureBundleArtifactId": closure_bundle_artifact_id,
            "reportArtifactManifestHash": report_artifact_manifest_hash,
        })),
    });

    // Close the object-store TOCTOU window as far as the worker can immediately before the
    // database performs its final lease, revocation, and authoritative-metadata checks.
    if let Some(closure_check_id) = closure_check_id {
        verify_certified_package_snapshot(
            state,
            *closure_check_id,
            closure_snapshot_id
                .ok_or_else(|| anyhow::anyhow!("certified package payload omitted snapshot_id"))?,
            closure_snapshot_hash.as_deref().ok_or_else(|| {
                anyhow::anyhow!("certified package payload omitted snapshot_hash")
            })?,
            effective_scope_hash.as_deref().ok_or_else(|| {
                anyhow::anyhow!("certified package payload omitted effective_scope_hash")
            })?,
            data_snapshot_token.as_deref().ok_or_else(|| {
                anyhow::anyhow!("certified package payload omitted data_snapshot_token")
            })?,
            closure_bundle_hash.as_deref().ok_or_else(|| {
                anyhow::anyhow!("certified package payload omitted closure_bundle_hash")
            })?,
            closure_bundle_artifact_id.ok_or_else(|| {
                anyhow::anyhow!("certified package payload omitted closure_bundle_artifact_id")
            })?,
            snapshot_artifact_id.ok_or_else(|| {
                anyhow::anyhow!("certified package payload omitted snapshot_artifact_id")
            })?,
            snapshot_index_sha256.as_deref().ok_or_else(|| {
                anyhow::anyhow!("certified package payload omitted snapshot_index_sha256")
            })?,
            snapshot_build_contract_hash.as_deref().ok_or_else(|| {
                anyhow::anyhow!("certified package payload omitted snapshot_build_contract_hash")
            })?,
            request_roots.as_slice(),
        )
        .await?;
    }

    let mark_ready = mark_lcia_result_package_ready(
        &state.pool,
        LciaResultPackageReadyInput {
            build_worker_job_id,
            package_version: lcia_result_package_version(*build_id),
            snapshot_id,
            result_id: artifacts.result_id,
            latest_all_unit_result_id: Some(artifacts.latest_all_unit_result_id),
            result_artifact_ref: lcia_result_artifact_ref(&artifacts.result_diag),
            query_artifact_ref: lcia_result_query_artifact_ref(&artifacts.query_artifact_meta),
            artifact_manifest,
            available_impact_categories: serde_json::json!([]),
            default_impact_category: default_impact_category.clone(),
            package_result_hash: artifacts
                .result_diag
                .get("artifact_sha256")
                .and_then(Value::as_str)
                .map(str::to_owned),
            audit: serde_json::json!({
                "command": "worker_lcia_result_package_build",
                "buildId": build_id,
                "buildWorkerJobId": build_worker_job_id,
                "snapshotId": snapshot_id,
                "closureCheckId": closure_check_id,
                "closureCertificateHash": closure_certificate_hash,
                "resultId": artifacts.result_id,
                "latestAllUnitResultId": artifacts.latest_all_unit_result_id,
            }),
        },
    )
    .await?;

    Ok(mark_ready)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PackageSnapshotExecutionMode {
    CertifiedReuse,
    LegacyLiveBuild,
}

fn package_snapshot_execution_mode(closure_check_id: Option<Uuid>) -> PackageSnapshotExecutionMode {
    if closure_check_id.is_some() {
        PackageSnapshotExecutionMode::CertifiedReuse
    } else {
        PackageSnapshotExecutionMode::LegacyLiveBuild
    }
}

#[allow(clippy::too_many_arguments)]
async fn prepare_certified_package_snapshot(
    state: &AppState,
    closure_check_id: Uuid,
    snapshot_id: Uuid,
    expected_snapshot_hash: &str,
    expected_effective_scope_hash: &str,
    expected_data_snapshot_token: &str,
    expected_closure_bundle_hash: &str,
    expected_closure_bundle_artifact_id: Uuid,
    expected_snapshot_artifact_id: Uuid,
    expected_snapshot_index_sha256: &str,
    expected_snapshot_build_contract_hash: &str,
    request_roots: &[RequestRootProcess],
) -> anyhow::Result<()> {
    let decoded = verify_certified_package_snapshot(
        state,
        closure_check_id,
        snapshot_id,
        expected_snapshot_hash,
        expected_effective_scope_hash,
        expected_data_snapshot_token,
        expected_closure_bundle_hash,
        expected_closure_bundle_artifact_id,
        expected_snapshot_artifact_id,
        expected_snapshot_index_sha256,
        expected_snapshot_build_contract_hash,
        request_roots,
    )
    .await?;
    state
        .solver
        .prepare(&decoded.payload, NumericOptions::default())?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn verify_certified_package_snapshot(
    state: &AppState,
    closure_check_id: Uuid,
    snapshot_id: Uuid,
    expected_snapshot_hash: &str,
    expected_effective_scope_hash: &str,
    expected_data_snapshot_token: &str,
    expected_closure_bundle_hash: &str,
    expected_closure_bundle_artifact_id: Uuid,
    expected_snapshot_artifact_id: Uuid,
    expected_snapshot_index_sha256: &str,
    expected_snapshot_build_contract_hash: &str,
    request_roots: &[RequestRootProcess],
) -> anyhow::Result<DecodedSnapshotArtifact> {
    verify_certified_closure_bundle_artifact(
        state,
        closure_check_id,
        expected_closure_bundle_artifact_id,
        expected_closure_bundle_hash,
        expected_data_snapshot_token,
    )
    .await?;
    let binding = ScopeClosureSnapshotBinding {
        schema_version: "lcia.scope-closure-snapshot-binding.v1".to_owned(),
        effective_scope_hash: expected_effective_scope_hash.to_owned(),
        data_snapshot_token: expected_data_snapshot_token.to_owned(),
        closure_bundle_hash: expected_closure_bundle_hash.to_owned(),
    };
    let (facts, decoded) = load_scope_closure_snapshot_facts(
        state,
        snapshot_id,
        &binding,
        request_roots,
        Some(expected_snapshot_artifact_id),
    )
    .await?;
    if facts.snapshot_hash != expected_snapshot_hash
        || facts.snapshot_artifact_id != expected_snapshot_artifact_id
        || facts.snapshot_index_sha256 != expected_snapshot_index_sha256
        || facts.snapshot_build_contract_hash != expected_snapshot_build_contract_hash
    {
        return Err(anyhow::anyhow!("certified_snapshot_evidence_mismatch"));
    }
    Ok(decoded)
}

async fn verify_certified_closure_bundle_artifact(
    state: &AppState,
    expected_closure_check_id: Uuid,
    expected_artifact_id: Uuid,
    expected_bundle_hash: &str,
    expected_data_snapshot_token: &str,
) -> anyhow::Result<()> {
    let row = sqlx::query(
        r"
        SELECT artifact_type, storage_path, content_type, byte_size,
               checksum_sha256, metadata
        FROM public.worker_job_artifacts
        WHERE id = $1
        ",
    )
    .bind(expected_artifact_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("certified_closure_bundle_artifact_not_found"))?;

    let artifact_type = row.try_get::<String, _>("artifact_type")?;
    let storage_path = row.try_get::<String, _>("storage_path")?;
    let content_type = row.try_get::<String, _>("content_type")?;
    let byte_size = row.try_get::<i64, _>("byte_size")?;
    let checksum_sha256 = row.try_get::<String, _>("checksum_sha256")?;
    let metadata = row.try_get::<Value, _>("metadata")?;
    let expected_closure_check_id = expected_closure_check_id.to_string();
    if artifact_type != "closure_bundle"
        || content_type != "application/json"
        || checksum_sha256 != expected_bundle_hash
        || metadata.get("schemaVersion").and_then(Value::as_str)
            != Some("lcia.scope-closure-artifact.v1")
        || metadata.get("closureCheckId").and_then(Value::as_str)
            != Some(expected_closure_check_id.as_str())
    {
        return Err(anyhow::anyhow!(
            "certified_closure_bundle_artifact_metadata_mismatch"
        ));
    }

    let bytes = state
        .object_store
        .download_object_key(storage_path.as_str())
        .await?;
    if i64::try_from(bytes.len())? != byte_size
        || hex::encode(Sha256::digest(bytes.as_slice())) != expected_bundle_hash
    {
        return Err(anyhow::anyhow!(
            "certified_closure_bundle_artifact_content_mismatch"
        ));
    }
    let bundle = serde_json::from_slice::<Value>(bytes.as_slice())?;
    if bundle.get("schemaVersion").and_then(Value::as_str) != Some("lcia.scope-closure-bundle.v1")
        || bundle.get("dataSnapshotToken").and_then(Value::as_str)
            != Some(expected_data_snapshot_token)
    {
        return Err(anyhow::anyhow!(
            "certified_closure_bundle_artifact_binding_mismatch"
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScopeClosureSnapshotFacts {
    pub(crate) snapshot_id: Uuid,
    pub(crate) snapshot_hash: String,
    pub(crate) snapshot_artifact_id: Uuid,
    pub(crate) snapshot_index_sha256: String,
    pub(crate) snapshot_build_contract_hash: String,
    pub(crate) artifact_format: String,
}

pub(crate) async fn fetch_scope_closure_snapshot_facts(
    state: &AppState,
    snapshot_id: Uuid,
    binding: &ScopeClosureSnapshotBinding,
    expected_axis: &[RequestRootProcess],
) -> anyhow::Result<ScopeClosureSnapshotFacts> {
    load_scope_closure_snapshot_facts(state, snapshot_id, binding, expected_axis, None)
        .await
        .map(|(facts, _)| facts)
}

async fn load_scope_closure_snapshot_facts(
    state: &AppState,
    snapshot_id: Uuid,
    binding: &ScopeClosureSnapshotBinding,
    expected_axis: &[RequestRootProcess],
    expected_artifact_id: Option<Uuid>,
) -> anyhow::Result<(ScopeClosureSnapshotFacts, DecodedSnapshotArtifact)> {
    let row = sqlx::query(
        r"
        SELECT s.status AS snapshot_status,
               a.id AS snapshot_artifact_id, a.artifact_url,
               a.artifact_format, a.artifact_sha256,
               a.snapshot_index_sha256, a.snapshot_build_contract_hash,
               a.effective_scope_hash, a.data_snapshot_token, a.closure_bundle_hash
        FROM public.lca_network_snapshots s
        JOIN public.lca_snapshot_artifacts a
          ON a.snapshot_id = s.id AND a.status = 'ready'
        WHERE s.id = $1
          AND ($2::uuid IS NULL OR a.id = $2)
          AND a.artifact_format = 'snapshot-hdf5:v1'
        ORDER BY a.created_at DESC
        LIMIT 1
        ",
    )
    .bind(snapshot_id)
    .bind(expected_artifact_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("certified_snapshot_artifact_not_found"))?;
    if row.try_get::<String, _>("snapshot_status")? != "ready" {
        return Err(anyhow::anyhow!("certified_snapshot_not_ready"));
    }
    let artifact_format = row.try_get::<String, _>("artifact_format")?;
    let artifact_sha256 = row.try_get::<String, _>("artifact_sha256")?;
    let snapshot_index_sha256 = row.try_get::<String, _>("snapshot_index_sha256")?;
    let snapshot_build_contract_hash = row.try_get::<String, _>("snapshot_build_contract_hash")?;
    if row.try_get::<String, _>("effective_scope_hash")? != binding.effective_scope_hash
        || row.try_get::<String, _>("data_snapshot_token")? != binding.data_snapshot_token
        || row.try_get::<String, _>("closure_bundle_hash")? != binding.closure_bundle_hash
    {
        return Err(anyhow::anyhow!("certified_snapshot_contract_mismatch"));
    }
    let expected_build_contract =
        scope_closure_snapshot_build_contract_hash(binding, snapshot_id, artifact_format.as_str());
    if snapshot_build_contract_hash != expected_build_contract {
        return Err(anyhow::anyhow!(
            "certified_snapshot_build_contract_mismatch"
        ));
    }
    let meta = SnapshotArtifactMeta {
        url: row.try_get("artifact_url")?,
        format: artifact_format.clone(),
        sha256: artifact_sha256.clone(),
    };
    let decoded = fetch_decoded_snapshot_artifact_from_meta(state, snapshot_id, &meta).await?;
    validate_certified_snapshot_contract(
        &decoded,
        binding.effective_scope_hash.as_str(),
        binding.data_snapshot_token.as_str(),
        binding.closure_bundle_hash.as_str(),
        expected_axis,
    )?;

    let snapshot_index_url = derive_snapshot_index_url(meta.url.as_str());
    let snapshot_index_bytes = state
        .object_store
        .download_object_url(snapshot_index_url.as_str())
        .await?;
    let observed_index_sha256 = hex::encode(Sha256::digest(snapshot_index_bytes.as_slice()));
    if observed_index_sha256 != snapshot_index_sha256 {
        return Err(anyhow::anyhow!("certified_snapshot_index_hash_mismatch"));
    }
    let snapshot_index: SnapshotIndexDocument = serde_json::from_slice(&snapshot_index_bytes)?;
    if snapshot_index.snapshot_id != snapshot_id {
        return Err(anyhow::anyhow!(
            "certified_snapshot_index_identity_mismatch"
        ));
    }
    validate_certified_snapshot_index(&snapshot_index, &decoded, expected_axis)?;

    Ok((
        ScopeClosureSnapshotFacts {
            snapshot_id,
            snapshot_hash: artifact_sha256,
            snapshot_artifact_id: row.try_get("snapshot_artifact_id")?,
            snapshot_index_sha256,
            snapshot_build_contract_hash,
            artifact_format,
        },
        decoded,
    ))
}

pub(crate) fn scope_closure_snapshot_build_contract_hash(
    binding: &ScopeClosureSnapshotBinding,
    snapshot_id: Uuid,
    artifact_format: &str,
) -> String {
    hex::encode(Sha256::digest(
        format!(
            "lcia.numerical-snapshot-build-contract.v1\n{}\n{}\n{}\n{}\n{}",
            binding.effective_scope_hash,
            binding.data_snapshot_token,
            binding.closure_bundle_hash,
            snapshot_id,
            artifact_format,
        )
        .as_bytes(),
    ))
}

pub(crate) fn scope_closure_evidence_hash(
    source_fingerprint: &str,
    resolution_map_hash: &str,
    closure_bundle_hash: &str,
    closure_bundle_artifact_id: Uuid,
    facts: &ScopeClosureSnapshotFacts,
) -> String {
    hex::encode(Sha256::digest(
        format!(
            "lcia.scope-closure-evidence.v2\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
            source_fingerprint,
            resolution_map_hash,
            closure_bundle_hash,
            closure_bundle_artifact_id,
            facts.snapshot_id,
            facts.snapshot_hash,
            facts.snapshot_artifact_id,
            facts.snapshot_index_sha256,
            facts.snapshot_build_contract_hash,
        )
        .as_bytes(),
    ))
}

fn validate_certified_snapshot_contract(
    decoded: &DecodedSnapshotArtifact,
    expected_effective_scope_hash: &str,
    expected_data_snapshot_token: &str,
    expected_closure_bundle_hash: &str,
    request_roots: &[RequestRootProcess],
) -> anyhow::Result<()> {
    let binding = decoded
        .config
        .scope_closure_binding
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("certified snapshot lacks scope closure binding"))?;
    if binding.schema_version != "lcia.scope-closure-snapshot-binding.v1"
        || binding.effective_scope_hash != expected_effective_scope_hash
        || binding.data_snapshot_token != expected_data_snapshot_token
        || binding.closure_bundle_hash != expected_closure_bundle_hash
    {
        return Err(anyhow::anyhow!("certified_snapshot_contract_mismatch"));
    }
    let graph = decoded.compiled_graph.as_ref().ok_or_else(|| {
        anyhow::anyhow!("certified snapshot lacks compiled graph and exact process axis evidence")
    })?;
    let process_axis = graph
        .processes
        .iter()
        .map(|process| {
            (
                process.process_idx,
                process.process_id,
                process.process_version.clone(),
            )
        })
        .collect::<Vec<_>>();
    validate_certified_process_axis(&process_axis, decoded.payload.process_count, request_roots)
}

fn validate_certified_process_axis(
    process_axis: &[(i32, Uuid, String)],
    payload_process_count: i32,
    request_roots: &[RequestRootProcess],
) -> anyhow::Result<()> {
    let requested_axis = request_roots
        .iter()
        .map(|root| (root.process_id, root.process_version.clone()))
        .collect::<Vec<_>>();
    let observed_axis = process_axis
        .iter()
        .map(|(_, id, version)| (*id, version.clone()))
        .collect::<Vec<_>>();
    let contiguous_indices = process_axis
        .iter()
        .enumerate()
        .all(|(index, (process_idx, _, _))| i32::try_from(index) == Ok(*process_idx));
    if observed_axis != requested_axis || !contiguous_indices {
        return Err(anyhow::anyhow!(
            "certified_snapshot_axis_mismatch: artifact evidence must preserve the exact ordered effective process axis"
        ));
    }
    if payload_process_count != i32::try_from(process_axis.len())? {
        return Err(anyhow::anyhow!(
            "certified_snapshot_axis_mismatch: payload process count differs from compiled graph"
        ));
    }
    Ok(())
}

fn validate_certified_snapshot_index(
    snapshot_index: &SnapshotIndexDocument,
    decoded: &DecodedSnapshotArtifact,
    expected_axis: &[RequestRootProcess],
) -> anyhow::Result<()> {
    let graph = decoded.compiled_graph.as_ref().ok_or_else(|| {
        anyhow::anyhow!("certified snapshot lacks compiled graph and exact process axis evidence")
    })?;
    if snapshot_index.process_count != decoded.payload.process_count
        || snapshot_index.impact_count != decoded.payload.impact_count
        || snapshot_index.process_map.len() != expected_axis.len()
        || snapshot_index.process_map.len() != graph.processes.len()
        || usize::try_from(snapshot_index.impact_count)? != snapshot_index.impact_map.len()
    {
        return Err(anyhow::anyhow!("certified_snapshot_index_axis_mismatch"));
    }

    let index_axis = snapshot_index
        .process_map
        .iter()
        .map(|entry| {
            (
                entry.process_index,
                entry.process_id,
                entry.process_version.clone(),
            )
        })
        .collect::<Vec<_>>();
    validate_certified_process_axis(&index_axis, snapshot_index.process_count, expected_axis)
        .map_err(|_| anyhow::anyhow!("certified_snapshot_index_axis_mismatch"))?;
    if !snapshot_index
        .impact_map
        .iter()
        .enumerate()
        .all(|(index, entry)| i32::try_from(index) == Ok(entry.impact_index))
    {
        return Err(anyhow::anyhow!("certified_snapshot_index_axis_mismatch"));
    }
    Ok(())
}

async fn run_lcia_result_package_snapshot_builder(
    state: &AppState,
    requested_snapshot_id: Uuid,
    request_roots: &[RequestRootProcess],
) -> anyhow::Result<(SnapshotBuilderExecution, Value)> {
    let process_states = crate::default_snapshot_process_states_arg();
    let lock_guard = acquire_build_snapshot_lock(
        &state.pool,
        state.build_snapshot_max_concurrency,
        state.build_snapshot_lock_poll_interval,
    )
    .await?;
    let build_snapshot_lock = lock_guard.diagnostics();
    let executed_result = run_snapshot_builder_job(
        requested_snapshot_id,
        Some(process_states.as_str()),
        None,
        Some(request_roots),
        Some("split_by_process_volume"),
        Some("lenient"),
        Some("lenient"),
        None,
        None,
        None,
        None,
        None,
        Some("lcia_result_package"),
        None,
        None,
        None,
        None,
        None,
        false,
    )
    .await;
    let release_result = lock_guard.release().await;

    match executed_result {
        Ok(executed) => {
            if let Err(err) = release_result {
                return Err(anyhow::anyhow!(
                    "failed to release build_snapshot advisory lock: {err}"
                ));
            }
            Ok((executed, build_snapshot_lock))
        }
        Err(err) => {
            if let Err(release_err) = release_result {
                warn!(
                    error = %release_err,
                    "failed to release build_snapshot advisory lock after lcia result package snapshot failure"
                );
            }
            Err(err)
        }
    }
}

async fn persist_lcia_result_package_all_unit_artifacts(
    state: &AppState,
    result_job_id: Uuid,
    snapshot_id: Uuid,
) -> anyhow::Result<LciaResultPackageArtifacts> {
    ensure_prepared(state, snapshot_id, 0.0).await?;
    let process_count = fetch_snapshot_process_count(&state.pool, snapshot_id).await?;
    let n = usize::try_from(process_count)
        .map_err(|_| anyhow::anyhow!("process count overflow: {process_count}"))?;
    if n == 0 {
        return Err(anyhow::anyhow!(
            "lcia_result package build requires non-zero process count"
        ));
    }

    let batch_size = normalize_all_unit_batch_size(None, n);
    let (solved, calculation_bundle) = solve_all_unit_with_calculation_bundle(
        state,
        result_job_id,
        snapshot_id,
        n,
        batch_size,
        0.0,
    )
    .await?;
    let query_artifact_meta =
        persist_solve_all_unit_query_artifact(state, result_job_id, snapshot_id, &solved).await?;
    let result_diag = persist_solve_batch_result(
        state,
        result_job_id,
        snapshot_id,
        &solved,
        "solve_all_unit",
        None,
        Some(calculation_bundle.clone()),
    )
    .await?;
    let result_id = latest_result_id_for_job(&state.pool, result_job_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("lcia_result package build did not persist an lca_results row")
        })?;
    let latest_all_unit_result_id = upsert_latest_all_unit_result(
        &state.pool,
        snapshot_id,
        result_job_id,
        result_id,
        &query_artifact_meta,
    )
    .await?;

    Ok(LciaResultPackageArtifacts {
        result_id,
        latest_all_unit_result_id,
        result_diag,
        query_artifact_meta,
        calculation_bundle,
    })
}

fn lcia_result_artifact_ref(result_diag: &Value) -> Value {
    serde_json::json!({
        "artifactUrl": result_diag.get("artifact_url").cloned().unwrap_or(Value::Null),
        "artifactSha256": result_diag.get("artifact_sha256").cloned().unwrap_or(Value::Null),
        "artifactByteSize": result_diag.get("artifact_bytes").cloned().unwrap_or(Value::Null),
        "artifactFormat": result_diag.get("artifact_format").cloned().unwrap_or(Value::Null),
    })
}

fn lcia_result_query_artifact_ref(query_artifact: &QueryArtifactMeta) -> Value {
    serde_json::json!({
        "artifactUrl": query_artifact.url.clone(),
        "artifactSha256": query_artifact.sha256.clone(),
        "artifactByteSize": query_artifact.byte_size,
        "artifactFormat": query_artifact.format.clone(),
    })
}

async fn persist_result_artifact(
    state: &AppState,
    job_id: Uuid,
    snapshot_id: Uuid,
    input: PersistArtifactInput,
) -> anyhow::Result<Value> {
    let PersistArtifactInput {
        suffix,
        encoded,
        compute_timing,
        encode_artifact_sec,
        calculation_evidence,
        calculation_bundle,
    } = input;
    let EncodedArtifact {
        format,
        extension,
        content_type,
        sha256,
        bytes,
    } = encoded;
    let encoded_len = bytes.len();
    let artifact_meta = ArtifactMeta {
        format: format.to_owned(),
        sha256,
        encoded_len,
        artifact_len: i64::try_from(encoded_len)
            .map_err(|_| anyhow::anyhow!("artifact size overflow: {encoded_len}"))?,
    };
    let upload_started = Instant::now();
    let artifact_url = state
        .object_store
        .upload_result(snapshot_id, job_id, suffix, extension, content_type, bytes)
        .await?;
    let timing = PersistTimingContext {
        compute_timing,
        encode_artifact_sec,
        upload_artifact_sec: upload_started.elapsed().as_secs_f64(),
        calculation_evidence,
        calculation_bundle,
    };
    persist_object_storage_result(
        state,
        job_id,
        snapshot_id,
        &artifact_meta,
        &timing,
        &artifact_url,
    )
    .await
}

async fn mark_lcia_result_package_ready(
    pool: &PgPool,
    input: LciaResultPackageReadyInput,
) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.cmd_lcia_result_package_mark_ready(
            $1,
            $2,
            $3,
            $4,
            $5,
            $6::jsonb,
            $7::jsonb,
            $8::jsonb,
            $9::jsonb,
            $10,
            $11,
            $12::jsonb
        ) AS result
        FROM _service_role
        ",
    )
    .bind(input.build_worker_job_id)
    .bind(input.package_version)
    .bind(input.snapshot_id)
    .bind(input.result_id)
    .bind(input.latest_all_unit_result_id)
    .bind(input.result_artifact_ref)
    .bind(input.query_artifact_ref)
    .bind(input.artifact_manifest)
    .bind(input.available_impact_categories)
    .bind(input.default_impact_category)
    .bind(input.package_result_hash)
    .bind(input.audit)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(result)
    } else {
        Err(anyhow::anyhow!(
            "cmd_lcia_result_package_mark_ready returned non-ok result: {result}"
        ))
    }
}

async fn link_lcia_result_package_worker_job_domain_refs(
    pool: &PgPool,
    worker_job_id: Uuid,
    build_id: Uuid,
) -> anyhow::Result<()> {
    execute_optional_lcia_result_package_worker_job_ref_update(
        pool,
        r"
        UPDATE public.lca_results
           SET worker_job_id = $1
         WHERE job_id = $2
        ",
        worker_job_id,
        build_id,
    )
    .await?;
    execute_optional_lcia_result_package_worker_job_ref_update(
        pool,
        r"
        UPDATE public.lca_latest_all_unit_results
           SET worker_job_id = $1
         WHERE job_id = $2
        ",
        worker_job_id,
        build_id,
    )
    .await?;

    Ok(())
}

async fn execute_optional_lcia_result_package_worker_job_ref_update(
    pool: &PgPool,
    statement: &str,
    worker_job_id: Uuid,
    build_id: Uuid,
) -> anyhow::Result<()> {
    let result = sqlx::query(statement)
        .bind(worker_job_id)
        .bind(build_id)
        .execute(pool)
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(err) if is_undefined_table(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

async fn run_snapshot_builder_job_with_worker_heartbeat<F>(
    pool: &PgPool,
    lease: &BuildSnapshotWorkerLease,
    build_snapshot_lock: Value,
    build_future: F,
) -> anyhow::Result<SnapshotBuilderExecution>
where
    F: Future<Output = anyhow::Result<SnapshotBuilderExecution>>,
{
    let mut build_future = Box::pin(build_future);
    let heartbeat_interval = build_snapshot_heartbeat_interval(lease.lease_seconds);
    let lease_seconds = lease.lease_seconds.clamp(1, 86_400);

    loop {
        tokio::select! {
            result = &mut build_future => return result,
            () = sleep(heartbeat_interval) => {
                crate::worker_jobs::heartbeat_worker_job(
                    pool,
                    lease.worker_job_id,
                    lease.lease_token,
                    "build_snapshot",
                    0.20,
                    Some(serde_json::json!({
                        "build_snapshot_lock": build_snapshot_lock.clone(),
                        "builder": {
                            "running": true,
                        },
                    })),
                    lease_seconds,
                )
                .await
                .map_err(|err| {
                    anyhow::anyhow!("build_snapshot worker_jobs lease heartbeat failed: {err}")
                })?;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn run_snapshot_builder_job(
    snapshot_id: Uuid,
    process_states: Option<&str>,
    include_user_id: Option<Uuid>,
    request_roots: Option<&[crate::graph_types::RequestRootProcess]>,
    provider_rule: Option<&str>,
    reference_normalization_mode: Option<&str>,
    allocation_fraction_mode: Option<&str>,
    process_limit: Option<i32>,
    self_loop_cutoff: Option<f64>,
    singular_eps: Option<f64>,
    method_id: Option<Uuid>,
    method_version: Option<&str>,
    artifact_purpose: Option<&str>,
    artifact_expires_in_seconds: Option<i64>,
    reuse_max_age_seconds: Option<i64>,
    review_submit_revision_checksum: Option<&str>,
    versioned_scope: Option<&VersionedSnapshotBuilderArgs>,
    scope_closure: Option<&ScopeClosureSnapshotBuilderArgs>,
    no_lcia: bool,
) -> anyhow::Result<SnapshotBuilderExecution> {
    let mut builder_args = vec![
        "--snapshot-id".to_owned(),
        snapshot_id.to_string(),
        "--process-states".to_owned(),
        process_states.map_or_else(
            crate::default_snapshot_process_states_arg,
            ToOwned::to_owned,
        ),
        "--provider-rule".to_owned(),
        provider_rule
            .unwrap_or("split_by_process_volume")
            .to_owned(),
        "--reference-normalization-mode".to_owned(),
        reference_normalization_mode.unwrap_or("lenient").to_owned(),
        "--allocation-fraction-mode".to_owned(),
        allocation_fraction_mode.unwrap_or("lenient").to_owned(),
    ];

    if let Some(user_id) = include_user_id {
        builder_args.push("--include-user-id".to_owned());
        builder_args.push(user_id.to_string());
    }
    if let Some(roots) = request_roots {
        for root in roots {
            builder_args.push("--root-process".to_owned());
            builder_args.push(root.to_string());
        }
    }
    if let Some(limit) = process_limit {
        builder_args.push("--process-limit".to_owned());
        builder_args.push(limit.max(0).to_string());
    }
    if let Some(cutoff) = self_loop_cutoff {
        builder_args.push("--self-loop-cutoff".to_owned());
        builder_args.push(cutoff.to_string());
    }
    if let Some(eps) = singular_eps {
        builder_args.push("--singular-eps".to_owned());
        builder_args.push(eps.to_string());
    }
    if let Some(mid) = method_id {
        builder_args.push("--method-id".to_owned());
        builder_args.push(mid.to_string());
    }
    if let Some(mver) = method_version.map(str::trim).filter(|s| !s.is_empty()) {
        builder_args.push("--method-version".to_owned());
        builder_args.push(mver.to_owned());
    }
    if no_lcia {
        builder_args.push("--no-lcia".to_owned());
    }
    if let Some(purpose) = artifact_purpose
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        builder_args.push("--artifact-purpose".to_owned());
        builder_args.push(purpose.to_owned());
    }
    if let Some(ttl_seconds) = artifact_expires_in_seconds.filter(|seconds| *seconds > 0) {
        builder_args.push("--artifact-expires-in-seconds".to_owned());
        builder_args.push(ttl_seconds.to_string());
    }
    if let Some(max_age_seconds) = reuse_max_age_seconds.filter(|seconds| *seconds > 0) {
        builder_args.push("--reuse-max-age-seconds".to_owned());
        builder_args.push(max_age_seconds.to_string());
    }
    if let Some(checksum) = review_submit_revision_checksum
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        builder_args.push("--review-submit-revision-checksum".to_owned());
        builder_args.push(checksum.to_owned());
    }
    if let Some(scope) = versioned_scope {
        builder_args.push("--all-states".to_owned());
        builder_args.push(scope.all_states.to_string());
        builder_args.push("--include-user-state-codes".to_owned());
        builder_args.push(scope.include_user_state_codes.clone());
        if scope.include_user_unassigned_only {
            builder_args.push("--include-user-unassigned-only".to_owned());
        }
        if scope.include_user_review_free_only {
            builder_args.push("--include-user-review-free-only".to_owned());
        }
        builder_args.push("--data-scope".to_owned());
        builder_args.push(scope.data_scope.clone());
        builder_args.push("--scope-manifest-json".to_owned());
        builder_args.push(serde_json::to_string(&scope.scope_manifest)?);
        builder_args.push("--scope-manifest-sha256".to_owned());
        builder_args.push(scope.scope_manifest_sha256.clone());
        builder_args.push("--lcia-method-factor-source-json".to_owned());
        builder_args.push(serde_json::to_string(&scope.lcia_method_factor_source)?);
        builder_args.push("--lcia-factor-coverage-contract-json".to_owned());
        builder_args.push(serde_json::to_string(&scope.lcia_factor_coverage_contract)?);
    }
    if let Some(scope) = scope_closure {
        builder_args.push("--scope-closure-mode".to_owned());
        builder_args.push(scope.mode.as_str().to_owned());
        builder_args.push("--scope-closure-binding-json".to_owned());
        builder_args.push(serde_json::to_string(&scope.binding)?);
        builder_args.push("--scope-closure-data-snapshot-json".to_owned());
        builder_args.push(serde_json::to_string(&scope.data_snapshot)?);
    }

    let candidates = snapshot_builder_candidates(builder_args);
    let mut last_not_found = false;
    for candidate in candidates {
        let cmd_vec = std::iter::once(candidate.program.clone())
            .chain(candidate.args.iter().cloned())
            .collect::<Vec<_>>();
        let program = candidate.program.clone();
        let args = candidate.args.clone();
        let current_dir = candidate.current_dir.clone();
        let output = match tokio::task::spawn_blocking(move || {
            let mut command = Command::new(&program);
            command.args(&args);
            if let Some(dir) = current_dir {
                command.current_dir(dir);
            }
            command.output()
        })
        .await
        .map_err(|err| anyhow::anyhow!("snapshot_builder join error: {err}"))?
        {
            Ok(output) => output,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                last_not_found = true;
                continue;
            }
            Err(err) => return Err(err.into()),
        };

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            return Err(anyhow::anyhow!(
                "snapshot_builder failed: code={} cmd={} stdout_tail={} stderr_tail={}",
                code,
                cmd_vec.join(" "),
                tail_text(&stdout, 2000),
                tail_text(&stderr, 2000),
            ));
        }

        let discovery = parse_scope_closure_discovery(&stdout)?;
        let resolved_snapshot_id = if discovery.is_some() {
            snapshot_id
        } else {
            parse_snapshot_builder_resolved_snapshot_id(&stdout).ok_or_else(|| {
                anyhow::anyhow!(
                    "snapshot_builder succeeded but did not report resolved snapshot id"
                )
            })?
        };

        return Ok(SnapshotBuilderExecution {
            requested_snapshot_id: snapshot_id,
            resolved_snapshot_id,
            build_timing_sec: parse_snapshot_builder_build_timing(&stdout),
            command: cmd_vec,
            exit_code: output.status.code().unwrap_or(0),
            stdout_tail: tail_text(&stdout, 4000),
            stderr_tail: tail_text(&stderr, 2000),
            scope_closure_discovery: discovery,
        });
    }

    if last_not_found {
        return Err(anyhow::anyhow!(
            "snapshot_builder command not found; set SNAPSHOT_BUILDER_BIN or install cargo"
        ));
    }
    Err(anyhow::anyhow!("failed to execute snapshot_builder"))
}

fn snapshot_builder_candidates(builder_args: Vec<String>) -> Vec<BuilderCommandCandidate> {
    let mut out = Vec::new();

    if let Ok(custom) = std::env::var("SNAPSHOT_BUILDER_BIN")
        && !custom.trim().is_empty()
    {
        out.push(BuilderCommandCandidate {
            program: custom,
            args: builder_args.clone(),
            current_dir: None,
        });
    }

    if let Ok(current_exe) = std::env::current_exe()
        && let Some(parent) = current_exe.parent()
    {
        let sibling = parent.join("snapshot_builder");
        out.push(BuilderCommandCandidate {
            program: sibling.to_string_lossy().to_string(),
            args: builder_args.clone(),
            current_dir: None,
        });
    }

    out.push(BuilderCommandCandidate {
        program: "snapshot_builder".to_owned(),
        args: builder_args.clone(),
        current_dir: None,
    });

    let root = std::env::var("LCA_WORKER_ROOT")
        .or_else(|_| std::env::var("LCA_CALCULATOR_ROOT"))
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok());

    let mut cargo_args = vec![
        "run".to_owned(),
        "-p".to_owned(),
        "solver-worker".to_owned(),
        "--bin".to_owned(),
        "snapshot_builder".to_owned(),
        "--release".to_owned(),
        "--".to_owned(),
    ];
    cargo_args.extend(builder_args);
    out.push(BuilderCommandCandidate {
        program: "cargo".to_owned(),
        args: cargo_args,
        current_dir: root,
    });

    out
}

fn tail_text(input: &str, max_len: usize) -> String {
    if input.len() <= max_len {
        return input.to_owned();
    }
    input[input.len() - max_len..].to_owned()
}

fn parse_snapshot_builder_resolved_snapshot_id(stdout: &str) -> Option<Uuid> {
    stdout
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix("[resolved_snapshot_id] "))
        .and_then(|value| Uuid::parse_str(value.trim()).ok())
}

fn parse_scope_closure_discovery(stdout: &str) -> anyhow::Result<Option<Value>> {
    stdout
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix("[scope_closure_discovery] "))
        .map(|value| {
            serde_json::from_str(value.trim()).map_err(|error| {
                anyhow::anyhow!("snapshot_builder emitted invalid scope closure discovery: {error}")
            })
        })
        .transpose()
}

fn parse_snapshot_builder_build_timing(stdout: &str) -> Option<Value> {
    stdout
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix("[build_timing_sec] "))
        .and_then(|value| serde_json::from_str(value.trim()).ok())
}

async fn fetch_snapshot_source_hash(
    pool: &PgPool,
    snapshot_id: Uuid,
) -> anyhow::Result<Option<String>> {
    let row = sqlx::query("SELECT source_hash FROM public.lca_network_snapshots WHERE id = $1")
        .bind(snapshot_id)
        .fetch_optional(pool)
        .await?;
    match row {
        Some(row) => Ok(row.try_get::<Option<String>, _>("source_hash")?),
        None => Ok(None),
    }
}

async fn set_job_snapshot_id(pool: &PgPool, job_id: Uuid, snapshot_id: Uuid) -> anyhow::Result<()> {
    let result = sqlx::query(
        r"
        UPDATE lca_jobs
        SET snapshot_id = $2,
            updated_at = NOW()
        WHERE id = $1
        ",
    )
    .bind(job_id)
    .bind(snapshot_id)
    .execute(pool)
    .await;
    match result {
        Ok(_) => Ok(()),
        Err(err) if is_undefined_table(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

async fn upsert_active_snapshot(
    pool: &PgPool,
    scope: &str,
    snapshot_id: Uuid,
    source_hash: Option<&str>,
    activated_by: Option<Uuid>,
    job_id: Uuid,
) -> anyhow::Result<()> {
    sqlx::query(
        r"
        INSERT INTO public.lca_active_snapshots (
            scope,
            snapshot_id,
            source_hash,
            activated_at,
            activated_by,
            note
        )
        VALUES ($1, $2, $3, NOW(), $4, $5)
        ON CONFLICT (scope)
        DO UPDATE SET
            snapshot_id = EXCLUDED.snapshot_id,
            source_hash = EXCLUDED.source_hash,
            activated_at = EXCLUDED.activated_at,
            activated_by = EXCLUDED.activated_by,
            note = EXCLUDED.note
        ",
    )
    .bind(scope)
    .bind(snapshot_id)
    .bind(source_hash)
    .bind(activated_by)
    .bind(format!("auto build_snapshot job {job_id}"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn persist_object_storage_result(
    state: &AppState,
    job_id: Uuid,
    snapshot_id: Uuid,
    artifact_meta: &ArtifactMeta,
    timing: &PersistTimingContext,
    artifact_url: &str,
) -> anyhow::Result<Value> {
    let diagnostics_without_db_write = serde_json::json!({
        "storage": "object_storage",
        "persist_mode": "s3-strict",
        "artifact_format": artifact_meta.format,
        "artifact_sha256": artifact_meta.sha256,
        "artifact_bytes": artifact_meta.encoded_len,
        "artifact_url": artifact_url,
        "calculation_evidence": timing.calculation_evidence.clone(),
        "calculation_bundle": timing.calculation_bundle.clone(),
        "compute_timing_sec": timing.compute_timing,
        "persistence_timing_sec": persistence_timing_json(
            Some(timing.encode_artifact_sec),
            Some(timing.upload_artifact_sec),
            None,
        ),
    });

    let db_write_started = Instant::now();
    let result_id = insert_result(
        &state.pool,
        job_id,
        snapshot_id,
        ResultInsert {
            diagnostics: diagnostics_without_db_write.clone(),
            artifact_url: artifact_url.to_owned(),
            artifact_sha256: artifact_meta.sha256.clone(),
            artifact_byte_size: artifact_meta.artifact_len,
            artifact_format: artifact_meta.format.clone(),
        },
    )
    .await?;
    let db_write_sec = db_write_started.elapsed().as_secs_f64();

    let diagnostics = serde_json::json!({
        "storage": "object_storage",
        "persist_mode": "s3-strict",
        "artifact_format": artifact_meta.format,
        "artifact_sha256": artifact_meta.sha256,
        "artifact_bytes": artifact_meta.encoded_len,
        "artifact_url": artifact_url,
        "calculation_evidence": timing.calculation_evidence.clone(),
        "calculation_bundle": timing.calculation_bundle.clone(),
        "compute_timing_sec": timing.compute_timing,
        "persistence_timing_sec": persistence_timing_json(
            Some(timing.encode_artifact_sec),
            Some(timing.upload_artifact_sec),
            Some(db_write_sec),
        ),
    });
    if diagnostics != diagnostics_without_db_write {
        update_result_diagnostics(&state.pool, result_id, diagnostics.clone()).await?;
    }

    Ok(diagnostics)
}

fn persistence_timing_json(
    encode_artifact_sec: Option<f64>,
    upload_artifact_sec: Option<f64>,
    db_write_sec: Option<f64>,
) -> Value {
    let encode = encode_artifact_sec.unwrap_or(0.0);
    let db_write = db_write_sec.unwrap_or(0.0);
    serde_json::json!({
        "encode_artifact_sec": encode_artifact_sec,
        "upload_artifact_sec": upload_artifact_sec,
        "db_write_sec": db_write_sec,
        "total_sec": encode + upload_artifact_sec.unwrap_or(0.0) + db_write,
    })
}

fn to_core_solve_options(solve: SolveOptionsPayload) -> SolveOptions {
    SolveOptions {
        return_x: solve.return_x,
        return_g: solve.return_g,
        return_h: solve.return_h,
    }
}

fn resolve_solve_all_unit_options(
    solve: Option<SolveOptionsPayload>,
) -> anyhow::Result<SolveOptions> {
    let solve = solve.unwrap_or(SolveOptionsPayload {
        return_x: false,
        return_g: false,
        return_h: true,
    });
    if solve.return_x || solve.return_g || !solve.return_h {
        return Err(anyhow::anyhow!(
            "solve_all_unit supports only solve={{return_x:false, return_g:false, return_h:true}}"
        ));
    }
    Ok(to_core_solve_options(solve))
}

fn normalize_all_unit_batch_size(requested: Option<usize>, process_count: usize) -> usize {
    if process_count == 0 {
        return 1;
    }
    let requested = requested.unwrap_or(DEFAULT_ALL_UNIT_BATCH_SIZE);
    requested.clamp(1, process_count.min(MAX_ALL_UNIT_BATCH_SIZE))
}

fn build_all_unit_rhs_batch(process_count: usize, start: usize, end: usize) -> Vec<Vec<f64>> {
    let mut rhs_batch = Vec::with_capacity(end.saturating_sub(start));
    for idx in start..end {
        let mut rhs = vec![0.0; process_count];
        rhs[idx] = 1.0;
        rhs_batch.push(rhs);
    }
    rhs_batch
}

fn build_single_rhs(process_count: usize, process_index: usize, amount: f64) -> Vec<f64> {
    let mut rhs = vec![0.0; process_count];
    rhs[process_index] = amount;
    rhs
}

fn lcia_result_package_request_roots(
    input_manifest: &Value,
    require_published_state: bool,
) -> anyhow::Result<Vec<RequestRootProcess>> {
    let processes = input_manifest
        .get("processes")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            anyhow::anyhow!("LCIA result package input_manifest.processes must be an array")
        })?;
    if processes.is_empty() {
        return Err(anyhow::anyhow!(
            "LCIA result package input_manifest.processes must not be empty"
        ));
    }

    let mut roots = Vec::with_capacity(processes.len());
    for (idx, process) in processes.iter().enumerate() {
        let process_id = process
            .get("id")
            .or_else(|| process.get("processId"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("LCIA result package process[{idx}] is missing id"))?
            .parse::<Uuid>()?;
        let process_version = process
            .get("version")
            .or_else(|| process.get("processVersion"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|version| !version.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("LCIA result package process[{idx}] is missing version")
            })?;
        let state_code = process
            .get("stateCode")
            .or_else(|| process.get("state_code"))
            .and_then(Value::as_i64);
        if require_published_state {
            let state_code = state_code.ok_or_else(|| {
                anyhow::anyhow!("LCIA result package process[{idx}] is missing stateCode")
            })?;
            if !(100..=199).contains(&state_code) {
                return Err(anyhow::anyhow!(
                    "LCIA result package process[{idx}] must use a published process state, got {state_code}"
                ));
            }
        }

        roots.push(RequestRootProcess::new(process_id, process_version));
    }

    Ok(roots)
}

fn lcia_result_package_version(build_id: Uuid) -> String {
    format!("lcia-result-{build_id}")
}

async fn fetch_snapshot_process_count(pool: &PgPool, snapshot_id: Uuid) -> anyhow::Result<i32> {
    let row = sqlx::query(
        r"
        SELECT process_count
        FROM lca_snapshot_artifacts
        WHERE snapshot_id = $1
          AND status = 'ready'
        ORDER BY created_at DESC
        LIMIT 1
        ",
    )
    .bind(snapshot_id)
    .fetch_optional(pool)
    .await;

    match row {
        Ok(Some(row)) => Ok(row.try_get::<i32, _>("process_count")?),
        Ok(None) => fetch_process_count(pool, snapshot_id).await,
        Err(err) if is_undefined_table(&err) => fetch_process_count(pool, snapshot_id).await,
        Err(err) => Err(err.into()),
    }
}

async fn fetch_process_count(pool: &PgPool, snapshot_id: Uuid) -> anyhow::Result<i32> {
    let count: i64 = sqlx::query_scalar(
        r"
        SELECT COUNT(*)::bigint
        FROM lca_process_index
        WHERE snapshot_id = $1
        ",
    )
    .bind(snapshot_id)
    .fetch_one(pool)
    .await?;

    i32::try_from(count).map_err(|_| anyhow::anyhow!("process count overflow: {count}"))
}

async fn fetch_flow_count(pool: &PgPool, snapshot_id: Uuid) -> anyhow::Result<i32> {
    let count: i64 = sqlx::query_scalar(
        r"
        SELECT COUNT(*)::bigint
        FROM lca_flow_index
        WHERE snapshot_id = $1
        ",
    )
    .bind(snapshot_id)
    .fetch_one(pool)
    .await?;

    i32::try_from(count).map_err(|_| anyhow::anyhow!("flow count overflow: {count}"))
}

async fn fetch_impact_count(pool: &PgPool, snapshot_id: Uuid) -> anyhow::Result<i32> {
    let count: i64 = sqlx::query_scalar(
        r#"
        SELECT COALESCE(MAX("row"), -1)::bigint + 1
        FROM lca_characterization_factors
        WHERE snapshot_id = $1
        "#,
    )
    .bind(snapshot_id)
    .fetch_one(pool)
    .await?;

    i32::try_from(count).map_err(|_| anyhow::anyhow!("impact count overflow: {count}"))
}

async fn fetch_triplets(
    pool: &PgPool,
    snapshot_id: Uuid,
    sql: &str,
) -> anyhow::Result<Vec<SparseTriplet>> {
    let rows = sqlx::query(sql).bind(snapshot_id).fetch_all(pool).await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(SparseTriplet {
            row: row.try_get::<i32, _>("row_idx")?,
            col: row.try_get::<i32, _>("col_idx")?,
            value: row.try_get::<f64, _>("value")?,
        });
    }

    Ok(out)
}

#[allow(dead_code)]
fn _assert_result_types(_a: SolveResult, _b: SolveBatchResult) {}

#[cfg(test)]
mod tests {
    use super::{
        PackageSnapshotExecutionMode, SolveOptionsPayload,
        acquire_build_snapshot_worker_jobs_slot_sql, build_all_unit_rhs_batch,
        build_snapshot_heartbeat_interval, lcia_result_package_request_roots,
        lcia_result_package_version, missing_legacy_tables_sparse_data_error,
        normalize_all_unit_batch_size, package_snapshot_execution_mode,
        parse_snapshot_builder_build_timing, parse_snapshot_builder_resolved_snapshot_id,
        resolve_solve_all_unit_options, validate_certified_process_axis,
    };
    use serde_json::json;
    use std::time::Duration;
    use uuid::Uuid;

    use crate::graph_types::RequestRootProcess;

    #[test]
    fn build_snapshot_worker_jobs_slot_sql_uses_short_transaction_and_lease_fencing() {
        let sql = acquire_build_snapshot_worker_jobs_slot_sql();

        assert!(sql.contains("pg_advisory_xact_lock"));
        assert!(
            !sql.contains("pg_try_advisory_xact_lock"),
            "worker_jobs build_snapshot gating must not hold a transaction advisory lock for the build duration"
        );
        assert!(sql.contains("lease_token is not distinct from $2"));
        assert!(sql.contains("lease_expires_at >= NOW()"));
        assert!(sql.contains("phase = 'build_snapshot'"));
    }

    #[test]
    fn build_snapshot_heartbeat_interval_refreshes_before_lease_expiry() {
        assert_eq!(
            build_snapshot_heartbeat_interval(900),
            Duration::from_mins(1)
        );
        assert_eq!(
            build_snapshot_heartbeat_interval(30),
            Duration::from_secs(10)
        );
        assert_eq!(build_snapshot_heartbeat_interval(1), Duration::from_secs(1));
    }

    #[test]
    fn certified_snapshot_axis_must_exactly_match_effective_package_axis() {
        let root = RequestRootProcess::new(Uuid::new_v4(), "01.00.000");
        let provider = RequestRootProcess::new(Uuid::new_v4(), "02.00.000");
        let exact = vec![(0, root.process_id, root.process_version.clone())];
        validate_certified_process_axis(&exact, 1, std::slice::from_ref(&root)).unwrap();

        let expanded = vec![
            (0, root.process_id, root.process_version.clone()),
            (1, provider.process_id, provider.process_version.clone()),
        ];
        assert!(
            validate_certified_process_axis(&expanded, 2, std::slice::from_ref(&root)).is_err()
        );
        assert!(validate_certified_process_axis(&exact, 2, std::slice::from_ref(&root)).is_err());

        let expected = vec![root.clone(), provider.clone()];
        let reordered = vec![
            (0, provider.process_id, provider.process_version.clone()),
            (1, root.process_id, root.process_version.clone()),
        ];
        assert!(validate_certified_process_axis(&reordered, 2, &expected).is_err());

        let non_contiguous = vec![
            (1, root.process_id, root.process_version.clone()),
            (0, provider.process_id, provider.process_version.clone()),
        ];
        assert!(validate_certified_process_axis(&non_contiguous, 2, &expected).is_err());
    }

    #[test]
    fn certified_package_execution_plan_never_invokes_live_snapshot_builder() {
        assert_eq!(
            package_snapshot_execution_mode(Some(Uuid::new_v4())),
            PackageSnapshotExecutionMode::CertifiedReuse
        );
        assert_eq!(
            package_snapshot_execution_mode(None),
            PackageSnapshotExecutionMode::LegacyLiveBuild
        );
    }

    #[test]
    fn solve_all_unit_options_default_to_h_only() {
        let options = resolve_solve_all_unit_options(None).expect("resolve options");
        assert!(!options.return_x);
        assert!(!options.return_g);
        assert!(options.return_h);
    }

    #[test]
    fn solve_all_unit_options_reject_large_payload_modes() {
        let err = resolve_solve_all_unit_options(Some(SolveOptionsPayload {
            return_x: true,
            return_g: false,
            return_h: true,
        }))
        .expect_err("expected invalid solve options");
        assert!(err.to_string().contains("solve_all_unit supports only"));
    }

    #[test]
    fn normalize_batch_size_clamps_to_safe_range() {
        assert_eq!(normalize_all_unit_batch_size(None, 500), 128);
        assert_eq!(normalize_all_unit_batch_size(Some(0), 500), 1);
        assert_eq!(normalize_all_unit_batch_size(Some(9_999), 500), 500);
    }

    #[test]
    fn build_rhs_batch_generates_unit_vectors() {
        let batch = build_all_unit_rhs_batch(5, 1, 4);
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0], vec![0.0, 1.0, 0.0, 0.0, 0.0]);
        assert_eq!(batch[1], vec![0.0, 0.0, 1.0, 0.0, 0.0]);
        assert_eq!(batch[2], vec![0.0, 0.0, 0.0, 1.0, 0.0]);
    }

    #[test]
    fn parses_resolved_snapshot_id_from_builder_stdout() {
        let expected = Uuid::new_v4();
        let stdout = format!(
            "[reuse] matched existing ready snapshot={expected}\n[resolved_snapshot_id] {expected}\n[done] snapshot ready: {expected}\n"
        );

        assert_eq!(
            parse_snapshot_builder_resolved_snapshot_id(&stdout),
            Some(expected)
        );
    }

    #[test]
    fn missing_resolved_snapshot_id_returns_none() {
        assert_eq!(
            parse_snapshot_builder_resolved_snapshot_id("[done] snapshot ready: not-used\n"),
            None
        );
    }

    #[test]
    fn parses_build_timing_from_builder_stdout() {
        let stdout = r#"[build_timing_sec] {"total_sec":1.25,"reused_snapshot":false}
[resolved_snapshot_id] 9b19e81d-e81b-4c11-8585-7adcff2fcb95
"#;

        assert_eq!(
            parse_snapshot_builder_build_timing(stdout),
            Some(json!({"total_sec": 1.25, "reused_snapshot": false}))
        );
    }

    #[test]
    fn missing_legacy_tables_error_preserves_artifact_failure_context() {
        let snapshot_id =
            Uuid::parse_str("3d620e54-2b83-47f6-9809-0b65ab00bfd9").expect("valid uuid");
        let artifact_error = anyhow::anyhow!(
            "decode snapshot artifact failed: No space left on device (os error 28)"
        );

        let err = missing_legacy_tables_sparse_data_error(snapshot_id, Some(&artifact_error));
        let message = err.to_string();

        assert!(message.contains("no readable artifact"));
        assert!(message.contains("legacy lca_* matrix tables are missing"));
        assert!(message.contains("No space left on device"));
    }

    #[test]
    fn lcia_result_package_manifest_roots_use_published_processes() {
        let process_id = Uuid::new_v4();
        let manifest = json!({
            "predicateVersion": "published-state-code-100-199:v1",
            "processes": [
                {
                    "id": process_id,
                    "version": "01.00.000",
                    "stateCode": 150
                }
            ]
        });

        let roots = lcia_result_package_request_roots(&manifest, true).expect("roots");

        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].process_id, process_id);
        assert_eq!(roots[0].process_version, "01.00.000");
    }

    #[test]
    fn lcia_result_package_manifest_rejects_draft_inputs() {
        let manifest = json!({
            "processes": [
                {
                    "id": Uuid::new_v4(),
                    "version": "01.00.000",
                    "stateCode": 0
                }
            ]
        });

        let err = lcia_result_package_request_roots(&manifest, true).expect_err("draft rejected");

        assert!(err.to_string().contains("published process state"));
    }

    #[test]
    fn certified_package_manifest_uses_exact_axis_without_live_state() {
        let process_id = Uuid::new_v4();
        let manifest = json!({
            "predicateVersion": "published-state-code-100-199:v1",
            "selectionMode": "closure_certificate",
            "processes": [{"id": process_id, "version": "01.00.000"}]
        });

        let roots = lcia_result_package_request_roots(&manifest, false)
            .expect("certificate-owned exact process axis");
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].process_id, process_id);
        assert_eq!(roots[0].process_version, "01.00.000");

        let error = lcia_result_package_request_roots(&manifest, true)
            .expect_err("legacy live build still requires a published state");
        assert!(error.to_string().contains("missing stateCode"));

        let mut forged_state = manifest;
        forged_state["processes"][0]["stateCode"] = json!(0);
        let forged_roots = lcia_result_package_request_roots(&forged_state, false)
            .expect("certified path ignores client-style state metadata");
        assert_eq!(forged_roots, roots);
    }

    #[test]
    fn legacy_package_manifest_rejects_non_public_inputs() {
        let manifest = json!({
            "processes": [{
                "id": Uuid::new_v4(),
                "version": "01.00.000",
                "stateCode": 200
            }]
        });

        let error = lcia_result_package_request_roots(&manifest, true)
            .expect_err("non-public legacy input rejected");
        assert!(error.to_string().contains("published process state"));
    }

    #[test]
    fn lcia_result_package_version_is_stable_and_namespaced() {
        let build_id = Uuid::parse_str("3d620e54-2b83-47f6-9809-0b65ab00bfd9").expect("valid uuid");

        assert_eq!(
            lcia_result_package_version(build_id),
            "lcia-result-3d620e54-2b83-47f6-9809-0b65ab00bfd9"
        );
    }
}
