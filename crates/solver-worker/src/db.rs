use std::{
    io::ErrorKind,
    path::PathBuf,
    process::Command,
    time::{Duration, Instant},
};

use crate::pgbouncer_sqlx::{self as sqlx, PgPool, Postgres, Row, Transaction};
use serde_json::{Map, Value};
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
    config::AppConfig,
    contribution_path::{ContributionPathArtifact, analyze_contribution_path},
    db_pool::{APP_SOLVER_WORKER, APP_SOLVER_WORKER_QUEUE, WorkerDbPoolOptions},
    graph_types::RequestRootProcess,
    snapshot_artifacts::{DecodedSnapshotArtifact, decode_snapshot_artifact},
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
    key: i64,
    slot: u32,
    wait_sec: f64,
    max_concurrency: u32,
    acquired_at: Instant,
}

impl BuildSnapshotLockGuard {
    fn diagnostics(&self) -> Value {
        serde_json::json!({
            "enabled": true,
            "strategy": "postgres_transaction_advisory_lock",
            "max_concurrency": self.max_concurrency,
            "slot": self.slot,
            "wait_sec": self.wait_sec,
            "hold_sec": self.acquired_at.elapsed().as_secs_f64(),
        })
    }

    async fn release(mut self) -> anyhow::Result<()> {
        let Some(tx) = self.tx.take() else {
            return Ok(());
        };

        let hold_sec = self.acquired_at.elapsed().as_secs_f64();
        tx.commit().await?;
        info!(
            lock_key = self.key,
            slot = self.slot,
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
                lock_key = self.key,
                slot = self.slot,
                max_concurrency = self.max_concurrency,
                wait_sec = self.wait_sec,
                hold_sec,
                release_path = "drop",
                "build_snapshot transaction advisory lock guard dropped before explicit release"
            );
        }
    }
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
                    key,
                    slot,
                    wait_sec,
                    max_concurrency,
                    acquired_at: Instant::now(),
                });
            }
            tx.rollback().await?;
        }

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
    artifact_url: String,
    artifact_format: String,
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
                    artifact_format = %meta.artifact_format,
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
        SELECT artifact_url, artifact_format
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
            artifact_url: r.try_get::<String, _>("artifact_url")?,
            artifact_format: r.try_get::<String, _>("artifact_format")?,
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
    let bytes = state
        .object_store
        .download_object_url(&meta.artifact_url)
        .await?;

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
    let snapshot_index_url = derive_snapshot_index_url(&meta.artifact_url);
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

fn is_undefined_table(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().as_deref() == Some("42P01"),
        _ => false,
    }
}

/// Executes one queue payload end-to-end.
#[instrument(skip(state))]
#[allow(clippy::too_many_lines)]
pub async fn handle_job_payload(state: &AppState, payload: JobPayload) -> anyhow::Result<()> {
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
            ensure_prepared(state, snapshot_id, level).await?;
            let timed = state.solver.solve_one_timed(
                snapshot_id,
                NumericOptions { print_level: level },
                &rhs,
                to_core_solve_options(solve),
            )?;
            let solved = timed.result;

            let result_diag =
                persist_solve_one_result(state, job_id, snapshot_id, &solved, &timed.timing)
                    .await?;
            let completed_diag = merge_job_status_update_timing(
                serde_json::json!({"result": "stored", "storage": result_diag}),
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
            ensure_prepared(state, snapshot_id, level).await?;
            let solved = state.solver.solve_batch(
                snapshot_id,
                NumericOptions { print_level: level },
                &rhs_batch,
                to_core_solve_options(solve),
            )?;

            let result_diag =
                persist_solve_batch_result(state, job_id, snapshot_id, &solved, "solve_batch")
                    .await?;
            let completed_diag = merge_job_status_update_timing(
                serde_json::json!({"result": "stored", "storage": result_diag}),
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
            let solve_options = resolve_solve_all_unit_options(solve)?;

            let mut items = Vec::with_capacity(n);
            for start in (0..n).step_by(batch_size) {
                let end = (start + batch_size).min(n);
                let rhs_batch = build_all_unit_rhs_batch(n, start, end);
                let partial = state.solver.solve_batch(
                    snapshot_id,
                    NumericOptions { print_level: level },
                    rhs_batch.as_slice(),
                    solve_options,
                )?;
                items.extend(partial.items);
            }

            let solved = SolveBatchResult { items };
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
            let result_diag =
                persist_solve_batch_result(state, job_id, snapshot_id, &solved, "solve_all_unit")
                    .await?;
            let completed_diag = merge_job_status_update_timing(
                serde_json::json!({
                    "result": "stored",
                    "storage": result_diag,
                    "solve_all_unit": {
                        "process_count": n,
                        "unit_batch_size": batch_size,
                    }
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

            let result_diag =
                persist_contribution_path_result(state, job_id, snapshot_id, &analysis).await?;
            let completed_diag = merge_job_status_update_timing(
                serde_json::json!({
                    "result": "stored",
                    "storage": result_diag,
                    "contribution_path": {
                        "process_id": process_id,
                        "impact_id": impact_id,
                        "amount": amount,
                        "summary": analysis.summary,
                    }
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
            process_states,
            include_user_id,
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
            let running_db_write_sec = update_job_status(
                &state.pool,
                job_id,
                "running",
                serde_json::json!({
                    "phase": "build_snapshot",
                    "snapshot_id": snapshot_id,
                    "build_snapshot_lock": {
                        "enabled": true,
                        "strategy": "postgres_transaction_advisory_lock",
                        "max_concurrency": state.build_snapshot_max_concurrency,
                        "waiting": true,
                    },
                }),
            )
            .await?;

            let lock_guard = acquire_build_snapshot_lock(
                &state.pool,
                state.build_snapshot_max_concurrency,
                state.build_snapshot_lock_poll_interval,
            )
            .await?;
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

            let executed_result = run_snapshot_builder_job(
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
                no_lcia.unwrap_or(false),
            )
            .await;
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

            let resolved_snapshot_id = executed.resolved_snapshot_id;
            let build_timing_sec = executed.build_timing_sec.clone();
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
        },
    )
    .await
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
}

#[derive(Debug, Clone)]
struct BuilderCommandCandidate {
    program: String,
    args: Vec<String>,
    current_dir: Option<PathBuf>,
}

#[derive(Clone)]
struct PersistTimingContext {
    compute_timing: Option<Value>,
    encode_artifact_sec: f64,
    upload_artifact_sec: f64,
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
        ..
    } = payload
    else {
        return Err(anyhow::anyhow!(
            "expected lcia_result_package_build payload"
        ));
    };

    let requested_snapshot_id = *build_id;
    let result_job_id = *build_id;
    let request_roots = lcia_result_package_request_roots(input_manifest)?;
    let (executed, build_snapshot_lock) = run_lcia_result_package_snapshot_builder(
        state,
        requested_snapshot_id,
        request_roots.as_slice(),
    )
    .await?;
    let snapshot_id = executed.resolved_snapshot_id;
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
        "snapshotBuilder": executed,
        "buildSnapshotLock": build_snapshot_lock,
        "resultDiagnostics": artifacts.result_diag.clone(),
        "queryArtifact": {
            "artifactUrl": artifacts.query_artifact_meta.url.clone(),
            "artifactSha256": artifacts.query_artifact_meta.sha256.clone(),
            "artifactByteSize": artifacts.query_artifact_meta.byte_size,
            "artifactFormat": artifacts.query_artifact_meta.format.clone(),
        }
    });

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
                "resultId": artifacts.result_id,
                "latestAllUnitResultId": artifacts.latest_all_unit_result_id,
            }),
        },
    )
    .await?;

    Ok(mark_ready)
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

    let mut items = Vec::with_capacity(n);
    let batch_size = normalize_all_unit_batch_size(None, n);
    let solve_options = resolve_solve_all_unit_options(None)?;
    for start in (0..n).step_by(batch_size) {
        let end = (start + batch_size).min(n);
        let rhs_batch = build_all_unit_rhs_batch(n, start, end);
        let partial = state.solver.solve_batch(
            snapshot_id,
            NumericOptions { print_level: 0.0 },
            rhs_batch.as_slice(),
            solve_options,
        )?;
        items.extend(partial.items);
    }

    let solved = SolveBatchResult { items };
    let query_artifact_meta =
        persist_solve_all_unit_query_artifact(state, result_job_id, snapshot_id, &solved).await?;
    let result_diag =
        persist_solve_batch_result(state, result_job_id, snapshot_id, &solved, "solve_all_unit")
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

        let resolved_snapshot_id = parse_snapshot_builder_resolved_snapshot_id(&stdout)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "snapshot_builder succeeded but did not report resolved snapshot id"
                )
            })?;

        return Ok(SnapshotBuilderExecution {
            requested_snapshot_id: snapshot_id,
            resolved_snapshot_id,
            build_timing_sec: parse_snapshot_builder_build_timing(&stdout),
            command: cmd_vec,
            exit_code: output.status.code().unwrap_or(0),
            stdout_tail: tail_text(&stdout, 4000),
            stderr_tail: tail_text(&stderr, 2000),
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
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                anyhow::anyhow!("LCIA result package process[{idx}] is missing stateCode")
            })?;
        if !(100..=199).contains(&state_code) {
            return Err(anyhow::anyhow!(
                "LCIA result package process[{idx}] must use a published process state, got {state_code}"
            ));
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
        SolveOptionsPayload, build_all_unit_rhs_batch, lcia_result_package_request_roots,
        lcia_result_package_version, missing_legacy_tables_sparse_data_error,
        normalize_all_unit_batch_size, parse_snapshot_builder_build_timing,
        parse_snapshot_builder_resolved_snapshot_id, resolve_solve_all_unit_options,
    };
    use serde_json::json;
    use uuid::Uuid;

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

        let roots = lcia_result_package_request_roots(&manifest).expect("roots");

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

        let err = lcia_result_package_request_roots(&manifest).expect_err("draft rejected");

        assert!(err.to_string().contains("published process state"));
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
