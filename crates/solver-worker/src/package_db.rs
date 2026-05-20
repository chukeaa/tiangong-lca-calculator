use std::time::Instant;

use crate::pgbouncer_sqlx::{self as sqlx, PgPool, Row};
use serde_json::{Map, Value};
use tracing::{instrument, warn};
use uuid::Uuid;

use crate::{
    artifacts::EncodedArtifact,
    db::AppState,
    package_artifacts::{PackageArtifactUploadMeta, package_artifact_meta_from_encoded},
    package_execution::{
        clear_runtime_export_traversal_cache, execute_export_package, execute_import_package,
    },
    package_types::{PACKAGE_QUEUE_NAME, PackageArtifactKind, PackageJobPayload},
};

/// Insert contract for one package artifact row.
#[derive(Debug, Clone)]
pub struct PackageArtifactInsert {
    /// Owning package job id.
    pub job_id: Uuid,
    /// Stored artifact role.
    pub artifact_kind: PackageArtifactKind,
    /// Object storage URL.
    pub artifact_url: String,
    /// Artifact checksum in hex.
    pub artifact_sha256: String,
    /// Artifact byte size.
    pub artifact_byte_size: u64,
    /// Artifact format identifier.
    pub artifact_format: &'static str,
    /// Artifact content type.
    pub content_type: &'static str,
    /// Additional JSON metadata for status APIs.
    pub metadata: Value,
    /// Row status.
    pub status: &'static str,
}

impl PackageArtifactInsert {
    /// Creates a ready artifact row from one prepared upload metadata payload.
    #[must_use]
    pub fn ready(
        job_id: Uuid,
        artifact_kind: PackageArtifactKind,
        artifact_url: String,
        meta: PackageArtifactUploadMeta,
        metadata: Value,
    ) -> Self {
        Self {
            job_id,
            artifact_kind,
            artifact_url,
            artifact_sha256: meta.sha256,
            artifact_byte_size: meta.byte_size,
            artifact_format: meta.format,
            content_type: meta.content_type,
            metadata,
            status: "ready",
        }
    }

    /// Creates a ready artifact row from one in-memory encoded artifact payload.
    pub fn ready_from_encoded(
        job_id: Uuid,
        artifact_kind: PackageArtifactKind,
        artifact_url: String,
        encoded: &EncodedArtifact,
        metadata: Value,
    ) -> anyhow::Result<Self> {
        let meta = package_artifact_meta_from_encoded(encoded)?;
        Ok(Self::ready(
            job_id,
            artifact_kind,
            artifact_url,
            meta,
            metadata,
        ))
    }
}

/// Updates `lca_package_jobs` status and diagnostics.
#[instrument(skip(pool, diagnostics))]
pub async fn update_package_job_status(
    pool: &PgPool,
    job_id: Uuid,
    status: &str,
    diagnostics: Value,
) -> anyhow::Result<f64> {
    let db_write_started = Instant::now();
    let _ = sqlx::query(
        r"
        UPDATE lca_package_jobs
        SET status = $2,
            diagnostics = $3::jsonb,
            updated_at = NOW(),
            started_at = CASE WHEN $2 = 'running' AND started_at IS NULL THEN NOW() ELSE started_at END,
            finished_at = CASE WHEN $2 IN ('ready','completed','failed','stale') AND finished_at IS NULL THEN NOW() ELSE finished_at END
        WHERE id = $1
        ",
    )
    .bind(job_id)
    .bind(status)
    .bind(diagnostics.clone())
    .execute(pool)
    .await?;
    let db_write_sec = db_write_started.elapsed().as_secs_f64();

    let diagnostics_with_timing =
        merge_package_job_status_update_timing(diagnostics.clone(), status, db_write_sec);
    if diagnostics_with_timing != diagnostics {
        set_package_job_diagnostics(pool, job_id, diagnostics_with_timing).await?;
    }

    Ok(db_write_sec)
}

/// Inserts one `lca_package_artifacts` row.
#[instrument(skip(pool, insert))]
pub async fn insert_package_artifact(
    pool: &PgPool,
    insert: PackageArtifactInsert,
) -> anyhow::Result<Uuid> {
    let artifact_kind = serde_json::to_value(insert.artifact_kind)?
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("failed to serialize artifact kind"))?
        .to_owned();
    let byte_size = i64::try_from(insert.artifact_byte_size)
        .map_err(|_| anyhow::anyhow!("artifact size exceeds i64"))?;

    let row = sqlx::query(
        r"
        INSERT INTO lca_package_artifacts (
            job_id,
            artifact_kind,
            status,
            artifact_url,
            artifact_sha256,
            artifact_byte_size,
            artifact_format,
            content_type,
            metadata,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, NOW(), NOW())
        ON CONFLICT (job_id, artifact_kind) DO UPDATE
        SET status = EXCLUDED.status,
            artifact_url = EXCLUDED.artifact_url,
            artifact_sha256 = EXCLUDED.artifact_sha256,
            artifact_byte_size = EXCLUDED.artifact_byte_size,
            artifact_format = EXCLUDED.artifact_format,
            content_type = EXCLUDED.content_type,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        RETURNING id
        ",
    )
    .bind(insert.job_id)
    .bind(artifact_kind)
    .bind(insert.status)
    .bind(insert.artifact_url)
    .bind(insert.artifact_sha256)
    .bind(byte_size)
    .bind(insert.artifact_format)
    .bind(insert.content_type)
    .bind(insert.metadata)
    .fetch_one(pool)
    .await?;

    Ok(row.try_get::<Uuid, _>("id")?)
}

/// Marks package request cache row as running for a given job.
#[instrument(skip(pool))]
pub async fn mark_package_request_cache_running(pool: &PgPool, job_id: Uuid) -> anyhow::Result<()> {
    let result = sqlx::query(
        r"
        UPDATE lca_package_request_cache
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
        Ok(_) => Ok(()),
        Err(err) if is_undefined_table(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Marks package request cache row as ready and stores result artifact ids.
#[instrument(skip(pool))]
pub async fn mark_package_request_cache_ready(
    pool: &PgPool,
    job_id: Uuid,
    export_artifact_id: Option<Uuid>,
    report_artifact_id: Option<Uuid>,
) -> anyhow::Result<()> {
    let result = sqlx::query(
        r"
        UPDATE lca_package_request_cache
        SET status = 'ready',
            export_artifact_id = $2,
            report_artifact_id = $3,
            error_code = NULL,
            error_message = NULL,
            updated_at = NOW(),
            last_accessed_at = NOW()
        WHERE job_id = $1
        ",
    )
    .bind(job_id)
    .bind(export_artifact_id)
    .bind(report_artifact_id)
    .execute(pool)
    .await;

    match result {
        Ok(_) => Ok(()),
        Err(err) if is_undefined_table(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Marks package request cache row as failed for a given job.
#[instrument(skip(pool))]
pub async fn mark_package_request_cache_failed(
    pool: &PgPool,
    job_id: Uuid,
    error_code: &str,
    error_message: &str,
) -> anyhow::Result<()> {
    let result = sqlx::query(
        r"
        UPDATE lca_package_request_cache
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
        Ok(_) => Ok(()),
        Err(err) if is_undefined_table(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Extracts `job_id` from a typed package payload.
#[must_use]
pub fn extract_package_job_id(payload: &PackageJobPayload) -> Uuid {
    match payload {
        PackageJobPayload::ExportPackage { job_id, .. }
        | PackageJobPayload::ImportPackage { job_id, .. } => *job_id,
    }
}

/// Extracts `job_id` from raw queue JSON.
#[must_use]
pub fn extract_package_job_id_from_raw_payload(payload: &Value) -> Option<Uuid> {
    payload
        .get("job_id")
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

/// Enqueues one package payload back onto the package queue.
#[instrument(skip(pool, payload))]
pub async fn enqueue_package_job_payload(
    pool: &PgPool,
    payload: &PackageJobPayload,
) -> anyhow::Result<i64> {
    let row = sqlx::query("SELECT pgmq.send($1, $2::jsonb) AS msg_id")
        .bind(PACKAGE_QUEUE_NAME)
        .bind(serde_json::to_value(payload)?)
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i64, _>("msg_id")?)
}

/// Returns whether one package job error is likely transient and worth retrying.
#[must_use]
pub fn is_retryable_package_job_error(err: &anyhow::Error) -> bool {
    if err.chain().any(|cause| {
        cause
            .downcast_ref::<sqlx::Error>()
            .is_some_and(is_retryable_sqlx_error)
    }) {
        return true;
    }

    let lowered = err.to_string().to_ascii_lowercase();
    lowered.contains("pool timed out while waiting for an open connection")
        || (lowered.contains("error communicating with database")
            && (lowered.contains("at eof")
                || lowered.contains("connection reset by peer")
                || lowered.contains("broken pipe")
                || lowered.contains("connection closed")
                || lowered.contains("unexpected eof")))
}

/// Re-enqueues one package payload after incrementing the retry attempt, if budget remains.
#[instrument(skip(pool, payload))]
pub async fn reschedule_retryable_package_job(
    pool: &PgPool,
    payload: &PackageJobPayload,
    error_message: &str,
) -> anyhow::Result<bool> {
    let job_id = extract_package_job_id(payload);
    let mut tx = pool.begin().await?;
    let retry_row = sqlx::query(
        r"
        UPDATE lca_package_jobs
        SET attempt = attempt + 1,
            status = 'queued',
            diagnostics = COALESCE(diagnostics, '{}'::jsonb) || jsonb_build_object(
                'message', 'Retrying after transient database error',
                'last_retryable_error', $2,
                'retry_count', attempt + 1,
                'max_attempt', max_attempt,
                'retry_scheduled', TRUE
            ),
            updated_at = NOW()
        WHERE id = $1
          AND attempt < max_attempt
        RETURNING attempt, max_attempt
        ",
    )
    .bind(job_id)
    .bind(error_message)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(row) = retry_row else {
        tx.rollback().await?;
        return Ok(false);
    };

    let _ = sqlx::query("SELECT pgmq.send($1, $2::jsonb) AS msg_id")
        .bind(PACKAGE_QUEUE_NAME)
        .bind(serde_json::to_value(payload)?)
        .fetch_one(&mut *tx)
        .await?;

    tx.commit().await?;

    warn!(
        job_id = %job_id,
        attempt = row.try_get::<i32, _>("attempt").unwrap_or_default(),
        max_attempt = row.try_get::<i32, _>("max_attempt").unwrap_or_default(),
        error = error_message,
        "rescheduled package job after retryable database error"
    );

    Ok(true)
}

/// Executes one package queue payload end-to-end.
#[instrument(skip(state))]
#[allow(clippy::too_many_lines)]
pub async fn handle_package_job_payload(
    state: &AppState,
    payload: PackageJobPayload,
) -> anyhow::Result<()> {
    match payload {
        PackageJobPayload::ExportPackage {
            job_id,
            requested_by,
            scope,
            roots,
        } => {
            if let Err(err) = mark_package_request_cache_running(&state.pool, job_id).await {
                warn!(
                    error = %err,
                    job_id = %job_id,
                    "failed to mark package request cache running"
                );
            }

            let outcome =
                match execute_export_package(state, job_id, requested_by, scope, roots.as_slice())
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        clear_runtime_export_traversal_cache(job_id);
                        return Err(err);
                    }
                };
            let _ = update_package_job_status(
                &state.pool,
                job_id,
                outcome.final_status,
                outcome.diagnostics,
            )
            .await?;

            if outcome.final_status == "running" {
                let _ = enqueue_package_job_payload(
                    &state.pool,
                    &PackageJobPayload::ExportPackage {
                        job_id,
                        requested_by,
                        scope,
                        roots,
                    },
                )
                .await?;
            } else if let Err(err) = mark_package_request_cache_ready(
                &state.pool,
                job_id,
                outcome.export_artifact_id,
                outcome.report_artifact_id,
            )
            .await
            {
                warn!(
                    error = %err,
                    job_id = %job_id,
                    "failed to mark package request cache ready"
                );
            }

            if outcome.final_status != "running" {
                clear_runtime_export_traversal_cache(job_id);
            }

            Ok(())
        }
        PackageJobPayload::ImportPackage {
            job_id,
            requested_by,
            source_artifact_id,
        } => {
            let _ = update_package_job_status(
                &state.pool,
                job_id,
                "running",
                serde_json::json!({
                    "phase": "import_package",
                    "source_artifact_id": source_artifact_id
                }),
            )
            .await?;

            if let Err(err) = mark_package_request_cache_running(&state.pool, job_id).await {
                warn!(
                    error = %err,
                    job_id = %job_id,
                    "failed to mark package request cache running"
                );
            }

            let outcome =
                execute_import_package(state, job_id, requested_by, source_artifact_id).await?;
            let _ = update_package_job_status(
                &state.pool,
                job_id,
                outcome.final_status,
                outcome.diagnostics,
            )
            .await?;
            if let Err(err) = mark_package_request_cache_ready(
                &state.pool,
                job_id,
                outcome.export_artifact_id,
                outcome.report_artifact_id,
            )
            .await
            {
                warn!(
                    error = %err,
                    job_id = %job_id,
                    "failed to mark package request cache ready"
                );
            }

            Ok(())
        }
    }
}

fn merge_package_job_status_update_timing(
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

#[instrument(skip(pool, diagnostics))]
async fn set_package_job_diagnostics(
    pool: &PgPool,
    job_id: Uuid,
    diagnostics: Value,
) -> anyhow::Result<()> {
    let _ = sqlx::query(
        r"
        UPDATE lca_package_jobs
        SET diagnostics = $2::jsonb
        WHERE id = $1
        ",
    )
    .bind(job_id)
    .bind(diagnostics)
    .execute(pool)
    .await?;
    Ok(())
}

fn is_undefined_table(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().as_deref() == Some("42P01"),
        _ => false,
    }
}

fn is_retryable_sqlx_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Io(io_err) => matches!(
            io_err.kind(),
            std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::UnexpectedEof
        ),
        sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::Protocol(_) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        extract_package_job_id, extract_package_job_id_from_raw_payload,
        is_retryable_package_job_error, merge_package_job_status_update_timing,
    };
    use crate::package_types::{PackageExportScope, PackageJobPayload};

    #[test]
    fn extract_job_id_from_typed_export_payload() {
        let payload = PackageJobPayload::ExportPackage {
            job_id: Uuid::nil(),
            requested_by: Uuid::nil(),
            scope: PackageExportScope::CurrentUser,
            roots: Vec::new(),
        };

        assert_eq!(extract_package_job_id(&payload), Uuid::nil());
    }

    #[test]
    fn extract_job_id_from_raw_payload() {
        let payload = json!({
            "job_id": Uuid::nil().to_string()
        });

        assert_eq!(
            extract_package_job_id_from_raw_payload(&payload),
            Some(Uuid::nil())
        );
    }

    #[test]
    fn merge_job_status_update_timing_appends_fields() {
        let merged = merge_package_job_status_update_timing(
            json!({"phase": "export_package"}),
            "running",
            0.125,
        );

        assert_eq!(
            merged["job_status_update_timing_sec"]["last_status"],
            "running"
        );
        assert_eq!(
            merged["job_status_update_timing_sec"]["running_db_write_sec"],
            0.125
        );
        assert_eq!(
            merged["job_status_update_timing_sec"]["last_db_write_sec"],
            0.125
        );
    }

    #[test]
    fn retryable_package_error_matches_sqlx_pool_timeout() {
        let err = anyhow::Error::new(sqlx::Error::PoolTimedOut);
        assert!(is_retryable_package_job_error(&err));
    }

    #[test]
    fn retryable_package_error_matches_eof_message() {
        let err = anyhow!(
            "error communicating with database: expected to read 9784 bytes, got 6812 bytes at EOF"
        );
        assert!(is_retryable_package_job_error(&err));
    }
}
