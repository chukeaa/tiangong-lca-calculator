use uuid::Uuid;

use crate::{
    package_types::PackageArtifactKind,
    pgbouncer_sqlx::{self as sqlx, PgPool, Row},
};

pub const DEFAULT_EXPORT_PACKAGE_ARTIFACT_RETENTION_DAYS: i32 = 30;
pub const DEFAULT_IMPORT_PACKAGE_ARTIFACT_RETENTION_DAYS: i32 = 14;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRetentionSummaryRow {
    pub retention_area: String,
    pub retention_action: String,
    pub is_eligible: bool,
    pub reason: String,
    pub row_count: i64,
    pub total_artifact_bytes: i64,
    pub total_hit_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageArtifactGcCandidate {
    pub artifact_id: Uuid,
    pub job_id: Uuid,
    pub artifact_kind: String,
    pub artifact_url: String,
}

#[must_use]
pub const fn default_package_artifact_retention_days(kind: PackageArtifactKind) -> i32 {
    match kind {
        PackageArtifactKind::ExportZip | PackageArtifactKind::ExportReport => {
            DEFAULT_EXPORT_PACKAGE_ARTIFACT_RETENTION_DAYS
        }
        PackageArtifactKind::ImportSource | PackageArtifactKind::ImportReport => {
            DEFAULT_IMPORT_PACKAGE_ARTIFACT_RETENTION_DAYS
        }
    }
}

pub fn validate_retention_days(value: i32, name: &str) -> anyhow::Result<i32> {
    if !(1..=3650).contains(&value) {
        return Err(anyhow::anyhow!("{name} must be between 1 and 3650 days"));
    }
    Ok(value)
}

#[allow(clippy::too_many_lines)]
pub async fn fetch_package_retention_summary(
    pool: &PgPool,
    job_retention_days: i32,
    request_cache_retention_days: i32,
) -> anyhow::Result<Vec<PackageRetentionSummaryRow>> {
    if legacy_package_jobs_exists(pool).await? {
        fetch_package_retention_summary_with_legacy_jobs(
            pool,
            job_retention_days,
            request_cache_retention_days,
        )
        .await
    } else {
        fetch_package_retention_summary_without_legacy_jobs(pool, request_cache_retention_days)
            .await
    }
}

#[allow(clippy::too_many_lines)]
async fn fetch_package_retention_summary_with_legacy_jobs(
    pool: &PgPool,
    job_retention_days: i32,
    request_cache_retention_days: i32,
) -> anyhow::Result<Vec<PackageRetentionSummaryRow>> {
    let rows = sqlx::query(
        r"
        WITH job_rollup AS (
          SELECT
            jobs.id,
            jobs.status,
            COALESCE(jobs.finished_at, jobs.updated_at, jobs.created_at) AS lifecycle_at,
            COALESCE(BOOL_OR(artifacts.is_pinned) FILTER (WHERE artifacts.id IS NOT NULL), FALSE) AS has_pinned_artifact,
            COALESCE(BOOL_OR(artifacts.status <> 'deleted') FILTER (WHERE artifacts.id IS NOT NULL), FALSE) AS has_live_artifact,
            COALESCE(SUM(COALESCE(artifacts.artifact_byte_size, 0)), 0)::bigint AS artifact_bytes,
            EXISTS (
              SELECT 1
              FROM public.lca_package_request_cache AS recent_cache
              WHERE recent_cache.job_id = jobs.id
                AND (
                  recent_cache.status IN ('pending', 'running')
                  OR recent_cache.last_accessed_at >= NOW() - make_interval(days => $2::integer)
                )
            ) AS has_protected_request_cache
          FROM public.lca_package_jobs AS jobs
          LEFT JOIN public.lca_package_artifacts AS artifacts
            ON artifacts.job_id = jobs.id
          GROUP BY jobs.id
        ),
        artifact_classified AS (
          SELECT
            'lca_package_artifacts'::text AS retention_area,
            'delete_object_then_mark_deleted'::text AS retention_action,
            (classified.reason = 'eligible_expired_unpinned_artifact') AS is_eligible,
            classified.reason,
            1::bigint AS row_count,
            COALESCE(classified.artifact_byte_size, 0)::bigint AS total_artifact_bytes,
            0::bigint AS total_hit_count
          FROM (
            SELECT
              artifacts.*,
              jobs.status AS parent_job_status,
              CASE
                WHEN jobs.id IS NULL THEN 'protected_missing_parent_job'
                WHEN artifacts.is_pinned THEN 'protected_pinned_artifact'
                WHEN artifacts.status = 'deleted' THEN 'protected_already_deleted'
                WHEN artifacts.status <> 'ready' THEN 'protected_artifact_not_ready'
                WHEN jobs.status IN ('queued', 'running') THEN 'protected_active_parent_job'
                WHEN artifacts.expires_at IS NULL THEN 'protected_missing_expires_at'
                WHEN artifacts.expires_at > NOW() THEN 'protected_expires_at_in_future'
                WHEN EXISTS (
                  SELECT 1
                  FROM public.lca_package_request_cache AS recent_cache
                  WHERE (
                      recent_cache.export_artifact_id = artifacts.id
                      OR recent_cache.report_artifact_id = artifacts.id
                    )
                    AND (
                      recent_cache.status IN ('pending', 'running')
                      OR recent_cache.last_accessed_at >= NOW() - make_interval(days => $2::integer)
                    )
                ) THEN 'protected_request_cache_reference'
                ELSE 'eligible_expired_unpinned_artifact'
              END AS reason
            FROM public.lca_package_artifacts AS artifacts
            LEFT JOIN public.lca_package_jobs AS jobs
              ON jobs.id = artifacts.job_id
          ) AS classified
        ),
        request_cache_classified AS (
          SELECT
            'lca_package_request_cache'::text AS retention_area,
            'delete_stale_request_cache_row'::text AS retention_action,
            (classified.reason = 'eligible_stale_request_cache') AS is_eligible,
            classified.reason,
            1::bigint AS row_count,
            0::bigint AS total_artifact_bytes,
            classified.hit_count::bigint AS total_hit_count
          FROM (
            SELECT
              request_cache.*,
              jobs.status AS parent_job_status,
              CASE
                WHEN request_cache.status IN ('pending', 'running') THEN 'protected_active_request_cache'
                WHEN request_cache.last_accessed_at >= NOW() - make_interval(days => $2::integer) THEN 'protected_recent_request_cache_access'
                WHEN jobs.status IN ('queued', 'running') THEN 'protected_active_parent_job'
                ELSE 'eligible_stale_request_cache'
              END AS reason
            FROM public.lca_package_request_cache AS request_cache
            LEFT JOIN public.lca_package_jobs AS jobs
              ON jobs.id = request_cache.job_id
          ) AS classified
        ),
        job_classified AS (
          SELECT
            'lca_package_jobs'::text AS retention_area,
            'delete_job_metadata_after_object_gc'::text AS retention_action,
            (classified.reason = 'eligible_terminal_job_after_object_gc') AS is_eligible,
            classified.reason,
            1::bigint AS row_count,
            classified.artifact_bytes AS total_artifact_bytes,
            0::bigint AS total_hit_count
          FROM (
            SELECT
              job_rollup.*,
              CASE
                WHEN job_rollup.status IN ('queued', 'running') THEN 'protected_active_job'
                WHEN job_rollup.status NOT IN ('ready', 'completed', 'failed', 'stale') THEN 'protected_unknown_job_status'
                WHEN job_rollup.lifecycle_at >= NOW() - make_interval(days => $1::integer) THEN 'protected_inside_job_retention_window'
                WHEN job_rollup.has_pinned_artifact THEN 'protected_pinned_artifact'
                WHEN job_rollup.has_live_artifact THEN 'protected_object_not_deleted'
                WHEN job_rollup.has_protected_request_cache THEN 'protected_request_cache_reference'
                ELSE 'eligible_terminal_job_after_object_gc'
              END AS reason
            FROM job_rollup
          ) AS classified
        ),
        classified AS (
          SELECT * FROM artifact_classified
          UNION ALL
          SELECT * FROM request_cache_classified
          UNION ALL
          SELECT * FROM job_classified
        )
        SELECT
          retention_area,
          retention_action,
          is_eligible,
          reason,
          SUM(row_count)::bigint AS row_count,
          COALESCE(SUM(total_artifact_bytes), 0)::bigint AS total_artifact_bytes,
          COALESCE(SUM(total_hit_count), 0)::bigint AS total_hit_count
        FROM classified
        GROUP BY retention_area, retention_action, is_eligible, reason
        ORDER BY retention_area, is_eligible DESC, reason
        ",
    )
    .bind(job_retention_days)
    .bind(request_cache_retention_days)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(PackageRetentionSummaryRow {
                retention_area: row.try_get("retention_area")?,
                retention_action: row.try_get("retention_action")?,
                is_eligible: row.try_get("is_eligible")?,
                reason: row.try_get("reason")?,
                row_count: row.try_get("row_count")?,
                total_artifact_bytes: row.try_get("total_artifact_bytes")?,
                total_hit_count: row.try_get("total_hit_count")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(Into::into)
}

#[allow(clippy::too_many_lines)]
async fn fetch_package_retention_summary_without_legacy_jobs(
    pool: &PgPool,
    request_cache_retention_days: i32,
) -> anyhow::Result<Vec<PackageRetentionSummaryRow>> {
    let rows = sqlx::query(
        r"
        WITH artifact_classified AS (
          SELECT
            'lca_package_artifacts'::text AS retention_area,
            'delete_object_then_mark_deleted'::text AS retention_action,
            (classified.reason = 'eligible_expired_unpinned_artifact') AS is_eligible,
            classified.reason,
            1::bigint AS row_count,
            COALESCE(classified.artifact_byte_size, 0)::bigint AS total_artifact_bytes,
            0::bigint AS total_hit_count
          FROM (
            SELECT
              artifacts.*,
              worker_job.status AS parent_worker_job_status,
              CASE
                WHEN artifacts.worker_job_id IS NULL THEN 'protected_missing_parent_worker_job'
                WHEN worker_job.id IS NULL THEN 'protected_missing_parent_worker_job'
                WHEN artifacts.is_pinned THEN 'protected_pinned_artifact'
                WHEN artifacts.status = 'deleted' THEN 'protected_already_deleted'
                WHEN artifacts.status <> 'ready' THEN 'protected_artifact_not_ready'
                WHEN worker_job.status IN ('queued', 'running', 'waiting') THEN 'protected_active_parent_worker_job'
                WHEN artifacts.expires_at IS NULL THEN 'protected_missing_expires_at'
                WHEN artifacts.expires_at > NOW() THEN 'protected_expires_at_in_future'
                WHEN EXISTS (
                  SELECT 1
                  FROM public.lca_package_request_cache AS recent_cache
                  WHERE (
                      recent_cache.export_artifact_id = artifacts.id
                      OR recent_cache.report_artifact_id = artifacts.id
                    )
                    AND (
                      recent_cache.status IN ('pending', 'running')
                      OR recent_cache.last_accessed_at >= NOW() - make_interval(days => $1::integer)
                    )
                ) THEN 'protected_request_cache_reference'
                ELSE 'eligible_expired_unpinned_artifact'
              END AS reason
            FROM public.lca_package_artifacts AS artifacts
            LEFT JOIN public.worker_jobs AS worker_job
              ON worker_job.id = artifacts.worker_job_id
          ) AS classified
        ),
        request_cache_classified AS (
          SELECT
            'lca_package_request_cache'::text AS retention_area,
            'delete_stale_request_cache_row'::text AS retention_action,
            (classified.reason = 'eligible_stale_request_cache') AS is_eligible,
            classified.reason,
            1::bigint AS row_count,
            0::bigint AS total_artifact_bytes,
            classified.hit_count::bigint AS total_hit_count
          FROM (
            SELECT
              request_cache.*,
              worker_job.status AS parent_worker_job_status,
              CASE
                WHEN request_cache.status IN ('pending', 'running') THEN 'protected_active_request_cache'
                WHEN request_cache.last_accessed_at >= NOW() - make_interval(days => $1::integer) THEN 'protected_recent_request_cache_access'
                WHEN worker_job.status IN ('queued', 'running', 'waiting') THEN 'protected_active_parent_worker_job'
                ELSE 'eligible_stale_request_cache'
              END AS reason
            FROM public.lca_package_request_cache AS request_cache
            LEFT JOIN public.worker_jobs AS worker_job
              ON worker_job.id = request_cache.worker_job_id
          ) AS classified
        ),
        classified AS (
          SELECT * FROM artifact_classified
          UNION ALL
          SELECT * FROM request_cache_classified
        )
        SELECT
          retention_area,
          retention_action,
          is_eligible,
          reason,
          SUM(row_count)::bigint AS row_count,
          COALESCE(SUM(total_artifact_bytes), 0)::bigint AS total_artifact_bytes,
          COALESCE(SUM(total_hit_count), 0)::bigint AS total_hit_count
        FROM classified
        GROUP BY retention_area, retention_action, is_eligible, reason
        ORDER BY retention_area, is_eligible DESC, reason
        ",
    )
    .bind(request_cache_retention_days)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(PackageRetentionSummaryRow {
                retention_area: row.try_get("retention_area")?,
                retention_action: row.try_get("retention_action")?,
                is_eligible: row.try_get("is_eligible")?,
                reason: row.try_get("reason")?,
                row_count: row.try_get("row_count")?,
                total_artifact_bytes: row.try_get("total_artifact_bytes")?,
                total_hit_count: row.try_get("total_hit_count")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(Into::into)
}

pub async fn fetch_package_artifact_gc_candidates(
    pool: &PgPool,
    batch_size: i64,
    request_cache_retention_days: i32,
) -> anyhow::Result<Vec<PackageArtifactGcCandidate>> {
    let rows = sqlx::query(
        r"
        SELECT
          artifacts.id AS artifact_id,
          artifacts.job_id,
          artifacts.artifact_kind,
          artifacts.artifact_url
        FROM public.lca_package_artifacts AS artifacts
        JOIN public.worker_jobs AS worker_job
          ON worker_job.id = artifacts.worker_job_id
        WHERE artifacts.status = 'ready'
          AND artifacts.is_pinned = FALSE
          AND artifacts.expires_at IS NOT NULL
          AND artifacts.expires_at <= NOW()
          AND worker_job.status NOT IN ('queued', 'running', 'waiting')
          AND NOT EXISTS (
            SELECT 1
            FROM public.lca_package_request_cache AS request_cache
            WHERE (
                request_cache.export_artifact_id = artifacts.id
                OR request_cache.report_artifact_id = artifacts.id
              )
              AND (
                request_cache.status IN ('pending', 'running')
                OR request_cache.last_accessed_at >= NOW() - make_interval(days => $2::integer)
              )
          )
        ORDER BY artifacts.expires_at ASC, artifacts.created_at ASC, artifacts.id ASC
        LIMIT $1
        ",
    )
    .bind(batch_size)
    .bind(request_cache_retention_days)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(PackageArtifactGcCandidate {
                artifact_id: row.try_get("artifact_id")?,
                job_id: row.try_get("job_id")?,
                artifact_kind: row.try_get("artifact_kind")?,
                artifact_url: row.try_get("artifact_url")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(Into::into)
}

pub async fn mark_package_artifact_deleted(
    pool: &PgPool,
    artifact_id: Uuid,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r"
        UPDATE public.lca_package_artifacts
        SET status = 'deleted',
            metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
              'gc',
              jsonb_build_object(
                'status', 'object_deleted',
                'deleted_at', NOW(),
                'reason', 'eligible_expired_unpinned_artifact'
              )
            ),
            updated_at = NOW()
        WHERE id = $1
          AND status <> 'deleted'
        ",
    )
    .bind(artifact_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

pub async fn record_package_artifact_gc_error(
    pool: &PgPool,
    artifact_id: Uuid,
    error_message: &str,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r"
        UPDATE public.lca_package_artifacts
        SET metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
              'gc',
              jsonb_build_object(
                'status', 'object_delete_failed',
                'last_error_at', NOW(),
                'last_error', $2
              )
            ),
            updated_at = NOW()
        WHERE id = $1
          AND status <> 'deleted'
        ",
    )
    .bind(artifact_id)
    .bind(error_message)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

pub async fn delete_stale_package_request_cache_rows(
    pool: &PgPool,
    batch_size: i64,
    request_cache_retention_days: i32,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r"
        WITH candidates AS (
          SELECT request_cache.id
          FROM public.lca_package_request_cache AS request_cache
          LEFT JOIN public.worker_jobs AS worker_job
            ON worker_job.id = request_cache.worker_job_id
          WHERE request_cache.status NOT IN ('pending', 'running')
            AND request_cache.last_accessed_at < NOW() - make_interval(days => $2::integer)
            AND COALESCE(worker_job.status NOT IN ('queued', 'running', 'waiting'), TRUE)
          ORDER BY request_cache.last_accessed_at ASC, request_cache.id ASC
          LIMIT $1
          FOR UPDATE OF request_cache SKIP LOCKED
        )
        DELETE FROM public.lca_package_request_cache AS request_cache
        USING candidates
        WHERE request_cache.id = candidates.id
        ",
    )
    .bind(batch_size)
    .bind(request_cache_retention_days)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

pub async fn delete_package_jobs_after_object_gc(
    pool: &PgPool,
    batch_size: i64,
    job_retention_days: i32,
    request_cache_retention_days: i32,
) -> anyhow::Result<u64> {
    if !legacy_package_jobs_exists(pool).await? {
        return Ok(0);
    }

    let result = sqlx::query(
        r"
        WITH candidates AS (
          SELECT jobs.id
          FROM public.lca_package_jobs AS jobs
          WHERE jobs.status IN ('ready', 'completed', 'failed', 'stale')
            AND COALESCE(jobs.finished_at, jobs.updated_at, jobs.created_at)
              < NOW() - make_interval(days => $2::integer)
            AND NOT EXISTS (
              SELECT 1
              FROM public.lca_package_artifacts AS artifacts
              WHERE artifacts.job_id = jobs.id
                AND artifacts.is_pinned
            )
            AND NOT EXISTS (
              SELECT 1
              FROM public.lca_package_artifacts AS artifacts
              WHERE artifacts.job_id = jobs.id
                AND artifacts.status <> 'deleted'
            )
            AND NOT EXISTS (
              SELECT 1
              FROM public.lca_package_request_cache AS request_cache
              WHERE request_cache.job_id = jobs.id
                AND (
                  request_cache.status IN ('pending', 'running')
                  OR request_cache.last_accessed_at >= NOW() - make_interval(days => $3::integer)
                )
            )
          ORDER BY COALESCE(jobs.finished_at, jobs.updated_at, jobs.created_at) ASC, jobs.id ASC
          LIMIT $1
          FOR UPDATE OF jobs SKIP LOCKED
        )
        DELETE FROM public.lca_package_jobs AS jobs
        USING candidates
        WHERE jobs.id = candidates.id
        ",
    )
    .bind(batch_size)
    .bind(job_retention_days)
    .bind(request_cache_retention_days)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

async fn legacy_package_jobs_exists(pool: &PgPool) -> anyhow::Result<bool> {
    let table_name =
        sqlx::query_scalar::<Option<String>>("SELECT to_regclass('public.lca_package_jobs')::text")
            .fetch_one(pool)
            .await?;
    Ok(table_name.is_some())
}

pub async fn refresh_import_source_retention(
    pool: &PgPool,
    artifact_id: Uuid,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r"
        UPDATE public.lca_package_artifacts
        SET expires_at = NOW() + make_interval(days => $2::integer),
            metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
              'retention',
              jsonb_build_object(
                'policy', 'import_source_terminal',
                'retention_days', $2::integer,
                'refreshed_at', NOW()
              )
            ),
            updated_at = NOW()
        WHERE id = $1
          AND artifact_kind = 'import_source'
          AND status = 'ready'
          AND is_pinned = FALSE
        ",
    )
    .bind(artifact_id)
    .bind(DEFAULT_IMPORT_PACKAGE_ARTIFACT_RETENTION_DAYS)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_EXPORT_PACKAGE_ARTIFACT_RETENTION_DAYS,
        DEFAULT_IMPORT_PACKAGE_ARTIFACT_RETENTION_DAYS, default_package_artifact_retention_days,
        validate_retention_days,
    };
    use crate::package_types::PackageArtifactKind;

    #[test]
    fn package_artifact_retention_defaults_follow_kind() {
        assert_eq!(
            default_package_artifact_retention_days(PackageArtifactKind::ExportZip),
            DEFAULT_EXPORT_PACKAGE_ARTIFACT_RETENTION_DAYS
        );
        assert_eq!(
            default_package_artifact_retention_days(PackageArtifactKind::ExportReport),
            DEFAULT_EXPORT_PACKAGE_ARTIFACT_RETENTION_DAYS
        );
        assert_eq!(
            default_package_artifact_retention_days(PackageArtifactKind::ImportSource),
            DEFAULT_IMPORT_PACKAGE_ARTIFACT_RETENTION_DAYS
        );
        assert_eq!(
            default_package_artifact_retention_days(PackageArtifactKind::ImportReport),
            DEFAULT_IMPORT_PACKAGE_ARTIFACT_RETENTION_DAYS
        );
    }

    #[test]
    fn retention_days_must_be_positive_and_bounded() {
        assert!(validate_retention_days(1, "retention").is_ok());
        assert!(validate_retention_days(3650, "retention").is_ok());
        assert!(validate_retention_days(0, "retention").is_err());
        assert!(validate_retention_days(3651, "retention").is_err());
    }
}
