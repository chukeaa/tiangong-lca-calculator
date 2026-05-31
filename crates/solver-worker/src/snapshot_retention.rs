use std::collections::BTreeMap;

use serde_json::Value;
use uuid::Uuid;

use crate::pgbouncer_sqlx::{self as sqlx, PgPool, Row};

pub const DEFAULT_SNAPSHOT_RETENTION_DAYS: i32 = 30;
pub const DEFAULT_ORPHAN_RETENTION_DAYS: i32 = 30;
pub const DEFAULT_MAX_SNAPSHOTS: i32 = 100;
pub const DEFAULT_MAX_ORPHAN_DIRS: i32 = 200;
pub const DEFAULT_MAX_BYTES: i64 = 2_147_483_648;
pub const DEFAULT_BATCH_SIZE: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotGcPolicy {
    pub snapshot_retention_days: i32,
    pub orphan_retention_days: i32,
    pub max_snapshots: i32,
    pub max_orphan_dirs: i32,
    pub max_bytes: i64,
}

impl Default for SnapshotGcPolicy {
    fn default() -> Self {
        Self {
            snapshot_retention_days: DEFAULT_SNAPSHOT_RETENTION_DAYS,
            orphan_retention_days: DEFAULT_ORPHAN_RETENTION_DAYS,
            max_snapshots: DEFAULT_MAX_SNAPSHOTS,
            max_orphan_dirs: DEFAULT_MAX_ORPHAN_DIRS,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRetentionSummaryRow {
    pub retention_area: String,
    pub retention_action: String,
    pub is_eligible: bool,
    pub reason: String,
    pub snapshot_count: i64,
    pub object_count: i64,
    pub total_storage_bytes: i64,
    pub downstream_active_count: i64,
    pub downstream_job_count: i64,
    pub downstream_result_count: i64,
    pub downstream_cache_count: i64,
    pub downstream_latest_count: i64,
    pub downstream_factorization_count: i64,
    pub downstream_artifact_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotGcCandidate {
    pub candidate_type: String,
    pub snapshot_id: Option<Uuid>,
    pub snapshot_directory: String,
    pub bucket_id: String,
    pub object_name: String,
    pub storage_bytes: i64,
    pub reason: String,
    pub delete_db_snapshot: bool,
    pub object_count: i64,
    pub snapshot_storage_bytes: i64,
    pub downstream_job_count: i64,
    pub downstream_result_count: i64,
    pub downstream_cache_count: i64,
    pub downstream_latest_count: i64,
    pub downstream_factorization_count: i64,
    pub downstream_artifact_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotGcDirectory {
    pub candidate_type: String,
    pub snapshot_id: Option<Uuid>,
    pub snapshot_directory: String,
    pub reason: String,
    pub delete_db_snapshot: bool,
    pub storage_bytes: i64,
    pub objects: Vec<SnapshotGcCandidate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectDeleteStatus {
    Deleted,
    Missing,
    Failed,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotGcRunTotals {
    pub storage_deleted_count: i32,
    pub storage_failed_count: i32,
    pub db_snapshot_deleted_count: i32,
}

pub fn validate_snapshot_gc_policy(policy: SnapshotGcPolicy) -> anyhow::Result<SnapshotGcPolicy> {
    validate_days(
        policy.snapshot_retention_days,
        "SNAPSHOT_GC_SNAPSHOT_RETENTION_DAYS",
    )?;
    validate_days(
        policy.orphan_retention_days,
        "SNAPSHOT_GC_ORPHAN_RETENTION_DAYS",
    )?;
    validate_positive_i32(policy.max_snapshots, "SNAPSHOT_GC_MAX_SNAPSHOTS")?;
    validate_positive_i32(policy.max_orphan_dirs, "SNAPSHOT_GC_MAX_ORPHAN_DIRS")?;
    if policy.max_bytes <= 0 {
        return Err(anyhow::anyhow!("SNAPSHOT_GC_MAX_BYTES must be > 0"));
    }
    Ok(policy)
}

pub fn validate_days(value: i32, name: &str) -> anyhow::Result<i32> {
    if !(1..=3650).contains(&value) {
        return Err(anyhow::anyhow!("{name} must be between 1 and 3650 days"));
    }
    Ok(value)
}

pub fn validate_positive_i32(value: i32, name: &str) -> anyhow::Result<i32> {
    if value <= 0 {
        return Err(anyhow::anyhow!("{name} must be > 0"));
    }
    Ok(value)
}

#[must_use]
pub fn group_candidates_by_directory(
    candidates: &[SnapshotGcCandidate],
) -> Vec<SnapshotGcDirectory> {
    let mut grouped: BTreeMap<(String, String), SnapshotGcDirectory> = BTreeMap::new();
    for candidate in candidates {
        let key = (
            candidate.candidate_type.clone(),
            candidate.snapshot_directory.clone(),
        );
        let entry = grouped.entry(key).or_insert_with(|| SnapshotGcDirectory {
            candidate_type: candidate.candidate_type.clone(),
            snapshot_id: candidate.snapshot_id,
            snapshot_directory: candidate.snapshot_directory.clone(),
            reason: candidate.reason.clone(),
            delete_db_snapshot: candidate.delete_db_snapshot,
            storage_bytes: candidate.snapshot_storage_bytes,
            objects: Vec::new(),
        });
        entry.objects.push(candidate.clone());
    }

    grouped.into_values().collect()
}

#[must_use]
pub const fn object_delete_succeeded(status: ObjectDeleteStatus) -> bool {
    matches!(
        status,
        ObjectDeleteStatus::Deleted | ObjectDeleteStatus::Missing
    )
}

#[must_use]
pub fn should_delete_db_snapshot(
    directory: &SnapshotGcDirectory,
    object_statuses: &[ObjectDeleteStatus],
) -> bool {
    directory.delete_db_snapshot
        && object_statuses.len() == directory.objects.len()
        && object_statuses.iter().copied().all(object_delete_succeeded)
}

pub async fn fetch_snapshot_retention_summary(
    pool: &PgPool,
    policy: SnapshotGcPolicy,
) -> anyhow::Result<Vec<SnapshotRetentionSummaryRow>> {
    let rows = sqlx::query(
        r"
        SELECT
          retention_area,
          retention_action,
          is_eligible,
          reason,
          snapshot_count,
          object_count,
          total_storage_bytes,
          downstream_active_count,
          downstream_job_count,
          downstream_result_count,
          downstream_cache_count,
          downstream_latest_count,
          downstream_factorization_count,
          downstream_artifact_count
        FROM util.preview_lca_snapshot_retention(
          make_interval(days => $1::integer),
          make_interval(days => $2::integer),
          NOW()
        )
        ",
    )
    .bind(policy.snapshot_retention_days)
    .bind(policy.orphan_retention_days)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(SnapshotRetentionSummaryRow {
                retention_area: row.try_get("retention_area")?,
                retention_action: row.try_get("retention_action")?,
                is_eligible: row.try_get("is_eligible")?,
                reason: row.try_get("reason")?,
                snapshot_count: row.try_get("snapshot_count")?,
                object_count: row.try_get("object_count")?,
                total_storage_bytes: row.try_get("total_storage_bytes")?,
                downstream_active_count: row.try_get("downstream_active_count")?,
                downstream_job_count: row.try_get("downstream_job_count")?,
                downstream_result_count: row.try_get("downstream_result_count")?,
                downstream_cache_count: row.try_get("downstream_cache_count")?,
                downstream_latest_count: row.try_get("downstream_latest_count")?,
                downstream_factorization_count: row.try_get("downstream_factorization_count")?,
                downstream_artifact_count: row.try_get("downstream_artifact_count")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(Into::into)
}

pub async fn fetch_snapshot_gc_candidates(
    pool: &PgPool,
    policy: SnapshotGcPolicy,
) -> anyhow::Result<Vec<SnapshotGcCandidate>> {
    let rows = sqlx::query(
        r"
        SELECT
          candidate_type,
          snapshot_id,
          snapshot_directory,
          bucket_id,
          object_name,
          storage_bytes,
          reason,
          delete_db_snapshot,
          object_count,
          snapshot_storage_bytes,
          downstream_job_count,
          downstream_result_count,
          downstream_cache_count,
          downstream_latest_count,
          downstream_factorization_count,
          downstream_artifact_count
        FROM util.list_lca_snapshot_gc_candidates(
          make_interval(days => $1::integer),
          make_interval(days => $2::integer),
          NOW(),
          $3::integer,
          $4::integer,
          $5::bigint
        )
        ",
    )
    .bind(policy.snapshot_retention_days)
    .bind(policy.orphan_retention_days)
    .bind(policy.max_snapshots)
    .bind(policy.max_orphan_dirs)
    .bind(policy.max_bytes)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(SnapshotGcCandidate {
                candidate_type: row.try_get("candidate_type")?,
                snapshot_id: row.try_get("snapshot_id")?,
                snapshot_directory: row.try_get("snapshot_directory")?,
                bucket_id: row.try_get("bucket_id")?,
                object_name: row.try_get("object_name")?,
                storage_bytes: row.try_get("storage_bytes")?,
                reason: row.try_get("reason")?,
                delete_db_snapshot: row.try_get("delete_db_snapshot")?,
                object_count: row.try_get("object_count")?,
                snapshot_storage_bytes: row.try_get("snapshot_storage_bytes")?,
                downstream_job_count: row.try_get("downstream_job_count")?,
                downstream_result_count: row.try_get("downstream_result_count")?,
                downstream_cache_count: row.try_get("downstream_cache_count")?,
                downstream_latest_count: row.try_get("downstream_latest_count")?,
                downstream_factorization_count: row.try_get("downstream_factorization_count")?,
                downstream_artifact_count: row.try_get("downstream_artifact_count")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(Into::into)
}

pub async fn create_snapshot_gc_run(
    pool: &PgPool,
    mode: &str,
    status: &str,
    policy: SnapshotGcPolicy,
    candidates: &[SnapshotGcCandidate],
    diagnostics: Value,
) -> anyhow::Result<Uuid> {
    let candidate_snapshot_count = count_directories(candidates, "snapshot_directory")?;
    let candidate_orphan_dir_count = count_directories(candidates, "orphan_storage_directory")?;
    let candidate_object_count = usize_to_i32(candidates.len(), "candidate object count")?;
    let candidate_storage_bytes = sum_candidate_storage_bytes(candidates)?;

    let row = sqlx::query(
        r"
        INSERT INTO public.lca_snapshot_gc_runs (
          mode,
          status,
          snapshot_retention_window,
          orphan_retention_window,
          max_snapshots,
          max_orphan_dirs,
          max_bytes,
          candidate_snapshot_count,
          candidate_orphan_dir_count,
          candidate_object_count,
          candidate_storage_bytes,
          diagnostics,
          finished_at
        ) VALUES (
          $1,
          $2,
          make_interval(days => $3::integer),
          make_interval(days => $4::integer),
          $5,
          $6,
          $7,
          $8,
          $9,
          $10,
          $11,
          $12,
          CASE WHEN $2 IN ('succeeded', 'failed', 'skipped') THEN NOW() ELSE NULL END
        )
        RETURNING id
        ",
    )
    .bind(mode)
    .bind(status)
    .bind(policy.snapshot_retention_days)
    .bind(policy.orphan_retention_days)
    .bind(policy.max_snapshots)
    .bind(policy.max_orphan_dirs)
    .bind(policy.max_bytes)
    .bind(candidate_snapshot_count)
    .bind(candidate_orphan_dir_count)
    .bind(candidate_object_count)
    .bind(candidate_storage_bytes)
    .bind(diagnostics)
    .fetch_one(pool)
    .await?;

    Ok(row.try_get("id")?)
}

pub async fn insert_snapshot_gc_run_items(
    pool: &PgPool,
    run_id: Uuid,
    candidates: &[SnapshotGcCandidate],
    action_status: &str,
) -> anyhow::Result<()> {
    for candidate in candidates {
        sqlx::query(
            r"
            INSERT INTO public.lca_snapshot_gc_run_items (
              run_id,
              candidate_type,
              snapshot_id,
              bucket_id,
              object_name,
              storage_bytes,
              reason,
              delete_db_snapshot,
              action_status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ",
        )
        .bind(run_id)
        .bind(&candidate.candidate_type)
        .bind(candidate.snapshot_id)
        .bind(&candidate.bucket_id)
        .bind(&candidate.object_name)
        .bind(candidate.storage_bytes)
        .bind(&candidate.reason)
        .bind(candidate.delete_db_snapshot)
        .bind(action_status)
        .execute(pool)
        .await?;
    }

    Ok(())
}

pub async fn update_snapshot_gc_run_item_status(
    pool: &PgPool,
    run_id: Uuid,
    object_name: &str,
    action_status: &str,
    error_message: Option<&str>,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r"
        UPDATE public.lca_snapshot_gc_run_items
        SET action_status = $3,
            error_message = $4,
            updated_at = NOW()
        WHERE run_id = $1
          AND object_name = $2
        ",
    )
    .bind(run_id)
    .bind(object_name)
    .bind(action_status)
    .bind(error_message)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

pub async fn finish_snapshot_gc_run(
    pool: &PgPool,
    run_id: Uuid,
    status: &str,
    totals: SnapshotGcRunTotals,
    diagnostics: Value,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r"
        UPDATE public.lca_snapshot_gc_runs
        SET status = $2,
            finished_at = NOW(),
            storage_deleted_count = $3,
            storage_failed_count = $4,
            db_snapshot_deleted_count = $5,
            diagnostics = COALESCE(diagnostics, '{}'::jsonb) || $6::jsonb
        WHERE id = $1
        ",
    )
    .bind(run_id)
    .bind(status)
    .bind(totals.storage_deleted_count)
    .bind(totals.storage_failed_count)
    .bind(totals.db_snapshot_deleted_count)
    .bind(diagnostics)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

pub async fn is_snapshot_active(pool: &PgPool, snapshot_id: Uuid) -> anyhow::Result<bool> {
    let active = sqlx::query_scalar::<bool>(
        r"
        SELECT EXISTS (
          SELECT 1
          FROM public.lca_active_snapshots
          WHERE snapshot_id = $1
        )
        ",
    )
    .bind(snapshot_id)
    .fetch_one(pool)
    .await?;

    Ok(active)
}

pub async fn delete_snapshot_row_if_inactive(
    pool: &PgPool,
    snapshot_id: Uuid,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r"
        DELETE FROM public.lca_network_snapshots AS snapshots
        WHERE snapshots.id = $1
          AND NOT EXISTS (
            SELECT 1
            FROM public.lca_active_snapshots AS active
            WHERE active.snapshot_id = snapshots.id
          )
        ",
    )
    .bind(snapshot_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

fn count_directories(
    candidates: &[SnapshotGcCandidate],
    candidate_type: &str,
) -> anyhow::Result<i32> {
    let count = group_candidates_by_directory(candidates)
        .into_iter()
        .filter(|directory| directory.candidate_type == candidate_type)
        .count();
    usize_to_i32(count, "candidate directory count")
}

fn usize_to_i32(value: usize, name: &str) -> anyhow::Result<i32> {
    i32::try_from(value).map_err(|_| anyhow::anyhow!("{name} exceeds i32::MAX"))
}

fn sum_candidate_storage_bytes(candidates: &[SnapshotGcCandidate]) -> anyhow::Result<i64> {
    candidates.iter().try_fold(0_i64, |acc, candidate| {
        acc.checked_add(candidate.storage_bytes.max(0))
            .ok_or_else(|| anyhow::anyhow!("candidate storage bytes overflow"))
    })
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{
        ObjectDeleteStatus, SnapshotGcCandidate, group_candidates_by_directory,
        object_delete_succeeded, should_delete_db_snapshot, validate_snapshot_gc_policy,
    };

    #[test]
    fn groups_candidates_by_snapshot_directory() {
        let snapshot_id =
            Uuid::parse_str("91540000-0000-4000-8000-000000000001").expect("valid fixture uuid");
        let candidates = vec![
            candidate(
                "snapshot_directory",
                Some(snapshot_id),
                "91540000-0000-4000-8000-000000000001",
                "a.json",
                true,
            ),
            candidate(
                "snapshot_directory",
                Some(snapshot_id),
                "91540000-0000-4000-8000-000000000001",
                "b.json",
                true,
            ),
            candidate(
                "orphan_storage_directory",
                None,
                "91540000-0000-4000-8000-000000000002",
                "c.json",
                false,
            ),
        ];

        let grouped = group_candidates_by_directory(&candidates);

        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0].objects.len(), 1);
        assert_eq!(grouped[1].objects.len(), 2);
        assert!(grouped[1].delete_db_snapshot);
    }

    #[test]
    fn partial_object_delete_failure_blocks_db_snapshot_delete() {
        let snapshot_id =
            Uuid::parse_str("91540000-0000-4000-8000-000000000001").expect("valid fixture uuid");
        let candidates = vec![
            candidate(
                "snapshot_directory",
                Some(snapshot_id),
                "91540000-0000-4000-8000-000000000001",
                "a.json",
                true,
            ),
            candidate(
                "snapshot_directory",
                Some(snapshot_id),
                "91540000-0000-4000-8000-000000000001",
                "b.json",
                true,
            ),
        ];
        let directory = group_candidates_by_directory(&candidates)
            .pop()
            .expect("one directory");

        assert!(!should_delete_db_snapshot(
            &directory,
            &[ObjectDeleteStatus::Deleted, ObjectDeleteStatus::Failed]
        ));
        assert!(should_delete_db_snapshot(
            &directory,
            &[ObjectDeleteStatus::Deleted, ObjectDeleteStatus::Missing]
        ));
    }

    #[test]
    fn object_missing_counts_as_success_for_retry_semantics() {
        assert!(object_delete_succeeded(ObjectDeleteStatus::Deleted));
        assert!(object_delete_succeeded(ObjectDeleteStatus::Missing));
        assert!(!object_delete_succeeded(ObjectDeleteStatus::Failed));
    }

    #[test]
    fn validates_policy_bounds() {
        assert!(validate_snapshot_gc_policy(super::SnapshotGcPolicy::default()).is_ok());
        assert!(
            validate_snapshot_gc_policy(super::SnapshotGcPolicy {
                snapshot_retention_days: 0,
                ..super::SnapshotGcPolicy::default()
            })
            .is_err()
        );
        assert!(
            validate_snapshot_gc_policy(super::SnapshotGcPolicy {
                max_bytes: 0,
                ..super::SnapshotGcPolicy::default()
            })
            .is_err()
        );
    }

    fn candidate(
        candidate_type: &str,
        snapshot_id: Option<Uuid>,
        snapshot_directory: &str,
        object_name: &str,
        delete_db_snapshot: bool,
    ) -> SnapshotGcCandidate {
        SnapshotGcCandidate {
            candidate_type: candidate_type.to_owned(),
            snapshot_id,
            snapshot_directory: snapshot_directory.to_owned(),
            bucket_id: "lca_results".to_owned(),
            object_name: format!("lca-results/snapshots/{snapshot_directory}/{object_name}"),
            storage_bytes: 10,
            reason: "eligible_default_30d_snapshot".to_owned(),
            delete_db_snapshot,
            object_count: 2,
            snapshot_storage_bytes: 20,
            downstream_job_count: 0,
            downstream_result_count: 0,
            downstream_cache_count: 0,
            downstream_latest_count: 0,
            downstream_factorization_count: 0,
            downstream_artifact_count: 0,
        }
    }
}
