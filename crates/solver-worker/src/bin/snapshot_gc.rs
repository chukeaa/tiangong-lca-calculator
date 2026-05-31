use clap::Parser;
use serde_json::json;
use solver_worker::{
    pgbouncer_sqlx::{self as sqlx, postgres::PgPoolOptions},
    snapshot_retention::{
        DEFAULT_BATCH_SIZE, DEFAULT_MAX_BYTES, DEFAULT_MAX_ORPHAN_DIRS, DEFAULT_MAX_SNAPSHOTS,
        DEFAULT_ORPHAN_RETENTION_DAYS, DEFAULT_SNAPSHOT_RETENTION_DAYS, ObjectDeleteStatus,
        SnapshotGcDirectory, SnapshotGcPolicy, SnapshotGcRunTotals, create_snapshot_gc_run,
        delete_snapshot_row_if_inactive, fetch_snapshot_gc_candidates,
        fetch_snapshot_retention_summary, finish_snapshot_gc_run, group_candidates_by_directory,
        insert_snapshot_gc_run_items, is_snapshot_active, should_delete_db_snapshot,
        update_snapshot_gc_run_item_status, validate_positive_i32, validate_snapshot_gc_policy,
    },
    storage::{ObjectDeleteOutcome, ObjectStoreClient},
};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "snapshot-gc")]
struct Cli {
    /// `PostgreSQL` URL (preferred env: `DATABASE_URL`, fallback: `CONN`).
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    /// `PostgreSQL` URL fallback used by this project in local `.env`.
    #[arg(long, env = "CONN")]
    conn: Option<String>,
    /// S3-compatible endpoint for snapshot artifacts.
    #[arg(long, env = "S3_ENDPOINT")]
    s3_endpoint: Option<String>,
    /// S3 region.
    #[arg(long, env = "S3_REGION")]
    s3_region: Option<String>,
    /// S3 bucket.
    #[arg(long, env = "S3_BUCKET")]
    s3_bucket: Option<String>,
    /// S3 access key id.
    #[arg(long, env = "S3_ACCESS_KEY_ID")]
    s3_access_key_id: Option<String>,
    /// S3 secret access key.
    #[arg(long, env = "S3_SECRET_ACCESS_KEY")]
    s3_secret_access_key: Option<String>,
    /// Optional S3 session token.
    #[arg(long, env = "S3_SESSION_TOKEN")]
    s3_session_token: Option<String>,
    /// Object key prefix.
    #[arg(long, env = "S3_PREFIX", default_value = "lca-results")]
    s3_prefix: String,
    /// Default retention window for non-active snapshots without TTL metadata.
    #[arg(
        long,
        env = "SNAPSHOT_GC_SNAPSHOT_RETENTION_DAYS",
        default_value_t = DEFAULT_SNAPSHOT_RETENTION_DAYS
    )]
    snapshot_retention_days: i32,
    /// Retention window for orphan snapshot storage directories.
    #[arg(
        long,
        env = "SNAPSHOT_GC_ORPHAN_RETENTION_DAYS",
        default_value_t = DEFAULT_ORPHAN_RETENTION_DAYS
    )]
    orphan_retention_days: i32,
    /// Max non-active DB snapshot directories per run.
    #[arg(
        long,
        env = "SNAPSHOT_GC_MAX_SNAPSHOTS",
        default_value_t = DEFAULT_MAX_SNAPSHOTS
    )]
    max_snapshots: i32,
    /// Max orphan storage directories per run.
    #[arg(
        long,
        env = "SNAPSHOT_GC_MAX_ORPHAN_DIRS",
        default_value_t = DEFAULT_MAX_ORPHAN_DIRS
    )]
    max_orphan_dirs: i32,
    /// Max storage bytes per run.
    #[arg(long, env = "SNAPSHOT_GC_MAX_BYTES", default_value_t = DEFAULT_MAX_BYTES)]
    max_bytes: i64,
    /// Object processing chunk size.
    #[arg(
        long,
        env = "SNAPSHOT_GC_BATCH_SIZE",
        default_value_t = DEFAULT_BATCH_SIZE
    )]
    batch_size: usize,
    /// Execute destructive object/metadata cleanup. Omit for dry-run only.
    #[arg(long)]
    execute: bool,
}

#[derive(Debug, Default)]
struct SnapshotGcExecutionTotals {
    candidate_objects: usize,
    candidate_directories: usize,
    storage_missing_count: i32,
    skipped_active_directories: i32,
    run_totals: SnapshotGcRunTotals,
}

fn required<'a>(value: Option<&'a str>, name: &str) -> anyhow::Result<&'a str> {
    value.ok_or_else(|| anyhow::anyhow!("missing {name}"))
}

fn resolve_db_url(cli: &Cli) -> anyhow::Result<&str> {
    cli.database_url
        .as_deref()
        .or(cli.conn.as_deref())
        .ok_or_else(|| anyhow::anyhow!("missing DB connection: set DATABASE_URL or CONN"))
}

fn build_policy(cli: &Cli) -> anyhow::Result<SnapshotGcPolicy> {
    validate_snapshot_gc_policy(SnapshotGcPolicy {
        snapshot_retention_days: cli.snapshot_retention_days,
        orphan_retention_days: cli.orphan_retention_days,
        max_snapshots: cli.max_snapshots,
        max_orphan_dirs: cli.max_orphan_dirs,
        max_bytes: cli.max_bytes,
    })
}

fn validate_batch_size(value: usize) -> anyhow::Result<usize> {
    if value == 0 {
        return Err(anyhow::anyhow!("SNAPSHOT_GC_BATCH_SIZE must be > 0"));
    }
    Ok(value)
}

fn build_object_store(cli: &Cli) -> anyhow::Result<ObjectStoreClient> {
    let endpoint = required(cli.s3_endpoint.as_deref(), "S3_ENDPOINT")?;
    let region = required(cli.s3_region.as_deref(), "S3_REGION")?;
    let bucket = required(cli.s3_bucket.as_deref(), "S3_BUCKET")?;
    let access_key = required(cli.s3_access_key_id.as_deref(), "S3_ACCESS_KEY_ID")?;
    let secret = required(cli.s3_secret_access_key.as_deref(), "S3_SECRET_ACCESS_KEY")?;

    ObjectStoreClient::new(
        endpoint,
        region,
        bucket,
        &cli.s3_prefix,
        access_key,
        secret,
        cli.s3_session_token.clone(),
    )
}

async fn try_acquire_snapshot_gc_lock(
    pool: &sqlx::PgPool,
) -> anyhow::Result<Option<sqlx::pool::PoolConnection<sqlx::Postgres>>> {
    let mut conn = pool.acquire().await?;
    let acquired = sqlx::query_scalar::<bool>(
        "SELECT pg_try_advisory_lock(hashtext('solver_worker_snapshot_gc'))",
    )
    .fetch_one(&mut *conn)
    .await?;

    if acquired { Ok(Some(conn)) } else { Ok(None) }
}

async fn release_snapshot_gc_lock(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
) -> anyhow::Result<()> {
    let _ = sqlx::query_scalar::<bool>(
        "SELECT pg_advisory_unlock(hashtext('solver_worker_snapshot_gc'))",
    )
    .fetch_one(&mut **conn)
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let policy = build_policy(&cli)?;
    let batch_size = validate_batch_size(cli.batch_size)?;
    validate_positive_i32(cli.max_snapshots, "SNAPSHOT_GC_MAX_SNAPSHOTS")?;
    validate_positive_i32(cli.max_orphan_dirs, "SNAPSHOT_GC_MAX_ORPHAN_DIRS")?;

    let db_url = resolve_db_url(&cli)?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(db_url)
        .await?;
    let store = if cli.execute {
        Some(build_object_store(&cli)?)
    } else {
        None
    };
    let mode = if cli.execute { "execute" } else { "dry_run" };

    let Some(mut lock_conn) = try_acquire_snapshot_gc_lock(&pool).await? else {
        let run_id = create_snapshot_gc_run(
            &pool,
            mode,
            "skipped",
            policy,
            &[],
            json!({"reason": "advisory_lock_held"}),
        )
        .await?;
        println!(
            "[summary] run_id={run_id} dry_run={} status=skipped reason=advisory_lock_held",
            !cli.execute
        );
        return Ok(());
    };

    let result = run_snapshot_gc(&pool, store.as_ref(), &cli, policy, batch_size).await;
    let unlock_result = release_snapshot_gc_lock(&mut lock_conn).await;
    let totals = result?;
    unlock_result?;

    println!(
        "[summary] dry_run={} candidate_directories={} candidate_objects={} storage_deleted_or_missing={} storage_missing={} storage_failed={} db_snapshot_deleted={} skipped_active_directories={}",
        !cli.execute,
        totals.candidate_directories,
        totals.candidate_objects,
        totals.run_totals.storage_deleted_count,
        totals.storage_missing_count,
        totals.run_totals.storage_failed_count,
        totals.run_totals.db_snapshot_deleted_count,
        totals.skipped_active_directories
    );

    if totals.run_totals.storage_failed_count > 0 {
        return Err(anyhow::anyhow!(
            "snapshot GC finished with {} storage delete failures",
            totals.run_totals.storage_failed_count
        ));
    }

    Ok(())
}

async fn run_snapshot_gc(
    pool: &sqlx::PgPool,
    store: Option<&ObjectStoreClient>,
    cli: &Cli,
    policy: SnapshotGcPolicy,
    batch_size: usize,
) -> anyhow::Result<SnapshotGcExecutionTotals> {
    let summary = fetch_snapshot_retention_summary(pool, policy).await?;
    print_retention_summary(summary.as_slice());

    let candidates = fetch_snapshot_gc_candidates(pool, policy).await?;
    let directories = group_candidates_by_directory(candidates.as_slice());
    let run_id = create_snapshot_gc_run(
        pool,
        if cli.execute { "execute" } else { "dry_run" },
        "running",
        policy,
        candidates.as_slice(),
        json!({
            "contract": "util.list_lca_snapshot_gc_candidates",
            "execute": cli.execute
        }),
    )
    .await?;

    insert_snapshot_gc_run_items(
        pool,
        run_id,
        candidates.as_slice(),
        if cli.execute { "planned" } else { "dry_run" },
    )
    .await?;

    let mut totals = SnapshotGcExecutionTotals {
        candidate_objects: candidates.len(),
        candidate_directories: directories.len(),
        ..SnapshotGcExecutionTotals::default()
    };

    if !cli.execute {
        print_dry_run_candidates(directories.as_slice());
        finish_snapshot_gc_run(
            pool,
            run_id,
            "succeeded",
            totals.run_totals,
            json!({
                "storage_missing_count": totals.storage_missing_count,
                "skipped_active_directories": totals.skipped_active_directories
            }),
        )
        .await?;
        return Ok(totals);
    }

    let store = store.ok_or_else(|| anyhow::anyhow!("object store is required for --execute"))?;
    for directory_batch in directories.chunks(batch_size) {
        for directory in directory_batch {
            process_snapshot_gc_directory(pool, store, run_id, directory, batch_size, &mut totals)
                .await?;
        }
    }

    let run_status = if totals.run_totals.storage_failed_count > 0 {
        "failed"
    } else {
        "succeeded"
    };
    finish_snapshot_gc_run(
        pool,
        run_id,
        run_status,
        totals.run_totals,
        json!({
            "storage_missing_count": totals.storage_missing_count,
            "skipped_active_directories": totals.skipped_active_directories
        }),
    )
    .await?;

    Ok(totals)
}

fn print_retention_summary(
    summary: &[solver_worker::snapshot_retention::SnapshotRetentionSummaryRow],
) {
    for row in summary {
        println!(
            "[retention] area={} action={} eligible={} reason={} snapshots={} objects={} bytes={} downstream_jobs={} downstream_results={} downstream_cache={} downstream_latest={} downstream_factorization={} downstream_artifacts={}",
            row.retention_area,
            row.retention_action,
            row.is_eligible,
            row.reason,
            row.snapshot_count,
            row.object_count,
            row.total_storage_bytes,
            row.downstream_job_count,
            row.downstream_result_count,
            row.downstream_cache_count,
            row.downstream_latest_count,
            row.downstream_factorization_count,
            row.downstream_artifact_count
        );
    }
}

fn print_dry_run_candidates(directories: &[SnapshotGcDirectory]) {
    for directory in directories {
        println!(
            "[dry-run] type={} snapshot_id={} directory={} objects={} bytes={} reason={} delete_db_snapshot={}",
            directory.candidate_type,
            directory
                .snapshot_id
                .map_or_else(|| "null".to_owned(), |snapshot_id| snapshot_id.to_string()),
            directory.snapshot_directory,
            directory.objects.len(),
            directory.storage_bytes,
            directory.reason,
            directory.delete_db_snapshot
        );
        for object in &directory.objects {
            println!(
                "[dry-run-object] directory={} object={} bytes={}",
                directory.snapshot_directory, object.object_name, object.storage_bytes
            );
        }
    }
}

async fn process_snapshot_gc_directory(
    pool: &sqlx::PgPool,
    store: &ObjectStoreClient,
    run_id: Uuid,
    directory: &SnapshotGcDirectory,
    batch_size: usize,
    totals: &mut SnapshotGcExecutionTotals,
) -> anyhow::Result<()> {
    if directory.delete_db_snapshot {
        let snapshot_id = directory
            .snapshot_id
            .ok_or_else(|| anyhow::anyhow!("DB snapshot candidate missing snapshot_id"))?;
        if is_snapshot_active(pool, snapshot_id).await? {
            totals.skipped_active_directories += 1;
            for object in &directory.objects {
                update_snapshot_gc_run_item_status(
                    pool,
                    run_id,
                    &object.object_name,
                    "skipped",
                    Some("snapshot became active before GC"),
                )
                .await?;
            }
            eprintln!(
                "[warn] skip active snapshot directory={} snapshot_id={snapshot_id}",
                directory.snapshot_directory
            );
            return Ok(());
        }
    }

    let mut object_statuses = Vec::with_capacity(directory.objects.len());
    for object_batch in directory.objects.chunks(batch_size) {
        for object in object_batch {
            let status = delete_snapshot_object(pool, store, run_id, &object.object_name).await?;
            match status {
                ObjectDeleteStatus::Deleted => totals.run_totals.storage_deleted_count += 1,
                ObjectDeleteStatus::Missing => {
                    totals.run_totals.storage_deleted_count += 1;
                    totals.storage_missing_count += 1;
                }
                ObjectDeleteStatus::Failed => totals.run_totals.storage_failed_count += 1,
            }
            object_statuses.push(status);
        }
    }

    if should_delete_db_snapshot(directory, object_statuses.as_slice()) {
        let snapshot_id = directory
            .snapshot_id
            .ok_or_else(|| anyhow::anyhow!("DB snapshot candidate missing snapshot_id"))?;
        let deleted = delete_snapshot_row_if_inactive(pool, snapshot_id).await?;
        if deleted > 0 {
            totals.run_totals.db_snapshot_deleted_count += i32::try_from(deleted)
                .map_err(|_| anyhow::anyhow!("deleted snapshot row count exceeds i32::MAX"))?;
            for object in &directory.objects {
                update_snapshot_gc_run_item_status(
                    pool,
                    run_id,
                    &object.object_name,
                    "db_deleted",
                    None,
                )
                .await?;
            }
            println!(
                "[info] deleted snapshot row snapshot_id={snapshot_id} directory={} objects={}",
                directory.snapshot_directory,
                directory.objects.len()
            );
        } else {
            eprintln!(
                "[warn] snapshot row was not deleted snapshot_id={snapshot_id} directory={}",
                directory.snapshot_directory
            );
        }
    } else if directory.delete_db_snapshot {
        eprintln!(
            "[warn] keep DB snapshot row after object delete failure directory={} snapshot_id={}",
            directory.snapshot_directory,
            directory
                .snapshot_id
                .map_or_else(|| "null".to_owned(), |snapshot_id| snapshot_id.to_string())
        );
    }

    Ok(())
}

async fn delete_snapshot_object(
    pool: &sqlx::PgPool,
    store: &ObjectStoreClient,
    run_id: Uuid,
    object_name: &str,
) -> anyhow::Result<ObjectDeleteStatus> {
    match store.delete_object_key(object_name).await {
        Ok(ObjectDeleteOutcome::Deleted) => {
            update_snapshot_gc_run_item_status(pool, run_id, object_name, "storage_deleted", None)
                .await?;
            Ok(ObjectDeleteStatus::Deleted)
        }
        Ok(ObjectDeleteOutcome::Missing) => {
            update_snapshot_gc_run_item_status(pool, run_id, object_name, "storage_missing", None)
                .await?;
            Ok(ObjectDeleteStatus::Missing)
        }
        Err(err) => {
            let message = err.to_string();
            update_snapshot_gc_run_item_status(
                pool,
                run_id,
                object_name,
                "storage_failed",
                Some(&message),
            )
            .await?;
            eprintln!("[warn] snapshot object delete failed object={object_name} error={message}");
            Ok(ObjectDeleteStatus::Failed)
        }
    }
}
