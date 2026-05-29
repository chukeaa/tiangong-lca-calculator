use clap::Parser;
use solver_worker::{
    package_retention::{
        PackageArtifactGcCandidate, delete_package_jobs_after_object_gc,
        delete_stale_package_request_cache_rows, fetch_package_artifact_gc_candidates,
        fetch_package_retention_summary, mark_package_artifact_deleted,
        record_package_artifact_gc_error, validate_retention_days,
    },
    pgbouncer_sqlx::{self as sqlx, postgres::PgPoolOptions},
    storage::ObjectStoreClient,
};

#[derive(Debug, Parser)]
#[command(name = "package-gc")]
struct Cli {
    /// `PostgreSQL` URL (preferred env: `DATABASE_URL`, fallback: `CONN`).
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    /// `PostgreSQL` URL fallback used by this project in local `.env`.
    #[arg(long, env = "CONN")]
    conn: Option<String>,
    /// S3-compatible endpoint for package artifacts.
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
    /// Max rows per GC batch.
    #[arg(long, env = "PACKAGE_GC_BATCH_SIZE", default_value_t = 100_i64)]
    batch_size: i64,
    /// Optional hard cap on total processed batches.
    #[arg(long, env = "PACKAGE_GC_MAX_BATCHES")]
    max_batches: Option<i64>,
    /// Terminal package job metadata retention window.
    #[arg(long, env = "PACKAGE_GC_JOB_RETENTION_DAYS", default_value_t = 30_i32)]
    job_retention_days: i32,
    /// Request-cache recent-access protection window.
    #[arg(
        long,
        env = "PACKAGE_GC_REQUEST_CACHE_RETENTION_DAYS",
        default_value_t = 30_i32
    )]
    request_cache_retention_days: i32,
    /// Execute destructive object/metadata cleanup. Omit for dry-run only.
    #[arg(long)]
    execute: bool,
}

#[derive(Debug, Default)]
struct PackageGcTotals {
    batches: i64,
    artifact_candidates: u64,
    object_deleted: u64,
    object_delete_failed: u64,
    artifacts_marked_deleted: u64,
    request_cache_deleted: u64,
    jobs_deleted: u64,
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

async fn acquire_package_gc_lock(
    pool: &sqlx::PgPool,
) -> anyhow::Result<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let mut conn = pool.acquire().await?;
    let acquired = sqlx::query_scalar::<bool>(
        "SELECT pg_try_advisory_lock(hashtext('solver_worker_package_gc'))",
    )
    .fetch_one(&mut *conn)
    .await?;

    if !acquired {
        return Err(anyhow::anyhow!(
            "another package-gc run holds the advisory lock"
        ));
    }

    Ok(conn)
}

async fn release_package_gc_lock(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
) -> anyhow::Result<()> {
    let _ = sqlx::query_scalar::<bool>(
        "SELECT pg_advisory_unlock(hashtext('solver_worker_package_gc'))",
    )
    .fetch_one(&mut **conn)
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if cli.batch_size <= 0 {
        return Err(anyhow::anyhow!("PACKAGE_GC_BATCH_SIZE must be > 0"));
    }
    if let Some(max_batches) = cli.max_batches
        && max_batches <= 0
    {
        return Err(anyhow::anyhow!("PACKAGE_GC_MAX_BATCHES must be > 0"));
    }
    let job_retention_days =
        validate_retention_days(cli.job_retention_days, "PACKAGE_GC_JOB_RETENTION_DAYS")?;
    let request_cache_retention_days = validate_retention_days(
        cli.request_cache_retention_days,
        "PACKAGE_GC_REQUEST_CACHE_RETENTION_DAYS",
    )?;

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

    let mut lock_conn = if cli.execute {
        Some(acquire_package_gc_lock(&pool).await?)
    } else {
        None
    };

    let result = run_package_gc(
        &pool,
        store.as_ref(),
        &cli,
        job_retention_days,
        request_cache_retention_days,
    )
    .await;

    let unlock_result = if let Some(conn) = lock_conn.as_mut() {
        release_package_gc_lock(conn).await
    } else {
        Ok(())
    };

    let totals = result?;
    unlock_result?;

    println!(
        "[summary] dry_run={} batches={} artifact_candidates={} object_deleted={} object_delete_failed={} artifacts_marked_deleted={} request_cache_deleted={} jobs_deleted={}",
        !cli.execute,
        totals.batches,
        totals.artifact_candidates,
        totals.object_deleted,
        totals.object_delete_failed,
        totals.artifacts_marked_deleted,
        totals.request_cache_deleted,
        totals.jobs_deleted
    );

    Ok(())
}

async fn run_package_gc(
    pool: &sqlx::PgPool,
    store: Option<&ObjectStoreClient>,
    cli: &Cli,
    job_retention_days: i32,
    request_cache_retention_days: i32,
) -> anyhow::Result<PackageGcTotals> {
    let summary =
        fetch_package_retention_summary(pool, job_retention_days, request_cache_retention_days)
            .await?;
    for row in summary {
        println!(
            "[retention] area={} action={} eligible={} reason={} rows={} artifact_bytes={} hits={}",
            row.retention_area,
            row.retention_action,
            row.is_eligible,
            row.reason,
            row.row_count,
            row.total_artifact_bytes,
            row.total_hit_count
        );
    }

    if !cli.execute {
        let candidates = fetch_package_artifact_gc_candidates(
            pool,
            cli.batch_size,
            request_cache_retention_days,
        )
        .await?;
        print_dry_run_candidates(candidates.as_slice());
        return Ok(PackageGcTotals {
            artifact_candidates: u64::try_from(candidates.len())
                .map_err(|_| anyhow::anyhow!("candidate count overflow"))?,
            ..PackageGcTotals::default()
        });
    }

    let store = store.ok_or_else(|| anyhow::anyhow!("object store is required for --execute"))?;
    let mut totals = PackageGcTotals::default();
    loop {
        if let Some(max_batches) = cli.max_batches
            && totals.batches >= max_batches
        {
            break;
        }

        let candidates = fetch_package_artifact_gc_candidates(
            pool,
            cli.batch_size,
            request_cache_retention_days,
        )
        .await?;
        let candidate_count = u64::try_from(candidates.len())
            .map_err(|_| anyhow::anyhow!("candidate count overflow"))?;

        for candidate in candidates {
            process_artifact_candidate(pool, store, &candidate, &mut totals).await?;
        }

        let request_cache_deleted = delete_stale_package_request_cache_rows(
            pool,
            cli.batch_size,
            request_cache_retention_days,
        )
        .await?;
        let jobs_deleted = delete_package_jobs_after_object_gc(
            pool,
            cli.batch_size,
            job_retention_days,
            request_cache_retention_days,
        )
        .await?;

        totals.request_cache_deleted += request_cache_deleted;
        totals.jobs_deleted += jobs_deleted;

        if candidate_count == 0 && request_cache_deleted == 0 && jobs_deleted == 0 {
            break;
        }
        totals.batches += 1;
    }

    Ok(totals)
}

fn print_dry_run_candidates(candidates: &[PackageArtifactGcCandidate]) {
    for candidate in candidates {
        println!(
            "[dry-run] artifact_id={} job_id={} kind={} url={}",
            candidate.artifact_id,
            candidate.job_id,
            candidate.artifact_kind,
            candidate.artifact_url
        );
    }
}

async fn process_artifact_candidate(
    pool: &sqlx::PgPool,
    store: &ObjectStoreClient,
    candidate: &PackageArtifactGcCandidate,
    totals: &mut PackageGcTotals,
) -> anyhow::Result<()> {
    totals.artifact_candidates += 1;
    match store.delete_object_url(&candidate.artifact_url).await {
        Ok(()) => {
            totals.object_deleted += 1;
            totals.artifacts_marked_deleted +=
                mark_package_artifact_deleted(pool, candidate.artifact_id).await?;
            println!(
                "[info] deleted package artifact object artifact_id={} job_id={} kind={}",
                candidate.artifact_id, candidate.job_id, candidate.artifact_kind
            );
        }
        Err(err) => {
            totals.object_delete_failed += 1;
            let message = err.to_string();
            let _ = record_package_artifact_gc_error(pool, candidate.artifact_id, &message).await?;
            eprintln!(
                "[warn] package artifact object delete failed artifact_id={} job_id={} kind={} error={message}",
                candidate.artifact_id, candidate.job_id, candidate.artifact_kind
            );
        }
    }

    Ok(())
}
