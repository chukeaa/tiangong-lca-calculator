use clap::Parser;
use solver_worker::pgbouncer_sqlx::{self as sqlx, Row, postgres::PgPoolOptions};
use solver_worker::storage::ObjectStoreClient;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "result-gc")]
struct Cli {
    /// `PostgreSQL` URL (preferred env: `DATABASE_URL`, fallback: `CONN`).
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    /// `PostgreSQL` URL fallback used by this project in local `.env`.
    #[arg(long, env = "CONN")]
    conn: Option<String>,
    /// S3-compatible endpoint for result artifacts.
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
    #[arg(long, env = "GC_BATCH_SIZE", default_value_t = 200_i64)]
    batch_size: i64,
    /// Optional hard cap on total processed batches.
    #[arg(long, env = "GC_MAX_BATCHES")]
    max_batches: Option<i64>,
    /// Dry-run mode: list candidates only, do not delete S3/DB rows.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Clone)]
struct GcCandidate {
    result_id: Uuid,
    artifact_url: String,
}

#[derive(Debug, Default)]
struct GcTotals {
    total_candidates: u64,
    total_db_deleted: u64,
    total_s3_deleted: u64,
    total_s3_failed: u64,
    batches: i64,
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

async fn run_gc(
    pool: &sqlx::PgPool,
    store: &ObjectStoreClient,
    cli: &Cli,
) -> anyhow::Result<GcTotals> {
    let mut totals = GcTotals::default();
    loop {
        if let Some(limit) = cli.max_batches
            && totals.batches >= limit
        {
            break;
        }
        let candidates = fetch_gc_candidates(pool, cli.batch_size).await?;
        if candidates.is_empty() {
            break;
        }
        totals.batches += 1;
        totals.total_candidates += u64::try_from(candidates.len())
            .map_err(|_| anyhow::anyhow!("candidate count overflow"))?;

        if cli.dry_run {
            for c in &candidates {
                println!(
                    "[dry-run] candidate result_id={} url={}",
                    c.result_id, c.artifact_url
                );
            }
            continue;
        }

        let mut deletable_ids = Vec::with_capacity(candidates.len());
        for c in candidates {
            match store.delete_object_url(&c.artifact_url).await {
                Ok(()) => {
                    deletable_ids.push(c.result_id);
                    totals.total_s3_deleted += 1;
                }
                Err(err) => {
                    totals.total_s3_failed += 1;
                    eprintln!(
                        "[warn] delete object failed result_id={} url={} error={err}",
                        c.result_id, c.artifact_url
                    );
                }
            }
        }

        if deletable_ids.is_empty() {
            eprintln!(
                "[warn] no DB rows deleted in batch={} because all S3 deletes failed; stop loop",
                totals.batches
            );
            break;
        }

        let deleted = delete_results_by_ids(pool, deletable_ids).await?;
        totals.total_db_deleted += deleted;
        println!("[info] batch={} db_deleted={deleted}", totals.batches);
    }

    Ok(totals)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let db_url = resolve_db_url(&cli)?;
    if cli.batch_size <= 0 {
        return Err(anyhow::anyhow!("GC_BATCH_SIZE must be > 0"));
    }

    let endpoint = required(cli.s3_endpoint.as_deref(), "S3_ENDPOINT")?;
    let region = required(cli.s3_region.as_deref(), "S3_REGION")?;
    let bucket = required(cli.s3_bucket.as_deref(), "S3_BUCKET")?;
    let access_key = required(cli.s3_access_key_id.as_deref(), "S3_ACCESS_KEY_ID")?;
    let secret = required(cli.s3_secret_access_key.as_deref(), "S3_SECRET_ACCESS_KEY")?;

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(db_url)
        .await?;
    let store = ObjectStoreClient::new(
        endpoint,
        region,
        bucket,
        &cli.s3_prefix,
        access_key,
        secret,
        cli.s3_session_token.clone(),
    )?;

    let totals = run_gc(&pool, &store, &cli).await?;

    println!(
        "[summary] dry_run={} batches={} candidates={} s3_deleted={} s3_failed={} db_deleted={}",
        cli.dry_run,
        totals.batches,
        totals.total_candidates,
        totals.total_s3_deleted,
        totals.total_s3_failed,
        totals.total_db_deleted
    );

    Ok(())
}

async fn fetch_gc_candidates(
    pool: &sqlx::PgPool,
    batch_size: i64,
) -> anyhow::Result<Vec<GcCandidate>> {
    let rows = sqlx::query(
        r"
        WITH ranked AS (
          SELECT
            r.id AS result_id,
            r.artifact_url,
            r.created_at,
            r.expires_at,
            r.is_pinned,
            ROW_NUMBER() OVER (
              PARTITION BY j.requested_by, j.snapshot_id, COALESCE(j.request_key, j.id::text)
              ORDER BY r.created_at DESC, r.id DESC
            ) AS rn,
            rc.result_id AS active_cache_result_id
          FROM public.lca_results AS r
          JOIN public.lca_jobs AS j
            ON j.id = r.job_id
          LEFT JOIN public.lca_result_cache AS rc
            ON rc.result_id = r.id
           AND rc.status IN ('pending', 'running', 'ready')
        )
        SELECT result_id, artifact_url
        FROM ranked
        WHERE expires_at < now()
          AND is_pinned = false
          AND active_cache_result_id IS NULL
          AND rn > 1
        ORDER BY created_at ASC
        LIMIT $1
        ",
    )
    .bind(batch_size)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(GcCandidate {
                result_id: r.try_get::<Uuid, _>("result_id")?,
                artifact_url: r.try_get::<String, _>("artifact_url")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(Into::into)
}

async fn delete_results_by_ids(pool: &sqlx::PgPool, ids: Vec<Uuid>) -> anyhow::Result<u64> {
    let result = sqlx::query("DELETE FROM public.lca_results WHERE id = ANY($1::uuid[])")
        .bind(ids)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}
