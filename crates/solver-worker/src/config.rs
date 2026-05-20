use std::{net::SocketAddr, str::FromStr, time::Duration};

use clap::{Parser, ValueEnum};

/// Solver worker launch mode.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum RunMode {
    /// Only queue worker.
    Worker,
    /// Only internal HTTP server.
    Http,
    /// Run both components in one process.
    Both,
}

/// CLI + env config.
#[derive(Debug, Clone, Parser)]
#[command(name = "solver-worker")]
pub struct AppConfig {
    /// Launch mode.
    #[arg(long, env = "SOLVER_MODE", default_value = "both")]
    pub mode: RunMode,
    /// `PostgreSQL` URL (preferred env: `DATABASE_URL`, fallback: `CONN`).
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,
    /// `PostgreSQL` URL fallback used by this project in local `.env`.
    #[arg(long, env = "CONN")]
    pub conn: Option<String>,
    /// Optional queue-only `PostgreSQL` URL. Use this for transaction poolers
    /// such as Supabase 6543 while keeping main compute queries on `DATABASE_URL`.
    #[arg(long, env = "QUEUE_DATABASE_URL")]
    pub queue_database_url: Option<String>,
    /// Queue-only `PostgreSQL` URL fallback used by this project in local `.env`.
    #[arg(long, env = "QUEUE_CONN")]
    pub queue_conn: Option<String>,
    /// Queue name in pgmq.
    #[arg(long, env = "PGMQ_QUEUE", default_value = "lca_jobs")]
    pub pgmq_queue: String,
    /// Poll interval for queue worker (ms).
    #[arg(long, env = "WORKER_POLL_MS", default_value_t = 1_000_u64)]
    pub worker_poll_ms: u64,
    /// Message visibility timeout for pgmq.read.
    #[arg(long, env = "WORKER_VT_SECONDS", default_value_t = 30_i32)]
    pub worker_vt_seconds: i32,
    /// Maximum number of DB connections held by the worker process.
    #[arg(long, env = "DB_MAX_CONNECTIONS", default_value_t = 8_u32)]
    pub db_max_connections: u32,
    /// Minimum number of DB connections kept by the worker process.
    #[arg(long, env = "DB_MIN_CONNECTIONS", default_value_t = 1_u32)]
    pub db_min_connections: u32,
    /// DB connection acquire timeout for the worker process.
    #[arg(long, env = "DB_ACQUIRE_TIMEOUT_SECONDS", default_value_t = 30_u64)]
    pub db_acquire_timeout_seconds: u64,
    /// Maximum number of DB connections held by the queue polling pool.
    #[arg(long, env = "QUEUE_DB_MAX_CONNECTIONS", default_value_t = 2_u32)]
    pub queue_db_max_connections: u32,
    /// Minimum number of DB connections kept by the queue polling pool.
    #[arg(long, env = "QUEUE_DB_MIN_CONNECTIONS", default_value_t = 0_u32)]
    pub queue_db_min_connections: u32,
    /// DB connection acquire timeout for the queue polling pool.
    #[arg(
        long,
        env = "QUEUE_DB_ACQUIRE_TIMEOUT_SECONDS",
        default_value_t = 30_u64
    )]
    pub queue_db_acquire_timeout_seconds: u64,
    /// Maximum number of concurrent `build_snapshot` jobs across worker instances.
    #[arg(long, env = "BUILD_SNAPSHOT_MAX_CONCURRENCY", default_value_t = 1_u32)]
    pub build_snapshot_max_concurrency: u32,
    /// Poll interval while waiting for a `build_snapshot` concurrency slot.
    #[arg(long, env = "BUILD_SNAPSHOT_LOCK_POLL_MS", default_value_t = 5_000_u64)]
    pub build_snapshot_lock_poll_ms: u64,
    /// Internal HTTP bind address.
    #[arg(long, env = "HTTP_ADDR", default_value = "0.0.0.0:8080")]
    pub http_addr: String,
    /// S3-compatible endpoint for large result artifacts.
    #[arg(long, env = "S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,
    /// S3 region.
    #[arg(long, env = "S3_REGION")]
    pub s3_region: Option<String>,
    /// S3 bucket.
    #[arg(long, env = "S3_BUCKET")]
    pub s3_bucket: Option<String>,
    /// S3 access key id for `SigV4` authenticated uploads.
    #[arg(long, env = "S3_ACCESS_KEY_ID")]
    pub s3_access_key_id: Option<String>,
    /// S3 secret access key for `SigV4` authenticated uploads.
    #[arg(long, env = "S3_SECRET_ACCESS_KEY")]
    pub s3_secret_access_key: Option<String>,
    /// Optional S3 session token for temporary credentials.
    #[arg(long, env = "S3_SESSION_TOKEN")]
    pub s3_session_token: Option<String>,
    /// Object key prefix under the bucket.
    #[arg(long, env = "S3_PREFIX", default_value = "lca-results")]
    pub s3_prefix: String,
}

impl AppConfig {
    /// Returns resolved database URL from `DATABASE_URL` or `CONN`.
    pub fn resolved_database_url(&self) -> anyhow::Result<&str> {
        self.database_url
            .as_deref()
            .or(self.conn.as_deref())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "missing database URL: set DATABASE_URL or CONN environment variable"
                )
            })
    }

    /// Returns queue-only database URL from `QUEUE_DATABASE_URL` / `QUEUE_CONN`,
    /// falling back to the main runtime database URL.
    pub fn resolved_queue_database_url(&self) -> anyhow::Result<&str> {
        self.queue_database_url
            .as_deref()
            .or(self.queue_conn.as_deref())
            .map_or_else(|| self.resolved_database_url(), Ok)
    }

    /// Returns whether queue DB URL was explicitly configured.
    #[must_use]
    pub fn has_explicit_queue_database_url(&self) -> bool {
        self.queue_database_url.is_some() || self.queue_conn.is_some()
    }

    /// Poll interval as Duration.
    #[must_use]
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.worker_poll_ms)
    }

    /// Sanitized maximum DB connections for the worker process.
    #[must_use]
    pub fn db_max_connections(&self) -> u32 {
        self.db_max_connections.max(1)
    }

    /// Sanitized minimum DB connections for the worker process.
    #[must_use]
    pub fn db_min_connections(&self) -> u32 {
        self.db_min_connections.min(self.db_max_connections())
    }

    /// DB connection acquire timeout.
    #[must_use]
    pub fn db_acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.db_acquire_timeout_seconds.max(1))
    }

    /// Sanitized maximum DB connections for the queue polling pool.
    #[must_use]
    pub fn queue_db_max_connections(&self) -> u32 {
        self.queue_db_max_connections.max(1)
    }

    /// Sanitized minimum DB connections for the queue polling pool.
    #[must_use]
    pub fn queue_db_min_connections(&self) -> u32 {
        self.queue_db_min_connections
            .min(self.queue_db_max_connections())
    }

    /// Queue DB connection acquire timeout.
    #[must_use]
    pub fn queue_db_acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.queue_db_acquire_timeout_seconds.max(1))
    }

    /// Sanitized maximum `build_snapshot` concurrency.
    #[must_use]
    pub fn build_snapshot_max_concurrency(&self) -> u32 {
        self.build_snapshot_max_concurrency.max(1)
    }

    /// Poll interval used when all `build_snapshot` concurrency slots are busy.
    #[must_use]
    pub fn build_snapshot_lock_poll_interval(&self) -> Duration {
        Duration::from_millis(self.build_snapshot_lock_poll_ms.max(100))
    }

    /// Parsed http socket addr.
    pub fn http_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        SocketAddr::from_str(&self.http_addr)
            .map_err(|err| anyhow::anyhow!("invalid HTTP_ADDR {}: {err}", self.http_addr))
    }
}

#[cfg(test)]
mod tests {
    use super::AppConfig;
    use clap::Parser;
    use std::time::Duration;

    #[test]
    fn db_and_build_snapshot_config_defaults_match_previous_limits() {
        let config = AppConfig::parse_from([
            "solver-worker",
            "--database-url",
            "postgres://example.local/app",
        ]);

        assert_eq!(config.db_max_connections(), 8);
        assert_eq!(config.db_min_connections(), 1);
        assert_eq!(config.db_acquire_timeout(), Duration::from_secs(30));
        assert_eq!(
            config.resolved_queue_database_url().unwrap(),
            "postgres://example.local/app"
        );
        assert!(!config.has_explicit_queue_database_url());
        assert_eq!(config.queue_db_max_connections(), 2);
        assert_eq!(config.queue_db_min_connections(), 0);
        assert_eq!(config.queue_db_acquire_timeout(), Duration::from_secs(30));
        assert_eq!(config.build_snapshot_max_concurrency(), 1);
        assert_eq!(
            config.build_snapshot_lock_poll_interval(),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn db_and_build_snapshot_config_clamps_invalid_low_values() {
        let config = AppConfig::parse_from([
            "solver-worker",
            "--database-url",
            "postgres://example.local/app",
            "--db-max-connections",
            "0",
            "--db-min-connections",
            "4",
            "--db-acquire-timeout-seconds",
            "0",
            "--queue-db-max-connections",
            "0",
            "--queue-db-min-connections",
            "4",
            "--queue-db-acquire-timeout-seconds",
            "0",
            "--build-snapshot-max-concurrency",
            "0",
            "--build-snapshot-lock-poll-ms",
            "1",
        ]);

        assert_eq!(config.db_max_connections(), 1);
        assert_eq!(config.db_min_connections(), 1);
        assert_eq!(config.db_acquire_timeout(), Duration::from_secs(1));
        assert_eq!(config.queue_db_max_connections(), 1);
        assert_eq!(config.queue_db_min_connections(), 1);
        assert_eq!(config.queue_db_acquire_timeout(), Duration::from_secs(1));
        assert_eq!(config.build_snapshot_max_concurrency(), 1);
        assert_eq!(
            config.build_snapshot_lock_poll_interval(),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn queue_database_url_overrides_main_database_url() {
        let config = AppConfig::parse_from([
            "solver-worker",
            "--database-url",
            "postgres://example.local/app",
            "--queue-database-url",
            "postgres://pooler.example.local/app",
        ]);

        assert_eq!(
            config.resolved_queue_database_url().unwrap(),
            "postgres://pooler.example.local/app"
        );
        assert!(config.has_explicit_queue_database_url());
    }
}
