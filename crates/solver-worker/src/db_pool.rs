use std::time::Duration;

use crate::pgbouncer_sqlx::{Executor, PgPool, postgres::PgPoolOptions};

pub const APP_SOLVER_WORKER: &str = "solver-worker";
pub const APP_SOLVER_WORKER_QUEUE: &str = "solver-worker-queue";
pub const APP_SNAPSHOT_BUILDER: &str = "snapshot-builder";
pub const APP_PACKAGE_WORKER: &str = "package-worker";
pub const APP_PACKAGE_WORKER_QUEUE: &str = "package-worker-queue";
pub const APP_REVIEW_SUBMIT_GATE_RUNNER: &str = "review-submit-gate-runner";
pub const APP_REVIEW_SUBMIT_GATE_RUNNER_QUEUE: &str = "review-submit-gate-runner-queue";
pub const APP_PACKAGE_GC: &str = "package-gc";
pub const APP_SNAPSHOT_GC: &str = "snapshot-gc";
pub const APP_RESULT_GC: &str = "result-gc";
pub const APP_MAINTENANCE_WORKER: &str = "maintenance-worker";
pub const APP_MAINTENANCE_ENQUEUE: &str = "maintenance-enqueue";

const POSTGRES_APPLICATION_NAME_MAX_BYTES: usize = 63;
const DEFAULT_MAX_CONNECTIONS: u32 = 4;
const DEFAULT_MIN_CONNECTIONS: u32 = 0;
const DEFAULT_ACQUIRE_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_IDLE_TIMEOUT_SECONDS: u64 = 5 * 60;
const DEFAULT_MAX_LIFETIME_SECONDS: u64 = 30 * 60;

#[derive(Debug, Clone)]
pub struct WorkerDbPoolOptions {
    application_name: String,
    max_connections: u32,
    min_connections: u32,
    acquire_timeout: Duration,
    idle_timeout: Duration,
    max_lifetime: Duration,
    statement_timeout: Option<Duration>,
}

impl WorkerDbPoolOptions {
    #[must_use]
    pub fn new(application_name: impl AsRef<str>) -> Self {
        Self {
            application_name: normalize_application_name(
                application_name.as_ref(),
                APP_SOLVER_WORKER,
            ),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            min_connections: DEFAULT_MIN_CONNECTIONS,
            acquire_timeout: Duration::from_secs(DEFAULT_ACQUIRE_TIMEOUT_SECONDS),
            idle_timeout: Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECONDS),
            max_lifetime: Duration::from_secs(DEFAULT_MAX_LIFETIME_SECONDS),
            statement_timeout: None,
        }
    }

    #[must_use]
    pub fn max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections.max(1);
        self
    }

    #[must_use]
    pub fn min_connections(mut self, min_connections: u32) -> Self {
        self.min_connections = min_connections.min(self.max_connections);
        self
    }

    #[must_use]
    pub fn acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = acquire_timeout.max(Duration::from_secs(1));
        self
    }

    #[must_use]
    pub fn statement_timeout(mut self, statement_timeout: Option<Duration>) -> Self {
        self.statement_timeout = statement_timeout;
        self
    }

    #[must_use]
    pub fn application_name(&self) -> &str {
        &self.application_name
    }

    #[must_use]
    pub fn max_connections_value(&self) -> u32 {
        self.max_connections
    }

    #[must_use]
    pub fn min_connections_value(&self) -> u32 {
        self.min_connections
    }

    #[must_use]
    pub fn acquire_timeout_value(&self) -> Duration {
        self.acquire_timeout
    }

    #[must_use]
    pub fn idle_timeout_value(&self) -> Duration {
        self.idle_timeout
    }

    #[must_use]
    pub fn max_lifetime_value(&self) -> Duration {
        self.max_lifetime
    }

    #[must_use]
    pub fn statement_timeout_value(&self) -> Option<Duration> {
        self.statement_timeout
    }

    pub async fn connect(self, database_url: &str) -> anyhow::Result<PgPool> {
        let application_name = self.application_name.clone();
        let statement_timeout_ms = self.statement_timeout.map(duration_millis_text);

        Ok(PgPoolOptions::new()
            .max_connections(self.max_connections)
            .min_connections(self.min_connections)
            .acquire_timeout(self.acquire_timeout)
            .idle_timeout(self.idle_timeout)
            .max_lifetime(self.max_lifetime)
            .test_before_acquire(true)
            .after_connect(move |conn, _meta| {
                let application_name = application_name.clone();
                let statement_timeout_ms = statement_timeout_ms.clone();
                Box::pin(async move {
                    let application_name_sql = format!(
                        "SELECT set_config('application_name', {}, false)",
                        sql_string_literal(application_name.as_str())
                    );
                    conn.execute(application_name_sql.as_str()).await?;
                    if let Some(statement_timeout_ms) = statement_timeout_ms.as_deref() {
                        let statement_timeout_sql = format!(
                            "SELECT set_config('statement_timeout', {}, false)",
                            sql_string_literal(statement_timeout_ms)
                        );
                        conn.execute(statement_timeout_sql.as_str()).await?;
                    }
                    Ok(())
                })
            })
            .connect(database_url)
            .await?)
    }
}

#[must_use]
pub fn normalize_application_name(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    let candidate = if trimmed.is_empty() {
        fallback.trim()
    } else {
        trimmed
    };

    truncate_to_bytes(candidate, POSTGRES_APPLICATION_NAME_MAX_BYTES)
}

#[must_use]
pub fn duration_millis_text(duration: Duration) -> String {
    duration.as_millis().to_string()
}

#[must_use]
pub fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn truncate_to_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }

    let mut out = String::new();
    for ch in value.chars() {
        if out.len() + ch.len_utf8() > max_bytes {
            break;
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{duration_millis_text, normalize_application_name, sql_string_literal};

    #[test]
    fn normalize_application_name_uses_fallback_for_blank_values() {
        assert_eq!(
            normalize_application_name("   ", "solver-worker"),
            "solver-worker"
        );
    }

    #[test]
    fn normalize_application_name_limits_postgres_name_length() {
        let name = normalize_application_name(
            "snapshot-builder-with-a-very-long-deployment-suffix-that-exceeds-the-limit",
            "solver-worker",
        );
        assert!(name.len() <= 63);
        assert!(name.starts_with("snapshot-builder"));
    }

    #[test]
    fn duration_millis_text_formats_postgres_statement_timeout_value() {
        assert_eq!(duration_millis_text(Duration::from_secs(900)), "900000");
        assert_eq!(duration_millis_text(Duration::from_millis(250)), "250");
    }

    #[test]
    fn sql_string_literal_escapes_quotes_for_raw_after_connect_sql() {
        assert_eq!(sql_string_literal("solver-worker"), "'solver-worker'");
        assert_eq!(sql_string_literal("worker 'quoted'"), "'worker ''quoted'''");
    }
}
