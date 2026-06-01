use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::pgbouncer_sqlx::{self as sqlx, PgPool, Row};

pub const REVIEW_SUBMIT_GATE_JOB_KIND: &str = "review_submit.gate";
pub const REVIEW_SUBMIT_GATE_PAYLOAD_SCHEMA_VERSION: &str = "review_submit.gate.request.v1";
pub const REVIEW_SUBMIT_GATE_WORKER_QUEUE: &str = "review_submit_gate";

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerJob {
    pub id: Uuid,
    pub job_kind: String,
    pub worker_queue: String,
    pub payload_schema_version: String,
    pub payload: Value,
    pub requested_by: Option<Uuid>,
    pub lease_token: Uuid,
    pub attempt_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewSubmitGateWorkerRequest {
    pub dataset_table: String,
    pub dataset_id: Uuid,
    pub dataset_version: String,
    pub revision_checksum: Option<String>,
    pub policy_profile: Option<String>,
    pub report_schema_version: Option<String>,
    pub requested_by: Uuid,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerJobResult {
    pub status: String,
    pub result_json: Option<Value>,
    pub result_schema_version: Option<String>,
    pub result_ref: Option<Value>,
    pub diagnostics: Option<Value>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub error_details: Option<Value>,
    pub blocker_codes: Vec<String>,
    pub resolution_scope: Option<String>,
    pub retryable: Option<bool>,
}

impl WorkerJob {
    pub fn from_json(value: &Value) -> anyhow::Result<Self> {
        let id = value
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("worker job is missing id"))?
            .parse::<Uuid>()?;
        let lease_token = value
            .get("leaseToken")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("worker job is missing leaseToken"))?
            .parse::<Uuid>()?;
        let requested_by = value
            .get("requestedBy")
            .and_then(Value::as_str)
            .map(str::parse::<Uuid>)
            .transpose()?;

        Ok(Self {
            id,
            job_kind: required_text(value, "jobKind")?,
            worker_queue: required_text(value, "workerQueue")?,
            payload_schema_version: required_text(value, "payloadSchemaVersion")?,
            payload: value.get("payload").cloned().unwrap_or_else(|| json!({})),
            requested_by,
            lease_token,
            attempt_count: value
                .get("attemptCount")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
        })
    }

    pub fn review_submit_gate_request(&self) -> anyhow::Result<ReviewSubmitGateWorkerRequest> {
        if self.job_kind != REVIEW_SUBMIT_GATE_JOB_KIND {
            return Err(anyhow::anyhow!(
                "unsupported worker job kind for review-submit gate: {}",
                self.job_kind
            ));
        }
        if self.worker_queue != REVIEW_SUBMIT_GATE_WORKER_QUEUE {
            return Err(anyhow::anyhow!(
                "unsupported worker queue for review-submit gate: {}",
                self.worker_queue
            ));
        }
        if self.payload_schema_version != REVIEW_SUBMIT_GATE_PAYLOAD_SCHEMA_VERSION {
            return Err(anyhow::anyhow!(
                "unsupported review-submit gate payload schema: {}",
                self.payload_schema_version
            ));
        }

        let payload = serde_json::from_value::<ReviewSubmitGatePayload>(self.payload.clone())?;
        let requested_by = payload.requested_by.or(self.requested_by).ok_or_else(|| {
            anyhow::anyhow!("review-submit gate worker job is missing requestedBy")
        })?;

        Ok(ReviewSubmitGateWorkerRequest {
            dataset_table: payload.dataset_revision.dataset_table,
            dataset_id: payload.dataset_revision.dataset_id,
            dataset_version: payload.dataset_revision.dataset_version,
            revision_checksum: payload.dataset_revision.revision_checksum,
            policy_profile: payload.policy_profile,
            report_schema_version: payload.report_schema_version,
            requested_by,
        })
    }
}

impl WorkerJobResult {
    pub fn completed(result_json: Value, result_schema_version: impl Into<String>) -> Self {
        Self {
            status: "completed".to_owned(),
            result_json: Some(result_json),
            result_schema_version: Some(result_schema_version.into()),
            result_ref: None,
            diagnostics: None,
            error_code: None,
            error_message: None,
            error_details: None,
            blocker_codes: Vec::new(),
            resolution_scope: None,
            retryable: None,
        }
    }

    pub fn blocked(
        result_json: Value,
        result_schema_version: impl Into<String>,
        blocker_codes: Vec<String>,
        resolution_scope: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            status: "blocked".to_owned(),
            result_json: Some(result_json),
            result_schema_version: Some(result_schema_version.into()),
            result_ref: None,
            diagnostics: None,
            error_code: None,
            error_message: None,
            error_details: None,
            blocker_codes,
            resolution_scope: Some(resolution_scope.into()),
            retryable: Some(retryable),
        }
    }

    pub fn failed(
        error_code: impl Into<String>,
        error_message: impl Into<String>,
        error_details: Value,
        diagnostics: Option<Value>,
        result_json: Option<Value>,
    ) -> Self {
        Self {
            status: "failed".to_owned(),
            result_json,
            result_schema_version: None,
            result_ref: None,
            diagnostics,
            error_code: Some(error_code.into()),
            error_message: Some(error_message.into()),
            error_details: Some(error_details),
            blocker_codes: Vec::new(),
            resolution_scope: None,
            retryable: Some(true),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerJobProgress<'a> {
    pool: &'a PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    lease_seconds: i32,
}

impl<'a> WorkerJobProgress<'a> {
    #[must_use]
    pub const fn new(
        pool: &'a PgPool,
        job_id: Uuid,
        lease_token: Uuid,
        lease_seconds: i32,
    ) -> Self {
        Self {
            pool,
            job_id,
            lease_token,
            lease_seconds,
        }
    }

    pub async fn heartbeat(
        &self,
        phase: &str,
        progress: f64,
        diagnostics: Option<Value>,
    ) -> anyhow::Result<()> {
        heartbeat_worker_job(
            self.pool,
            self.job_id,
            self.lease_token,
            phase,
            progress,
            diagnostics,
            self.lease_seconds,
        )
        .await
    }
}

pub async fn claim_worker_jobs(
    pool: &PgPool,
    worker_queue: &str,
    worker_id: &str,
    limit: i32,
    lease_seconds: i32,
) -> anyhow::Result<Vec<WorkerJob>> {
    let row = sqlx::query(
        r"
        SELECT public.worker_claim_jobs($1, $2, $3, $4) AS result
        ",
    )
    .bind(worker_queue)
    .bind(worker_id)
    .bind(limit)
    .bind(lease_seconds)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_ok(&result, "worker_claim_jobs")?;

    result.get("data").and_then(Value::as_array).map_or_else(
        || Ok(Vec::new()),
        |items| items.iter().map(WorkerJob::from_json).collect(),
    )
}

pub async fn heartbeat_worker_job(
    pool: &PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    phase: &str,
    progress: f64,
    diagnostics: Option<Value>,
    lease_seconds: i32,
) -> anyhow::Result<()> {
    let row = sqlx::query(
        r"
        SELECT public.worker_heartbeat_job($1, $2, $3, $4::double precision::numeric, $5::jsonb, $6) AS result
        ",
    )
    .bind(job_id)
    .bind(lease_token)
    .bind(phase)
    .bind(progress)
    .bind(diagnostics)
    .bind(lease_seconds)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_ok(&result, "worker_heartbeat_job")?;
    Ok(())
}

pub async fn record_worker_job_result(
    pool: &PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    result: WorkerJobResult,
) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        SELECT public.worker_record_job_result(
            $1,
            $2,
            $3,
            $4::jsonb,
            $5,
            $6::jsonb,
            $7::jsonb,
            $8,
            $9,
            $10::jsonb,
            $11::text[],
            $12,
            $13
        ) AS result
        ",
    )
    .bind(job_id)
    .bind(lease_token)
    .bind(result.status)
    .bind(result.result_json)
    .bind(result.result_schema_version)
    .bind(result.result_ref)
    .bind(result.diagnostics)
    .bind(result.error_code)
    .bind(result.error_message)
    .bind(result.error_details)
    .bind(result.blocker_codes)
    .bind(result.resolution_scope)
    .bind(result.retryable)
    .fetch_one(pool)
    .await?;
    let rpc_result = row.try_get::<Value, _>("result")?;
    ensure_ok(&rpc_result, "worker_record_job_result")?;
    Ok(rpc_result)
}

fn ensure_ok(result: &Value, rpc_name: &str) -> anyhow::Result<()> {
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "{rpc_name} returned non-ok result: {result}"
        ))
    }
}

fn required_text(value: &Value, key: &str) -> anyhow::Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("worker job is missing {key}"))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReviewSubmitGatePayload {
    dataset_revision: ReviewSubmitGateDatasetRevision,
    #[serde(default)]
    requested_by: Option<Uuid>,
    #[serde(default)]
    policy_profile: Option<String>,
    #[serde(default)]
    report_schema_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReviewSubmitGateDatasetRevision {
    #[serde(alias = "table", alias = "datasetTable")]
    dataset_table: String,
    #[serde(alias = "id", alias = "datasetId")]
    dataset_id: Uuid,
    #[serde(alias = "version", alias = "datasetVersion")]
    dataset_version: String,
    #[serde(default, alias = "revisionChecksum")]
    revision_checksum: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        REVIEW_SUBMIT_GATE_JOB_KIND, REVIEW_SUBMIT_GATE_PAYLOAD_SCHEMA_VERSION,
        REVIEW_SUBMIT_GATE_WORKER_QUEUE, WorkerJob,
    };

    #[test]
    fn parses_review_submit_gate_worker_job_payload() {
        let job_id = Uuid::new_v4();
        let lease_token = Uuid::new_v4();
        let requested_by = Uuid::new_v4();
        let dataset_id = Uuid::new_v4();
        let job = WorkerJob::from_json(&json!({
            "id": job_id,
            "jobKind": REVIEW_SUBMIT_GATE_JOB_KIND,
            "workerQueue": REVIEW_SUBMIT_GATE_WORKER_QUEUE,
            "payloadSchemaVersion": REVIEW_SUBMIT_GATE_PAYLOAD_SCHEMA_VERSION,
            "payload": {
                "datasetRevision": {
                    "table": "processes",
                    "id": dataset_id,
                    "version": "01.00.000",
                    "revisionChecksum": "abc123"
                }
            },
            "requestedBy": requested_by,
            "leaseToken": lease_token,
            "attemptCount": 2
        }))
        .unwrap();

        let request = job.review_submit_gate_request().unwrap();

        assert_eq!(request.dataset_table, "processes");
        assert_eq!(request.dataset_id, dataset_id);
        assert_eq!(request.dataset_version, "01.00.000");
        assert_eq!(request.revision_checksum.as_deref(), Some("abc123"));
        assert_eq!(request.requested_by, requested_by);
        assert_eq!(job.attempt_count, 2);
    }

    #[test]
    fn rejects_wrong_review_submit_worker_job_kind() {
        let job = WorkerJob::from_json(&json!({
            "id": Uuid::new_v4(),
            "jobKind": "lca.solve_one",
            "workerQueue": REVIEW_SUBMIT_GATE_WORKER_QUEUE,
            "payloadSchemaVersion": REVIEW_SUBMIT_GATE_PAYLOAD_SCHEMA_VERSION,
            "payload": {},
            "requestedBy": Uuid::new_v4(),
            "leaseToken": Uuid::new_v4(),
            "attemptCount": 1
        }))
        .unwrap();

        assert!(job.review_submit_gate_request().is_err());
    }
}
