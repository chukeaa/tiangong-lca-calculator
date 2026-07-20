#![allow(
    clippy::missing_const_for_fn,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::struct_field_names,
    clippy::too_many_lines
)]

use std::collections::HashSet;
use std::time::Duration;

use anyhow::Context;
use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    db::{self, AppState},
    graph_types::RequestRootProcess,
    pgbouncer_sqlx::{self as sqlx, PgPool, Row},
    readiness::{FindingSeverity, ReadinessFinding},
    review_submit_gate::{
        ReviewExchangeRecord, ReviewProcessRecord, ReviewSubmitGateInput, ReviewSubmitGateMetrics,
        ReviewSubmitGatePolicy, ReviewSubmitGateReport, ReviewSubmitGateStatus,
        verify_review_submit_gate,
    },
    snapshot_index::SnapshotIndexDocument,
    worker_jobs::{
        REVIEW_SUBMIT_GATE_WORKER_QUEUE, WorkerJob, WorkerJobProgress, WorkerJobResult,
        claim_worker_jobs, record_worker_job_result,
    },
};

pub const REVIEW_SUBMIT_GATE_POLICY_PROFILE: &str = "review_submit_fast.v1";
pub const REVIEW_SUBMIT_GATE_REPORT_SCHEMA_VERSION: &str = "review_submit_gate_report.v1";

const SUPPORTED_DATASET_TABLE: &str = "processes";
const RUNNER_NAME: &str = "review_submit_gate_runner";

#[derive(Debug, Clone)]
pub struct ReviewSubmitGateRunnerOptions {
    pub poll_interval: Duration,
    pub max_runs: Option<usize>,
    pub exit_when_idle: bool,
    pub stale_running_after: Duration,
}

#[derive(Debug, Clone)]
pub struct ReviewSubmitGateWorkerJobsOptions {
    pub poll_interval: Duration,
    pub max_runs: Option<usize>,
    pub exit_when_idle: bool,
    pub worker_id: String,
    pub lease_seconds: i32,
}

impl Default for ReviewSubmitGateWorkerJobsOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            max_runs: None,
            exit_when_idle: false,
            worker_id: RUNNER_NAME.to_owned(),
            lease_seconds: 900,
        }
    }
}

impl Default for ReviewSubmitGateRunnerOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            max_runs: None,
            exit_when_idle: false,
            stale_running_after: Duration::from_hours(6),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ReviewSubmitGateRunnerSummary {
    pub claimed: usize,
    pub passed: usize,
    pub blocked: usize,
    pub errors: usize,
    pub idle_polls: usize,
}

impl ReviewSubmitGateRunnerSummary {
    fn record_status(&mut self, status: RecordedGateStatus) {
        self.claimed += 1;
        match status {
            RecordedGateStatus::Passed => self.passed += 1,
            RecordedGateStatus::Blocked => self.blocked += 1,
            RecordedGateStatus::Error => self.errors += 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewSubmitGateRun {
    pub id: Uuid,
    pub dataset_table: String,
    pub dataset_id: Uuid,
    pub dataset_version: String,
    pub revision_checksum: Option<String>,
    pub policy_profile: String,
    pub report_schema_version: String,
    pub requested_by: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedGateStatus {
    Passed,
    Blocked,
    Error,
}

impl RecordedGateStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Blocked => "blocked",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone)]
struct GateExecutionOutcome {
    status: RecordedGateStatus,
    calculator_report: Value,
    blocking_reasons: Value,
    audit: Value,
    authoritative_revision_checksum: Option<String>,
}

#[derive(Debug, Clone)]
struct ProcessRevision {
    process_id: Uuid,
    process_version: String,
    state_code: Option<i32>,
    json_ordered: Value,
}

pub async fn run_review_submit_gate_runner(
    state: &AppState,
    options: ReviewSubmitGateRunnerOptions,
) -> anyhow::Result<ReviewSubmitGateRunnerSummary> {
    let mut summary = ReviewSubmitGateRunnerSummary::default();

    loop {
        if options
            .max_runs
            .is_some_and(|max_runs| summary.claimed >= max_runs)
        {
            break;
        }

        if let Some(status) = run_one_review_submit_gate(state, options.stale_running_after).await?
        {
            summary.record_status(status);
        } else {
            summary.idle_polls += 1;
            if options.exit_when_idle {
                break;
            }
            sleep(options.poll_interval).await;
        }
    }

    Ok(summary)
}

pub async fn run_review_submit_gate_worker_jobs_runner(
    state: &AppState,
    options: ReviewSubmitGateWorkerJobsOptions,
) -> anyhow::Result<ReviewSubmitGateRunnerSummary> {
    let mut summary = ReviewSubmitGateRunnerSummary::default();

    loop {
        if options
            .max_runs
            .is_some_and(|max_runs| summary.claimed >= max_runs)
        {
            break;
        }

        if let Some(status) = run_one_review_submit_gate_worker_job(state, &options).await? {
            summary.record_status(status);
        } else {
            summary.idle_polls += 1;
            if options.exit_when_idle {
                break;
            }
            sleep(options.poll_interval).await;
        }
    }

    Ok(summary)
}

pub async fn run_one_review_submit_gate(
    state: &AppState,
    stale_running_after: Duration,
) -> anyhow::Result<Option<RecordedGateStatus>> {
    let Some(run) = claim_next_review_submit_gate_run(&state.pool, stale_running_after).await?
    else {
        return Ok(None);
    };

    let status = process_claimed_review_submit_gate_run(state, &run).await?;
    Ok(Some(status))
}

pub async fn run_one_review_submit_gate_worker_job(
    state: &AppState,
    options: &ReviewSubmitGateWorkerJobsOptions,
) -> anyhow::Result<Option<RecordedGateStatus>> {
    let jobs = claim_worker_jobs(
        &state.pool,
        REVIEW_SUBMIT_GATE_WORKER_QUEUE,
        &options.worker_id,
        1,
        options.lease_seconds,
    )
    .await?;

    let Some(job) = jobs.into_iter().next() else {
        return Ok(None);
    };

    let status = process_claimed_review_submit_gate_worker_job(state, &job, options).await?;
    Ok(Some(status))
}

async fn process_claimed_review_submit_gate_run(
    state: &AppState,
    run: &ReviewSubmitGateRun,
) -> anyhow::Result<RecordedGateStatus> {
    let outcome = match execute_claimed_gate_run(state, run, None).await {
        Ok(outcome) => outcome,
        Err(err) => {
            warn!(
                gate_run_id = %run.id,
                dataset_table = %run.dataset_table,
                dataset_id = %run.dataset_id,
                dataset_version = %run.dataset_version,
                error = %err,
                "review-submit gate execution failed; recording error result"
            );
            runtime_error_outcome(
                run,
                "calculator_gate_error",
                "calculator review-submit gate runner failed before producing a passed/blocked report",
                json!({ "error": err.to_string() }),
            )?
        }
    };

    record_review_submit_gate_result(
        &state.pool,
        run.id,
        outcome.status,
        outcome.calculator_report,
        outcome.blocking_reasons,
        outcome.audit,
    )
    .await?;

    info!(
        gate_run_id = %run.id,
        dataset_table = %run.dataset_table,
        dataset_id = %run.dataset_id,
        dataset_version = %run.dataset_version,
        status = outcome.status.as_str(),
        "recorded review-submit gate result"
    );
    Ok(outcome.status)
}

async fn process_claimed_review_submit_gate_worker_job(
    state: &AppState,
    job: &WorkerJob,
    options: &ReviewSubmitGateWorkerJobsOptions,
) -> anyhow::Result<RecordedGateStatus> {
    let progress =
        WorkerJobProgress::new(&state.pool, job.id, job.lease_token, options.lease_seconds);
    let run = match worker_job_to_gate_run(job) {
        Ok(run) => run,
        Err(err) => {
            record_worker_job_result(
                &state.pool,
                job.id,
                job.lease_token,
                WorkerJobResult::failed(
                    "invalid_review_submit_gate_job",
                    "review-submit gate worker job payload is invalid",
                    json!({ "error": err.to_string() }),
                    Some(json!({ "runner": RUNNER_NAME, "workerJobId": job.id })),
                    None,
                ),
            )
            .await?;
            return Ok(RecordedGateStatus::Error);
        }
    };

    let outcome = match execute_claimed_gate_run(state, &run, Some(&progress)).await {
        Ok(outcome) => outcome,
        Err(err) => {
            warn!(
                worker_job_id = %job.id,
                dataset_table = %run.dataset_table,
                dataset_id = %run.dataset_id,
                dataset_version = %run.dataset_version,
                error = %err,
                "worker_jobs review-submit gate execution failed; recording failed result"
            );
            runtime_error_outcome(
                &run,
                "calculator_gate_error",
                "calculator review-submit gate worker failed before producing a passed/blocked report",
                json!({ "error": err.to_string(), "worker_job_id": job.id }),
            )?
        }
    };

    let worker_result = worker_job_result_for_outcome(&run, &outcome);
    record_worker_job_result(&state.pool, job.id, job.lease_token, worker_result).await?;

    info!(
        worker_job_id = %job.id,
        dataset_table = %run.dataset_table,
        dataset_id = %run.dataset_id,
        dataset_version = %run.dataset_version,
        status = outcome.status.as_str(),
        "recorded worker_jobs review-submit gate result"
    );
    Ok(outcome.status)
}

async fn execute_claimed_gate_run(
    state: &AppState,
    run: &ReviewSubmitGateRun,
    progress: Option<&WorkerJobProgress<'_>>,
) -> anyhow::Result<GateExecutionOutcome> {
    if run.dataset_table != SUPPORTED_DATASET_TABLE {
        return runtime_error_outcome(
            run,
            "unsupported_dataset_table",
            "calculator review-submit gate runner currently supports process revisions only",
            json!({ "dataset_table": run.dataset_table }),
        );
    }

    if let Some(progress) = progress {
        progress.heartbeat("fetching_revision", 0.05, None).await?;
    }

    let revision = fetch_process_revision(&state.pool, run.dataset_id, &run.dataset_version)
        .await
        .with_context(|| {
            format!(
                "failed to fetch process revision {}@{}",
                run.dataset_id, run.dataset_version
            )
        })?;
    let actual_revision_checksum = stable_json_sha256(&revision.json_ordered)?;
    let expected_revision_checksum = run
        .revision_checksum
        .clone()
        .unwrap_or_else(|| actual_revision_checksum.clone());
    let mut process_record = build_review_process_record(&revision, None);
    if let Some(report) = allocation_preflight_report(
        run,
        &process_record,
        &expected_revision_checksum,
        &actual_revision_checksum,
    ) {
        let blocking_reasons = blocking_reasons_for_report(&report)?;
        let calculator_report = serde_json::to_value(&report)?;
        return Ok(GateExecutionOutcome {
            status: RecordedGateStatus::Blocked,
            calculator_report,
            blocking_reasons,
            audit: json!({
                "runner": RUNNER_NAME,
                "phase": "process_allocation_preflight",
                "blocker_count": report.blockers.len()
            }),
            authoritative_revision_checksum: Some(actual_revision_checksum),
        });
    }
    let request_roots = vec![RequestRootProcess::new(
        run.dataset_id,
        run.dataset_version.clone(),
    )];
    if let Some(progress) = progress {
        progress
            .heartbeat(
                "building_snapshot",
                0.15,
                Some(json!({
                    "datasetTable": run.dataset_table,
                    "datasetId": run.dataset_id,
                    "datasetVersion": run.dataset_version,
                })),
            )
            .await?;
    }
    let snapshot = db::run_review_submit_gate_snapshot_builder(
        state,
        run.id,
        run.requested_by,
        request_roots.as_slice(),
        &actual_revision_checksum,
    )
    .await
    .context("failed to build review-submit gate snapshot")?;
    if let Some(progress) = progress {
        progress
            .heartbeat(
                "loading_snapshot_artifact",
                0.65,
                Some(json!({
                    "requestedSnapshotId": snapshot.requested_snapshot_id,
                    "resolvedSnapshotId": snapshot.resolved_snapshot_id,
                    "snapshotExitCode": snapshot.exit_code,
                })),
            )
            .await?;
    }
    let artifact = db::fetch_decoded_snapshot_artifact(state, snapshot.resolved_snapshot_id)
        .await
        .context("failed to fetch decoded review-submit gate snapshot artifact")?;
    let snapshot_index = db::fetch_snapshot_index_document(state, snapshot.resolved_snapshot_id)
        .await
        .context("failed to fetch review-submit gate snapshot index")?;
    let target_process_idx =
        find_process_index(&snapshot_index, run.dataset_id, &run.dataset_version);
    process_record.process_idx = target_process_idx;

    let mut policy = ReviewSubmitGatePolicy::default();
    policy.policy_profile.clone_from(&run.policy_profile);
    let input = ReviewSubmitGateInput {
        schema_version: "review_submit_gate_input.v1".to_owned(),
        dataset_revision_id: Some(run.dataset_id),
        expected_revision_checksum: Some(expected_revision_checksum),
        actual_revision_checksum: Some(actual_revision_checksum),
        snapshot_id: Some(snapshot.resolved_snapshot_id),
        config: Some(artifact.config),
        coverage: artifact.coverage,
        payload: artifact.payload,
        compiled_graph: artifact.compiled_graph,
        target_process_indices: target_process_idx.into_iter().collect(),
        process_records: vec![process_record],
        policy,
    };

    if let Some(progress) = progress {
        progress.heartbeat("running_gate", 0.80, None).await?;
    }
    let report = verify_review_submit_gate(&input);
    let status = recorded_status_for_report(&report);
    let blocking_reasons = blocking_reasons_for_report(&report)?;
    let calculator_report = serde_json::to_value(&report)?;
    let audit = json!({
        "runner": RUNNER_NAME,
        "requested_snapshot_id": snapshot.requested_snapshot_id,
        "resolved_snapshot_id": snapshot.resolved_snapshot_id,
        "snapshot_builder": {
            "exit_code": snapshot.exit_code,
            "command": snapshot.command,
            "build_timing_sec": snapshot.build_timing_sec,
            "stdout_tail": snapshot.stdout_tail,
            "stderr_tail": snapshot.stderr_tail
        },
        "blocker_count": report.blockers.len()
    });
    if let Some(progress) = progress {
        progress
            .heartbeat(
                "recording_result",
                0.95,
                Some(json!({ "blockerCount": report.blockers.len() })),
            )
            .await?;
    }

    Ok(GateExecutionOutcome {
        status,
        calculator_report,
        blocking_reasons,
        audit,
        authoritative_revision_checksum: input.actual_revision_checksum,
    })
}

async fn claim_next_review_submit_gate_run(
    pool: &PgPool,
    stale_running_after: Duration,
) -> anyhow::Result<Option<ReviewSubmitGateRun>> {
    let stale_seconds = i64::try_from(stale_running_after.as_secs())
        .map_err(|_| anyhow::anyhow!("stale_running_after is too large"))?
        .max(60);
    let row = sqlx::query(
        r"
        WITH claimed AS (
            SELECT id
            FROM public.dataset_review_submit_gate_runs
            WHERE policy_profile = $2
              AND report_schema_version = $3
              AND (
                status = 'queued'
                OR (
                  status = 'running'
                  AND modified_at < now() - ($1::bigint * interval '1 second')
                )
              )
            ORDER BY
              CASE status WHEN 'queued' THEN 0 ELSE 1 END,
              created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE public.dataset_review_submit_gate_runs AS gate_run
        SET status = 'running',
            modified_at = now()
        FROM claimed
        WHERE gate_run.id = claimed.id
        RETURNING
            gate_run.id,
            gate_run.dataset_table,
            gate_run.dataset_id,
            gate_run.dataset_version,
            gate_run.revision_checksum,
            gate_run.policy_profile,
            gate_run.report_schema_version,
            gate_run.requested_by
        ",
    )
    .bind(stale_seconds)
    .bind(REVIEW_SUBMIT_GATE_POLICY_PROFILE)
    .bind(REVIEW_SUBMIT_GATE_REPORT_SCHEMA_VERSION)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(row) => Ok(Some(review_submit_gate_run_from_row(&row)?)),
        None => Ok(None),
    }
}

fn review_submit_gate_run_from_row(
    row: &crate::pgbouncer_sqlx::postgres::PgRow,
) -> anyhow::Result<ReviewSubmitGateRun> {
    Ok(ReviewSubmitGateRun {
        id: row.try_get::<Uuid, _>("id")?,
        dataset_table: row.try_get::<String, _>("dataset_table")?,
        dataset_id: row.try_get::<Uuid, _>("dataset_id")?,
        dataset_version: row.try_get::<String, _>("dataset_version")?,
        revision_checksum: Some(row.try_get::<String, _>("revision_checksum")?),
        policy_profile: row.try_get::<String, _>("policy_profile")?,
        report_schema_version: row.try_get::<String, _>("report_schema_version")?,
        requested_by: row.try_get::<Uuid, _>("requested_by")?,
    })
}

fn worker_job_to_gate_run(job: &WorkerJob) -> anyhow::Result<ReviewSubmitGateRun> {
    let request = job.review_submit_gate_request()?;
    Ok(ReviewSubmitGateRun {
        id: job.id,
        dataset_table: request.dataset_table,
        dataset_id: request.dataset_id,
        dataset_version: request.dataset_version,
        revision_checksum: request.revision_checksum,
        policy_profile: request
            .policy_profile
            .unwrap_or_else(|| REVIEW_SUBMIT_GATE_POLICY_PROFILE.to_owned()),
        report_schema_version: request
            .report_schema_version
            .unwrap_or_else(|| REVIEW_SUBMIT_GATE_REPORT_SCHEMA_VERSION.to_owned()),
        requested_by: request.requested_by,
    })
}

fn worker_job_result_for_outcome(
    run: &ReviewSubmitGateRun,
    outcome: &GateExecutionOutcome,
) -> WorkerJobResult {
    let result_json = json!({
        "status": outcome.status.as_str(),
        "datasetRevision": {
            "table": run.dataset_table,
            "id": run.dataset_id,
            "version": run.dataset_version,
            "revisionChecksum": outcome.authoritative_revision_checksum
        },
        "calculatorReport": outcome.calculator_report,
        "blockingReasons": outcome.blocking_reasons
    });

    let mut worker_result = match outcome.status {
        RecordedGateStatus::Passed => {
            WorkerJobResult::completed(result_json, REVIEW_SUBMIT_GATE_REPORT_SCHEMA_VERSION)
        }
        RecordedGateStatus::Blocked => WorkerJobResult::blocked(
            result_json,
            REVIEW_SUBMIT_GATE_REPORT_SCHEMA_VERSION,
            blocker_codes_from_reasons(&outcome.blocking_reasons),
            "user",
            true,
        ),
        RecordedGateStatus::Error => WorkerJobResult::failed(
            runtime_error_code(&outcome.calculator_report),
            runtime_error_message(&outcome.calculator_report),
            json!({
                "calculatorReport": outcome.calculator_report,
                "blockingReasons": outcome.blocking_reasons
            }),
            None,
            Some(result_json),
        ),
    };
    worker_result.diagnostics = Some(outcome.audit.clone());
    worker_result
}

fn blocker_codes_from_reasons(blocking_reasons: &Value) -> Vec<String> {
    blocking_reasons
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|reason| reason.get("code").and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

fn runtime_error_code(report: &Value) -> String {
    report
        .get("runtime_error")
        .and_then(|value| value.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("calculator_gate_error")
        .to_owned()
}

fn runtime_error_message(report: &Value) -> String {
    report
        .get("runtime_error")
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or(
            "calculator review-submit gate worker failed before producing a passed/blocked report",
        )
        .to_owned()
}

async fn record_review_submit_gate_result(
    pool: &PgPool,
    gate_run_id: Uuid,
    status: RecordedGateStatus,
    calculator_report: Value,
    blocking_reasons: Value,
    audit: Value,
) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        SELECT public.cmd_dataset_review_submit_gate_record_result(
            $1,
            $2,
            $3::jsonb,
            $4::jsonb,
            $5,
            $6::jsonb
        ) AS result
        ",
    )
    .bind(gate_run_id)
    .bind(status.as_str())
    .bind(calculator_report)
    .bind(blocking_reasons)
    .bind(REVIEW_SUBMIT_GATE_REPORT_SCHEMA_VERSION)
    .bind(audit)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(result)
    } else {
        Err(anyhow::anyhow!(
            "cmd_dataset_review_submit_gate_record_result returned non-ok result: {result}"
        ))
    }
}

async fn fetch_process_revision(
    pool: &PgPool,
    process_id: Uuid,
    process_version: &str,
) -> anyhow::Result<ProcessRevision> {
    let row = sqlx::query(
        r"
        SELECT id, version, state_code, json_ordered
        FROM public.processes
        WHERE id = $1
          AND version = $2
        LIMIT 1
        ",
    )
    .bind(process_id)
    .bind(process_version)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "process revision not found: process_id={process_id} version={process_version}"
        )
    })?;

    let json_ordered = row
        .try_get::<Option<Value>, _>("json_ordered")?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "process revision has no json_ordered payload: process_id={process_id} version={process_version}"
            )
        })?;

    Ok(ProcessRevision {
        process_id: row.try_get::<Uuid, _>("id")?,
        process_version: row.try_get::<String, _>("version")?,
        state_code: row.try_get::<Option<i32>, _>("state_code")?,
        json_ordered,
    })
}

fn recorded_status_for_report(report: &ReviewSubmitGateReport) -> RecordedGateStatus {
    match report.status {
        ReviewSubmitGateStatus::Passed => RecordedGateStatus::Passed,
        ReviewSubmitGateStatus::Blocked => RecordedGateStatus::Blocked,
    }
}

fn blocking_reasons_for_report(report: &ReviewSubmitGateReport) -> anyhow::Result<Value> {
    let reasons = report
        .blockers
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::Array(reasons))
}

fn runtime_error_outcome(
    run: &ReviewSubmitGateRun,
    code: &str,
    message: &str,
    details: Value,
) -> anyhow::Result<GateExecutionOutcome> {
    let finding = synthetic_blocker(code, message, details);
    let blocking_reasons = Value::Array(vec![serde_json::to_value(&finding)?]);
    let calculator_report = json!({
        "schema_version": REVIEW_SUBMIT_GATE_REPORT_SCHEMA_VERSION,
        "generated_at_utc": utc_now_text(),
        "dataset_revision_id": run.dataset_id,
        "snapshot_id": null,
        "status": "error",
        "policy": {
            "policy_profile": run.policy_profile
        },
        "metrics": null,
        "blockers": blocking_reasons.clone(),
        "runtime_error": {
            "code": code,
            "message": message
        }
    });
    let audit = json!({
        "runner": RUNNER_NAME,
        "error_code": code,
        "error_message": message
    });

    Ok(GateExecutionOutcome {
        status: RecordedGateStatus::Error,
        calculator_report,
        blocking_reasons,
        audit,
        authoritative_revision_checksum: None,
    })
}

fn synthetic_blocker(code: &str, message: &str, details: Value) -> ReadinessFinding {
    ReadinessFinding {
        code: code.to_owned(),
        severity: FindingSeverity::Blocker,
        message: message.to_owned(),
        details,
    }
}

fn allocation_preflight_report(
    run: &ReviewSubmitGateRun,
    process_record: &ReviewProcessRecord,
    expected_revision_checksum: &str,
    actual_revision_checksum: &str,
) -> Option<ReviewSubmitGateReport> {
    let invalid_allocations = process_record
        .exchanges
        .iter()
        .filter(|exchange| {
            exchange
                .allocation_fraction
                .as_deref()
                .is_some_and(|fraction| fraction.starts_with("invalid:"))
        })
        .map(|exchange| {
            json!({
                "exchange_id": exchange.exchange_id,
                "flow_id": exchange.flow_id,
                "allocation_error": exchange.allocation_fraction
            })
        })
        .collect::<Vec<_>>();
    if invalid_allocations.is_empty() {
        return None;
    }

    let mut policy = ReviewSubmitGatePolicy::default();
    policy.policy_profile.clone_from(&run.policy_profile);
    let mut metrics = ReviewSubmitGateMetrics::default();
    metrics.revision.checksum_checked = true;
    metrics.revision.checksum_matched = expected_revision_checksum == actual_revision_checksum;
    metrics.process_scan.process_records_total = 1;
    metrics.process_scan.invalid_allocation_fraction_count = invalid_allocations.len();
    let mut blockers = vec![synthetic_blocker(
        "invalid_allocation_fraction",
        "review-submit scope contains invalid target-aware allocation fractions",
        json!({
            "invalid_allocation_fraction_count": invalid_allocations.len(),
            "examples": invalid_allocations
        }),
    )];
    if !metrics.revision.checksum_matched {
        blockers.push(synthetic_blocker(
            "revision_report_stale",
            "review-submit gate input is not tied to the current dataset revision checksum",
            json!({
                "dataset_revision_id": run.dataset_id,
                "expected_revision_checksum": expected_revision_checksum,
                "actual_revision_checksum": actual_revision_checksum
            }),
        ));
    }

    Some(ReviewSubmitGateReport {
        schema_version: REVIEW_SUBMIT_GATE_REPORT_SCHEMA_VERSION.to_owned(),
        generated_at_utc: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        dataset_revision_id: Some(run.dataset_id),
        snapshot_id: None,
        status: ReviewSubmitGateStatus::Blocked,
        policy,
        metrics,
        blockers,
    })
}

fn find_process_index(
    snapshot_index: &SnapshotIndexDocument,
    process_id: Uuid,
    process_version: &str,
) -> Option<i32> {
    snapshot_index
        .process_map
        .iter()
        .find(|entry| {
            entry.process_id == process_id && entry.process_version.trim() == process_version.trim()
        })
        .map(|entry| entry.process_index)
}

fn build_review_process_record(
    revision: &ProcessRevision,
    process_idx: Option<i32>,
) -> ReviewProcessRecord {
    let exchange_items = process_exchange_items(&revision.json_ordered);
    let reference_exchange_id = reference_exchange_id(&revision.json_ordered);
    let valid_exchange_internal_ids = exchange_items
        .iter()
        .filter_map(|exchange| exchange_id(exchange))
        .collect::<HashSet<_>>();
    let reference_exchange_count = reference_exchange_id.as_deref().map_or(0, |reference| {
        exchange_items
            .iter()
            .filter(|exchange| exchange_id(exchange).as_deref() == Some(reference))
            .count()
    });
    ReviewProcessRecord {
        process_idx,
        process_id: revision.process_id,
        process_version: revision.process_version.clone(),
        process_name: parse_process_name(&revision.json_ordered),
        state_code: revision.state_code,
        reference_exchange_id: reference_exchange_id.clone(),
        exchanges: exchange_items
            .into_iter()
            .filter_map(|exchange| {
                review_exchange_record(
                    exchange,
                    reference_exchange_id.as_deref(),
                    &valid_exchange_internal_ids,
                    reference_exchange_count,
                )
            })
            .collect(),
    }
}

fn reference_exchange_id(process_json: &Value) -> Option<String> {
    value_at_path(
        process_json,
        &[
            "processDataSet",
            "processInformation",
            "quantitativeReference",
            "referenceToReferenceFlow",
        ],
    )
    .and_then(value_to_trimmed_string)
}

fn process_exchange_items(process_json: &Value) -> Vec<&Value> {
    let Some(exchanges) = value_at_path(process_json, &["processDataSet", "exchanges", "exchange"])
    else {
        return Vec::new();
    };

    match exchanges {
        Value::Array(items) => items.iter().collect(),
        Value::Object(_) => vec![exchanges],
        _ => Vec::new(),
    }
}

fn review_exchange_record(
    exchange_json: &Value,
    reference_exchange_id: Option<&str>,
    valid_exchange_internal_ids: &HashSet<String>,
    reference_exchange_count: usize,
) -> Option<ReviewExchangeRecord> {
    Some(ReviewExchangeRecord {
        exchange_id: exchange_id(exchange_json),
        flow_id: flow_id(exchange_json)?,
        direction: value_at_path(exchange_json, &["exchangeDirection"])
            .and_then(value_to_trimmed_string)
            .unwrap_or_default(),
        amount: exchange_amount(exchange_json),
        allocation_fraction: allocation_fraction(
            exchange_json,
            reference_exchange_id,
            valid_exchange_internal_ids,
            reference_exchange_count,
        ),
    })
}

fn exchange_id(exchange_json: &Value) -> Option<String> {
    value_at_path(exchange_json, &["@dataSetInternalID"]).and_then(value_to_trimmed_string)
}

fn flow_id(exchange_json: &Value) -> Option<Uuid> {
    value_at_path(exchange_json, &["referenceToFlowDataSet", "@refObjectId"])
        .or_else(|| value_at_path(exchange_json, &["referenceToFlowDataSet", "refObjectId"]))
        .and_then(value_to_trimmed_string)
        .and_then(|raw| raw.parse::<Uuid>().ok())
}

fn exchange_amount(exchange_json: &Value) -> Option<String> {
    crate::tidas_process_semantics::preferred_calculation_amount_value(exchange_json)
        .and_then(value_to_trimmed_string)
}

fn allocation_fraction(
    exchange_json: &Value,
    reference_exchange_id: Option<&str>,
    valid_exchange_internal_ids: &HashSet<String>,
    reference_exchange_count: usize,
) -> Option<String> {
    exchange_json.get("allocations")?;
    let Some(reference_exchange_id) = reference_exchange_id else {
        return Some("invalid:missing quantitative reference".to_owned());
    };
    match crate::tidas_process_semantics::resolve_tidas_exchange_allocation(
        exchange_json,
        reference_exchange_id,
        valid_exchange_internal_ids,
        reference_exchange_count,
    ) {
        Ok(
            crate::tidas_process_semantics::TidasAllocationResolution::Undeclared
            | crate::tidas_process_semantics::TidasAllocationResolution::LegacyEmptyUndeclared,
        ) => None,
        Ok(
            crate::tidas_process_semantics::TidasAllocationResolution::Explicit { fraction }
            | crate::tidas_process_semantics::TidasAllocationResolution::LegacyInferredReference {
                fraction,
            },
        ) => Some((fraction * 100.0).to_string()),
        Ok(crate::tidas_process_semantics::TidasAllocationResolution::SparseZero) => {
            Some("0".to_owned())
        }
        Err(error) => Some(format!("invalid:{error}")),
    }
}

fn parse_process_name(process_json: &Value) -> Option<String> {
    value_at_path(
        process_json,
        &[
            "processDataSet",
            "processInformation",
            "dataSetInformation",
            "name",
            "baseName",
        ],
    )
    .and_then(|value| match value {
        Value::Array(items) => items.first(),
        Value::Object(_) => Some(value),
        _ => None,
    })
    .and_then(|value| value.get("#text").or(Some(value)))
    .and_then(value_to_trimmed_string)
}

fn value_at_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn value_to_trimmed_string(value: &Value) -> Option<String> {
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Array(items) => return items.iter().find_map(value_to_trimmed_string),
        Value::Object(_) => return value.get("#text").and_then(value_to_trimmed_string),
        Value::Null => return None,
    };
    let trimmed = text.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

pub fn stable_json_sha256(value: &Value) -> anyhow::Result<String> {
    let normalized = sorted_json(value);
    let bytes = serde_json::to_vec(&normalized)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(hex::encode(hasher.finalize()))
}

fn sorted_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(sorted_json).collect()),
        Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(left, _)| *left);
            let sorted = entries
                .into_iter()
                .map(|(key, item)| (key.clone(), sorted_json(item)))
                .collect::<Map<_, _>>();
            Value::Object(sorted)
        }
        _ => value.clone(),
    }
}

fn utc_now_text() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        GateExecutionOutcome, ProcessRevision, RecordedGateStatus, ReviewExchangeRecord,
        ReviewProcessRecord, ReviewSubmitGateRun, ReviewSubmitGateStatus,
        build_review_process_record, stable_json_sha256, worker_job_result_for_outcome,
    };

    #[test]
    fn stable_json_sha256_ignores_object_key_order() {
        let left = json!({
            "b": 2,
            "a": {
                "y": [3, { "d": true, "c": null }],
                "x": "value"
            }
        });
        let right = json!({
            "a": {
                "x": "value",
                "y": [3, { "c": null, "d": true }]
            },
            "b": 2
        });

        assert_eq!(
            stable_json_sha256(&left).unwrap(),
            stable_json_sha256(&right).unwrap()
        );
    }

    #[test]
    fn build_review_process_record_extracts_ilcd_exchange_details() {
        let process_id = Uuid::new_v4();
        let flow_id = Uuid::new_v4();
        let revision = ProcessRevision {
            process_id,
            process_version: "01.00.000".to_owned(),
            state_code: Some(100),
            json_ordered: json!({
                "processDataSet": {
                    "processInformation": {
                        "dataSetInformation": {
                            "name": {
                                "baseName": { "#text": "Test process" }
                            }
                        },
                        "quantitativeReference": {
                            "referenceToReferenceFlow": "1"
                        }
                    },
                    "exchanges": {
                        "exchange": {
                            "@dataSetInternalID": "1",
                            "referenceToFlowDataSet": {
                                "@refObjectId": flow_id
                            },
                            "exchangeDirection": "Output",
                            "meanAmount": "1.5",
                            "allocations": {
                                "allocation": {
                                    "@internalReferenceToCoProduct": "1",
                                    "@allocatedFraction": "100"
                                }
                            }
                        }
                    }
                }
            }),
        };

        let record = build_review_process_record(&revision, Some(7));

        assert_eq!(record.process_idx, Some(7));
        assert_eq!(record.process_id, process_id);
        assert_eq!(record.process_name.as_deref(), Some("Test process"));
        assert_eq!(record.reference_exchange_id.as_deref(), Some("1"));
        assert_eq!(record.exchanges.len(), 1);
        assert_eq!(record.exchanges[0].flow_id, flow_id);
        assert_eq!(record.exchanges[0].direction, "Output");
        assert_eq!(record.exchanges[0].amount.as_deref(), Some("1.5"));
        assert_eq!(
            record.exchanges[0].allocation_fraction.as_deref(),
            Some("100")
        );
    }

    #[test]
    fn build_review_process_record_projects_safe_legacy_allocation_fallbacks() {
        let reference_flow = Uuid::new_v4();
        let input_flow = Uuid::new_v4();
        let revision = ProcessRevision {
            process_id: Uuid::new_v4(),
            process_version: "01.00.000".to_owned(),
            state_code: Some(100),
            json_ordered: json!({
                "processDataSet": {
                    "processInformation": {
                        "quantitativeReference": { "referenceToReferenceFlow": "1" }
                    },
                    "exchanges": {
                        "exchange": [
                            {
                                "@dataSetInternalID": "1",
                                "referenceToFlowDataSet": { "@refObjectId": reference_flow },
                                "exchangeDirection": "Output",
                                "resultingAmount": "1",
                                "allocations": { "allocation": {} }
                            },
                            {
                                "@dataSetInternalID": "2",
                                "referenceToFlowDataSet": { "@refObjectId": input_flow },
                                "exchangeDirection": "Input",
                                "resultingAmount": "5",
                                "allocations": {
                                    "allocation": { "@allocatedFraction": "100%" }
                                }
                            }
                        ]
                    }
                }
            }),
        };

        let record = build_review_process_record(&revision, Some(0));
        assert_eq!(record.exchanges[0].allocation_fraction, None);
        assert_eq!(
            record.exchanges[1].allocation_fraction.as_deref(),
            Some("100")
        );
    }

    #[test]
    fn build_review_process_record_infers_targetless_allocation_from_unique_reference() {
        let reference_flow = Uuid::new_v4();
        let coproduct_flow = Uuid::new_v4();
        let input_flow = Uuid::new_v4();
        let revision = ProcessRevision {
            process_id: Uuid::new_v4(),
            process_version: "01.00.000".to_owned(),
            state_code: Some(100),
            json_ordered: json!({
                "processDataSet": {
                    "processInformation": {
                        "quantitativeReference": { "referenceToReferenceFlow": "1" }
                    },
                    "exchanges": {
                        "exchange": [
                            {
                                "@dataSetInternalID": "1",
                                "referenceToFlowDataSet": { "@refObjectId": reference_flow },
                                "exchangeDirection": "Output",
                                "resultingAmount": "1"
                            },
                            {
                                "referenceToFlowDataSet": { "@refObjectId": coproduct_flow },
                                "exchangeDirection": "Output",
                                "resultingAmount": "1"
                            },
                            {
                                "@dataSetInternalID": "3",
                                "referenceToFlowDataSet": { "@refObjectId": input_flow },
                                "exchangeDirection": "Input",
                                "resultingAmount": "5",
                                "allocations": {
                                    "allocation": { "@allocatedFraction": "100" }
                                }
                            }
                        ]
                    }
                }
            }),
        };

        let record = build_review_process_record(&revision, Some(0));
        let allocation = record.exchanges[2]
            .allocation_fraction
            .as_deref()
            .expect("inferred allocation projection");
        assert_eq!(allocation, "100");
    }

    #[test]
    fn build_review_process_record_selects_targeted_allocation_and_sparse_zero() {
        let flow_a = Uuid::new_v4();
        let flow_b = Uuid::new_v4();
        let input_flow = Uuid::new_v4();
        let revision = ProcessRevision {
            process_id: Uuid::new_v4(),
            process_version: "01.00.000".to_owned(),
            state_code: Some(100),
            json_ordered: json!({
                "processDataSet": {
                    "processInformation": {
                        "quantitativeReference": { "referenceToReferenceFlow": "2" }
                    },
                    "exchanges": {
                        "exchange": [
                            {
                                "@dataSetInternalID": "1",
                                "referenceToFlowDataSet": { "@refObjectId": flow_a },
                                "exchangeDirection": "Output",
                                "resultingAmount": "1"
                            },
                            {
                                "@dataSetInternalID": "2",
                                "referenceToFlowDataSet": { "@refObjectId": flow_b },
                                "exchangeDirection": "Output",
                                "resultingAmount": "1"
                            },
                            {
                                "@dataSetInternalID": "3",
                                "referenceToFlowDataSet": { "@refObjectId": input_flow },
                                "exchangeDirection": "Input",
                                "resultingAmount": "5",
                                "allocations": {
                                    "allocation": [
                                        {
                                            "@internalReferenceToCoProduct": "1",
                                            "@allocatedFraction": "60"
                                        },
                                        {
                                            "@internalReferenceToCoProduct": "2",
                                            "@allocatedFraction": "40"
                                        }
                                    ]
                                }
                            },
                            {
                                "@dataSetInternalID": "4",
                                "referenceToFlowDataSet": { "@refObjectId": input_flow },
                                "exchangeDirection": "Input",
                                "resultingAmount": "2",
                                "allocations": {
                                    "allocation": {
                                        "@internalReferenceToCoProduct": "1",
                                        "@allocatedFraction": "100"
                                    }
                                }
                            }
                        ]
                    }
                }
            }),
        };

        let record = build_review_process_record(&revision, Some(4));

        assert_eq!(
            record.exchanges[2].allocation_fraction.as_deref(),
            Some("40")
        );
        assert_eq!(
            record.exchanges[3].allocation_fraction.as_deref(),
            Some("0")
        );
    }

    #[test]
    fn allocation_preflight_blocks_invalid_declared_vector_before_snapshot_build() {
        let flow_id = Uuid::new_v4();
        let run = ReviewSubmitGateRun {
            id: Uuid::new_v4(),
            dataset_table: "processes".to_owned(),
            dataset_id: Uuid::new_v4(),
            dataset_version: "01.00.000".to_owned(),
            revision_checksum: Some("a".repeat(64)),
            policy_profile: super::REVIEW_SUBMIT_GATE_POLICY_PROFILE.to_owned(),
            report_schema_version: super::REVIEW_SUBMIT_GATE_REPORT_SCHEMA_VERSION.to_owned(),
            requested_by: Uuid::new_v4(),
        };
        let record = ReviewProcessRecord {
            process_idx: None,
            process_id: run.dataset_id,
            process_version: run.dataset_version.clone(),
            process_name: None,
            state_code: Some(0),
            reference_exchange_id: Some("1".to_owned()),
            exchanges: vec![ReviewExchangeRecord {
                exchange_id: Some("2".to_owned()),
                flow_id,
                direction: "Input".to_owned(),
                amount: Some("1".to_owned()),
                allocation_fraction: Some("invalid:duplicate allocation target".to_owned()),
            }],
        };

        let report =
            super::allocation_preflight_report(&run, &record, &"a".repeat(64), &"a".repeat(64))
                .expect("preflight blocker");

        assert_eq!(report.status, ReviewSubmitGateStatus::Blocked);
        assert_eq!(
            report
                .metrics
                .process_scan
                .invalid_allocation_fraction_count,
            1
        );
        assert_eq!(report.blockers[0].code, "invalid_allocation_fraction");
        assert!(report.snapshot_id.is_none());
    }

    #[test]
    fn runtime_error_outcome_uses_error_status_and_array_reasons() {
        let run = ReviewSubmitGateRun {
            id: Uuid::new_v4(),
            dataset_table: "lifecyclemodels".to_owned(),
            dataset_id: Uuid::new_v4(),
            dataset_version: "01.00.000".to_owned(),
            revision_checksum: Some("a".repeat(64)),
            policy_profile: super::REVIEW_SUBMIT_GATE_POLICY_PROFILE.to_owned(),
            report_schema_version: super::REVIEW_SUBMIT_GATE_REPORT_SCHEMA_VERSION.to_owned(),
            requested_by: Uuid::new_v4(),
        };

        let outcome = super::runtime_error_outcome(
            &run,
            "unsupported_dataset_table",
            "unsupported table",
            json!({ "dataset_table": run.dataset_table }),
        )
        .unwrap();

        assert_eq!(outcome.status, RecordedGateStatus::Error);
        assert!(outcome.blocking_reasons.is_array());
        assert_eq!(outcome.calculator_report["status"], "error");
    }

    #[test]
    fn worker_job_result_maps_blocked_report_to_blocker_codes() {
        let run = ReviewSubmitGateRun {
            id: Uuid::new_v4(),
            dataset_table: "processes".to_owned(),
            dataset_id: Uuid::new_v4(),
            dataset_version: "01.00.000".to_owned(),
            revision_checksum: None,
            policy_profile: super::REVIEW_SUBMIT_GATE_POLICY_PROFILE.to_owned(),
            report_schema_version: super::REVIEW_SUBMIT_GATE_REPORT_SCHEMA_VERSION.to_owned(),
            requested_by: Uuid::new_v4(),
        };
        let outcome = GateExecutionOutcome {
            status: RecordedGateStatus::Blocked,
            calculator_report: json!({
                "status": "blocked",
                "blockers": [{ "code": "service_loop_detected" }]
            }),
            blocking_reasons: json!([{ "code": "service_loop_detected" }]),
            audit: json!({ "runner": "review_submit_gate_runner" }),
            authoritative_revision_checksum: Some("b".repeat(64)),
        };

        let result = worker_job_result_for_outcome(&run, &outcome);

        assert_eq!(result.status, "blocked");
        assert_eq!(result.blocker_codes, vec!["service_loop_detected"]);
        assert_eq!(result.resolution_scope.as_deref(), Some("user"));
        assert_eq!(result.retryable, Some(true));
        assert_eq!(
            result
                .result_json
                .as_ref()
                .unwrap()
                .pointer("/datasetRevision/revisionChecksum")
                .and_then(serde_json::Value::as_str),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
    }
}
