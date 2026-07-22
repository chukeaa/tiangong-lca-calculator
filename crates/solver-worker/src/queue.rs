use std::sync::Arc;

use crate::pgbouncer_sqlx::{self as sqlx, Row};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

use crate::{
    calculation_evidence::{
        PublicOwnerDraftBuildRequest, canonical_json_sha256, expected_scope_manifest,
        validate_calculation_evidence, validate_public_owner_draft_build_request,
    },
    db::{
        AppState, archive_queue_message, fetch_snapshot_index_document, handle_job_payload,
        handle_lcia_result_package_build_worker_job, handle_worker_jobs_job_payload,
        latest_result_id_for_job, mark_result_cache_failed, read_one_queue_message,
        update_job_status,
    },
    scope_closure::{
        SCOPE_CLOSURE_JOB_KIND, SCOPE_CLOSURE_REQUEST_SCHEMA_VERSION, execute_scope_closure_job,
        record_scope_closure_failure,
    },
    types::JobPayload,
    worker_jobs::{WorkerJob, WorkerJobResult, claim_worker_jobs, record_worker_job_result},
};

const SOLVER_WORKER_QUEUE: &str = "solver";

const LCIA_RESULT_PACKAGE_BUILD_V2: &str = "lcia_result.package_build.request.v2";

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AuthoritativePackageClosureBinding {
    closure_check_id: Uuid,
    certificate_hash: String,
    requested_scope_hash: String,
    policy_fingerprint: String,
    effective_scope_hash: String,
    data_snapshot_token: String,
    snapshot_id: Uuid,
    snapshot_hash: String,
    snapshot_artifact_id: Uuid,
    snapshot_index_sha256: String,
    snapshot_build_contract_hash: String,
    closure_bundle_artifact_id: Uuid,
    closure_bundle_hash: String,
    coverage_mode: String,
    lcia_method_set: Value,
    input_manifest: Value,
    input_manifest_hash: String,
    effective_scope: Value,
}

fn extract_snapshot_id(payload: &JobPayload) -> Option<Uuid> {
    match payload {
        JobPayload::PrepareFactorization { snapshot_id, .. }
        | JobPayload::SolveOne { snapshot_id, .. }
        | JobPayload::SolveBatch { snapshot_id, .. }
        | JobPayload::SolveAllUnit { snapshot_id, .. }
        | JobPayload::AnalyzeContributionPath { snapshot_id, .. }
        | JobPayload::InvalidateFactorization { snapshot_id, .. }
        | JobPayload::RebuildFactorization { snapshot_id, .. } => Some(*snapshot_id),
        JobPayload::BuildSnapshot { .. }
        | JobPayload::LciaResultPackageBuild { .. }
        | JobPayload::ScopeClosureCheck { .. } => None,
    }
}

fn calculation_evidence_binding_for_payload(payload: &JobPayload) -> Option<Value> {
    match payload {
        JobPayload::SolveOne {
            calculation_evidence_binding,
            ..
        }
        | JobPayload::SolveBatch {
            calculation_evidence_binding,
            ..
        }
        | JobPayload::SolveAllUnit {
            calculation_evidence_binding,
            ..
        }
        | JobPayload::AnalyzeContributionPath {
            calculation_evidence_binding,
            ..
        } => calculation_evidence_binding
            .as_ref()
            .and_then(|evidence| serde_json::to_value(evidence).ok()),
        _ => None,
    }
}

/// Fetches snapshot coverage from `lca_snapshot_artifacts` for richer error diagnostics.
async fn fetch_snapshot_coverage(pool: &sqlx::PgPool, snapshot_id: Uuid) -> Option<Value> {
    sqlx::query_scalar::<Value>(
        "SELECT coverage FROM public.lca_snapshot_artifacts \
         WHERE snapshot_id = $1 AND status = 'ready' \
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(snapshot_id)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
}

/// Detects processes with identical exchange structures within a snapshot's scope.
///
/// Returns groups of processes whose `(flow_id, direction, amount)` tuples are identical,
/// which produce linearly dependent columns in the technosphere matrix.
async fn detect_duplicate_exchange_processes(
    pool: &sqlx::PgPool,
    process_ids: &[Uuid],
    process_versions: &[String],
) -> Option<Value> {
    if process_ids.is_empty() || process_ids.len() != process_versions.len() {
        return None;
    }
    let result = sqlx::query_scalar::<Value>(
        r"
        scope_procs AS (
            SELECT p.id, p.version, p.json
            FROM unnest($1::uuid[], $2::text[]) AS scope(id, version)
            INNER JOIN public.processes p
              ON p.id = scope.id
             AND p.version = scope.version::bpchar
            WHERE p.json ? 'processDataSet'
        ),
        exchange_fp AS (
            SELECT
                sp.id AS process_id,
                sp.version,
                COALESCE(
                    sp.json #>> '{processDataSet,processInformation,dataSetInformation,name,baseName}',
                    ''
                ) AS name,
                md5((SELECT jsonb_agg(
                    jsonb_build_object(
                        'f', ex.value -> 'referenceToFlowDataSet' ->> '@refObjectId',
                        'd', ex.value ->> 'exchangeDirection',
                        'a', COALESCE(ex.value ->> 'meanAmount', ex.value ->> 'resultingAmount', '')
                    ) ORDER BY
                        ex.value -> 'referenceToFlowDataSet' ->> '@refObjectId',
                        ex.value ->> 'exchangeDirection'
                ) FROM jsonb_array_elements(
                    CASE jsonb_typeof(sp.json #> '{processDataSet,exchanges,exchange}')
                        WHEN 'array' THEN sp.json #> '{processDataSet,exchanges,exchange}'
                        ELSE '[]'::jsonb
                    END
                ) ex)::text) AS fp
            FROM scope_procs sp
        ),
        dup_groups AS (
            SELECT fp, jsonb_agg(jsonb_build_object(
                'process_id', process_id,
                'version', version,
                'name', name
            ) ORDER BY process_id) AS processes, COUNT(*) AS cnt
            FROM exchange_fp
            GROUP BY fp
            HAVING COUNT(*) > 1
        )
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
            'count', cnt,
            'processes', processes
        ) ORDER BY cnt DESC), '[]'::jsonb)
        FROM dup_groups
        ",
    )
    .bind(process_ids)
    .bind(process_versions)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    // Only return non-empty arrays.
    result.filter(|v| v.as_array().is_some_and(|a| !a.is_empty()))
}

/// Detects service-loop processes within a snapshot's scope.
///
/// A service-loop is when the same `flow_id` appears as both Input and Output
/// in the same process with identical amounts — the process "provides to itself".
/// This creates numerical instability (negative activities) in the solver.
async fn detect_service_loop_processes(
    pool: &sqlx::PgPool,
    process_ids: &[Uuid],
    process_versions: &[String],
) -> Option<Value> {
    if process_ids.is_empty() || process_ids.len() != process_versions.len() {
        return None;
    }
    let result = sqlx::query_scalar::<Value>(
        r"
        scope_procs AS (
            SELECT p.id, p.version, p.json
            FROM unnest($1::uuid[], $2::text[]) AS scope(id, version)
            INNER JOIN public.processes p
              ON p.id = scope.id
             AND p.version = scope.version::bpchar
            WHERE p.json ? 'processDataSet'
        ),
        exchanges AS (
            SELECT
                sp.id AS process_id,
                COALESCE(
                    sp.json #>> '{processDataSet,processInformation,dataSetInformation,name,baseName}',
                    ''
                ) AS process_name,
                ex.value ->> 'exchangeDirection' AS direction,
                ex.value -> 'referenceToFlowDataSet' ->> '@refObjectId' AS flow_id,
                COALESCE(
                    ex.value -> 'referenceToFlowDataSet' -> 'common:shortDescription' ->> '#text',
                    ex.value -> 'referenceToFlowDataSet' -> 'shortDescription' ->> '#text',
                    ''
                ) AS flow_name,
                trim(replace(replace(
                    COALESCE(ex.value ->> 'resultingAmount', ex.value ->> 'meanAmount', ''),
                    chr(160), ''), ',', '')) AS amount_text
            FROM scope_procs sp,
            LATERAL jsonb_array_elements(
                CASE jsonb_typeof(sp.json #> '{processDataSet,exchanges,exchange}')
                    WHEN 'array' THEN sp.json #> '{processDataSet,exchanges,exchange}'
                    ELSE '[]'::jsonb
                END
            ) ex(value)
        )
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
            'process_id', i.process_id,
            'process_name', i.process_name,
            'flow_id', i.flow_id,
            'flow_name', i.flow_name,
            'amount', i.amount_text
        ) ORDER BY i.process_id), '[]'::jsonb)
        FROM exchanges i
        JOIN exchanges o
          ON i.process_id = o.process_id
         AND i.flow_id = o.flow_id
         AND i.direction = 'Input'
         AND o.direction = 'Output'
        WHERE i.amount_text <> ''
          AND i.amount_text = o.amount_text
        ",
    )
    .bind(process_ids)
    .bind(process_versions)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    result.filter(|v| v.as_array().is_some_and(|a| !a.is_empty()))
}

/// Builds enriched diagnostics JSON when a job fails with a factorization error.
async fn build_failure_diagnostics(
    state: &AppState,
    payload: &JobPayload,
    err_message: &str,
) -> Value {
    let mut diag = serde_json::json!({"error": err_message});

    // For factorization/singular errors, attach snapshot coverage and problem process info.
    if (err_message.contains("singular") || err_message.contains("factorization"))
        && let Some(snapshot_id) = extract_snapshot_id(payload)
    {
        diag["snapshot_id"] = serde_json::json!(snapshot_id.to_string());
        if let Some(coverage) = fetch_snapshot_coverage(&state.pool, snapshot_id).await {
            diag["snapshot_coverage"] = coverage;
        }
        if let Ok(index) = fetch_snapshot_index_document(state, snapshot_id).await {
            let (process_ids, process_versions) = snapshot_diagnostic_scope_pairs(&index);
            diag["diagnostic_scope"] = json!({
                "source": "snapshot_index.process_map",
                "process_count": process_ids.len(),
            });
            if let Some(duplicates) =
                detect_duplicate_exchange_processes(&state.pool, &process_ids, &process_versions)
                    .await
            {
                diag["duplicate_exchange_processes"] = duplicates;
            }
            if let Some(loops) =
                detect_service_loop_processes(&state.pool, &process_ids, &process_versions).await
            {
                diag["service_loop_processes"] = loops;
            }
        }
    }

    diag
}

fn snapshot_diagnostic_scope_pairs(
    index: &crate::snapshot_index::SnapshotIndexDocument,
) -> (Vec<Uuid>, Vec<String>) {
    (
        index
            .process_map
            .iter()
            .map(|process| process.process_id)
            .collect(),
        index
            .process_map
            .iter()
            .map(|process| process.process_version.clone())
            .collect(),
    )
}

#[instrument(skip(state))]
pub async fn run_solver_worker_jobs_loop(
    state: Arc<AppState>,
    worker_id: String,
    claim_limit: i32,
    lease_seconds: i32,
    poll_interval: std::time::Duration,
) -> anyhow::Result<()> {
    loop {
        match claim_worker_jobs(
            &state.pool,
            SOLVER_WORKER_QUEUE,
            &worker_id,
            claim_limit,
            lease_seconds,
        )
        .await
        {
            Ok(jobs) if jobs.is_empty() => {
                sleep(poll_interval).await;
            }
            Ok(jobs) => {
                for job in jobs {
                    Box::pin(process_solver_worker_job(&state, job, lease_seconds)).await;
                }
            }
            Err(err) => {
                error!(error = %err, "worker_jobs claim error");
                sleep(poll_interval).await;
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn process_solver_worker_job(state: &AppState, job: WorkerJob, lease_seconds: i32) {
    let payload = match solver_worker_job_payload(&job) {
        Ok(payload) => payload,
        Err(err) => {
            let err_message = err.to_string();
            record_invalid_solver_worker_job_payload(state, &job, &err_message).await;
            return;
        }
    };

    let lca_job_id = extract_job_id(&payload);
    let phase = solver_worker_phase(&payload);
    let heartbeat_details = match &payload {
        JobPayload::LciaResultPackageBuild { build_id, .. } => json!({
            "buildId": build_id,
            "payloadType": payload_type_name(&payload),
        }),
        JobPayload::ScopeClosureCheck {
            closure_check_id, ..
        } => json!({
            "closureCheckId": closure_check_id,
            "payloadType": payload_type_name(&payload),
        }),
        _ => json!({
            "lcaJobId": lca_job_id,
            "payloadType": payload_type_name(&payload),
        }),
    };
    if let Err(err) = crate::worker_jobs::heartbeat_worker_job(
        &state.pool,
        job.id,
        job.lease_token,
        phase,
        0.05,
        Some(heartbeat_details),
        lease_seconds,
    )
    .await
    {
        error!(
            error = %err,
            worker_job_id = %job.id,
            lca_job_id = %lca_job_id,
            "failed to heartbeat solver worker_jobs row before execution"
        );
        return;
    }

    if let JobPayload::ScopeClosureCheck {
        closure_check_id,
        scan_execution_id,
        data_snapshot_token,
        request_fingerprint,
    } = &payload
    {
        match execute_scope_closure_job(
            state,
            job.id,
            job.lease_token,
            lease_seconds,
            *closure_check_id,
            *scan_execution_id,
            data_snapshot_token,
            request_fingerprint,
        )
        .await
        {
            Ok(outcome) => {
                info!(
                    worker_job_id = %job.id,
                    closure_check_id = %closure_check_id,
                    status = %outcome.status,
                    "scope closure worker job completed"
                );
            }
            Err(err) => {
                let message = err.to_string();
                error!(
                    error = %message,
                    worker_job_id = %job.id,
                    closure_check_id = %closure_check_id,
                    "scope closure worker job failed"
                );
                if let Err(record_err) = record_scope_closure_failure(
                    &state.pool,
                    *closure_check_id,
                    job.id,
                    job.lease_token,
                    message.as_str(),
                )
                .await
                {
                    error!(
                        error = %record_err,
                        worker_job_id = %job.id,
                        "failed to record scope closure terminal failure"
                    );
                }
            }
        }
        return;
    }

    let execution_result = match &payload {
        JobPayload::LciaResultPackageBuild { .. } => {
            let closure_binding_result = if job.payload_schema_version
                == LCIA_RESULT_PACKAGE_BUILD_V2
            {
                match fetch_authoritative_package_closure_binding(&state.pool, job.id).await {
                    Ok(binding) => {
                        match validate_authoritative_package_closure_binding(&payload, &binding) {
                            Ok(()) => {
                                validate_authoritative_package_closure_hashes(&state.pool, &binding)
                                    .await
                            }
                            Err(error) => Err(error),
                        }
                    }
                    Err(error) => Err(error),
                }
            } else {
                Ok(())
            };
            match closure_binding_result {
                Ok(()) => handle_lcia_result_package_build_worker_job(state, job.id, &payload)
                    .await
                    .map(|_| ()),
                Err(err) => Err(err),
            }
        }
        JobPayload::ScopeClosureCheck { .. } => unreachable!("handled above"),
        _ => {
            Box::pin(handle_worker_jobs_job_payload(
                state,
                payload.clone(),
                job.id,
                job.lease_token,
                lease_seconds,
            ))
            .await
        }
    };

    match execution_result {
        Ok(()) => {
            record_solver_worker_job_success(state, &job, &payload, lca_job_id).await;
        }
        Err(err) => {
            record_solver_worker_job_failure(state, &job, &payload, lca_job_id, &err.to_string())
                .await;
        }
    }
}

async fn fetch_authoritative_package_closure_binding(
    pool: &sqlx::PgPool,
    build_worker_job_id: Uuid,
) -> anyhow::Result<AuthoritativePackageClosureBinding> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_build_binding($1) AS result
        FROM _service_role
        ",
    )
    .bind(build_worker_job_id)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    if result.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(anyhow::anyhow!(
            "svc_lcia_scope_closure_build_binding returned non-ok result: {result}"
        ));
    }
    let data = result
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("scope closure build binding RPC omitted data"))?;
    Ok(serde_json::from_value(data)?)
}

fn validate_authoritative_package_closure_binding(
    payload: &JobPayload,
    binding: &AuthoritativePackageClosureBinding,
) -> anyhow::Result<()> {
    let JobPayload::LciaResultPackageBuild {
        closure_check_id,
        closure_certificate_hash,
        requested_scope_hash,
        policy_fingerprint,
        effective_scope_hash,
        effective_scope,
        data_snapshot_token,
        snapshot_id,
        snapshot_hash,
        snapshot_artifact_id,
        snapshot_index_sha256,
        snapshot_build_contract_hash,
        closure_bundle_artifact_id,
        closure_bundle_hash,
        coverage_mode,
        input_manifest_hash,
        input_manifest,
        lcia_method_set,
        ..
    } = payload
    else {
        return Err(anyhow::anyhow!(
            "authoritative package closure binding requires a package-build payload"
        ));
    };

    let matches_binding = closure_check_id == &Some(binding.closure_check_id)
        && closure_certificate_hash.as_deref() == Some(binding.certificate_hash.as_str())
        && requested_scope_hash.as_deref() == Some(binding.requested_scope_hash.as_str())
        && policy_fingerprint.as_deref() == Some(binding.policy_fingerprint.as_str())
        && effective_scope_hash.as_deref() == Some(binding.effective_scope_hash.as_str())
        && effective_scope.as_ref() == Some(&binding.effective_scope)
        && data_snapshot_token.as_deref() == Some(binding.data_snapshot_token.as_str())
        && snapshot_id == &Some(binding.snapshot_id)
        && snapshot_hash.as_deref() == Some(binding.snapshot_hash.as_str())
        && snapshot_artifact_id == &Some(binding.snapshot_artifact_id)
        && snapshot_index_sha256.as_deref() == Some(binding.snapshot_index_sha256.as_str())
        && snapshot_build_contract_hash.as_deref()
            == Some(binding.snapshot_build_contract_hash.as_str())
        && closure_bundle_artifact_id == &Some(binding.closure_bundle_artifact_id)
        && closure_bundle_hash.as_deref() == Some(binding.closure_bundle_hash.as_str())
        && coverage_mode == &binding.coverage_mode
        && input_manifest_hash == &binding.input_manifest_hash
        && input_manifest == &binding.input_manifest
        && lcia_method_set == &binding.lcia_method_set;
    if !matches_binding {
        return Err(anyhow::anyhow!(
            "package-build payload differs from authoritative scope-closure binding"
        ));
    }

    if binding.effective_scope.get("coverageMode")
        != Some(&Value::String(binding.coverage_mode.clone()))
    {
        return Err(anyhow::anyhow!(
            "authoritative coverage mode differs from effective scope"
        ));
    }
    if binding.effective_scope.get("lciaMethods") != Some(&binding.lcia_method_set) {
        return Err(anyhow::anyhow!(
            "authoritative LCIA method axis differs from effective scope"
        ));
    }

    let effective_processes = binding
        .effective_scope
        .get("processes")
        .ok_or_else(|| anyhow::anyhow!("authoritative effective scope omitted processes"))?;
    let input_processes = input_manifest
        .get("processes")
        .ok_or_else(|| anyhow::anyhow!("package input manifest omitted processes"))?;
    if input_processes != effective_processes {
        return Err(anyhow::anyhow!(
            "package input process axis differs from authoritative effective scope"
        ));
    }
    Ok(())
}

async fn validate_authoritative_package_closure_hashes(
    pool: &sqlx::PgPool,
    binding: &AuthoritativePackageClosureBinding,
) -> anyhow::Result<()> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.lcia_scope_closure_sha256($1::jsonb) AS effective_scope_hash,
               public.lcia_scope_closure_sha256($2::jsonb) AS input_manifest_hash
        FROM _service_role
        ",
    )
    .bind(&binding.effective_scope)
    .bind(&binding.input_manifest)
    .fetch_one(pool)
    .await?;
    if row.try_get::<String, _>("effective_scope_hash")? != binding.effective_scope_hash {
        return Err(anyhow::anyhow!(
            "authoritative effective scope does not match its database hash"
        ));
    }
    if row.try_get::<String, _>("input_manifest_hash")? != binding.input_manifest_hash {
        return Err(anyhow::anyhow!(
            "authoritative input manifest does not match its database hash"
        ));
    }
    Ok(())
}

async fn record_invalid_solver_worker_job_payload(
    state: &AppState,
    job: &WorkerJob,
    err_message: &str,
) {
    error!(
        error = %err_message,
        worker_job_id = %job.id,
        job_kind = %job.job_kind,
        "invalid solver worker_jobs payload"
    );
    let result = WorkerJobResult::failed(
        "invalid_solver_worker_job_payload",
        err_message.to_owned(),
        json!({
            "workerJobId": job.id,
            "jobKind": job.job_kind,
            "payloadSchemaVersion": job.payload_schema_version,
        }),
        Some(json!({"error": err_message})),
        None,
    );
    if let Err(record_err) =
        record_worker_job_result(&state.pool, job.id, job.lease_token, result).await
    {
        error!(error = %record_err, worker_job_id = %job.id, "failed to record invalid worker_jobs payload");
    }
}

async fn record_solver_worker_job_failure(
    state: &AppState,
    job: &WorkerJob,
    payload: &JobPayload,
    lca_job_id: Uuid,
    err_message: &str,
) {
    error!(
        error = %err_message,
        worker_job_id = %job.id,
        lca_job_id = %lca_job_id,
        "solver worker_jobs execution failed"
    );
    if let JobPayload::LciaResultPackageBuild {
        build_id,
        closure_check_id,
        ..
    } = payload
    {
        let diagnostics = json!({
            "error": err_message,
            "buildId": build_id,
        });
        let mut result = WorkerJobResult::failed(
            "lcia_result_package_build_failed",
            err_message.to_owned(),
            json!({
                "workerJobId": job.id,
                "buildId": build_id,
                "payloadType": payload_type_name(payload),
            }),
            Some(diagnostics),
            Some(json!({
                "workerJobId": job.id,
                "buildId": build_id,
                "payloadType": payload_type_name(payload),
            })),
        );
        result.result_ref = Some(lcia_result_package_worker_result_ref(
            job.id,
            *build_id,
            None,
            *closure_check_id,
        ));
        if let Err(record_err) =
            record_worker_job_result(&state.pool, job.id, job.lease_token, result).await
        {
            error!(error = %record_err, worker_job_id = %job.id, "failed to record lcia result package worker_jobs failure");
        }
        return;
    }

    let diagnostics = build_failure_diagnostics(state, payload, err_message).await;
    let _ = update_job_status(&state.pool, lca_job_id, "failed", diagnostics.clone()).await;
    let _ = mark_result_cache_failed(&state.pool, lca_job_id, "job_execution_failed", err_message)
        .await;
    if let Err(err) = link_lca_worker_job_domain_refs(&state.pool, job.id, lca_job_id).await {
        warn!(
            error = %err,
            worker_job_id = %job.id,
            lca_job_id = %lca_job_id,
            "failed to link solver domain rows to worker_jobs"
        );
    }
    let mut result = WorkerJobResult::failed(
        "solver_worker_job_failed",
        err_message.to_owned(),
        json!({
            "workerJobId": job.id,
            "lcaJobId": lca_job_id,
            "payloadType": payload_type_name(payload),
        }),
        Some(diagnostics),
        None,
    );
    result.result_ref = Some(solver_worker_result_ref(job.id, lca_job_id, None));
    if let Err(record_err) =
        record_worker_job_result(&state.pool, job.id, job.lease_token, result).await
    {
        error!(error = %record_err, worker_job_id = %job.id, "failed to record worker_jobs failure");
    }
}

async fn record_solver_worker_job_success(
    state: &AppState,
    job: &WorkerJob,
    payload: &JobPayload,
    lca_job_id: Uuid,
) {
    match build_solver_worker_job_result(state, job.id, payload).await {
        Ok(result) => {
            if let Err(err) =
                record_worker_job_result(&state.pool, job.id, job.lease_token, result).await
            {
                error!(error = %err, worker_job_id = %job.id, lca_job_id = %lca_job_id, "failed to record worker_jobs success");
            } else {
                info!(worker_job_id = %job.id, lca_job_id = %lca_job_id, "solver worker_jobs job completed");
            }
        }
        Err(err) => {
            let err_message = err.to_string();
            error!(
                error = %err_message,
                worker_job_id = %job.id,
                lca_job_id = %lca_job_id,
                "solver worker_jobs execution completed but result projection failed"
            );
            if let JobPayload::LciaResultPackageBuild {
                build_id,
                closure_check_id,
                ..
            } = payload
            {
                let mut result = WorkerJobResult::failed(
                    "lcia_result_package_projection_failed",
                    err_message.clone(),
                    json!({
                        "workerJobId": job.id,
                        "buildId": build_id,
                        "payloadType": payload_type_name(payload),
                    }),
                    Some(json!({"error": err_message})),
                    Some(json!({
                        "workerJobId": job.id,
                        "buildId": build_id,
                        "payloadType": payload_type_name(payload),
                    })),
                );
                result.result_ref = Some(lcia_result_package_worker_result_ref(
                    job.id,
                    *build_id,
                    None,
                    *closure_check_id,
                ));
                if let Err(record_err) =
                    record_worker_job_result(&state.pool, job.id, job.lease_token, result).await
                {
                    error!(error = %record_err, worker_job_id = %job.id, "failed to record lcia result package worker_jobs projection failure");
                }
                return;
            }

            let mut result = WorkerJobResult::failed(
                "solver_worker_job_projection_failed",
                err_message.clone(),
                json!({
                    "workerJobId": job.id,
                    "lcaJobId": lca_job_id,
                    "payloadType": payload_type_name(payload),
                }),
                Some(json!({"error": err_message})),
                None,
            );
            result.result_ref = Some(solver_worker_result_ref(job.id, lca_job_id, None));
            if let Err(record_err) =
                record_worker_job_result(&state.pool, job.id, job.lease_token, result).await
            {
                error!(error = %record_err, worker_job_id = %job.id, "failed to record worker_jobs projection failure");
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn build_solver_worker_job_result(
    state: &AppState,
    worker_job_id: Uuid,
    payload: &JobPayload,
) -> anyhow::Result<WorkerJobResult> {
    if let JobPayload::LciaResultPackageBuild {
        build_id,
        closure_check_id,
        ..
    } = payload
    {
        let package_projection =
            fetch_lcia_result_package_projection(&state.pool, worker_job_id).await?;
        let package_id = package_projection
            .get("packageId")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok());
        return Ok(WorkerJobResult {
            status: "completed".to_owned(),
            result_json: Some(json!({
                "workerJobId": worker_job_id,
                "buildId": build_id,
                "closureCheckId": closure_check_id,
                "payloadType": payload_type_name(payload),
                "package": package_projection,
            })),
            result_schema_version: Some(result_schema_version_for_payload(payload).to_owned()),
            result_ref: Some(lcia_result_package_worker_result_ref(
                worker_job_id,
                *build_id,
                package_id,
                *closure_check_id,
            )),
            diagnostics: Some(json!({
                "lciaResultPackage": package_projection,
                "closureCheckId": closure_check_id,
            })),
            error_code: None,
            error_message: None,
            error_details: None,
            blocker_codes: Vec::new(),
            resolution_scope: None,
            retryable: None,
        });
    }

    if let JobPayload::BuildSnapshot {
        job_id: lca_job_id,
        data_scope,
        ..
    } = payload
    {
        let diagnostics = fetch_worker_job_diagnostics(&state.pool, worker_job_id).await?;
        let (snapshot_id, calculation_evidence) =
            parse_build_snapshot_worker_projection(&diagnostics, data_scope.is_some())?;
        let result_json = json!({
            "workerJobId": worker_job_id,
            "lcaJobId": lca_job_id,
            "payloadType": payload_type_name(payload),
            "snapshotId": snapshot_id,
            "calculationEvidence": calculation_evidence.clone(),
        });
        return Ok(WorkerJobResult {
            status: "completed".to_owned(),
            result_json: Some(result_json),
            result_schema_version: Some(result_schema_version_for_payload(payload).to_owned()),
            result_ref: Some(json!({
                "domainSource": "worker_jobs",
                "workerJobId": worker_job_id,
                "lcaJobId": lca_job_id,
                "snapshot": {
                    "table": "lca_network_snapshots",
                    "id": snapshot_id,
                },
            })),
            diagnostics: Some(json!({
                "buildSnapshot": diagnostics.get("build_snapshot_result").cloned(),
                "calculationEvidence": calculation_evidence,
            })),
            error_code: None,
            error_message: None,
            error_details: None,
            blocker_codes: Vec::new(),
            resolution_scope: None,
            retryable: None,
        });
    }

    let lca_job_id = extract_job_id(payload);
    link_lca_worker_job_domain_refs(&state.pool, worker_job_id, lca_job_id).await?;
    let job_projection = fetch_lca_job_projection(&state.pool, lca_job_id).await?;
    let result_id = latest_result_id_for_job(&state.pool, lca_job_id).await?;
    let result_ref = solver_worker_result_ref(worker_job_id, lca_job_id, result_id);
    let snapshot_id = job_projection
        .get("snapshotId")
        .cloned()
        .filter(|value| !value.is_null())
        .or_else(|| extract_snapshot_id(payload).map(|id| json!(id)))
        .unwrap_or(Value::Null);
    let result_json = json!({
        "workerJobId": worker_job_id,
        "lcaJobId": lca_job_id,
        "payloadType": payload_type_name(payload),
        "snapshotId": snapshot_id,
        "lcaJobStatus": job_projection.get("status").cloned().unwrap_or(Value::Null),
        "resultId": result_id,
        "calculationEvidence": calculation_evidence_binding_for_payload(payload),
    });

    Ok(WorkerJobResult {
        status: "completed".to_owned(),
        result_json: Some(result_json),
        result_schema_version: Some(result_schema_version_for_payload(payload).to_owned()),
        result_ref: Some(result_ref),
        diagnostics: Some(json!({
            "lcaJob": job_projection,
            "calculationEvidence": calculation_evidence_binding_for_payload(payload),
        })),
        error_code: None,
        error_message: None,
        error_details: None,
        blocker_codes: Vec::new(),
        resolution_scope: None,
        retryable: None,
    })
}

async fn fetch_worker_job_diagnostics(
    pool: &sqlx::PgPool,
    worker_job_id: Uuid,
) -> anyhow::Result<Value> {
    let row = sqlx::query("SELECT diagnostics FROM public.worker_jobs WHERE id = $1")
        .bind(worker_job_id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("worker_jobs row disappeared before result projection"))?;
    Ok(row
        .try_get::<Option<Value>, _>("diagnostics")?
        .unwrap_or_else(|| json!({})))
}

fn parse_build_snapshot_worker_projection(
    diagnostics: &Value,
    requires_calculation_evidence: bool,
) -> anyhow::Result<(
    Uuid,
    Option<crate::calculation_evidence::LcaCalculationEvidence>,
)> {
    let result = diagnostics
        .get("build_snapshot_result")
        .ok_or_else(|| anyhow::anyhow!("worker_jobs build result diagnostics are missing"))?;
    let snapshot_id = result
        .get("resolved_snapshot_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("worker_jobs build result lacks resolved snapshot id"))?
        .parse::<Uuid>()?;
    let calculation_evidence = result
        .get("calculation_evidence")
        .filter(|value| !value.is_null())
        .cloned()
        .map(serde_json::from_value)
        .transpose()?;
    if requires_calculation_evidence {
        let evidence = calculation_evidence.as_ref().ok_or_else(|| {
            anyhow::anyhow!("versioned worker_jobs build result lacks calculation evidence")
        })?;
        validate_calculation_evidence(evidence)?;
    }
    Ok((snapshot_id, calculation_evidence))
}

async fn fetch_lcia_result_package_projection(
    pool: &sqlx::PgPool,
    worker_job_id: Uuid,
) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        SELECT id, package_version, status, build_id, snapshot_id, result_id,
               latest_all_unit_result_id, included_input_count, created_at
        FROM public.lcia_result_packages
        WHERE build_worker_job_id = $1
        ORDER BY created_at DESC
        LIMIT 1
        ",
    )
    .bind(worker_job_id)
    .fetch_optional(pool)
    .await;

    let Some(row) = (match row {
        Ok(row) => row,
        Err(err) if is_undefined_table(&err) => {
            return Err(anyhow::anyhow!(
                "lcia_result_packages table is missing for package worker result projection"
            ));
        }
        Err(err) => return Err(err.into()),
    }) else {
        return Err(anyhow::anyhow!(
            "lcia_result package not found for worker_job_id={worker_job_id}"
        ));
    };

    Ok(json!({
        "packageId": row.try_get::<Uuid, _>("id").ok(),
        "packageVersion": row.try_get::<String, _>("package_version").ok(),
        "status": row.try_get::<String, _>("status").ok(),
        "buildId": row.try_get::<Uuid, _>("build_id").ok(),
        "snapshotId": row.try_get::<Uuid, _>("snapshot_id").ok(),
        "resultId": row.try_get::<Uuid, _>("result_id").ok(),
        "latestAllUnitResultId": row.try_get::<Option<Uuid>, _>("latest_all_unit_result_id").ok().flatten(),
        "includedInputCount": row.try_get::<i32, _>("included_input_count").ok(),
    }))
}

fn lcia_result_package_worker_result_ref(
    worker_job_id: Uuid,
    build_id: Uuid,
    package_id: Option<Uuid>,
    closure_check_id: Option<Uuid>,
) -> Value {
    json!({
        "domainSource": "worker_jobs",
        "workerJobId": worker_job_id,
        "buildId": build_id,
        "closureCheckId": closure_check_id,
        "package": package_id.map(|id| json!({
            "table": "lcia_result_packages",
            "id": id,
        })),
    })
}

async fn link_lca_worker_job_domain_refs(
    pool: &sqlx::PgPool,
    worker_job_id: Uuid,
    lca_job_id: Uuid,
) -> anyhow::Result<()> {
    execute_optional_worker_job_ref_update(
        pool,
        r"
        UPDATE public.lca_jobs
           SET worker_job_id = $1
         WHERE id = $2
        ",
        worker_job_id,
        lca_job_id,
    )
    .await?;
    execute_optional_worker_job_ref_update(
        pool,
        r"
        UPDATE public.lca_results
           SET worker_job_id = $1
         WHERE job_id = $2
        ",
        worker_job_id,
        lca_job_id,
    )
    .await?;
    execute_optional_worker_job_ref_update(
        pool,
        r"
        UPDATE public.lca_result_cache
           SET worker_job_id = $1
         WHERE job_id = $2
        ",
        worker_job_id,
        lca_job_id,
    )
    .await?;
    execute_optional_worker_job_ref_update(
        pool,
        r"
        UPDATE public.lca_latest_all_unit_results
           SET worker_job_id = $1
         WHERE job_id = $2
        ",
        worker_job_id,
        lca_job_id,
    )
    .await?;
    execute_optional_worker_job_ref_update(
        pool,
        r"
        UPDATE public.lca_factorization_registry
           SET prepared_worker_job_id = $1
         WHERE prepared_job_id = $2
        ",
        worker_job_id,
        lca_job_id,
    )
    .await?;

    Ok(())
}

async fn execute_optional_worker_job_ref_update(
    pool: &sqlx::PgPool,
    statement: &str,
    worker_job_id: Uuid,
    compat_job_id: Uuid,
) -> anyhow::Result<()> {
    let result = sqlx::query(statement)
        .bind(worker_job_id)
        .bind(compat_job_id)
        .execute(pool)
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(err) if is_undefined_table(&err) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn solver_worker_result_ref(
    worker_job_id: Uuid,
    lca_job_id: Uuid,
    result_id: Option<Uuid>,
) -> Value {
    json!({
        "domainSource": "worker_jobs",
        "workerJobId": worker_job_id,
        "lcaJobId": lca_job_id,
        "result": result_id.map(|id| json!({
            "table": "lca_results",
            "id": id,
        })),
    })
}

async fn fetch_lca_job_projection(pool: &sqlx::PgPool, job_id: Uuid) -> anyhow::Result<Value> {
    let result = sqlx::query(
        r"
        SELECT status, job_type, snapshot_id, diagnostics
        FROM public.lca_jobs
        WHERE id = $1
        ",
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await;

    let row = match result {
        Ok(row) => row,
        Err(err) if is_undefined_table(&err) => {
            return Ok(json!({
                "id": job_id,
                "missing": true,
                "legacyTableMissing": true,
            }));
        }
        Err(err) => return Err(err.into()),
    };

    Ok(row.map_or_else(
        || json!({"id": job_id, "missing": true}),
        |row| {
            json!({
                "id": job_id,
                "status": row.try_get::<String, _>("status").ok(),
                "jobType": row.try_get::<String, _>("job_type").ok(),
                "snapshotId": row.try_get::<Uuid, _>("snapshot_id").ok(),
                "diagnostics": row.try_get::<Value, _>("diagnostics").ok(),
            })
        },
    ))
}

fn is_undefined_table(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().as_deref() == Some("42P01"),
        _ => false,
    }
}

fn solver_worker_job_payload(job: &WorkerJob) -> anyhow::Result<JobPayload> {
    if job.worker_queue != SOLVER_WORKER_QUEUE {
        return Err(anyhow::anyhow!(
            "unsupported worker queue for solver job: {}",
            job.worker_queue
        ));
    }

    if !payload_schema_supported_for_job_kind(&job.job_kind, &job.payload_schema_version) {
        return Err(anyhow::anyhow!(
            "unsupported payload schema for {}: {}",
            job.job_kind,
            job.payload_schema_version
        ));
    }

    let mut payload = normalize_worker_payload_object(job.payload.clone())?;
    if !payload.contains_key("type") {
        payload.insert(
            "type".to_owned(),
            Value::String(
                payload_type_for_job_kind(&job.job_kind)
                    .ok_or_else(|| {
                        anyhow::anyhow!("unsupported solver worker job kind: {}", job.job_kind)
                    })?
                    .to_owned(),
            ),
        );
    }

    let parsed = serde_json::from_value(Value::Object(payload))?;
    let expected_payload_type = payload_type_for_job_kind(&job.job_kind)
        .ok_or_else(|| anyhow::anyhow!("unsupported solver worker job kind: {}", job.job_kind))?;
    if payload_type_name(&parsed) != expected_payload_type {
        return Err(anyhow::anyhow!(
            "payload type mismatch for {}: expected={} actual={}",
            job.job_kind,
            expected_payload_type,
            payload_type_name(&parsed)
        ));
    }
    validate_versioned_payload_contract(&parsed, &job.payload_schema_version, job.requested_by)?;
    Ok(parsed)
}

#[allow(clippy::too_many_lines)]
fn normalize_worker_payload_object(value: Value) -> anyhow::Result<Map<String, Value>> {
    let Value::Object(mut payload) = value else {
        return Err(anyhow::anyhow!(
            "solver worker job payload must be an object"
        ));
    };

    copy_alias(&mut payload, "jobId", "job_id");
    copy_alias(&mut payload, "lcaJobId", "job_id");
    copy_alias(&mut payload, "snapshotId", "snapshot_id");
    copy_alias(&mut payload, "modelVersion", "snapshot_id");
    copy_alias(&mut payload, "rhsBatch", "rhs_batch");
    copy_alias(&mut payload, "unitBatchSize", "unit_batch_size");
    copy_alias(&mut payload, "printLevel", "print_level");
    copy_alias(&mut payload, "processId", "process_id");
    copy_alias(&mut payload, "processIndex", "process_index");
    copy_alias(&mut payload, "impactId", "impact_id");
    copy_alias(&mut payload, "impactIndex", "impact_index");
    copy_alias(&mut payload, "processStates", "process_states");
    copy_alias(&mut payload, "allStates", "all_states");
    copy_alias(&mut payload, "includeUserId", "include_user_id");
    copy_alias(
        &mut payload,
        "includeUserStateCodes",
        "include_user_state_codes",
    );
    copy_alias(
        &mut payload,
        "includeUserUnassignedOnly",
        "include_user_unassigned_only",
    );
    copy_alias(
        &mut payload,
        "includeUserReviewFreeOnly",
        "include_user_review_free_only",
    );
    copy_alias(&mut payload, "dataScope", "data_scope");
    copy_alias(&mut payload, "scopeManifest", "scope_manifest");
    copy_alias(&mut payload, "scopeManifestSha256", "scope_manifest_sha256");
    copy_alias(
        &mut payload,
        "lciaMethodFactorSource",
        "lcia_method_factor_source",
    );
    copy_alias(
        &mut payload,
        "lciaFactorCoverageContract",
        "lcia_factor_coverage_contract",
    );
    copy_alias(
        &mut payload,
        "calculationEvidenceBinding",
        "calculation_evidence_binding",
    );
    copy_alias(&mut payload, "requestRoots", "request_roots");
    copy_alias(&mut payload, "providerRule", "provider_rule");
    copy_alias(
        &mut payload,
        "referenceNormalizationMode",
        "reference_normalization_mode",
    );
    copy_alias(
        &mut payload,
        "allocationFractionMode",
        "allocation_fraction_mode",
    );
    copy_alias(&mut payload, "processLimit", "process_limit");
    copy_alias(&mut payload, "selfLoopCutoff", "self_loop_cutoff");
    copy_alias(&mut payload, "singularEps", "singular_eps");
    copy_alias(&mut payload, "methodId", "method_id");
    copy_alias(&mut payload, "methodVersion", "method_version");
    copy_alias(&mut payload, "noLcia", "no_lcia");
    copy_alias(&mut payload, "buildId", "build_id");
    copy_alias(&mut payload, "requestedBy", "requested_by");
    copy_alias(&mut payload, "coverageMode", "coverage_mode");
    copy_alias(&mut payload, "inputStatusFilter", "input_status_filter");
    copy_alias(
        &mut payload,
        "eligibilityDefinition",
        "eligibility_definition",
    );
    copy_alias(&mut payload, "eligibleInputCount", "eligible_input_count");
    copy_alias(&mut payload, "includedInputCount", "included_input_count");
    copy_alias(&mut payload, "inputManifestHash", "input_manifest_hash");
    copy_alias(&mut payload, "inputManifest", "input_manifest");
    copy_alias(&mut payload, "lciaMethodSet", "lcia_method_set");
    copy_alias(
        &mut payload,
        "defaultImpactCategory",
        "default_impact_category",
    );
    copy_alias(&mut payload, "postprocessManifest", "postprocess_manifest");
    copy_alias(&mut payload, "closureCheckId", "closure_check_id");
    copy_alias(&mut payload, "scanExecutionId", "scan_execution_id");
    copy_alias(
        &mut payload,
        "closureCertificateHash",
        "closure_certificate_hash",
    );
    copy_alias(&mut payload, "effectiveScopeHash", "effective_scope_hash");
    copy_alias(&mut payload, "dataSnapshotToken", "data_snapshot_token");
    copy_alias(&mut payload, "snapshotHash", "snapshot_hash");
    copy_alias(&mut payload, "closureBundleHash", "closure_bundle_hash");
    copy_alias(
        &mut payload,
        "closureBundleArtifactId",
        "closure_bundle_artifact_id",
    );
    copy_alias(
        &mut payload,
        "reportArtifactManifestHash",
        "report_artifact_manifest_hash",
    );
    copy_alias(&mut payload, "snapshotArtifactId", "snapshot_artifact_id");
    copy_alias(&mut payload, "snapshotIndexSha256", "snapshot_index_sha256");
    copy_alias(
        &mut payload,
        "snapshotBuildContractHash",
        "snapshot_build_contract_hash",
    );
    copy_alias(&mut payload, "requestedScopeHash", "requested_scope_hash");
    copy_alias(&mut payload, "policyFingerprint", "policy_fingerprint");
    copy_alias(
        &mut payload,
        "expectedValidatorScannerFingerprint",
        "expected_validator_scanner_fingerprint",
    );
    copy_alias(&mut payload, "requestFingerprint", "request_fingerprint");
    copy_alias(&mut payload, "requestKey", "request_key");
    normalize_request_roots(&mut payload);

    Ok(payload)
}

fn copy_alias(payload: &mut Map<String, Value>, alias: &str, canonical: &str) {
    if !payload.contains_key(canonical)
        && let Some(value) = payload.get(alias).cloned()
    {
        payload.insert(canonical.to_owned(), value);
    }
}

fn normalize_request_roots(payload: &mut Map<String, Value>) {
    let Some(Value::Array(roots)) = payload.get_mut("request_roots") else {
        return;
    };
    for root in roots {
        let Value::Object(root_obj) = root else {
            continue;
        };
        copy_alias(root_obj, "processId", "process_id");
        copy_alias(root_obj, "processVersion", "process_version");
        copy_alias(root_obj, "version", "process_version");
    }
}

fn payload_schema_supported_for_job_kind(job_kind: &str, schema: &str) -> bool {
    matches!(
        (job_kind, schema),
        (
            "lca.solve_one",
            "lca.solve_one.request.v1" | "lca.solve_one.request.v2"
        ) | ("lca.solve_batch", "lca.solve_batch.request.v1")
            | (
                "lca.solve_all_unit",
                "lca.solve_all_unit.request.v1" | "lca.solve_all_unit.request.v2"
            )
            | (
                "lca.build_snapshot",
                "lca.build_snapshot.request.v1" | "lca.build_snapshot.request.v2"
            )
            | (
                "lca.contribution_path",
                "lca.contribution_path.request.v1" | "lca.contribution_path.request.v2"
            )
            | (
                "lca.factorization_prepare",
                "lca.factorization_prepare.request.v1"
            )
            | (
                "lcia_result.package_build",
                "lcia_result.package_build.request.v1" | "lcia_result.package_build.request.v2"
            )
            | (SCOPE_CLOSURE_JOB_KIND, SCOPE_CLOSURE_REQUEST_SCHEMA_VERSION)
    )
}

#[allow(clippy::too_many_lines)]
fn validate_versioned_payload_contract(
    payload: &JobPayload,
    schema: &str,
    requested_by: Option<Uuid>,
) -> anyhow::Result<()> {
    match (schema, payload) {
        (
            "lca.build_snapshot.request.v2",
            JobPayload::BuildSnapshot {
                all_states,
                process_states,
                include_user_id,
                include_user_state_codes,
                include_user_unassigned_only,
                include_user_review_free_only,
                data_scope,
                scope_manifest,
                scope_manifest_sha256,
                lcia_method_factor_source,
                lcia_factor_coverage_contract,
                no_lcia,
                ..
            },
        ) => {
            validate_public_owner_draft_build_request(PublicOwnerDraftBuildRequest {
                all_states: *all_states,
                process_states: process_states.as_deref(),
                include_user_id: *include_user_id,
                include_user_state_codes: include_user_state_codes.as_deref(),
                include_user_unassigned_only: *include_user_unassigned_only,
                include_user_review_free_only: *include_user_review_free_only,
                data_scope: data_scope.as_deref(),
                scope_manifest: scope_manifest.as_ref(),
                scope_manifest_sha256: scope_manifest_sha256.as_deref(),
                lcia_method_factor_source: lcia_method_factor_source.as_ref(),
                lcia_factor_coverage_contract: lcia_factor_coverage_contract.as_ref(),
                no_lcia: *no_lcia,
                requested_by,
            })?;
        }
        (
            "lca.solve_one.request.v2"
            | "lca.solve_all_unit.request.v2"
            | "lca.contribution_path.request.v2",
            JobPayload::SolveOne {
                calculation_evidence_binding,
                ..
            }
            | JobPayload::SolveAllUnit {
                calculation_evidence_binding,
                ..
            }
            | JobPayload::AnalyzeContributionPath {
                calculation_evidence_binding,
                ..
            },
        ) => {
            let binding = calculation_evidence_binding
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("v2 solve requires calculation_evidence_binding"))?;
            validate_calculation_evidence(binding)?;
            let actor = requested_by
                .ok_or_else(|| anyhow::anyhow!("v2 solve requires authenticated requested_by"))?;
            let expected_scope_hash = canonical_json_sha256(&expected_scope_manifest(actor))?;
            if binding.scope_manifest_sha256 != expected_scope_hash {
                return Err(anyhow::anyhow!(
                    "v2 solve requested_by differs from snapshot scope actor"
                ));
            }
        }
        (
            "lca.build_snapshot.request.v1",
            JobPayload::BuildSnapshot {
                include_user_state_codes,
                include_user_unassigned_only,
                include_user_review_free_only,
                data_scope,
                scope_manifest,
                scope_manifest_sha256,
                lcia_method_factor_source,
                lcia_factor_coverage_contract,
                ..
            },
        ) if include_user_state_codes.is_some()
            || include_user_unassigned_only.is_some()
            || include_user_review_free_only.is_some()
            || data_scope.is_some()
            || scope_manifest.is_some()
            || scope_manifest_sha256.is_some()
            || lcia_method_factor_source.is_some()
            || lcia_factor_coverage_contract.is_some() =>
        {
            return Err(anyhow::anyhow!(
                "versioned scope fields cannot be carried by a v1 build payload"
            ));
        }
        (
            schema,
            JobPayload::SolveOne {
                calculation_evidence_binding,
                ..
            }
            | JobPayload::SolveBatch {
                calculation_evidence_binding,
                ..
            }
            | JobPayload::SolveAllUnit {
                calculation_evidence_binding,
                ..
            }
            | JobPayload::AnalyzeContributionPath {
                calculation_evidence_binding,
                ..
            },
        ) if schema.ends_with(".request.v1") && calculation_evidence_binding.is_some() => {
            return Err(anyhow::anyhow!(
                "calculation_evidence_binding cannot be carried by a v1 solve payload"
            ));
        }
        (
            "lcia_result.package_build.request.v1",
            JobPayload::LciaResultPackageBuild {
                closure_check_id,
                closure_certificate_hash,
                requested_scope_hash,
                policy_fingerprint,
                effective_scope_hash,
                effective_scope,
                data_snapshot_token,
                snapshot_id,
                snapshot_hash,
                closure_bundle_artifact_id,
                closure_bundle_hash,
                report_artifact_manifest_hash,
                snapshot_artifact_id,
                snapshot_index_sha256,
                snapshot_build_contract_hash,
                ..
            },
        ) => {
            let binding_count = [
                closure_check_id.is_some(),
                closure_certificate_hash.is_some(),
                requested_scope_hash.is_some(),
                policy_fingerprint.is_some(),
                effective_scope_hash.is_some(),
                effective_scope.is_some(),
                data_snapshot_token.is_some(),
                snapshot_id.is_some(),
                snapshot_hash.is_some(),
                closure_bundle_artifact_id.is_some(),
                closure_bundle_hash.is_some(),
                report_artifact_manifest_hash.is_some(),
                snapshot_artifact_id.is_some(),
                snapshot_index_sha256.is_some(),
                snapshot_build_contract_hash.is_some(),
            ]
            .into_iter()
            .filter(|present| *present)
            .count();
            if binding_count != 0 {
                return Err(anyhow::anyhow!(
                    "legacy lcia result package v1 payload cannot carry closure evidence; use request.v2"
                ));
            }
        }
        (
            "lcia_result.package_build.request.v2",
            JobPayload::LciaResultPackageBuild {
                closure_check_id,
                closure_certificate_hash,
                requested_scope_hash,
                policy_fingerprint,
                effective_scope_hash,
                effective_scope,
                data_snapshot_token,
                snapshot_id,
                snapshot_hash,
                closure_bundle_artifact_id,
                closure_bundle_hash,
                report_artifact_manifest_hash,
                snapshot_artifact_id,
                snapshot_index_sha256,
                snapshot_build_contract_hash,
                ..
            },
        ) => {
            let binding_count = [
                closure_check_id.is_some(),
                closure_certificate_hash.is_some(),
                requested_scope_hash.is_some(),
                policy_fingerprint.is_some(),
                effective_scope_hash.is_some(),
                effective_scope.is_some(),
                data_snapshot_token.is_some(),
                snapshot_id.is_some(),
                snapshot_hash.is_some(),
                closure_bundle_artifact_id.is_some(),
                closure_bundle_hash.is_some(),
                report_artifact_manifest_hash.is_some(),
                snapshot_artifact_id.is_some(),
                snapshot_index_sha256.is_some(),
                snapshot_build_contract_hash.is_some(),
            ]
            .into_iter()
            .filter(|present| *present)
            .count();
            if binding_count != 15 {
                return Err(anyhow::anyhow!(
                    "lcia result package v2 requires the complete closure evidence binding"
                ));
            }
        }
        (
            SCOPE_CLOSURE_REQUEST_SCHEMA_VERSION,
            JobPayload::ScopeClosureCheck {
                request_fingerprint,
                data_snapshot_token,
                ..
            },
        ) => {
            if requested_by.is_none() {
                return Err(anyhow::anyhow!(
                    "scope closure requires authenticated requested_by"
                ));
            }
            if request_fingerprint.trim().is_empty() {
                return Err(anyhow::anyhow!(
                    "scope closure payload requires request_fingerprint"
                ));
            }
            if data_snapshot_token.trim().is_empty() {
                return Err(anyhow::anyhow!(
                    "scope closure payload requires data_snapshot_token"
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

fn payload_type_for_job_kind(job_kind: &str) -> Option<&'static str> {
    match job_kind {
        "lca.solve_one" => Some("solve_one"),
        "lca.solve_batch" => Some("solve_batch"),
        "lca.solve_all_unit" => Some("solve_all_unit"),
        "lca.build_snapshot" => Some("build_snapshot"),
        "lca.contribution_path" => Some("analyze_contribution_path"),
        "lca.factorization_prepare" => Some("prepare_factorization"),
        "lcia_result.package_build" => Some("lcia_result_package_build"),
        SCOPE_CLOSURE_JOB_KIND => Some("scope_closure_check"),
        _ => None,
    }
}

fn result_schema_version_for_payload(payload: &JobPayload) -> &'static str {
    match payload {
        JobPayload::BuildSnapshot { .. } => "lca.snapshot.result.v2",
        JobPayload::AnalyzeContributionPath { .. } => "lca.contribution_path.result.v1",
        JobPayload::PrepareFactorization { .. } => "lca.factorization_prepare.result.v1",
        JobPayload::LciaResultPackageBuild { .. } => "lcia_result.package_build.result.v1",
        JobPayload::ScopeClosureCheck { .. } => "lcia.scope_closure_check.result.v1",
        _ => "lca.solve.result.v1",
    }
}

fn payload_type_name(payload: &JobPayload) -> &'static str {
    match payload {
        JobPayload::PrepareFactorization { .. } => "prepare_factorization",
        JobPayload::SolveOne { .. } => "solve_one",
        JobPayload::SolveBatch { .. } => "solve_batch",
        JobPayload::SolveAllUnit { .. } => "solve_all_unit",
        JobPayload::AnalyzeContributionPath { .. } => "analyze_contribution_path",
        JobPayload::InvalidateFactorization { .. } => "invalidate_factorization",
        JobPayload::RebuildFactorization { .. } => "rebuild_factorization",
        JobPayload::BuildSnapshot { .. } => "build_snapshot",
        JobPayload::LciaResultPackageBuild { .. } => "lcia_result_package_build",
        JobPayload::ScopeClosureCheck { .. } => "scope_closure_check",
    }
}

fn solver_worker_phase(payload: &JobPayload) -> &'static str {
    match payload {
        JobPayload::BuildSnapshot { .. } => "build_snapshot",
        JobPayload::LciaResultPackageBuild { .. } => "lcia_result_package_build",
        JobPayload::ScopeClosureCheck { .. } => "load_scope",
        JobPayload::AnalyzeContributionPath { .. } => "analyze_contribution_path",
        JobPayload::PrepareFactorization { .. } | JobPayload::RebuildFactorization { .. } => {
            "prepare_factorization"
        }
        JobPayload::InvalidateFactorization { .. } => "invalidate_factorization",
        JobPayload::SolveOne { .. } => "solve_one",
        JobPayload::SolveBatch { .. } => "solve_batch",
        JobPayload::SolveAllUnit { .. } => "solve_all_unit",
    }
}

/// Runs pgmq polling loop.
#[instrument(skip(state))]
pub async fn run_worker_loop(
    state: Arc<AppState>,
    queue_name: String,
    vt_seconds: i32,
    poll_interval: std::time::Duration,
) -> anyhow::Result<()> {
    loop {
        match read_one_queue_message(&state.queue_pool, &queue_name, vt_seconds).await {
            Ok(Some(message)) => {
                let parsed = serde_json::from_value::<JobPayload>(message.payload.clone());
                match parsed {
                    Ok(payload) => {
                        if let Err(err) =
                            Box::pin(handle_job_payload(&state, payload.clone())).await
                        {
                            error!(error = %err, "job execution failed");
                            let job_id = extract_job_id(&payload);
                            let err_message = err.to_string();
                            let diagnostics =
                                build_failure_diagnostics(&state, &payload, &err_message).await;
                            let _ =
                                update_job_status(&state.pool, job_id, "failed", diagnostics).await;
                            let _ = mark_result_cache_failed(
                                &state.pool,
                                job_id,
                                "job_execution_failed",
                                &err_message,
                            )
                            .await;
                        } else {
                            info!("job completed");
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, "invalid job payload");
                        if let Some(job_id) = extract_job_id_from_raw_payload(&message.payload) {
                            let err_message = format!("invalid job payload: {err}");
                            let _ = update_job_status(
                                &state.pool,
                                job_id,
                                "failed",
                                serde_json::json!({"error": err_message}),
                            )
                            .await;
                            let _ = mark_result_cache_failed(
                                &state.pool,
                                job_id,
                                "invalid_job_payload",
                                &err_message,
                            )
                            .await;
                        }
                    }
                }

                if let Err(err) =
                    archive_queue_message(&state.queue_pool, &queue_name, message.msg_id).await
                {
                    error!(error = %err, msg_id = message.msg_id, "failed to archive queue message");
                }
            }
            Ok(None) => {
                sleep(poll_interval).await;
            }
            Err(err) => {
                error!(error = %err, "queue read error");
                sleep(poll_interval).await;
            }
        }
    }
}

fn extract_job_id(payload: &JobPayload) -> uuid::Uuid {
    match payload {
        JobPayload::PrepareFactorization { job_id, .. }
        | JobPayload::SolveOne { job_id, .. }
        | JobPayload::SolveBatch { job_id, .. }
        | JobPayload::SolveAllUnit { job_id, .. }
        | JobPayload::AnalyzeContributionPath { job_id, .. }
        | JobPayload::InvalidateFactorization { job_id, .. }
        | JobPayload::RebuildFactorization { job_id, .. }
        | JobPayload::BuildSnapshot { job_id, .. } => *job_id,
        JobPayload::LciaResultPackageBuild { build_id, .. } => *build_id,
        JobPayload::ScopeClosureCheck {
            closure_check_id, ..
        } => *closure_check_id,
    }
}

fn extract_job_id_from_raw_payload(payload: &Value) -> Option<Uuid> {
    payload
        .get("job_id")
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use crate::{
        calculation_evidence::{
            CALCULATION_EVIDENCE_SCHEMA_VERSION, FACTOR_COVERAGE_EVIDENCE_SCHEMA_VERSION,
            LcaCalculationEvidence, LcaMethodFactorSourceSnapshot, LciaFactorCoverageCounts,
            LciaFactorCoverageEvidence, LciaMethodFactorCoverage,
            METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION, MISSING_FACTOR_SEMANTICS,
            RELEASE_BUNDLE_MANIFEST_SHA256, RELEASE_BUNDLE_VERSION, RELEASE_FACTOR_MANIFEST_SHA256,
            RELEASE_METHOD_COUNT, RELEASE_METHOD_IDENTITIES,
            RELEASE_METHOD_IDENTITY_MANIFEST_SHA256, RELEASE_METHOD_MANIFEST_SHA256,
            RELEASE_SOURCE_SNAPSHOT_SHA256, STATIC_CACHE_BUNDLE_MANIFEST_PATH,
            STATIC_CACHE_BUNDLE_SCHEMA_VERSION, canonical_json_sha256,
            expected_factor_coverage_contract, expected_scope_manifest,
            method_factor_source_contract_fixture,
        },
        queue::{
            AuthoritativePackageClosureBinding, lcia_result_package_worker_result_ref,
            parse_build_snapshot_worker_projection, payload_type_name,
            result_schema_version_for_payload, snapshot_diagnostic_scope_pairs,
            solver_worker_job_payload, solver_worker_result_ref,
            validate_authoritative_package_closure_binding,
        },
        types::JobPayload,
        worker_jobs::WorkerJob,
    };

    fn worker_job(
        job_kind: &str,
        payload_schema_version: &str,
        payload: serde_json::Value,
    ) -> WorkerJob {
        WorkerJob {
            id: Uuid::new_v4(),
            job_kind: job_kind.to_owned(),
            worker_queue: "solver".to_owned(),
            payload_schema_version: payload_schema_version.to_owned(),
            payload,
            requested_by: Some(Uuid::new_v4()),
            lease_token: Uuid::new_v4(),
            attempt_count: 1,
        }
    }

    fn summary_only_method_factor_source() -> serde_json::Value {
        let mut source = method_factor_source_contract_fixture();
        let methods = RELEASE_METHOD_IDENTITIES
            .iter()
            .map(|(method_id, method_version, artifact_locator_id)| {
                json!({
                    "method_id": method_id,
                    "method_version": method_version,
                    "artifact_locator_id": artifact_locator_id,
                })
            })
            .collect::<Vec<_>>();
        source["bundle_manifest"] = json!({
            "schema_version": STATIC_CACHE_BUNDLE_SCHEMA_VERSION,
            "source_kind": "static_cache_bundle",
            "bundle_version": RELEASE_BUNDLE_VERSION,
            "source_snapshot_sha256": RELEASE_SOURCE_SNAPSHOT_SHA256,
            "method_manifest_sha256": RELEASE_METHOD_MANIFEST_SHA256,
            "factor_manifest_sha256": RELEASE_FACTOR_MANIFEST_SHA256,
            "method_identity_manifest_sha256": RELEASE_METHOD_IDENTITY_MANIFEST_SHA256,
            "methods": methods,
        });
        source
    }

    fn complete_calculation_evidence(scope_hash: String) -> LcaCalculationEvidence {
        let by_method = RELEASE_METHOD_IDENTITIES
            .iter()
            .map(
                |(method_id, method_version, artifact_locator_id)| LciaMethodFactorCoverage {
                    method_id: Uuid::parse_str(method_id).expect("method id"),
                    method_version: (*method_version).to_owned(),
                    artifact_locator_id: Uuid::parse_str(artifact_locator_id)
                        .expect("artifact locator id"),
                    counts: LciaFactorCoverageCounts {
                        matched: 1,
                        ..LciaFactorCoverageCounts::default()
                    },
                },
            )
            .collect::<Vec<_>>();
        LcaCalculationEvidence {
            schema_version: CALCULATION_EVIDENCE_SCHEMA_VERSION.to_owned(),
            scope_manifest_sha256: scope_hash,
            lcia_method_factor_source: LcaMethodFactorSourceSnapshot {
                schema_version: METHOD_SOURCE_SNAPSHOT_SCHEMA_VERSION.to_owned(),
                source_kind: "static_cache_bundle".to_owned(),
                bundle_manifest_path: STATIC_CACHE_BUNDLE_MANIFEST_PATH.to_owned(),
                bundle_manifest_sha256: RELEASE_BUNDLE_MANIFEST_SHA256.to_owned(),
                bundle_version: RELEASE_BUNDLE_VERSION.to_owned(),
                source_snapshot_sha256: RELEASE_SOURCE_SNAPSHOT_SHA256.to_owned(),
                method_manifest_sha256: RELEASE_METHOD_MANIFEST_SHA256.to_owned(),
                factor_manifest_sha256: RELEASE_FACTOR_MANIFEST_SHA256.to_owned(),
                method_identity_manifest_sha256: RELEASE_METHOD_IDENTITY_MANIFEST_SHA256.to_owned(),
                method_count: RELEASE_METHOD_COUNT,
            },
            lcia_factor_coverage: LciaFactorCoverageEvidence {
                schema_version: FACTOR_COVERAGE_EVIDENCE_SCHEMA_VERSION.to_owned(),
                source_snapshot_sha256: RELEASE_SOURCE_SNAPSHOT_SHA256.to_owned(),
                method_manifest_sha256: RELEASE_METHOD_MANIFEST_SHA256.to_owned(),
                factor_manifest_sha256: RELEASE_FACTOR_MANIFEST_SHA256.to_owned(),
                method_identity_manifest_sha256: RELEASE_METHOD_IDENTITY_MANIFEST_SHA256.to_owned(),
                count_unit: "exchange_method_pair".to_owned(),
                key_dimensions: ["method_id", "method_version", "flow_uuid", "direction"]
                    .into_iter()
                    .map(ToOwned::to_owned)
                    .collect(),
                coverage_status: "complete".to_owned(),
                missing_factor_semantics: MISSING_FACTOR_SEMANTICS.to_owned(),
                counts: LciaFactorCoverageCounts {
                    matched: RELEASE_METHOD_COUNT,
                    ..LciaFactorCoverageCounts::default()
                },
                by_method,
                uncharacterized_evidence: None,
            },
        }
    }

    #[test]
    fn maps_worker_jobs_solve_one_payload_with_camel_case_aliases() {
        let lca_job_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();
        let job = worker_job(
            "lca.solve_one",
            "lca.solve_one.request.v1",
            json!({
                "lcaJobId": lca_job_id,
                "snapshotId": snapshot_id,
                "rhs": [1.0, 0.0],
                "printLevel": 1.0,
                "solve": {
                    "return_x": true,
                    "return_g": false,
                    "return_h": true
                }
            }),
        );

        let payload = solver_worker_job_payload(&job).expect("payload");

        match payload {
            JobPayload::SolveOne {
                job_id,
                snapshot_id: parsed_snapshot_id,
                rhs,
                print_level,
                ..
            } => {
                assert_eq!(job_id, lca_job_id);
                assert_eq!(parsed_snapshot_id, snapshot_id);
                assert_eq!(rhs, vec![1.0, 0.0]);
                assert_eq!(print_level, Some(1.0));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn maps_worker_jobs_contribution_path_kind_to_legacy_payload_type() {
        let lca_job_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();
        let process_id = Uuid::new_v4();
        let impact_id = Uuid::new_v4();
        let job = worker_job(
            "lca.contribution_path",
            "lca.contribution_path.request.v1",
            json!({
                "jobId": lca_job_id,
                "snapshotId": snapshot_id,
                "processId": process_id,
                "processIndex": 2,
                "impactId": impact_id,
                "impactIndex": 3,
                "amount": 4.0
            }),
        );

        let payload = solver_worker_job_payload(&job).expect("payload");

        match payload {
            JobPayload::AnalyzeContributionPath {
                job_id,
                snapshot_id: parsed_snapshot_id,
                process_id: parsed_process_id,
                process_index,
                impact_id: parsed_impact_id,
                impact_index,
                amount,
                ..
            } => {
                assert_eq!(job_id, lca_job_id);
                assert_eq!(parsed_snapshot_id, snapshot_id);
                assert_eq!(parsed_process_id, process_id);
                assert_eq!(process_index, 2);
                assert_eq!(parsed_impact_id, impact_id);
                assert_eq!(impact_index, 3);
                assert!((amount - 4.0).abs() < f64::EPSILON);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn maps_worker_jobs_build_snapshot_request_roots() {
        let lca_job_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();
        let process_id = Uuid::new_v4();
        let job = worker_job(
            "lca.build_snapshot",
            "lca.build_snapshot.request.v1",
            json!({
                "jobId": lca_job_id,
                "snapshotId": snapshot_id,
                "requestRoots": [
                    {
                        "processId": process_id,
                        "version": "01.00.000"
                    }
                ],
                "processStates": "100,101",
                "includeUserId": Uuid::new_v4(),
                "noLcia": true
            }),
        );

        let payload = solver_worker_job_payload(&job).expect("payload");

        match payload {
            JobPayload::BuildSnapshot {
                job_id,
                snapshot_id: parsed_snapshot_id,
                request_roots,
                process_states,
                no_lcia,
                ..
            } => {
                assert_eq!(job_id, lca_job_id);
                assert_eq!(parsed_snapshot_id, snapshot_id);
                assert_eq!(request_roots.expect("roots")[0].process_id, process_id);
                assert_eq!(process_states.as_deref(), Some("100,101"));
                assert_eq!(no_lcia, Some(true));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn maps_exact_public_owner_draft_build_v2() {
        let actor = Uuid::new_v4();
        let manifest = expected_scope_manifest(actor);
        let manifest_hash = canonical_json_sha256(&manifest).expect("manifest hash");
        let mut job = worker_job(
            "lca.build_snapshot",
            "lca.build_snapshot.request.v2",
            json!({
                "job_id": Uuid::new_v4(),
                "snapshot_id": Uuid::new_v4(),
                "all_states": false,
                "process_states": "100",
                "include_user_id": actor,
                "include_user_state_codes": "0",
                "include_user_unassigned_only": true,
                "include_user_review_free_only": true,
                "data_scope": "public_plus_owner_draft",
                "scope_manifest": manifest,
                "scope_manifest_sha256": manifest_hash,
                "lcia_method_factor_source": method_factor_source_contract_fixture(),
                "lcia_factor_coverage_contract": expected_factor_coverage_contract(),
                "no_lcia": false,
            }),
        );
        job.requested_by = Some(actor);

        let payload = solver_worker_job_payload(&job).expect("v2 payload");
        assert!(matches!(
            payload,
            JobPayload::BuildSnapshot {
                include_user_id: Some(parsed_actor),
                data_scope: Some(ref scope),
                ..
            } if parsed_actor == actor && scope == "public_plus_owner_draft"
        ));
    }

    #[test]
    fn rejects_summary_only_lcia_manifest_before_build_execution() {
        let actor = Uuid::new_v4();
        let manifest = expected_scope_manifest(actor);
        let manifest_hash = canonical_json_sha256(&manifest).expect("manifest hash");
        let mut job = worker_job(
            "lca.build_snapshot",
            "lca.build_snapshot.request.v2",
            json!({
                "job_id": Uuid::new_v4(),
                "snapshot_id": Uuid::new_v4(),
                "all_states": false,
                "process_states": "100",
                "include_user_id": actor,
                "include_user_state_codes": "0",
                "include_user_unassigned_only": true,
                "include_user_review_free_only": true,
                "data_scope": "public_plus_owner_draft",
                "scope_manifest": manifest,
                "scope_manifest_sha256": manifest_hash,
                "lcia_method_factor_source": summary_only_method_factor_source(),
                "lcia_factor_coverage_contract": expected_factor_coverage_contract(),
                "no_lcia": false,
            }),
        );
        job.requested_by = Some(actor);

        let error = solver_worker_job_payload(&job)
            .expect_err("summary-only bundle manifest must fail during payload validation");
        assert!(
            error
                .to_string()
                .contains("complete reviewed release manifest")
        );
    }

    #[test]
    fn rejects_public_101_or_actor_drift_in_build_v2() {
        let actor = Uuid::new_v4();
        let manifest = expected_scope_manifest(actor);
        let manifest_hash = canonical_json_sha256(&manifest).expect("manifest hash");
        let job = worker_job(
            "lca.build_snapshot",
            "lca.build_snapshot.request.v2",
            json!({
                "job_id": Uuid::new_v4(),
                "snapshot_id": Uuid::new_v4(),
                "all_states": false,
                "process_states": "100,101",
                "include_user_id": actor,
                "include_user_state_codes": "0",
                "include_user_unassigned_only": true,
                "include_user_review_free_only": true,
                "data_scope": "public_plus_owner_draft",
                "scope_manifest": manifest,
                "scope_manifest_sha256": manifest_hash,
                "lcia_method_factor_source": method_factor_source_contract_fixture(),
                "lcia_factor_coverage_contract": expected_factor_coverage_contract(),
                "no_lcia": false,
            }),
        );
        assert!(solver_worker_job_payload(&job).is_err());
    }

    #[test]
    fn solve_v2_requires_valid_calculation_evidence_binding() {
        let actor = Uuid::new_v4();
        let scope_hash =
            canonical_json_sha256(&expected_scope_manifest(actor)).expect("scope hash");
        let evidence = complete_calculation_evidence(scope_hash);
        let mut job = worker_job(
            "lca.solve_one",
            "lca.solve_one.request.v2",
            json!({
                "job_id": Uuid::new_v4(),
                "snapshot_id": Uuid::new_v4(),
                "rhs": [1.0],
                "calculation_evidence_binding": evidence,
            }),
        );
        job.requested_by = Some(actor);
        assert!(solver_worker_job_payload(&job).is_ok());

        let mut actor_drift = job.clone();
        actor_drift.requested_by = Some(Uuid::new_v4());
        assert!(solver_worker_job_payload(&actor_drift).is_err());

        let mut missing = worker_job(
            "lca.solve_one",
            "lca.solve_one.request.v2",
            json!({
                "job_id": Uuid::new_v4(),
                "snapshot_id": Uuid::new_v4(),
                "rhs": [1.0],
            }),
        );
        missing.requested_by = Some(actor);
        assert!(solver_worker_job_payload(&missing).is_err());
    }

    #[test]
    fn build_snapshot_projection_uses_worker_diagnostics_without_lca_jobs() {
        let actor = Uuid::new_v4();
        let evidence = complete_calculation_evidence(
            canonical_json_sha256(&expected_scope_manifest(actor)).expect("scope hash"),
        );
        let resolved_snapshot_id = Uuid::new_v4();
        let diagnostics = json!({
            "build_snapshot_result": {
                "requested_snapshot_id": Uuid::new_v4(),
                "resolved_snapshot_id": resolved_snapshot_id,
                "calculation_evidence": evidence,
            }
        });
        let (parsed_snapshot_id, parsed_evidence) =
            parse_build_snapshot_worker_projection(&diagnostics, true)
                .expect("worker-only build projection");
        assert_eq!(parsed_snapshot_id, resolved_snapshot_id);
        assert_eq!(parsed_evidence, Some(evidence));

        let payload = JobPayload::BuildSnapshot {
            job_id: Uuid::new_v4(),
            snapshot_id: Uuid::new_v4(),
            scope: None,
            all_states: None,
            process_states: None,
            include_user_id: None,
            include_user_state_codes: None,
            include_user_unassigned_only: None,
            include_user_review_free_only: None,
            data_scope: None,
            scope_manifest: None,
            scope_manifest_sha256: None,
            lcia_method_factor_source: None,
            lcia_factor_coverage_contract: None,
            request_roots: None,
            provider_rule: None,
            reference_normalization_mode: None,
            allocation_fraction_mode: None,
            process_limit: None,
            self_loop_cutoff: None,
            singular_eps: None,
            method_id: None,
            method_version: None,
            no_lcia: None,
        };
        assert_eq!(
            result_schema_version_for_payload(&payload),
            "lca.snapshot.result.v2"
        );
    }

    #[test]
    fn failure_diagnostics_scope_uses_exact_snapshot_process_versions() {
        let process_a = Uuid::new_v4();
        let process_b = Uuid::new_v4();
        let index = crate::snapshot_index::SnapshotIndexDocument {
            version: 1,
            snapshot_id: Uuid::new_v4(),
            process_count: 2,
            impact_count: 0,
            process_map: vec![
                crate::snapshot_index::SnapshotProcessMapEntry {
                    process_id: process_a,
                    process_index: 0,
                    process_version: "01.00.000".to_owned(),
                    process_name: None,
                    location: None,
                },
                crate::snapshot_index::SnapshotProcessMapEntry {
                    process_id: process_b,
                    process_index: 1,
                    process_version: "02.03.004".to_owned(),
                    process_name: None,
                    location: None,
                },
            ],
            impact_map: Vec::new(),
            calculation_evidence: None,
        };
        let (ids, versions) = snapshot_diagnostic_scope_pairs(&index);
        assert_eq!(ids, vec![process_a, process_b]);
        assert_eq!(versions, vec!["01.00.000", "02.03.004"]);
    }

    #[test]
    fn maps_worker_jobs_lcia_result_package_build_payload() {
        let build_id = Uuid::new_v4();
        let process_id = Uuid::new_v4();
        let requested_by = Uuid::new_v4();
        let job = worker_job(
            "lcia_result.package_build",
            "lcia_result.package_build.request.v1",
            json!({
                "type": "lcia_result_package_build",
                "buildId": build_id,
                "requestedBy": requested_by,
                "coverageMode": "global_eligible",
                "eligibleInputCount": 1,
                "includedInputCount": 1,
                "inputManifestHash": "hash-1",
                "inputManifest": {
                    "predicateVersion": "published-state-code-100-199:v1",
                    "processes": [
                        {
                            "id": process_id,
                            "version": "01.00.000",
                            "stateCode": 100
                        }
                    ]
                },
                "lciaMethodSet": [],
                "defaultImpactCategory": "climate-change"
            }),
        );

        let payload = solver_worker_job_payload(&job).expect("payload");

        match payload {
            JobPayload::LciaResultPackageBuild {
                build_id: parsed_build_id,
                requested_by: parsed_requested_by,
                coverage_mode,
                included_input_count,
                input_manifest_hash,
                default_impact_category,
                ..
            } => {
                assert_eq!(parsed_build_id, build_id);
                assert_eq!(parsed_requested_by, requested_by);
                assert_eq!(coverage_mode, "global_eligible");
                assert_eq!(included_input_count, 1);
                assert_eq!(input_manifest_hash, "hash-1");
                assert_eq!(default_impact_category.as_deref(), Some("climate-change"));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn maps_scope_closure_payload_from_database_envelope() {
        let closure_check_id = Uuid::new_v4();
        let scan_execution_id = Uuid::new_v4();
        let job = worker_job(
            "lcia.scope_closure_check",
            "lcia.scope_closure_check.request.v1",
            json!({
                "closure_check_id": closure_check_id,
                "scan_execution_id": scan_execution_id,
                "data_snapshot_token": "snapshot-token",
                "request_fingerprint": "request-fingerprint",
            }),
        );

        let payload = solver_worker_job_payload(&job).expect("scope closure payload");
        assert!(matches!(
            payload,
            JobPayload::ScopeClosureCheck {
                closure_check_id: parsed_check,
                scan_execution_id: parsed_execution,
                ref data_snapshot_token,
                ref request_fingerprint,
            } if parsed_check == closure_check_id
                && parsed_execution == scan_execution_id
                && data_snapshot_token == "snapshot-token"
                && request_fingerprint == "request-fingerprint"
        ));
    }

    #[test]
    fn package_v2_requires_complete_closure_binding_and_preserves_check_id() {
        let closure_check_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();
        let requested_by = Uuid::new_v4();
        let closure_bundle_artifact_id = Uuid::new_v4();
        let full = worker_job(
            "lcia_result.package_build",
            "lcia_result.package_build.request.v2",
            json!({
                "build_id": Uuid::new_v4(),
                "requested_by": requested_by,
                "coverage_mode": "subset",
                "eligible_input_count": 1,
                "included_input_count": 1,
                "input_manifest_hash": "input-hash",
                "input_manifest": {"processes": []},
                "lcia_method_set": [],
                "closure_check_id": closure_check_id,
                "closure_certificate_hash": "certificate-hash",
                "requested_scope_hash": "requested-scope-hash",
                "policy_fingerprint": "policy-fingerprint",
                "effective_scope_hash": "scope-hash",
                "effective_scope": {"processes": []},
                "data_snapshot_token": "snapshot-token",
                "snapshot_id": snapshot_id,
                "snapshot_hash": "snapshot-hash",
                "closure_bundle_artifact_id": closure_bundle_artifact_id,
                "closure_bundle_hash": "bundle-hash",
                "report_artifact_manifest_hash": "report-hash",
                "snapshot_artifact_id": Uuid::new_v4(),
                "snapshot_index_sha256": "index-hash",
                "snapshot_build_contract_hash": "build-contract-hash",
            }),
        );
        assert!(solver_worker_job_payload(&full).is_ok());

        let mut partial = full;
        partial
            .payload
            .as_object_mut()
            .unwrap()
            .remove("snapshot_hash");
        assert!(
            solver_worker_job_payload(&partial)
                .unwrap_err()
                .to_string()
                .contains("complete closure evidence binding")
        );

        let result_ref = lcia_result_package_worker_result_ref(
            Uuid::new_v4(),
            Uuid::new_v4(),
            None,
            Some(closure_check_id),
        );
        assert_eq!(result_ref["closureCheckId"], json!(closure_check_id));
    }

    #[test]
    fn package_v2_requires_exact_authoritative_binding_and_process_axis() {
        let closure_check_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();
        let snapshot_artifact_id = Uuid::new_v4();
        let closure_bundle_artifact_id = Uuid::new_v4();
        let process_id = Uuid::new_v4();
        let process_axis = json!([{
            "id": process_id,
            "version": "01.00.000",
            "stateCode": 100,
        }]);
        let effective_scope = json!({
            "coverageMode": "subset",
            "lciaMethods": [],
            "processes": process_axis.clone(),
        });
        let input_manifest = json!({"processes": process_axis.clone()});
        let effective_scope_hash =
            canonical_json_sha256(&effective_scope).expect("effective scope hash");
        let input_manifest_hash =
            canonical_json_sha256(&input_manifest).expect("input manifest hash");
        let job = worker_job(
            "lcia_result.package_build",
            "lcia_result.package_build.request.v2",
            json!({
                "build_id": Uuid::new_v4(),
                "requested_by": Uuid::new_v4(),
                "coverage_mode": "subset",
                "eligible_input_count": 1,
                "included_input_count": 1,
                "input_manifest_hash": input_manifest_hash,
                "input_manifest": input_manifest.clone(),
                "lcia_method_set": [],
                "closure_check_id": closure_check_id,
                "closure_certificate_hash": "certificate-hash",
                "requested_scope_hash": "requested-scope-hash",
                "policy_fingerprint": "policy-fingerprint",
                "effective_scope_hash": effective_scope_hash,
                "effective_scope": effective_scope.clone(),
                "data_snapshot_token": "snapshot-token",
                "snapshot_id": snapshot_id,
                "snapshot_hash": "snapshot-hash",
                "closure_bundle_artifact_id": closure_bundle_artifact_id,
                "closure_bundle_hash": "bundle-hash",
                "report_artifact_manifest_hash": "report-hash",
                "snapshot_artifact_id": snapshot_artifact_id,
                "snapshot_index_sha256": "index-hash",
                "snapshot_build_contract_hash": "build-contract-hash",
            }),
        );
        let payload = solver_worker_job_payload(&job).expect("valid v2 payload");
        let mut binding = AuthoritativePackageClosureBinding {
            closure_check_id,
            certificate_hash: "certificate-hash".to_owned(),
            requested_scope_hash: "requested-scope-hash".to_owned(),
            policy_fingerprint: "policy-fingerprint".to_owned(),
            effective_scope_hash: effective_scope_hash.clone(),
            data_snapshot_token: "snapshot-token".to_owned(),
            snapshot_id,
            snapshot_hash: "snapshot-hash".to_owned(),
            snapshot_artifact_id,
            snapshot_index_sha256: "index-hash".to_owned(),
            snapshot_build_contract_hash: "build-contract-hash".to_owned(),
            closure_bundle_artifact_id,
            closure_bundle_hash: "bundle-hash".to_owned(),
            coverage_mode: "subset".to_owned(),
            lcia_method_set: json!([]),
            input_manifest: input_manifest.clone(),
            input_manifest_hash: input_manifest_hash.clone(),
            effective_scope: effective_scope.clone(),
        };

        validate_authoritative_package_closure_binding(&payload, &binding)
            .expect("exact authoritative binding");

        binding.snapshot_hash = "tampered".to_owned();
        assert!(
            validate_authoritative_package_closure_binding(&payload, &binding)
                .unwrap_err()
                .to_string()
                .contains("differs from authoritative")
        );
        binding.snapshot_hash = "snapshot-hash".to_owned();
        binding.closure_bundle_artifact_id = Uuid::new_v4();
        assert!(
            validate_authoritative_package_closure_binding(&payload, &binding)
                .unwrap_err()
                .to_string()
                .contains("differs from authoritative")
        );
        binding.closure_bundle_artifact_id = closure_bundle_artifact_id;
        binding.input_manifest_hash = "tampered-input".to_owned();
        assert!(
            validate_authoritative_package_closure_binding(&payload, &binding)
                .unwrap_err()
                .to_string()
                .contains("differs from authoritative")
        );
        binding.input_manifest_hash = input_manifest_hash;
        binding.requested_scope_hash = "tampered-requested".to_owned();
        assert!(
            validate_authoritative_package_closure_binding(&payload, &binding)
                .unwrap_err()
                .to_string()
                .contains("differs from authoritative")
        );
        binding.requested_scope_hash = "requested-scope-hash".to_owned();
        binding.policy_fingerprint = "tampered-policy".to_owned();
        assert!(
            validate_authoritative_package_closure_binding(&payload, &binding)
                .unwrap_err()
                .to_string()
                .contains("differs from authoritative")
        );
        binding.policy_fingerprint = "policy-fingerprint".to_owned();
        binding.effective_scope = json!({
            "coverageMode": "subset",
            "lciaMethods": [],
            "processes": [],
        });
        binding.effective_scope_hash =
            canonical_json_sha256(&binding.effective_scope).expect("tampered scope hash");
        assert!(
            validate_authoritative_package_closure_binding(&payload, &binding)
                .unwrap_err()
                .to_string()
                .contains("differs from authoritative")
        );

        let mut authoritative = serde_json::to_value(&binding).unwrap_or_default();
        authoritative["unexpectedField"] = json!(true);
        assert!(
            serde_json::from_value::<AuthoritativePackageClosureBinding>(authoritative).is_err()
        );
    }

    #[test]
    fn rejects_wrong_solver_worker_jobs_schema() {
        let job = worker_job(
            "lca.solve_batch",
            "lca.solve_one.request.v1",
            json!({
                "job_id": Uuid::new_v4(),
                "snapshot_id": Uuid::new_v4(),
                "rhs_batch": [[1.0]]
            }),
        );

        let err = solver_worker_job_payload(&job).expect_err("schema mismatch");
        assert!(err.to_string().contains("unsupported payload schema"));
    }

    #[test]
    fn solver_worker_result_ref_points_to_worker_jobs_domain_source() {
        let worker_job_id = Uuid::new_v4();
        let lca_job_id = Uuid::new_v4();
        let result_id = Uuid::new_v4();

        let result_ref = solver_worker_result_ref(worker_job_id, lca_job_id, Some(result_id));

        assert_eq!(
            result_ref,
            json!({
                "domainSource": "worker_jobs",
                "workerJobId": worker_job_id,
                "lcaJobId": lca_job_id,
                "result": {
                    "table": "lca_results",
                    "id": result_id,
                },
            })
        );
    }

    #[test]
    fn preserves_legacy_payload_type_when_supplied() {
        let lca_job_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();
        let job = worker_job(
            "lca.factorization_prepare",
            "lca.factorization_prepare.request.v1",
            json!({
                "type": "prepare_factorization",
                "job_id": lca_job_id,
                "snapshot_id": snapshot_id
            }),
        );

        let payload = solver_worker_job_payload(&job).expect("payload");

        assert_eq!(payload_type_name(&payload), "prepare_factorization");
        assert!(matches!(payload, JobPayload::PrepareFactorization { .. }));
    }
}
