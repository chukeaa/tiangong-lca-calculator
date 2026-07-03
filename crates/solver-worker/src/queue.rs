use std::sync::Arc;

use crate::pgbouncer_sqlx::{self as sqlx, Row};
use serde_json::{Map, Value, json};
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

use crate::{
    db::{
        AppState, archive_queue_message, handle_job_payload,
        handle_lcia_result_package_build_worker_job, handle_worker_jobs_job_payload,
        latest_result_id_for_job, mark_result_cache_failed, read_one_queue_message,
        update_job_status,
    },
    types::JobPayload,
    worker_jobs::{WorkerJob, WorkerJobResult, claim_worker_jobs, record_worker_job_result},
};

const SOLVER_WORKER_QUEUE: &str = "solver";

fn extract_snapshot_id(payload: &JobPayload) -> Option<Uuid> {
    match payload {
        JobPayload::PrepareFactorization { snapshot_id, .. }
        | JobPayload::SolveOne { snapshot_id, .. }
        | JobPayload::SolveBatch { snapshot_id, .. }
        | JobPayload::SolveAllUnit { snapshot_id, .. }
        | JobPayload::AnalyzeContributionPath { snapshot_id, .. }
        | JobPayload::InvalidateFactorization { snapshot_id, .. }
        | JobPayload::RebuildFactorization { snapshot_id, .. } => Some(*snapshot_id),
        JobPayload::BuildSnapshot { .. } | JobPayload::LciaResultPackageBuild { .. } => None,
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
    snapshot_id: Uuid,
) -> Option<Value> {
    let result = sqlx::query_scalar::<Value>(
        r"
        WITH snapshot_scope AS (
            SELECT
                process_filter->>'include_user_id' AS uid,
                process_filter->'process_states' AS states
            FROM public.lca_network_snapshots
            WHERE id = $1
        ),
        state_array AS (
            SELECT array_agg(s::int) AS codes
            FROM snapshot_scope, jsonb_array_elements_text(snapshot_scope.states) AS s
        ),
        scope_procs AS (
            SELECT DISTINCT ON (p.id) p.id, p.version, p.json
            FROM public.processes p, snapshot_scope ss, state_array sa
            WHERE (p.state_code = ANY(sa.codes) OR p.user_id = ss.uid::uuid)
              AND p.json ? 'processDataSet'
            ORDER BY p.id, p.version DESC
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
    .bind(snapshot_id)
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
async fn detect_service_loop_processes(pool: &sqlx::PgPool, snapshot_id: Uuid) -> Option<Value> {
    let result = sqlx::query_scalar::<Value>(
        r"
        WITH snapshot_scope AS (
            SELECT
                process_filter->>'include_user_id' AS uid,
                process_filter->'process_states' AS states
            FROM public.lca_network_snapshots
            WHERE id = $1
        ),
        state_array AS (
            SELECT array_agg(s::int) AS codes
            FROM snapshot_scope, jsonb_array_elements_text(snapshot_scope.states) AS s
        ),
        scope_procs AS (
            SELECT DISTINCT ON (p.id) p.id, p.version, p.json
            FROM public.processes p, snapshot_scope ss, state_array sa
            WHERE (p.state_code = ANY(sa.codes) OR p.user_id = ss.uid::uuid)
              AND p.json ? 'processDataSet'
            ORDER BY p.id, p.version DESC
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
    .bind(snapshot_id)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    result.filter(|v| v.as_array().is_some_and(|a| !a.is_empty()))
}

/// Builds enriched diagnostics JSON when a job fails with a factorization error.
async fn build_failure_diagnostics(
    pool: &sqlx::PgPool,
    payload: &JobPayload,
    err_message: &str,
) -> Value {
    let mut diag = serde_json::json!({"error": err_message});

    // For factorization/singular errors, attach snapshot coverage and problem process info.
    if (err_message.contains("singular") || err_message.contains("factorization"))
        && let Some(snapshot_id) = extract_snapshot_id(payload)
    {
        diag["snapshot_id"] = serde_json::json!(snapshot_id.to_string());
        if let Some(coverage) = fetch_snapshot_coverage(pool, snapshot_id).await {
            diag["snapshot_coverage"] = coverage;
        }
        if let Some(duplicates) = detect_duplicate_exchange_processes(pool, snapshot_id).await {
            diag["duplicate_exchange_processes"] = duplicates;
        }
        if let Some(loops) = detect_service_loop_processes(pool, snapshot_id).await {
            diag["service_loop_processes"] = loops;
        }
    }

    diag
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
                    process_solver_worker_job(&state, job, lease_seconds).await;
                }
            }
            Err(err) => {
                error!(error = %err, "worker_jobs claim error");
                sleep(poll_interval).await;
            }
        }
    }
}

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

    let execution_result = match &payload {
        JobPayload::LciaResultPackageBuild { .. } => {
            handle_lcia_result_package_build_worker_job(state, job.id, &payload)
                .await
                .map(|_| ())
        }
        _ => {
            handle_worker_jobs_job_payload(
                state,
                payload.clone(),
                job.id,
                job.lease_token,
                lease_seconds,
            )
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
    if let JobPayload::LciaResultPackageBuild { build_id, .. } = payload {
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
            job.id, *build_id, None,
        ));
        if let Err(record_err) =
            record_worker_job_result(&state.pool, job.id, job.lease_token, result).await
        {
            error!(error = %record_err, worker_job_id = %job.id, "failed to record lcia result package worker_jobs failure");
        }
        return;
    }

    let diagnostics = build_failure_diagnostics(&state.pool, payload, err_message).await;
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
            if let JobPayload::LciaResultPackageBuild { build_id, .. } = payload {
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
                    job.id, *build_id, None,
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

async fn build_solver_worker_job_result(
    state: &AppState,
    worker_job_id: Uuid,
    payload: &JobPayload,
) -> anyhow::Result<WorkerJobResult> {
    if let JobPayload::LciaResultPackageBuild { build_id, .. } = payload {
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
                "payloadType": payload_type_name(payload),
                "package": package_projection,
            })),
            result_schema_version: Some(result_schema_version_for_payload(payload).to_owned()),
            result_ref: Some(lcia_result_package_worker_result_ref(
                worker_job_id,
                *build_id,
                package_id,
            )),
            diagnostics: Some(json!({
                "lciaResultPackage": package_projection,
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
    });

    Ok(WorkerJobResult {
        status: "completed".to_owned(),
        result_json: Some(result_json),
        result_schema_version: Some(result_schema_version_for_payload(payload).to_owned()),
        result_ref: Some(result_ref),
        diagnostics: Some(json!({
            "lcaJob": job_projection,
        })),
        error_code: None,
        error_message: None,
        error_details: None,
        blocker_codes: Vec::new(),
        resolution_scope: None,
        retryable: None,
    })
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
) -> Value {
    json!({
        "domainSource": "worker_jobs",
        "workerJobId": worker_job_id,
        "buildId": build_id,
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

    let expected_schema = payload_schema_version_for_job_kind(&job.job_kind)
        .ok_or_else(|| anyhow::anyhow!("unsupported solver worker job kind: {}", job.job_kind))?;
    if job.payload_schema_version != expected_schema {
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

    Ok(serde_json::from_value(Value::Object(payload))?)
}

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
    copy_alias(&mut payload, "includeUserId", "include_user_id");
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

fn payload_schema_version_for_job_kind(job_kind: &str) -> Option<&'static str> {
    match job_kind {
        "lca.solve_one" => Some("lca.solve_one.request.v1"),
        "lca.solve_batch" => Some("lca.solve_batch.request.v1"),
        "lca.solve_all_unit" => Some("lca.solve_all_unit.request.v1"),
        "lca.build_snapshot" => Some("lca.build_snapshot.request.v1"),
        "lca.contribution_path" => Some("lca.contribution_path.request.v1"),
        "lca.factorization_prepare" => Some("lca.factorization_prepare.request.v1"),
        "lcia_result.package_build" => Some("lcia_result.package_build.request.v1"),
        _ => None,
    }
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
        _ => None,
    }
}

fn result_schema_version_for_payload(payload: &JobPayload) -> &'static str {
    match payload {
        JobPayload::BuildSnapshot { .. } => "lca.snapshot.result.v1",
        JobPayload::AnalyzeContributionPath { .. } => "lca.contribution_path.result.v1",
        JobPayload::PrepareFactorization { .. } => "lca.factorization_prepare.result.v1",
        JobPayload::LciaResultPackageBuild { .. } => "lcia_result.package_build.result.v1",
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
    }
}

fn solver_worker_phase(payload: &JobPayload) -> &'static str {
    match payload {
        JobPayload::BuildSnapshot { .. } => "build_snapshot",
        JobPayload::LciaResultPackageBuild { .. } => "lcia_result_package_build",
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
                        if let Err(err) = handle_job_payload(&state, payload.clone()).await {
                            error!(error = %err, "job execution failed");
                            let job_id = extract_job_id(&payload);
                            let err_message = err.to_string();
                            let diagnostics =
                                build_failure_diagnostics(&state.pool, &payload, &err_message)
                                    .await;
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
        queue::{payload_type_name, solver_worker_job_payload, solver_worker_result_ref},
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
