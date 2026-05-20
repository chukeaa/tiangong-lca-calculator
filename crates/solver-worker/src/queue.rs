use std::sync::Arc;

use crate::pgbouncer_sqlx as sqlx;
use serde_json::Value;
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

use crate::{
    db::{
        AppState, archive_queue_message, handle_job_payload, mark_result_cache_failed,
        read_one_queue_message, update_job_status,
    },
    types::JobPayload,
};

fn extract_snapshot_id(payload: &JobPayload) -> Option<Uuid> {
    match payload {
        JobPayload::PrepareFactorization { snapshot_id, .. }
        | JobPayload::SolveOne { snapshot_id, .. }
        | JobPayload::SolveBatch { snapshot_id, .. }
        | JobPayload::SolveAllUnit { snapshot_id, .. }
        | JobPayload::AnalyzeContributionPath { snapshot_id, .. }
        | JobPayload::InvalidateFactorization { snapshot_id, .. }
        | JobPayload::RebuildFactorization { snapshot_id, .. } => Some(*snapshot_id),
        JobPayload::BuildSnapshot { .. } => None,
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
    }
}

fn extract_job_id_from_raw_payload(payload: &Value) -> Option<Uuid> {
    payload
        .get("job_id")
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}
