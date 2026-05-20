use std::sync::Arc;

use clap::Parser;
use serde_json::json;
use solver_worker::{
    config::AppConfig,
    db::{AppState, archive_queue_message, read_one_queue_message},
    package_db::{
        extract_package_job_id, extract_package_job_id_from_raw_payload,
        handle_package_job_payload, is_retryable_package_job_error,
        mark_package_request_cache_failed, reschedule_retryable_package_job,
        update_package_job_status,
    },
    package_execution::clear_runtime_export_traversal_cache,
    package_types::{PACKAGE_QUEUE_NAME, PackageJobPayload},
    storage::ObjectStoreUploadError,
};
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};

#[derive(Debug, Clone, Parser)]
#[command(name = "package-worker")]
struct PackageWorkerCli {
    #[command(flatten)]
    app: AppConfig,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = PackageWorkerCli::parse();
    let state = Arc::new(AppState::new(&cli.app).await?);
    let queue_name = resolve_queue_name(&cli.app.pgmq_queue);

    if cli.app.pgmq_queue == "lca_jobs" {
        info!(
            queue = %queue_name,
            "using package queue default instead of solver-worker queue default"
        );
    }

    run_package_worker_loop(
        state,
        queue_name,
        cli.app.worker_vt_seconds,
        cli.app.poll_interval(),
    )
    .await
}

#[instrument(skip(state))]
#[allow(clippy::too_many_lines)]
async fn run_package_worker_loop(
    state: Arc<AppState>,
    queue_name: String,
    vt_seconds: i32,
    poll_interval: std::time::Duration,
) -> anyhow::Result<()> {
    loop {
        match read_one_queue_message(&state.queue_pool, &queue_name, vt_seconds).await {
            Ok(Some(message)) => {
                let parsed = serde_json::from_value::<PackageJobPayload>(message.payload.clone());
                match parsed {
                    Ok(payload) => {
                        if let Err(err) = handle_package_job_payload(&state, payload.clone()).await
                        {
                            error!(error = %err, "package job execution failed");
                            let job_id = extract_package_job_id(&payload);
                            let err_message = err.to_string();
                            let mut rescheduled = false;
                            clear_runtime_export_traversal_cache(job_id);
                            if is_retryable_package_job_error(&err) {
                                match reschedule_retryable_package_job(
                                    &state.pool,
                                    &payload,
                                    &err_message,
                                )
                                .await
                                {
                                    Ok(true) => {
                                        rescheduled = true;
                                    }
                                    Ok(false) => {
                                        warn!(
                                            job_id = %job_id,
                                            error = %err_message,
                                            "package job retry budget exhausted"
                                        );
                                    }
                                    Err(retry_err) => {
                                        warn!(
                                            job_id = %job_id,
                                            error = %retry_err,
                                            original_error = %err_message,
                                            "failed to reschedule retryable package job"
                                        );
                                    }
                                }
                            }
                            if !rescheduled {
                                let diagnostics =
                                    build_package_job_failure_diagnostics(&payload, &err);
                                let cache_error_code =
                                    package_request_cache_error_code(&err).to_owned();
                                let cache_error_message =
                                    package_request_cache_error_message(&payload, &err);
                                let _ = update_package_job_status(
                                    &state.pool,
                                    job_id,
                                    "failed",
                                    diagnostics,
                                )
                                .await;
                                let _ = mark_package_request_cache_failed(
                                    &state.pool,
                                    job_id,
                                    cache_error_code.as_str(),
                                    cache_error_message.as_str(),
                                )
                                .await;
                            }
                        } else {
                            info!("package job completed");
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, "invalid package job payload");
                        if let Some(job_id) =
                            extract_package_job_id_from_raw_payload(&message.payload)
                        {
                            let err_message = format!("invalid package job payload: {err}");
                            let _ = update_package_job_status(
                                &state.pool,
                                job_id,
                                "failed",
                                json!({"error": err_message}),
                            )
                            .await;
                            let _ = mark_package_request_cache_failed(
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
                error!(error = %err, "package queue read error");
                sleep(poll_interval).await;
            }
        }
    }
}

fn resolve_queue_name(requested: &str) -> String {
    if requested == "lca_jobs" {
        PACKAGE_QUEUE_NAME.to_owned()
    } else {
        requested.to_owned()
    }
}

fn build_package_job_failure_diagnostics(
    payload: &PackageJobPayload,
    err: &anyhow::Error,
) -> serde_json::Value {
    let phase = match payload {
        PackageJobPayload::ExportPackage { .. } => "export_package",
        PackageJobPayload::ImportPackage { .. } => "import_package",
    };
    let message = package_request_cache_error_message(payload, err);
    if let Some(upload_err) = find_object_store_upload_error(err) {
        return json!({
            "phase": phase,
            "stage": upload_err.stage,
            "result": "failed",
            "error_code": upload_err.error_code(),
            "message": message,
            "error": err.to_string(),
            "upload_mode": upload_err.upload_mode,
            "artifact_byte_size": upload_err.object_byte_size,
            "http_status": upload_err.status_code,
            "storage_error_code": upload_err.s3_error_code,
            "part_number": upload_err.part_number,
            "part_count": upload_err.part_count,
            "is_oversize": upload_err.is_oversize(),
        });
    }

    json!({
        "phase": phase,
        "stage": phase,
        "result": "failed",
        "error_code": "job_execution_failed",
        "message": message,
        "error": err.to_string(),
    })
}

fn package_request_cache_error_code(err: &anyhow::Error) -> &'static str {
    find_object_store_upload_error(err)
        .map_or("job_execution_failed", ObjectStoreUploadError::error_code)
}

fn package_request_cache_error_message(payload: &PackageJobPayload, err: &anyhow::Error) -> String {
    if let Some(upload_err) = find_object_store_upload_error(err)
        && upload_err.is_oversize()
    {
        let operation = match payload {
            PackageJobPayload::ExportPackage { .. } => "export package",
            PackageJobPayload::ImportPackage { .. } => "package artifact",
        };
        return format!(
            "The {operation} exceeded the object storage upload size limit; upload mode={}, stage={}, artifact_byte_size={}.",
            upload_err.upload_mode,
            upload_err.stage,
            upload_err
                .object_byte_size
                .map_or_else(|| "unknown".to_owned(), |size| size.to_string())
        );
    }

    err.to_string()
}

fn find_object_store_upload_error(err: &anyhow::Error) -> Option<&ObjectStoreUploadError> {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<ObjectStoreUploadError>())
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use reqwest::StatusCode;
    use solver_worker::package_types::PackageExportScope;
    use solver_worker::storage::ObjectStoreUploadError;
    use uuid::Uuid;

    use super::{
        build_package_job_failure_diagnostics, package_request_cache_error_code,
        package_request_cache_error_message,
    };
    fn export_payload() -> solver_worker::package_types::PackageJobPayload {
        solver_worker::package_types::PackageJobPayload::ExportPackage {
            job_id: Uuid::nil(),
            requested_by: Uuid::nil(),
            scope: PackageExportScope::OpenData,
            roots: Vec::new(),
        }
    }

    #[test]
    fn package_failure_diagnostics_surface_structured_upload_fields() {
        let payload = export_payload();
        let err = Error::new(ObjectStoreUploadError {
            stage: "upload_object",
            upload_mode: "single_put",
            status_code: Some(StatusCode::PAYLOAD_TOO_LARGE.as_u16()),
            s3_error_code: Some("EntityTooLarge".to_owned()),
            object_byte_size: Some(12),
            part_number: None,
            part_count: None,
            message: "object upload failed".to_owned(),
        });

        let diagnostics = build_package_job_failure_diagnostics(&payload, &err);

        assert_eq!(diagnostics["error_code"], "artifact_too_large");
        assert_eq!(diagnostics["upload_mode"], "single_put");
        assert_eq!(diagnostics["artifact_byte_size"], 12);
        assert_eq!(diagnostics["is_oversize"], true);
    }

    #[test]
    fn package_request_cache_error_message_humanizes_oversize_uploads() {
        let payload = export_payload();
        let err = Error::new(ObjectStoreUploadError {
            stage: "upload_object",
            upload_mode: "single_put",
            status_code: Some(StatusCode::PAYLOAD_TOO_LARGE.as_u16()),
            s3_error_code: Some("EntityTooLarge".to_owned()),
            object_byte_size: Some(34),
            part_number: None,
            part_count: None,
            message: "object upload failed".to_owned(),
        });

        assert_eq!(package_request_cache_error_code(&err), "artifact_too_large");
        assert!(package_request_cache_error_message(&payload, &err).contains("upload size limit"));
    }
}
