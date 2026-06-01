use std::sync::Arc;

use axum::serve;
use clap::Parser;
use solver_worker::{
    config::{AppConfig, QueueBackend, RunMode},
    db::AppState,
    http, queue,
};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = AppConfig::parse();
    let state = Arc::new(AppState::new(&config).await?);

    match config.mode {
        RunMode::Worker => {
            info!("starting queue worker mode");
            run_worker(state, &config).await?;
        }
        RunMode::Http => {
            info!("starting internal HTTP mode");
            run_http(state, config.http_socket_addr()?).await?;
        }
        RunMode::Both => {
            info!("starting worker + internal HTTP mode");
            let worker_state = Arc::clone(&state);
            let worker_config = config.clone();
            let worker_handle =
                tokio::spawn(async move { run_worker(worker_state, &worker_config).await });

            let http_handle =
                tokio::spawn(run_http(Arc::clone(&state), config.http_socket_addr()?));

            tokio::select! {
                worker_result = worker_handle => {
                    worker_result??;
                }
                http_result = http_handle => {
                    http_result??;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("ctrl-c received, exiting");
                }
            }
        }
    }

    Ok(())
}

async fn run_worker(state: Arc<AppState>, config: &AppConfig) -> anyhow::Result<()> {
    match config.queue_backend {
        QueueBackend::Pgmq => {
            let queue_name = config.pgmq_queue.clone();
            queue::run_worker_loop(
                state,
                queue_name,
                config.worker_vt_seconds,
                config.poll_interval(),
            )
            .await
        }
        QueueBackend::WorkerJobs => {
            queue::run_solver_worker_jobs_loop(
                state,
                config.worker_id(),
                config.worker_jobs_claim_limit(),
                config.worker_jobs_lease_seconds(),
                config.poll_interval(),
            )
            .await
        }
    }
}

async fn run_http(state: Arc<AppState>, addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let app = http::router(state);
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "internal HTTP listening");
    serve(listener, app).await?;
    Ok(())
}
