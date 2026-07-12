//! Worker crate library modules shared by binaries.

pub const DEFAULT_SNAPSHOT_PROCESS_STATE_START: i32 = 100;
pub const DEFAULT_SNAPSHOT_PROCESS_STATE_END: i32 = 199;

#[must_use]
pub fn default_snapshot_process_states_arg() -> String {
    (DEFAULT_SNAPSHOT_PROCESS_STATE_START..=DEFAULT_SNAPSHOT_PROCESS_STATE_END)
        .map(|state| state.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

pub mod artifacts;
pub mod calculation_evidence;
pub mod compiled_graph;
pub mod config;
pub mod contribution_path;
pub mod db;
pub mod db_pool;
pub mod graph_types;
pub mod http;
pub mod local_reports;
pub mod package_artifacts;
pub mod package_db;
pub mod package_execution;
pub mod package_retention;
pub mod package_types;
pub mod pgbouncer_sqlx;
pub mod queue;
pub mod readiness;
pub mod review_submit_gate;
pub mod review_submit_gate_runner;
pub mod snapshot_artifacts;
pub mod snapshot_index;
pub mod snapshot_retention;
pub mod static_lcia_cache;
pub mod storage;
pub mod types;
pub mod worker_jobs;
