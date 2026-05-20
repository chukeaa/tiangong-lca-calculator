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
pub mod compiled_graph;
pub mod config;
pub mod contribution_path;
pub mod db;
pub mod graph_types;
pub mod http;
pub mod package_artifacts;
pub mod package_db;
pub mod package_execution;
pub mod package_types;
pub mod pgbouncer_sqlx;
pub mod queue;
pub mod snapshot_artifacts;
pub mod snapshot_index;
pub mod storage;
pub mod types;
