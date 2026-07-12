use std::{
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use uuid::Uuid;

pub const DEFAULT_LOCAL_SNAPSHOT_REPORT_RETENTION_DAYS: u64 = 14;
pub const DEFAULT_LOCAL_SNAPSHOT_REPORT_MAX_FILES: usize = 100;
pub const DEFAULT_LOCAL_SNAPSHOT_REPORT_MIN_FREE_BYTES: u64 = 1_073_741_824;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalSnapshotReportMode {
    Guarded,
    Force,
    Disabled,
}

impl LocalSnapshotReportMode {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "guarded" | "enabled" | "true" | "1" => Ok(Self::Guarded),
            "force" => Ok(Self::Force),
            "disabled" | "false" | "0" | "off" => Ok(Self::Disabled),
            other => Err(anyhow::anyhow!(
                "SNAPSHOT_REPORT_MODE must be one of guarded, force, or disabled; got {other}"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalSnapshotReportPolicy {
    pub retention_days: u64,
    pub max_files: usize,
    pub min_free_bytes: u64,
    pub mode: LocalSnapshotReportMode,
}

impl Default for LocalSnapshotReportPolicy {
    fn default() -> Self {
        Self {
            retention_days: DEFAULT_LOCAL_SNAPSHOT_REPORT_RETENTION_DAYS,
            max_files: DEFAULT_LOCAL_SNAPSHOT_REPORT_MAX_FILES,
            min_free_bytes: DEFAULT_LOCAL_SNAPSHOT_REPORT_MIN_FREE_BYTES,
            mode: LocalSnapshotReportMode::Guarded,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalReportCandidate {
    pub path: PathBuf,
    pub modified_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalReportWriteDecision {
    Write,
    Skip { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalReportCleanupSummary {
    pub deleted_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalReportWriteOutcome {
    Written {
        paths: Vec<PathBuf>,
        deleted_paths: Vec<PathBuf>,
    },
    Skipped {
        reason: String,
        deleted_paths: Vec<PathBuf>,
    },
}

pub fn validate_local_snapshot_report_policy(
    policy: LocalSnapshotReportPolicy,
) -> anyhow::Result<LocalSnapshotReportPolicy> {
    if !(1..=3650).contains(&policy.retention_days) {
        return Err(anyhow::anyhow!(
            "SNAPSHOT_REPORT_RETENTION_DAYS must be between 1 and 3650"
        ));
    }
    if policy.max_files == 0 {
        return Err(anyhow::anyhow!("SNAPSHOT_REPORT_MAX_FILES must be > 0"));
    }
    Ok(policy)
}

#[must_use]
pub fn select_report_files_to_delete(
    candidates: &[LocalReportCandidate],
    policy: LocalSnapshotReportPolicy,
    now: SystemTime,
) -> Vec<PathBuf> {
    let mut ordered = candidates.to_vec();
    ordered.sort_by(|left, right| {
        left.modified_at
            .cmp(&right.modified_at)
            .then_with(|| left.path.cmp(&right.path))
    });

    let retention = Duration::from_secs(policy.retention_days.saturating_mul(24 * 60 * 60));
    let mut delete = Vec::new();
    let mut retained = Vec::new();

    for candidate in ordered {
        let expired = now
            .duration_since(candidate.modified_at)
            .is_ok_and(|age| age > retention);
        if expired {
            delete.push(candidate.path);
        } else {
            retained.push(candidate);
        }
    }

    let excess_count = retained.len().saturating_sub(policy.max_files);
    delete.extend(
        retained
            .into_iter()
            .take(excess_count)
            .map(|candidate| candidate.path),
    );

    delete
}

#[must_use]
pub fn decide_local_report_write(
    policy: LocalSnapshotReportPolicy,
    available_bytes: Option<u64>,
    planned_write_bytes: u64,
) -> LocalReportWriteDecision {
    match policy.mode {
        LocalSnapshotReportMode::Disabled => {
            return LocalReportWriteDecision::Skip {
                reason: "local snapshot reports disabled by SNAPSHOT_REPORT_MODE".to_owned(),
            };
        }
        LocalSnapshotReportMode::Force => return LocalReportWriteDecision::Write,
        LocalSnapshotReportMode::Guarded => {}
    }

    if policy.min_free_bytes == 0 {
        return LocalReportWriteDecision::Write;
    }

    let Some(available_bytes) = available_bytes else {
        return LocalReportWriteDecision::Write;
    };

    let remaining_after_write = available_bytes.saturating_sub(planned_write_bytes);
    if remaining_after_write < policy.min_free_bytes {
        return LocalReportWriteDecision::Skip {
            reason: format!(
                "local snapshot report write would leave {remaining_after_write} bytes available, below minimum free space {} bytes",
                policy.min_free_bytes
            ),
        };
    }

    LocalReportWriteDecision::Write
}

pub fn prune_local_snapshot_reports(
    report_dir: &Path,
    policy: LocalSnapshotReportPolicy,
) -> anyhow::Result<LocalReportCleanupSummary> {
    if !report_dir.exists() {
        return Ok(LocalReportCleanupSummary {
            deleted_paths: Vec::new(),
        });
    }

    let candidates = collect_local_report_candidates(report_dir)?;
    let paths_to_delete = select_report_files_to_delete(&candidates, policy, SystemTime::now());
    let mut removed_paths = Vec::with_capacity(paths_to_delete.len());
    for path in paths_to_delete {
        match fs::remove_file(&path) {
            Ok(()) => removed_paths.push(path),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }

    Ok(LocalReportCleanupSummary {
        deleted_paths: removed_paths,
    })
}

pub fn write_optional_local_report_files(
    report_dir: &Path,
    files: Vec<(PathBuf, Vec<u8>)>,
    policy: LocalSnapshotReportPolicy,
) -> anyhow::Result<LocalReportWriteOutcome> {
    fs::create_dir_all(report_dir)?;
    let cleanup_before = prune_local_snapshot_reports(report_dir, policy)?;
    let planned_write_bytes = files.iter().fold(0_u64, |acc, (_, bytes)| {
        acc.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
    });
    let available_bytes = fs2::available_space(report_dir).ok();

    if let LocalReportWriteDecision::Skip { reason } =
        decide_local_report_write(policy, available_bytes, planned_write_bytes)
    {
        return Ok(LocalReportWriteOutcome::Skipped {
            reason,
            deleted_paths: cleanup_before.deleted_paths,
        });
    }

    let mut written_paths = Vec::with_capacity(files.len());
    for (path, bytes) in files {
        match fs::write(&path, bytes) {
            Ok(()) => written_paths.push(path),
            Err(err) if is_storage_full_error(&err) => {
                for written_path in &written_paths {
                    let _ = fs::remove_file(written_path);
                }
                return Ok(LocalReportWriteOutcome::Skipped {
                    reason: format!(
                        "local snapshot report write failed because storage is full: {err}"
                    ),
                    deleted_paths: cleanup_before.deleted_paths,
                });
            }
            Err(err) => return Err(err.into()),
        }
    }

    let cleanup_after = prune_local_snapshot_reports(report_dir, policy)?;
    let mut deleted_paths = cleanup_before.deleted_paths;
    deleted_paths.extend(cleanup_after.deleted_paths);

    Ok(LocalReportWriteOutcome::Written {
        paths: written_paths,
        deleted_paths,
    })
}

fn collect_local_report_candidates(report_dir: &Path) -> anyhow::Result<Vec<LocalReportCandidate>> {
    let mut candidates = Vec::new();
    for entry in fs::read_dir(report_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let path = entry.path();
        if !is_generated_snapshot_report_file(&path) {
            continue;
        }
        let modified_at = entry
            .metadata()?
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH);
        candidates.push(LocalReportCandidate { path, modified_at });
    }
    Ok(candidates)
}

fn is_generated_snapshot_report_file(path: &Path) -> bool {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    if file_name.starts_with("matrix-readiness-")
        && path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
    {
        return true;
    }

    let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        return false;
    };
    let Some(extension) = path.extension().and_then(|extension| extension.to_str()) else {
        return false;
    };

    matches!(extension, "json" | "md") && Uuid::parse_str(stem).is_ok()
}

fn is_storage_full_error(error: &io::Error) -> bool {
    error.raw_os_error() == Some(28)
}

#[cfg(test)]
mod tests {
    use super::{
        LocalReportCandidate, LocalReportWriteDecision, LocalSnapshotReportMode,
        LocalSnapshotReportPolicy, decide_local_report_write, select_report_files_to_delete,
    };
    use std::{
        path::PathBuf,
        time::{Duration, SystemTime},
    };

    #[test]
    fn retention_deletes_expired_reports_and_excess_oldest_files() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_hours(720);
        let policy = LocalSnapshotReportPolicy {
            retention_days: 14,
            max_files: 2,
            ..LocalSnapshotReportPolicy::default()
        };
        let candidates = vec![
            candidate("matrix-readiness-old.json", now - Duration::from_hours(480)),
            candidate("matrix-readiness-b.json", now - Duration::from_hours(72)),
            candidate("matrix-readiness-c.json", now - Duration::from_hours(48)),
            candidate("matrix-readiness-d.json", now - Duration::from_hours(24)),
        ];

        let delete = select_report_files_to_delete(&candidates, policy, now);

        assert_eq!(
            delete,
            vec![
                PathBuf::from("matrix-readiness-old.json"),
                PathBuf::from("matrix-readiness-b.json"),
            ]
        );
    }

    #[test]
    fn guarded_write_skips_when_planned_report_would_cross_min_free_space() {
        let policy = LocalSnapshotReportPolicy {
            min_free_bytes: 1_000,
            mode: LocalSnapshotReportMode::Guarded,
            ..LocalSnapshotReportPolicy::default()
        };

        let decision = decide_local_report_write(policy, Some(1_200), 250);

        match decision {
            LocalReportWriteDecision::Skip { reason } => {
                assert!(reason.contains("below minimum free space"));
                assert!(reason.contains("1000"));
            }
            LocalReportWriteDecision::Write => panic!("expected low-disk write to be skipped"),
        }
    }

    #[test]
    fn disabled_mode_skips_even_when_space_is_available() {
        let policy = LocalSnapshotReportPolicy {
            mode: LocalSnapshotReportMode::Disabled,
            ..LocalSnapshotReportPolicy::default()
        };

        assert!(matches!(
            decide_local_report_write(policy, Some(10_000), 1),
            LocalReportWriteDecision::Skip { .. }
        ));
    }

    fn candidate(path: &str, modified_at: SystemTime) -> LocalReportCandidate {
        LocalReportCandidate {
            path: PathBuf::from(path),
            modified_at,
        }
    }
}
