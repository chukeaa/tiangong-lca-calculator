use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Dedicated queue name for package worker jobs.
pub const PACKAGE_QUEUE_NAME: &str = "lca_package_jobs";
/// Unified `worker_jobs` queue name for package worker jobs.
pub const PACKAGE_WORKER_QUEUE: &str = "package";
/// Unified `worker_jobs` kind for TIDAS package exports.
pub const PACKAGE_EXPORT_WORKER_JOB_KIND: &str = "tidas.export_package";
/// Unified `worker_jobs` kind for TIDAS package imports.
pub const PACKAGE_IMPORT_WORKER_JOB_KIND: &str = "tidas.import_package";
/// Payload schema for TIDAS package export worker jobs.
pub const PACKAGE_EXPORT_PAYLOAD_SCHEMA_VERSION: &str = "tidas.export_package.request.v1";
/// Payload schema for TIDAS package import worker jobs.
pub const PACKAGE_IMPORT_PAYLOAD_SCHEMA_VERSION: &str = "tidas.import_package.request.v1";
/// Result schema for TIDAS package export worker jobs.
pub const PACKAGE_EXPORT_RESULT_SCHEMA_VERSION: &str = "tidas.export_package.result.v1";
/// Result schema for TIDAS package import worker jobs.
pub const PACKAGE_IMPORT_RESULT_SCHEMA_VERSION: &str = "tidas.import_package.result.v1";
/// ZIP artifact format for exported packages and uploaded import sources.
pub const PACKAGE_ZIP_ARTIFACT_FORMAT: &str = "tidas-package-zip:v1";
/// JSON artifact format for export summaries.
pub const PACKAGE_EXPORT_REPORT_ARTIFACT_FORMAT: &str = "tidas-package-export-report:v1";
/// JSON artifact format for import summaries and conflict reports.
pub const PACKAGE_IMPORT_REPORT_ARTIFACT_FORMAT: &str = "tidas-package-import-report:v1";
/// MIME type for ZIP artifacts.
pub const PACKAGE_ZIP_CONTENT_TYPE: &str = "application/zip";
/// MIME type for JSON report artifacts.
pub const PACKAGE_REPORT_CONTENT_TYPE: &str = "application/json";

/// Supported top-level TIDAS entities that can seed package export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageRootTable {
    Contacts,
    Sources,
    Unitgroups,
    Flowproperties,
    Flows,
    Processes,
    Lifecyclemodels,
}

/// Export scopes exposed to edge/frontend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageExportScope {
    CurrentUser,
    OpenData,
    CurrentUserAndOpenData,
    SelectedRoots,
}

/// Stored artifact kinds for one package job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageArtifactKind {
    ImportSource,
    ExportZip,
    ExportReport,
    ImportReport,
}

impl PackageArtifactKind {
    /// Artifact format identifier stored in `PostgreSQL`.
    #[must_use]
    pub const fn artifact_format(self) -> &'static str {
        match self {
            Self::ImportSource | Self::ExportZip => PACKAGE_ZIP_ARTIFACT_FORMAT,
            Self::ExportReport => PACKAGE_EXPORT_REPORT_ARTIFACT_FORMAT,
            Self::ImportReport => PACKAGE_IMPORT_REPORT_ARTIFACT_FORMAT,
        }
    }

    /// HTTP content type used for upload/download.
    #[must_use]
    pub const fn content_type(self) -> &'static str {
        match self {
            Self::ImportSource | Self::ExportZip => PACKAGE_ZIP_CONTENT_TYPE,
            Self::ExportReport | Self::ImportReport => PACKAGE_REPORT_CONTENT_TYPE,
        }
    }
}

/// Root item selected for package export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageRootRef {
    pub table: PackageRootTable,
    pub id: Uuid,
    pub version: String,
}

/// Queue payload for package export/import jobs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PackageJobPayload {
    ExportPackage {
        job_id: Uuid,
        requested_by: Uuid,
        scope: PackageExportScope,
        #[serde(default)]
        roots: Vec<PackageRootRef>,
    },
    ImportPackage {
        job_id: Uuid,
        requested_by: Uuid,
        source_artifact_id: Uuid,
    },
}

/// Lightweight artifact row contract shared with edge-facing status APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageArtifactDescriptor {
    pub job_id: Uuid,
    pub artifact_kind: PackageArtifactKind,
    pub artifact_url: String,
    pub artifact_format: String,
    pub content_type: String,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        PACKAGE_EXPORT_REPORT_ARTIFACT_FORMAT, PACKAGE_IMPORT_REPORT_ARTIFACT_FORMAT,
        PACKAGE_ZIP_ARTIFACT_FORMAT, PackageArtifactKind, PackageExportScope, PackageJobPayload,
    };

    #[test]
    fn deserialize_export_package_payload() {
        let payload = json!({
            "type": "export_package",
            "job_id": Uuid::nil(),
            "requested_by": Uuid::nil(),
            "scope": "selected_roots",
            "roots": [{
                "table": "processes",
                "id": Uuid::nil(),
                "version": "01.00.000"
            }]
        });

        let parsed: PackageJobPayload =
            serde_json::from_value(payload).expect("parse export payload");
        match parsed {
            PackageJobPayload::ExportPackage { scope, roots, .. } => {
                assert_eq!(scope, PackageExportScope::SelectedRoots);
                assert_eq!(roots.len(), 1);
                assert_eq!(roots[0].version, "01.00.000");
            }
            other @ PackageJobPayload::ImportPackage { .. } => {
                panic!("unexpected payload: {other:?}");
            }
        }
    }

    #[test]
    fn deserialize_import_package_payload() {
        let payload = json!({
            "type": "import_package",
            "job_id": Uuid::nil(),
            "requested_by": Uuid::nil(),
            "source_artifact_id": Uuid::nil()
        });

        let parsed: PackageJobPayload =
            serde_json::from_value(payload).expect("parse import payload");
        assert!(matches!(parsed, PackageJobPayload::ImportPackage { .. }));
    }

    #[test]
    fn artifact_kind_maps_to_expected_formats() {
        assert_eq!(
            PackageArtifactKind::ExportZip.artifact_format(),
            PACKAGE_ZIP_ARTIFACT_FORMAT
        );
        assert_eq!(
            PackageArtifactKind::ImportSource.artifact_format(),
            PACKAGE_ZIP_ARTIFACT_FORMAT
        );
        assert_eq!(
            PackageArtifactKind::ExportReport.artifact_format(),
            PACKAGE_EXPORT_REPORT_ARTIFACT_FORMAT
        );
        assert_eq!(
            PackageArtifactKind::ImportReport.artifact_format(),
            PACKAGE_IMPORT_REPORT_ARTIFACT_FORMAT
        );
    }
}
