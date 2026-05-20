use anyhow::Context;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::{Cursor, Read, Seek, Write, copy},
    path::Path,
    process::Command,
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

use crate::pgbouncer_sqlx::{self as sqlx, PgPool, Postgres, QueryBuilder, Row};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tempfile::{Builder, NamedTempFile, TempDir};
use uuid::Uuid;
use zip::{CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions};

use crate::{
    db::AppState,
    package_artifacts::{
        encode_export_report_artifact, encode_import_report_artifact,
        prepare_package_zip_artifact_from_path,
    },
    package_db::{PackageArtifactInsert, insert_package_artifact},
    package_types::{
        PACKAGE_ZIP_ARTIFACT_FORMAT, PackageArtifactKind, PackageExportScope, PackageRootRef,
        PackageRootTable,
    },
};

const OPEN_DATA_STATE_CODE_START: i32 = 100;
const OPEN_DATA_STATE_CODE_END: i32 = 199;
const PACKAGE_MANIFEST_FORMAT: &str = "tiangong-tidas-package";
const PACKAGE_MANIFEST_VERSION: u8 = 2;
const PACKAGE_ZIP_COMPRESSION_LEVEL: i64 = 6;
const LEGACY_PACKAGE_DIR: &str = "data";
const EXPORT_ZIP_SUFFIX: &str = "export-package";
const EXPORT_REPORT_SUFFIX: &str = "export-report";
const IMPORT_REPORT_SUFFIX: &str = "import-report";
const EXPORT_REF_BATCH_SIZE: i64 = 96;
const EXPORT_SEED_SCAN_BATCH_SIZE: i64 = 256;
const EXPORT_FINALIZE_FETCH_BATCH_SIZE: usize = 256;
const EXPORT_ITEM_INSERT_CHUNK_SIZE: usize = 500;
const EXPORT_BATCHES_PER_PASS: usize = 6;
const EXPORT_PASS_TIME_BUDGET: Duration = Duration::from_secs(20);

const SUPPORTED_PACKAGE_TABLES: [PackageRootTable; 7] = [
    PackageRootTable::Contacts,
    PackageRootTable::Sources,
    PackageRootTable::Unitgroups,
    PackageRootTable::Flowproperties,
    PackageRootTable::Flows,
    PackageRootTable::Processes,
    PackageRootTable::Lifecyclemodels,
];

const INSERT_ORDER: [PackageRootTable; 7] = [
    PackageRootTable::Contacts,
    PackageRootTable::Sources,
    PackageRootTable::Unitgroups,
    PackageRootTable::Flowproperties,
    PackageRootTable::Flows,
    PackageRootTable::Lifecyclemodels,
    PackageRootTable::Processes,
];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageEntry {
    table: PackageRootTable,
    id: Uuid,
    version: String,
    json_ordered: Value,
    rule_verification: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    json_tg: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct ReferenceTarget {
    table: PackageRootTable,
    id: Uuid,
    version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageManifestEntry {
    table: PackageRootTable,
    id: Uuid,
    version: String,
    file_path: String,
    rule_verification: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageManifest {
    format: String,
    version: u8,
    exported_at: String,
    scope: PackageExportScope,
    roots: Vec<PackageRootRef>,
    #[serde(default)]
    entries: Vec<PackageManifestEntry>,
    #[serde(default)]
    counts: BTreeMap<String, usize>,
    total_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ConflictRecord {
    table: PackageRootTable,
    id: Uuid,
    version: String,
    state_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct ConflictRow {
    id: Uuid,
    version: String,
    state_code: Option<i32>,
    user_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct PackageArtifactMeta {
    artifact_kind: PackageArtifactKind,
    status: String,
    artifact_url: String,
    artifact_format: String,
}

#[derive(Debug, Clone, Serialize)]
struct ExportReportSummary {
    total_entries: usize,
    root_count: usize,
    counts: BTreeMap<String, usize>,
    filename: String,
    scope: PackageExportScope,
}

#[derive(Debug, Clone, Serialize)]
struct ExportReportDocument {
    ok: bool,
    code: &'static str,
    message: &'static str,
    summary: ExportReportSummary,
    manifest: PackageManifest,
}

#[derive(Debug, Clone, Serialize)]
struct ImportReportSummary {
    total_entries: usize,
    filtered_open_data_count: usize,
    user_conflict_count: usize,
    importable_count: usize,
    imported_count: usize,
    validation_issue_count: usize,
    error_count: usize,
    warning_count: usize,
}

fn default_issue_location() -> String {
    "<root>".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidationIssueDetail {
    issue_code: String,
    severity: String,
    category: String,
    file_path: String,
    #[serde(default = "default_issue_location", alias = "path")]
    location: String,
    message: String,
    #[serde(default)]
    context: Value,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[allow(clippy::struct_field_names)]
struct TidasValidationSummary {
    #[serde(default)]
    issue_count: usize,
    #[serde(default)]
    error_count: usize,
    #[serde(default)]
    warning_count: usize,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct TidasValidationReport {
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    summary: TidasValidationSummary,
    #[serde(default)]
    issues: Vec<ValidationIssueDetail>,
}

#[derive(Debug, Clone, Serialize)]
struct ImportReportDocument {
    ok: bool,
    code: &'static str,
    message: &'static str,
    summary: ImportReportSummary,
    filtered_open_data: Vec<ConflictRecord>,
    user_conflicts: Vec<ConflictRecord>,
    validation_issues: Vec<ValidationIssueDetail>,
}

#[derive(Debug, Clone)]
pub struct PackageExecutionOutcome {
    pub final_status: &'static str,
    pub diagnostics: Value,
    pub export_artifact_id: Option<Uuid>,
    pub report_artifact_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct PackageExportItem {
    table: PackageRootTable,
    id: Uuid,
    version: String,
}

#[derive(Debug, Clone)]
struct PackageSeedScanEntry {
    table: PackageRootTable,
    id: Uuid,
    version: String,
    ref_candidates: Value,
    submodels: Value,
    model_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct ExportBatchResult {
    processed_count: usize,
    discovered_count: usize,
}

#[derive(Debug, Clone, Default)]
struct ExportTraversalCache {
    known_exact: HashSet<String>,
    known_any_version: HashSet<(PackageRootTable, Uuid)>,
    resolved_latest: HashMap<(PackageRootTable, Uuid), Option<String>>,
}

static EXPORT_TRAVERSAL_RUNTIME_CACHE: LazyLock<Mutex<HashMap<Uuid, ExportTraversalCache>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Default)]
struct PlannedReferenceResolution {
    cached_roots: Vec<PackageRootRef>,
    exact_roots_to_fetch: Vec<PackageRootRef>,
    latest_refs_to_fetch: Vec<ReferenceTarget>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ExportSeedScanState {
    table_index: usize,
    #[serde(default)]
    last_id: Option<Uuid>,
    #[serde(default)]
    last_version: Option<String>,
    #[serde(default)]
    scanned_seed_count: usize,
    #[serde(default)]
    discovered_external_count: usize,
    #[serde(default)]
    complete: bool,
}

#[derive(Debug, Clone, Default)]
struct ExportSeedScanResult {
    state: ExportSeedScanState,
    pass_scanned_count: usize,
    pass_discovered_count: usize,
}

#[allow(clippy::too_many_lines)]
pub async fn execute_export_package(
    state: &AppState,
    job_id: Uuid,
    requested_by: Uuid,
    scope: PackageExportScope,
    roots: &[PackageRootRef],
) -> anyhow::Result<PackageExecutionOutcome> {
    if matches!(scope, PackageExportScope::SelectedRoots) && roots.is_empty() {
        return Err(anyhow::anyhow!(
            "selected_roots export requires at least one root"
        ));
    }

    let root_count =
        ensure_export_seed_items(&state.pool, job_id, requested_by, scope, roots).await?;
    let pass_started = Instant::now();
    let full_scope_export = !matches!(scope, PackageExportScope::SelectedRoots);
    if full_scope_export {
        mark_seed_export_items_refs_done(&state.pool, job_id).await?;
    }

    let mut traversal_cache = load_export_traversal_cache(&state.pool, job_id, scope).await?;
    let mut seed_scan_complete = !full_scope_export;
    let mut seed_scan_scanned_in_pass = 0usize;
    let mut seed_scan_discovered_in_pass = 0usize;

    if full_scope_export {
        let diagnostics = fetch_package_job_diagnostics(&state.pool, job_id).await?;
        let seed_scan_state = load_export_seed_scan_state(&diagnostics);
        if !seed_scan_state.complete {
            let seed_scan_result = process_seed_scan_pass(
                &state.pool,
                requested_by,
                scope,
                job_id,
                &mut traversal_cache,
                seed_scan_state,
                pass_started,
            )
            .await?;
            seed_scan_scanned_in_pass = seed_scan_result.pass_scanned_count;
            seed_scan_discovered_in_pass = seed_scan_result.pass_discovered_count;
            store_runtime_export_traversal_cache(job_id, &traversal_cache);
            if !seed_scan_result.state.complete {
                let total_items = count_export_items(&state.pool, job_id).await?;
                let pending_after = count_pending_export_items(&state.pool, job_id).await?;
                let processed_items = total_items.saturating_sub(pending_after);
                return Ok(PackageExecutionOutcome {
                    final_status: "running",
                    diagnostics: export_progress_diagnostics(
                        "scan_seed_roots",
                        scope,
                        root_count,
                        total_items,
                        processed_items,
                        pending_after,
                        json!({
                            "message": "Scanning root datasets for external references",
                            "seed_scan_complete": false,
                            "seed_scan": seed_scan_result.state,
                            "batch_processed_count": seed_scan_result.pass_scanned_count,
                            "batch_discovered_count": seed_scan_result.pass_discovered_count,
                        }),
                    ),
                    export_artifact_id: None,
                    report_artifact_id: None,
                });
            }
        }
        seed_scan_complete = true;
    }

    let mut batches_processed = 0usize;
    let mut processed_in_pass = 0usize;
    let mut discovered_in_pass = 0usize;
    let collect_pass_started = Instant::now();

    while batches_processed < EXPORT_BATCHES_PER_PASS
        && collect_pass_started.elapsed() < EXPORT_PASS_TIME_BUDGET
    {
        let batch = fetch_pending_export_items(&state.pool, job_id, EXPORT_REF_BATCH_SIZE).await?;
        if batch.is_empty() {
            break;
        }

        let batch_result =
            process_export_item_batch(&state.pool, scope, job_id, &batch, &mut traversal_cache)
                .await?;
        processed_in_pass += batch_result.processed_count;
        discovered_in_pass += batch_result.discovered_count;
        batches_processed += 1;
    }

    let total_items = count_export_items(&state.pool, job_id).await?;
    let pending_after = count_pending_export_items(&state.pool, job_id).await?;
    let processed_items = total_items.saturating_sub(pending_after);

    if pending_after > 0 {
        store_runtime_export_traversal_cache(job_id, &traversal_cache);
        if processed_in_pass == 0 && seed_scan_scanned_in_pass == 0 {
            return Ok(PackageExecutionOutcome {
                final_status: "running",
                diagnostics: export_progress_diagnostics(
                    "collect_refs",
                    scope,
                    root_count,
                    total_items,
                    processed_items,
                    pending_after,
                    json!({
                        "batch_processed_count": 0,
                        "batch_discovered_count": 0,
                        "batches_processed": 0,
                        "message": "Waiting to resume related dataset collection",
                        "seed_scan_complete": seed_scan_complete,
                        "seed_scan_batch_processed_count": seed_scan_scanned_in_pass,
                        "seed_scan_batch_discovered_count": seed_scan_discovered_in_pass,
                        "idle_pass": true,
                    }),
                ),
                export_artifact_id: None,
                report_artifact_id: None,
            });
        }

        return Ok(PackageExecutionOutcome {
            final_status: "running",
            diagnostics: export_progress_diagnostics(
                "collect_refs",
                scope,
                root_count,
                total_items,
                processed_items,
                pending_after,
                json!({
                    "batch_processed_count": processed_in_pass,
                    "batch_discovered_count": discovered_in_pass,
                    "batches_processed": batches_processed,
                    "message": "Collecting related datasets",
                    "seed_scan_complete": seed_scan_complete,
                    "seed_scan_batch_processed_count": seed_scan_scanned_in_pass,
                    "seed_scan_batch_discovered_count": seed_scan_discovered_in_pass,
                }),
            ),
            export_artifact_id: None,
            report_artifact_id: None,
        });
    }

    let roots = list_export_seed_roots(&state.pool, job_id).await?;
    let item_refs = list_export_items(&state.pool, job_id).await?;
    let entries =
        fetch_export_entries_by_items(state, job_id, scope, root_count, &item_refs).await?;
    let manifest = build_manifest(scope, &roots, &entries);
    let filename = build_zip_filename(&roots, scope);
    let zip_file = build_package_zip(&manifest, &entries)?;
    let zip_artifact = prepare_package_zip_artifact_from_path(zip_file.path())?;
    let zip_upload = state
        .object_store
        .upload_package_artifact_file(
            job_id,
            EXPORT_ZIP_SUFFIX,
            zip_artifact.extension,
            zip_artifact.content_type,
            zip_file.path(),
            zip_artifact.byte_size,
        )
        .await
        .context("failed to upload export package ZIP artifact")?;
    let zip_url = zip_upload.object_url.clone();
    let zip_artifact_byte_size = zip_artifact.byte_size;
    let zip_artifact_id = insert_package_artifact(
        &state.pool,
        PackageArtifactInsert::ready(
            job_id,
            PackageArtifactKind::ExportZip,
            zip_url.clone(),
            zip_artifact,
            json!({
                "filename": filename,
                "scope": scope,
                "root_count": roots.len(),
                "total_count": entries.len(),
            }),
        ),
    )
    .await?;

    let report_document = ExportReportDocument {
        ok: true,
        code: "EXPORTED",
        message: "TIDAS package exported successfully",
        summary: ExportReportSummary {
            total_entries: entries.len(),
            root_count: roots.len(),
            counts: manifest.counts.clone(),
            filename: filename.clone(),
            scope,
        },
        manifest: manifest.clone(),
    };
    let report_artifact = encode_export_report_artifact(job_id, &report_document)?;
    let report_url = state
        .object_store
        .upload_package_artifact(
            job_id,
            EXPORT_REPORT_SUFFIX,
            report_artifact.extension,
            report_artifact.content_type,
            report_artifact.bytes.clone(),
        )
        .await?;
    let report_artifact_id = insert_package_artifact(
        &state.pool,
        PackageArtifactInsert::ready_from_encoded(
            job_id,
            PackageArtifactKind::ExportReport,
            report_url.clone(),
            &report_artifact,
            json!({
                "filename": format!("{filename}.report.json"),
                "code": report_document.code,
                "total_entries": entries.len(),
                "root_count": roots.len(),
            }),
        )?,
    )
    .await?;

    clear_runtime_export_traversal_cache(job_id);
    Ok(PackageExecutionOutcome {
        final_status: "ready",
        diagnostics: json!({
            "phase": "export_package",
            "stage": "ready",
            "result": "ready",
            "scope": scope,
            "filename": filename,
            "total_entries": entries.len(),
            "root_count": roots.len(),
            "counts": manifest.counts,
            "artifact_byte_size": zip_artifact_byte_size,
            "upload_mode": zip_upload.upload_mode,
            "multipart_part_count": zip_upload.part_count,
            "export_artifact_id": zip_artifact_id,
            "report_artifact_id": report_artifact_id,
            "export_artifact_url": zip_url,
            "report_artifact_url": report_url,
            "message": "TIDAS package exported successfully",
        }),
        export_artifact_id: Some(zip_artifact_id),
        report_artifact_id: Some(report_artifact_id),
    })
}

async fn ensure_export_seed_items(
    pool: &PgPool,
    job_id: Uuid,
    requested_by: Uuid,
    scope: PackageExportScope,
    roots: &[PackageRootRef],
) -> anyhow::Result<usize> {
    if has_export_items(pool, job_id).await? {
        let root_count = count_seed_export_items(pool, job_id).await?;
        update_package_job_root_count(pool, job_id, root_count).await?;
        return Ok(usize::try_from(root_count).unwrap_or_default());
    }

    let seed_roots = if roots.is_empty() {
        fetch_scope_root_refs(pool, requested_by, scope).await?
    } else {
        let existing_roots = fetch_root_refs_by_exact_roots(pool, roots).await?;
        let fetched_keys = existing_roots
            .iter()
            .map(|root| table_key(root.table, root.id, &root.version))
            .collect::<HashSet<_>>();
        let missing_roots = roots
            .iter()
            .filter(|root| !fetched_keys.contains(&table_key(root.table, root.id, &root.version)))
            .count();
        if missing_roots > 0 {
            return Err(anyhow::anyhow!(
                "some selected datasets were not found or are not exportable"
            ));
        }

        existing_roots
    };

    let seed_refs_done = !matches!(scope, PackageExportScope::SelectedRoots);
    insert_export_items(pool, job_id, seed_roots.as_slice(), true, seed_refs_done).await?;
    update_package_job_root_count(
        pool,
        job_id,
        i64::try_from(seed_roots.len()).unwrap_or(i64::MAX),
    )
    .await?;
    Ok(seed_roots.len())
}

async fn load_export_traversal_cache(
    pool: &PgPool,
    job_id: Uuid,
    scope: PackageExportScope,
) -> anyhow::Result<ExportTraversalCache> {
    if let Some(cache) = load_runtime_export_traversal_cache(job_id) {
        return Ok(cache);
    }

    let mut cache = ExportTraversalCache::default();

    if matches!(scope, PackageExportScope::SelectedRoots) {
        let items = list_export_items(pool, job_id).await?;
        for item in items {
            remember_root_in_traversal_cache(
                &mut cache,
                &PackageRootRef {
                    table: item.table,
                    id: item.id,
                    version: item.version,
                },
            );
        }
        return Ok(cache);
    }

    let seed_items = list_seed_export_items_for_cache(pool, job_id).await?;
    for root in seed_items {
        remember_root_in_traversal_cache(&mut cache, &root);
    }

    let external_items = list_non_seed_export_items(pool, job_id).await?;
    for item in external_items {
        remember_root_in_traversal_cache(
            &mut cache,
            &PackageRootRef {
                table: item.table,
                id: item.id,
                version: item.version,
            },
        );
    }

    store_runtime_export_traversal_cache(job_id, &cache);
    Ok(cache)
}

fn load_runtime_export_traversal_cache(job_id: Uuid) -> Option<ExportTraversalCache> {
    let guard = EXPORT_TRAVERSAL_RUNTIME_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    guard.get(&job_id).cloned()
}

fn store_runtime_export_traversal_cache(job_id: Uuid, cache: &ExportTraversalCache) {
    let mut guard = EXPORT_TRAVERSAL_RUNTIME_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _ = guard.insert(job_id, cache.clone());
}

pub fn clear_runtime_export_traversal_cache(job_id: Uuid) {
    let mut guard = EXPORT_TRAVERSAL_RUNTIME_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _ = guard.remove(&job_id);
}

async fn fetch_package_job_diagnostics(pool: &PgPool, job_id: Uuid) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        SELECT diagnostics
        FROM lca_package_jobs
        WHERE id = $1
        ",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await?;
    Ok(row
        .try_get::<Option<Value>, _>("diagnostics")?
        .unwrap_or_else(|| json!({})))
}

fn load_export_seed_scan_state(diagnostics: &Value) -> ExportSeedScanState {
    if diagnostics
        .get("seed_scan_complete")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return ExportSeedScanState {
            complete: true,
            ..ExportSeedScanState::default()
        };
    }

    diagnostics
        .get("seed_scan")
        .cloned()
        .and_then(|value| serde_json::from_value::<ExportSeedScanState>(value).ok())
        .unwrap_or_default()
}

async fn mark_seed_export_items_refs_done(pool: &PgPool, job_id: Uuid) -> anyhow::Result<()> {
    let _ = sqlx::query(
        r"
        UPDATE lca_package_export_items
        SET refs_done = TRUE,
            updated_at = NOW()
        WHERE job_id = $1
          AND is_seed = TRUE
          AND refs_done = FALSE
        ",
    )
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn has_export_items(pool: &PgPool, job_id: Uuid) -> anyhow::Result<bool> {
    let row = sqlx::query(
        r"
        SELECT EXISTS (
            SELECT 1
            FROM lca_package_export_items
            WHERE job_id = $1
        ) AS exists
        ",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get::<bool, _>("exists")?)
}

async fn count_export_items(pool: &PgPool, job_id: Uuid) -> anyhow::Result<i64> {
    let row = sqlx::query(
        r"
        SELECT COUNT(*)::bigint AS count
        FROM lca_package_export_items
        WHERE job_id = $1
        ",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get::<i64, _>("count")?)
}

async fn count_seed_export_items(pool: &PgPool, job_id: Uuid) -> anyhow::Result<i64> {
    let row = sqlx::query(
        r"
        SELECT COUNT(*)::bigint AS count
        FROM lca_package_export_items
        WHERE job_id = $1
          AND is_seed = TRUE
        ",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get::<i64, _>("count")?)
}

async fn count_pending_export_items(pool: &PgPool, job_id: Uuid) -> anyhow::Result<i64> {
    let row = sqlx::query(
        r"
        SELECT COUNT(*)::bigint AS count
        FROM lca_package_export_items
        WHERE job_id = $1
          AND refs_done = FALSE
        ",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get::<i64, _>("count")?)
}

async fn update_package_job_root_count(
    pool: &PgPool,
    job_id: Uuid,
    root_count: i64,
) -> anyhow::Result<()> {
    let _ = sqlx::query(
        r"
        UPDATE lca_package_jobs
        SET root_count = $2,
            updated_at = NOW()
        WHERE id = $1
        ",
    )
    .bind(job_id)
    .bind(root_count.max(0))
    .execute(pool)
    .await?;
    Ok(())
}

async fn fetch_scope_root_refs(
    pool: &PgPool,
    requested_by: Uuid,
    scope: PackageExportScope,
) -> anyhow::Result<Vec<PackageRootRef>> {
    match scope {
        PackageExportScope::CurrentUser => {
            fetch_scope_root_refs_single(pool, requested_by, PackageExportScope::CurrentUser).await
        }
        PackageExportScope::OpenData => {
            fetch_scope_root_refs_single(pool, requested_by, PackageExportScope::OpenData).await
        }
        PackageExportScope::CurrentUserAndOpenData => {
            let current_user =
                fetch_scope_root_refs_single(pool, requested_by, PackageExportScope::CurrentUser)
                    .await?;
            let open_data =
                fetch_scope_root_refs_single(pool, requested_by, PackageExportScope::OpenData)
                    .await?;
            let mut deduped = BTreeMap::<String, PackageRootRef>::new();
            for root in current_user.into_iter().chain(open_data) {
                deduped.insert(table_key(root.table, root.id, &root.version), root);
            }
            Ok(deduped.into_values().collect())
        }
        PackageExportScope::SelectedRoots => Err(anyhow::anyhow!(
            "selected_roots exports must provide explicit roots"
        )),
    }
}

fn open_data_state_codes() -> Vec<i32> {
    (OPEN_DATA_STATE_CODE_START..=OPEN_DATA_STATE_CODE_END).collect()
}

fn is_open_data_state_code(state_code: i32) -> bool {
    (OPEN_DATA_STATE_CODE_START..=OPEN_DATA_STATE_CODE_END).contains(&state_code)
}

async fn fetch_scope_root_refs_single(
    pool: &PgPool,
    requested_by: Uuid,
    scope: PackageExportScope,
) -> anyhow::Result<Vec<PackageRootRef>> {
    let mut output = Vec::new();
    for table in SUPPORTED_PACKAGE_TABLES {
        let rows = match scope {
            PackageExportScope::CurrentUser => {
                sqlx::query(&scope_root_refs_by_user_sql(table))
                    .bind(requested_by)
                    .fetch_all(pool)
                    .await?
            }
            PackageExportScope::OpenData => {
                sqlx::query(&scope_root_refs_by_open_data_sql(table))
                    .bind(open_data_state_codes())
                    .fetch_all(pool)
                    .await?
            }
            PackageExportScope::CurrentUserAndOpenData | PackageExportScope::SelectedRoots => {
                unreachable!("scope is normalized before fetch_scope_root_refs_single")
            }
        };

        for row in rows {
            if let Some(root) = parse_root_ref_row(table, &row)? {
                output.push(root);
            }
        }
    }

    Ok(output)
}

fn scope_root_refs_by_user_sql(table: PackageRootTable) -> String {
    format!(
        r"
        SELECT id::text AS id, version::text AS version
        FROM {}
        WHERE user_id = $1
        ",
        table_name(table)
    )
}

fn scope_root_refs_by_open_data_sql(table: PackageRootTable) -> String {
    format!(
        r"
        SELECT id::text AS id, version::text AS version
        FROM {}
        WHERE state_code = ANY($1::int[])
        ",
        table_name(table)
    )
}

#[allow(clippy::unnecessary_wraps)]
fn parse_root_ref_row(
    table: PackageRootTable,
    row: &sqlx::postgres::PgRow,
) -> anyhow::Result<Option<PackageRootRef>> {
    let id = row
        .try_get::<String, _>("id")
        .ok()
        .and_then(|raw| parse_uuid_opt(raw.as_str()));
    let version = row
        .try_get::<String, _>("version")
        .map(|value| normalize_version_string(value.as_str()))
        .unwrap_or_default();

    let Some(id) = id else {
        return Ok(None);
    };
    if version.is_empty() {
        return Ok(None);
    }

    Ok(Some(PackageRootRef { table, id, version }))
}

async fn fetch_scope_seed_scan_batch_after_cursor(
    pool: &PgPool,
    requested_by: Uuid,
    scope: PackageExportScope,
    table: PackageRootTable,
    after_id: Option<Uuid>,
    after_version: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<PackageSeedScanEntry>> {
    let mut builder = QueryBuilder::<Postgres>::new(scope_seed_scan_select_prefix_sql(table));
    builder.push(" WHERE ");
    match scope {
        PackageExportScope::CurrentUser => {
            builder.push("user_id = ").push_bind(requested_by);
        }
        PackageExportScope::OpenData => {
            builder
                .push("state_code = ANY(")
                .push_bind(open_data_state_codes())
                .push("::int[])");
        }
        PackageExportScope::CurrentUserAndOpenData => {
            builder
                .push("(user_id = ")
                .push_bind(requested_by)
                .push(" OR state_code = ANY(")
                .push_bind(open_data_state_codes())
                .push("::int[]))");
        }
        PackageExportScope::SelectedRoots => {
            return Err(anyhow::anyhow!(
                "selected_roots should not use scope cursor batching"
            ));
        }
    }

    if let Some(after_id) = after_id {
        builder
            .push(" AND (id, version) > (")
            .push_bind(after_id)
            .push(", ")
            .push_bind(after_version.unwrap_or_default())
            .push(")");
    }

    builder
        .push(" ORDER BY id ASC, version ASC LIMIT ")
        .push_bind(limit);
    let rows = builder.build().persistent(false).fetch_all(pool).await?;

    rows.iter()
        .map(|row| parse_seed_scan_entry_row(table, row))
        .collect::<anyhow::Result<Vec<_>>>()
        .map(|entries| entries.into_iter().flatten().collect())
}

async fn insert_export_items(
    pool: &PgPool,
    job_id: Uuid,
    roots: &[PackageRootRef],
    is_seed: bool,
    refs_done: bool,
) -> anyhow::Result<()> {
    if roots.is_empty() {
        return Ok(());
    }

    let mut deduped = BTreeMap::<String, PackageRootRef>::new();
    for root in roots {
        deduped.insert(
            table_key(root.table, root.id, &root.version),
            PackageRootRef {
                table: root.table,
                id: root.id,
                version: normalize_version_string(&root.version),
            },
        );
    }

    let items = deduped.into_values().collect::<Vec<_>>();
    for chunk in items.chunks(EXPORT_ITEM_INSERT_CHUNK_SIZE) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO lca_package_export_items (job_id, table_name, dataset_id, version, is_seed, refs_done) ",
        );
        builder.push_values(chunk, |mut row, item| {
            row.push_bind(job_id)
                .push_bind(table_name(item.table))
                .push_bind(item.id)
                .push_bind(normalize_version_string(&item.version))
                .push_bind(is_seed)
                .push_bind(refs_done);
        });
        builder.push(
            " ON CONFLICT (job_id, table_name, dataset_id, version) DO UPDATE \
              SET is_seed = lca_package_export_items.is_seed OR EXCLUDED.is_seed, \
                  refs_done = lca_package_export_items.refs_done OR EXCLUDED.refs_done, \
                  updated_at = NOW()",
        );
        builder.build().persistent(false).execute(pool).await?;
    }

    Ok(())
}

async fn fetch_pending_export_items(
    pool: &PgPool,
    job_id: Uuid,
    limit: i64,
) -> anyhow::Result<Vec<PackageExportItem>> {
    let rows = sqlx::query(
        r"
        SELECT table_name, dataset_id::text AS dataset_id, version
        FROM lca_package_export_items
        WHERE job_id = $1
          AND refs_done = FALSE
        ORDER BY created_at ASC, table_name ASC, dataset_id ASC, version ASC
        LIMIT $2
        ",
    )
    .bind(job_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    rows.iter()
        .map(parse_export_item_row)
        .collect::<anyhow::Result<Vec<_>>>()
}

fn parse_export_item_row(row: &sqlx::postgres::PgRow) -> anyhow::Result<PackageExportItem> {
    let table_name_raw = row.try_get::<String, _>("table_name")?;
    let id = row
        .try_get::<String, _>("dataset_id")?
        .parse::<Uuid>()
        .map_err(|err| anyhow::anyhow!("invalid export item id: {err}"))?;
    Ok(PackageExportItem {
        table: parse_table_name(table_name_raw.as_str())
            .ok_or_else(|| anyhow::anyhow!("unsupported export item table: {table_name_raw}"))?,
        id,
        version: normalize_version_string(&row.try_get::<String, _>("version")?),
    })
}

async fn process_export_item_batch(
    pool: &PgPool,
    scope: PackageExportScope,
    job_id: Uuid,
    items: &[PackageExportItem],
    cache: &mut ExportTraversalCache,
) -> anyhow::Result<ExportBatchResult> {
    let roots = items
        .iter()
        .map(|item| PackageRootRef {
            table: item.table,
            id: item.id,
            version: item.version.clone(),
        })
        .collect::<Vec<_>>();
    let rows = fetch_reference_scan_rows_by_exact_roots(pool, roots.as_slice()).await?;
    let selected_roots_export = matches!(scope, PackageExportScope::SelectedRoots);
    let mut refs = Vec::new();
    let mut discovered = BTreeMap::<String, PackageRootRef>::new();

    for current in &rows {
        refs.extend(extract_ref_targets(&current.ref_candidates));

        if selected_roots_export && current.table == PackageRootTable::Lifecyclemodels {
            let related_processes =
                fetch_model_process_roots(pool, current.id, &current.version).await?;
            for root in related_processes {
                push_discovered_root(&mut discovered, cache, root);
            }
            refs.extend(extract_model_submodels_from_value(
                &current.version,
                &current.submodels,
            ));
        }

        if selected_roots_export
            && current.table == PackageRootTable::Processes
            && let Some(model_id) = current.model_id
        {
            let related_models =
                fetch_process_model_roots(pool, model_id, &current.version).await?;
            for root in related_models {
                push_discovered_root(&mut discovered, cache, root);
            }
        }
    }

    resolve_discovered_roots_from_refs(
        pool,
        refs.as_slice(),
        cache,
        &mut discovered,
        !selected_roots_export,
    )
    .await?;

    let discovered_roots = discovered.into_values().collect::<Vec<_>>();
    insert_export_items(pool, job_id, discovered_roots.as_slice(), false, false).await?;
    mark_export_items_refs_done(pool, job_id, items).await?;

    Ok(ExportBatchResult {
        processed_count: items.len(),
        discovered_count: discovered_roots.len(),
    })
}

async fn resolve_discovered_roots_from_refs(
    pool: &PgPool,
    refs: &[ReferenceTarget],
    cache: &mut ExportTraversalCache,
    discovered: &mut BTreeMap<String, PackageRootRef>,
    skip_covered_versionless_refs: bool,
) -> anyhow::Result<()> {
    let plan = plan_reference_resolution(
        refs,
        &cache.known_exact,
        &cache.known_any_version,
        &cache.resolved_latest,
        skip_covered_versionless_refs,
    );
    for root in plan.cached_roots {
        push_discovered_root(discovered, cache, root);
    }

    if !plan.exact_roots_to_fetch.is_empty() {
        let exact_roots =
            fetch_root_refs_by_exact_roots(pool, plan.exact_roots_to_fetch.as_slice()).await?;
        for root in exact_roots {
            push_discovered_root(discovered, cache, root);
        }
    }

    if !plan.latest_refs_to_fetch.is_empty() {
        let resolved_latest =
            fetch_latest_reference_roots(pool, plan.latest_refs_to_fetch.as_slice()).await?;
        for reference in plan.latest_refs_to_fetch {
            let cache_key = (reference.table, reference.id);
            if let Some(root) = resolved_latest.get(&cache_key) {
                cache
                    .resolved_latest
                    .insert(cache_key, Some(root.version.clone()));
                push_discovered_root(discovered, cache, root.clone());
            } else {
                cache.resolved_latest.insert(cache_key, None);
            }
        }
    }

    Ok(())
}

async fn resolve_process_model_roots_from_seed_entries(
    pool: &PgPool,
    entries: &[PackageSeedScanEntry],
    cache: &mut ExportTraversalCache,
    discovered: &mut BTreeMap<String, PackageRootRef>,
) -> anyhow::Result<()> {
    let requested = entries
        .iter()
        .filter_map(|entry| {
            (entry.table == PackageRootTable::Processes)
                .then_some(
                    entry
                        .model_id
                        .map(|model_id| (model_id, entry.version.clone())),
                )
                .flatten()
        })
        .collect::<Vec<_>>();
    if requested.is_empty() {
        return Ok(());
    }

    let ids = requested.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let rows = sqlx::query(&select_root_refs_by_ids_sql(
        PackageRootTable::Lifecyclemodels,
    ))
    .bind(ids)
    .fetch_all(pool)
    .await?;
    let parsed = rows
        .iter()
        .map(|row| parse_root_ref_row(PackageRootTable::Lifecyclemodels, row))
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    for root in
        resolve_exact_or_latest_roots(PackageRootTable::Lifecyclemodels, &requested, &parsed)
    {
        push_discovered_root(discovered, cache, root);
    }

    Ok(())
}

async fn process_seed_scan_pass(
    pool: &PgPool,
    requested_by: Uuid,
    scope: PackageExportScope,
    job_id: Uuid,
    cache: &mut ExportTraversalCache,
    mut state: ExportSeedScanState,
    pass_started: Instant,
) -> anyhow::Result<ExportSeedScanResult> {
    let mut pass_scanned_count = 0usize;
    let mut pass_discovered_count = 0usize;

    while !state.complete && pass_started.elapsed() < EXPORT_PASS_TIME_BUDGET {
        let Some(table) = SUPPORTED_PACKAGE_TABLES.get(state.table_index).copied() else {
            state.complete = true;
            break;
        };

        let entries = fetch_scope_seed_scan_batch_after_cursor(
            pool,
            requested_by,
            scope,
            table,
            state.last_id,
            state.last_version.as_deref(),
            EXPORT_SEED_SCAN_BATCH_SIZE,
        )
        .await?;

        if entries.is_empty() {
            state.table_index += 1;
            state.last_id = None;
            state.last_version = None;
            continue;
        }

        let mut discovered = BTreeMap::<String, PackageRootRef>::new();
        let mut refs = Vec::new();
        for entry in &entries {
            refs.extend(extract_ref_targets(&entry.ref_candidates));
            refs.extend(extract_model_submodels_from_value(
                &entry.version,
                &entry.submodels,
            ));
        }
        resolve_process_model_roots_from_seed_entries(
            pool,
            entries.as_slice(),
            cache,
            &mut discovered,
        )
        .await?;
        resolve_discovered_roots_from_refs(pool, refs.as_slice(), cache, &mut discovered, true)
            .await?;

        let discovered_roots = discovered.into_values().collect::<Vec<_>>();
        insert_export_items(pool, job_id, discovered_roots.as_slice(), false, false).await?;

        let scanned = entries.len();
        let discovered_count = discovered_roots.len();
        pass_scanned_count += scanned;
        pass_discovered_count += discovered_count;
        state.scanned_seed_count += scanned;
        state.discovered_external_count += discovered_count;

        if let Some(last) = entries.last() {
            state.last_id = Some(last.id);
            state.last_version = Some(last.version.clone());
        }

        if entries.len() < usize::try_from(EXPORT_SEED_SCAN_BATCH_SIZE).unwrap_or(usize::MAX) {
            state.table_index += 1;
            state.last_id = None;
            state.last_version = None;
        }
    }

    if state.table_index >= SUPPORTED_PACKAGE_TABLES.len() {
        state.complete = true;
    }

    Ok(ExportSeedScanResult {
        state,
        pass_scanned_count,
        pass_discovered_count,
    })
}

fn push_discovered_root(
    discovered: &mut BTreeMap<String, PackageRootRef>,
    cache: &mut ExportTraversalCache,
    root: PackageRootRef,
) {
    let key = table_key(root.table, root.id, &root.version);
    if cache.known_exact.insert(key.clone()) {
        cache.known_any_version.insert((root.table, root.id));
        discovered.insert(key, root);
    }
}

fn remember_root_in_traversal_cache(cache: &mut ExportTraversalCache, root: &PackageRootRef) {
    cache
        .known_exact
        .insert(table_key(root.table, root.id, &root.version));
    cache.known_any_version.insert((root.table, root.id));
}

fn plan_reference_resolution(
    refs: &[ReferenceTarget],
    known_exact: &HashSet<String>,
    known_any_version: &HashSet<(PackageRootTable, Uuid)>,
    resolved_latest: &HashMap<(PackageRootTable, Uuid), Option<String>>,
    skip_covered_versionless_refs: bool,
) -> PlannedReferenceResolution {
    let mut cached_roots = BTreeMap::<String, PackageRootRef>::new();
    let mut exact_roots_to_fetch = BTreeMap::<String, PackageRootRef>::new();
    let mut latest_refs_to_fetch = BTreeMap::<(PackageRootTable, Uuid), ReferenceTarget>::new();

    for reference in refs {
        if let Some(version) = &reference.version {
            let normalized_version = normalize_version_string(version);
            let root = PackageRootRef {
                table: reference.table,
                id: reference.id,
                version: normalized_version.clone(),
            };
            let key = table_key(root.table, root.id, &root.version);
            if !known_exact.contains(&key) {
                exact_roots_to_fetch.insert(key, root);
            }
        } else {
            let cache_key = (reference.table, reference.id);
            if skip_covered_versionless_refs && known_any_version.contains(&cache_key) {
                continue;
            }
            if let Some(cached_version) = resolved_latest.get(&cache_key) {
                if let Some(version) = cached_version {
                    let root = PackageRootRef {
                        table: reference.table,
                        id: reference.id,
                        version: version.clone(),
                    };
                    let key = table_key(root.table, root.id, &root.version);
                    if !known_exact.contains(&key) {
                        cached_roots.insert(key, root);
                    }
                }
                continue;
            }

            latest_refs_to_fetch
                .entry(cache_key)
                .or_insert_with(|| reference.clone());
        }
    }

    PlannedReferenceResolution {
        cached_roots: cached_roots.into_values().collect(),
        exact_roots_to_fetch: exact_roots_to_fetch.into_values().collect(),
        latest_refs_to_fetch: latest_refs_to_fetch.into_values().collect(),
    }
}

async fn fetch_latest_reference_roots(
    pool: &PgPool,
    refs: &[ReferenceTarget],
) -> anyhow::Result<HashMap<(PackageRootTable, Uuid), PackageRootRef>> {
    if refs.is_empty() {
        return Ok(HashMap::new());
    }

    let mut grouped = BTreeMap::<PackageRootTable, Vec<ReferenceTarget>>::new();
    for reference in refs {
        grouped
            .entry(reference.table)
            .or_default()
            .push(reference.clone());
    }

    let mut output = HashMap::<(PackageRootTable, Uuid), PackageRootRef>::new();
    for (table, table_refs) in grouped {
        let ids = table_refs
            .iter()
            .map(|reference| reference.id)
            .collect::<Vec<_>>();
        let query_sql = select_root_refs_by_ids_sql(table);
        let rows = sqlx::query(query_sql.as_str())
            .bind(ids)
            .fetch_all(pool)
            .await?;
        let parsed_rows = rows
            .iter()
            .map(|row| parse_root_ref_row(table, row))
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        for root in resolve_referenced_roots_from_rows(table, &table_refs, &parsed_rows) {
            output.insert((root.table, root.id), root);
        }
    }

    Ok(output)
}

#[allow(dead_code)]
async fn filter_missing_reference_targets(
    pool: &PgPool,
    job_id: Uuid,
    refs: &[ReferenceTarget],
) -> anyhow::Result<Vec<ReferenceTarget>> {
    if refs.is_empty() {
        return Ok(Vec::new());
    }

    let mut deduped = Vec::<ReferenceTarget>::new();
    let mut seen = HashSet::<String>::new();
    for reference in refs {
        let key = match &reference.version {
            Some(version) => table_key(reference.table, reference.id, version),
            None => format!("{}:{}:latest", table_name(reference.table), reference.id),
        };
        if seen.insert(key) {
            deduped.push(reference.clone());
        }
    }

    let mut existing_exact = HashSet::<String>::new();
    let grouped = deduped
        .iter()
        .filter(|reference| reference.version.is_some())
        .fold(
            BTreeMap::<PackageRootTable, Vec<Uuid>>::new(),
            |mut acc, reference| {
                acc.entry(reference.table).or_default().push(reference.id);
                acc
            },
        );

    for (table, ids) in grouped {
        let rows = sqlx::query(
            r"
            SELECT dataset_id::text AS dataset_id, version
            FROM lca_package_export_items
            WHERE job_id = $1
              AND table_name = $2
              AND dataset_id = ANY($3::uuid[])
            ",
        )
        .bind(job_id)
        .bind(table_name(table))
        .bind(ids)
        .fetch_all(pool)
        .await?;

        for row in rows {
            let id = row
                .try_get::<String, _>("dataset_id")
                .ok()
                .and_then(|raw| parse_uuid_opt(raw.as_str()));
            let version = row
                .try_get::<String, _>("version")
                .map(|value| normalize_version_string(value.as_str()))
                .unwrap_or_default();
            if let Some(id) = id {
                existing_exact.insert(table_key(table, id, &version));
            }
        }
    }

    Ok(deduped
        .into_iter()
        .filter(|reference| match &reference.version {
            Some(version) => {
                !existing_exact.contains(&table_key(reference.table, reference.id, version))
            }
            None => true,
        })
        .collect())
}

async fn mark_export_items_refs_done(
    pool: &PgPool,
    job_id: Uuid,
    items: &[PackageExportItem],
) -> anyhow::Result<()> {
    for item in items {
        let _ = sqlx::query(
            r"
            UPDATE lca_package_export_items
            SET refs_done = TRUE,
                updated_at = NOW()
            WHERE job_id = $1
              AND table_name = $2
              AND dataset_id = $3
              AND version = $4
            ",
        )
        .bind(job_id)
        .bind(table_name(item.table))
        .bind(item.id)
        .bind(normalize_version_string(&item.version))
        .execute(pool)
        .await?;
    }

    Ok(())
}

async fn list_export_seed_roots(
    pool: &PgPool,
    job_id: Uuid,
) -> anyhow::Result<Vec<PackageRootRef>> {
    let rows = sqlx::query(
        r"
        SELECT table_name, dataset_id::text AS dataset_id, version
        FROM lca_package_export_items
        WHERE job_id = $1
          AND is_seed = TRUE
        ORDER BY table_name ASC, dataset_id ASC, version ASC
        ",
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;

    rows.iter()
        .map(|row| {
            let item = parse_export_item_row(row)?;
            Ok(PackageRootRef {
                table: item.table,
                id: item.id,
                version: item.version,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()
}

async fn list_seed_export_items_for_cache(
    pool: &PgPool,
    job_id: Uuid,
) -> anyhow::Result<Vec<PackageRootRef>> {
    let rows = sqlx::query(
        r"
        SELECT table_name, dataset_id::text AS dataset_id, version
        FROM lca_package_export_items
        WHERE job_id = $1
          AND is_seed = TRUE
        ",
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;

    rows.iter()
        .map(|row| {
            let item = parse_export_item_row(row)?;
            Ok(PackageRootRef {
                table: item.table,
                id: item.id,
                version: item.version,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()
}

async fn list_export_items(pool: &PgPool, job_id: Uuid) -> anyhow::Result<Vec<PackageExportItem>> {
    let rows = sqlx::query(
        r"
        SELECT table_name, dataset_id::text AS dataset_id, version
        FROM lca_package_export_items
        WHERE job_id = $1
        ORDER BY table_name ASC, dataset_id ASC, version ASC
        ",
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;

    rows.iter()
        .map(parse_export_item_row)
        .collect::<anyhow::Result<Vec<_>>>()
}

async fn list_non_seed_export_items(
    pool: &PgPool,
    job_id: Uuid,
) -> anyhow::Result<Vec<PackageExportItem>> {
    let rows = sqlx::query(
        r"
        SELECT table_name, dataset_id::text AS dataset_id, version
        FROM lca_package_export_items
        WHERE job_id = $1
          AND is_seed = FALSE
        ",
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;

    rows.iter()
        .map(parse_export_item_row)
        .collect::<anyhow::Result<Vec<_>>>()
}

async fn fetch_export_entries_by_items(
    state: &AppState,
    job_id: Uuid,
    scope: PackageExportScope,
    root_count: usize,
    items: &[PackageExportItem],
) -> anyhow::Result<Vec<PackageEntry>> {
    let mut entries = Vec::new();
    for chunk in items.chunks(EXPORT_FINALIZE_FETCH_BATCH_SIZE) {
        let roots = chunk
            .iter()
            .map(|item| PackageRootRef {
                table: item.table,
                id: item.id,
                version: item.version.clone(),
            })
            .collect::<Vec<_>>();
        let mut fetched = fetch_rows_by_exact_roots(&state.pool, roots.as_slice()).await?;
        entries.append(&mut fetched);
    }

    let total_items = i64::try_from(items.len()).unwrap_or(i64::MAX);
    let diagnostics = export_progress_diagnostics(
        "finalize_zip",
        scope,
        root_count,
        total_items,
        total_items,
        0,
        json!({
            "message": "Materializing ZIP package",
            "fetched_entry_count": entries.len(),
        }),
    );
    let _ = sqlx::query(
        r"
        UPDATE lca_package_jobs
        SET status = 'running',
            diagnostics = $2::jsonb,
            updated_at = NOW()
        WHERE id = $1
        ",
    )
    .bind(job_id)
    .bind(diagnostics)
    .execute(&state.pool)
    .await?;

    Ok(sort_entries(entries))
}

fn export_progress_diagnostics(
    stage: &str,
    scope: PackageExportScope,
    root_count: usize,
    total_items: i64,
    processed_items: i64,
    pending_items: i64,
    extra: Value,
) -> Value {
    let mut value = json!({
        "phase": "export_package",
        "stage": stage,
        "scope": scope,
        "root_count": root_count,
        "total_items": total_items.max(0),
        "processed_items": processed_items.max(0),
        "pending_items": pending_items.max(0),
        "progress": {
            "processed_items": processed_items.max(0),
            "total_items": total_items.max(0),
            "pending_items": pending_items.max(0),
        },
    });

    if let (Value::Object(root), Value::Object(extra_map)) = (&mut value, extra) {
        root.extend(extra_map);
    }

    value
}

fn validator_command_candidates(input_dir: &Path) -> Vec<(String, Vec<String>)> {
    let mut candidates = Vec::new();
    let base_args = vec![
        "--input-dir".to_owned(),
        input_dir.display().to_string(),
        "--format".to_owned(),
        "json".to_owned(),
    ];

    if let Ok(custom) = std::env::var("TIDAS_VALIDATE_BIN")
        && !custom.trim().is_empty()
    {
        candidates.push((custom, base_args.clone()));
    }

    let mut python3_args = vec!["-m".to_owned(), "tidas_tools.validate".to_owned()];
    python3_args.extend(base_args.clone());
    candidates.push(("python3".to_owned(), python3_args));

    let mut python_args = vec!["-m".to_owned(), "tidas_tools.validate".to_owned()];
    python_args.extend(base_args.clone());
    candidates.push(("python".to_owned(), python_args));

    candidates.push(("tidas-validate".to_owned(), base_args));

    candidates
}

fn extract_package_zip_to_tempdir(zip_bytes: &[u8]) -> anyhow::Result<TempDir> {
    let tempdir = tempfile::tempdir().context("create temp dir for package validation")?;
    let cursor = Cursor::new(zip_bytes.to_vec());
    let mut archive = ZipArchive::new(cursor).context("open package ZIP for validation")?;

    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).context("read package ZIP entry")?;
        let relative_path = entry
            .enclosed_name()
            .ok_or_else(|| anyhow::anyhow!("package ZIP contains unsafe entry path"))?;
        let output_path = tempdir.path().join(relative_path);

        if entry.is_dir() {
            fs::create_dir_all(&output_path)
                .with_context(|| format!("create extracted dir {}", output_path.display()))?;
            continue;
        }

        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create extracted dir {}", parent.display()))?;
        }

        let mut output_file = File::create(&output_path)
            .with_context(|| format!("create extracted file {}", output_path.display()))?;
        copy(&mut entry, &mut output_file)
            .with_context(|| format!("write extracted file {}", output_path.display()))?;
    }

    Ok(tempdir)
}

fn normalize_validation_issue_paths(report: &mut TidasValidationReport, root: &Path) {
    for issue in &mut report.issues {
        let path = Path::new(&issue.file_path);
        if let Ok(relative) = path.strip_prefix(root) {
            issue.file_path = relative.display().to_string();
        }
    }
}

fn parse_tidas_validation_report(raw_output: &str) -> anyhow::Result<TidasValidationReport> {
    let mut report: TidasValidationReport =
        serde_json::from_str(raw_output).context("parse TIDAS validator JSON report")?;

    if report.summary.issue_count == 0 && !report.issues.is_empty() {
        report.summary.issue_count = report.issues.len();
    }
    if report.summary.error_count == 0 {
        report.summary.error_count = report
            .issues
            .iter()
            .filter(|issue| issue.severity.eq_ignore_ascii_case("error"))
            .count();
    }
    if report.summary.warning_count == 0 {
        report.summary.warning_count = report
            .issues
            .iter()
            .filter(|issue| issue.severity.eq_ignore_ascii_case("warning"))
            .count();
    }
    if report.summary.issue_count > 0 {
        report.ok = report.summary.error_count == 0;
    }

    Ok(report)
}

fn run_tidas_validation(input_dir: &Path) -> anyhow::Result<TidasValidationReport> {
    let mut last_not_found = false;

    for (program, args) in validator_command_candidates(input_dir) {
        let output = match Command::new(&program).args(&args).output() {
            Ok(output) => output,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                last_not_found = true;
                continue;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("execute TIDAS validator via {program}"));
            }
        };

        let stdout =
            String::from_utf8(output.stdout).context("decode validator stdout as UTF-8")?;
        let stderr =
            String::from_utf8(output.stderr).context("decode validator stderr as UTF-8")?;
        if stdout.trim().is_empty() {
            continue;
        }

        let mut report = parse_tidas_validation_report(stdout.as_str()).with_context(|| {
            format!("parse TIDAS validator JSON output from {program} (stderr: {stderr})")
        })?;
        normalize_validation_issue_paths(&mut report, input_dir);

        let has_validation_issues = report.summary.issue_count > 0 || !report.issues.is_empty();
        if output.status.success() || has_validation_issues {
            return Ok(report);
        }

        return Err(anyhow::anyhow!(
            "TIDAS validator {program} exited unsuccessfully without a validation report: {stderr}"
        ));
    }

    if last_not_found {
        return Err(anyhow::anyhow!(
            "TIDAS validator command not found; set TIDAS_VALIDATE_BIN or install tidas-tools with python3/tidas-validate available"
        ));
    }

    Err(anyhow::anyhow!("failed to execute TIDAS validator"))
}

fn report_from_validation_failure(
    total_entries: usize,
    validation_report: &TidasValidationReport,
) -> ImportReportDocument {
    ImportReportDocument {
        ok: false,
        code: "VALIDATION_FAILED",
        message: "TIDAS package validation failed",
        summary: ImportReportSummary {
            total_entries,
            filtered_open_data_count: 0,
            user_conflict_count: 0,
            importable_count: 0,
            imported_count: 0,
            validation_issue_count: validation_report.summary.issue_count,
            error_count: validation_report.summary.error_count,
            warning_count: validation_report.summary.warning_count,
        },
        filtered_open_data: Vec::new(),
        user_conflicts: Vec::new(),
        validation_issues: validation_report.issues.clone(),
    }
}

fn preflight_import_validation(
    package_entries: &[PackageEntry],
    validation_report: &TidasValidationReport,
) -> anyhow::Result<Option<ImportReportDocument>> {
    if validation_report.summary.error_count > 0 {
        return Ok(Some(report_from_validation_failure(
            package_entries.len(),
            validation_report,
        )));
    }

    if package_entries.is_empty() {
        return Err(anyhow::anyhow!(
            "the package does not contain any supported TIDAS datasets"
        ));
    }

    Ok(None)
}

#[allow(clippy::too_many_lines)]
pub async fn execute_import_package(
    state: &AppState,
    job_id: Uuid,
    requested_by: Uuid,
    source_artifact_id: Uuid,
) -> anyhow::Result<PackageExecutionOutcome> {
    let source_artifact = fetch_package_artifact(&state.pool, source_artifact_id).await?;
    if source_artifact.artifact_kind != PackageArtifactKind::ImportSource {
        return Err(anyhow::anyhow!(
            "artifact {source_artifact_id} is not an import source"
        ));
    }
    if source_artifact.status != "ready" {
        return Err(anyhow::anyhow!(
            "artifact {source_artifact_id} is not ready for import"
        ));
    }
    if source_artifact.artifact_format != PACKAGE_ZIP_ARTIFACT_FORMAT {
        return Err(anyhow::anyhow!(
            "artifact {source_artifact_id} has unsupported format {}",
            source_artifact.artifact_format
        ));
    }

    let zip_bytes = state
        .object_store
        .download_object_url(&source_artifact.artifact_url)
        .await?;
    let extracted_dir = extract_package_zip_to_tempdir(zip_bytes.as_slice())?;
    let validation_report = run_tidas_validation(extracted_dir.path())?;
    let package_entries = parse_package_entries(zip_bytes.as_slice())?;
    if let Some(report_document) =
        preflight_import_validation(&package_entries, &validation_report)?
    {
        let report_artifact = encode_import_report_artifact(job_id, &report_document)?;
        let report_url = state
            .object_store
            .upload_package_artifact(
                job_id,
                IMPORT_REPORT_SUFFIX,
                report_artifact.extension,
                report_artifact.content_type,
                report_artifact.bytes.clone(),
            )
            .await?;
        let report_artifact_id = insert_package_artifact(
            &state.pool,
            PackageArtifactInsert::ready_from_encoded(
                job_id,
                PackageArtifactKind::ImportReport,
                report_url.clone(),
                &report_artifact,
                json!({
                    "code": report_document.code,
                    "total_entries": package_entries.len(),
                    "validation_issue_count": report_document.summary.validation_issue_count,
                    "error_count": report_document.summary.error_count,
                    "warning_count": report_document.summary.warning_count,
                    "filtered_open_data_count": 0,
                    "user_conflict_count": 0,
                    "imported_count": 0,
                }),
            )?,
        )
        .await?;

        return Ok(PackageExecutionOutcome {
            final_status: "completed",
            diagnostics: json!({
                "phase": "import_package",
                "result": report_document.code,
                "total_entries": package_entries.len(),
                "validation_issue_count": report_document.summary.validation_issue_count,
                "error_count": report_document.summary.error_count,
                "warning_count": report_document.summary.warning_count,
                "source_artifact_id": source_artifact_id,
                "report_artifact_id": report_artifact_id,
                "report_artifact_url": report_url,
            }),
            export_artifact_id: None,
            report_artifact_id: Some(report_artifact_id),
        });
    }

    let conflicts = find_conflicts(&state.pool, &package_entries).await?;
    let open_conflict_keys = conflicts
        .open_data_conflicts
        .iter()
        .map(|record| table_key(record.table, record.id, &record.version))
        .collect::<HashSet<_>>();
    let insertable_entries = package_entries
        .iter()
        .filter(|entry| {
            !open_conflict_keys.contains(&table_key(entry.table, entry.id, &entry.version))
        })
        .cloned()
        .collect::<Vec<_>>();

    let (code, message, importable_count, imported_count) = if conflicts.user_conflicts.is_empty() {
        if !insertable_entries.is_empty() {
            insert_entries(&state.pool, requested_by, &insertable_entries).await?;
        }
        (
            "IMPORTED",
            "TIDAS package imported successfully",
            insertable_entries.len(),
            insertable_entries.len(),
        )
    } else {
        (
            "USER_DATA_CONFLICT",
            "Conflicts with existing user datasets, import rejected",
            0,
            0,
        )
    };

    let report_document = ImportReportDocument {
        ok: conflicts.user_conflicts.is_empty(),
        code,
        message,
        summary: ImportReportSummary {
            total_entries: package_entries.len(),
            filtered_open_data_count: conflicts.open_data_conflicts.len(),
            user_conflict_count: conflicts.user_conflicts.len(),
            importable_count,
            imported_count,
            validation_issue_count: validation_report.summary.issue_count,
            error_count: validation_report.summary.error_count,
            warning_count: validation_report.summary.warning_count,
        },
        filtered_open_data: conflicts.open_data_conflicts.clone(),
        user_conflicts: conflicts.user_conflicts.clone(),
        validation_issues: validation_report.issues.clone(),
    };
    let report_artifact = encode_import_report_artifact(job_id, &report_document)?;
    let report_url = state
        .object_store
        .upload_package_artifact(
            job_id,
            IMPORT_REPORT_SUFFIX,
            report_artifact.extension,
            report_artifact.content_type,
            report_artifact.bytes.clone(),
        )
        .await?;
    let report_artifact_id = insert_package_artifact(
        &state.pool,
        PackageArtifactInsert::ready_from_encoded(
            job_id,
            PackageArtifactKind::ImportReport,
            report_url.clone(),
            &report_artifact,
            json!({
                "code": code,
                "total_entries": package_entries.len(),
                "filtered_open_data_count": conflicts.open_data_conflicts.len(),
                "user_conflict_count": conflicts.user_conflicts.len(),
                "imported_count": imported_count,
                "validation_issue_count": validation_report.summary.issue_count,
                "error_count": validation_report.summary.error_count,
                "warning_count": validation_report.summary.warning_count,
            }),
        )?,
    )
    .await?;

    Ok(PackageExecutionOutcome {
        final_status: "completed",
        diagnostics: json!({
            "phase": "import_package",
            "result": code,
            "total_entries": package_entries.len(),
            "filtered_open_data_count": conflicts.open_data_conflicts.len(),
            "user_conflict_count": conflicts.user_conflicts.len(),
            "imported_count": imported_count,
            "validation_issue_count": validation_report.summary.issue_count,
            "error_count": validation_report.summary.error_count,
            "warning_count": validation_report.summary.warning_count,
            "source_artifact_id": source_artifact_id,
            "report_artifact_id": report_artifact_id,
            "report_artifact_url": report_url,
        }),
        export_artifact_id: None,
        report_artifact_id: Some(report_artifact_id),
    })
}

#[allow(dead_code)]
#[derive(Debug)]
struct CollectedPackageEntries {
    entries: Vec<PackageEntry>,
    roots: Vec<PackageRootRef>,
    scope: PackageExportScope,
}

#[allow(dead_code)]
async fn collect_package_entries(
    state: &AppState,
    requested_by: Uuid,
    scope: PackageExportScope,
    roots: &[PackageRootRef],
) -> anyhow::Result<CollectedPackageEntries> {
    let seed_entries = if roots.is_empty() {
        fetch_scope_roots(&state.pool, requested_by, scope).await?
    } else {
        fetch_rows_by_exact_roots(&state.pool, roots).await?
    };

    if !roots.is_empty() {
        let fetched_keys = seed_entries
            .iter()
            .map(|entry| table_key(entry.table, entry.id, &entry.version))
            .collect::<HashSet<_>>();
        let missing_roots = roots
            .iter()
            .filter(|root| !fetched_keys.contains(&table_key(root.table, root.id, &root.version)))
            .count();
        if missing_roots > 0 {
            return Err(anyhow::anyhow!(
                "some selected datasets were not found or are not exportable"
            ));
        }
    }

    let mut collected = BTreeMap::<String, PackageEntry>::new();
    let mut queue = VecDeque::<PackageEntry>::new();

    for entry in seed_entries {
        enqueue_package_entry(&mut collected, &mut queue, entry);
    }

    while let Some(current) = queue.pop_front() {
        let mut refs = extract_ref_targets(&current.json_ordered);

        if current.table == PackageRootTable::Lifecyclemodels {
            let related_processes =
                fetch_model_processes(&state.pool, current.id, &current.version).await?;
            for entry in related_processes {
                enqueue_package_entry(&mut collected, &mut queue, entry);
            }
            refs.extend(extract_model_submodels(&current));
        }

        if current.table == PackageRootTable::Processes
            && let Some(model_id) = current.model_id
        {
            let related_models =
                fetch_process_model(&state.pool, model_id, &current.version).await?;
            for entry in related_models {
                enqueue_package_entry(&mut collected, &mut queue, entry);
            }
        }

        if refs.is_empty() {
            continue;
        }

        let referenced_entries = fetch_referenced_entries(&state.pool, &refs).await?;
        for entry in referenced_entries {
            enqueue_package_entry(&mut collected, &mut queue, entry);
        }
    }

    let resolved_roots = if roots.is_empty() {
        sort_entries(collected.values().cloned().collect())
            .into_iter()
            .map(|entry| PackageRootRef {
                table: entry.table,
                id: entry.id,
                version: entry.version,
            })
            .collect()
    } else {
        roots.to_vec()
    };

    Ok(CollectedPackageEntries {
        entries: sort_entries(collected.into_values().collect()),
        roots: resolved_roots,
        scope,
    })
}

#[allow(dead_code)]
fn enqueue_package_entry(
    collected: &mut BTreeMap<String, PackageEntry>,
    queue: &mut VecDeque<PackageEntry>,
    entry: PackageEntry,
) {
    let key = table_key(entry.table, entry.id, &entry.version);
    if collected.contains_key(&key) {
        return;
    }
    collected.insert(key, entry.clone());
    queue.push_back(entry);
}

fn build_manifest(
    scope: PackageExportScope,
    roots: &[PackageRootRef],
    entries: &[PackageEntry],
) -> PackageManifest {
    let counts = SUPPORTED_PACKAGE_TABLES
        .iter()
        .map(|table| {
            (
                table_name(*table).to_owned(),
                entries.iter().filter(|entry| entry.table == *table).count(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let manifest_entries = entries
        .iter()
        .map(|entry| PackageManifestEntry {
            table: entry.table,
            id: entry.id,
            version: entry.version.clone(),
            file_path: build_entry_file_path(entry.table, entry.id, &entry.version),
            rule_verification: entry.rule_verification,
            model_id: entry.model_id,
        })
        .collect::<Vec<_>>();

    PackageManifest {
        format: PACKAGE_MANIFEST_FORMAT.to_owned(),
        version: PACKAGE_MANIFEST_VERSION,
        exported_at: Utc::now().to_rfc3339(),
        scope,
        roots: roots.to_vec(),
        entries: manifest_entries,
        counts,
        total_count: entries.len(),
    }
}

fn build_package_zip(
    manifest: &PackageManifest,
    entries: &[PackageEntry],
) -> anyhow::Result<NamedTempFile> {
    let temp = Builder::new()
        .prefix("tidas-export-")
        .suffix(".zip")
        .tempfile()?;
    let file = temp.reopen()?;
    let mut writer = ZipWriter::new(file);
    let options = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .compression_level(Some(PACKAGE_ZIP_COMPRESSION_LEVEL));

    writer.start_file("manifest.json", options)?;
    writer.write_all(serde_json::to_string_pretty(manifest)?.as_bytes())?;

    for entry in entries {
        writer.start_file(
            build_entry_file_path(entry.table, entry.id, &entry.version),
            options,
        )?;
        writer
            .write_all(serde_json::to_string_pretty(&serialize_entry_dataset(entry))?.as_bytes())?;
    }

    let file = writer.finish()?;
    file.sync_all()?;
    Ok(temp)
}

fn parse_package_entries(zip_bytes: &[u8]) -> anyhow::Result<Vec<PackageEntry>> {
    let cursor = Cursor::new(zip_bytes.to_vec());
    let mut archive = ZipArchive::new(cursor)?;

    let manifest_content = read_zip_file_string(&mut archive, "manifest.json")?;
    if let Some(content) = manifest_content
        && let Ok(manifest) = serde_json::from_str::<PackageManifest>(content.as_str())
    {
        if !manifest.entries.is_empty() {
            let entries = parse_manifest_package_entries(&mut archive, &manifest)?;
            if !entries.is_empty() {
                return Ok(backfill_process_model_ids(entries));
            }
        }

        let inferred_entries = parse_folder_package_entries(&mut archive, Some(&manifest))?;
        if !inferred_entries.is_empty() {
            return Ok(backfill_process_model_ids(inferred_entries));
        }
    }

    let inferred_entries = parse_folder_package_entries(&mut archive, None)?;
    if !inferred_entries.is_empty() {
        return Ok(backfill_process_model_ids(inferred_entries));
    }

    Ok(backfill_process_model_ids(parse_legacy_package_entries(
        &mut archive,
    )?))
}

async fn find_conflicts(pool: &PgPool, entries: &[PackageEntry]) -> anyhow::Result<ConflictSets> {
    let mut open_data_conflicts = Vec::new();
    let mut user_conflicts = Vec::new();

    for table in SUPPORTED_PACKAGE_TABLES {
        let table_entries = entries
            .iter()
            .filter(|entry| entry.table == table)
            .cloned()
            .collect::<Vec<_>>();
        if table_entries.is_empty() {
            continue;
        }

        let ids = table_entries
            .iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>();
        let rows = sqlx::query(conflict_select_sql(table))
            .bind(ids)
            .fetch_all(pool)
            .await?;

        let parsed_rows = rows
            .iter()
            .map(parse_conflict_row)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let partitioned = partition_conflicts_from_rows(table, &table_entries, &parsed_rows);
        open_data_conflicts.extend(partitioned.open_data_conflicts);
        user_conflicts.extend(partitioned.user_conflicts);
    }

    Ok(ConflictSets {
        open_data_conflicts,
        user_conflicts,
    })
}

#[allow(clippy::too_many_lines)]
async fn insert_entries(
    pool: &PgPool,
    requested_by: Uuid,
    entries: &[PackageEntry],
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;

    for table in INSERT_ORDER {
        for entry in entries.iter().filter(|entry| entry.table == table) {
            let normalized_json_ordered = normalize_json_ordered_for_insert(
                table,
                &entry.version,
                entry.json_ordered.clone(),
            );
            match table {
                PackageRootTable::Contacts => {
                    sqlx::query(
                        r"
                        INSERT INTO contacts (id, version, json_ordered, rule_verification, user_id)
                        VALUES ($1, $2, $3::jsonb, $4, $5)
                        ",
                    )
                    .bind(entry.id)
                    .bind(entry.version.clone())
                    .bind(normalized_json_ordered)
                    .bind(entry.rule_verification)
                    .bind(requested_by)
                    .execute(&mut *tx)
                    .await?;
                }
                PackageRootTable::Sources => {
                    sqlx::query(
                        r"
                        INSERT INTO sources (id, version, json_ordered, rule_verification, user_id)
                        VALUES ($1, $2, $3::jsonb, $4, $5)
                        ",
                    )
                    .bind(entry.id)
                    .bind(entry.version.clone())
                    .bind(normalized_json_ordered)
                    .bind(entry.rule_verification)
                    .bind(requested_by)
                    .execute(&mut *tx)
                    .await?;
                }
                PackageRootTable::Unitgroups => {
                    sqlx::query(
                        r"
                        INSERT INTO unitgroups (id, version, json_ordered, rule_verification, user_id)
                        VALUES ($1, $2, $3::jsonb, $4, $5)
                        ",
                    )
                    .bind(entry.id)
                    .bind(entry.version.clone())
                    .bind(normalized_json_ordered)
                    .bind(entry.rule_verification)
                    .bind(requested_by)
                    .execute(&mut *tx)
                    .await?;
                }
                PackageRootTable::Flowproperties => {
                    sqlx::query(
                        r"
                        INSERT INTO flowproperties (id, version, json_ordered, rule_verification, user_id)
                        VALUES ($1, $2, $3::jsonb, $4, $5)
                        ",
                    )
                    .bind(entry.id)
                    .bind(entry.version.clone())
                    .bind(normalized_json_ordered)
                    .bind(entry.rule_verification)
                    .bind(requested_by)
                    .execute(&mut *tx)
                    .await?;
                }
                PackageRootTable::Flows => {
                    sqlx::query(
                        r"
                        INSERT INTO flows (id, version, json_ordered, rule_verification, user_id)
                        VALUES ($1, $2, $3::jsonb, $4, $5)
                        ",
                    )
                    .bind(entry.id)
                    .bind(entry.version.clone())
                    .bind(normalized_json_ordered)
                    .bind(entry.rule_verification)
                    .bind(requested_by)
                    .execute(&mut *tx)
                    .await?;
                }
                PackageRootTable::Lifecyclemodels => {
                    sqlx::query(
                        r"
                        INSERT INTO lifecyclemodels (id, version, json_ordered, rule_verification, json_tg, user_id)
                        VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, $6)
                        ",
                    )
                    .bind(entry.id)
                    .bind(entry.version.clone())
                    .bind(normalized_json_ordered)
                    .bind(entry.rule_verification)
                    .bind(entry.json_tg.clone().unwrap_or_else(|| json!({})))
                    .bind(requested_by)
                    .execute(&mut *tx)
                    .await?;
                }
                PackageRootTable::Processes => {
                    sqlx::query(
                        r"
                        INSERT INTO processes (id, version, json_ordered, rule_verification, model_id, user_id)
                        VALUES ($1, $2, $3::jsonb, $4, $5, $6)
                        ",
                    )
                    .bind(entry.id)
                    .bind(entry.version.clone())
                    .bind(normalized_json_ordered)
                    .bind(entry.rule_verification)
                    .bind(entry.model_id)
                    .bind(requested_by)
                    .execute(&mut *tx)
                    .await?;
                }
            }
        }
    }

    tx.commit().await?;
    Ok(())
}

async fn fetch_rows_by_exact_roots(
    pool: &PgPool,
    roots: &[PackageRootRef],
) -> anyhow::Result<Vec<PackageEntry>> {
    let mut grouped = BTreeMap::<PackageRootTable, Vec<PackageRootRef>>::new();
    for root in roots {
        grouped.entry(root.table).or_default().push(root.clone());
    }

    let mut output = Vec::new();
    for (table, table_roots) in grouped {
        let ids = table_roots.iter().map(|root| root.id).collect::<Vec<_>>();
        let expected_keys = table_roots
            .iter()
            .map(|root| table_key(root.table, root.id, &root.version))
            .collect::<HashSet<_>>();
        let rows = sqlx::query(select_by_ids_sql(table))
            .bind(ids)
            .fetch_all(pool)
            .await?;
        for row in rows {
            let Some(entry) = parse_package_entry_row(table, &row)? else {
                continue;
            };
            if expected_keys.contains(&table_key(entry.table, entry.id, &entry.version)) {
                output.push(entry);
            }
        }
    }

    Ok(dedupe_entries(output))
}

async fn fetch_root_refs_by_exact_roots(
    pool: &PgPool,
    roots: &[PackageRootRef],
) -> anyhow::Result<Vec<PackageRootRef>> {
    let mut grouped = BTreeMap::<PackageRootTable, Vec<PackageRootRef>>::new();
    for root in roots {
        grouped.entry(root.table).or_default().push(root.clone());
    }

    let mut output = Vec::new();
    for (table, table_roots) in grouped {
        let ids = table_roots.iter().map(|root| root.id).collect::<Vec<_>>();
        let expected_keys = table_roots
            .iter()
            .map(|root| table_key(root.table, root.id, &root.version))
            .collect::<HashSet<_>>();
        let query_sql = select_root_refs_by_ids_sql(table);
        let rows = sqlx::query(query_sql.as_str())
            .bind(ids)
            .fetch_all(pool)
            .await?;
        for row in rows {
            let Some(root) = parse_root_ref_row(table, &row)? else {
                continue;
            };
            if expected_keys.contains(&table_key(root.table, root.id, &root.version)) {
                output.push(root);
            }
        }
    }

    Ok(dedupe_root_refs(output))
}

async fn fetch_reference_scan_rows_by_exact_roots(
    pool: &PgPool,
    roots: &[PackageRootRef],
) -> anyhow::Result<Vec<PackageSeedScanEntry>> {
    let mut grouped = BTreeMap::<PackageRootTable, Vec<PackageRootRef>>::new();
    for root in roots {
        grouped.entry(root.table).or_default().push(root.clone());
    }

    let mut output = Vec::new();
    for (table, table_roots) in grouped {
        let ids = table_roots.iter().map(|root| root.id).collect::<Vec<_>>();
        let expected_keys = table_roots
            .iter()
            .map(|root| table_key(root.table, root.id, &root.version))
            .collect::<HashSet<_>>();
        let query_sql = select_reference_scan_by_ids_sql(table);
        let rows = sqlx::query(query_sql.as_str())
            .bind(ids)
            .fetch_all(pool)
            .await?;
        for row in rows {
            let Some(entry) = parse_seed_scan_entry_row(table, &row)? else {
                continue;
            };
            if expected_keys.contains(&table_key(entry.table, entry.id, &entry.version)) {
                output.push(entry);
            }
        }
    }

    Ok(output)
}

#[allow(dead_code)]
async fn fetch_scope_roots(
    pool: &PgPool,
    requested_by: Uuid,
    scope: PackageExportScope,
) -> anyhow::Result<Vec<PackageEntry>> {
    match scope {
        PackageExportScope::CurrentUser | PackageExportScope::OpenData => {
            fetch_scope_entries(pool, requested_by, scope).await
        }
        PackageExportScope::CurrentUserAndOpenData => {
            let current_user =
                fetch_scope_entries(pool, requested_by, PackageExportScope::CurrentUser).await?;
            let open_data =
                fetch_scope_entries(pool, requested_by, PackageExportScope::OpenData).await?;
            Ok(dedupe_entries(
                current_user.into_iter().chain(open_data).collect(),
            ))
        }
        PackageExportScope::SelectedRoots => Err(anyhow::anyhow!(
            "selected_roots exports must provide explicit roots"
        )),
    }
}

#[allow(dead_code)]
async fn fetch_scope_entries(
    pool: &PgPool,
    requested_by: Uuid,
    scope: PackageExportScope,
) -> anyhow::Result<Vec<PackageEntry>> {
    let mut output = Vec::new();

    for table in SUPPORTED_PACKAGE_TABLES {
        let query = match scope {
            PackageExportScope::CurrentUser => {
                sqlx::query(select_by_user_sql(table))
                    .bind(requested_by)
                    .fetch_all(pool)
                    .await?
            }
            PackageExportScope::OpenData => {
                sqlx::query(select_by_open_data_sql(table))
                    .bind(open_data_state_codes())
                    .fetch_all(pool)
                    .await?
            }
            PackageExportScope::CurrentUserAndOpenData | PackageExportScope::SelectedRoots => {
                unreachable!("scope is normalized before fetch_scope_entries")
            }
        };

        for row in query {
            if let Some(entry) = parse_package_entry_row(table, &row)? {
                output.push(entry);
            }
        }
    }

    Ok(output)
}

async fn fetch_referenced_entries(
    pool: &PgPool,
    refs: &[ReferenceTarget],
) -> anyhow::Result<Vec<PackageEntry>> {
    let mut grouped = BTreeMap::<PackageRootTable, Vec<ReferenceTarget>>::new();
    for reference in refs {
        grouped
            .entry(reference.table)
            .or_default()
            .push(reference.clone());
    }

    let mut output = Vec::new();
    for (table, table_refs) in grouped {
        let ids = table_refs
            .iter()
            .map(|reference| reference.id)
            .collect::<Vec<_>>();
        let rows = sqlx::query(select_by_ids_sql(table))
            .bind(ids)
            .fetch_all(pool)
            .await?;
        let parsed_rows = rows
            .iter()
            .map(|row| parse_package_entry_row(table, row))
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        output.extend(resolve_referenced_entries_from_rows(
            table,
            &table_refs,
            &parsed_rows,
        ));
    }

    Ok(dedupe_entries(output))
}

async fn fetch_model_processes(
    pool: &PgPool,
    model_id: Uuid,
    version: &str,
) -> anyhow::Result<Vec<PackageEntry>> {
    let rows = sqlx::query(
        r"
        SELECT
            id::text AS id,
            version::text AS version,
            json_ordered,
            rule_verification,
            NULL::jsonb AS json_tg,
            model_id::text AS model_id
        FROM processes
        WHERE model_id = $1
          AND version = $2
        ",
    )
    .bind(model_id)
    .bind(version)
    .fetch_all(pool)
    .await?;

    rows.iter()
        .map(|row| parse_package_entry_row(PackageRootTable::Processes, row))
        .collect::<anyhow::Result<Vec<_>>>()
        .map(|entries| entries.into_iter().flatten().collect())
}

async fn fetch_model_process_roots(
    pool: &PgPool,
    model_id: Uuid,
    version: &str,
) -> anyhow::Result<Vec<PackageRootRef>> {
    let rows = sqlx::query(
        r"
        SELECT
            id::text AS id,
            version::text AS version
        FROM processes
        WHERE model_id = $1
          AND version = $2
        ",
    )
    .bind(model_id)
    .bind(version)
    .fetch_all(pool)
    .await?;

    rows.iter()
        .map(|row| parse_root_ref_row(PackageRootTable::Processes, row))
        .collect::<anyhow::Result<Vec<_>>>()
        .map(|roots| roots.into_iter().flatten().collect())
}

async fn fetch_process_model(
    pool: &PgPool,
    model_id: Uuid,
    version: &str,
) -> anyhow::Result<Vec<PackageEntry>> {
    let exact = fetch_rows_by_exact_roots(
        pool,
        &[PackageRootRef {
            table: PackageRootTable::Lifecyclemodels,
            id: model_id,
            version: version.to_owned(),
        }],
    )
    .await?;
    if !exact.is_empty() {
        return Ok(exact);
    }

    let rows = sqlx::query(
        r"
        SELECT
            id::text AS id,
            version::text AS version,
            json_ordered,
            rule_verification,
            json_tg,
            NULL::text AS model_id
        FROM lifecyclemodels
        WHERE id = $1
        ",
    )
    .bind(model_id)
    .fetch_all(pool)
    .await?;

    let mut entries = rows
        .iter()
        .map(|row| parse_package_entry_row(PackageRootTable::Lifecyclemodels, row))
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| compare_versions(&right.version, &left.version));
    Ok(entries.into_iter().take(1).collect())
}

async fn fetch_process_model_roots(
    pool: &PgPool,
    model_id: Uuid,
    version: &str,
) -> anyhow::Result<Vec<PackageRootRef>> {
    let exact = fetch_root_refs_by_exact_roots(
        pool,
        &[PackageRootRef {
            table: PackageRootTable::Lifecyclemodels,
            id: model_id,
            version: version.to_owned(),
        }],
    )
    .await?;
    if !exact.is_empty() {
        return Ok(exact);
    }

    let query_sql = select_root_refs_by_ids_sql(PackageRootTable::Lifecyclemodels);
    let rows = sqlx::query(query_sql.as_str())
        .bind(vec![model_id])
        .fetch_all(pool)
        .await?;
    let parsed = rows
        .iter()
        .map(|row| parse_root_ref_row(PackageRootTable::Lifecyclemodels, row))
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    Ok(resolve_exact_or_latest_roots(
        PackageRootTable::Lifecyclemodels,
        &[(model_id, version.to_owned())],
        &parsed,
    ))
}

async fn fetch_package_artifact(
    pool: &PgPool,
    artifact_id: Uuid,
) -> anyhow::Result<PackageArtifactMeta> {
    let row = sqlx::query(
        r"
        SELECT artifact_kind, status, artifact_url, artifact_format
        FROM lca_package_artifacts
        WHERE id = $1
        ",
    )
    .bind(artifact_id)
    .fetch_one(pool)
    .await?;

    let artifact_kind_raw = row.try_get::<String, _>("artifact_kind")?;
    let artifact_kind = parse_artifact_kind(artifact_kind_raw.as_str()).ok_or_else(|| {
        anyhow::anyhow!("unsupported package artifact kind in database: {artifact_kind_raw}")
    })?;

    Ok(PackageArtifactMeta {
        artifact_kind,
        status: row.try_get::<String, _>("status")?,
        artifact_url: row.try_get::<String, _>("artifact_url")?,
        artifact_format: row.try_get::<String, _>("artifact_format")?,
    })
}

fn read_zip_file_string<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    file_path: &str,
) -> anyhow::Result<Option<String>> {
    let mut file = match archive.by_name(file_path) {
        Ok(file) => file,
        Err(zip::result::ZipError::FileNotFound) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    Ok(Some(content))
}

fn parse_manifest_package_entries<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    manifest: &PackageManifest,
) -> anyhow::Result<Vec<PackageEntry>> {
    let mut entries = Vec::new();

    for meta in &manifest.entries {
        let content = read_zip_file_string(archive, meta.file_path.as_str())?
            .ok_or_else(|| anyhow::anyhow!("package file is missing: {}", meta.file_path))?;
        let parsed = serde_json::from_str::<Value>(content.as_str())?;
        entries.push(normalize_manifest_dataset_payload(meta.table, parsed, meta));
    }

    Ok(dedupe_entries(entries))
}

fn parse_folder_package_entries<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    manifest: Option<&PackageManifest>,
) -> anyhow::Result<Vec<PackageEntry>> {
    let manifest_entries = manifest
        .map(|value| {
            value
                .entries
                .iter()
                .map(|entry| (entry.file_path.as_str(), entry))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();

    let mut entries = Vec::new();
    for index in 0..archive.len() {
        let file_path = {
            let file = archive.by_index(index)?;
            if file.is_dir() {
                continue;
            }
            file.name().to_owned()
        };

        if file_path == "manifest.json"
            || !std::path::Path::new(&file_path)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        {
            continue;
        }

        let Some((table, id, version)) = parse_root_from_package_file_path(file_path.as_str())
        else {
            continue;
        };
        let Some(content) = read_zip_file_string(archive, file_path.as_str())? else {
            continue;
        };
        let parsed = serde_json::from_str::<Value>(content.as_str())?;

        if let Some(meta) = manifest_entries.get(file_path.as_str()) {
            entries.push(normalize_manifest_dataset_payload(table, parsed, meta));
        } else {
            entries.push(normalize_path_dataset_payload(table, id, &version, parsed));
        }
    }

    Ok(dedupe_entries(entries))
}

fn parse_legacy_package_entries<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
) -> anyhow::Result<Vec<PackageEntry>> {
    let mut entries = Vec::new();

    for table in SUPPORTED_PACKAGE_TABLES {
        let path = format!("{LEGACY_PACKAGE_DIR}/{}.json", table_name(table));
        let Some(content) = read_zip_file_string(archive, path.as_str())? else {
            continue;
        };
        let parsed = serde_json::from_str::<Value>(content.as_str())?;
        let Value::Array(items) = parsed else {
            return Err(anyhow::anyhow!(
                "invalid legacy package payload for table {}",
                table_name(table)
            ));
        };
        for item in items {
            if let Some(entry) = normalize_imported_entry(table, item)? {
                entries.push(entry);
            }
        }
    }

    Ok(dedupe_entries(entries))
}

#[allow(clippy::unnecessary_wraps)]
fn normalize_imported_entry(
    table: PackageRootTable,
    value: Value,
) -> anyhow::Result<Option<PackageEntry>> {
    let Value::Object(candidate) = value else {
        return Ok(None);
    };

    let Some(id) = candidate
        .get("id")
        .and_then(Value::as_str)
        .and_then(parse_uuid_opt)
    else {
        return Ok(None);
    };
    let version = normalize_version_string(
        candidate
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or_default(),
    );
    if version.is_empty() {
        return Ok(None);
    }

    let model_id = candidate
        .get("model_id")
        .and_then(Value::as_str)
        .and_then(parse_uuid_opt);
    let json_tg = candidate.get("json_tg").cloned();
    let json_ordered = candidate
        .get("json_ordered")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));

    Ok(Some(PackageEntry {
        table,
        id,
        version,
        json_ordered,
        json_tg,
        model_id,
        rule_verification: candidate
            .get("rule_verification")
            .and_then(Value::as_bool)
            .unwrap_or(true),
    }))
}

fn normalize_manifest_dataset_payload(
    table: PackageRootTable,
    dataset: Value,
    meta: &PackageManifestEntry,
) -> PackageEntry {
    if table != PackageRootTable::Lifecyclemodels {
        return PackageEntry {
            table,
            id: meta.id,
            version: normalize_version_string(meta.version.as_str()),
            json_ordered: dataset,
            json_tg: None,
            model_id: meta.model_id,
            rule_verification: meta.rule_verification,
        };
    }

    if let Value::Object(mut candidate) = dataset {
        let json_tg = candidate
            .remove("json_tg")
            .unwrap_or_else(|| Value::Object(Map::new()));
        return PackageEntry {
            table,
            id: meta.id,
            version: normalize_version_string(meta.version.as_str()),
            json_ordered: Value::Object(candidate),
            json_tg: Some(json_tg),
            model_id: meta.model_id,
            rule_verification: meta.rule_verification,
        };
    }

    PackageEntry {
        table,
        id: meta.id,
        version: normalize_version_string(meta.version.as_str()),
        json_ordered: dataset,
        json_tg: Some(Value::Object(Map::new())),
        model_id: meta.model_id,
        rule_verification: meta.rule_verification,
    }
}

fn normalize_path_dataset_payload(
    table: PackageRootTable,
    id: Uuid,
    version: &str,
    dataset: Value,
) -> PackageEntry {
    if table != PackageRootTable::Lifecyclemodels {
        return PackageEntry {
            table,
            id,
            version: normalize_version_string(version),
            json_ordered: dataset,
            json_tg: None,
            model_id: None,
            rule_verification: true,
        };
    }

    if let Value::Object(mut candidate) = dataset {
        let json_tg = candidate
            .remove("json_tg")
            .unwrap_or_else(|| Value::Object(Map::new()));
        return PackageEntry {
            table,
            id,
            version: normalize_version_string(version),
            json_ordered: Value::Object(candidate),
            json_tg: Some(json_tg),
            model_id: None,
            rule_verification: true,
        };
    }

    PackageEntry {
        table,
        id,
        version: normalize_version_string(version),
        json_ordered: dataset,
        json_tg: Some(Value::Object(Map::new())),
        model_id: None,
        rule_verification: true,
    }
}

fn serialize_entry_dataset(entry: &PackageEntry) -> Value {
    if entry.table != PackageRootTable::Lifecyclemodels {
        return entry.json_ordered.clone();
    }

    if let Value::Object(mut candidate) = entry.json_ordered.clone() {
        candidate.insert(
            "json_tg".to_owned(),
            entry
                .json_tg
                .clone()
                .unwrap_or_else(|| Value::Object(Map::new())),
        );
        return Value::Object(candidate);
    }

    json!({
        "json_ordered": entry.json_ordered,
        "json_tg": entry.json_tg.clone().unwrap_or_else(|| Value::Object(Map::new())),
    })
}

fn normalize_json_ordered_for_insert(
    table: PackageRootTable,
    version: &str,
    dataset: Value,
) -> Value {
    let root_key = dataset_root_key(table);
    let mut outer = match dataset {
        Value::Object(map) => map,
        other => {
            let mut root = Map::new();
            root.insert(root_key.to_owned(), other);
            return ensure_dataset_version_path(Value::Object(root), root_key, version);
        }
    };

    if !outer.contains_key(root_key) {
        let inner = std::mem::take(&mut outer);
        outer.insert(root_key.to_owned(), Value::Object(inner));
    }

    ensure_dataset_version_path(Value::Object(outer), root_key, version)
}

fn ensure_dataset_version_path(dataset: Value, root_key: &str, version: &str) -> Value {
    let Value::Object(mut outer) = dataset else {
        return dataset;
    };

    let mut root_object = outer
        .remove(root_key)
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();

    let mut administrative = root_object
        .remove("administrativeInformation")
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let mut publication = administrative
        .remove("publicationAndOwnership")
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    publication.insert(
        "common:dataSetVersion".to_owned(),
        Value::String(normalize_version_string(version)),
    );
    administrative.insert(
        "publicationAndOwnership".to_owned(),
        Value::Object(publication),
    );
    root_object.insert(
        "administrativeInformation".to_owned(),
        Value::Object(administrative),
    );
    outer.insert(root_key.to_owned(), Value::Object(root_object));

    Value::Object(outer)
}

fn dataset_root_key(table: PackageRootTable) -> &'static str {
    match table {
        PackageRootTable::Contacts => "contactDataSet",
        PackageRootTable::Sources => "sourceDataSet",
        PackageRootTable::Unitgroups => "unitGroupDataSet",
        PackageRootTable::Flowproperties => "flowPropertyDataSet",
        PackageRootTable::Flows => "flowDataSet",
        PackageRootTable::Processes => "processDataSet",
        PackageRootTable::Lifecyclemodels => "lifeCycleModelDataSet",
    }
}

fn extract_ref_targets(source: &Value) -> Vec<ReferenceTarget> {
    let mut result = Vec::new();
    walk_ref_targets(source, &mut result);
    result
}

fn walk_ref_targets(value: &Value, output: &mut Vec<ReferenceTarget>) {
    match value {
        Value::Array(items) => {
            for item in items {
                walk_ref_targets(item, output);
            }
        }
        Value::Object(map) => {
            let ref_object_id = map.get("@refObjectId").and_then(Value::as_str);
            let ref_type = map.get("@type").and_then(Value::as_str);
            if let (Some(ref_object_id), Some(ref_type)) = (ref_object_id, ref_type)
                && let (Some(id), Some(table)) =
                    (parse_uuid_opt(ref_object_id), ref_type_to_table(ref_type))
            {
                output.push(ReferenceTarget {
                    table,
                    id,
                    version: map
                        .get("@version")
                        .and_then(Value::as_str)
                        .map(normalize_version_string)
                        .filter(|value| !value.is_empty()),
                });
            }

            for item in map.values() {
                walk_ref_targets(item, output);
            }
        }
        _ => {}
    }
}

fn parse_root_from_package_file_path(file_path: &str) -> Option<(PackageRootTable, Uuid, String)> {
    let (table_raw, file_name) = file_path.split_once('/')?;
    let table = parse_table_name(table_raw)?;
    let file_stem = file_name.strip_suffix(".json")?;
    let (id_raw, version_raw) = file_stem.rsplit_once('_')?;
    let id = parse_uuid_opt(id_raw)?;
    let version = normalize_version_string(version_raw);
    if version.is_empty() {
        return None;
    }
    Some((table, id, version))
}

fn backfill_process_model_ids(mut entries: Vec<PackageEntry>) -> Vec<PackageEntry> {
    let process_model_ids = entries
        .iter()
        .filter(|entry| entry.table == PackageRootTable::Lifecyclemodels)
        .filter_map(|entry| {
            entry.json_tg.as_ref().map(|json_tg| {
                (
                    entry.id,
                    extract_model_submodels_from_value(
                        entry.version.as_str(),
                        json_tg.get("submodels").unwrap_or(json_tg),
                    ),
                )
            })
        })
        .flat_map(|(model_id, refs)| {
            refs.into_iter().filter_map(move |reference| {
                (reference.table == PackageRootTable::Processes)
                    .then(|| {
                        reference.version.map(|version| {
                            (table_key(reference.table, reference.id, &version), model_id)
                        })
                    })
                    .flatten()
            })
        })
        .collect::<HashMap<_, _>>();

    for entry in &mut entries {
        if entry.table != PackageRootTable::Processes || entry.model_id.is_some() {
            continue;
        }
        entry.model_id = process_model_ids
            .get(&table_key(entry.table, entry.id, &entry.version))
            .copied();
    }

    entries
}

fn extract_model_submodels(entry: &PackageEntry) -> Vec<ReferenceTarget> {
    if entry.table != PackageRootTable::Lifecyclemodels {
        return Vec::new();
    }

    let Some(json_tg) = &entry.json_tg else {
        return Vec::new();
    };
    extract_model_submodels_from_value(
        &entry.version,
        json_tg.get("submodels").unwrap_or(&Value::Null),
    )
}

fn extract_model_submodels_from_value(
    parent_version: &str,
    submodels: &Value,
) -> Vec<ReferenceTarget> {
    let Some(submodels) = submodels.as_array() else {
        return Vec::new();
    };

    submodels
        .iter()
        .filter_map(|item| {
            let object = item.as_object()?;
            let id = object
                .get("id")
                .or_else(|| object.get("processId"))
                .and_then(Value::as_str)
                .and_then(parse_uuid_opt)?;
            let version = object
                .get("version")
                .and_then(Value::as_str)
                .map(normalize_version_string)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| parent_version.to_owned());
            Some(ReferenceTarget {
                table: PackageRootTable::Processes,
                id,
                version: Some(version),
            })
        })
        .collect()
}

#[must_use]
pub fn normalize_version_string(value: &str) -> String {
    let raw = value.trim();
    if raw.is_empty() {
        return String::new();
    }

    let parts = raw.split('.').collect::<Vec<_>>();
    if parts.len() != 3
        || parts
            .iter()
            .any(|part| !part.chars().all(|char| char.is_ascii_digit()))
    {
        return raw.to_owned();
    }

    parts
        .iter()
        .enumerate()
        .map(|(index, part)| {
            if index == 2 {
                format!("{part:0>3}")
            } else {
                format!("{part:0>2}")
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn parse_version_parts(value: &str) -> Option<[u32; 3]> {
    let normalized = normalize_version_string(value);
    let parts = normalized.split('.').collect::<Vec<_>>();
    if parts.len() != 3
        || parts
            .iter()
            .any(|part| !part.chars().all(|char| char.is_ascii_digit()))
    {
        return None;
    }

    Some([
        parts[0].parse().ok()?,
        parts[1].parse().ok()?,
        parts[2].parse().ok()?,
    ])
}

fn compare_versions(left: &str, right: &str) -> std::cmp::Ordering {
    match (parse_version_parts(left), parse_version_parts(right)) {
        (Some(left_parts), Some(right_parts)) => left_parts.cmp(&right_parts),
        _ => normalize_version_string(left).cmp(&normalize_version_string(right)),
    }
}

fn resolve_referenced_entries_from_rows(
    table: PackageRootTable,
    refs: &[ReferenceTarget],
    rows: &[PackageEntry],
) -> Vec<PackageEntry> {
    let mut requested_by_id = HashMap::<Uuid, RequestedVersions>::new();
    for reference in refs.iter().filter(|reference| reference.table == table) {
        let current = requested_by_id.entry(reference.id).or_default();
        if let Some(version) = &reference.version {
            current.versions.insert(normalize_version_string(version));
        } else {
            current.wants_latest = true;
        }
    }

    let mut entries_by_id = HashMap::<Uuid, Vec<PackageEntry>>::new();
    for row in rows.iter().filter(|entry| entry.table == table) {
        entries_by_id.entry(row.id).or_default().push(row.clone());
    }

    let mut output = BTreeMap::<String, PackageEntry>::new();
    for (id, requested) in requested_by_id {
        let mut candidates = entries_by_id.get(&id).cloned().unwrap_or_default();
        if candidates.is_empty() {
            continue;
        }
        candidates.sort_by(|left, right| compare_versions(&right.version, &left.version));

        let by_version = candidates
            .iter()
            .map(|entry| (normalize_version_string(&entry.version), entry.clone()))
            .collect::<HashMap<_, _>>();
        for version in requested.versions {
            if let Some(entry) = by_version.get(&version) {
                output.insert(table_key(table, entry.id, &entry.version), entry.clone());
            }
        }

        if requested.wants_latest
            && let Some(latest) = candidates.first()
        {
            output.insert(table_key(table, latest.id, &latest.version), latest.clone());
        }
    }

    output.into_values().collect()
}

fn resolve_referenced_roots_from_rows(
    table: PackageRootTable,
    refs: &[ReferenceTarget],
    rows: &[PackageRootRef],
) -> Vec<PackageRootRef> {
    let mut requested_by_id = HashMap::<Uuid, RequestedVersions>::new();
    for reference in refs.iter().filter(|reference| reference.table == table) {
        let current = requested_by_id.entry(reference.id).or_default();
        if let Some(version) = &reference.version {
            current.versions.insert(normalize_version_string(version));
        } else {
            current.wants_latest = true;
        }
    }

    let mut roots_by_id = HashMap::<Uuid, Vec<PackageRootRef>>::new();
    for row in rows.iter().filter(|root| root.table == table) {
        roots_by_id.entry(row.id).or_default().push(row.clone());
    }

    let mut output = BTreeMap::<String, PackageRootRef>::new();
    for (id, requested) in requested_by_id {
        let mut candidates = roots_by_id.get(&id).cloned().unwrap_or_default();
        if candidates.is_empty() {
            continue;
        }
        candidates.sort_by(|left, right| compare_versions(&right.version, &left.version));

        let by_version = candidates
            .iter()
            .map(|root| (normalize_version_string(&root.version), root.clone()))
            .collect::<HashMap<_, _>>();
        for version in requested.versions {
            if let Some(root) = by_version.get(&version) {
                output.insert(table_key(table, root.id, &root.version), root.clone());
            }
        }

        if requested.wants_latest
            && let Some(latest) = candidates.first()
        {
            output.insert(table_key(table, latest.id, &latest.version), latest.clone());
        }
    }

    output.into_values().collect()
}

fn resolve_exact_or_latest_roots(
    table: PackageRootTable,
    requested: &[(Uuid, String)],
    rows: &[PackageRootRef],
) -> Vec<PackageRootRef> {
    let mut requested_by_id = HashMap::<Uuid, HashSet<String>>::new();
    for (id, version) in requested {
        requested_by_id
            .entry(*id)
            .or_default()
            .insert(normalize_version_string(version));
    }

    let mut roots_by_id = HashMap::<Uuid, Vec<PackageRootRef>>::new();
    for row in rows.iter().filter(|root| root.table == table) {
        roots_by_id.entry(row.id).or_default().push(row.clone());
    }

    let mut output = BTreeMap::<String, PackageRootRef>::new();
    for (id, versions) in requested_by_id {
        let mut candidates = roots_by_id.get(&id).cloned().unwrap_or_default();
        if candidates.is_empty() {
            continue;
        }
        candidates.sort_by(|left, right| compare_versions(&right.version, &left.version));
        let by_version = candidates
            .iter()
            .map(|root| (normalize_version_string(&root.version), root.clone()))
            .collect::<HashMap<_, _>>();

        for version in versions {
            if let Some(exact) = by_version.get(&version) {
                output.insert(table_key(table, exact.id, &exact.version), exact.clone());
            } else if let Some(latest) = candidates.first() {
                output.insert(table_key(table, latest.id, &latest.version), latest.clone());
            }
        }
    }

    output.into_values().collect()
}

#[derive(Debug, Default)]
struct RequestedVersions {
    versions: HashSet<String>,
    wants_latest: bool,
}

#[derive(Debug, Default)]
struct ConflictSets {
    open_data_conflicts: Vec<ConflictRecord>,
    user_conflicts: Vec<ConflictRecord>,
}

fn partition_conflicts_from_rows(
    table: PackageRootTable,
    entries: &[PackageEntry],
    rows: &[ConflictRow],
) -> ConflictSets {
    let expected_keys = entries
        .iter()
        .map(|entry| table_key(table, entry.id, &entry.version))
        .collect::<HashSet<_>>();
    let mut sets = ConflictSets::default();

    for row in rows {
        if !expected_keys.contains(&table_key(table, row.id, &row.version)) {
            continue;
        }

        let record = ConflictRecord {
            table,
            id: row.id,
            version: row.version.clone(),
            state_code: row.state_code,
            user_id: row.user_id,
        };
        if row.state_code.is_some_and(is_open_data_state_code) {
            sets.open_data_conflicts.push(record);
        } else {
            sets.user_conflicts.push(record);
        }
    }

    sets
}

fn dedupe_entries(entries: Vec<PackageEntry>) -> Vec<PackageEntry> {
    let mut map = BTreeMap::<String, PackageEntry>::new();
    for entry in entries {
        map.insert(table_key(entry.table, entry.id, &entry.version), entry);
    }
    map.into_values().collect()
}

fn dedupe_root_refs(roots: Vec<PackageRootRef>) -> Vec<PackageRootRef> {
    let mut map = BTreeMap::<String, PackageRootRef>::new();
    for root in roots {
        map.insert(table_key(root.table, root.id, &root.version), root);
    }
    map.into_values().collect()
}

fn sort_entries(mut entries: Vec<PackageEntry>) -> Vec<PackageEntry> {
    entries.sort_by(|left, right| {
        table_order(left.table)
            .cmp(&table_order(right.table))
            .then_with(|| left.id.cmp(&right.id))
            .then_with(|| compare_versions(&left.version, &right.version))
    });
    entries
}

fn build_entry_file_path(table: PackageRootTable, id: Uuid, version: &str) -> String {
    format!("{}/{id}_{version}.json", table_name(table))
}

fn build_zip_filename(roots: &[PackageRootRef], scope: PackageExportScope) -> String {
    if roots.len() == 1 {
        let root = &roots[0];
        return format!(
            "tidas-package-{}-{}-{}.zip",
            table_name(root.table),
            root.id,
            root.version
        );
    }

    format!(
        "tidas-package-{}-{}.zip",
        package_scope_name(scope),
        Utc::now().timestamp_millis()
    )
}

fn table_key(table: PackageRootTable, id: Uuid, version: &str) -> String {
    format!(
        "{}:{id}:{}",
        table_name(table),
        normalize_version_string(version)
    )
}

fn table_name(table: PackageRootTable) -> &'static str {
    match table {
        PackageRootTable::Contacts => "contacts",
        PackageRootTable::Sources => "sources",
        PackageRootTable::Unitgroups => "unitgroups",
        PackageRootTable::Flowproperties => "flowproperties",
        PackageRootTable::Flows => "flows",
        PackageRootTable::Processes => "processes",
        PackageRootTable::Lifecyclemodels => "lifecyclemodels",
    }
}

fn parse_table_name(value: &str) -> Option<PackageRootTable> {
    match value.trim() {
        "contacts" => Some(PackageRootTable::Contacts),
        "sources" => Some(PackageRootTable::Sources),
        "unitgroups" => Some(PackageRootTable::Unitgroups),
        "flowproperties" => Some(PackageRootTable::Flowproperties),
        "flows" => Some(PackageRootTable::Flows),
        "processes" => Some(PackageRootTable::Processes),
        "lifecyclemodels" => Some(PackageRootTable::Lifecyclemodels),
        _ => None,
    }
}

fn package_scope_name(scope: PackageExportScope) -> &'static str {
    match scope {
        PackageExportScope::CurrentUser => "current_user",
        PackageExportScope::OpenData => "open_data",
        PackageExportScope::CurrentUserAndOpenData => "current_user_and_open_data",
        PackageExportScope::SelectedRoots => "selected_roots",
    }
}

fn table_order(table: PackageRootTable) -> usize {
    SUPPORTED_PACKAGE_TABLES
        .iter()
        .position(|candidate| *candidate == table)
        .unwrap_or(usize::MAX)
}

fn ref_type_to_table(value: &str) -> Option<PackageRootTable> {
    match value.trim() {
        "contact data set" => Some(PackageRootTable::Contacts),
        "source data set" => Some(PackageRootTable::Sources),
        "unit group data set" => Some(PackageRootTable::Unitgroups),
        "flow property data set" => Some(PackageRootTable::Flowproperties),
        "flow data set" => Some(PackageRootTable::Flows),
        "process data set" => Some(PackageRootTable::Processes),
        "lifeCycleModel data set" => Some(PackageRootTable::Lifecyclemodels),
        _ => None,
    }
}

fn parse_uuid_opt(value: &str) -> Option<Uuid> {
    Uuid::parse_str(value.trim()).ok()
}

fn parse_artifact_kind(value: &str) -> Option<PackageArtifactKind> {
    match value.trim() {
        "import_source" => Some(PackageArtifactKind::ImportSource),
        "export_zip" => Some(PackageArtifactKind::ExportZip),
        "export_report" => Some(PackageArtifactKind::ExportReport),
        "import_report" => Some(PackageArtifactKind::ImportReport),
        _ => None,
    }
}

fn select_root_refs_by_ids_sql(table: PackageRootTable) -> String {
    format!(
        r"
        SELECT
            id::text AS id,
            version::text AS version
        FROM {}
        WHERE id = ANY($1::uuid[])
        ",
        table_name(table)
    )
}

fn select_reference_scan_by_ids_sql(table: PackageRootTable) -> String {
    format!(
        r"
        {}
        WHERE id = ANY($1::uuid[])
        ",
        scope_seed_scan_select_prefix_sql(table)
    )
}

fn select_by_ids_sql(table: PackageRootTable) -> &'static str {
    match table {
        PackageRootTable::Contacts => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM contacts
            WHERE id = ANY($1::uuid[])
            "
        }
        PackageRootTable::Sources => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM sources
            WHERE id = ANY($1::uuid[])
            "
        }
        PackageRootTable::Unitgroups => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM unitgroups
            WHERE id = ANY($1::uuid[])
            "
        }
        PackageRootTable::Flowproperties => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM flowproperties
            WHERE id = ANY($1::uuid[])
            "
        }
        PackageRootTable::Flows => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM flows
            WHERE id = ANY($1::uuid[])
            "
        }
        PackageRootTable::Processes => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                model_id::text AS model_id
            FROM processes
            WHERE id = ANY($1::uuid[])
            "
        }
        PackageRootTable::Lifecyclemodels => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                json_tg,
                NULL::text AS model_id
            FROM lifecyclemodels
            WHERE id = ANY($1::uuid[])
            "
        }
    }
}

#[allow(clippy::too_many_lines)]
fn scope_seed_scan_select_prefix_sql(table: PackageRootTable) -> &'static str {
    match table {
        PackageRootTable::Contacts => {
            r#"
            SELECT
                id::text AS id,
                version::text AS version,
                COALESCE(
                    jsonb_path_query_array(
                        json_ordered::jsonb,
                        '$.** ? (exists (@."@refObjectId") && exists (@."@type"))'
                    ),
                    '[]'::jsonb
                ) AS ref_candidates,
                '[]'::jsonb AS submodels,
                NULL::text AS model_id
            FROM contacts
            "#
        }
        PackageRootTable::Sources => {
            r#"
            SELECT
                id::text AS id,
                version::text AS version,
                COALESCE(
                    jsonb_path_query_array(
                        json_ordered::jsonb,
                        '$.** ? (exists (@."@refObjectId") && exists (@."@type"))'
                    ),
                    '[]'::jsonb
                ) AS ref_candidates,
                '[]'::jsonb AS submodels,
                NULL::text AS model_id
            FROM sources
            "#
        }
        PackageRootTable::Unitgroups => {
            r#"
            SELECT
                id::text AS id,
                version::text AS version,
                COALESCE(
                    jsonb_path_query_array(
                        json_ordered::jsonb,
                        '$.** ? (exists (@."@refObjectId") && exists (@."@type"))'
                    ),
                    '[]'::jsonb
                ) AS ref_candidates,
                '[]'::jsonb AS submodels,
                NULL::text AS model_id
            FROM unitgroups
            "#
        }
        PackageRootTable::Flowproperties => {
            r#"
            SELECT
                id::text AS id,
                version::text AS version,
                COALESCE(
                    jsonb_path_query_array(
                        json_ordered::jsonb,
                        '$.** ? (exists (@."@refObjectId") && exists (@."@type"))'
                    ),
                    '[]'::jsonb
                ) AS ref_candidates,
                '[]'::jsonb AS submodels,
                NULL::text AS model_id
            FROM flowproperties
            "#
        }
        PackageRootTable::Flows => {
            r#"
            SELECT
                id::text AS id,
                version::text AS version,
                COALESCE(
                    jsonb_path_query_array(
                        json_ordered::jsonb,
                        '$.** ? (exists (@."@refObjectId") && exists (@."@type"))'
                    ),
                    '[]'::jsonb
                ) AS ref_candidates,
                '[]'::jsonb AS submodels,
                NULL::text AS model_id
            FROM flows
            "#
        }
        PackageRootTable::Processes => {
            r#"
            SELECT
                id::text AS id,
                version::text AS version,
                COALESCE(
                    jsonb_path_query_array(
                        json_ordered::jsonb,
                        '$.** ? (exists (@."@refObjectId") && exists (@."@type"))'
                    ),
                    '[]'::jsonb
                ) AS ref_candidates,
                '[]'::jsonb AS submodels,
                model_id::text AS model_id
            FROM processes
            "#
        }
        PackageRootTable::Lifecyclemodels => {
            r#"
            SELECT
                id::text AS id,
                version::text AS version,
                COALESCE(
                    jsonb_path_query_array(
                        json_ordered::jsonb,
                        '$.** ? (exists (@."@refObjectId") && exists (@."@type"))'
                    ),
                    '[]'::jsonb
                ) AS ref_candidates,
                COALESCE((json_tg::jsonb)->'submodels', '[]'::jsonb) AS submodels,
                NULL::text AS model_id
            FROM lifecyclemodels
            "#
        }
    }
}

#[allow(dead_code)]
fn select_by_user_sql(table: PackageRootTable) -> &'static str {
    match table {
        PackageRootTable::Contacts => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM contacts
            WHERE user_id = $1
            "
        }
        PackageRootTable::Sources => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM sources
            WHERE user_id = $1
            "
        }
        PackageRootTable::Unitgroups => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM unitgroups
            WHERE user_id = $1
            "
        }
        PackageRootTable::Flowproperties => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM flowproperties
            WHERE user_id = $1
            "
        }
        PackageRootTable::Flows => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM flows
            WHERE user_id = $1
            "
        }
        PackageRootTable::Processes => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                model_id::text AS model_id
            FROM processes
            WHERE user_id = $1
            "
        }
        PackageRootTable::Lifecyclemodels => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                json_tg,
                NULL::text AS model_id
            FROM lifecyclemodels
            WHERE user_id = $1
            "
        }
    }
}

#[allow(dead_code)]
fn select_by_open_data_sql(table: PackageRootTable) -> &'static str {
    match table {
        PackageRootTable::Contacts => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM contacts
            WHERE state_code = ANY($1::int[])
            "
        }
        PackageRootTable::Sources => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM sources
            WHERE state_code = ANY($1::int[])
            "
        }
        PackageRootTable::Unitgroups => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM unitgroups
            WHERE state_code = ANY($1::int[])
            "
        }
        PackageRootTable::Flowproperties => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM flowproperties
            WHERE state_code = ANY($1::int[])
            "
        }
        PackageRootTable::Flows => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                NULL::text AS model_id
            FROM flows
            WHERE state_code = ANY($1::int[])
            "
        }
        PackageRootTable::Processes => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                NULL::jsonb AS json_tg,
                model_id::text AS model_id
            FROM processes
            WHERE state_code = ANY($1::int[])
            "
        }
        PackageRootTable::Lifecyclemodels => {
            r"
            SELECT
                id::text AS id,
                version::text AS version,
                json_ordered,
                rule_verification,
                json_tg,
                NULL::text AS model_id
            FROM lifecyclemodels
            WHERE state_code = ANY($1::int[])
            "
        }
    }
}

fn conflict_select_sql(table: PackageRootTable) -> &'static str {
    match table {
        PackageRootTable::Contacts => {
            r"SELECT id::text AS id, version::text AS version, state_code, user_id::text AS user_id FROM contacts WHERE id = ANY($1::uuid[])"
        }
        PackageRootTable::Sources => {
            r"SELECT id::text AS id, version::text AS version, state_code, user_id::text AS user_id FROM sources WHERE id = ANY($1::uuid[])"
        }
        PackageRootTable::Unitgroups => {
            r"SELECT id::text AS id, version::text AS version, state_code, user_id::text AS user_id FROM unitgroups WHERE id = ANY($1::uuid[])"
        }
        PackageRootTable::Flowproperties => {
            r"SELECT id::text AS id, version::text AS version, state_code, user_id::text AS user_id FROM flowproperties WHERE id = ANY($1::uuid[])"
        }
        PackageRootTable::Flows => {
            r"SELECT id::text AS id, version::text AS version, state_code, user_id::text AS user_id FROM flows WHERE id = ANY($1::uuid[])"
        }
        PackageRootTable::Processes => {
            r"SELECT id::text AS id, version::text AS version, state_code, user_id::text AS user_id FROM processes WHERE id = ANY($1::uuid[])"
        }
        PackageRootTable::Lifecyclemodels => {
            r"SELECT id::text AS id, version::text AS version, state_code, user_id::text AS user_id FROM lifecyclemodels WHERE id = ANY($1::uuid[])"
        }
    }
}

fn parse_seed_scan_entry_row(
    table: PackageRootTable,
    row: &sqlx::postgres::PgRow,
) -> anyhow::Result<Option<PackageSeedScanEntry>> {
    let id_raw = row.try_get::<String, _>("id")?;
    let Some(id) = parse_uuid_opt(id_raw.as_str()) else {
        return Ok(None);
    };
    let version = normalize_version_string(&row.try_get::<String, _>("version")?);
    if version.is_empty() {
        return Ok(None);
    }

    let model_id = row
        .try_get::<Option<String>, _>("model_id")?
        .and_then(|raw| parse_uuid_opt(raw.as_str()));

    Ok(Some(PackageSeedScanEntry {
        table,
        id,
        version,
        ref_candidates: row
            .try_get::<Option<Value>, _>("ref_candidates")?
            .unwrap_or_else(|| Value::Array(Vec::new())),
        submodels: row
            .try_get::<Option<Value>, _>("submodels")?
            .unwrap_or_else(|| Value::Array(Vec::new())),
        model_id,
    }))
}

fn parse_package_entry_row(
    table: PackageRootTable,
    row: &sqlx::postgres::PgRow,
) -> anyhow::Result<Option<PackageEntry>> {
    let id_raw = row.try_get::<String, _>("id")?;
    let Some(id) = parse_uuid_opt(id_raw.as_str()) else {
        return Ok(None);
    };
    let version = normalize_version_string(&row.try_get::<String, _>("version")?);
    if version.is_empty() {
        return Ok(None);
    }

    let model_id = row
        .try_get::<Option<String>, _>("model_id")?
        .and_then(|raw| parse_uuid_opt(raw.as_str()));

    Ok(Some(PackageEntry {
        table,
        id,
        version,
        json_ordered: row.try_get::<Value, _>("json_ordered")?,
        rule_verification: row
            .try_get::<Option<bool>, _>("rule_verification")?
            .unwrap_or(true),
        json_tg: row.try_get::<Option<Value>, _>("json_tg")?,
        model_id,
    }))
}

fn parse_conflict_row(row: &sqlx::postgres::PgRow) -> anyhow::Result<ConflictRow> {
    let id_raw = row.try_get::<String, _>("id")?;
    let id = parse_uuid_opt(id_raw.as_str())
        .ok_or_else(|| anyhow::anyhow!("invalid conflict row id: {id_raw}"))?;
    let version = normalize_version_string(&row.try_get::<String, _>("version")?);
    let user_id = row
        .try_get::<Option<String>, _>("user_id")?
        .and_then(|raw| parse_uuid_opt(raw.as_str()));

    Ok(ConflictRow {
        id,
        version,
        state_code: row.try_get::<Option<i32>, _>("state_code")?,
        user_id,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        io::{Cursor, Write},
    };

    use serde_json::{Value, json};
    use uuid::Uuid;
    use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

    use super::{
        ConflictRow, ExportTraversalCache, PackageEntry, PackageManifest, PackageManifestEntry,
        ReferenceTarget, clear_runtime_export_traversal_cache, extract_model_submodels_from_value,
        extract_package_zip_to_tempdir, load_runtime_export_traversal_cache,
        normalize_json_ordered_for_insert, normalize_version_string, parse_package_entries,
        parse_tidas_validation_report, partition_conflicts_from_rows, plan_reference_resolution,
        preflight_import_validation, remember_root_in_traversal_cache,
        report_from_validation_failure, resolve_exact_or_latest_roots,
        resolve_referenced_entries_from_rows, store_runtime_export_traversal_cache,
    };
    use crate::package_types::{PackageExportScope, PackageRootRef, PackageRootTable};

    fn build_test_package_zip(
        manifest: Option<Value>,
        files: &[(&str, Value)],
    ) -> anyhow::Result<Vec<u8>> {
        let cursor = Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Deflated)
            .compression_level(Some(6));

        if let Some(manifest_value) = manifest {
            writer.start_file("manifest.json", options)?;
            writer.write_all(serde_json::to_string_pretty(&manifest_value)?.as_bytes())?;
        }

        for (path, payload) in files {
            writer.start_file(*path, options)?;
            writer.write_all(serde_json::to_string_pretty(payload)?.as_bytes())?;
        }

        Ok(writer.finish()?.into_inner())
    }

    #[test]
    fn normalize_version_string_pads_numeric_versions() {
        assert_eq!(normalize_version_string("1.1.0"), "01.01.000");
        assert_eq!(normalize_version_string("01.01.000"), "01.01.000");
        assert_eq!(normalize_version_string("1.12.3"), "01.12.003");
        assert_eq!(normalize_version_string("draft"), "draft");
    }

    #[test]
    fn resolve_referenced_entries_matches_normalized_versions() {
        let id = Uuid::nil();
        let result = resolve_referenced_entries_from_rows(
            PackageRootTable::Flowproperties,
            &[ReferenceTarget {
                table: PackageRootTable::Flowproperties,
                id,
                version: Some("1.1.0".to_owned()),
            }],
            &[
                PackageEntry {
                    table: PackageRootTable::Flowproperties,
                    id,
                    version: "01.01.000".to_owned(),
                    json_ordered: json!({"name": "Mass"}),
                    rule_verification: true,
                    json_tg: None,
                    model_id: None,
                },
                PackageEntry {
                    table: PackageRootTable::Flowproperties,
                    id,
                    version: "02.00.000".to_owned(),
                    json_ordered: json!({"name": "Volume"}),
                    rule_verification: true,
                    json_tg: None,
                    model_id: None,
                },
            ],
        );

        assert_eq!(
            result
                .iter()
                .map(|entry| (entry.id, entry.version.clone()))
                .collect::<Vec<_>>(),
            vec![(id, "01.01.000".to_owned())]
        );
    }

    #[test]
    fn resolve_referenced_entries_falls_back_to_latest_when_version_missing() {
        let id = Uuid::nil();
        let result = resolve_referenced_entries_from_rows(
            PackageRootTable::Flows,
            &[ReferenceTarget {
                table: PackageRootTable::Flows,
                id,
                version: None,
            }],
            &[
                PackageEntry {
                    table: PackageRootTable::Flows,
                    id,
                    version: "01.00.000".to_owned(),
                    json_ordered: json!({"name": "Older flow"}),
                    rule_verification: true,
                    json_tg: None,
                    model_id: None,
                },
                PackageEntry {
                    table: PackageRootTable::Flows,
                    id,
                    version: "02.00.000".to_owned(),
                    json_ordered: json!({"name": "Latest flow"}),
                    rule_verification: true,
                    json_tg: None,
                    model_id: None,
                },
            ],
        );

        assert_eq!(
            result
                .iter()
                .map(|entry| (entry.id, entry.version.clone()))
                .collect::<Vec<_>>(),
            vec![(id, "02.00.000".to_owned())]
        );
    }

    #[test]
    fn plan_reference_resolution_skips_known_exact_and_uses_latest_cache() {
        let known_id = Uuid::nil();
        let latest_id = Uuid::from_u128(1);
        let uncached_id = Uuid::from_u128(2);
        let known_exact = [super::table_key(
            PackageRootTable::Processes,
            known_id,
            "01.00.000",
        )]
        .into_iter()
        .collect();
        let resolved_latest = [(
            (PackageRootTable::Flows, latest_id),
            Some("02.00.000".to_owned()),
        )]
        .into_iter()
        .collect();

        let plan = plan_reference_resolution(
            &[
                ReferenceTarget {
                    table: PackageRootTable::Processes,
                    id: known_id,
                    version: Some("01.00.000".to_owned()),
                },
                ReferenceTarget {
                    table: PackageRootTable::Flows,
                    id: latest_id,
                    version: None,
                },
                ReferenceTarget {
                    table: PackageRootTable::Flows,
                    id: uncached_id,
                    version: None,
                },
                ReferenceTarget {
                    table: PackageRootTable::Flows,
                    id: uncached_id,
                    version: None,
                },
            ],
            &known_exact,
            &HashSet::new(),
            &resolved_latest,
            false,
        );

        assert!(plan.exact_roots_to_fetch.is_empty());
        assert_eq!(
            plan.cached_roots,
            vec![crate::package_types::PackageRootRef {
                table: PackageRootTable::Flows,
                id: latest_id,
                version: "02.00.000".to_owned(),
            }]
        );
        assert_eq!(plan.latest_refs_to_fetch.len(), 1);
        assert_eq!(plan.latest_refs_to_fetch[0].id, uncached_id);
    }

    #[test]
    fn plan_reference_resolution_normalizes_versioned_refs() {
        let id = Uuid::from_u128(3);
        let plan = plan_reference_resolution(
            &[ReferenceTarget {
                table: PackageRootTable::Flowproperties,
                id,
                version: Some("1.1.0".to_owned()),
            }],
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
            false,
        );

        assert_eq!(
            plan.exact_roots_to_fetch,
            vec![crate::package_types::PackageRootRef {
                table: PackageRootTable::Flowproperties,
                id,
                version: "01.01.000".to_owned(),
            }]
        );
    }

    #[test]
    fn plan_reference_resolution_skips_versionless_refs_for_covered_ids() {
        let id = Uuid::from_u128(4);
        let known_any_version = [(PackageRootTable::Flows, id)].into_iter().collect();

        let plan = plan_reference_resolution(
            &[ReferenceTarget {
                table: PackageRootTable::Flows,
                id,
                version: None,
            }],
            &HashSet::new(),
            &known_any_version,
            &HashMap::new(),
            true,
        );

        assert!(plan.cached_roots.is_empty());
        assert!(plan.exact_roots_to_fetch.is_empty());
        assert!(plan.latest_refs_to_fetch.is_empty());
    }

    #[test]
    fn plan_reference_resolution_keeps_versionless_lookup_for_selected_roots() {
        let id = Uuid::from_u128(5);
        let known_any_version = [(PackageRootTable::Flows, id)].into_iter().collect();

        let plan = plan_reference_resolution(
            &[ReferenceTarget {
                table: PackageRootTable::Flows,
                id,
                version: None,
            }],
            &HashSet::new(),
            &known_any_version,
            &HashMap::new(),
            false,
        );

        assert!(plan.cached_roots.is_empty());
        assert!(plan.exact_roots_to_fetch.is_empty());
        assert_eq!(plan.latest_refs_to_fetch.len(), 1);
        assert_eq!(plan.latest_refs_to_fetch[0].id, id);
    }

    #[test]
    fn resolve_exact_or_latest_roots_prefers_exact_match_and_falls_back_to_latest() {
        let exact_id = Uuid::from_u128(6);
        let fallback_id = Uuid::from_u128(7);
        let roots = vec![
            PackageRootRef {
                table: PackageRootTable::Lifecyclemodels,
                id: exact_id,
                version: "02.00.000".to_owned(),
            },
            PackageRootRef {
                table: PackageRootTable::Lifecyclemodels,
                id: exact_id,
                version: "01.00.000".to_owned(),
            },
            PackageRootRef {
                table: PackageRootTable::Lifecyclemodels,
                id: fallback_id,
                version: "03.00.000".to_owned(),
            },
            PackageRootRef {
                table: PackageRootTable::Lifecyclemodels,
                id: fallback_id,
                version: "02.00.000".to_owned(),
            },
        ];

        let resolved = resolve_exact_or_latest_roots(
            PackageRootTable::Lifecyclemodels,
            &[
                (exact_id, "01.00.000".to_owned()),
                (fallback_id, "01.00.000".to_owned()),
            ],
            &roots,
        );

        assert_eq!(
            resolved
                .iter()
                .map(|root| (root.id, root.version.clone()))
                .collect::<Vec<_>>(),
            vec![
                (exact_id, "01.00.000".to_owned()),
                (fallback_id, "03.00.000".to_owned()),
            ]
        );
    }

    #[test]
    fn extract_model_submodels_from_value_uses_parent_version_as_fallback() {
        let process_id = Uuid::from_u128(8);
        let refs = extract_model_submodels_from_value(
            "02.00.000",
            &json!([
                {
                    "id": process_id,
                }
            ]),
        );

        assert_eq!(
            refs.iter()
                .map(|reference| (reference.id, reference.version.clone()))
                .collect::<Vec<_>>(),
            vec![(process_id, Some("02.00.000".to_owned()))]
        );
    }

    #[test]
    fn partition_conflicts_filters_open_data_but_flags_user_conflicts() {
        let id = Uuid::nil();
        let entries = vec![PackageEntry {
            table: PackageRootTable::Processes,
            id,
            version: "01.00.000".to_owned(),
            json_ordered: json!({"name": "Process"}),
            rule_verification: true,
            json_tg: None,
            model_id: None,
        }];
        let rows = vec![
            ConflictRow {
                id,
                version: "01.00.000".to_owned(),
                state_code: Some(100),
                user_id: None,
            },
            ConflictRow {
                id,
                version: "02.00.000".to_owned(),
                state_code: Some(150),
                user_id: None,
            },
        ];

        let partitioned =
            partition_conflicts_from_rows(PackageRootTable::Processes, &entries, &rows);
        assert_eq!(partitioned.open_data_conflicts.len(), 1);
        assert_eq!(partitioned.user_conflicts.len(), 0);
    }

    #[test]
    fn partition_conflicts_treats_legacy_99_as_non_open_data() {
        let id = Uuid::nil();
        let entries = vec![PackageEntry {
            table: PackageRootTable::Processes,
            id,
            version: "01.00.000".to_owned(),
            json_ordered: json!({"name": "Process"}),
            rule_verification: true,
            json_tg: None,
            model_id: None,
        }];
        let rows = vec![ConflictRow {
            id,
            version: "01.00.000".to_owned(),
            state_code: Some(99),
            user_id: None,
        }];

        let partitioned =
            partition_conflicts_from_rows(PackageRootTable::Processes, &entries, &rows);
        assert_eq!(partitioned.open_data_conflicts.len(), 0);
        assert_eq!(partitioned.user_conflicts.len(), 1);
    }

    #[test]
    fn partition_conflicts_rejects_existing_user_data() {
        let id = Uuid::nil();
        let user_id = Uuid::new_v4();
        let entries = vec![PackageEntry {
            table: PackageRootTable::Sources,
            id,
            version: "01.00.000".to_owned(),
            json_ordered: json!({"title": "Source"}),
            rule_verification: true,
            json_tg: None,
            model_id: None,
        }];
        let rows = vec![ConflictRow {
            id,
            version: "01.00.000".to_owned(),
            state_code: Some(10),
            user_id: Some(user_id),
        }];

        let partitioned = partition_conflicts_from_rows(PackageRootTable::Sources, &entries, &rows);
        assert_eq!(partitioned.open_data_conflicts.len(), 0);
        assert_eq!(partitioned.user_conflicts.len(), 1);
        assert_eq!(partitioned.user_conflicts[0].user_id, Some(user_id));
    }

    #[test]
    fn runtime_export_traversal_cache_round_trips_and_clears() {
        let job_id = Uuid::from_u128(9);
        let process_id = Uuid::from_u128(10);
        let flow_id = Uuid::from_u128(11);
        clear_runtime_export_traversal_cache(job_id);

        let mut cache = ExportTraversalCache::default();
        remember_root_in_traversal_cache(
            &mut cache,
            &PackageRootRef {
                table: PackageRootTable::Processes,
                id: process_id,
                version: "01.00.000".to_owned(),
            },
        );
        let _ = cache.resolved_latest.insert(
            (PackageRootTable::Flows, flow_id),
            Some("02.00.000".to_owned()),
        );

        store_runtime_export_traversal_cache(job_id, &cache);
        let loaded = load_runtime_export_traversal_cache(job_id)
            .expect("runtime export traversal cache should exist");

        assert!(loaded.known_exact.contains(&super::table_key(
            PackageRootTable::Processes,
            process_id,
            "01.00.000"
        )));
        assert!(
            loaded
                .known_any_version
                .contains(&(PackageRootTable::Processes, process_id))
        );
        assert_eq!(
            loaded
                .resolved_latest
                .get(&(PackageRootTable::Flows, flow_id))
                .cloned()
                .flatten(),
            Some("02.00.000".to_owned())
        );

        clear_runtime_export_traversal_cache(job_id);
        assert!(load_runtime_export_traversal_cache(job_id).is_none());
    }

    #[test]
    fn parse_package_entries_accepts_manifest_without_current_version_marker() {
        let process_id = Uuid::from_u128(12);
        let model_id = Uuid::from_u128(13);
        let manifest = json!(PackageManifest {
            format: "tidas-package".to_owned(),
            version: 1,
            exported_at: "2026-03-20T00:00:00Z".to_owned(),
            scope: PackageExportScope::SelectedRoots,
            roots: vec![PackageRootRef {
                table: PackageRootTable::Processes,
                id: process_id,
                version: "01.00.000".to_owned(),
            }],
            entries: vec![PackageManifestEntry {
                table: PackageRootTable::Processes,
                id: process_id,
                version: "01.00.000".to_owned(),
                file_path: format!("processes/{process_id}_01.00.000.json"),
                rule_verification: true,
                model_id: Some(model_id),
            }],
            counts: HashMap::from([("processes".to_owned(), 1_usize)])
                .into_iter()
                .collect(),
            total_count: 1,
        });
        let zip_bytes = build_test_package_zip(
            Some(manifest),
            &[(
                &format!("processes/{process_id}_01.00.000.json"),
                json!({
                    "processInformation": {
                        "dataSetInformation": { "name": { "baseName": "Legacy manifest process" } }
                    }
                }),
            )],
        )
        .expect("build package zip");

        let entries = parse_package_entries(zip_bytes.as_slice()).expect("parse package entries");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].table, PackageRootTable::Processes);
        assert_eq!(entries[0].id, process_id);
        assert_eq!(entries[0].version, "01.00.000");
        assert_eq!(entries[0].model_id, Some(model_id));
    }

    #[test]
    fn parse_package_entries_can_infer_entries_without_manifest() {
        let process_id = Uuid::from_u128(14);
        let model_id = Uuid::from_u128(15);
        let zip_bytes = build_test_package_zip(
            None,
            &[
                (
                    &format!("lifecyclemodels/{model_id}_01.00.000.json"),
                    json!({
                        "name": "Model without manifest",
                        "json_tg": {
                            "submodels": [
                                { "processId": process_id, "version": "01.00.000" }
                            ]
                        }
                    }),
                ),
                (
                    &format!("processes/{process_id}_01.00.000.json"),
                    json!({
                        "processInformation": {
                            "dataSetInformation": { "name": { "baseName": "Process without manifest" } }
                        }
                    }),
                ),
            ],
        )
        .expect("build package zip");

        let entries = parse_package_entries(zip_bytes.as_slice()).expect("parse package entries");
        assert_eq!(entries.len(), 2);

        let process_entry = entries
            .iter()
            .find(|entry| entry.table == PackageRootTable::Processes)
            .expect("process entry should exist");
        assert_eq!(process_entry.id, process_id);
        assert_eq!(process_entry.version, "01.00.000");
        assert_eq!(process_entry.model_id, Some(model_id));
    }

    #[test]
    fn parse_tidas_validation_report_accepts_location_or_path() {
        let report = parse_tidas_validation_report(
            &json!({
                "ok": false,
                "summary": {
                    "issue_count": 2,
                    "error_count": 1,
                    "warning_count": 1
                },
                "issues": [
                    {
                        "issue_code": "schema_error",
                        "severity": "error",
                        "category": "sources",
                        "file_path": "sources/a.json",
                        "location": "root/path",
                        "message": "bad schema",
                        "context": {"validator": "required"}
                    },
                    {
                        "issue_code": "localized_text_language_error",
                        "severity": "warning",
                        "category": "processes",
                        "file_path": "processes/b.json",
                        "path": "processDataSet/name/baseName/0",
                        "message": "language mismatch",
                        "context": {}
                    }
                ]
            })
            .to_string(),
        )
        .expect("parse report");

        assert_eq!(report.summary.issue_count, 2);
        assert_eq!(report.summary.error_count, 1);
        assert_eq!(report.summary.warning_count, 1);
        assert_eq!(report.issues[0].location, "root/path");
        assert_eq!(report.issues[1].location, "processDataSet/name/baseName/0");
    }

    #[test]
    fn extract_package_zip_to_tempdir_materializes_archive() {
        let process_id = Uuid::from_u128(16);
        let zip_bytes = build_test_package_zip(
            Some(json!({
                "format": "tiangong-tidas-package",
                "version": 2,
                "exported_at": "2026-03-23T00:00:00Z",
                "scope": "selected_roots",
                "roots": [],
                "entries": [],
                "counts": {},
                "total_count": 1
            })),
            &[(
                &format!("processes/{process_id}_01.00.000.json"),
                json!({"foo": "bar"}),
            )],
        )
        .expect("build zip");

        let temp_dir = extract_package_zip_to_tempdir(zip_bytes.as_slice()).expect("extract zip");
        let manifest_path = temp_dir.path().join("manifest.json");
        let process_path = temp_dir
            .path()
            .join(format!("processes/{process_id}_01.00.000.json"));

        assert!(manifest_path.exists());
        assert!(process_path.exists());
    }

    #[test]
    fn report_from_validation_failure_marks_import_as_blocked() {
        let validation = parse_tidas_validation_report(
            &json!({
                "ok": false,
                "summary": {
                    "issue_count": 1,
                    "error_count": 1,
                    "warning_count": 0
                },
                "issues": [{
                    "issue_code": "schema_error",
                    "severity": "error",
                    "category": "sources",
                    "file_path": "sources/a.json",
                    "location": "<root>",
                    "message": "schema failure",
                    "context": {}
                }]
            })
            .to_string(),
        )
        .expect("parse report");

        let report = report_from_validation_failure(7, &validation);
        assert!(!report.ok);
        assert_eq!(report.code, "VALIDATION_FAILED");
        assert_eq!(report.summary.total_entries, 7);
        assert_eq!(report.summary.validation_issue_count, 1);
        assert_eq!(report.summary.error_count, 1);
        assert_eq!(report.summary.warning_count, 0);
        assert_eq!(report.summary.user_conflict_count, 0);
        assert_eq!(report.summary.filtered_open_data_count, 0);
        assert_eq!(report.summary.importable_count, 0);
        assert_eq!(report.summary.imported_count, 0);
        assert_eq!(report.validation_issues.len(), 1);
    }

    #[test]
    fn preflight_import_validation_prefers_validation_failure_over_empty_package() {
        let validation = parse_tidas_validation_report(
            &json!({
                "ok": false,
                "summary": {
                    "issue_count": 1,
                    "error_count": 1,
                    "warning_count": 0
                },
                "issues": [{
                    "issue_code": "manifest_missing",
                    "severity": "error",
                    "category": "package",
                    "file_path": "manifest.json",
                    "location": "<root>",
                    "message": "manifest is missing",
                    "context": {}
                }]
            })
            .to_string(),
        )
        .expect("parse report");

        let report = preflight_import_validation(&[], &validation)
            .expect("validation failure should be converted into report")
            .expect("expected validation failure report");

        assert_eq!(report.code, "VALIDATION_FAILED");
        assert_eq!(report.summary.total_entries, 0);
        assert_eq!(report.summary.error_count, 1);
    }

    #[test]
    fn normalize_json_ordered_for_insert_wraps_bare_lifecyclemodel_payload() {
        let payload = json!({
            "name": "Bare lifecycle model"
        });

        let normalized = normalize_json_ordered_for_insert(
            PackageRootTable::Lifecyclemodels,
            "01.00.000",
            payload,
        );

        assert_eq!(
            normalized["lifeCycleModelDataSet"]["administrativeInformation"]["publicationAndOwnership"]
                ["common:dataSetVersion"],
            json!("01.00.000")
        );
        assert_eq!(
            normalized["lifeCycleModelDataSet"]["name"],
            json!("Bare lifecycle model")
        );
    }
}
