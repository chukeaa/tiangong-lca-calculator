---
title: TIDAS Package Async Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: zh-CN
whenToUse:
  - 当你需要 package-worker 的异步 import/export 契约时
  - 当 package jobs、artifacts、request cache 或 import validation 规则变化时
whenToUpdate:
  - 当 package-worker payload、artifact 格式、状态机或权限边界变化时
checkPaths:
  - docs/tidas-package-contract.md
  - AGENTS.md
  - .docpact/config.yaml
  - crates/solver-worker/**
  - docs/agents/repo-validation.md
lastReviewedAt: 2026-06-10
lastReviewedCommit: 4546fb8fff034c84cd1b699cb049345b70eabe16
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
---

# TIDAS Package Async Contract

本文档定义 TIDAS 数据包异步导入/导出在 `tiangong-lca-worker` 中的 worker、表结构与 artifact 契约。

## 1. 目标

- 将完整 ZIP 导入/导出从同步 edge function 挪到异步 worker。
- 默认使用统一 `worker_jobs(worker_queue=package)` 生命周期，并保留 legacy `pgmq + object storage + job status` 作为显式兼容/debug 路径。
- 避免把 `snapshot_id` 语义强行塞进 package 任务。

## 2. 为什么不复用 `lca_jobs`

`lca_jobs` / `lca_results` 当前强绑定 `snapshot_id` 和数值求解语义，见：

- `public.lca_jobs.snapshot_id uuid NOT NULL`
- `public.lca_results.snapshot_id uuid NOT NULL`
- `job_type` 与 `artifact_format` 也都围绕求解/快照设计

因此 package worker 复用的是“异步模式”，不是“同一张运行时表”。

## 3. 关键表

- `lca_package_jobs`
  - optional retained package domain/history 表，用于 legacy pgmq/debug 路径、历史状态和诊断；统一 `worker_jobs` 路径不得要求该表存在
- `lca_package_artifacts`
  - import 源 ZIP、export ZIP、import/export report 的 artifact 元数据
- `lca_package_request_cache`
  - 按用户 + 操作 + request key 做去重与状态追踪
- `worker_jobs`
  - package worker 的 canonical 生命周期、lease、进度、错误和 result projection

## 4. 队列与 RPC

legacy 路径：

- `pgmq` queue: `lca_package_jobs`
- enqueue RPC: `public.lca_package_enqueue_job(jsonb)`
- 仅 `service_role` 可执行

统一任务路径：

- `worker_jobs.worker_queue`: `package`
- enqueue RPC: `public.worker_enqueue_job(...)`
- claim RPC: `public.worker_claim_jobs('package', ...)`
- result RPC: `public.worker_record_job_result(...)`
- 仅 `service_role` 可 enqueue / claim / heartbeat / record result

## 5. 任务类型

legacy `lca_package_jobs.job_type` 与 worker payload `type` 必须一致：

- `export_package`
- `import_package`

`worker_jobs` 路径使用 job kind 表达统一任务类型，并映射回同一组 package payload：

| `worker_jobs.job_kind` | `payload_schema_version` | legacy payload `type` | result schema |
| --- | --- | --- | --- |
| `tidas.export_package` | `tidas.export_package.request.v1` | `export_package` | `tidas.export_package.result.v1` |
| `tidas.import_package` | `tidas.import_package.request.v1` | `import_package` | `tidas.import_package.result.v1` |

`package_worker` 默认走 `worker_jobs`；legacy `pgmq` 兼容/debug 路径必须同时使用 `--package-queue-backend pgmq` 或 `PACKAGE_QUEUE_BACKEND=pgmq`，并显式设置 `ALLOW_LEGACY_JOB_TABLE_BACKEND=true` 或传入 `--allow-legacy-job-table-backend`。`worker_jobs` 模式领取 `worker_queue=package`。`PACKAGE_WORKER_ID`、`PACKAGE_WORKER_JOBS_CLAIM_LIMIT`、`PACKAGE_WORKER_JOBS_LEASE_SECONDS` 控制 worker_jobs claim/diagnostics/lease。

## 6. Payload 契约

### 6.1 `export_package`

```json
{
  "type": "export_package",
  "job_id": "<uuid>",
  "requested_by": "<uuid>",
  "scope": "current_user",
  "roots": []
}
```

`scope` 支持：

- `current_user`
- `open_data`
- `current_user_and_open_data`
- `selected_roots`

### 6.2 `import_package`

```json
{
  "type": "import_package",
  "job_id": "<uuid>",
  "requested_by": "<uuid>",
  "source_artifact_id": "<uuid>"
}
```

`worker_jobs.payload_json` 可以使用上述 snake_case 字段，也可以使用 Edge 友好的 alias：

- `jobId` / `packageJobId` -> `job_id`
- `requestedBy` -> `requested_by`
- `sourceArtifactId` -> `source_artifact_id`
- export roots 中的 `tableName` / `rootTable`、`datasetId`、`datasetVersion` 会映射到 `table`、`id`、`version`

payload 必须仍携带有效 `job_id` compatibility UUID，因为 `lca_package_artifacts`、`lca_package_export_items` 和 `lca_package_request_cache` 的历史 `job_id` columns 仍用于同一次 package 请求内分组与 artifact/cache lookup。该 UUID 不要求存在 `lca_package_jobs` parent row。

## 6.3 `import_package` worker 执行顺序（新增）

`import_package` 在 worker 侧执行时，必须先做结构化校验，再进入冲突检测/写库：

1. 下载上传 ZIP artifact；
2. 解压到临时目录；
3. 调用 `python3 -m tidas_tools.validate --input-dir <dir> --format json`（允许按运行环境 fallback 到其他等价命令）；
4. 解析结构化 JSON 校验报告；
5. 若 `summary.error_count > 0`，直接产出 import report：
   - `code = VALIDATION_FAILED`
   - 不执行 conflict checks
   - 不执行任何 inserts
6. 若无校验错误，再执行现有冲突检测和导入流程。

## 7. Artifact 契约

`lca_package_artifacts.artifact_kind`：

- `import_source`
- `export_zip`
- `export_report`
- `import_report`

`artifact_format`：

- `tidas-package-zip:v1`
- `tidas-package-export-report:v1`
- `tidas-package-import-report:v1`

推荐 `content_type`：

- ZIP: `application/zip`
- report: `application/json`

### 7.0.1 大 artifact 上传上限

package worker 通过 S3-compatible object storage 写入 export ZIP / report artifact。对象存储平台的真实 max-file-limit 必须大于预期 package artifact 体积；例如全量 `open_data` export 可能达到数百 MB。若生产后端限制低于 artifact 体积，运维应优先调高平台侧 max-file-limit。

`S3_MAX_UPLOAD_BYTES` 是 worker 本地 preflight guard，应配置为与平台侧 max-file-limit 一致或略低。设置后，worker 会在 single PUT 或 multipart upload 发起前检查 artifact byte size；超限时使用 `artifact_too_large` 失败诊断，并保留 `upload_mode`、`stage=preflight_upload_size`、`artifact_byte_size`、`max_upload_bytes` 和 `storage_error_code=EntityTooLarge`，避免 multipart 上传到中途才失败。

### 7.1 Artifact retention / GC 契约

package artifact 必须带或刷新 `expires_at`：

- `export_zip` / `export_report`：默认 30 天；
- `import_source` / `import_report`：默认 14 天；
- worker 写入的新 artifact 在插入时写入 `expires_at`；
- `import_source` 由 API 上传创建时，worker 在 import job 进入 terminal 成功/失败状态后刷新 14 天 TTL；
- `is_pinned = true` 的 artifact 不参与自动 GC；
- `status = deleted` 表示对象 payload 已被 GC 删除，API 不应再返回可下载 URL。

worker 侧 GC 必须 object-aware：

1. dry-run 先输出 eligible/protected reason；
2. 只处理 `expires_at <= now()`、`is_pinned = false`、父 job 非 `queued/running`、且无 active/recent request-cache 引用的 ready artifact；
3. 先删除对象存储 payload；
4. 对象删除成功后，才把 artifact 标记为 `deleted`；
5. 对象删除失败时只记录 `metadata.gc` 错误，不删除 DB metadata；
6. 当 optional `lca_package_jobs` 仍存在时，一个 terminal package job 的 artifact 都已 `deleted`、且无 active/recent cache 引用后，才允许删除 legacy job metadata。`lca_package_export_items` 已不依赖该表 FK；新路径应以 `worker_job_id` 和 artifact/cache TTL 作为保护条件。

当前 worker runtime 提供 `package_gc` CLI：

```bash
cargo run -p solver-worker --bin package_gc --
cargo run -p solver-worker --bin package_gc -- --execute
```

切到统一 `worker_jobs` 后，package artifact GC 使用 `job_kind=tidas.package_artifact_gc`、`worker_queue=maintenance`。timer/operator action 通过 `maintenance_enqueue package-artifact-gc` 创建任务；`maintenance_worker` 领取任务后仍调用现有 `package_gc` binary。payload 表达 `execute`、`batchSize`、`maxBatches`、`jobRetentionDays` 和 `requestCacheRetentionDays`，result 记录 parsed `[summary]`、exit code、stdout/stderr tail，并通过 `result_ref` 指向 operator-only `maintenance_gc_report` artifact metadata row。缺省不传 `execute=true` 时必须保持 dry-run 行为。

生产部署契约：

- `package_gc` release binary 应随 `package_worker` 一起部署到所有活跃 worker 主机；
- legacy `package-gc.timer` 只能在一个调度主机启用，其他主机保留 binary 作为故障切换候选；
- 统一队列模式下，timer 或 operator action 只负责 enqueue `tidas.package_artifact_gc` worker job，不直接代表任务事实；
- timer 首次启用必须 dry-run，不带 `--execute`，并检查 `[retention]` eligible/protected reason 与 `[summary] dry_run=true ...`；
- destructive 清理必须显式加 `--execute`，并在首轮保留小批量限制，例如 `PACKAGE_GC_BATCH_SIZE=100`、`PACKAGE_GC_MAX_BATCHES=1`；
- `--execute` 模式需要对象存储环境变量，且会先删对象 payload，再标记 artifact `deleted`；对象删除失败时只记录 `metadata.gc` 错误，不删除 DB metadata；
- `--execute` 模式使用 PostgreSQL advisory lock 防止重叠执行；在统一队列模式下还必须使用 `worker_jobs.concurrency_key` 防止同环境同类 GC 并发；
- destructive execute job 默认 `max_attempts=1`，失败后由 operator 显式 retry，避免删除类任务自动重复执行。

### 7.2 import report payload（新增字段）

`tidas-package-import-report:v1` 的 payload 结构扩展如下：

- `summary.validation_issue_count`
- `summary.error_count`
- `summary.warning_count`
- `validation_issues[]`

`validation_issues[]` 每条包含：

- `issue_code`
- `severity`
- `category`
- `file_path`
- `location`
- `message`
- `context`

无论最终结果是 `IMPORTED` / `USER_DATA_CONFLICT` / `VALIDATION_FAILED`，report 都会携带校验统计；当 `VALIDATION_FAILED` 时，还会包含导致阻断导入的校验问题详情。

## 8. 状态机

legacy `lca_package_jobs.status`：

- `queued`
- `running`
- `ready`
- `completed`
- `failed`
- `stale`

legacy pgmq/debug 路径语义：

- `export_package`: `queued -> running -> ready`
- `import_package`: `queued -> running -> completed`
- 失败统一落 `failed`

`worker_jobs` 路径外层生命周期：

- `queued/stale -> running -> completed|failed|cancelled`
- `phase` 使用 `export_package` 或 `import_package`
- `progress` 仅用于任务中心提示，不替代 package artifact 或 request-cache 状态
- terminal `result_json` 包含 `workerJobId`、`packageJobId`、`payloadType`、`packageJobStatus`、`artifacts[]`
- `result_ref` 使用 `{"domainSource":"worker_jobs","workerJobId":"<uuid>","packageJobId":"<uuid>"}`，并且 worker 会把 `lca_package_artifacts`、`lca_package_export_items`、`lca_package_request_cache` 中可关联的 rows 回填到同一个 `worker_job_id`
- optional `lca_package_jobs` 存在时，worker 也会 best-effort 回填 `lca_package_jobs.worker_job_id`、状态和 diagnostics；该表不存在时不得影响任务完成

重要差异：

- legacy `export_package` 多 pass 通过重新写入 `pgmq.lca_package_jobs` 继续执行；
- `worker_jobs` 模式下，worker runtime 不再把 continuation 写回 legacy pgmq，而是在同一个 worker job lease 内连续执行 export pass，并在 pass 间 heartbeat；
- `worker_jobs` 模式下，长导出的 seed-scan continuation state 以 `worker_jobs.diagnostics.seed_scan` 为 canonical resume source；当 optional `lca_package_jobs` 缺失或没有可用 seed-scan diagnostics 时，worker 必须从最新匹配 `tidas.export_package` worker job diagnostics 恢复游标；
- 因此 `PACKAGE_WORKER_JOBS_LEASE_SECONDS` 必须大于正常单 pass 时间，长导出仍应依赖 pass 间 heartbeat 续租。

## 9. 权限边界

- 前端不直接写 `lca_package_jobs`
- 前端不直接写 `worker_jobs`
- Edge Functions 负责鉴权、幂等和入队
- worker 负责大包处理、对象存储写入、导入冲突规则执行
- `authenticated` 仅可读自己的 package jobs / artifacts / request cache
- `service_role` 保留完整权限
