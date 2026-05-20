---
title: TIDAS Package Async Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: calculator
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
lastReviewedAt: 2026-05-20
lastReviewedCommit: f7c7d97e64dab987631281c3835eb7d2a343b94a
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
---

# TIDAS Package Async Contract

本文档定义 TIDAS 数据包异步导入/导出在 `tiangong-lca-calculator` 中的 worker、表结构与 artifact 契约。

## 1. 目标

- 将完整 ZIP 导入/导出从同步 edge function 挪到异步 worker。
- 复用当前 `pgmq + object storage + job status` 模式。
- 避免把 `snapshot_id` 语义强行塞进 package 任务。

## 2. 为什么不复用 `lca_jobs`

`lca_jobs` / `lca_results` 当前强绑定 `snapshot_id` 和数值求解语义，见：

- `public.lca_jobs.snapshot_id uuid NOT NULL`
- `public.lca_results.snapshot_id uuid NOT NULL`
- `job_type` 与 `artifact_format` 也都围绕求解/快照设计

因此 package worker 复用的是“异步模式”，不是“同一张运行时表”。

## 3. 关键表

- `lca_package_jobs`
  - package 导入/导出主任务表
- `lca_package_artifacts`
  - import 源 ZIP、export ZIP、import/export report 的 artifact 元数据
- `lca_package_request_cache`
  - 按用户 + 操作 + request key 做去重与状态追踪

## 4. 队列与 RPC

- `pgmq` queue: `lca_package_jobs`
- enqueue RPC: `public.lca_package_enqueue_job(jsonb)`
- 仅 `service_role` 可执行

## 5. 任务类型

`lca_package_jobs.job_type` 与 worker payload `type` 必须一致：

- `export_package`
- `import_package`

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

### 7.1 import report payload（新增字段）

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

`lca_package_jobs.status`：

- `queued`
- `running`
- `ready`
- `completed`
- `failed`
- `stale`

当前建议语义：

- `export_package`: `queued -> running -> ready`
- `import_package`: `queued -> running -> completed`
- 失败统一落 `failed`

## 9. 权限边界

- 前端不直接写 `lca_package_jobs`
- Edge Functions 负责鉴权、幂等和入队
- worker 负责大包处理、对象存储写入、导入冲突规则执行
- `authenticated` 仅可读自己的 package jobs / artifacts / request cache
- `service_role` 保留完整权限
