---
title: Edge Function Integration Guide
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: zh-CN
whenToUse:
  - 当你需要把 edge-functions 请求稳定映射到 worker 异步求解链路时
  - 当 enqueue、polling、service-role、request_key 或 snapshot 选择规则变化时
  - 当 Edge 需要接入 dataset review-submit gate 的 enqueue/read/rerun/status contract 时
whenToUpdate:
  - 当 edge-facing solve API、入队流程、worker 边界或错误处理约定变化时
  - 当 review-submit gate 的 Edge RPC 边界或 worker runner 结果回写边界变化时
checkPaths:
  - docs/edge-function-integration.md
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/review-submit-fast-gate-contract.md
  - crates/solver-worker/src/review_submit_gate_runner.rs
  - crates/solver-worker/src/worker_jobs.rs
  - crates/solver-worker/src/bin/review_submit_gate_runner.rs
  - crates/**
  - supabase/migrations/**
lastReviewedAt: 2026-07-12
lastReviewedCommit: 9b66c8714fbbe15c7e25418ac963dc2c764ed8e1
lastReviewedNote: "Reviewed for the Edge/Next/Worker static-bundle and 25-method matrix v2 handoff in Issue #116."
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/frontend-integration.md
---

# Edge Function Integration Guide

本文档给 Supabase Edge Functions 项目使用，目标是把前端请求稳定地映射到 worker 异步链路。legacy 路径是 `lca_jobs + pgmq`；统一任务路径是 `worker_jobs(worker_queue=solver)`，result/cache domain truth 由 `worker_jobs`、`lca_results` 与 `lca_result_cache` 承载，`lca_jobs` 仅为 optional historical compatibility。

## 1. 为什么必须走 Edge Function

- 前端不应持有 `service_role`。
- `worker_enqueue_job` / `pgmq.send`、缓存去重、历史 compatibility id 生成都属于受控写操作。
- RLS 已收紧，前端只适合读取自己的 `jobs/results`，不适合写任务。

## 2. 推荐的 Edge API

建议提供以下 API（函数路由名可调整）：

- `POST /lca/solve`
- `GET /lca/jobs/:jobId`
- `GET /lca/results/:resultId`
- `POST /lca/prepare`（管理员/运维）
- `POST /lca/invalidate`（管理员/运维）
- `POST /data-products/lcia-result-packages/build`（data product manager）
- `POST /data-products/lcia-result-packages/:packageId/publish`（data product manager）

## 3. `POST /lca/solve` 输入/输出

### 3.1 请求体（建议）

```json
{
  "scope": "prod",
  "snapshot_id": "optional-uuid",
  "demand_mode": "single",
  "demand": {
    "process_index": 123,
    "amount": 1.0
  },
  "solve": {
    "return_x": true,
    "return_g": true,
    "return_h": true
  }
}
```

全量单位需求模式（不传 `process_index/amount`）：

```json
{
  "scope": "prod",
  "snapshot_id": "optional-uuid",
  "demand_mode": "all_unit",
  "solve": {
    "return_x": false,
    "return_g": false,
    "return_h": true
  },
  "unit_batch_size": 128
}
```

Header 建议：

- `X-Idempotency-Key: <uuid-or-hash>`

### 3.2 响应（建议）

首次入队：

```json
{
  "mode": "queued",
  "job_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "cache_key": "<request_key>"
}
```

命中缓存：

```json
{
  "mode": "cache_hit",
  "result_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "cache_key": "<request_key>"
}
```

命中运行中任务：

```json
{
  "mode": "in_progress",
  "job_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "cache_key": "<request_key>"
}
```

## 4. Edge 端处理流程（强约束）

1. 验证用户 JWT，拿到 `user_id`。
2. 解析请求并标准化（默认 `amount=1.0`，补 `solve` 默认值）。
3. 选择 `snapshot_id`：
   - 若请求显式给出，校验存在且可用。
   - 否则读 `lca_active_snapshots(scope='prod')`。
   - `data_scope=public_plus_owner_draft` 时，Edge 必须使用 `lca.build_snapshot.request.v2` 创建独立 snapshot family：只传 public `state_code=100` 与当前 actor `state_code=0`，并携带 frozen scope manifest、method/factor source contract 和 factor-coverage contract。不能复用 legacy `100..199 + all owner states` snapshot。
4. 构造求解负载：
   - `demand_mode=single`：构造 `rhs`（长度 = `process_count`，只在目标 index 赋值 `amount`）。
   - `demand_mode=all_unit`：构造 `solve_all_unit` payload（不在 Edge 侧生成整块 `rhs_batch`）。
   - 对 `public_plus_owner_draft`，先从 `snapshot-index-v1.json.calculation_evidence` 读取并校验 `lca.calculation_evidence.v2`，再原样写入 `calculation_evidence_binding`；分别使用 `lca.solve_one.request.v2`、`lca.solve_all_unit.request.v2` 或 `lca.contribution_path.request.v2`。证据缺失、scope hash 漂移、static-bundle source/identity hash 非法、25-method matrix 成员或计数不一致、coverage 状态与 gap/artifact 不一致时返回冲突，不入队 v1 fallback。
5. 生成：
   - `request_key`（标准化请求哈希）
   - `idempotency_key`（优先 header，否则退化为 `user_id + request_key`）
6. 在事务中操作：
   - upsert/读取 `lca_result_cache(scope,snapshot_id,request_key)`
   - 若 `ready` 且有 `result_id`，直接返回 `cache_hit`
   - 若 `pending/running` 且有 `job_id`，返回 `in_progress`
   - 否则生成 `lcaJobId` compatibility UUID，并创建或更新 `lca_result_cache(status='pending', job_id=lcaJobId)`
   - legacy pgmq 路径：调用 `public.lca_enqueue_job('lca_jobs', payload)` RPC 入队
   - `worker_jobs` 路径：调用 `public.worker_enqueue_job(...)`，使用 `job_kind=lca.solve_one|lca.solve_batch|lca.solve_all_unit|lca.build_snapshot|lca.contribution_path`、`worker_queue=solver`，并在 payload 中携带 `lcaJobId`
   - 回写 `lca_result_cache.worker_job_id`
7. 返回 `queued`。`worker_jobs` 路径应额外返回 `workerJobId`，供任务中心和 operator 查询使用。

worker 侧以 `worker_jobs` 为任务生命周期事实，并继续推进 domain/cache 表：`lca_result_cache` 从 `pending -> running -> ready`（或失败时 `failed`）。终态写回时会把 `lca_results`、`lca_result_cache`、`lca_latest_all_unit_results`、`lca_factorization_registry` 中可关联的 rows 回填到同一个 `worker_job_id`；optional `lca_jobs` 存在时才做 best-effort retained row 回填。

`public_plus_owner_draft` 是 fail-closed 协议：Edge 负责生产和预校验证据，worker 仍会独立验证 payload、process/flow 数据库行可见性、reviewed static LCIA bundle、snapshot-index evidence 与 solve binding。scope manifest 只覆盖 processes/flows；LCIA 来源是 actor-independent、hash-bound 的 25-method cache bundle。Edge 只能发送固定相对清单路径、最终 raw SHA 和完全相同的 embedded manifest，不能发送 URL。worker 从可信配置的 HTTPS base（或本地验证目录）取文件，验证大小、全部哈希、alias、方法成员和 factor 数值后参与计算。coverage 按 method/exchange pair 统计；任一方法缺 factor 都保持 `incomplete_coverage_not_zero` 和外置 JSONL 证据，不能被 UI 当成“完整的零影响”。

LCIA result package 构建走同一个 `worker_jobs(worker_queue=solver)` 生命周期，但不是普通 `/lca/solve` 请求。Edge 的 data product manager command 应先通过数据库 command 解析权限、published-only eligibility 和默认 impact category，再 enqueue `job_kind=lcia_result.package_build` / `payload_schema_version=lcia_result.package_build.request.v1`。payload 使用数据库返回的 `buildId`、`requestedBy`、`coverageMode`、`inputManifest`、`inputManifestHash`、`eligibleInputCount`、`includedInputCount`、`lciaMethodSet` 和可选 `defaultImpactCategory`；worker 只消费已发布 `stateCode/state_code=100..199` 的 manifest 输入。worker 完成后用 service-role DB 连接调用 `public.cmd_lcia_result_package_mark_ready(...)` 固化 `lcia_result_packages` preview package；发布仍由 Edge manager command 调用数据库 publish RPC 完成。

## 5. 与 worker 的职责边界

Edge：

- 鉴权
- 快速参数校验
- 缓存去重与入队
- 结果读取聚合（可选）

worker：

- 取快照数据
- 分解/求解
- heartbeat `worker_jobs.phase/progress`
- 用 `worker_record_job_result` 写统一任务终态、错误、`result_json` 和 `result_ref`
- 写 domain/cache metadata（如 `lca_results` artifact、`lca_result_cache`），并在 optional `lca_jobs` 存在时 best-effort 写兼容状态；这些都不替代 `worker_jobs` 任务生命周期事实
- 对 `lcia_result.package_build`，构建 published-only snapshot、持久化 all-unit result/query artifacts，并通过 service-role `cmd_lcia_result_package_mark_ready` 标记 package preview ready；失败只写 `worker_jobs` package-specific result，不更新 `lca_result_cache`
- 对 `build_snapshot`，从同一 `worker_jobs` heartbeat diagnostics 投影 resolved snapshot ID 与 calculation evidence；不依赖 optional `lca_jobs`，snapshot reuse 也必须返回真实 resolved ID

不要让 Edge 自己更新 worker lease/result 字段。

## 6. 失败与重试建议

- Edge 入队失败：返回 `5xx`，前端可用同 `X-Idempotency-Key` 重试。
- worker 失败：以 `worker_jobs.status=failed` 和 `error_*` 字段为任务事实；optional `lca_jobs.status=failed` 与 `diagnostics.error` 仅用于兼容诊断，不要求表存在。
- 前端轮询到 `failed` 时，提示用户重试并保留 `job_id` 便于追踪。

## 7. 最小实现清单

- 使用 service role client（仅服务端）。
- 封装 `resolve_snapshot(scope)`。
- 封装 `build_rhs(process_count, process_index, amount)`。
- 封装 `build_solve_all_unit_payload(snapshot_id, solve, unit_batch_size)`。
- 封装 `make_request_key(normalized_input)`。
- 封装 `enqueue_job_and_update_cache(...)` 事务函数；统一任务路径必须创建/复用 `lca_result_cache`，生成 compatibility `lcaJobId`，enqueue `worker_jobs`，并回写 `worker_job_id`。
- 输出统一错误码（如 `BAD_INPUT` / `SNAPSHOT_NOT_READY` / `QUEUE_ERROR`）。

## 8. 不要做的事

- 不要让前端直接写 `lca_jobs`。
- 不要让前端直接调用 `pgmq.send`。
- 不要让前端直接写 `worker_jobs`；统一任务也必须由 Edge/database service-role 边界 enqueue。
- 不要在 Edge Function 同步等待完整求解结果。
- 不要在 Edge Function 中进行重数值计算。

## 9. Review Submit Gate 集成边界

提交审核前的数值稳定性 gate 分成三层；legacy gate-run 表和新的 `worker_jobs` 模式在切流期并存：

- Edge 负责鉴权、请求校验、创建 / 读取 / rerun gate task，并把返回状态透出给 Next。
- Database 负责 `review_submit.submit` root job、`dataset_review_submit_jobs` retained coordinator/history、`worker_jobs` 生命周期、result projection、lease fencing 和发布前断言。
- worker `review_submit_gate_runner` legacy 模式负责领取 queued gate run，并通过 `cmd_dataset_review_submit_gate_record_result` 写入 `passed`、`blocked` 或 `error`。
- worker `review_submit_gate_runner --worker-jobs` 模式负责领取 `worker_queue=review_submit_gate` 的 child `review_submit.gate` job，并通过 `worker_record_job_result` 写入 `completed`、`blocked` 或 `failed`。worker 不 claim `review_submit.submit` root job；root job 由 DB/Edge coordinator 随 `dataset_review_submit_jobs` 推进。

Edge 不应执行 snapshot builder、provider scan、sparse factorization probe 或 targeted RHS solve。Edge 也不应直接更新 `dataset_review_submit_gate_runs.calculator_report`；结果写入只能由 worker runner 使用 service-role DB 连接完成。

worker_jobs enqueue payload 只需要表达 dataset revision 与可选诊断 checksum：

```json
{
  "datasetRevision": {
    "table": "processes",
    "id": "<process uuid>",
    "version": "01.00.000",
    "revisionChecksum": "optional diagnostic checksum"
  }
}
```

worker runtime 会从 `processes.json_ordered` 计算权威 checksum，并在 worker job result 的 `datasetRevision.revisionChecksum` 返回。Edge 不应把浏览器端 checksum 当作权威值。

状态语义：

- `queued` / `running`：Edge 返回待处理，Next 继续轮询。
- `passed`：提交审核可继续调用发布 / 审核流的后续 RPC。
- `blocked`：数据修复问题，Next 应展示 `blockingReasons` 和 `calculatorReport.blockers`。
- `error`：runner、artifact、DB 可见性或部署问题，Next 应提示稍后重试或联系运维。
- `stale`：旧 gate run 被新的 ensure/rerun 替代，Edge/Next 应读取最新 gate run。

部署上，`review_submit_gate_runner` 需要与 solver worker 相同的 DB 和 S3 artifact 环境变量。常驻运行时使用 `REVIEW_SUBMIT_GATE_POLL_MS` 轮询；运维和 CI smoke 可使用 `--once` 处理一条后退出。

worker_jobs 模式部署时增加 `--worker-jobs` 或 `REVIEW_SUBMIT_GATE_WORKER_JOBS=true`。`REVIEW_SUBMIT_GATE_WORKER_ID` 用于 operator 诊断；`REVIEW_SUBMIT_GATE_WORKER_LEASE_SECONDS` 默认 `900`，必须大于一次 heartbeat 间隔和常见 snapshot build 阶段耗时。
