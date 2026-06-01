---
title: Tiangong LCA Calculator Landing
docType: overview
scope: repo
status: active
authoritative: false
owner: calculator
language: zh-CN
whenToUse:
  - 当你需要这个仓库最短的高层说明时
  - 当你刚进入仓库但暂时不需要完整 AI contract surface 时
whenToUpdate:
  - 当仓库定位、运行时概览、对接文档入口或 AI entry surface 发生变化时
checkPaths:
  - README.md
  - AGENTS.md
  - .env.example
  - .docpact/config.yaml
  - docs/agents/**
  - docs/lca-api-contract.md
  - docs/review-submit-fast-gate-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
  - docs/tidas-package-contract.md
lastReviewedAt: 2026-06-01
lastReviewedCommit: cc31672ee15d1769b4e8aa7e2e0b516128dd920f
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
  - docs/lca-api-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
  - docs/tidas-package-contract.md
---

# Tiangong LCA Calculator

面向 Supabase + Rust + SuiteSparse 的大规模 LCA 稀疏求解服务。

## AI Docs Entry

面向 AI 的 checked-in contract layer 从这里开始：

1. `AGENTS.md`
2. `.docpact/config.yaml`
3. `docs/agents/repo-validation.md`
4. `docs/agents/repo-architecture.md`
5. 再按任务加载对应窄契约：
   - `docs/lca-api-contract.md`
   - `docs/review-submit-fast-gate-contract.md`
   - `docs/edge-function-integration.md`
   - `docs/frontend-integration.md`
   - `docs/tidas-package-contract.md`

这些文件构成低熵入口面：repo ownership、routing、validation、runtime boundary 先读这里；`README.md` 继续承担更长的运行时概览和操作说明。

## 0. 对接文档（给 Edge / 前端）

- Edge Function 对接：`docs/edge-function-integration.md`
- 前端对接：`docs/frontend-integration.md`
- 统一契约（jobs/results/payload/status）：`docs/lca-api-contract.md`
- TIDAS package 异步契约：`docs/tidas-package-contract.md`

## 1. 架构定位

- Supabase: 业务数据、鉴权、Edge Functions 编排、`pgmq` 队列。
- Rust Solver Worker: 构建稀疏矩阵、缓存分解、重复回代、写回结果。
- SuiteSparse (UMFPACK): 稀疏线性系统求解核心。

核心不变：

- 只解 `M x = y`，其中 `M = I - A`
- 不求显式逆矩阵
- 重计算只走异步 worker，不走前端/同步请求

## 2. 当前实现状态

- 已接入 `snapshot_id` 语义（全库 process 网络）。
- 已支持作业类型：
  - `prepare_factorization`
  - `solve_one`
  - `solve_batch`
  - `solve_all_unit`
  - `invalidate_factorization`
  - `rebuild_factorization`
- 已完成 additive schema：
  - `lca_jobs` / `lca_results`（作业与结果）
  - `lca_network_snapshots`（snapshot 元信息）
  - `lca_snapshot_artifacts`（矩阵 artifact 元信息）
- 已切换为结果 S3-only：
  - 所有 `solve` 结果统一上传对象存储（HDF5）
  - `lca_results` 仅存 artifact 元数据 + diagnostics（不存 inline payload）
- 已支持 snapshot artifact-first：
  - builder 直接生成 `M/B/C` 并上传 `HDF5`
  - worker 优先从 `lca_snapshot_artifacts` 下载 artifact，失败才回退到旧 `lca_*_entries` 读取
- 已支持 review-submit gate：
  - `review_submit_gate` 可对文件输入产出 `review_submit_gate_report.v1`
  - `review_submit_gate_runner` 可领取数据库中的 submit-review gate run，并写回 `passed` / `blocked` / `error`

## 3. 结果文件格式（已选定）

对象存储中的大结果采用：

- 容器：`HDF5`
- 格式标识：`hdf5:v1`
- 文件后缀：`.h5`
- 哈希：`SHA-256`
- 压缩：`HDF5 deflate`（内置 zlib，level=4，chunked dataset）

说明：

- 压缩作用在 `envelope_json` dataset（不是额外包一层 `.gz`）
- `hdf5:v1` / `snapshot-hdf5:v1` 的读写接口保持不变，读取端会透明解压

worker 上传 artifact 到 S3 兼容存储，并在 `lca_results` 中写入：

- `artifact_url`
- `artifact_sha256`
- `artifact_byte_size`
- `artifact_format`

## 4. 数据库迁移

已提供 migration：

- `supabase/migrations/20260304073000_lca_snapshot_phase1.sql`
- `supabase/migrations/20260304103000_lca_snapshot_artifacts.sql`
- `supabase/migrations/20260304120000_lca_drop_legacy_entry_tables.sql`（清理旧 `lca_*_entries/index` 表）
- `supabase/migrations/20260305052000_lca_request_cache_and_factorization_registry.sql`（additive-only：active snapshot + cache + factorization registry + jobs 幂等列）
- `supabase/migrations/20260305070000_lca_rls_lockdown.sql`（启用 RLS + 收紧 anon/authenticated 权限）
- `supabase/migrations/20260305093000_lca_enqueue_job_rpc.sql`（新增 `public.lca_enqueue_job` RPC，供 Edge Functions 通过 supabase.rpc 入队）
- `supabase/migrations/20260305094000_lca_enqueue_job_rpc_acl.sql`（收紧 RPC 权限，仅 `service_role` 可执行）
- `supabase/migrations/20260306090000_lca_results_s3_strict_and_retention.sql`（破坏性：清理旧结果并切换为 S3-only + retention 字段）
- `supabase/migrations/20260308104000_lca_jobs_add_solve_all_unit.sql`（扩展 `lca_jobs.job_type` 约束，支持 `solve_all_unit`）
- `supabase/migrations/20260309042000_lca_latest_all_unit_results.sql`（新增 snapshot 级 latest all-unit 查询指针表）
- `supabase/migrations/20260319120000_tidas_package_job_tables.sql`（新增 `lca_package_*` 异步 job/artifact/cache 表、队列、RPC 和 RLS，用于 TIDAS package）

对已有业务源表（`processes/flows/lciamethods/...`）不做修改。
其中 `20260304120000` 会删除旧的 `lca_*_entries/index` 中间表，只保留 artifact-first 所需表。
其中 `20260305052000` 只新增缓存/幂等相关结构，运行时主路径暂未强依赖这些新表。
其中 `20260305070000` 为安全基线：不再允许 `anon` 对 `lca_*` 表读写；`authenticated` 默认只可读取“自己的 jobs/results”。
其中 `20260305093000` 增加 `public.lca_enqueue_job(text,jsonb)`（`security definer`），用于 Edge Functions 在不直连 postgres 客户端的前提下调用 `pgmq.send`。
其中 `20260305094000` 收紧该 RPC 的执行权限，确保只有 `service_role` 可以调用。

可先做静态检查：

```bash
./scripts/validate_additive_migration.sh supabase/migrations/20260304073000_lca_snapshot_phase1.sql
./scripts/validate_additive_migration.sh supabase/migrations/20260304103000_lca_snapshot_artifacts.sql
./scripts/validate_additive_migration.sh supabase/migrations/20260305052000_lca_request_cache_and_factorization_registry.sql
./scripts/validate_additive_migration.sh supabase/migrations/20260305070000_lca_rls_lockdown.sql
./scripts/validate_additive_migration.sh supabase/migrations/20260305093000_lca_enqueue_job_rpc.sql
./scripts/validate_additive_migration.sh supabase/migrations/20260305094000_lca_enqueue_job_rpc_acl.sql
./scripts/validate_additive_migration.sh supabase/migrations/20260306090000_lca_results_s3_strict_and_retention.sql
./scripts/validate_additive_migration.sh supabase/migrations/20260308104000_lca_jobs_add_solve_all_unit.sql
./scripts/validate_additive_migration.sh supabase/migrations/20260309042000_lca_latest_all_unit_results.sql
```

执行迁移：

```bash
set -a && source .env && set +a
psql "$CONN" -v ON_ERROR_STOP=1 -f supabase/migrations/20260304073000_lca_snapshot_phase1.sql
psql "$CONN" -v ON_ERROR_STOP=1 -f supabase/migrations/20260304103000_lca_snapshot_artifacts.sql
psql "$CONN" -v ON_ERROR_STOP=1 -f supabase/migrations/20260304120000_lca_drop_legacy_entry_tables.sql
psql "$CONN" -v ON_ERROR_STOP=1 -f supabase/migrations/20260305052000_lca_request_cache_and_factorization_registry.sql
psql "$CONN" -v ON_ERROR_STOP=1 -f supabase/migrations/20260305070000_lca_rls_lockdown.sql
psql "$CONN" -v ON_ERROR_STOP=1 -f supabase/migrations/20260305093000_lca_enqueue_job_rpc.sql
psql "$CONN" -v ON_ERROR_STOP=1 -f supabase/migrations/20260305094000_lca_enqueue_job_rpc_acl.sql
psql "$CONN" -v ON_ERROR_STOP=1 -f supabase/migrations/20260306090000_lca_results_s3_strict_and_retention.sql
psql "$CONN" -v ON_ERROR_STOP=1 -f supabase/migrations/20260308104000_lca_jobs_add_solve_all_unit.sql
psql "$CONN" -v ON_ERROR_STOP=1 -f supabase/migrations/20260309042000_lca_latest_all_unit_results.sql
```

### 4.0.1 访问控制基线（RLS）

`20260305070000_lca_rls_lockdown.sql` 生效后：

- `lca_*` 表全部启用 RLS。
- `anon`：无表级权限。
- `authenticated`：仅可 `SELECT` 自己的 `lca_jobs` 与其关联的 `lca_results`。
- `service_role`：保留完整权限（供 Edge Functions / worker 使用）。

### 4.1 构建可计算 snapshot（artifact-first）

快速生成一个可计算 snapshot：

```bash
./scripts/build_snapshot_from_ilcd.sh
```

默认就是“全库正式版”：

- 默认纳入 `state_code=100~199`（`--process-states` 默认覆盖 `100,101,...,199`）
- 不限 process 数量（`--process-limit` 默认 `0`，即 no limit）
- 生成 coverage 报表到 `reports/snapshot-coverage/<snapshot_id>.{json,md}`
- 报表包含：匹配率、奇异风险、矩阵规模、build 分阶段耗时
- 矩阵 artifact 直接写入 S3（`snapshot-hdf5:v1`，HDF5 deflate 压缩）

常用参数：

- `--process-limit 100`：先做小样本调试 snapshot（正式跑不要加）
- `--process-states all`：取消 `state_code` 过滤，按所有 `processes` 构建 snapshot
- `--include-user-id <uuid>`：在 `process_states` 过滤基础上，额外包含该 `user_id` 的 process（并集）
- `--root-process <uuid@version>`：显式给出一个或多个 request roots，只构建从这些 roots 可达的 public+private process 闭包
- `--no-lcia`：先不构建 C 矩阵（只跑到 LCI）
- `--method-id <uuid> --method-version <ver>`：指定 LCIA 方法
- `--self-loop-cutoff 0.999999`：过滤会导致 `M = I - A` 奇异的对角自环（`|A_ii|` 过大）
- `--report-dir <path>`：指定 coverage 报表输出目录

`--root-process` 模式说明：

- roots 语法是 `<process_id>@<version>`，可重复传多个
- builder 先按 `process_states` / `include_user_id` 取候选集，再从 roots 出发按当前 `provider_rule` 解析可达 public+private process 闭包
- snapshot 元数据会记录：
  - `selection_mode`
  - `request_roots`
  - `scope_hash`
  - resolved scope 的 public/private process 数量
- `--root-process` 与 `--process-limit` 不能同时使用；前者要求闭包完整，后者会破坏 scope 语义

脚本行为：

- 从 `processes/flows/lciamethods` 构建 `A/B/C`（内存）
- 上传 snapshot artifact 到 S3（HDF5）
- 只写 metadata 到：
  - `lca_network_snapshots`
  - `lca_snapshot_artifacts`
- 不修改原始 `processes/flows/lciamethods` 数据
- 不再要求写入大体量 `lca_*_entries` 表
- 默认启用“同源跳过重建”：
  - 基于 `processes/flows/lciamethods` 的 `count(*) + max(modified_at)` 和构建参数计算 fingerprint
  - 若命中已有 `ready` snapshot artifact，则直接复用并秒级返回
  - 若传了 `--snapshot-id`，会按该 ID 执行构建（不走自动复用）
- request-root 模式下，process source summary 会按 resolved closure 收窄，而不是继续按整个 broad candidate scope 统计
- 冷构建优化：
  - flow 元数据按候选 `id` 查询（避免全表扫 `flows`）
  - process JSON 解析使用并行分片

建议调试流程：

```bash
# 1) 先构建一个小样本可计算 snapshot
./scripts/build_snapshot_from_ilcd.sh --process-limit 100

# 2) 用返回的 snapshot_id 跑 prepare + solve + 结果写回，并记录日志
./scripts/run_full_compute_debug.sh --snapshot-id <snapshot_id>
```

当前默认 provider rule：

- `split_by_process_volume`
- 单 provider case 仍然直接按唯一 provider 写入 `A`
- multi-provider case 先按 local-first geography tier 选择最优非空 provider 层级，再在该层级内按 process annual volume 归一化分配
- annual volume 缺失、非法或非正时，该 provider 的 raw weight 使用 `1.0`
- 如需强制唯一 provider 模式，可显式传 `--provider-rule strict_unique_provider`

### 4.1.1 导出 provider link 问题 process 诊断 Excel

```bash
./scripts/export_provider_link_diagnostics.sh
```

默认输出到：

- `reports/provider-link-diagnostics/provider_link_problem_processes_<timestamp>.xlsx`

常用参数：

- `--report-dir <path>`：指定输出目录
- `--output <path.xlsx>`：指定完整输出文件路径
- `--filename-prefix <name>`：自定义文件名前缀

输出内容包括：

- `summary`：统计汇总、中文 tag 说明、关键字段说明
- `service_loop_candidates`：同 flow input/output 金额完全相等的可疑 service-loop process
- `pn_pm_candidates`：PN / PM0.2 / particle 相关的可疑 process

## 5. 环境变量

可从 `.env.example` 复制模板到 `.env`、`.env.dev` 或 systemd `EnvironmentFile`，再填入真实连接串和密钥。`.env.example` 只应保留占位值和安全默认值。

最小必需：

- `DATABASE_URL` 或 `CONN`
- `QUEUE_DATABASE_URL` 或 `QUEUE_CONN`（可选；仅用于 pgmq read/archive，未设置时复用 `DATABASE_URL` / `CONN`）
- `PGMQ_QUEUE`（默认 `lca_jobs`）
- `SOLVER_MODE`（`worker` / `http` / `both`）
- `HTTP_ADDR`（默认 `0.0.0.0:8080`）
- `WORKER_POLL_MS`（默认 `1000`）
- `WORKER_VT_SECONDS`（默认 `30`；生产 `build_snapshot` 队列建议按最长任务耗时设置，例如 `1800`）

数据库连接池与 `build_snapshot` 并发：

- `DB_MAX_CONNECTIONS`（worker 进程连接池上限，默认 `8`）
- `DB_MIN_CONNECTIONS`（worker 进程连接池保留连接数，默认 `1`）
- `DB_ACQUIRE_TIMEOUT_SECONDS`（worker 获取连接超时，默认 `30`）
- `QUEUE_DB_MAX_CONNECTIONS`（队列轮询连接池上限，默认 `2`）
- `QUEUE_DB_MIN_CONNECTIONS`（队列轮询连接池保留连接数，默认 `0`）
- `QUEUE_DB_ACQUIRE_TIMEOUT_SECONDS`（队列轮询获取连接超时，默认 `30`）
- `BUILD_SNAPSHOT_MAX_CONCURRENCY`（跨 worker 实例的 `build_snapshot` 并发上限，默认 `1`）
- `BUILD_SNAPSHOT_LOCK_POLL_MS`（等待 `build_snapshot` 并发槽位时的轮询间隔，默认 `5000`）
- `SNAPSHOT_BUILDER_DB_MAX_CONNECTIONS`（`snapshot_builder` 子进程连接池上限，默认 `4`）
- `SNAPSHOT_BUILDER_DB_ACQUIRE_TIMEOUT_SECONDS`（`snapshot_builder` 获取连接超时，默认 `30`）
- `REVIEW_SUBMIT_GATE_POLL_MS`（review-submit gate runner 轮询间隔，默认 `1000`）
- `REVIEW_SUBMIT_GATE_MAX_RUNS`（可选；设置后 runner 处理指定条数后退出）
- `REVIEW_SUBMIT_GATE_STALE_RUNNING_SECONDS`（runner 重新领取 stale `running` gate run 的阈值，默认 `21600`）

Supabase 连接说明：

- worker / package worker 支持双连接池：`DATABASE_URL` / `CONN` 用于 solver、package、snapshot builder 与结果写入等主业务查询；`QUEUE_DATABASE_URL` / `QUEUE_CONN` 仅用于 pgmq polling / archive。
- 推荐生产配置：主业务连接保留在 session/direct 连接或 session pooler；`QUEUE_DATABASE_URL` 使用 Supabase transaction pooler（通常是 `:6543`）。
- 运行时 SQLx 查询使用非持久 prepared statement，以避免后端复用导致 `sqlx_s_*` 语句名冲突；高频 pgmq polling / archive 操作使用 `raw_sql` 简单查询协议与受限队列名字面量，避免 6543 transaction pooler 不支持 prepared statement 协议导致空轮询失败。
- `build_snapshot` 全局并发控制使用 transaction-level advisory lock，适配 transaction pooler；生产环境仍建议保持 `BUILD_SNAPSHOT_MAX_CONCURRENCY=1`。

对象存储（snapshot builder / solver-worker / result_gc 必需；`package_gc --execute` 删除 package artifact 对象时也必需）：

- `S3_ENDPOINT`
- `S3_REGION`
- `S3_BUCKET`
- `S3_ACCESS_KEY_ID`
- `S3_SECRET_ACCESS_KEY`
- `S3_SESSION_TOKEN`（可选，临时凭证时使用）
- `S3_PREFIX`（默认 `lca-results`）

说明：结果持久化已改为 S3-only。`S3_ENDPOINT/S3_REGION/S3_BUCKET/S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY` 必须同时提供。上传请求使用 SigV4 签名认证。

## 6. 启动与检查

Ubuntu 依赖：

```bash
sudo apt-get update
sudo apt-get install -y libsuitesparse-dev libopenblas-dev liblapack-dev pkg-config cmake
```

macOS (Homebrew) 依赖：

```bash
brew install cmake suite-sparse
export PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig
export LIBRARY_PATH=/opt/homebrew/lib
```

说明：`HDF5` 通过 `hdf5-sys(static,zlib)` 在编译期构建，因此需要本机可用 `cmake`。

质量检查：

```bash
make check
```

全量链路调试（prepare + solve + 结果写回 + 日志落盘）：

```bash
./scripts/run_full_compute_debug.sh --snapshot-id <your-snapshot-uuid>
```

说明：

- 脚本会启动 `solver-worker`（queue 模式）、投递 `prepare_factorization` 和 `solve_one` 两个 job、轮询状态并打印诊断。
- 日志默认写到 `logs/full-run/`，包含：
  - `run-<ts>.log`（执行过程）
  - `worker-<ts>.log`（worker 详细日志）
- 报表默认写到 `reports/full-run/`，包含：
  - `run-<ts>.json`（结构化结果 + 阶段耗时 + `result.compute_timing_sec` + `result.persistence_timing_sec`）
  - `run-<ts>.md`（便于人工查看）
- 计时精度：
  - 本地编排计时为纳秒采样、秒小数输出（6 位）
  - 同时写入数据库作业计时 `job_timing_sec`（`queue_wait/run/end_to_end`）
- 若不传 `--snapshot-id`，会自动选最新 snapshot。
- 脚本会优先读取 `lca_snapshot_artifacts` 的矩阵规模；若不存在则回退读取旧 `lca_*_entries`。
- 结果固定走 HDF5 + 对象存储（S3-only，无 inline payload fallback）

### 6.1 Brightway25 手动校验（默认不触发）

已引入独立校验工具：`tools/bw25-validator`（`brightway25==1.1.1`）。

设计约束：

- 不参与 worker 主链路
- 不自动随 `prepare/solve` 执行
- 仅手动触发，用于数值交叉验证

手动运行：

```bash
./scripts/run_bw25_validation.sh --snapshot-id <snapshot_id>
```

按作业类型选择（`--snapshot-id` 或默认最新时生效，默认 `solve_one`）：

```bash
./scripts/run_bw25_validation.sh --snapshot-id <snapshot_id> --job-type solve_one
./scripts/run_bw25_validation.sh --snapshot-id <snapshot_id> --job-type solve_all_unit
```

可选指定目标：

```bash
./scripts/run_bw25_validation.sh --result-id <result_uuid>
./scripts/run_bw25_validation.sh --job-id <job_uuid>
```

`solve_all_unit` 可选做抽样校验（只校验前 N 个 process）：

```bash
./scripts/run_bw25_validation.sh --result-id <result_uuid> --all-unit-max-processes 200
```

输出：

- `reports/bw25-validation/<result_id>.json`
- `reports/bw25-validation/<result_id>.md`

校验内容：

- Brightway 重建 `M` 并求 `x`
- 对比 Rust 的 `x/g/h`
  - `solve_one`：单个 `rhs`
  - `solve_all_unit`：按 unit demand `e_i` 逐过程重算，并输出聚合后的最坏误差
- 记录残差与阈值判断（`atol/rtol`）
- 输出速度对比（优先比较“可比计算时间”）：
  - Rust：`solve_mx_sec + bx_sec + cg_sec`（来自 `lca_results.diagnostics.compute_timing_sec`）
  - Brightway：`solve_sec` / `build_plus_solve_sec`
  - 同时保留 `rust_job_run_sec`（含持久化与上传）供端到端参考
- 输出 Rust 持久化拆分耗时（来自 `lca_results.diagnostics.persistence_timing_sec`）：
  - `encode_artifact_sec`
  - `upload_artifact_sec`
  - `db_write_sec`
  - `total_sec`

性能说明（x64 Linux）：

- 校验工具默认安装 `pypardiso`（`pypardiso>=0.4.6`）
- 用于消除 Brightway 在 AMD/Intel x64 上的“未安装 pypardiso”警告并提升线性求解速度

启动服务：

```bash
set -a && source .env && set +a
cargo run -p solver-worker --bin solver-worker --release -- --mode worker
```

启动 TIDAS package 导入导出 worker：

```bash
set -a && source .env && set +a
cargo run -p solver-worker --bin package_worker --release -- --pgmq-queue lca_package_jobs --worker-vt-seconds 600 --worker-poll-ms 300
```

启动 review-submit gate runner：

```bash
set -a && source .env && set +a
cargo run -p solver-worker --bin review_submit_gate_runner --release --
```

说明：

- `solver-worker` 消费 `lca_jobs`，处理 `prepare_factorization` / `solve_one` / `solve_all_unit` 等计算任务。
- `package_worker` 消费 `lca_package_jobs`，处理前端 TIDAS package 导出/导入异步任务。
- `review_submit_gate_runner` 消费数据库表 `dataset_review_submit_gate_runs` 中的 gate run，执行 request-root snapshot + calculator gate，并通过数据库 RPC 写回结果。

### 6.2 计算正确性基线流程（Expected 对比）

`bw25-validator` 适合做“同一 snapshot 下的数值交叉验证”；若要在数据反复变更后持续验收，建议使用“手动 expected 基线 + API 对比”流程。

核心脚本：

- `scripts/generate_lcia_expected.sh`：从 snapshot artifact 直接解方程，生成 `expected` TSV。
- `scripts/validate_lcia_targets.sh`：调用 `lca_query_results`，将实际值与 `expected` TSV 对比。

#### 6.2.1 准备 process 列表

创建一个仅含 `process_id` 的文件（可带表头，第一列必须是 `process_id`）：

```tsv
process_id
<uuid-1>
<uuid-2>
...
```

#### 6.2.2 生成 expected 基线

默认 impact 是 GWP（`6209b35f-9447-40b5-b68c-a1099e3674a0`）。可通过 `--impact-id` 覆盖。

```bash
./scripts/generate_lcia_expected.sh \
  --snapshot-id <snapshot_id> \
  --process-ids-file reports/lcia-targets/<process-list>.tsv \
  --output reports/lcia-targets/<expected>.tsv \
  --include-process-name
```

输出列：

- `process_id`
- `expected_value`
- `abs_tol`
- `process_index`
- `direct_value`
- `indirect_value`
- `process_name`（仅 `--include-process-name`）

#### 6.2.3 对比当前 API 结果

```bash
USER_API_KEY=<base64-email-password> \
./scripts/validate_lcia_targets.sh \
  --snapshot-id <snapshot_id> \
  --expected reports/lcia-targets/<expected>.tsv \
  --out reports/lcia-targets/<compare>.tsv
```

通过标准：

- 全部 `pass=true`
- 任一条超出 `abs_tol` 则脚本退出码为 `3`

#### 6.2.4 推荐复用方式

当 process/flow/lcia 数据变更后，重复以下三步：

1. 重新构建 snapshot（建议记录新的 `snapshot_id`）。
2. 用 `generate_lcia_expected.sh` 生成新的 expected 文件（建议带日期后缀）。
3. 用 `validate_lcia_targets.sh` 对比并留存 compare 报告。

说明：

- 如果 expected 来自当前 snapshot 的手动求解，它是“回归基线验证”（保证实现一致性）。
- 如果 expected 来自外部审定结果，它可用于“绝对准确性验证”。

#### 6.2.5 导出最新矩阵（A/B/M + 单一 LCIA）

若要查看“当前最新 `solve_all_unit` 对应 snapshot”的矩阵，可使用：

```bash
./scripts/export_latest_matrices.sh
```

默认行为：

- 导出 `A`、`B`、`M=I-A` 三个矩阵（triplets TSV）
- 仅导出一个 impact 的 `C` 行和 `H` 列向量（默认 GWP: `6209b35f-9447-40b5-b68c-a1099e3674a0`）
- 输出到 `reports/result-matrices/`

常用参数：

```bash
# 指定 impact（例如 climate change / GWP）
./scripts/export_latest_matrices.sh --impact-id 6209b35f-9447-40b5-b68c-a1099e3674a0

# 指定 snapshot（会选该 snapshot 下最新 solve_all_unit 结果）
./scripts/export_latest_matrices.sh --snapshot-id <snapshot_id>

# 直接指定 result
./scripts/export_latest_matrices.sh --result-id <result_id>
```

### 6.3 生产常驻（systemd，推荐）

`cargo run` 适合开发调试。生产环境建议使用 `systemd` 托管 `release` 二进制（开机自启、崩溃自恢复、统一日志）。

构建：

```bash
cd /home/ubuntu/projects/lca_workspace/tiangong-lca-calculator
cargo build -p solver-worker --bin solver-worker --release
```

创建服务模板 `/etc/systemd/system/solver-worker@.service`：

```ini
[Unit]
Description=TianGong LCA Solver Worker %i
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator
EnvironmentFile=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator/.env
Environment=RUST_LOG=info
ExecStart=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator/target/release/solver-worker --mode worker --worker-vt-seconds 1800 --worker-poll-ms 300
Restart=always
RestartSec=2
TimeoutStopSec=30
LimitNOFILE=65535
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

加载并启动（示例：2 个实例）：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now solver-worker@1 solver-worker@2
```

查看状态与日志：

```bash
systemctl status solver-worker@1 solver-worker@2 --no-pager
journalctl -u solver-worker@1 -f
journalctl -u solver-worker@2 -f
```

更新二进制后重启：

```bash
sudo systemctl restart solver-worker@1 solver-worker@2
```

建议：

- 先从 2 个 worker 实例开始，再根据队列积压和 CPU 使用率调整。
- `WORKER_VT_SECONDS` 需要大于“等待 `build_snapshot` 并发槽位 + 实际构建”的最慢耗时，避免消息重复消费。
- 多台机器共享同一个数据库队列时，生产环境建议保持 `BUILD_SNAPSHOT_MAX_CONCURRENCY=1`，用全局 advisory lock 串行化全量快照构建；普通 solve 类任务仍可由多个 worker 实例并行消费。
- 如 Supabase 连接数紧张，优先调低每个 worker 的 `DB_MAX_CONNECTIONS`，再结合 `SNAPSHOT_BUILDER_DB_MAX_CONNECTIONS` 控制全量构建期间的连接峰值。

### 6.4 TIDAS Package Worker 常驻（systemd，推荐）

若前端需要异步导入/导出 TIDAS package，建议单独用 `systemd` 托管 `package_worker`，避免与 `solver-worker` 共用同一进程。

构建：

```bash
cd /home/ubuntu/projects/lca_workspace/tiangong-lca-calculator
cargo build -p solver-worker --bin package_worker --release
```

创建服务文件 `/etc/systemd/system/package-worker.service`：

```ini
[Unit]
Description=TianGong LCA Package Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator
EnvironmentFile=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator/.env
Environment=RUST_LOG=info
ExecStart=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator/target/release/package_worker --pgmq-queue lca_package_jobs --worker-vt-seconds 600 --worker-poll-ms 300
Restart=always
RestartSec=2
TimeoutStopSec=30
LimitNOFILE=65535
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

加载并启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now package-worker.service
```

查看状态与日志：

```bash
systemctl status package-worker.service --no-pager
journalctl -u package-worker.service -f
```

更新二进制后重启：

```bash
sudo systemctl restart package-worker.service
```

### 6.5 Maintenance Worker Jobs（systemd，推荐）

`worker_jobs` 模式下，GC timer/operator action 不再直接代表任务事实；它们只 enqueue `worker_queue=maintenance` job。常驻 `maintenance_worker` 负责 claim job、调用现有 GC binary、写回 summary 和 operator-only report artifact metadata。

构建：

```bash
cd /home/ubuntu/projects/lca_workspace/tiangong-lca-calculator
cargo build -p solver-worker --bin maintenance_worker --bin maintenance_enqueue --bin package_gc --bin snapshot_gc --bin result_gc --release
```

创建常驻 worker 服务 `/etc/systemd/system/maintenance-worker.service`：

```ini
[Unit]
Description=TianGong LCA Maintenance Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator
EnvironmentFile=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator/.env
Environment=RUST_LOG=info
Environment=MAINTENANCE_JOB_ENVIRONMENT=main
ExecStart=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator/target/release/maintenance_worker
Restart=always
RestartSec=2
TimeoutStopSec=30
LimitNOFILE=65535
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

启用：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now maintenance-worker.service
```

手动 enqueue dry-run 示例：

```bash
target/release/maintenance_enqueue snapshot-gc --environment main --batch-size 50
target/release/maintenance_enqueue result-gc --environment main --batch-size 100 --max-batches 1
target/release/maintenance_enqueue package-artifact-gc --environment main --batch-size 100 --max-batches 1
```

destructive execute 必须显式传 `--execute`。`maintenance_enqueue` 会为 dry-run / execute 生成不同的 idempotency/concurrency key；execute 默认 `max_attempts=1`。

### 6.6 TIDAS Package Artifact GC（systemd timer，推荐）

`package_gc` 负责清理 package artifact 对象、过期 request cache 和已无 artifact 依赖的 terminal package job metadata。统一队列模式下，timer 只 enqueue `tidas.package_artifact_gc` job，实际执行由 `maintenance_worker` 调用 `package_gc`。

- 所有活跃 calculator worker 主机都构建并保留最新 `target/release/package_gc` 和 `target/release/maintenance_worker`，便于切换。
- `package-gc.timer` 只负责调用 `maintenance_enqueue package-artifact-gc` enqueue worker job，不直接执行删除。
- 首次启用必须保持 dry-run，确认 `worker_jobs.result_json.summary` 与 operator report 后，再把 timer 切到 `--execute`。

构建：

```bash
cd /home/ubuntu/projects/lca_workspace/tiangong-lca-calculator
cargo build -p solver-worker --bin maintenance_enqueue --bin maintenance_worker --bin package_gc --release
```

创建 dry-run 服务文件 `/etc/systemd/system/package-gc.service`：

```ini
[Unit]
Description=TianGong LCA Package Artifact GC (dry-run)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator
EnvironmentFile=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator/.env
Environment=RUST_LOG=info
Environment=MAINTENANCE_JOB_ENVIRONMENT=main
SyslogIdentifier=maintenance_enqueue
ExecStart=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator/target/release/maintenance_enqueue package-artifact-gc --batch-size 100 --max-batches 1
```

创建 timer 文件 `/etc/systemd/system/package-gc.timer`：

```ini
[Unit]
Description=Daily TianGong LCA Package Artifact GC dry-run

[Timer]
OnCalendar=*-*-* 03:15:00
RandomizedDelaySec=15m
Persistent=true
Unit=package-gc.service

[Install]
WantedBy=timers.target
```

启用 timer 并立即跑一次 dry-run：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now package-gc.timer
sudo systemctl start package-gc.service
systemctl status package-gc.timer package-gc.service --no-pager
journalctl -u package-gc.service -n 100 --no-pager
```

job 完成后，`worker_jobs.result_json.summary` 里应出现 `dry_run=true` 和候选/保护摘要，`result_ref` 指向 operator-only `maintenance_gc_report` artifact metadata。切到实际清理时，把 `ExecStart` 改为：

```ini
ExecStart=/home/ubuntu/projects/lca_workspace/tiangong-lca-calculator/target/release/maintenance_enqueue package-artifact-gc --execute --batch-size 100 --max-batches 1
```

execute job 由 `maintenance_worker` 调用 `package_gc --execute`，仍会先删除对象存储 payload，再标记 artifact 为 `deleted`；对象删除失败时不会删除 DB metadata。建议保留 `--batch-size 100 --max-batches 1` 作为首轮 execute canary，再按运行结果逐步调整。

### 6.7 Snapshot Storage GC（systemd timer，推荐）

`snapshot_gc` 负责清理 `lca-results/snapshots/<snapshot_id>/...` 存储目录。候选判断来自 database-engine 的 `util.list_lca_snapshot_gc_candidates(...)`，calculator 只执行对象删除与安全的 DB row 删除：

- CLI 默认 dry-run；实际删除必须显式传 `--execute`。
- active snapshot 永远保护；执行前会再次检查 `lca_active_snapshots`。
- 非 active snapshot 超过 TTL 后，先删除该 snapshot directory 下所有 Storage objects；全部成功后才删除 `public.lca_network_snapshots`，由现有 FK cascade 清理 jobs/results/cache/latest/factorization/artifact metadata。
- orphan storage directory 只删除 Storage objects，不做 DB 操作。
- 404 object delete 视为成功，便于重试幂等。
- timer 只负责 enqueue `lca.snapshot_gc` worker job；`snapshot_gc` 仍使用 `solver_worker_snapshot_gc` PostgreSQL advisory lock，抢不到锁会写 `skipped` audit run 并退出 0。

构建：

```bash
cd /home/ubuntu/projects/lca_workspace/tiangong-lca-calculator
cargo build -p solver-worker --bin maintenance_enqueue --bin maintenance_worker --bin snapshot_gc --release
```

手动 dry-run：

```bash
./scripts/gc_lca_snapshots.sh
```

首轮 execute canary enqueue：

```bash
./scripts/gc_lca_snapshots.sh --execute \
  --max-bytes 536870912 \
  --max-snapshots 10 \
  --max-orphan-dirs 50
```

systemd 模板位于：

- `deploy/systemd/snapshot-gc.service`
- `deploy/systemd/snapshot-gc.timer`

timer 策略：

```ini
OnCalendar=Sun 20:30:00 UTC
RandomizedDelaySec=30m
Persistent=true
```

部署 timer：

```bash
sudo cp deploy/systemd/snapshot-gc.service /etc/systemd/system/snapshot-gc.service
sudo cp deploy/systemd/snapshot-gc.timer /etc/systemd/system/snapshot-gc.timer
sudo systemctl daemon-reload
sudo systemctl enable --now snapshot-gc.timer
```

上线后检查：

```bash
systemctl status snapshot-gc.timer snapshot-gc.service --no-pager
journalctl -u snapshot-gc.service -n 100 --no-pager
```

### 6.8 结果保留与 GC（S3 + DB）

`lca_results` 采用过期字段 + 保留规则：

- `expires_at` 到期才进入删除候选
- `is_pinned=true` 永不自动删除
- 被 `lca_result_cache` 引用（`pending/running/ready`）的结果不会删
- 同一请求分组（`requested_by + snapshot_id + request_key`）至少保留最新 1 条

执行 GC：

```bash
# 实际删除（先删 S3 对象，再删 DB 行）
./scripts/gc_lca_results.sh

# 仅查看候选，不执行删除
./scripts/gc_lca_results.sh --dry-run
```

可选参数：

- `--batch-size <n>`（默认 `200`）
- `--max-batches <n>`（限制本次最多处理批次数）

## 7. 内部 API

推荐路径（snapshot 语义）：

- `POST /internal/snapshots/{snapshot_id}/prepare`
- `GET /internal/snapshots/{snapshot_id}/factorization`
- `POST /internal/snapshots/{snapshot_id}/solve`
- `POST /internal/snapshots/{snapshot_id}/invalidate`

兼容路径（旧命名别名）：

- `.../models/{snapshot_id}/...`

## 8. Queue Payload 契约

作业 payload 使用 `snapshot_id`。为兼容旧消息，worker 仍接受 `model_version` 字段别名。

## 9. 说明文档

- 面向 AI 的持续上下文：`AGENTS.md`
- 架构与建模方案：`LCA_SCHEMA_UPDATE_PLAN.md`（旧路径 `LCA_SCHEMA_PLAN.md` 为跳转说明）
- 优化评估与优先级：`OPTIMIZATION_REVIEW.md`

## 10. 项目文件整理

当前建议只保留“可复现代码 + 核心文档”，本地运行产物都视为临时文件：

- `logs/`：运行日志（临时）
- `reports/`：调试/验证报告（临时）
- `tools/bw25-validator/.venv/`：本地 Python 环境（临时）

一键清理：

```bash
# 清理 logs/reports/.venv
./scripts/cleanup_local_artifacts.sh

# 仅预览
./scripts/cleanup_local_artifacts.sh --dry-run

# 连同 Rust target 一起清理
./scripts/cleanup_local_artifacts.sh --with-target
```
