# P2a 实施计划：规则分片（key 分区 + N shard worker）

> 状态：Plan（待开工）
>
> 2026-08-13 · 关联 [concurrency-scaling.md](concurrency-scaling.md) §3.2
> （规则分片设计定案）。本步只做**无 conv** 规则的 key 分片；conv（P2c）、共享原子（P2b）另行。

---

## 1. 目标与范围

把单个规则的 per-key match 并行化：**同一规则拆 N 个 shard worker，事件按 `hash(match_key) % N`
分区投递**。每个 shard 持有该规则状态的一个 key 分片（各自的 `CepStateMachine`）。

本步（P2a）范围：
- ✅ 编译期 key 提取（`extract_scope_key`，单一来源）
- ✅ 分片可行性判定（有 key 且无 conv 才分片）
- ✅ 批分区（`Arc<Vec<Event>>` → N 子批）
- ✅ N shard worker（复用 `run_rule_task`）
- ✅ 回退开关（`shards=1` = 现行为）

**不做**（后续）：conv 变换算子（P2c）、共享原子限流/预算（P2b）、each 规则事件级并行。

---

## 2. 前置（设计定案，见设计文档 §0.1）

1. key 提取同源：dispatch 分区层与状态机共用 `extract_scope_key`。
2. 无 key 规则退单 worker。
3. conv 规则本步退单 worker（等 P2c 变换算子）。

---

## 3. 实施步骤（每步可编译可测）

### P2a-1：编译期 key 提取（KeyExtractor）

**改动**：
- 在 `RuleExecutor`（或 `MatchPlan` 旁）新增一个 `KeyExtractor`：`fn(&Event) -> Option<Vec<Value>>`
  （提取 `MatchPlan.keys` 的值；`keys` 空或字段缺失 → `None`）。
- 由编译期（`compile.rs::build_run_rules`）从 `MatchPlan.keys` 生成，随 `RunRule` 传给 `RuleTask`。
- `CepStateMachine::advance_at` 内部改为复用这个 extractor 的结果（或接受已提取的 key），
  保证「分区 key == 状态机 key」。

**关键点**：
- `scope_key` 是分区/哈希的 key；fixed 窗口的 `bucket_start` 是**实例维度**，**不参与分区哈希**
  （分区按 match key，不按时间桶）。
- 字段缺失行为要明确：`extract_scope_key` 返回 `None` → 该事件不进任何 shard 的状态
  （或投到一个固定 shard 但被状态机忽略）。实现时对齐现有 `advance_at` 对缺字段的处理。

### P2a-2：分片可行性判定

**改动**：
- 在 `spawn_rule_tasks` 里判定每条规则：
  - `keys` 非空（有 match key）→ 可分片；
  - `conv_plan.is_none()` → 可分片（本步；有 conv 退单，等 P2c）；
  - `shards = executor_parallelism`（`>1` 才实际分片）。
- 不可分片 → 1 worker（现行为）。

### P2a-3：批分区（partitioned fanout）

**改动**：
- `RuleFanout` 的订阅条目从「1 个 channel」升级为「N 个 shard channel + 一个 `KeyExtractor`」。
- `broadcast(window_name, events)`：
  - 有 `KeyExtractor`（分片规则）→ 对批内每个 event 算 `hash(scope_key) % N`，拆 N 个子批，
    分别投到对应 shard 的 channel；
  - 无 `KeyExtractor`（单 worker 规则）→ 原样投整批（现行为）。
- `hash` 用确定性哈希（`foldhash` 或 `std` 稳定哈希），**不依赖 `HashMap` 的随机 seed**。

### P2a-4：N shard worker

**改动**：
- `spawn_rule_tasks` 对可分片规则，创建 N 个 `RuleTaskConfig`（每 shard 一个），
  各自 `run_rule_task`（接收该 shard 的事件，推进各自的 `CepStateMachine`）。
- 每 shard 的 `CepStateMachine` 独立（`instances` 只装本 shard 的 key）。
- `emit` / 中间窗口 / sink fanout 路径不变（shard 直接走现有 emit）。

### P2a-5：回退开关

**改动**：
- `shards` 规则级配置（默认 `executor_parallelism`）；`shards=1` = 单 worker（现行为）。
- 分片不可行的规则（无 key / conv）自动退单 worker。

---

## 4. 测试用例

### 4.1 单元测试（wf-engine）

| # | 用例 | 断言 |
|---|---|---|
| T1 | `extract_scope_key` 单 key | 提取正确的字段值 |
| T2 | `extract_scope_key` 多 key | 按 `keys` 顺序提取多个值 |
| T3 | `extract_scope_key` 无 key（`keys` 空） | 返回 `None` |
| T4 | `extract_scope_key` 字段缺失 | 返回 `None`（与状态机行为一致） |
| T5 | `hash_key` 确定性 | 同 key 跨调用、跨进程（不依赖随机 seed）得到同 hash |
| T6 | `partition_batch` N=1 | 原批不拆（单子批 == 原批） |
| T7 | `partition_batch` N>1 完备性 | 所有子批的事件并集 == 原批（不丢事件、不多事件） |
| T8 | `partition_batch` 同 key 路由 | 同 key 的事件落在同一子批 |
| T9 | `partition_batch` 子批数 | ≤ N（每子批非空） |

### 4.2 集成测试（wf-runtime `engine_task::tests`）

| # | 用例 | 断言 |
|---|---|---|
| T10 | **正确性等价（核心）** | 同一规则，`shards=1` vs `shards=4`，`matches_total` + 逐条 alert 完全一致 |
| T11 | 多 key 分片 | 2 个 match key 的规则，分片结果与单 worker 一致 |
| T12 | 无 key 规则退单 | 无 key 规则在 `shards=4` 下实际单 worker，结果与 `shards=1` 一致 |
| T13 | conv 规则退单（P2a 阶段） | 有 conv 规则在 `shards=4` 下退单 worker，结果一致（等 P2c） |
| T14 | each 规则不受影响 | each 规则（无状态）行为不变 |
| T15 | 中间窗口 + 分片 | 上游分片规则 yield 到中间窗口，下游规则读到的结果与单 worker 一致 |
| T16 | shutdown/EOS flush | N shard 正常 drain + `flush`，close 输出与单 worker 一致 |
| T17 | 回退开关 | `shards=1` 时行为与改造前逐位一致（回归基线） |

### 4.3 端到端测试（nexmark_pk）

| # | 用例 | 断言 |
|---|---|---|
| T18 | 正确性门禁 | `validate.sh` 200K：`matches_total`/`delivered`/`dropped_late` 与基线一致（`shards=1` 与 `shards=4` 均一致） |
| T19 | 吞吐对比 | 2M：`shards=4` daemon runtime 相对 `shards=1` 有提升（记录数据，不作为硬门禁） |

---

## 5. 每步成功判据

| 步骤 | 判据 |
|---|---|
| P2a-1 | T1–T5 通过；`advance_at` 与分区用同一 extractor（代码可查） |
| P2a-2 | T12/T13 通过；无 key/conv 规则实际单 worker |
| P2a-3 | T6–T9 通过；`broadcast` 对分片规则走分区、单 worker 规则走原样 |
| P2a-4 | T10/T11/T15/T16 通过；`matches`/alerts 与单 worker 逐位一致 |
| P2a-5 | T17 通过；`shards=1` 与改造前回归一致 |

**总判据**：全量 `cargo test --workspace` 通过 + T10（正确性等价）通过 + T18（端到端正确性）通过。

---

## 6. 回退

- 规则级 `shards=1` 即现行为；分片不可行规则自动退单 worker。
- 代码回滚：改动集中在 `compile.rs`（extractor 生成）、`spawn.rs`（判定+N shard）、
  `router.rs`/`fanout.rs`（分区）、`rule_task.rs`/`match_engine`（复用 extractor）。
