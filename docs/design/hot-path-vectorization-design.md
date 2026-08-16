# 热路径向量化设计：emit 列批化（C1/C2）与 each 批式求值

> **状态：Implemented（当前权威）**
>
> 2026-08-15/16 · 落地 commit：C1（colc1c）→ C2（40ba71d）→ each 批式（7382048）
> · 关联：[window-channel-actor-design.md](window-channel-actor-design.md)（下游
> window 侧配套改造）· 性能记录见仓库根 `TASK_PK_FLINK.md` §9

---

## 1. 背景与动机

on-each 规则的 emit 链路旧版有**三次物化**：

```
Event（每事件 HashMap<String, Value>）
  → OutputRecord（中间结构）          ← 第二次物化
  → DataRecord（sink 行结构）         ← 第三次物化
  → sink 序列化
```

逐事件 × 三次分配 + 逐字段解释器求值，在 nexmark q1（每事件都 fire）下成为
吞吐主瓶颈。q1 30M 优化前基线：6/10 shard 端到端 2.9M/s。

## 2. C1：消灭 DataRecord —— AlertColumnBatch 列批

**代码**：`wf-engine/src/alert/column_batch.rs`、`wf-engine/src/sink/runtime.rs`

- alert 输出改为列式批：`AlertColumnBatch` + `AlertColumnBuilder`（staging API，
  追加式逐列构建）；
- **payload-blind sink**：sink 不感知行结构，只接受列批并转发——sink 侧不再为
  每行组装 map；
- 语义等价性：行省略可选字段时 `fill_row_gaps` 回填 `(Ignore, Null)`。

> **教训**：C1 曾有列错位隐患（可选字段跳过导致后续行错列），由 C2 的行级等价
> 测试抓出。**列批改造必须配「旧 record 路径 vs 新列批路径」的行级等价测试**，
> 不能只对总数。

## 3. C2：消灭 OutputRecord —— execute_each_direct 直写列

**代码**：`wf-engine/src/match_engine/executor/each_exec.rs`

- `execute_each_direct` 绕过 OutputRecord，on-each 求值结果**直写**
  `AlertColumnBuilder`；
- origin / close_reason 等静态字段提为 plan 常量 `Arc`（构建一次，全程共享）；
- 中间管道 target（规则输出 → 中间 window → 下游规则）**保持 record 路径**——
  只有终态 sink 输出走列批直写。

效果：q1 30M 3.36 → 3.69M/s。

## 4. each 批式向量化（吞吐最大单项）

**代码**：`each_exec.rs` + runtime 分段

逐事件求值改批级求值：

| 手法 | 说明 |
|---|---|
| 常量批级一次求值 | score/entity/yield 表达式中与行无关的部分**整批求一次**，不再逐行 |
| `Expr::Field` 直取 | 字段引用直接从行结构取值，跳过表达式解释器分发 |
| `wfx_id` scratch 复用 | 批间复用缓冲，避免重复分配 |
| `reserve_rows` 预留 | builder 按批大小预留列容量 |
| `ALERT_BATCH_SIZE` 分段 | runtime 按段切批，保证 flush 边界（不会整段 30M 攒在内存） |

效果（q1，30M/100M 不限速 A/B 交错、RSS 相位配对口径）：

- 30M：4.18 → **4.72-4.78M/s（+13-15%，同 RSS 相位）**
- 100M：**5.02M/s @ RSS 3.29GB**（首次破 5M）

### ⚠️ 对测量的副作用

批式化后**高 EPS 与低 RSS 解耦**——旧的「高相位=高 RSS」绑定失效。bench 双峰
相位（±8%）依旧存在，A/B 对比仍必须按 RSS 相位配对，但不能再用 RSS 反推相位。

## 5. 已证零效应方向（勿重试）

以下方向已实测证伪并回退，避免重复投入：

- **消费侧 `recv_many` 批式排空**：tokio mpsc `recv` 在有积压时立即返回、不经
  调度器，三边界实测零效应；
- **`ALERT_BATCH_SIZE` 256→1024**：无收益；
- **parse 合并批次消息**：30M 仅 398 帧（75k 行/帧），消息数本来就极少；
- **float 格式化整数快路径**：q1 每事件仅 3 次 float Display，profile ~6% 归因
  虚高。

42% 调度开销的正确杠杆在**生产侧**：更大消息（合并批次）、semaphore 记账
批量化、任务融合。

## 6. 关联

- [window-channel-actor-design.md](window-channel-actor-design.md) —— C2 之后
  窗口锁成为新断点，引出 window actor 化
- [match-expiry-semantics.md](match-expiry-semantics.md) —— match 引擎侧的
  正确性关键语义（与本文的性能改造互补）
- 仓库根 `TASK_PK_FLINK.md` §9.1/9.2/9.9 —— 性能轨迹与 A/B 数据全记录
