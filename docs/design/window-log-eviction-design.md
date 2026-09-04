# Window 生命周期与驱逐设计：去 SkipMap、消费感知驱逐、并发模型

> **状态：Implemented（当前权威）**
>
> 2026-08-16 · 落地 commit：b65d46d（window log 去 SkipMap）、2563b8a（消费感知驱逐）
> · 前置：[window-channel-actor-design.md](window-channel-actor-design.md)（window
> actor 化 + rule 通道化拓扑）、[window-memory-control.md](archive/window-memory-control.md)
> （旧版两层驱逐机制，Phase 1 已被本文取代）

---

## 1. window log 去 SkipMap（b65d46d，RSS 回归根因结案）

### 1.1 根因：crossbeam-epoch 延迟析构

LF（无锁）方案曾用 `crossbeam-skiplist::SkipMap<u64, TimedBatch>` 实现 window
log。RSS 从 2.9-3.4GB 回归到 11-12GB（EPS 4.14-4.18M）的根因：

- skiplist 的 `remove` 只做 **unlink**，value 的析构进入 crossbeam-epoch 垃圾袋
  （需推进 epoch+2 才真正释放）；
- bench 静息期无人 pin/pin+guard 推进 epoch → **已驱逐批次的内存永久驻留**。

诊断工具链（同类问题再遇时）：

```bash
# 全局分配器换 std::alloc::System + MallocStackLogging
MallocStackLogging=1 malloc_history <pid> -callTree
```

### 1.2 方案：RwLock<BTreeMap>，remove 即析构

**代码**：`wf-engine/src/window/buffer/mod.rs`

- window log 改为 `RwLock<BTreeMap<u64, TimedBatch>>`——`remove` 当场析构，
  crossbeam-skiplist / crossbeam-epoch 两个依赖**已删除**；
- **锁序纪律：log → join_index，绝不反向**（防死锁）；
- 写锁实际无争用的依据：push 模式热路径**不碰 log**（规则消费走通道广播，
  见 window-channel-actor-design），写者只有 actor append（~56 次/s × 亚 μs，
  占空比 ~0.006%）。100M 实测无争用。
- pull 模式（`run_pull_loop` / `Notify` / `events_since` 游标）生产装配已无分支
  使用，仅 engine_task/tests.rs 在用；清理时可直接删。

### 1.3 三个写者与遗留判定

现状写者：actor append / evictor 1s tick / inline commit（file sources、tests、
R2 rollback 无 actor）。

**纯 actor 拥有方案已评估且不做**——被三个约束卡住：evictor 第二写者、inline
commit 路径、metrics 并发读 atomic；改造成本（驱逐搬进 actor + inline 全改通道 +
30+ 测试迁移）与无争用 RwLock 收益相同。除非将来要删 inline/R2 路径时顺带做。

**锁内析构**：evict/append 内存驱逐路径的 `drop(tb)` 在写锁内，销毁 75k 事件批次
（HashMap×75k）可达数百 μs-ms，可能造成 append 延迟尖峰。既定改法（5 行）：pop
进局部 Vec，释放 guard 后再 drop。**触发条件：profile 出现 append 被驱逐阻塞的
证据才做**。

## 2. 消费感知驱逐（2563b8a）

### 2.1 旧机制的两个缺陷（[window-memory-control.md](archive/window-memory-control.md) 描述的版本）

1. **驱逐器从不 tick**：`evict_interval` 30s，10s bench 里 Phase 1 时间驱逐从未
   执行 → 窗口全量驻留（30M RSS 21.5GB 的主因之一）；
2. **时间炸弹**：Phase 1 用**墙钟**对比**事件时间**，简单调小 tick 会瞬间清空
   未消费窗口（cursor_gap）。

### 2.2 方案：WindowProgress ack floor

**代码**：`wf-engine/src/window/progress.rs`、`fanout/`、`evictor.rs`

核心不变量：**batch 可驱逐 ⇔ 事件时间过期 && 所有消费者已 ack seq+1**

- `RulePush` 消息携带 window seq；
- 规则任务处理完该 seq 后 ack（`WindowProgress` 记录 per-consumer floor）;
- `RuleTask` Drop 时释放 slot（防慢规则/崩溃任务永久钉住窗口）;
- Phase 2 内存驱逐（超 `max_total_bytes` 时逐出最大窗口最老批）保持**显式有损**，
  计入 `memory_evicted_total`，与 Phase 1 的无损语义严格区分。

效果（30M bench，evict_interval 调 1s）：

| 指标 | 前 | 后 |
|---|---|---|
| RSS 峰值 | 21.5GB | **6.8GB（-68%）** |
| bid rows 峰值 | 27.6M | 12.6M（仅剩未消费 backlog） |
| cursor_gap / memory_evicted | — | **0 / 0（纯无损）** |

### 2.3 推翻的旧假设

「慢规则 cursor 钉行」不成立——驱逐根本不查 cursor；慢规则的行由 ack floor
保护。反压路径：window→rule 通道有界（32），慢规则积压的行在窗口里由 ack floor
挡住，不丢。

## 3. 当前 window 并发模型总览

```
                     ┌─ actor append（热路径不碰 log）
window log ── RwLock ┼─ evictor 1s tick（ack floor + 时间过期）
（BTreeMap）         └─ inline commit（file sources / tests / R2 rollback）

rule 消费 ── push 通道（RulePush 带 seq，处理完 ack；Drop 释放 slot）
```

- 通道有界 32 → 慢规则反压正常；
- 写锁占空比 ~0.006%，100M 实测无争用；
- 锁序：log → join_index。

## 4. 关联

- [window-channel-actor-design.md](window-channel-actor-design.md) —— push 拓扑
  与 RSS 诊断链全记录（SkipMap 实验留档在附录）
- 仓库根 `TASK_PK_FLINK.md` §9.3/9.5/9.6 —— A/B 数据与决策过程
