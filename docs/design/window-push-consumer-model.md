# Window → Rule 推送模型（Push Consumer）

> **状态：Proposal（暂不实施）**
>
> 2026-08-12 · 关联 [architecture.md](architecture.md)（Window-Centric Cursor+Notify pull 模型）

---

## 1. 背景：性能压测暴露的锁竞争

Nexmark_PK combined 2M 事件基准（`wf-examples/performance/nexmark_pk`）在
逐项优化后（metrics 分片、alert MP2C、内存 base×N、SmolStr、foldhash），
matches 完成时间 20s → ~10.6s（~1.9×）。剩余剖面中最大的阻塞项是
**`semaphore_wait_trap` ≈ 10000 样本**（非 CPU，是锁等待）。

调用栈归因（macOS `sample`）：

```
OnceLock 61 · get_or_init 20 · OnceLock 20   ← 主导
events_since 36 · read 16 · pull_and_advance 19
append 4 · write 1                            ← receiver 写路径较少
```

结论：锁竞争**主要来自规则侧访问窗口数据时的 `OnceLock` 懒解析**，其次是
窗口 `RwLock` 读/写交替。

## 2. 现状模型（Pull：Cursor + Notify）

见 [architecture.md](architecture.md)。关键路径（`rule_task.rs`）：

```rust
// 规则 task 被 Notify 唤醒后：
let win = source.window.read();               // ① 窗口读锁
let result = win.events_since(cursor);        // ② 内部调 tb.events()
                                              // ③ OnceLock::get_or_init(解析 50k 事件 ≈7ms)
```

问题链：

1. **12 规则同时首访同一批** → 第一个在 `OnceLock` 内解析，其余 11 个阻塞在
   `OnceLock` 内部等待（`semaphore_wait_trap` 主来源）；
2. **解析发生在窗口读锁内** → 解析 7ms 期间读锁被持住；
3. macOS `std::RwLock` 是 **writer-preference** → receiver 的写锁（append）在
   规则读锁持有期间排队，形成跨阶段连锁阻塞。

## 3. 候选方案 A：Route 侧锁外预解析（小改动）—— 已实测，无收益，回退

> **2026-08-13 实测结论：方案 A 已实施并回退。**

**思路**：把解析从「规则首访、读锁内」挪到「receiver 路由、窗口锁外」。

```
Router::route:
  1. 读锁拿 window.materialize_fields（Arc clone，O(1)），立即释放
  2. 锁外 batch_to_events → Arc<Vec<Event>>
  3. 写锁 append，预解析 Arc 直接放进 TimedBatch
规则读 events_since → OnceLock 已 set → 直接 Arc clone，零等待
```

- 改动：`window/buffer/{mod,watermark}.rs` + `window/router.rs`，3 文件；
- `TimedBatch.parsed_events` 保持 `OnceLock`（预解析时 `set()`，懒解析路径保留给
  中间窗口）；语义与懒解析一致（同一 `materialize_fields`，每批一次物化）。

**实测结果（同机同法）**：

| 指标 | foldhash 基线 | 预解析后 |
|------|--------------|---------|
| matches 2.37M 完成 | ~10.6s | **~11.2s（反而略慢）** |
| 早期 drain（+2.6s） | 9.4M events | **6.1M（明显变慢）** |
| `semaphore_wait_trap` | ~10006 | **~9363（仅 -6%，未达预期）** |

**失败根因**：
1. **解析挪到 receiver（单线程流水线入口）使入口变慢** → 数据落窗慢 → 早期
   drain 明显退化；receiver 是整条链路的瓶颈敏感点，不能在它上面加解析工作；
2. **`semaphore_wait_trap` 主要是 window `RwLock` 读/写竞争**（规则 `events_since`
   读锁 vs receiver/evictor 写锁），不是 `OnceLock` 竞争——预解析只解决了后者，
   所以 semaphore 几乎没降。

**结论**：解析既不能放规则读锁内（12 规则竞争），也不能放 receiver（入口变慢）。
根治需方案 B，且 B 中「解析放哪」必须单独设计（不能简单放 append/receiver）。

## 4. 候选方案 B：Window → Rule 推送模型（架构改造）

**目标**：规则是 window 的订阅者，数据源 append 后**直接推给订阅它的规则**，
规则不再自行拿锁拉取。

### 4.1 设计

```
receiver → Router::route
   └─ window append（解析一次）
        ├─ 写锁内 push 已解析 Arc<Vec<Event>> 到每个订阅规则的 per-rule channel
        └─ notify / 直接唤醒对应规则 task

规则 task 的 loop 改为：
  loop {
      recv(batch)          // 从自己的 channel 收，无窗口锁
      process_batch(...)   // 推进 CepStateMachine
  }
```

- 每规则每订阅窗口一个 `mpsc`/`async_channel`；
- 解析在 append 侧一次，Arc 广播给所有订阅者；
- 规则 `cursor`/`events_since`/`read_since` 语义可废弃；
- 订阅关系在 `spawn_rule_tasks` 时注册（规则 → window → channel）。

### 4.2 必须解决的新问题

| # | 问题 | 现状（pull） | push 需重新设计 |
|---|------|-------------|----------------|
| 1 | **背压/丢数据** | cursor 天然容忍 eviction（gap 检测 + 警告） | bounded channel 满时：阻塞 append（receiver 会卡）/ 丢弃（需显式 gap 语义）/ drop-oldest |
| 2 | **订阅动态性** | 规则主动连 window（`resolve_window_sources`） | 需 window → 规则反向注册；**hot reload** 重建规则 task 时 channel 重新绑定 |
| 3 | **中间窗口** | 规则输出 → 中间 window → 下游规则，走同一 append/read 路径 | push 链路要贯通（上游 push 进 window，window push 给下游） |
| 4 | **timeout/close/watermark** | `scan_timeouts` 自行轮询水位 + 过期 | 除数据外还要 push 水位/过期控制信号 |
| 5 | **批粒度 vs 逐批** | `pull_and_advance` 一次拉所有新批（攒批） | 逐批 push，攒批语义需显式缓冲 |
| 6 | **启动/关闭顺序** | 两阶段 cancel 保证数据先落窗再 drain | 需保证 push 通道在 shutdown 时排空，等价于现有 flush 语义 |

### 4.3 收益 vs 代价

- **收益**：彻底消除规则侧窗口读锁 + `OnceLock` 竞争 + Notify→拉 往返；
  更符合「订阅者」语义；规则 loop 简化。
- **代价**：架构级重构（rule_task loop、window 订阅管理、route 路径、中间窗口、
  reload、shutdown），改造量数倍于方案 A，且引入背压/丢数据的新语义决策；
  当前瓶颈的增量收益仅 ~3-5%（semaphore 占剖面 ~3.5%），且其中大部分已被
  方案 A 覆盖。

## 5. 对比

| 维度 | A. 锁外预解析 | B. Push 模型 |
|------|--------------|-------------|
| 改动面 | 3-4 文件 | 架构级（rule_task/window/route/reload） |
| 规则侧窗口锁 | 保留（但读锁内无解析，锁极短） | 消除 |
| OnceLock 竞争 | 消除 | 消除 |
| 新增语义难点 | 无 | 背压/丢数据/动态订阅/timeout 信号 |
| 风险 | 低 | 高 |
| 预期收益 | +3-5% | 理论上限相近（锁已不是唯一矛盾） |

## 6. 决策记录

- **2026-08-12**：用户确认方向后，决定先记录方案、暂不实施；待方案 A 实测验证。
- **2026-08-13（上午）**：方案 A（锁外预解析）首版已实施并**回退**——实测无收益
  （吞吐略降、semaphore 仅 -6%）。结论：解析不能放 receiver（入口变慢）；
  `semaphore_wait_trap` 主要是 window `RwLock` 竞争而非 `OnceLock`。
- **2026-08-13（下午）**：方案 A 第二版（修复「解析在读锁内」的 bug：读锁只 O(1)
  clone 字段集、锁外解析）实测：早期 drain 恢复（9.1M），但 semaphore 仍仅 -4%、
  总时长略慢 —— **A 净负再次确认**。参考学习 wp-motor（连接级 picker + pending +
  解析 worker 池 round-robin 分发，全程 channel 无共享锁）后，**方案 B 进入正式
  设计**：[window-push-model-design.md](window-push-model-design.md)。
  **当前状态：保持 pull 现状（源码已回退），方案 B 待评审。**

## 7. 关联

- [architecture.md](architecture.md) —— 现行 Cursor+Notify pull 模型
- `wf-examples/performance/nexmark_pk` —— 基准场景
- 实测工具：`/tmp/wf_measure.sh`（events/matches 时间线）、`/tmp/wfprof_run.sh`
  （macOS `sample` 采样）
