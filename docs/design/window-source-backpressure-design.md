# 窗口源反压设计（Window ↔ Source Backpressure）

> 状态：**草案待评审** · 2026-08-18
> 问题背景：`docs/issues/window-overload-drop-vs-backpressure.md`（大 N 冻结 + 满时丢弃）
> 关联：#18 门禁、`window-memory-control.md`、`window-log-eviction-design.md`、
> `window-push-consumer-model.md`（窗口→规则已反压，窗口←源未反压）

---

## 1. 背景

qradar_pk（450 规则）跑大 N（10M）时，窗口在超 `max_window_bytes` 时**弹掉最旧批次
（丢数据）**而不是对源反压，导致：

- 引擎在内存驱逐风暴中失联 → `wfgen send` 永久阻塞 → 脚本卡死
- 3M 实测 72 条驱逐、RSS 12.3GB、#18 门禁 FAIL

详细根因（满时丢弃 + 时间老化被 `consumed` 门槛卡住 + 事件时间压缩）见 issue 文档。

## 2. 现状：两条数据路径的反压状态

```
源(TCP) → parse pool(2GB content) → 窗口 mailbox(permits 预算) → 窗口 actor → 窗口 buffer(≤max_window_bytes)
                                                          → 规则(有界通道 RULE_CHANNEL_CAPACITY=32) → sink
```

| 路径 | 有界？ | 满时行为 |
|---|---|---|
| 窗口 → 规则 | ✅ mpsc 有界 + `send().await` 阻塞 | **反压**（规则慢则窗口派发停） |
| 源 → 窗口 mailbox | ✅ Semaphore permits 预算 | 发送方 `acquire_many_owned` 阻塞（已有，含防死锁） |
| 窗口 actor → 窗口 buffer | ✅ `max_window_bytes` | **丢弃**（`buffer/mod.rs` append 超限弹最旧）❌ |

**缺口**：actor 满时**继续消费** mailbox 并丢批，而不是停消费让 permits 预算填满 → 源阻塞。
mailbox 的背压能力（permits）已在，只是没被触发。

**缓冲字节释放路径**：只在 `evict_expired` / `evict_oldest`（`buffer/eviction.rs` `fetch_sub`），
由 evictor 任务周期调用（`evict_interval` 默认 10s）。**规则消费不释放字节**（批次保留到
`over` 期满才被时间老化释放）。

## 3. 设计目标

1. **无损**：#18 门禁（驱逐 = 0）天然通过
2. **自调节**：不需预知规则吞吐，源自动节流到规则真实能力，测到的是真实最大持续吞吐
3. **任意 N**：窗口内存被 `max_window_bytes` 天然有界，10M / 100M 都能跑
4. **1M 基线不变**：窗口未满不触发背压，行为与现状完全一致
5. **过载语义正确**：延迟增大（上游排队）但不丢数据

## 4. 设计方案

### 4.1 背压门控（window actor，核心）

在 `actor.rs` 的 commit 路径前增加字节门控，采用**高水位 / 低水位滞回**：

```
high_water = max_window_bytes                 // 满，停消费
low_water  = max_window_bytes × 0.7           // 排水到 70%，恢复消费
```

- actor 消费下一条 `WindowMsg::Append` 前，检查 `win.current_bytes()`：
  - `≥ high_water` → **不消费**（消息留在 mailbox / pending），等待排水信号
  - `≤ low_water` → 恢复消费
- 停消费期间，mailbox 的 permits 预算被发送方持有 → 预算填满 → parse pool 的
  下游通道填满 → **源自然阻塞**（复用现有背压链，无需新机制）
- 滞回避免在 `max_window_bytes` 边界上反复启停（排水-再灌-排水抖动）

### 4.2 排水节奏（关键挑战）

**问题**：缓冲字节只在 evictor（`evict_interval` 默认 10s）释放。若背压后干等，
源会以 10s 为周期"灌满-停 10s-再灌"突发，延迟和吞吐都难看。

**方案（推荐 B + A）**：

- **B. 背压触发即时 sweep**：actor 进入背压时，主动请求一次 aging sweep
  （`evict_expired(now_watermark, acked_floor)`），不等周期。排水后 `Notify` 唤醒 actor 恢复。
- **A. Notify 通知**：复用 actor 现有的 `tokio::sync::Notify`（`actor.rs:120`），
  evictor/actor 释放字节后 `notify_one()`，背压等待用 `notified().await`（带超时兜底）。
- 备选 C：调小 `evict_interval`（全局影响所有窗口，不推荐单独为背压改默认）。

### 4.3 mailbox 预算配合（复用已有基础设施）

`WindowMsg` 携带 `OwnedSemaphorePermit`（mailbox 预算）。actor 停消费 → 发送方
`acquire_many_owned` 阻塞 → 预算满 → 源停。**已有防 dining-philosophers 死锁逻辑**
（`budget.acquire_many_owned` 等待完整目标额度，避免多个半额度互等）——设计必须复用
该路径，不引入新的锁序/持锁点。

### 4.4 over 语义边界

窗口保留 `over`（qradar WFS schema = 2m）时长的事件是**规则正确性需要**，背压不缩减
`over`。窗口稳态内容 = `over × 事件时间速率`：

- 配合 `gen_events` 300µs/事件（3333 事件/s 事件时间）→ 稳态 ~1GB，远小于 4GB cap
- 背压只让**墙钟入流速率**降至规则吞吐，不改变窗口的时间语义

### 4.5 与 pending 重排的交互

actor 有 `pending: BTreeMap<(source, seq), WindowMsg>` 处理乱序。背压门控在
commit 之前：pending 里已收到的消息继续持有 permits（不回退给发送方），但**不再
从 mailbox 取新消息**。需确认门控位置（`commit_append` 前）不与 pending 的
乱序窗口/permits 生命周期冲突——见决策点 D1。

## 5. 关键决策点（需评审）

| # | 决策 | 选项 | 倾向 |
|---|---|---|---|
| D1 | 门控放在哪：取消息前 vs commit 前 | 取消息前最干净（不消费即不持有），但要处理 pending 里已有消息 | **commit 前 + pending 已有消息照常 commit**（简化） |
| D2 | 排水触发 | 事件驱动 sweep vs 等 evict_interval vs 调小 interval | **事件驱动 sweep + Notify** |
| D3 | 滞回水位 | 高=100%，低=70% / 80% | 70%（防抖余量），可配置 |
| D4 | 超时兜底 | Notify 等待加超时（如 1s）重查 current_bytes | 加超时兜底，防信号丢失 |
| D5 | 是否独立任务 | 直接在 actor 内 await sweep，还是发消息给 evictor | actor 内 await（sweep 是同步方法，开销小） |

## 6. 测试计划

**单元（wf-engine）**
- buffer 满 → 门控生效（`current_bytes ≥ high_water` 时 commit 被拒/暂停）
- 滞回不抖（low_water 以下恢复，边界上来回不触发反复停启）
- `evict_expired` 在背压后能释放 → 门控恢复

**集成（wf-runtime）**
- 窗口满 → 源被节流（mailbox 预算填满、parse 停）→ **驱逐 = 0、不丢数据**
- 规则追平 → 窗口排水 → 源恢复，事件全部处理
- 过载下 EPS = 规则真实吞吐（对比现在的"虚高 + 丢弃"）

**基准回归**
- 1M 基线：EPS 150-162k / RSS ~6.7GB / 驱逐 0 **不变**
- qradar 3M：驱逐 0、RSS 有界（目标 < 8GB）、EPS 回到 ~150k
- 大 N（10M）：不卡死、不丢、内存有界

## 7. 性能预期

- **正常（窗口未满）**：零开销——append 前一个 O(1) `current_bytes` 比较
- **过载（规则瓶颈）**：
  - 吞吐上限不变（瓶颈是规则求值，背压不改变规则速度）
  - 测量变诚实：不再把"发送了但被丢弃"的事件计入 EPS
  - **省 CPU**：去掉驱逐风暴（弹批 / 重建 join 索引 / 刷 WARN 日志），实测可能
    从 118-135k 回到 ~150k
- **延迟**：正常无影响；过载时事件上游排队而非丢弃（等待一样，但不丢）
- **无死锁**：依赖链 `source → parse → window → rules` 单向，背压向后传播无环；
  复用已有 permits 防死锁路径

## 8. 实施范围

- `wf-engine/src/window/actor.rs`：背压门控 + 排水 sweep + Notify 恢复
- `wf-engine/src/window/buffer/`：暴露 `current_bytes()` / 满-未满判断（已有）
- `wf-config`：滞回水位若需可配置则加 `window.backpressure_low_water_ratio`（默认 0.7）
- `wf-runtime`：evictor 释放字节后 notify actor（或 actor 自 sweep，D5）
- `wf-examples/performance/qradar_pk`：gen_events 300µs 保留；`max_ingest_rate` 回退
  （反压后不再需要手动限速）

## 9. 关联

- issue：`docs/issues/window-overload-drop-vs-backpressure.md`
- `window-memory-control.md`（窗口内存上界与驱逐）、`window-log-eviction-design.md`（日志桶）
- `window-push-consumer-model.md`（窗口→规则有界反压模型，本设计补齐窗口←源半截）
