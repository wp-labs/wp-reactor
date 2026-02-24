# wp-reactor 并发架构

> Window-Centric Cursor+Notify 模型
>
> 2026-02-24

---

## 数据流总览

```
                         TCP Clients
                             │
                             ▼
                    ┌─────────────────┐
                    │    Receiver     │  TCP accept loop
                    │  (per-conn task)│  read_frame → decode_ipc
                    └────────┬────────┘
                             │ router.route(stream, batch)
                             ▼
                    ┌─────────────────┐
                    │     Router      │  按 stream 名分发到 Window
                    │                 │  watermark 检查 + append
                    └────────┬────────┘
                             │ append_with_watermark()
                             │ notify_waiters()
                             ▼
              ┌──────────────────────────────┐
              │     Window (RwLock)           │
              │  ┌──────────────────────────┐ │
              │  │ TimedBatch { batch, seq } │ │  VecDeque, 单调递增 seq
              │  │ TimedBatch { batch, seq } │ │
              │  │ ...                       │ │
              │  └──────────────────────────┘ │
              │  watermark_nanos              │
              │  next_seq                     │
              └──────────┬───────────────────┘
                         │ read_since(cursor)
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
   ┌─────────────┐┌─────────────┐┌─────────────┐
   │ Engine Task ││ Engine Task ││ Engine Task │  每个 rule 一个独立 task
   │ (rule A)    ││ (rule B)    ││ (rule C)    │  独占 CepStateMachine
   └──────┬──────┘└──────┬──────┘└──────┬──────┘
          │              │              │
          └──────────────┼──────────────┘
                         │ alert_tx.send()
                         ▼
              ┌─────────────────────┐
              │   Alert Sink Task   │  mpsc channel 消费者
              │   SinkDispatcher    │  → yield-target 路由
              └─────────────────────┘

              ┌─────────────────────┐
              │   Evictor Task      │  定时扫描, 独立运行
              │   (time + memory)   │
              └─────────────────────┘
```

---

## 任务清单

系统运行时包含以下异步任务：

| 任务 | 数量 | 唤醒条件 | 读 | 写 | 取消方式 |
|------|------|----------|----|----|----------|
| Receiver accept loop | 1 | TCP 连接到达 | TcpListener | — | `cancel` |
| Connection handler | N (per client) | TCP frame 到达 | TcpStream | Router → Window (写锁) | `cancel` (child) |
| Engine task | 每 rule 1 个 | Notify / timeout tick | Window (读锁) | CepStateMachine, alert_tx | `rule_cancel` |
| Alert sink | 1 | mpsc::recv() | alert channel | SinkDispatcher 路由 | channel 关闭 |
| Evictor | 1 | 定时 interval | Window (读锁 → 写锁) | Window eviction | `cancel` |

---

## 并发原语

### CancellationToken（两阶段关闭）

```rust
pub struct Reactor {
    cancel: CancellationToken,         // 主令牌：Receiver, Evictor
    rule_cancel: CancellationToken,    // 引擎令牌：仅 RuleTask
}
```

`cancel` 触发后，Receiver 先停止。`wait()` 中 Receiver join 完成后，再触发 `rule_cancel`，确保引擎能读到所有已路由数据。

### Notify（每 Window 一个）

```
Router::route()                        Engine Task
    │                                      │
    │  append_with_watermark()             │  notified = notify.notified()
    │  drop(write_lock)                    │  notified.enable()    ← 先注册
    │  notify.notify_waiters()  ─────────► │  process_new_data()   ← 再读数据
    │                                      │  select! {
    │                                      │      poll_any_notified ← 最后等待
    │                                      │      timeout_tick
    │                                      │      cancel
    │                                      │  }
```

关键：`enable()` 在 `process_new_data()` 之前调用，确保注册 waiter 后再读数据。如果数据在读之后、等待之前到达，通知不会丢失。

### RwLock\<Window\>（标准库）

| 操作 | 锁类型 | 调用方 |
|------|--------|--------|
| `append_with_watermark()` | 写锁 | Router (Connection handler) |
| `read_since(cursor)` | 读锁 | Engine tasks |
| `evict_expired()` | 写锁 | Evictor |
| `evict_oldest()` | 写锁 | Evictor |
| `memory_usage()` | 读锁 | Evictor |

多个 Engine task 可同时持有读锁并发读取同一 Window。

### mpsc channel（alert 通道）

```
Engine Task 1 ──┐
Engine Task 2 ──┼──► mpsc::Sender<AlertRecord> ──► Alert Sink Task
Engine Task 3 ──┘         容量 = 64
```

Alert sink 不使用 CancellationToken。所有 sender drop 后 channel 自动关闭，task 退出。这保证了引擎 flush 的最后一批 alert 不会丢失。

---

## 游标读取机制

每个 Engine task 为每个订阅的 Window 维护一个 cursor（`HashMap<String, u64>`）：

```
Window batches:   [seq=3] [seq=4] [seq=5] [seq=6]
                                    ▲
                              cursor = 5

read_since(5) → 返回 [seq=5, seq=6], new_cursor=7, gap=false
```

**三种情况：**

```
cursor = 5, oldest_seq = 3  →  正常读取, gap = false
cursor = 1, oldest_seq = 3  →  数据被淘汰, gap = true, 从 seq=3 开始读
cursor = 8, newest_seq = 6  →  已读完全部, 返回空
```

`RecordBatch::clone()` 是 Arc 引用计数，零数据拷贝。

---

## Watermark 与迟到处理

```
                    ┌─── 先检查迟到（用当前 watermark）
                    │
    batch 到达 ────►├─── 如果 min_t < watermark - allowed_lateness → DroppedLate
                    │
                    ├─── 再推进 watermark = max(current, max_t - delay)
                    │
                    └─── append 到 VecDeque
```

顺序很关键：**先检查，再推进**。一个跨度很大的 batch 不会被自身推进的 watermark 判定为迟到。

---

## 关闭顺序

```
shutdown()
    │
    ▼
cancel.cancel()
    │
    ├── Receiver: 停止 accept, 等待 in-flight 连接完成
    ├── Evictor:  停止定时扫描
    │
    ▼
wait(): join receiver  ◄── Receiver 完全停止, 所有数据已在 Window 中
    │
    ▼
rule_cancel.cancel()
    │
    ├── Engine tasks:
    │     1. process_new_data()  ← 最后一次 drain
    │     2. flush_all()         ← close 所有活跃状态机实例
    │     3. alert_tx drop       ← sender 引用计数归零
    │
    ▼
wait(): join engines
    │
    ▼
alert channel 关闭 (所有 sender 已 drop)
    │
    ├── Alert sink: drain 剩余 alert, 路由, 退出
    │                调用 dispatcher.stop_all() 优雅关闭
    │
    ▼
wait(): join alert
    │
    ▼
wait(): join infra (evictor)
    │
    ▼
系统完全停止
```

启动顺序（消费者先于生产者）：`alert → infra → engines → receiver`

关闭顺序（LIFO，生产者先于消费者）：`receiver → engines → alert → infra`

---

## 设计要点

**无锁引擎状态**：每个 Engine task 独占 `CepStateMachine`，无需 `Arc<Mutex>`。相比重构前的 `Arc<Mutex<EngineCore>>` 方案是重大简化。

**通知驱动，非轮询**：引擎不做忙等，通过 `Notify` 被动唤醒。Router append 成功后先释放写锁再 `notify_waiters()`，确保引擎醒来后可以立即获取读锁。

**两阶段关闭防丢数据**：如果 Receiver 和 Engine 同时收到 cancel，引擎的 final drain 可能在 Receiver 路由完最后一批数据之前执行，导致数据丢失。分离为两个 token 解决了这个竞态。

**Alert sink 靠 channel 关闭退出**：不用 cancel token，避免引擎 flush 期间 alert sink 提前退出的竞态。`run_alert_dispatcher` 在 channel 关闭后调用 `SinkDispatcher::stop_all()` 优雅关闭所有 sink 实例。
