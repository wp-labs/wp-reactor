# 预读有界化设计：PrereadBudget 字节预算

> **状态：Implemented（当前权威）**
>
> 2026-08-16 · **代码**：`wf-runtime/src/lifecycle/parse_pool.rs`（`PrereadBudget`）
> · 关联：[window-log-eviction-design.md](window-log-eviction-design.md)（下游
> 内存上界）、[window-channel-actor-design.md](window-channel-actor-design.md)

---

## 1. 根因

parse/commit 通道的队列**按条数有界、按字节无界**。8MiB 帧场景下，少量帧即可
在通道里囤积巨量未解析字节：

- 30M 实测：RSS 12.6GB（其中大头是通道内 in-flight 帧）
- 100M 实测：RSS 37.9GB——parse worker 跟不上时，接收侧无限预读

## 2. 方案：字节预算信号量贯穿

```rust
// wf-runtime/src/lifecycle/parse_pool.rs
pub(crate) struct PrereadBudget(/* Arc<Semaphore>，字节为单位 */);
```

- **预算申请在读取帧之前**（预读侧 acquire 字节数），parse worker 消费完帧后
  release——同一帧的字节在「读入内存」到「解析完成」全程被记账；
- `Arc<Semaphore>` 使多个 source/连接共享同一预算；
- 配置：`parse_buffer_bytes`，默认 **256MiB**，下限 16MiB。

## 3. 效果

| 规模 | RSS 前 | RSS 后 | EPS |
|---|---|---|---|
| 30M | 12.6GB | **5.8GB（-53%）** | 无回退 |
| 100M | 37.9GB | **6.8GB（-82%）** | 无回退 |

与消费感知驱逐（-68%）叠加后，30M 端到端 RSS 峰值 ~3.3GB（含 each 批式化后）。

## 4. 设计要点

- **记帐单位是帧字节**（读入的 IPC 帧），不是解析后的 Event 数——后者无法在
  读取时预知；
- 预算取尽时接收侧阻塞在 acquire 上，等效于对 source 反压，不丢帧；
- 8MiB 帧的语义：每帧 acquire 8MiB，256MiB 预算 ≈ 最多 32 帧在途——足够填满
  parse worker 池，又不至于在 worker 停顿时无限膨胀。

## 5. 关联

- 仓库根 `TASK_PK_FLINK.md` §9.4 —— A/B 数据
