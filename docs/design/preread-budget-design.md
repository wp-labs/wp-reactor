# 预读有界化设计：PrereadBudget 字节预算

> **状态：Implemented（当前权威）**
>
> 2026-08-16 · **代码**：`wf-runtime/src/lifecycle/parse_pool.rs`（`PrereadBudget`）
> · 关联：[window-log-eviction-design.md](window-log-eviction-design.md)（下游
> 内存上界）、[window-channel-actor-design.md](window-channel-actor-design.md)
> · **2026-08-17 修正（P0-②）**：记账单位从解码后 Arrow 内存
> （`get_array_memory_size`）改为内容字节（`content_bytes` ≈ wire），见 §4/§6。

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
- **实现单位（P0-② 修正，2026-08-17）**：`push_decoded_batch` 对解码后的
  RecordBatch 收取 `wf_engine::window::content_bytes`（内容字节，≈ wire 大小），
  而非 `batch.get_array_memory_size()`。理由：Arrow IPC 解码后的
  `get_array_memory_size` 对实际数据有**结构性高估 ~10×**（实测 2026-08-17：
  bid 类批次 71B/行 wire → 718B/行 记账，与字段宽度无关——IPC reader 缓冲
  视图共享的计数假象），按它记账会把预算槽位卡少 ~10×（256MB 默认仅 ~2 槽，
  第一道墙，见 concurrency-scaling.md §2.3）；`content_bytes` 与窗口 mailbox
  记账（`content_bytes + events_bytes`）同一口径，也符合本设计“帧字节”的
  原始意图。
- **配置语义随之改变**：`parse_buffer_bytes` 现在表示**在途内容字节**上界。
  同样吞吐下所需配置值降为旧的 ~1/10；注意在途解码 RSS 仍可能是配置值的
  数倍（缓冲视图+窗口驻留），生产按机器内存酌减；
- 预算取尽时接收侧阻塞在 acquire 上，等效于对 source 反压，不丢帧；
- 8MiB 帧的语义：每帧 acquire 8MiB，256MiB 预算 ≈ 最多 32 帧在途——足够填满
  parse worker 池，又不至于在 worker 停顿时无限膨胀。

## 5. 关联

- 仓库根 `TASK_PK_FLINK.md` §9.4 —— A/B 数据

## 6. P0-② 实验记录（2026-08-17，100m / 4 连接 / shard-files 正常形态）

| 配置（content 记账） | q1 EPS | q1 RSS | q2 EPS | q2 RSS | 槽位(≈content/批) |
|---|---|---|---|---|---|
| 256MB | 6.25~6.66M | 12~14.5GB | — | — | ~36 |
| 512MB | 7.02M | 15.1GB | — | — | ~72 |
| **1GB** | **7.56M** | 14.3GB | 6.69M | 6.5GB | ~144 |
| **2GB** | **7.58M** | 14.2GB | **7.23M** | 7.6GB | ~288 |
| 4GB | 5.90M | 8.9GB | 5.86M | 8.7GB | ~556（过度） |

- **结论**：记账单位修正后甜点在 **1~2GB content**——q1 7.56~7.58M、q2 7.23M，
  与旧 4GB 解码记账口径（q1 7.19M / q2 7.19M，RSS 14.8GB）持平或略高，且
  q2 RSS 减半（7.6 vs 14.8GB）；
- **4GB content 反而退化**（~5.9M）：预算不再受限 → 管线缓冲加深 → 窗口 actor
  乱序重排待补集变大、重排气泡增加（与实验⑤“放大 mailbox 加深乱序重排”同一
  机制）。**预算必须有界才有最佳吞吐**——它不只是内存阀，也是管线深度的
  节流器；
- **默认值 256MB → 128MB（2026-08-17 修正决策，两轮实测）**：content 记账下
  256MB ≈ 36 槽，吞吐从旧默认（解码记账 256MB，~2 槽）的 q1 5.93M 升到
  6.25~6.66M，但 **RSS 从 4.4GB 涨到 12~14.5GB**（吞吐上来后窗口/输出驻留变大，
  非在途记账）——推翻了 8-16 记忆“100M 6.8GB”的对外口径。先试 64MB（9 槽，
  q1 5.90M / 4.45GB，与旧默认一致但零吞吐红利），**再测 128MB（18 槽，q1 6.13M
  / RSS 5.88GB）：比旧默认吞吐 +3.4%、RSS 只多 ~1.5GB，且避开了 256MB 的
  12-14GB RSS 平台**——定为新默认。要更高吞吐显式调大（256MB ≈ 6.3~6.7M /
  RSS 12-14GB、512MB ≈ 7.0M、1-2GB ≈ 7.5M+）。
- **预算上限语义**：预算按 content 字节计，在途解码 footprint = 预算 × IPC 解码
  膨胀（~10× 实测）——下游 stall 时 RSS 可逼近 ~10× 配置值；生产按机器内存酌减。
