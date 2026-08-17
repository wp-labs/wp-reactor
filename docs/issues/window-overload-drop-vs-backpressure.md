# 窗口过载：满时丢弃 vs 反压（大 N 冻结）

> 状态：**待修复** · 2026-08-18 · 优先级：高（大 N 场景卡死 + #18 门禁语义）
> 关联：#18 门禁（object 大批次不被窗口内存驱逐）、`design/window-memory-control.md`、
> `design/window-log-eviction-design.md`、`design/window-push-consumer-model.md`
> 复现场景：`wf-examples/performance/qradar_pk`（450 条有状态规则 / 6 类事件源 / 1000 sip 键）

---

## 1. 症状

`./run.sh 10000000`（1000 万事件）在 Linux 8 核云主机（AMD EPYC，30 GiB）上**卡死**：

- `wfgen send` 永久阻塞（`send_payload().await` 无超时），脚本停在 step 3
- daemon 陷入内存驱逐风暴：窗口持续弹批、日志刷 WARN，不再读 TCP
- 根因是内存超限，不是 wfgen/run.sh 的 bug

## 2. 复现证据（3M 本地实测，Mac 64GB）

| 指标 | 1M 基线（验证口径） | 3M 修复前 |
|---|---|---|
| EPS | 150-162k（三轮） | 124k |
| 内存驱逐 | 0 | **72 条** |
| RSS 峰值 | ~6.7GB | **12.3GB** |
| #18 门禁 | 通过 | **FAIL** |

metrics 佐证：conn_events 窗口 `memory_bytes` **顶到 4.28GB**（`max_window_bytes=4GB`
上限），此后每批新事件被弹掉：

```
WARN window `conn_events` dropped 5000 row(s) / 24.7MB in memory eviction
     (max_window_bytes=4294967296 bytes, incoming batch = 5000 rows / 24.7MB)
```

10M 按线性外推：conn 窗口深内容 ~25GB，加 parse 缓冲解码膨胀 + 450 规则实例，
30 GiB 服务器必然超限 → 驱逐风暴 → 引擎失联 → send 永久阻塞。

## 3. 根因（两段式机制）

### 3.1 窗口满时**丢弃**，不是反压源

`wf-engine/src/window/buffer/mod.rs` 的 append 路径（`content_bytes` 记账之后）：

```rust
// Memory eviction: pop oldest batches while over budget.
while self.current_bytes.load(Relaxed) > max_bytes {
    let Some(tb) = log.remove(&front) else { ... };
    // drop(tb) —— 丢弃最旧批次，丢数据
}
```

超 `max_window_bytes` 时**弹掉最旧批次**（有损），不阻塞源。于是：

- 源 → parse → 窗口持续灌入，规则（瓶颈）永远追不上
- 窗口满就丢，永远不会对源形成背压 → 规则无法缩小 backlog
- 日志注释也承认：*"The incoming batch was dropped (in whole or part)... Log it so rules that stop seeing events aren't a mystery."*

### 3.2 时间老化被"所有规则已消费"卡住

`buffer/eviction.rs` 的 `evict_expired` 只释放 `expired && consumed` 的批次：

```rust
let expired = tb.event_time_range.1 < cutoff;   // 事件时间已过 over
let consumed = tb.seq < acked_floor;            // 所有消费者都 ack 过
```

- `over` 来自 WFS schema（qradar `network.wfs` 各窗口 `over = 2m`；`windows.toml`
  里的 `over_cap = "5m"` 只是校验上限，不是老化窗口）
- `acked_floor = min_acked`：**450 条规则中只要有最慢的一条没 ack 到该批次，就不能释放**
- 规则是瓶颈（~150k），入流不节流 → 规则 ack 滞后 → `acked_floor` 落后 → 时间老化无法释放
- Phase 2（全局 `max_total_bytes` 超限的兜底）是显式有损，与本问题同源

### 3.3 事件时间压缩放大

`gen_events.py` 原为 `event_time = BASE_NS + i * 1000`（1 事件/µs）：

- N 个事件压缩在 N µs 的事件时间（10M 仅 10s），远小于窗口 `over = 2m`
- 窗口在跑批期间永不按时间老化 → 一个桶装下全部 N → 内容 ∝ N
- 即使 3.2 的 consumed 门槛放开，窗口稳态内容仍可能 ∝ N（事件时间速率 × over 决定）

## 4. 尝试的缓解（部分有效，非根治）

| 方案 | RSS | 驱逐 | 结论 |
|---|---|---|---|
| 修复前 | 12.3GB | 72 | — |
| gen_events 300µs/事件 | 6.9GB | 65 | 窗口稳态内容压到 ~1GB（2m × 3333 事件/s），但老化仍被 consumed 卡住 |
| + `max_ingest_rate=150k` | 6.9GB | 62 | 限速 > 规则实际吞吐（~118k）仍积压；须 ≤ 规则速率才有效，但这样测的"最大"是假的且脆弱 |

结论：配置/生成器层面无法根治——问题在窗口满时的行为（丢 vs 反压）。

## 5. 修复方案：窗口反压（治本）

> **完整设计见 `docs/design/window-source-backpressure-design.md`**（背压门控 +
> 滞回 + 排水节奏 + mailbox 预算配合）。

**窗口 actor 在缓冲超限时停止消费源消息**（有界 mailbox 填满 → parse pool 满 →
源节流到规则速率），而不是丢。

- **无损**：#18 门禁（驱逐 = 0）天然通过
- **自调节**：不需预知规则吞吐，引擎自动平衡在规则真实能力上，测到的就是真实最大持续吞吐
- **任意 N**：窗口内存被 `max_window_bytes` 天然有界，10M / 100M 都能跑
- **1M 基线不变**：窗口没满就不触发反压，行为与现在完全一致

### 5.1 性能影响

- **正常（窗口没满）**：零开销——append 前一个 O(1) 字节比较（`current_bytes` 是增量维护的原子）
- **过载（规则瓶颈）**：
  - 吞吐上限不变（瓶颈是规则求值，反压不改变规则速度）
  - 测量变诚实：不再把"发送了但被丢弃"的事件计入 EPS（现在 EPS = N/墙钟 虚高）
  - **省 CPU**：去掉驱逐风暴开销（弹批 / 重建 join 索引 / 刷 WARN 日志），
    实测 3M 被拖到 118-135k，反压后规则拿回全部 CPU，可能回到 ~150k
- **延迟**：正常无影响；过载时事件上游排队而非丢弃——等待时间一样，但不丢
- **无死锁**：依赖链 `source → parse → window → rules` 单向，反压向后传播，无环；
  规则永远消费、窗口永远腾出空间

### 5.2 改动点

- `wf-engine/src/window/actor.rs`：`commit_append` 前检查缓冲字节，超 `max_window_bytes`
  则暂停消费源消息（配合 `WindowMsg` 携带的 mailbox `permits` 预算——发送方持 permit
  等待，预算填满后源自然阻塞）
- 注意 actor 的 `pending`（乱序重排）与超限门控的交互
- 补测试：
  - 窗口满 → 源被节流（mailbox/parse 预算填满）→ **不丢数据**
  - 规则追平后 → 窗口腾出空间 → 源恢复
  - 过载下 EPS = 规则真实吞吐（对比现在的"虚高 + 丢弃"）

## 6. 关联

- **#18 门禁**（`wp-reactor#18`：object 大批次不被窗口内存驱逐丢弃）——反压是
  "不驱逐"的天然实现，当前丢批行为与门禁意图相悖
- `docs/design/window-memory-control.md`、`docs/design/window-log-eviction-design.md`
- `docs/design/window-push-consumer-model.md`——窗口→规则通道已是有界反压
  （`RULE_CHANNEL_CAPACITY=32`），**窗口←源未反压**，本问题就是这半截缺口的后果
- `wf-examples/performance/qradar_pk/PK_REPORT_LINUX.md`——1M 稳态为验证口径，
  大 N 需要反压修复后才可跑
