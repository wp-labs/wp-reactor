# Window 内存控制机制

## 概述

wf-engine 的 window 内存控制分两层：

| 层级 | 配置项 | 检查位置 | 触发方式 |
|------|--------|---------|---------|
| **单窗口** | `max_window_bytes` | `Window::append()` 每次写入后检查 | `while current_bytes > max_bytes { pop_front() }` |
| **全局** | `max_total_bytes`（`window_defaults`） | `Evictor::run_once()` 周期检查 | 总内存超限时，逐出内存最大的窗口的最老 batch |

## 单窗口控制 (`max_window_bytes`)

每次 `Window::append()` 写入新 batch 后，检查 `current_bytes` 是否超过 `max_window_bytes`。如果超过，从 `VecDeque` 前端逐出最老的 batch，直到内存回到限制以下。

```rust
// crates/wf-engine/src/window/buffer/mod.rs
let max_bytes = self.config.max_window_bytes.as_bytes();
while self.current_bytes > max_bytes {
    if let Some(evicted) = self.batches.pop_front() {
        self.current_bytes -= evicted.byte_size;
        self.total_rows -= evicted.row_count;
    }
}
```

## 全局控制 (`max_total_bytes`)

`Evictor` 周期运行（`evict_interval`），遍历同一组窗口列表（`window_names()`），分两阶段：

### Phase 1 — 时间逐出

遍历所有窗口，对每个窗口调用 `evict_expired(now_nanos)`。根据 `now - over` 作为截止时间，移除最老事件时间早于截止时间的 batch。

```rust
// crates/wf-engine/src/window/buffer/eviction.rs
pub fn evict_expired(&mut self, now_nanos: i64) {
    let cutoff = now_nanos - self.over.as_nanos() as i64;
    while let Some(front) = self.batches.front() {
        if front.event_time_range.1 < cutoff {
            let evicted = self.batches.pop_front().unwrap();
            self.current_bytes -= evicted.byte_size;
            self.total_rows -= evicted.row_count;
        } else {
            break;
        }
    }
}
```

### Phase 2 — 内存逐出

计算所有 buffer 窗口的 `memory_usage()` 总和（复用 Phase 1 的 `names` 列表）。如果超过 `max_total_bytes`，找到内存占用最大的窗口，逐出其最老的 batch。重复此过程直到总内存回到限制以下。

```rust
// crates/wf-engine/src/window/evictor.rs
loop {
    let total = sum(window.memory_usage() for all windows);
    if total <= self.max_total_bytes { break; }
    let largest = window_with_largest_memory();
    largest.evict_oldest();
}
```

## 数据释放确认

### `RecordBatch` 生命周期

```
Router::route() → Window::append() → VecDeque<TimedBatch>
                                          ↑ RecordBatch (Arc)
规则读取 → Window::read_since() → batch.clone() (Arc clone) → match engine → drop
逐出     → VecDeque::pop_front() → TimedBatch drop → RecordBatch Arc refcount -1
```

当 `VecDeque` 中 pop 掉一个 `TimedBatch` 时，其持有的 `RecordBatch`（Arc）引用计数 -1。如果没有任何规则持有该 batch 的 snapshot 引用，底层 Arrow 数组内存即被释放。

`read_since()` 返回的 `Vec<RecordBatch>` 在规则引擎处理完 `process_step()` 后离开作用域，Arc 引用自动释放，不阻止逐出。

### 长时间运行验证

通过 `mem_test` feature 下的测试验证了以下场景：

| 测试 | 场景 | 结果 |
|------|------|------|
| `memory_stabilization` | 2000 次迭代，每 2s 注入 1 batch，over=10s | 内存稳定在 ~5 batch |
| `with_snapshots` | 同上 + 每次 `read_since()` | snapshot 不阻止释放 |
| `multi_window` | 3 个窗口，不同 over 时长，3000 次迭代 | 各窗口独立恢复，总量不增长 |
| **`burst_then_drain`** | 突发 100 batch → 静默 drain | **峰值后内存单调下降至 0** |

运行方式：

```bash
cargo test -p wf-engine --features mem_test evictor_long
cargo test -p wf-engine --features mem_test evictor_burst
```

## `current_bytes` 的内存统计范围

`Window::memory_usage()` 返回 `current_bytes`，其值来自每个 batch 的 `batch.get_array_memory_size()`——即 Arrow 数组 buffer 的大小（数据 buffer + 有效位 bitmap + offset buffer）。

**计入**：Arrow 数组的实际数据 buffer。

**未计入**：
- `TimedBatch` 结构体本身（`event_time_range`、`ingested_at`、`row_count`、`byte_size`、`seq`）~48 bytes/batch
- `RecordBatch` 元数据 ~64 bytes/batch
- `VecDeque` ring buffer 的未使用容量（通常 25-50% slot 浪费）

每 batch 合计约 100+ bytes 的结构体开销未被统计。对于 256MB 窗口、每 batch 几 KB 的场景，偏差可忽略。

## 已知局限

### Provider window 不受控

`WindowRegistry::window_names()` 只返回 buffer windows（`self.windows`），不包含 provider windows（`self.provider_windows`）。

影响：
- `Evictor::run_once()` 不逐出 provider window 数据
- `RuntimeMetrics::sample_windows()` 不上报 provider window 内存
- `ProviderWindow` 本身没有 `memory_usage()` 方法，数据存在 `Vec<HashMap<String, Value>>` 中

如果 knowdb provider window 加载了大量数据，这部分内存不受 `max_total_bytes` 约束，也不在 metrics 可见。

### 修复方向

1. `window_names()` 应同时返回 buffer + provider window 名称
2. `ProviderWindow` 需增加 `memory_usage()` 方法，估算 `Vec<HashMap>` 的内存
3. `Evictor` 需处理 provider window 的内存逐出（或记录为"不可逐出"并在 metrics 单独区分）
4. `sample_windows()` 需遍历 provider window 并上报其 `memory_usage()`
