# Arrow IPC Stream over TCP 兼容性问题

> **状态：已实现（2026-06）**
>
> 本文档描述的迁移方案已落地：
> - `spawn.rs` 内联 TCP handler 已删除，TCP source 走 connector factory
> - `wf-connector-api` 的 `BatchSource` 适配层（`DataSourceBatchSource`）已实现
> - `wp-core-connectors` 0.5.2 的 `WireFormat` + `data_format` 参数固化了格式契约
> - `receiver.rs` 中的 `Receiver` struct 及内联 TCP 逻辑已清理
>
> 以下为设计时的分析记录，保留供参考。

## 问题

`wparse` 通过 `tcp_sink`（`protocol = "arrow"`）向 `wfusion` 发送 Arrow 数据时，wfusion 端 `StreamReader` 报错：

```
failed to create Arrow StreamReader peer=127.0.0.1:XXXXX
error=arrow stream reader: Io error: failed to fill whole buffer
```

数据流断开，wfusion 消费 0 条事件。

## 根因

wparse 和 wfusion 对 Arrow IPC Stream over TCP 的语义不一致：

### wparse 端（`wp-core-connectors/src/sinks/tcp.rs:409`）

```rust
fn encode_batch_ipc_stream(batch: &RecordBatch) -> SinkResult<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
    writer.write(batch)?;
    writer.finish()?;  // 写入 EOS marker (0xFFFFFFFF)
    Ok(buf)
}
```

每个 `RecordBatch` 编码为一个**独立的、完整的** Arrow IPC Stream：
- schema
- record batch
- EOS (end-of-stream) marker

TCP 上连续发送多个这样的独立 stream。

### wfusion 端（`wf-runtime/src/receiver.rs` spawn.rs TCP 默认路径）

```rust
// 默认 format = "arrow_stream"
let stream_reader = StreamReader::try_new(tcp_stream, ...)?;
// 读取 batch — 期望一个连续 stream
```

`StreamReader` 期望一个**连续的** Arrow IPC Stream（一次 schema，多个 batch，最后 EOS）。读到第一个 EOS 后认为 stream 结束，后续到达的独立 stream（含新 schema）无法解析。

### 对比

| | wparse 发送 | wfusion 期望 |
|---|---|---|
| schema 出现次数 | 每 batch 一次 | 一次 |
| EOS marker | 每 batch 一个 | 流末尾一个 |
| 语义 | 多个独立 IPC Stream | 一个连续 IPC Stream |

## 影响范围

所有使用 `tcp_sink` + `protocol = "arrow"` 的 wp-pipeline 场景：
- `examples/wp-pipeline/streaming` — wparse → Arrow TCP → wfusion

## 解决方案：wfusion TCP source 走 connector factory

**方向：不在 wparse 端 hack StreamWriter，而是让 wfusion TCP source 走 connector factory。**

### 问题本质

当前 wfusion `spawn.rs` 中 TCP source 是**内联硬编码**的：

```rust
match source.kind() {
    "tcp" => {
        // 直接 bind + TcpListener + StreamReader — 绕过 connector 框架
        let listener = TcpListener::bind(addr).await?;
        // ... StreamReader::try_new(stream) → 报错 "failed to fill whole buffer"
    }
    other => {
        spawn_external_source_tasks(source, other, ...) // ← kafka/file/syslog 走这里
    }
}
```

`wp-core-connectors` 已经有完整的 TCP source 实现（`sources/tcp/`），支持 `line` / `len` / `auto` 三种 framing 模式，能正确按帧提取 TCP 数据。但 wfusion 的内联 TCP handler 绕过了它，直接用 `StreamReader` 去读 Arrow —— 而 StreamReader 无法处理多段独立 Arrow IPC Stream。

### 目标架构

```
spawn.rs
  match source.kind()
    "tcp" → 删除内联处理
    other → spawn_external_source_tasks ← tcp 也走这里
              │
              ├─ SourceFactory::build() → wp-core-connectors::TcpSource
              │     ├─ accept TCP connection
              │     ├─ framing (line/len/auto)
              │     └─ SourceEvent { payload: RawData::Bytes }
              │
              └─ 解码: detect format → Arrow IPC or NDJSON → RecordBatch → route_batch
```

### 具体改动

#### 1. `wf-runtime/src/lifecycle/spawn.rs` — 删除内联 TCP handler

删除 `"tcp"` match arm（~150 行），TCP source 走 `other` → `spawn_external_source_tasks`。内联 handler 中的 `arrow_framed` / `arrow_stream` 逻辑全部移除。

#### 2. `wf-runtime/src/lifecycle/spawn.rs` — `spawn_external_source_tasks` 增加 Arrow 解码

当前只支持 ndjson：

```rust
let payload = payload_to_string(&event.payload);
ndjson_to_record_batch(&[trimmed], schema)?;
```

新增 Arrow IPC Stream 解码路径：

```rust
match detect_format(&event.payload) {
    PayloadFormat::Ndjson => {
        let payload = payload_to_string(&event.payload);
        ndjson_to_record_batch(&[trimmed], schema)?;
    }
    PayloadFormat::ArrowIpc => {
        let batch = arrow_ipc_to_record_batch(&event.payload, schema)?;
        route_batch(stream, batch, router)?;
    }
}
```

`arrow_ipc_to_record_batch` 从 `Bytes` 中解析独立 Arrow IPC Stream（schema + batch + EOS → RecordBatch）。

#### 3. wfusion `tcp_src` topology — 配置 framing

```toml
# topology/sources/netflow_tcp.toml
connect = "tcp_src"
key = "nginx_tcp"
stream_tag = "nginx_access"

[sources.params]
addr = "127.0.0.1"
port = 9802
framing = "line"          # wparse 用 framing=line 发送，每行一个 Arrow IPC Stream
```

#### 4. arrow_ipc_to_record_batch 实现（`wp-core-connectors` 或 `wf-runtime`）

```rust
use arrow::ipc::reader::StreamReader;

fn arrow_ipc_to_record_batch(
    data: &[u8],
    expected_schema: &Schema,
) -> RuntimeResult<RecordBatch> {
    let cursor = std::io::Cursor::new(data);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| ...)?;
    // 每个独立 IPC Stream 只包含一个 batch
    reader.next()
        .transpose()
        .map_err(|e| ...)?
        .ok_or_else(|| ...)
}
```

每个 TCP frame 是一个完整的、独立的 Arrow IPC Stream（schema + batch + EOS）。`StreamReader` 一次读取一个 frame，天然兼容。

### 影响分析

| | 改动前 | 改动后 |
|---|---|---|
| wfusion TCP source | 内联 StreamReader | connector factory + TcpSource |
| TCP framing | 无（StreamReader 直接读流） | line/len/auto，由 TcpSource 处理 |
| Arrow 兼容 | StreamReader 读连续流，不兼容独立 frame | 每个 frame 独立解析，天然兼容 |
| wparse 端 | 需要改 | **无需改动** |
| 改动范围 | — | spawn.rs（删除 ~150 行 + 新增 Arrow 解码 ~30 行） |

### 不需要改动的部分

- wparse `tcp_sink` — 无需改动
- `wp-core-connectors/src/sources/tcp/` — 已有完整实现，无需改动
- wfusion `receiver.rs` — 无需改动

## 相关文件

| 文件 | 说明 |
|------|------|
| `wf-runtime/src/lifecycle/spawn.rs` | 删除内联 TCP handler + 新增 Arrow 解码 |
| `wp-core-connectors/src/sources/tcp/` | TCP source factory（已有，无需改） |
| `wp-core-connectors/src/sinks/tcp.rs` | wparse 端 Arrow sink（无需改） |
| `examples/wp-pipeline/streaming/` | topology 配置改为 `connect = "tcp_src"` |
