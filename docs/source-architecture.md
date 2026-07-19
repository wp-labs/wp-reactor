# Source 架构设计

## 当前架构：connector SourceFactory + WireFormat + BatchSource

warp-fusion 的外部 source（TCP / syslog / Kafka 等）通过 `wp-connector-api` 的
`SourceFactory` 体系构建。connector 层（`wp-core-connectors` 0.5.2+）负责声明并
校验 wire format（`data_format` 参数 → `WireFormat` 枚举）。runtime 通过
`wf-connector-api` 的 `BatchSource` 适配层消费解码后的 Arrow `RecordBatch`。

File source 走 runtime 内联 replay（ndjson / csv / arrow_framed / arrow_ipc）。

```
config.sources
  │
  ├─ kind="file" → 内联 replay (receiver.rs)
  │     ├─ ndjson / csv / arrow_framed / arrow_ipc
  │     └─ replay_* → route_batch
  │
  └─ kind=其他 (tcp/syslog/kafka/…)
        ├─ wp_core_connectors::registry::get_source_factory(kind)
        ├─ factory.validate_spec()                          ← 校验 data_format
        ├─ factory.build(ctx)                               ← wp-connector-api
        │     → SourceSvcIns { acceptor, sources }
        ├─ acceptor.accept_connection(ctrl_rx)              ← 连接接入
        └─ for handle in sources:
              handle.source.start(ctrl_rx)
              WireFormat::from_data_format(data_format)     ← connector 层格式契约
              DataSourceBatchSource::new(source, schema, wire_format)
              loop receive_batch() → Vec<RecordBatch>        ← wf-connector-api
                → route_batch → Router → Window
```

### 三层分工

| 层级 | Crate | 职责 |
|------|-------|------|
| 连接 + 格式契约 | `wp-connector-api` / `wp-core-connectors` | `SourceFactory` 构建 + `validate_spec` 校验 `data_format` |
| Wire format 定义 + 解码 | `wp-core-connectors` (`sources/batch/arrow.rs`) | `WireFormat` 枚举 + `decode_arrow_ipc_batches` / `decode_arrow_framed_batches` |
| Arrow 消费 | `wf-connector-api` (`BatchSource`) | trait 定义；runtime 通过 `DataSourceBatchSource` 实现 |

### WireFormat（connector 层的格式契约）

`wp-core-connectors` 0.5.2 在 `sources/batch/arrow.rs` 定义：

```rust
pub enum WireFormat {
    Ndjson,       // JSON Lines 文本
    ArrowStream,  // 原始 Arrow IPC Stream (schema + batch + EOS)
    ArrowFramed,  // wp_arrow 帧: [4B tag_len][tag][Arrow IPC Stream]
}
```

从 source spec 的 `data_format` 参数解析：

```rust
WireFormat::from_data_format(source.params.get("data_format").map(|s| s.as_str()))
```

TCP / file source factory 的 `validate_spec` 会校验 `data_format` 值是否合法，
在启动阶段就暴露配置错误。

### DataSourceBatchSource（runtime 适配层）

`DataSourceBatchSource`（`wf-runtime/src/source/mod.rs`）桥接 `DataSource` →
`BatchSource`：

- 包装 `Box<dyn DataSource>`
- 按 `WireFormat` 分派解码（NDJSON / ArrowStream / ArrowFramed）
- **ArrowFramed 额外提取 tag**：通过 `wp_arrow::decode_ipc` 解码，保留帧头中的
  stream 名（tag），供 runtime 在未配置 `stream` 参数时用作路由 stream 名
- EOF 正确映射：`wp_connector_api::SourceReason::EOF` → `wf_connector_api::SourceReason::EOF`

ArrowStream / Ndjson 的解码逻辑直接委托 connector 层的共享函数
（`decode_arrow_ipc_batches` / `ndjson_to_record_batch`），不重复实现。

### 内置 `__window_miss` 诊断窗口

Kafka / TCP 等动态输入链路允许通过 `stream_tag_field`（默认
`wp_oml_name`）从每条事件中提取逻辑 stream。生产环境中可能出现暂未注册 schema
的新 `log_type` / stream tag。此类事件无法构造目标业务 window 的 typed
`RecordBatch`，但它不是 source 级连接或解码故障，不能触发 receive retry 或阻塞
同一 topic / partition 的后续合法事件。

runtime 将这类情况定义为 **window miss**：

- `unknown_stream_schema`：事件携带了非空 stream tag，但没有任何 window schema
  订阅该 stream。
- `missing_stream_tag_field`：动态路由模式下，事件缺少可用的 stream tag 字段。
- 后续可扩展 `schema_mismatch` / `payload_decode_error`，但默认只把可恢复的数据质量
  问题纳入 miss；连接错误、EOF、不可恢复的 wire format 错误仍按 source error
  处理。

处理语义：

1. 对可恢复 miss，跳过当前事件或当前 stream 分组，不向外返回 `SourceError`。
2. 同一批次中的合法 stream 继续转换为 `RecordBatch` 并正常进入 `route_batch`。
3. 全批次均为 miss 时返回 `Ok(vec![])`，外层消费循环继续 receive。
4. 首次观察到某个 `(source_name, stream_tag, reason)` 时记录 warning，至少包含
   `source_name`、`source_kind`、`stream_tag_field`、`stream_tag`、`reason` 和一条
   截断后的 payload sample；后续同 key 只更新计数和最近样本。
5. 增加 metrics 计数，指标不携带 raw payload，避免高基数和敏感数据进入
   Prometheus。
6. Kafka connector 在成功读取该消息后应允许 offset 前进；未知 `log_type` 不应导致
   同一分区重复消费同一条消息。

`__window_miss` 是 runtime 启动时注册的内置 provider window，不是用户 WFS 中声明的
业务 window，也不参与普通 stream 路由。它用于保留近期 miss 样本，辅助后续补 schema
或排查上游数据质量问题。该名称为 runtime 保留名，用户不能声明同名 window。

建议 schema：

| 字段 | 类型 | 说明 |
|------|------|------|
| `source_name` | chars | runtime source 实例名 |
| `source_kind` | chars | source 类型，如 `kafka` / `tcp` / `file` |
| `stream_tag_field` | chars | 用于动态路由的字段名 |
| `stream_tag` | chars | 未命中的 tag；缺失时为空 |
| `reason` | chars | miss 原因，如 `unknown_stream_schema` |
| `raw_payload` | chars | 截断后的样本 payload |
| `payload_bytes` | digit | 原始 payload 字节数 |
| `first_seen` | time | 首次观察时间 |
| `last_seen` | time | 最近观察时间 |
| `count` | digit | 同一 key 聚合次数 |

容量与聚合策略：

- 按 `(source_name, stream_tag, reason)` 聚合，避免逐条保留导致内存不可控。
- 当前每个 key 保留最近一条样本，并累加 `count`。
- 当前全局上限为 1024 rows；达到上限时逐出最老 row。
- 后续可扩展为每个 key 保留少量样本，默认 3 条，并补充 dropped sample 计数。

落地顺序：

1. 先在 `DataSourceBatchSource::convert_dynamic_ndjson` 和文件 replay 的动态
   NDJSON / CSV flush 路径中，把未知 schema 从 `SourceError::Decode` 降级为
   window miss + skip。
2. 增加 receiver miss metrics 和单元测试，确保一批数据中 `known` 与 `unknown`
   混合时，`known` 仍正常进入业务 window，`unknown` 不触发 retry。
3. 引入 `__window_miss` 受限诊断存储，先通过 provider snapshot 可读，后续暴露给
   CLI / debug endpoint 查询。
4. 如后续确实需要规则消费 miss 数据，再以 provider window 形式暴露只读快照，而不是
   让 miss payload 直接进入普通业务 window。

### 为什么 `BatchSource` trait 不定死 wire format

`BatchSource::receive_batch()` 返回 `Vec<RecordBatch>`（已解码），不关心 payload
原本是什么格式。这允许第三方直接 impl `BatchSource`，按自己的方式构造
`RecordBatch`，不需要经过 `DataSource` 或 `WireFormat`。格式契约只在 connector
实现层（`wp-core-connectors`），trait 层保持格式无关。

---

## `wf-connector-api` BatchSource trait

```rust
#[async_trait]
pub trait BatchSource: Send {
    async fn start(&mut self) -> SourceResult<()> { Ok(()) }
    async fn receive_batch(&mut self) -> SourceResult<Vec<RecordBatch>>;
    async fn close(&mut self) -> SourceResult<()> { Ok(()) }
    fn identifier(&self) -> &str;
}
```

runtime 中的消费者是 `DataSourceBatchSource`（`wf-runtime/src/source/mod.rs`）。

### 历史背景

早期 warp-fusion 的 Source 层全部内联管理（`SourceConfig` enum + `Receiver` +
`replay_*`），没有复用 `wp-connector-api`。随着 `arrow-tcp-stream-compatibility.md`
设计文档的实施，外部 source 已迁移到 `SourceFactory` 体系。`Receiver` struct 及
其内联 TCP handler 已被删除。

`wp-core-connectors` 0.5.0 的 `TcpBatchSource` / `FileBatchSource` 只支持 NDJSON。
0.5.2 新增了 `WireFormat` + Arrow 解码，实现了完整的格式契约。
