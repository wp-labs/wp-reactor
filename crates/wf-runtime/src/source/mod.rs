//! Arrow-native source adapter: bridges `wp_connector_api::DataSource` to
//! `wf_connector_api::BatchSource`.
//!
//! `wp-core-connectors` 0.5.2+ defines [`WireFormat`] (parsed from the
//! `data_format` spec parameter) and shared Arrow decode helpers. This module
//! wraps a `DataSource` behind the [`BatchSource`] trait, delegating format
//! dispatch to the connector's decode functions while adding stream-tag
//! extraction for `ArrowFramed` frames and dynamic NDJSON payloads.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use wf_connector_api::{BatchSource, SourceError, SourceReason, SourceResult};
use wf_lang::WindowSchema;
use wp_connector_api::{DataSource, SourceBatch};
use wp_core_connectors::sources::batch::arrow::WireFormat;

use crate::receiver::{
    WindowMiss, WindowMissReason, maybe_resolve_stream_schema, normalize_stream_tag_field,
};

/// Adapter wrapping a [`wp_connector_api::DataSource`] as a
/// [`wf_connector_api::BatchSource`].
///
/// Uses the connector layer's [`WireFormat`] for format dispatch. For
/// `ArrowFramed` payloads, the stream tag (wp_arrow frame header) is extracted
/// and exposed via [`last_stream_tag`](Self::last_stream_tag) so the runtime
/// can route batches to the correct window when no explicit `stream` is
/// configured.
pub struct DataSourceBatchSource {
    id: String,
    inner: Box<dyn DataSource>,
    schema: SchemaRef,
    format: WireFormat,
    schemas: Arc<Vec<WindowSchema>>,
    schema_cache: HashMap<String, SchemaRef>,
    stream_tag_field: String,
    dynamic_ndjson: bool,
    /// Stream tags aligned with the batches returned by the last decode.
    batch_tags: VecDeque<Option<String>>,
    /// Recoverable input misses observed during the last decode.
    window_misses: Vec<WindowMiss>,
}

impl DataSourceBatchSource {
    /// Create a new adapter.
    ///
    /// The caller must call [`DataSource::start`] on the inner source
    /// **before** wrapping it, since `BatchSource::start()` has no
    /// control-channel parameter.
    pub fn new(
        id: impl Into<String>,
        inner: Box<dyn DataSource>,
        schema: SchemaRef,
        format: WireFormat,
        schemas: Arc<Vec<WindowSchema>>,
        stream_tag_field: impl Into<String>,
        dynamic_ndjson: bool,
    ) -> Self {
        Self {
            id: id.into(),
            inner,
            schema,
            format,
            schemas,
            schema_cache: HashMap::new(),
            stream_tag_field: stream_tag_field.into(),
            dynamic_ndjson,
            batch_tags: VecDeque::new(),
            window_misses: Vec::new(),
        }
    }

    /// Stream tag for the next batch returned by the previous
    /// [`receive_batch`](BatchSource::receive_batch) call.
    ///
    /// Returns `None` for formats that do not carry a per-batch tag.
    pub fn next_stream_tag(&mut self) -> Option<String> {
        self.batch_tags.pop_front().flatten()
    }

    /// Peek at the next stream tag without consuming it.
    pub fn pending_stream_tag(&self) -> Option<&str> {
        self.batch_tags.front().and_then(|tag| tag.as_deref())
    }

    /// Drain recoverable window misses observed by the previous
    /// [`receive_batch`](BatchSource::receive_batch) call.
    pub(crate) fn take_window_misses(&mut self) -> Vec<WindowMiss> {
        std::mem::take(&mut self.window_misses)
    }

    /// Convert a batch of raw events into zero or more `RecordBatch`es.
    fn convert(&mut self, events: SourceBatch) -> SourceResult<Vec<RecordBatch>> {
        self.batch_tags.clear();
        self.window_misses.clear();
        if events.is_empty() {
            return Ok(vec![]);
        }

        match self.format {
            WireFormat::Ndjson => {
                let lines: Vec<String> = events
                    .iter()
                    .map(|e| {
                        wp_core_connectors::sources::batch::payload::payload_to_string(&e.payload)
                    })
                    .collect();
                if self.dynamic_ndjson {
                    return self.convert_dynamic_ndjson(lines);
                }

                let json_machine_id = lines
                    .first()
                    .and_then(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                    .and_then(|v| {
                        v.get(wf_engine::match_engine::MACHINE_ID)
                            .and_then(|ip| ip.as_str().map(|s| s.to_string()))
                    });
                let machine_id = json_machine_id.as_deref().unwrap_or(&self.id);
                match wp_core_connectors::sources::batch::ndjson::ndjson_to_record_batch(
                    &lines,
                    &self.schema,
                ) {
                    Ok(Some(rb)) => {
                        self.batch_tags.push_back(None);
                        Ok(vec![ensure_machine_id_column(rb, machine_id)])
                    }
                    Ok(None) => Ok(vec![]),
                    Err(e) => Err(SourceReason::Decode.err_detail(e)),
                }
            }
            WireFormat::ArrowStream => {
                let batches =
                    wp_core_connectors::sources::batch::arrow::decode_arrow_ipc_batches(&events)?;
                Ok(batches
                    .into_iter()
                    .map(|rb| {
                        self.batch_tags.push_back(None);
                        ensure_machine_id_column(rb, &self.id)
                    })
                    .collect())
            }
            WireFormat::ArrowFramed => {
                // Decode via trusted path to preserve the tag (stream name) and
                // skip arrow-rs content re-validation (local wfgen frames).
                let mut batches = Vec::new();
                for event in &events {
                    let bytes = event.payload.as_bytes();
                    // perf-diag cut_recv 门控: 只读帧头 tag 识别哨兵流——非哨兵
                    // 帧 body **不解码即丢**（测「注入 + TCP 接收」字节率, 隔离
                    // 单线程 decode 的 ~2.3GB/s validate_utf8 墙）。哨兵帧走原
                    // 路径（测量协议必须活）。tag 无法识别时保守走原路径。
                    if crate::perf_diag::perf_cut_recv() {
                        match crate::receiver::arrow::frame_tag(bytes) {
                            Some(tag) if tag != crate::perf_diag::PERF_SENTINEL_STREAM => {
                                continue;
                            }
                            _ => {}
                        }
                    }
                    match crate::receiver::arrow::decode_ipc_trusted(bytes) {
                        Ok((tag, batch)) => {
                            self.batch_tags.push_back(Some(tag));
                            batches.push(ensure_machine_id_column(batch, &self.id));
                        }
                        Err(e) => {
                            return Err(SourceReason::Decode.err_detail(e.to_string()));
                        }
                    }
                }
                Ok(batches)
            }
        }
    }

    fn convert_dynamic_ndjson(&mut self, lines: Vec<String>) -> SourceResult<Vec<RecordBatch>> {
        let stream_tag_field = normalize_stream_tag_field(&self.stream_tag_field).to_string();
        let mut lines_by_stream: HashMap<String, Vec<String>> = HashMap::new();
        for (line_idx, line) in lines.into_iter().enumerate() {
            let value: serde_json::Value = serde_json::from_str(&line).map_err(|e| {
                SourceReason::Decode.err_detail(format!(
                    "invalid NDJSON event at row {}: {}",
                    line_idx + 1,
                    e
                ))
            })?;
            let stream = match value
                .as_object()
                .and_then(|obj| obj.get(stream_tag_field.as_str()))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .map(ToString::to_string)
            {
                Some(stream) => stream,
                None => {
                    self.window_misses.push(WindowMiss::new(
                        stream_tag_field.as_str(),
                        None,
                        WindowMissReason::MissingStreamTagField,
                        &line,
                        1,
                    ));
                    continue;
                }
            };
            lines_by_stream.entry(stream).or_default().push(line);
        }

        let mut batches = Vec::new();
        for (stream, stream_lines) in lines_by_stream {
            let Some(schema) = self.schema_for_stream(&stream)? else {
                let sample = stream_lines.first().map(String::as_str).unwrap_or_default();
                self.window_misses.push(WindowMiss::new(
                    stream_tag_field.as_str(),
                    Some(stream),
                    WindowMissReason::UnknownStreamSchema,
                    sample,
                    stream_lines.len(),
                ));
                continue;
            };
            match wp_core_connectors::sources::batch::ndjson::ndjson_to_record_batch(
                &stream_lines,
                &schema,
            ) {
                Ok(Some(rb)) => {
                    self.batch_tags.push_back(Some(stream));
                    batches.push(ensure_machine_id_column(rb, &self.id));
                }
                Ok(None) => {}
                Err(e) => return Err(SourceReason::Decode.err_detail(e)),
            }
        }
        Ok(batches)
    }

    fn schema_for_stream(&mut self, stream: &str) -> SourceResult<Option<SchemaRef>> {
        if let Some(schema) = self.schema_cache.get(stream) {
            return Ok(Some(Arc::clone(schema)));
        }
        let schema = maybe_resolve_stream_schema(self.schemas.as_slice(), stream)
            .map_err(|e| SourceReason::Decode.err_detail(e.to_string()))?;
        let Some(schema) = schema else {
            return Ok(None);
        };
        self.schema_cache
            .insert(stream.to_string(), Arc::clone(&schema));
        Ok(Some(schema))
    }
}

/// Ensure `MACHINE_ID` column exists on a RecordBatch.
///
/// If already present, returns the batch unchanged. Otherwise appends a
/// `Utf8` column filled with `fallback_value`, so downstream CEP engine
/// and metrics can identify the source machine.
fn ensure_machine_id_column(batch: RecordBatch, fallback_value: &str) -> RecordBatch {
    if batch
        .schema()
        .index_of(wf_engine::match_engine::MACHINE_ID)
        .is_ok()
    {
        return batch;
    }
    let col = StringArray::from(vec![Some(fallback_value); batch.num_rows()]);
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        wf_engine::match_engine::MACHINE_ID,
        arrow::datatypes::DataType::Utf8,
        true,
    )));
    let mut cols: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
    cols.push(Arc::new(col));
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, cols).unwrap_or(batch)
}

#[async_trait]
impl BatchSource for DataSourceBatchSource {
    async fn receive_batch(&mut self) -> SourceResult<Vec<RecordBatch>> {
        match self.inner.receive().await {
            Ok(events) => self.convert(events),
            Err(e) => Err(map_wp_error(e)),
        }
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.inner.close().await.ok();
        Ok(())
    }

    fn identifier(&self) -> &str {
        &self.id
    }
}

/// Map a `wp_connector_api` source error to a `wf_connector_api` source error.
///
/// Mirrors the mapping in `wp-core-connectors::sources::batch::error`.
fn map_wp_error(err: wp_connector_api::SourceError) -> SourceError {
    use wp_connector_api::SourceReason as Wp;
    match err.reason() {
        Wp::EOF => SourceError::from(SourceReason::EOF),
        Wp::SupplierError | Wp::Disconnect => SourceReason::Connect.err_detail(err.to_string()),
        _ => SourceReason::Decode.err_detail(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::await_holding_lock)] // perf-diag cut_recv 测试跨 await 持全局锁
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::Duration;
    use wp_connector_api::{SourceEvent, Tags};
    use wp_model_core::raw::RawData;

    struct VecSource {
        id: String,
        batches: Vec<SourceBatch>,
        idx: usize,
    }

    #[async_trait]
    impl DataSource for VecSource {
        async fn receive(&mut self) -> wp_connector_api::SourceResult<SourceBatch> {
            if self.idx < self.batches.len() {
                let b = std::mem::take(&mut self.batches[self.idx]);
                self.idx += 1;
                Ok(b)
            } else {
                Err(wp_connector_api::SourceReason::EOF.into())
            }
        }
        fn try_receive(&mut self) -> Option<SourceBatch> {
            None
        }
        fn identifier(&self) -> String {
            self.id.clone()
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, false),
            Field::new("n", DataType::Int64, false),
        ]))
    }

    fn empty_window_schemas() -> Arc<Vec<WindowSchema>> {
        Arc::new(Vec::new())
    }

    fn stream_schema(name: &str, stream: &str) -> WindowSchema {
        WindowSchema {
            name: name.to_string(),
            streams: vec![stream.to_string()],
            time_field: None,
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "msg".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Chars),
                },
                wf_lang::FieldDef {
                    name: "n".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        }
    }

    fn ndjson_event(json: &str) -> SourceEvent {
        SourceEvent::new(
            0,
            "test",
            RawData::from_string(json.to_string()),
            Arc::new(Tags::new()),
        )
    }

    fn arrow_ipc_event(rb: &RecordBatch) -> SourceEvent {
        let mut buf = Vec::new();
        let mut w = StreamWriter::try_new(&mut buf, rb.schema().as_ref()).unwrap();
        w.write(rb).unwrap();
        w.finish().unwrap();
        SourceEvent::new(0, "test", RawData::Bytes(buf.into()), Arc::new(Tags::new()))
    }

    #[tokio::test]
    async fn ndjson_decode() {
        let src = VecSource {
            id: "nd".into(),
            batches: vec![vec![
                ndjson_event(r#"{"msg":"a","n":1}"#),
                ndjson_event(r#"{"msg":"b","n":2}"#),
            ]],
            idx: 0,
        };
        let mut bs = DataSourceBatchSource::new(
            "nd",
            Box::new(src),
            schema(),
            WireFormat::Ndjson,
            empty_window_schemas(),
            crate::receiver::DEFAULT_STREAM_TAG_FIELD,
            false,
        );
        let batches = bs.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn ndjson_dynamic_stream_tag_field_decode() {
        let src = VecSource {
            id: "nd".into(),
            batches: vec![vec![
                ndjson_event(r#"{"wp_oml_name":"a","msg":"a1","n":1}"#),
                ndjson_event(r#"{"wp_oml_name":"b","msg":"b1","n":2}"#),
                ndjson_event(r#"{"wp_oml_name":"a","msg":"a2","n":3}"#),
            ]],
            idx: 0,
        };
        let schemas = Arc::new(vec![
            stream_schema("win_a", "a"),
            stream_schema("win_b", "b"),
        ]);
        let mut bs = DataSourceBatchSource::new(
            "nd",
            Box::new(src),
            Arc::new(Schema::empty()),
            WireFormat::Ndjson,
            schemas,
            crate::receiver::DEFAULT_STREAM_TAG_FIELD,
            true,
        );

        let batches = bs.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 2);
        let mut routed: Vec<(String, usize)> = batches
            .iter()
            .map(|batch| (bs.next_stream_tag().unwrap(), batch.num_rows()))
            .collect();
        routed.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(routed, vec![("a".to_string(), 2), ("b".to_string(), 1)]);
    }

    #[tokio::test]
    async fn ndjson_dynamic_unknown_stream_is_window_miss() {
        let src = VecSource {
            id: "nd".into(),
            batches: vec![vec![
                ndjson_event(r#"{"wp_oml_name":"known","msg":"ok","n":1}"#),
                ndjson_event(r#"{"wp_oml_name":"unknown","msg":"skip","n":2}"#),
                ndjson_event(r#"{"msg":"missing","n":3}"#),
            ]],
            idx: 0,
        };
        let schemas = Arc::new(vec![stream_schema("win_known", "known")]);
        let mut bs = DataSourceBatchSource::new(
            "nd",
            Box::new(src),
            Arc::new(Schema::empty()),
            WireFormat::Ndjson,
            schemas,
            crate::receiver::DEFAULT_STREAM_TAG_FIELD,
            true,
        );

        let batches = bs.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(bs.next_stream_tag().as_deref(), Some("known"));

        let mut misses = bs.take_window_misses();
        misses.sort_by_key(|a| a.reason);
        assert_eq!(misses.len(), 2);
        assert_eq!(
            misses[0].reason,
            crate::receiver::WindowMissReason::UnknownStreamSchema
        );
        assert_eq!(misses[0].stream_tag.as_deref(), Some("unknown"));
        assert_eq!(misses[0].rows, 1);
        assert_eq!(
            misses[1].reason,
            crate::receiver::WindowMissReason::MissingStreamTagField
        );
        assert_eq!(misses[1].stream_tag, None);
        assert_eq!(misses[1].rows, 1);
    }

    #[tokio::test]
    async fn arrow_ipc_decode() {
        let sc = schema();
        let rb = RecordBatch::try_new(
            sc.clone(),
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap();
        let src = VecSource {
            id: "ipc".into(),
            batches: vec![vec![arrow_ipc_event(&rb)]],
            idx: 0,
        };
        let mut bs = DataSourceBatchSource::new(
            "ipc",
            Box::new(src),
            sc,
            WireFormat::ArrowStream,
            empty_window_schemas(),
            crate::receiver::DEFAULT_STREAM_TAG_FIELD,
            false,
        );
        let batches = bs.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn arrow_framed_decode_and_tag_extraction() {
        let sc = schema();
        let rb = RecordBatch::try_new(
            sc.clone(),
            vec![
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .unwrap();
        let framed = wp_arrow::ipc::encode_ipc("syslog", &rb).unwrap();
        let src = VecSource {
            id: "framed".into(),
            batches: vec![vec![SourceEvent::new(
                0,
                "test",
                RawData::Bytes(framed.into()),
                Arc::new(Tags::new()),
            )]],
            idx: 0,
        };
        let mut bs = DataSourceBatchSource::new(
            "framed",
            Box::new(src),
            Arc::new(Schema::empty()),
            WireFormat::ArrowFramed,
            empty_window_schemas(),
            crate::receiver::DEFAULT_STREAM_TAG_FIELD,
            false,
        );
        let batches = bs.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(bs.pending_stream_tag(), Some("syslog"));
        assert_eq!(bs.next_stream_tag().as_deref(), Some("syslog"));
    }

    #[tokio::test]
    async fn cut_recv_skips_non_sentinel_frame_body() {
        // perf-diag cut_recv 门控: 非哨兵帧只读帧头 tag 即丢（不解码 body）——
        // 测纯 TCP 接收字节率; 哨兵帧正常解码（测量协议必须活）。全局门控跨
        // await 持锁（PERF_CUT_SERIAL）, 避免并行测试污染。
        let _g = crate::perf_diag::PERF_CUT_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let sc = schema();
        let rb = |n: i64| {
            RecordBatch::try_new(
                sc.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["x"])),
                    Arc::new(Int64Array::from(vec![n])),
                ],
            )
            .unwrap()
        };
        let syslog = wp_arrow::ipc::encode_ipc("syslog", &rb(1)).unwrap();
        let sentinel =
            wp_arrow::ipc::encode_ipc(crate::perf_diag::PERF_SENTINEL_STREAM, &rb(2)).unwrap();
        let mk_src = |frames: Vec<SourceEvent>| {
            DataSourceBatchSource::new(
                "framed",
                Box::new(VecSource {
                    id: "framed".into(),
                    batches: vec![frames],
                    idx: 0,
                }),
                Arc::new(Schema::empty()),
                WireFormat::ArrowFramed,
                empty_window_schemas(),
                crate::receiver::DEFAULT_STREAM_TAG_FIELD,
                false,
            )
        };
        // 未切: 两帧都解码（syslog + sentinel）。
        let mut bs = mk_src(vec![
            SourceEvent::new(
                0,
                "test",
                RawData::Bytes(syslog.clone().into()),
                Arc::new(Tags::new()),
            ),
            SourceEvent::new(
                0,
                "test",
                RawData::Bytes(sentinel.clone().into()),
                Arc::new(Tags::new()),
            ),
        ]);
        let batches = bs.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 2, "未切: 全部解码");

        // 切: 只解码哨兵帧（syslog 帧 body 即丢）。
        crate::perf_diag::set_perf_cuts(false, false, false, true, false);
        let mut bs = mk_src(vec![
            SourceEvent::new(
                0,
                "test",
                RawData::Bytes(syslog.into()),
                Arc::new(Tags::new()),
            ),
            SourceEvent::new(
                0,
                "test",
                RawData::Bytes(sentinel.clone().into()),
                Arc::new(Tags::new()),
            ),
        ]);
        let batches = bs.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1, "cut_recv: 非哨兵帧 body 即丢");
        assert_eq!(
            bs.next_stream_tag().as_deref(),
            Some(crate::perf_diag::PERF_SENTINEL_STREAM),
            "哨兵帧正常解码"
        );
        crate::perf_diag::reset_perf_diag();
    }

    #[tokio::test]
    async fn arrow_framed_decode_tracks_tag_per_batch() {
        let sc = schema();
        let rb_a = RecordBatch::try_new(
            sc.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let rb_b = RecordBatch::try_new(
            sc.clone(),
            vec![
                Arc::new(StringArray::from(vec!["b"])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap();
        let src = VecSource {
            id: "framed".into(),
            batches: vec![vec![
                SourceEvent::new(
                    0,
                    "test",
                    RawData::Bytes(wp_arrow::ipc::encode_ipc("stream_a", &rb_a).unwrap().into()),
                    Arc::new(Tags::new()),
                ),
                SourceEvent::new(
                    0,
                    "test",
                    RawData::Bytes(wp_arrow::ipc::encode_ipc("stream_b", &rb_b).unwrap().into()),
                    Arc::new(Tags::new()),
                ),
            ]],
            idx: 0,
        };
        let mut bs = DataSourceBatchSource::new(
            "framed",
            Box::new(src),
            Arc::new(Schema::empty()),
            WireFormat::ArrowFramed,
            empty_window_schemas(),
            crate::receiver::DEFAULT_STREAM_TAG_FIELD,
            false,
        );

        let batches = bs.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(bs.next_stream_tag().as_deref(), Some("stream_a"));
        assert_eq!(bs.next_stream_tag().as_deref(), Some("stream_b"));
    }

    #[tokio::test]
    async fn eof_maps_correctly() {
        let src = VecSource {
            id: "eof".into(),
            batches: vec![],
            idx: 0,
        };
        let mut bs = DataSourceBatchSource::new(
            "eof",
            Box::new(src),
            schema(),
            WireFormat::Ndjson,
            empty_window_schemas(),
            crate::receiver::DEFAULT_STREAM_TAG_FIELD,
            false,
        );
        let err = bs.receive_batch().await.unwrap_err();
        assert_eq!(err.reason(), &SourceReason::EOF);
    }

    #[test]
    fn test_ensure_machine_id_column() {
        // already has column → unchanged
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, false),
            Field::new(wf_engine::match_engine::MACHINE_ID, DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(StringArray::from(vec!["10.0.0.1"])),
            ],
        )
        .unwrap();
        let r = super::ensure_machine_id_column(batch, "fallback");
        assert_eq!(r.num_columns(), 2);

        // missing column → appended with fallback value
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["hello", "world"]))],
        )
        .unwrap();
        let r = super::ensure_machine_id_column(batch, "fallback_src");
        assert_eq!(r.num_columns(), 2);
        assert_eq!(r.num_rows(), 2);
        let col = r.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col.value(0), "fallback_src");
        assert_eq!(col.value(1), "fallback_src");
    }
}
