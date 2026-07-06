//! Arrow-native source adapter: bridges `wp_connector_api::DataSource` to
//! `wf_connector_api::BatchSource`.
//!
//! `wp-core-connectors` 0.5.2+ defines [`WireFormat`] (parsed from the
//! `data_format` spec parameter) and shared Arrow decode helpers. This module
//! wraps a `DataSource` behind the [`BatchSource`] trait, delegating format
//! dispatch to the connector's decode functions while adding stream-tag
//! extraction for `ArrowFramed` frames (warp-fusion uses the tag as the
//! routing stream name).

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use wf_connector_api::{BatchSource, SourceError, SourceReason, SourceResult};
use wp_connector_api::{DataSource, SourceBatch};
use wp_core_connectors::sources::batch::arrow::WireFormat;

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
    /// Stream tag extracted from the last decoded `ArrowFramed` frame.
    last_tag: Option<String>,
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
    ) -> Self {
        Self {
            id: id.into(),
            inner,
            schema,
            format,
            last_tag: None,
        }
    }

    /// Stream tag from the last decoded `ArrowFramed` frame.
    ///
    /// Returns `None` for non-framed formats — callers should use the
    /// configured `stream` param in that case.
    pub fn last_stream_tag(&self) -> Option<&str> {
        self.last_tag.as_deref()
    }

    /// Convert a batch of raw events into zero or more `RecordBatch`es.
    fn convert(&mut self, events: SourceBatch) -> SourceResult<Vec<RecordBatch>> {
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
                // For NDJSON we can peek into the raw JSON to find machine_id.
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
                    Ok(Some(rb)) => Ok(vec![ensure_machine_id_column(rb, machine_id)]),
                    Ok(None) => Ok(vec![]),
                    Err(e) => Err(SourceReason::Decode.err_detail(e)),
                }
            }
            WireFormat::ArrowStream => {
                let batches =
                    wp_core_connectors::sources::batch::arrow::decode_arrow_ipc_batches(&events)?;
                Ok(batches
                    .into_iter()
                    .map(|rb| ensure_machine_id_column(rb, &self.id))
                    .collect())
            }
            WireFormat::ArrowFramed => {
                // Decode via wp_arrow to preserve the tag (stream name).
                let mut batches = Vec::new();
                for event in &events {
                    let bytes = event.payload.as_bytes();
                    match wp_arrow::ipc::decode_ipc(bytes) {
                        Ok(frame) => {
                            self.last_tag = Some(frame.tag);
                            batches.push(ensure_machine_id_column(frame.batch, &self.id));
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
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use async_trait::async_trait;
    use std::sync::Arc;
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
        let mut bs = DataSourceBatchSource::new("nd", Box::new(src), schema(), WireFormat::Ndjson);
        let batches = bs.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
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
        let mut bs = DataSourceBatchSource::new("ipc", Box::new(src), sc, WireFormat::ArrowStream);
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
        );
        let batches = bs.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(bs.last_stream_tag(), Some("syslog"));
    }

    #[tokio::test]
    async fn eof_maps_correctly() {
        let src = VecSource {
            id: "eof".into(),
            batches: vec![],
            idx: 0,
        };
        let mut bs = DataSourceBatchSource::new("eof", Box::new(src), schema(), WireFormat::Ndjson);
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
