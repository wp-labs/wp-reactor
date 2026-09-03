use std::sync::Arc;

use orion_error::conversion::{SourceErr, ToStructError};
use tokio::sync::Mutex;
use wildmatch::WildMatch;
use wp_connector_api::{SinkHandle, SinkSpec as ResolvedSinkSpec};
use wp_model_core::model::{DataRecord, DataType};

use crate::alert::{AlertColumnBatch, WFU_PREFIX};
use crate::error::{CoreReason, CoreResult};

#[derive(Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.SinkDispatch")]
pub struct WfMetaDisableMatcher {
    patterns: Vec<WildMatch>,
}

impl WfMetaDisableMatcher {
    pub fn new(patterns: &[String]) -> Self {
        Self {
            patterns: patterns
                .iter()
                .map(|pattern| WildMatch::new(pattern))
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    pub fn matches(&self, name: &str) -> bool {
        name.starts_with(WFU_PREFIX) && self.patterns.iter().any(|pattern| pattern.matches(name))
    }
}

impl std::fmt::Debug for WfMetaDisableMatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WfMetaDisableMatcher")
            .field("patterns", &self.patterns.len())
            .finish()
    }
}

/// Runtime state for a single sink instance.
///
/// Wraps a `SinkHandle` (from wp-connector-api) with metadata and provides
/// convenience methods for sending alert JSON data and lifecycle management.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.SinkDispatch")]
pub struct SinkRuntime {
    pub name: String,
    pub spec: ResolvedSinkSpec,
    pub handle: Mutex<SinkHandle>,
    pub tags: Vec<String>,
    pub output_fields: Option<Vec<String>>,
    pub wf_meta_disable: Vec<String>,
    pub wf_meta_disable_matcher: WfMetaDisableMatcher,
    /// Max parallel writers (1..=10, from the sink group's `parallel`). The
    /// runtime spawns this many consumers per sink to parallelize the alert
    /// delivery fan-out.
    pub parallel: usize,
    /// Payload-blind contract: this sink discards payloads without reading
    /// them (blackhole / discarding sinks). Columnar batches are confirmed
    /// without materializing rows — the Flink discarding-sink equivalent.
    /// Derived from the connector kind until wp-connector-api grows a
    /// capability query.
    pub payload_blind: bool,
}

impl SinkRuntime {
    /// Send raw string payloads via `AsyncRawDataSink::sink_str`.
    pub async fn send_str(&self, data: &str) -> CoreResult<()> {
        let mut handle = self.handle.lock().await;
        handle.sink.sink_str(data).await.source_err(
            CoreReason::Sink,
            format!("sink {:?} send string", self.name),
        )
    }

    /// Send structured records via `AsyncRecordSink::sink_record`.
    pub async fn send_record(&self, data: &DataRecord) -> CoreResult<()> {
        let projected;
        let filtered;
        let data = if let Some(fields) = &self.output_fields {
            projected = project_record(data, fields)?;
            &projected
        } else {
            data
        };
        let data = if self.wf_meta_disable_matcher.is_empty() {
            data
        } else {
            filtered = mark_wf_meta_fields_ignored(data, &self.wf_meta_disable_matcher);
            &filtered
        };
        let mut handle = self.handle.lock().await;
        handle.sink.sink_record(data).await.source_err(
            CoreReason::Sink,
            format!("sink {:?} send record", self.name),
        )
    }

    /// Send a batch of structured records via `AsyncRecordSink::sink_records`.
    ///
    /// Projection / wf_meta_disable are applied per record (same as
    /// [`Self::send_record`]), then the batch is handed to the sink once —
    /// amortizing the per-record write for file sinks and matching the
    /// wp-motor batch delivery model.
    pub async fn send_records(&self, records: &[Arc<DataRecord>]) -> CoreResult<()> {
        let mut batch: Vec<Arc<DataRecord>> = Vec::with_capacity(records.len());
        for record in records {
            // Only materialize a new record when projection / wf_meta
            // filtering actually applies; the passthrough case shares the
            // caller's Arc. The previous unconditional `data.clone()` made
            // every dispatch deep-copy every record inside the serialized
            // sink-handle section — a hidden bottleneck once the DataRecord
            // conversion moved onto the sink consumers (3.4).
            let mut data: Arc<DataRecord> = Arc::clone(record);
            if let Some(fields) = &self.output_fields {
                data = Arc::new(project_record(record, fields)?);
            }
            if !self.wf_meta_disable_matcher.is_empty() {
                data = Arc::new(mark_wf_meta_fields_ignored(
                    &data,
                    &self.wf_meta_disable_matcher,
                ));
            }
            batch.push(data);
        }
        let mut handle = self.handle.lock().await;
        handle.sink.sink_records(batch).await.source_err(
            CoreReason::Sink,
            format!("sink {:?} send records", self.name),
        )
    }

    /// Send a columnar batch. Payload-blind sinks confirm without touching
    /// the payload; row-oriented sinks reconstruct `DataRecord`s via the
    /// column batch's row view (same field order as `to_data_record`) and
    /// take the existing [`Self::send_records`] path.
    pub async fn send_column_batch(&self, batch: &AlertColumnBatch) -> CoreResult<()> {
        if self.payload_blind {
            return Ok(());
        }
        let records: Vec<Arc<DataRecord>> = batch
            .iter_data_records()
            .collect::<CoreResult<Vec<_>>>()?
            .into_iter()
            .map(Arc::new)
            .collect();
        self.send_records(&records).await
    }

    /// Gracefully stop the sink.
    pub async fn stop(&self) -> CoreResult<()> {
        let mut handle = self.handle.lock().await;
        handle
            .sink
            .stop()
            .await
            .source_err(CoreReason::Sink, format!("sink {:?} stop", self.name))
    }
}

impl std::fmt::Debug for SinkRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkRuntime")
            .field("name", &self.name)
            .field("spec", &self.spec)
            .field("tags", &self.tags)
            .field("output_fields", &self.output_fields)
            .field("wf_meta_disable", &self.wf_meta_disable)
            .finish_non_exhaustive()
    }
}

fn project_record(data: &DataRecord, fields: &[String]) -> CoreResult<DataRecord> {
    let mut record = DataRecord::default();
    for name in fields {
        let Some(field) = data.field(name) else {
            return CoreReason::Sink
                .to_err()
                .with_detail(format!("sink requested missing output field {:?}", name))
                .err();
        };
        record.push(field.clone());
    }
    Ok(record)
}

fn mark_wf_meta_fields_ignored(data: &DataRecord, matcher: &WfMetaDisableMatcher) -> DataRecord {
    let mut record = data.clone();
    for field in record.items.iter_mut() {
        let name = field.get_name();
        if matcher.matches(name) {
            field.as_field_mut().meta = DataType::Ignore;
        }
    }
    record
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::task::{Context, Poll, Waker};
    use wp_connector_api::{AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkResult};
    use wp_model_core::model::{DataType, Field, FieldStorage, Value};

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut cx = Context::from_waker(Waker::noop());
        let mut future = Box::pin(future);
        loop {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[derive(Clone, Default)]
    struct CaptureSink {
        record: Arc<StdMutex<Option<DataRecord>>>,
    }

    #[async_trait::async_trait]
    impl AsyncCtrl for CaptureSink {
        async fn stop(&mut self) -> SinkResult<()> {
            Ok(())
        }

        async fn reconnect(&mut self) -> SinkResult<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl AsyncRawDataSink for CaptureSink {
        async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
            Ok(())
        }

        async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
            Ok(())
        }

        async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
            Ok(())
        }

        async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl AsyncRecordSink for CaptureSink {
        async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
            *self.record.lock().unwrap() = Some(data.clone());
            Ok(())
        }

        async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
            *self.record.lock().unwrap() = data.first().map(|record| record.as_ref().clone());
            Ok(())
        }
    }

    fn capture_runtime(
        captured: Arc<StdMutex<Option<DataRecord>>>,
        output_fields: Option<Vec<String>>,
        wf_meta_disable: Vec<String>,
    ) -> SinkRuntime {
        SinkRuntime {
            name: "capture".to_string(),
            spec: ResolvedSinkSpec {
                group: "test".to_string(),
                name: "capture".to_string(),
                kind: "capture".to_string(),
                connector_id: "capture".to_string(),
                params: Default::default(),
                filter: None,
            },
            handle: Mutex::new(SinkHandle::new(Box::new(CaptureSink { record: captured }))),
            tags: Vec::new(),
            output_fields,
            wf_meta_disable: wf_meta_disable.clone(),
            wf_meta_disable_matcher: WfMetaDisableMatcher::new(&wf_meta_disable),
            parallel: 1,
            payload_blind: false,
        }
    }

    fn sample_wf_meta_record() -> DataRecord {
        let mut record = DataRecord::default();
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "__wfu_rule_name",
            Value::from("r1"),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "__wfu_score",
            Value::from("80"),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "message",
            Value::from("hello"),
        )));
        record
    }

    #[test]
    fn project_record_filters_and_reorders_fields() {
        let mut record = DataRecord::default();
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "a",
            Value::from("va"),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "b",
            Value::from("vb"),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "c",
            Value::from("vc"),
        )));

        let projected = project_record(&record, &["c".to_string(), "a".to_string()]).unwrap();

        assert_eq!(projected.items.len(), 2);
        assert_eq!(projected.items[0].get_name(), "c");
        assert_eq!(projected.items[1].get_name(), "a");
    }

    #[test]
    fn project_record_rejects_missing_field() {
        let mut record = DataRecord::default();
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "a",
            Value::from("va"),
        )));

        let err = project_record(&record, &["missing".to_string()]).unwrap_err();
        assert!(err.to_string().contains("missing output field"));
    }

    #[test]
    fn mark_wf_meta_fields_ignored_marks_configured_reserved_prefix_fields() {
        let mut record = DataRecord::default();
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "__wfu_rule_name",
            Value::from("r1"),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "message",
            Value::from("hello"),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "__wfu_score",
            Value::from("80"),
        )));

        let matcher = WfMetaDisableMatcher::new(&["__wfu_rule_name".to_string()]);
        let marked = mark_wf_meta_fields_ignored(&record, &matcher);

        assert_eq!(marked.items.len(), 3);
        assert_eq!(marked.items[0].get_name(), "__wfu_rule_name");
        assert_eq!(marked.items[0].get_meta(), &DataType::Ignore);
        assert_eq!(marked.items[1].get_name(), "message");
        assert_eq!(marked.items[1].get_meta(), &DataType::Chars);
        assert_eq!(marked.items[2].get_name(), "__wfu_score");
        assert_eq!(marked.items[2].get_meta(), &DataType::Chars);
    }

    #[test]
    fn mark_wf_meta_fields_ignored_runs_after_projection() {
        let mut record = DataRecord::default();
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "__wfu_rule_name",
            Value::from("r1"),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "message",
            Value::from("hello"),
        )));

        let projected = project_record(
            &record,
            &["__wfu_rule_name".to_string(), "message".to_string()],
        )
        .unwrap();
        let matcher = WfMetaDisableMatcher::new(&["__wfu_rule_name".to_string()]);
        let marked = mark_wf_meta_fields_ignored(&projected, &matcher);

        assert_eq!(marked.items.len(), 2);
        assert_eq!(marked.items[0].get_name(), "__wfu_rule_name");
        assert_eq!(marked.items[0].get_meta(), &DataType::Ignore);
        assert_eq!(marked.items[1].get_name(), "message");
    }

    #[test]
    fn mark_wf_meta_fields_ignored_supports_all_wfu_wildcard() {
        let mut record = DataRecord::default();
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "__wfu_rule_name",
            Value::from("r1"),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "__wfu_score",
            Value::from("80"),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "message",
            Value::from("hello"),
        )));

        let matcher = WfMetaDisableMatcher::new(&["__wfu_*".to_string()]);
        let marked = mark_wf_meta_fields_ignored(&record, &matcher);

        assert_eq!(marked.items.len(), 3);
        assert_eq!(marked.items[0].get_meta(), &DataType::Ignore);
        assert_eq!(marked.items[1].get_meta(), &DataType::Ignore);
        assert_eq!(marked.items[2].get_meta(), &DataType::Chars);
    }

    #[test]
    fn mark_wf_meta_fields_ignored_supports_partial_wfu_wildcard() {
        let mut record = DataRecord::default();
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "__wfu_rule_name",
            Value::from("r1"),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "__wfu_score",
            Value::from("80"),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "message",
            Value::from("hello"),
        )));

        let matcher = WfMetaDisableMatcher::new(&["__wfu_rule_*".to_string()]);
        let marked = mark_wf_meta_fields_ignored(&record, &matcher);

        assert_eq!(marked.items.len(), 3);
        assert_eq!(marked.items[0].get_meta(), &DataType::Ignore);
        assert_eq!(marked.items[1].get_meta(), &DataType::Chars);
        assert_eq!(marked.items[2].get_meta(), &DataType::Chars);
    }

    #[test]
    fn mark_wf_meta_fields_ignored_does_not_mark_business_fields() {
        let mut record = DataRecord::default();
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "message",
            Value::from("hello"),
        )));

        let matcher = WfMetaDisableMatcher::new(&["message".to_string()]);
        let marked = mark_wf_meta_fields_ignored(&record, &matcher);

        assert_eq!(marked.items.len(), 1);
        assert_eq!(marked.items[0].get_name(), "message");
        assert_eq!(marked.items[0].get_meta(), &DataType::Chars);
    }

    #[test]
    fn send_record_applies_wf_meta_disable_wildmatch_before_sending() {
        let captured = Arc::new(StdMutex::new(None));
        let runtime = capture_runtime(Arc::clone(&captured), None, vec!["__wfu_*".to_string()]);

        block_on(runtime.send_record(&sample_wf_meta_record())).unwrap();

        let sent = captured.lock().unwrap().clone().expect("record captured");
        assert_eq!(
            sent.field("__wfu_rule_name").unwrap().get_meta(),
            &DataType::Ignore
        );
        assert_eq!(
            sent.field("__wfu_score").unwrap().get_meta(),
            &DataType::Ignore
        );
        assert_eq!(sent.field("message").unwrap().get_meta(), &DataType::Chars);
    }

    #[test]
    fn send_record_applies_wf_meta_disable_after_output_projection() {
        let captured = Arc::new(StdMutex::new(None));
        let runtime = capture_runtime(
            Arc::clone(&captured),
            Some(vec!["__wfu_rule_name".to_string(), "message".to_string()]),
            vec!["__wfu_rule_*".to_string()],
        );

        block_on(runtime.send_record(&sample_wf_meta_record())).unwrap();

        let sent = captured.lock().unwrap().clone().expect("record captured");
        assert_eq!(sent.items.len(), 2);
        assert!(sent.field("__wfu_score").is_none());
        assert_eq!(
            sent.field("__wfu_rule_name").unwrap().get_meta(),
            &DataType::Ignore
        );
        assert_eq!(sent.field("message").unwrap().get_meta(), &DataType::Chars);
    }
}
