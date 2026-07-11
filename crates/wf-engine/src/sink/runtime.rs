use orion_error::conversion::{SourceErr, ToStructError};
use tokio::sync::Mutex;
use wp_connector_api::{SinkHandle, SinkSpec as ResolvedSinkSpec};
use wp_model_core::model::{DataRecord, DataType};

use crate::alert::WFU_PREFIX;
use crate::error::{CoreReason, CoreResult};

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
        let data = if self.wf_meta_disable.is_empty() {
            data
        } else {
            filtered = mark_wf_meta_fields_ignored(data, &self.wf_meta_disable);
            &filtered
        };
        let mut handle = self.handle.lock().await;
        handle.sink.sink_record(data).await.source_err(
            CoreReason::Sink,
            format!("sink {:?} send record", self.name),
        )
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

fn mark_wf_meta_fields_ignored(data: &DataRecord, disabled_fields: &[String]) -> DataRecord {
    let mut record = data.clone();
    for field in record.items.iter_mut() {
        let name = field.get_name();
        if name.starts_with(WFU_PREFIX) && disabled_fields.iter().any(|disabled| disabled == name) {
            field.as_field_mut().meta = DataType::Ignore;
        }
    }
    record
}

#[cfg(test)]
mod tests {
    use super::*;
    use wp_model_core::model::{DataType, Field, FieldStorage, Value};

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

        let marked = mark_wf_meta_fields_ignored(&record, &["__wfu_rule_name".to_string()]);

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
        let marked = mark_wf_meta_fields_ignored(&projected, &["__wfu_rule_name".to_string()]);

        assert_eq!(marked.items.len(), 2);
        assert_eq!(marked.items[0].get_name(), "__wfu_rule_name");
        assert_eq!(marked.items[0].get_meta(), &DataType::Ignore);
        assert_eq!(marked.items[1].get_name(), "message");
    }

    #[test]
    fn mark_wf_meta_fields_ignored_does_not_mark_business_fields() {
        let mut record = DataRecord::default();
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "message",
            Value::from("hello"),
        )));

        let marked = mark_wf_meta_fields_ignored(&record, &["message".to_string()]);

        assert_eq!(marked.items.len(), 1);
        assert_eq!(marked.items[0].get_name(), "message");
        assert_eq!(marked.items[0].get_meta(), &DataType::Chars);
    }
}
