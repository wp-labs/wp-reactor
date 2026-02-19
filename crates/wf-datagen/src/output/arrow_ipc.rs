use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;

use crate::datagen::stream_gen::GenEvent;

/// Write events as Arrow IPC file.
///
/// All fields are stored as UTF-8 strings (JSON-encoded for non-string values)
/// with metadata columns `_stream`, `_window`, `_timestamp`.
pub fn write_arrow_ipc(events: &[GenEvent], output_path: &Path) -> anyhow::Result<()> {
    if events.is_empty() {
        anyhow::bail!("no events to write");
    }

    // Create parent directories if needed
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Collect all field names from events (preserving order from first event)
    let mut field_names: Vec<String> = Vec::new();
    // Always include metadata columns first
    field_names.push("_stream".to_string());
    field_names.push("_window".to_string());
    field_names.push("_timestamp".to_string());

    // Collect data field names from all events to avoid dropping sparse fields.
    for event in events {
        for key in event.fields.keys() {
            if !field_names.contains(key) {
                field_names.push(key.clone());
            }
        }
    }

    // Build Arrow schema — all fields as Utf8 for simplicity
    let arrow_fields: Vec<Field> = field_names
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(arrow_fields));

    // Build columns
    let mut columns: Vec<ArrayRef> = Vec::new();

    for field_name in &field_names {
        let values: Vec<Option<String>> = events
            .iter()
            .map(|event| match field_name.as_str() {
                "_stream" => Some(event.stream_alias.clone()),
                "_window" => Some(event.window_name.clone()),
                "_timestamp" => Some(event.timestamp.to_rfc3339()),
                name => event.fields.get(name).map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                }),
            })
            .collect();

        let array = StringArray::from(values);
        columns.push(Arc::new(array) as ArrayRef);
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    let file = File::create(output_path)?;
    let mut writer = FileWriter::try_new(file, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;

    Ok(())
}
