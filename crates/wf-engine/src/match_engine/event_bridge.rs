//! 列式批 ↔ 行事件桥（Arrow [`RecordBatch`] ↔ [`Event`]/[`JoinRow`] 读面）。
//!
//! 行式面留本层：值提取 / 物化（`batch_to_events` / `materialize_rows` /
//! `batch_to_timestamped_rows`）、事件时间读取与 structured-JSON 值提取
//! （`extract_field_value` / `json_to_value` 家族）。
//! 子模块边界（#[path] sibling）：`views` —— 列式免物化事件视图
//! （ColumnarEvent / JoinRow / TriggerEvent / join 行构建，on-each / P4 触发面），
//! `tests` —— 原内联单测外移。公开面经下方 `pub use` 以原 `event_bridge::*`
//! 路径与可见级 re-export 保持。

use std::collections::HashMap;

use arrow::array::{Array, Float64Array, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

use super::cep::{EngineHashMap, Event, Value};

// 子模块 #[path] sibling：列式免物化事件视图（ColumnarEvent / JoinRow /
// TriggerEvent + join 行构建 / column_scalar_string / batch_raw_ts_nanos）。
// 行式值提取（extract_field_value / wfl_structured_field_kind 等）留在本层，
// 子模块经 `use super::*` 复用（可见性只向下流，零提级）。
#[path = "event_bridge_views.rs"]
mod views;

pub use views::{
    ColumnarEvent, FieldIndex, JoinRow, TriggerEvent, batch_raw_ts_nanos, build_field_index,
    column_scalar_string, columnar_join_rows, columnar_timestamped_join_rows,
};

// 值提取核心与 wfl structured 判定（2026-09-04 P4-B0 下沉 `wf_cep::value_extract`，
// 本层 re-export 保原 `event_bridge::*` 路径与可见级；子模块经 `use super::*` 复用）。
pub(crate) use wf_cep::value_extract::extract_field_value;
pub use wf_cep::value_extract::{
    WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY, WFL_FIELD_TYPE_OBJECT,
    is_wfl_structured_field, wfl_structured_field_kind,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
#[cfg(test)]
#[path = "event_bridge_tests.rs"]
mod tests;

/// Resolve the batch column index of the event-time field.
pub fn batch_time_col_index(batch: &RecordBatch, time_field: Option<&str>) -> Option<usize> {
    let tf = time_field?;
    batch.schema().fields().iter().position(|f| f.name() == tf)
}

/// Read event-time nanos from a resolved time column at `row`, mirroring
/// [`super::cep::CepStateMachine::event_time_nanos`] exactly (including
/// the f64 round-trip that the eager `extract_event_time` path uses).
///
/// Returns 0 when the column is null or non-numeric.
pub fn batch_event_time_nanos_at(batch: &RecordBatch, time_col_index: usize, row: usize) -> i64 {
    let col = batch.column(time_col_index);
    if col.is_null(row) {
        return 0;
    }
    match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row) as f64 as i64)
            .unwrap_or(0),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row) as i64)
            .unwrap_or(0),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|a| a.value(row) as f64 as i64)
            .unwrap_or(0),
        _ => 0,
    }
}

/// Extract the event-time nanos for `row` straight from the batch time column.
///
/// Returns 0 when the time field is absent, non-numeric, or null. Prefer
/// [`batch_time_col_index`] + [`batch_event_time_nanos_at`] when reading many
/// rows, to resolve the column only once.
pub fn batch_event_time_nanos(batch: &RecordBatch, time_field: Option<&str>, row: usize) -> i64 {
    match batch_time_col_index(batch, time_field) {
        Some(idx) => batch_event_time_nanos_at(batch, idx, row),
        None => 0,
    }
}

/// Convert an Arrow [`RecordBatch`] into a `Vec<Event>`, one per row.
///
/// Each column is mapped to an [`Event`] field by column name. Null values
/// are skipped (the field is simply absent from that row's event).
///
/// | Arrow Type           | → | CEP Value               |
/// |----------------------|---|-------------------------|
/// | Int64                | → | Value::Number(i as f64) |
/// | Float64              | → | Value::Number(f)        |
/// | Utf8                 | → | Value::Str(s)           |
/// | Boolean              | → | Value::Bool(b)          |
/// | Timestamp(Ns, _)     | → | Value::Number(ns as f64)|
/// | Struct               | → | Value::Object           |
/// | List/LargeList       | → | Value::Array            |
pub fn batch_to_events(batch: &RecordBatch) -> Vec<Event> {
    batch_to_events_with(batch, None)
}

/// Like [`batch_to_events`], but only materializes the listed field names.
///
/// Used by the window layer to avoid materializing schema fields no rule
/// reads (see wf-lang `field_usage`), the dominant peak RSS on wide windows.
/// Fields absent from the batch schema are ignored.
pub fn batch_to_events_filtered(
    batch: &RecordBatch,
    fields: &std::collections::HashSet<String>,
) -> Vec<Event> {
    batch_to_events_with(batch, Some(fields))
}

fn batch_to_events_with(
    batch: &RecordBatch,
    only_fields: Option<&std::collections::HashSet<String>>,
) -> Vec<Event> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let mut events = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let mut fields = EngineHashMap::default();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            if let Some(only) = only_fields
                && !only.contains(field.name())
            {
                continue;
            }
            let col = batch.column(col_idx);
            if col.is_null(row) {
                continue;
            }
            if let Some(val) = extract_field_value(field, col.as_ref(), row) {
                fields.insert(field.name().into(), val);
            }
        }
        events.push(Event { fields });
    }
    events
}

/// Materialize only the given row indices into events (L2 deferred
/// materialization primitive).
///
/// `indices` must be ascending (the columnar mask → indices step preserves
/// batch row order). Rows out of range are skipped. This is the counterpart to
/// [`batch_to_events`] that avoids materializing the ~99% of rows a columnar
/// guard rejects (Q2 hit 0.81%).
pub fn materialize_rows(batch: &RecordBatch, indices: &[u32]) -> Vec<Event> {
    materialize_rows_with(batch, indices, None)
}

/// Like [`materialize_rows`], but only materializes the listed field names.
pub fn materialize_rows_filtered(
    batch: &RecordBatch,
    indices: &[u32],
    fields: &std::collections::HashSet<String>,
) -> Vec<Event> {
    materialize_rows_with(batch, indices, Some(fields))
}

fn materialize_rows_with(
    batch: &RecordBatch,
    indices: &[u32],
    only_fields: Option<&std::collections::HashSet<String>>,
) -> Vec<Event> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let mut events = Vec::with_capacity(indices.len());

    for &row_u32 in indices {
        let row = row_u32 as usize;
        if row >= num_rows {
            continue;
        }
        let mut fields = EngineHashMap::default();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            if let Some(only) = only_fields
                && !only.contains(field.name())
            {
                continue;
            }
            let col = batch.column(col_idx);
            if col.is_null(row) {
                continue;
            }
            if let Some(val) = extract_field_value(field, col.as_ref(), row) {
                fields.insert(field.name().into(), val);
            }
        }
        events.push(Event { fields });
    }
    events
}

/// Convert an Arrow [`RecordBatch`] into timestamped rows for asof join.
///
/// Each row becomes `(timestamp_nanos, fields)`. The timestamp is extracted
/// as a raw `i64` from the column at `time_col_index`. Rows with a null
/// timestamp are skipped. All columns (including the time column) are
/// included in the fields map via [`extract_value`], so the time field
/// remains available in the join context.
pub fn batch_to_timestamped_rows(
    batch: &RecordBatch,
    time_col_index: usize,
) -> Vec<(i64, HashMap<String, Value>)> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let ts_col = batch.column(time_col_index);
    let Some(ts_array) = ts_col.as_any().downcast_ref::<TimestampNanosecondArray>() else {
        return Vec::new();
    };

    let mut rows = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        if ts_array.is_null(row) {
            continue;
        }
        let ts = ts_array.value(row);
        let mut fields = HashMap::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            if col.is_null(row) {
                continue;
            }
            if let Some(val) = extract_field_value(field, col.as_ref(), row) {
                fields.insert(field.name().clone(), val);
            }
        }
        rows.push((ts, fields));
    }
    rows
}
