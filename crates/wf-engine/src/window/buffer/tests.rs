use crate::match_engine::{AsofLookup, JoinKey, Value};
use crate::window::buffer::JOIN_INDEX_SHARDS;
use crate::window::buffer::Window;
use crate::window::buffer::types::AppendOutcome;
use crate::window::buffer::types::WindowParams;
use crate::window::buffer::{content_bytes, events_bytes};
use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
use wf_lang::ast::FieldRef;

use crate::window::RuleFanout;
use crate::window::WindowProgress;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn test_schema_no_time() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn make_batch(schema: &SchemaRef, times: &[i64], values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(times.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn make_batch_no_time(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap()
}

fn test_config(max_bytes: usize) -> WindowConfig {
    WindowConfig {
        name: "test".into(),
        mode: wf_config::DistMode::Local,
        max_window_bytes: max_bytes.into(),
        over_cap: Duration::from_secs(3600).into(),
        evict_policy: wf_config::EvictPolicy::TimeFirst,
        watermark: Duration::from_secs(5).into(),
        allowed_lateness: Duration::from_secs(0).into(),
        late_policy: wf_config::LatePolicy::Drop,
        table: None,
    }
}

fn test_window(over_secs: u64, max_bytes: usize) -> Window {
    let schema = test_schema();
    Window::new(
        WindowParams {
            name: "test_win".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(over_secs),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(max_bytes),
    )
}

// 测试按主题拆分为兄弟子模块（`#[path]` 相对本文件目录，机制同 compile_tests.rs）。
#[path = "tests_cursor.rs"]
mod tests_cursor;
#[path = "tests_eviction.rs"]
mod tests_eviction;
#[path = "tests_join.rs"]
mod tests_join;
#[path = "tests_state.rs"]
mod tests_state;
