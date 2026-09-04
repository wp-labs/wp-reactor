use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
use wf_engine::match_engine::Value;
use wf_engine::window::{ProviderWindow, WindowDef, WindowParams, WindowRegistry};

// Items from the receiver module needed by tests.
// Cannot use `use super::*;` because receiver::arrow shadows the arrow crate.
use super::arrow::{replay_arrow_framed_file, replay_arrow_ipc_file};
use super::batch::build_record_batch_from_json;
use super::csv::replay_csv_file;
use super::ndjson::replay_ndjson_file;
use super::route::{batch_machine_id, coerce_column, coerce_column_for_field, prepare_batch};
use super::{DEFAULT_STREAM_TAG_FIELD, ReplayRoute};
use crate::lifecycle::ingest::{IngestLimiter, route_and_dispatch};
use crate::metrics::{MetricsRecord, RuntimeMetrics};
use wf_engine::match_engine::{
    WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY, WFL_FIELD_TYPE_OBJECT,
};
use wf_engine::window::Router;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("value", DataType::Int64, true),
    ]))
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

fn utf8_field_with_wfl_kind(name: &str, kind: &str) -> Field {
    Field::new(name, DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
        WFL_FIELD_TYPE_METADATA_KEY.to_string(),
        kind.to_string(),
    )]))
}

fn test_config() -> WindowConfig {
    WindowConfig {
        name: "default".into(),
        mode: DistMode::Local,
        max_window_bytes: usize::MAX.into(),
        over_cap: Duration::from_secs(3600).into(),
        evict_policy: EvictPolicy::TimeFirst,
        watermark: Duration::from_secs(0).into(),
        allowed_lateness: Duration::from_secs(3600).into(),
        late_policy: LatePolicy::Drop,
        table: None,
    }
}

fn make_router(stream_name: &str) -> Arc<Router> {
    let mut reg = WindowRegistry::build(vec![WindowDef {
        params: WindowParams {
            name: "test_win".into(),
            schema: test_schema(),
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec![stream_name.to_string()],
        config: test_config(),
    }])
    .unwrap();
    register_miss_provider(&mut reg);
    Arc::new(Router::new(reg))
}

fn make_multi_stream_router() -> Arc<Router> {
    let mut reg = WindowRegistry::build(vec![
        WindowDef {
            params: WindowParams {
                name: "win_a".into(),
                schema: test_schema(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec!["a".to_string()],
            config: test_config(),
        },
        WindowDef {
            params: WindowParams {
                name: "win_b".into(),
                schema: test_schema(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec!["b".to_string()],
            config: test_config(),
        },
    ])
    .unwrap();
    register_miss_provider(&mut reg);
    Arc::new(Router::new(reg))
}

fn register_miss_provider(registry: &mut WindowRegistry) {
    registry
        .register_provider(
            crate::receiver::miss::WINDOW_MISS_WINDOW_NAME.to_string(),
            ProviderWindow::new(
                crate::receiver::miss::WINDOW_MISS_WINDOW_NAME.to_string(),
                "internal://window_miss".to_string(),
                None,
            ),
        )
        .unwrap();
}

/// Build a router + a real parse worker pool (R2/R3), returning the router, the
/// parse channel sender, and the shared seq counter the replay functions push
/// through. The parse workers run on the test's tokio runtime, so tests poll
/// the window with [`wait_for_rows`] instead of asserting synchronously.
fn make_parse_router(stream_name: &str) -> (Arc<Router>, Arc<AtomicU64>) {
    (make_router(stream_name), Arc::new(AtomicU64::new(0)))
}

fn make_multi_parse_router() -> (Arc<Router>, Arc<AtomicU64>) {
    (make_multi_stream_router(), Arc::new(AtomicU64::new(0)))
}

/// Poll the test window until it holds at least `expected` rows, yielding to the
/// tokio runtime so the parse/commit workers can drain the pushed batches.
async fn wait_for_rows(router: &Router, expected: usize) {
    wait_for_rows_for(router, "test_win", expected).await;
}

/// [`wait_for_rows`] for a named window.
async fn wait_for_rows_for(router: &Router, window: &str, expected: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if snapshot_row_count_for(router, window) >= expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {} rows in {} (have {})",
            expected,
            window,
            snapshot_row_count_for(router, window)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn snapshot_row_count_for(router: &Router, window: &str) -> usize {
    router
        .registry()
        .snapshot(window)
        .unwrap_or_default()
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

fn window_miss_metric_value(records: &[MetricsRecord], source: &str, reason: &str) -> u64 {
    records
        .iter()
        .find(|record| {
            record
                .fields
                .iter()
                .any(|(k, v)| k == "name" && v == "window_miss_total")
                && record
                    .fields
                    .iter()
                    .any(|(k, v)| k == "label" && v == source)
                && record
                    .fields
                    .iter()
                    .any(|(k, v)| k == "reason" && v == reason)
        })
        .and_then(|record| {
            record
                .fields
                .iter()
                .find(|(k, _)| k == "value")
                .and_then(|(_, v)| v.parse().ok())
        })
        .unwrap_or(0)
}

fn window_miss_snapshot_count(
    router: &Router,
    source: &str,
    stream_tag: &str,
    reason: &str,
) -> u64 {
    router
        .registry()
        .provider_snapshot(crate::receiver::miss::WINDOW_MISS_WINDOW_NAME)
        .unwrap_or_default()
        .iter()
        .find(|row| {
            string_value(row, "source_name") == Some(source)
                && string_value(row, "stream_tag") == Some(stream_tag)
                && string_value(row, "reason") == Some(reason)
        })
        .and_then(|row| number_value(row, "count"))
        .unwrap_or(0.0) as u64
}

fn string_value<'a>(
    row: &'a std::collections::HashMap<String, Value>,
    key: &str,
) -> Option<&'a str> {
    match row.get(key) {
        Some(Value::Str(value)) => Some(value),
        _ => None,
    }
}

fn number_value(row: &std::collections::HashMap<String, Value>, key: &str) -> Option<f64> {
    match row.get(key) {
        Some(Value::Number(value)) => Some(*value),
        _ => None,
    }
}

// 测试用例按主题拆为 #[path] 兄弟子模块（共享 harness/import 留本文件，子文件顶部 //! 说明主题）：
#[path = "receiver_tests_file_replay.rs"]
mod receiver_tests_file_replay;

#[path = "receiver_tests_arrow_coerce.rs"]
mod receiver_tests_arrow_coerce;

#[path = "receiver_tests_route_dispatch.rs"]
mod receiver_tests_route_dispatch;
