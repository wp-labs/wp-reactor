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

#[tokio::test]
async fn file_ndjson_replay_routes_rows() {
    let (router, parse_seq) = make_parse_router("events");
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.ndjson");
    std::fs::write(
        &file_path,
        r#"{"ts":1000000000,"value":1}
{"ts":"2000000000","value":"2"}
"#,
    )
    .unwrap();

    replay_ndjson_file(
        &file_path,
        ReplayRoute {
            stream_name: "events",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &[wf_lang::WindowSchema {
            name: "test_win".to_string(),
            streams: vec!["events".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        }],
        Arc::clone(&router),
        None,
        Arc::clone(&parse_seq),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    wait_for_rows(&router, 2).await;
}

#[tokio::test]
async fn file_ndjson_replay_routes_rows_by_row_stream() {
    let (router, parse_seq) = make_multi_parse_router();
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.ndjson");
    std::fs::write(
        &file_path,
        r#"{"wp_oml_name":"a","ts":1000000000,"value":1}
{"wp_oml_name":"b","ts":"2000000000","value":"2"}
{"wp_oml_name":"a","ts":3000000000,"value":3}
"#,
    )
    .unwrap();

    let schemas = vec![
        wf_lang::WindowSchema {
            name: "win_a".to_string(),
            streams: vec!["a".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        },
        wf_lang::WindowSchema {
            name: "win_b".to_string(),
            streams: vec!["b".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        },
    ];

    replay_ndjson_file(
        &file_path,
        ReplayRoute {
            stream_name: "",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &schemas,
        Arc::clone(&router),
        None,
        Arc::clone(&parse_seq),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    wait_for_rows_for(&router, "win_a", 2).await;
    wait_for_rows_for(&router, "win_b", 1).await;
}

#[tokio::test]
async fn file_ndjson_dynamic_unknown_stream_is_window_miss() {
    let (router, parse_seq) = make_multi_parse_router();
    let metrics = Arc::new(RuntimeMetrics::new(
        &[],
        &["win_a".to_string(), "win_b".to_string()],
        &["test_source".to_string()],
        BTreeMap::new(),
    ));
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.ndjson");
    std::fs::write(
        &file_path,
        r#"{"wp_oml_name":"a","ts":1000000000,"value":1}
{"wp_oml_name":"unknown","ts":2000000000,"value":2}
{"ts":3000000000,"value":3}
{"wp_oml_name":"b","ts":4000000000,"value":4}
"#,
    )
    .unwrap();

    let schemas = vec![
        wf_lang::WindowSchema {
            name: "win_a".to_string(),
            streams: vec!["a".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        },
        wf_lang::WindowSchema {
            name: "win_b".to_string(),
            streams: vec!["b".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        },
    ];

    replay_ndjson_file(
        &file_path,
        ReplayRoute {
            stream_name: "",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &schemas,
        Arc::clone(&router),
        Some(Arc::clone(&metrics)),
        Arc::clone(&parse_seq),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    wait_for_rows_for(&router, "win_a", 1).await;
    wait_for_rows_for(&router, "win_b", 1).await;

    let records = metrics.snapshot().to_records();
    assert_eq!(
        window_miss_metric_value(&records, "test_source", "unknown_stream_schema"),
        1
    );
    assert_eq!(
        window_miss_metric_value(&records, "test_source", "missing_stream_tag_field"),
        1
    );
    assert_eq!(
        window_miss_snapshot_count(&router, "test_source", "unknown", "unknown_stream_schema"),
        1
    );
    assert_eq!(
        window_miss_snapshot_count(&router, "test_source", "", "missing_stream_tag_field"),
        1
    );
}

#[tokio::test]
async fn file_csv_replay_routes_rows_by_stream_tag_field_column() {
    let (router, parse_seq) = make_multi_parse_router();
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.csv");
    std::fs::write(
        &file_path,
        "wp_oml_name,ts,value\n\
a,1000000000,1\n\
b,2000000000,2\n\
a,3000000000,3\n",
    )
    .unwrap();

    let schemas = vec![
        wf_lang::WindowSchema {
            name: "win_a".to_string(),
            streams: vec!["a".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        },
        wf_lang::WindowSchema {
            name: "win_b".to_string(),
            streams: vec!["b".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        },
    ];

    replay_csv_file(
        &file_path,
        ReplayRoute {
            stream_name: "",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &schemas,
        Arc::clone(&router),
        None,
        Arc::clone(&parse_seq),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    wait_for_rows_for(&router, "win_a", 2).await;
    wait_for_rows_for(&router, "win_b", 1).await;
}

#[tokio::test]
async fn file_csv_dynamic_unknown_stream_is_window_miss() {
    let (router, parse_seq) = make_multi_parse_router();
    let metrics = Arc::new(RuntimeMetrics::new(
        &[],
        &["win_a".to_string(), "win_b".to_string()],
        &["test_source".to_string()],
        BTreeMap::new(),
    ));
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.csv");
    std::fs::write(
        &file_path,
        "wp_oml_name,ts,value\n\
a,1000000000,1\n\
unknown,2000000000,2\n\
,3000000000,3\n\
b,4000000000,4\n",
    )
    .unwrap();

    let schemas = vec![
        wf_lang::WindowSchema {
            name: "win_a".to_string(),
            streams: vec!["a".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        },
        wf_lang::WindowSchema {
            name: "win_b".to_string(),
            streams: vec!["b".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        },
    ];

    replay_csv_file(
        &file_path,
        ReplayRoute {
            stream_name: "",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &schemas,
        Arc::clone(&router),
        Some(Arc::clone(&metrics)),
        Arc::clone(&parse_seq),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    wait_for_rows_for(&router, "win_a", 1).await;
    wait_for_rows_for(&router, "win_b", 1).await;

    let records = metrics.snapshot().to_records();
    assert_eq!(
        window_miss_metric_value(&records, "test_source", "unknown_stream_schema"),
        1
    );
    assert_eq!(
        window_miss_metric_value(&records, "test_source", "missing_stream_tag_field"),
        1
    );
    assert_eq!(
        window_miss_snapshot_count(&router, "test_source", "unknown", "unknown_stream_schema"),
        1
    );
    assert_eq!(
        window_miss_snapshot_count(&router, "test_source", "", "missing_stream_tag_field"),
        1
    );
}

#[test]
fn json_time_column_accepts_epoch_millis() {
    let rows = vec![
        serde_json::json!({
            "ts": 1_700_000_000_000i64,
            "value": 1
        })
        .as_object()
        .unwrap()
        .clone(),
    ];

    let batch = build_record_batch_from_json(&test_schema(), &rows).unwrap();
    let ts = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();

    assert_eq!(ts.value(0), 1_700_000_000_000_000_000);
}

#[test]
fn json_time_column_accepts_rfc3339_string() {
    let rows = vec![
        serde_json::json!({
            "ts": "2023-11-14T22:13:20Z",
            "value": 1
        })
        .as_object()
        .unwrap()
        .clone(),
    ];

    let batch = build_record_batch_from_json(&test_schema(), &rows).unwrap();
    let ts = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();

    assert_eq!(ts.value(0), 1_700_000_000_000_000_000);
}

#[test]
fn json_object_and_array_columns_are_serialized_to_utf8() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ctx", DataType::Utf8, true),
        Field::new("tags", DataType::Utf8, true),
    ]));
    let rows = vec![
        serde_json::json!({
            "ctx": {"score": 70.5, "source": "auth"},
            "tags": ["bruteforce", "ssh"]
        })
        .as_object()
        .unwrap()
        .clone(),
    ];

    let batch = build_record_batch_from_json(&schema, &rows).unwrap();
    let ctx = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let tags = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(ctx.value(0), r#"{"score":70.5,"source":"auth"}"#);
    assert_eq!(tags.value(0), r#"["bruteforce","ssh"]"#);
}

#[test]
fn typed_array_schema_uses_utf8_storage_for_file_sources() {
    use crate::receiver::schema::field_type_to_arrow;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "ports",
        field_type_to_arrow(&wf_lang::FieldType::Array(wf_lang::BaseType::Digit)),
        true,
    )]));
    let rows = vec![
        serde_json::json!({
            "ports": [22, 2222]
        })
        .as_object()
        .unwrap()
        .clone(),
    ];

    let batch = build_record_batch_from_json(&schema, &rows).unwrap();
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    let ports = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ports.value(0), r#"[22,2222]"#);
}

#[test]
fn structured_stream_schema_accepts_arrow_struct_input() {
    use crate::receiver::schema::validate_batch_schema_for_stream;
    use arrow::array::StructArray;

    let schemas = vec![wf_lang::WindowSchema {
        name: "alerts".to_string(),
        streams: vec!["events".to_string()],
        time_field: None,
        over: Duration::ZERO,
        fields: vec![wf_lang::FieldDef {
            name: "extension".to_string(),
            field_type: wf_lang::FieldType::Object,
        }],
    }];
    let extension = StructArray::from(vec![(
        Arc::new(Field::new("severity", DataType::Int64, true)),
        Arc::new(Int64Array::from(vec![10])) as arrow::array::ArrayRef,
    )]);
    let batch_schema = Schema::new(vec![Field::new(
        "extension",
        extension.data_type().clone(),
        true,
    )]);

    validate_batch_schema_for_stream(&schemas, "events", &batch_schema).unwrap();
}

#[test]
fn structured_array_stream_schema_accepts_arrow_list_input() {
    use crate::receiver::schema::validate_batch_schema_for_stream;
    use arrow::array::ListArray;
    use arrow::datatypes::Int64Type;

    let schemas = vec![wf_lang::WindowSchema {
        name: "alerts".to_string(),
        streams: vec!["events".to_string()],
        time_field: None,
        over: Duration::ZERO,
        fields: vec![wf_lang::FieldDef {
            name: "ports".to_string(),
            field_type: wf_lang::FieldType::Array(wf_lang::BaseType::Digit),
        }],
    }];
    let ports =
        ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(22), Some(2222)])]);
    let batch_schema = Schema::new(vec![Field::new("ports", ports.data_type().clone(), true)]);

    validate_batch_schema_for_stream(&schemas, "events", &batch_schema).unwrap();
}

#[test]
fn structured_object_stream_schema_rejects_arrow_list_input() {
    use crate::receiver::schema::validate_batch_schema_for_stream;
    use arrow::array::ListArray;
    use arrow::datatypes::Int64Type;

    let schemas = vec![wf_lang::WindowSchema {
        name: "alerts".to_string(),
        streams: vec!["events".to_string()],
        time_field: None,
        over: Duration::ZERO,
        fields: vec![wf_lang::FieldDef {
            name: "extension".to_string(),
            field_type: wf_lang::FieldType::Object,
        }],
    }];
    let extension =
        ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(10), Some(20)])]);
    let batch_schema = Schema::new(vec![Field::new(
        "extension",
        extension.data_type().clone(),
        true,
    )]);

    assert!(validate_batch_schema_for_stream(&schemas, "events", &batch_schema).is_err());
}

#[test]
fn structured_array_stream_schema_rejects_arrow_struct_input() {
    use crate::receiver::schema::validate_batch_schema_for_stream;
    use arrow::array::StructArray;

    let schemas = vec![wf_lang::WindowSchema {
        name: "alerts".to_string(),
        streams: vec!["events".to_string()],
        time_field: None,
        over: Duration::ZERO,
        fields: vec![wf_lang::FieldDef {
            name: "tags".to_string(),
            field_type: wf_lang::FieldType::ArrayAny,
        }],
    }];
    let tags = StructArray::from(vec![(
        Arc::new(Field::new("tag", DataType::Utf8, true)),
        Arc::new(StringArray::from(vec!["ssh"])) as arrow::array::ArrayRef,
    )]);
    let batch_schema = Schema::new(vec![Field::new("tags", tags.data_type().clone(), true)]);

    assert!(validate_batch_schema_for_stream(&schemas, "events", &batch_schema).is_err());
}

#[test]
fn structured_object_stream_schema_rejects_utf8_array_metadata() {
    use crate::receiver::schema::validate_batch_schema_for_stream;

    let schemas = vec![wf_lang::WindowSchema {
        name: "alerts".to_string(),
        streams: vec!["events".to_string()],
        time_field: None,
        over: Duration::ZERO,
        fields: vec![wf_lang::FieldDef {
            name: "extension".to_string(),
            field_type: wf_lang::FieldType::Object,
        }],
    }];
    let batch_schema = Schema::new(vec![utf8_field_with_wfl_kind(
        "extension",
        WFL_FIELD_TYPE_ARRAY,
    )]);

    assert!(validate_batch_schema_for_stream(&schemas, "events", &batch_schema).is_err());
}

#[test]
fn structured_array_stream_schema_rejects_utf8_object_metadata() {
    use crate::receiver::schema::validate_batch_schema_for_stream;

    let schemas = vec![wf_lang::WindowSchema {
        name: "alerts".to_string(),
        streams: vec!["events".to_string()],
        time_field: None,
        over: Duration::ZERO,
        fields: vec![wf_lang::FieldDef {
            name: "ports".to_string(),
            field_type: wf_lang::FieldType::Array(wf_lang::BaseType::Digit),
        }],
    }];
    let batch_schema = Schema::new(vec![utf8_field_with_wfl_kind(
        "ports",
        WFL_FIELD_TYPE_OBJECT,
    )]);

    assert!(validate_batch_schema_for_stream(&schemas, "events", &batch_schema).is_err());
}

#[tokio::test]
async fn file_arrow_framed_replay_routes_rows() {
    let (router, parse_seq) = make_parse_router("events");
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.arrow_framed");
    let schema = test_schema();
    let batch_a = make_batch(&schema, &[1_000_000_000], &[1]);
    let batch_b = make_batch(&schema, &[2_000_000_000], &[2]);

    {
        let payload_a = wp_arrow::ipc::encode_ipc("events", &batch_a).unwrap();
        let payload_b = wp_arrow::ipc::encode_ipc("events", &batch_b).unwrap();
        let mut body = Vec::new();
        // `len` framing: `<ascii digits> <payload>` (matches the TCP sink /
        // dump-frames wire format read by `read_frame`).
        body.extend_from_slice(format!("{} ", payload_a.len()).as_bytes());
        body.extend_from_slice(&payload_a);
        body.extend_from_slice(format!("{} ", payload_b.len()).as_bytes());
        body.extend_from_slice(&payload_b);
        std::fs::write(&file_path, body).unwrap();
    }

    replay_arrow_framed_file(
        &file_path,
        "",
        "test_source",
        &[wf_lang::WindowSchema {
            name: "test_win".to_string(),
            streams: vec!["events".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        }],
        Arc::clone(&router),
        None,
        Arc::clone(&parse_seq),
        CancellationToken::new(),
        None,
    )
    .await
    .unwrap();

    wait_for_rows(&router, 2).await;
}

#[tokio::test]
async fn file_arrow_framed_unknown_tag_is_window_miss() {
    let (router, parse_seq) = make_parse_router("events");
    let metrics = Arc::new(RuntimeMetrics::new(
        &[],
        &["test_win".to_string()],
        &["test_source".to_string()],
        BTreeMap::new(),
    ));
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.arrow_framed");
    let schema = test_schema();
    let known = make_batch(&schema, &[1_000_000_000], &[1]);
    let unknown = make_batch(&schema, &[2_000_000_000], &[2]);

    {
        let payload_known = wp_arrow::ipc::encode_ipc("events", &known).unwrap();
        let payload_unknown = wp_arrow::ipc::encode_ipc("unknown", &unknown).unwrap();
        let mut body = Vec::new();
        body.extend_from_slice(format!("{} ", payload_known.len()).as_bytes());
        body.extend_from_slice(&payload_known);
        body.extend_from_slice(format!("{} ", payload_unknown.len()).as_bytes());
        body.extend_from_slice(&payload_unknown);
        std::fs::write(&file_path, body).unwrap();
    }

    replay_arrow_framed_file(
        &file_path,
        "",
        "test_source",
        &[wf_lang::WindowSchema {
            name: "test_win".to_string(),
            streams: vec!["events".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        }],
        Arc::clone(&router),
        Some(Arc::clone(&metrics)),
        Arc::clone(&parse_seq),
        CancellationToken::new(),
        None,
    )
    .await
    .unwrap();

    wait_for_rows(&router, 1).await;
    let records = metrics.snapshot().to_records();
    assert_eq!(
        window_miss_metric_value(&records, "test_source", "unknown_stream_schema"),
        1
    );
    assert_eq!(
        window_miss_snapshot_count(&router, "test_source", "unknown", "unknown_stream_schema"),
        1
    );
}

#[tokio::test]
async fn file_arrow_ipc_replay_routes_rows() {
    let (router, parse_seq) = make_parse_router("events");
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.arrow_ipc");
    let schema = test_schema();
    let batch_a = make_batch(&schema, &[1_000_000_000], &[1]);
    let batch_b = make_batch(&schema, &[2_000_000_000], &[2]);

    {
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch_a).unwrap();
        writer.write(&batch_b).unwrap();
        writer.finish().unwrap();
    }

    replay_arrow_ipc_file(
        &file_path,
        "events",
        "test_source",
        &[wf_lang::WindowSchema {
            name: "test_win".to_string(),
            streams: vec!["events".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        }],
        Arc::clone(&router),
        None,
        Arc::clone(&parse_seq),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    wait_for_rows(&router, 2).await;
}

// ---- coerce_column ----

#[test]
fn coerce_utf8_to_int64() {
    let arr: arrow::array::ArrayRef = Arc::new(StringArray::from(vec!["42", "99", "bad"]));
    let result = coerce_column(&arr, &DataType::Int64, 3);
    let ints = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ints.value(0), 42);
    assert_eq!(ints.value(1), 99);
    assert!(ints.is_null(2));
}

#[test]
fn coerce_utf8_to_float64() {
    let arr: arrow::array::ArrayRef = Arc::new(StringArray::from(vec!["1.5", "2.0", "x"]));
    let result = coerce_column(&arr, &DataType::Float64, 3);
    let floats = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((floats.value(0) - 1.5).abs() < 1e-10);
    assert!((floats.value(1) - 2.0).abs() < 1e-10);
    assert!(floats.is_null(2));
}

#[test]
fn coerce_utf8_to_bool() {
    let arr: arrow::array::ArrayRef = Arc::new(StringArray::from(vec!["true", "false", "1", "0"]));
    let result = coerce_column(&arr, &DataType::Boolean, 4);
    let bools = result.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(bools.value(0));
    assert!(!bools.value(1));
    assert!(bools.value(2));
    assert!(!bools.value(3));
}

#[test]
fn coerce_int64_to_float64() {
    let arr: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let result = coerce_column(&arr, &DataType::Float64, 3);
    let floats = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((floats.value(0) - 1.0).abs() < 1e-10);
    assert!((floats.value(1) - 2.0).abs() < 1e-10);
}

#[test]
fn coerce_float64_to_int64() {
    let arr: arrow::array::ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.5, 3.9]));
    let result = coerce_column(&arr, &DataType::Int64, 3);
    let ints = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ints.value(0), 1);
    assert_eq!(ints.value(1), 2);
    assert_eq!(ints.value(2), 3);
}

#[test]
fn coerce_int64_to_utf8() {
    let arr: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![42, 99]));
    let result = coerce_column(&arr, &DataType::Utf8, 2);
    let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "42");
    assert_eq!(strings.value(1), "99");
}

#[test]
fn coerce_structured_field_to_utf8_json() {
    use crate::receiver::schema::field_to_arrow;
    use arrow::array::{ArrayRef, StructArray};

    let extension = StructArray::from(vec![
        (
            Arc::new(Field::new("severity", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(10), None])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("source", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("wfl"), Some("test")])) as ArrayRef,
        ),
    ]);
    let col = Arc::new(extension) as ArrayRef;
    let target = field_to_arrow("extension", &wf_lang::FieldType::Object);

    let result = coerce_column_for_field(&col, &target, 2);
    let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), r#"{"severity":10,"source":"wfl"}"#);
    assert_eq!(strings.value(1), r#"{"source":"test"}"#);
}

#[test]
fn coerce_structured_array_field_to_utf8_json() {
    use crate::receiver::schema::field_to_arrow;
    use arrow::array::{ArrayRef, ListArray};
    use arrow::datatypes::Int64Type;

    let ports = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(22), Some(2222)]),
        Some(vec![Some(443)]),
    ]);
    let col = Arc::new(ports) as ArrayRef;
    let target = field_to_arrow(
        "ports",
        &wf_lang::FieldType::Array(wf_lang::BaseType::Digit),
    );

    let result = coerce_column_for_field(&col, &target, 2);
    let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), r#"[22,2222]"#);
    assert_eq!(strings.value(1), r#"[443]"#);
}

#[test]
fn coerce_same_type_noop() {
    let arr: arrow::array::ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let result = coerce_column(&arr, &DataType::Utf8, 2);
    // Same type should return the original column (clone)
    let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "a");
    assert_eq!(strings.value(1), "b");
}

#[test]
fn route_projects_plain_utf8_json_into_structured_window_schema() {
    use crate::receiver::schema::field_to_arrow;
    use wf_engine::match_engine::batch_to_events;

    let schema = Arc::new(Schema::new(vec![field_to_arrow(
        "extension",
        &wf_lang::FieldType::Object,
    )]));
    let mut reg = WindowRegistry::build(vec![WindowDef {
        params: WindowParams {
            name: "alerts".into(),
            schema,
            time_col_index: None,
            over: Duration::ZERO,
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec!["events".to_string()],
        config: test_config(),
    }])
    .unwrap();
    register_miss_provider(&mut reg);
    let router = Router::new(reg);
    let source_schema = Arc::new(Schema::new(vec![Field::new(
        "extension",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        source_schema,
        vec![Arc::new(StringArray::from(vec![r#"{"severity":10}"#])) as arrow::array::ArrayRef],
    )
    .unwrap();

    // R3: routing now goes through the parse pool, whose route_parse/route_commit
    // do not project — `prepare_batch` must produce the schema-conformant batch
    // before push. Verify the projection directly.
    let projected = prepare_batch("events", &batch, &router);
    assert_eq!(
        projected
            .schema()
            .field(0)
            .metadata()
            .get(WFL_FIELD_TYPE_METADATA_KEY)
            .map(String::as_str),
        Some("object")
    );
    let events = batch_to_events(&projected);
    let Value::Object(extension) = &events[0].fields["extension"] else {
        panic!("expected extension object");
    };
    assert_eq!(extension.get("severity"), Some(&Value::Number(10.0)));
}

#[test]
fn route_projects_wrong_structured_utf8_metadata_into_target_window_schema() {
    use crate::receiver::schema::field_to_arrow;
    use wf_engine::match_engine::batch_to_events;

    let schema = Arc::new(Schema::new(vec![field_to_arrow(
        "extension",
        &wf_lang::FieldType::Object,
    )]));
    let mut reg = WindowRegistry::build(vec![WindowDef {
        params: WindowParams {
            name: "alerts".into(),
            schema,
            time_col_index: None,
            over: Duration::ZERO,
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec!["events".to_string()],
        config: test_config(),
    }])
    .unwrap();
    register_miss_provider(&mut reg);
    let router = Router::new(reg);
    let source_schema = Arc::new(Schema::new(vec![utf8_field_with_wfl_kind(
        "extension",
        WFL_FIELD_TYPE_ARRAY,
    )]));
    let batch = RecordBatch::try_new(
        source_schema,
        vec![Arc::new(StringArray::from(vec![r#"{"severity":10}"#])) as arrow::array::ArrayRef],
    )
    .unwrap();

    // R3: routing goes through the parse pool — `prepare_batch` must coerce the
    // Utf8 field (carrying array metadata) to the target Object schema.
    let projected = prepare_batch("events", &batch, &router);
    assert_eq!(
        projected
            .schema()
            .field(0)
            .metadata()
            .get(WFL_FIELD_TYPE_METADATA_KEY)
            .map(String::as_str),
        Some(WFL_FIELD_TYPE_OBJECT)
    );
    let events = batch_to_events(&projected);
    let Value::Object(extension) = &events[0].fields["extension"] else {
        panic!("expected extension object");
    };
    assert_eq!(extension.get("severity"), Some(&Value::Number(10.0)));
}

#[test]
fn coerce_utf8_to_timestamp_accepts_epoch_millis_and_rfc3339() {
    let arr: arrow::array::ArrayRef = Arc::new(StringArray::from(vec![
        "1700000000000",
        "2023-11-14T22:13:20Z",
    ]));
    let result = coerce_column(&arr, &DataType::Timestamp(TimeUnit::Nanosecond, None), 2);
    let ts = result
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(ts.value(0), 1_700_000_000_000_000_000);
    assert_eq!(ts.value(1), 1_700_000_000_000_000_000);
}

#[test]
fn coerce_unmatched_falls_back_to_null() {
    let arr: arrow::array::ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
    let result = coerce_column(&arr, &DataType::Timestamp(TimeUnit::Nanosecond, None), 2);
    assert_eq!(result.len(), 2);
    // Unmatched type falls back to NullArray (null values for all rows)
}

fn machine_batch(cols: Vec<(&str, Vec<&str>)>) -> RecordBatch {
    use arrow::array::ArrayRef;

    let fields: Vec<_> = cols
        .iter()
        .map(|(n, _)| Field::new(*n, DataType::Utf8, true))
        .collect();
    let arrays: Vec<ArrayRef> = cols
        .iter()
        .map(|(_, v)| {
            Arc::new(StringArray::from(
                v.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

#[test]
fn test_batch_machine_id() {
    let b = machine_batch(vec![("msg", vec!["hello"])]);
    assert_eq!(batch_machine_id(&b), None);

    let b = machine_batch(vec![(
        wf_engine::match_engine::MACHINE_ID,
        vec!["10.0.0.1"],
    )]);
    assert_eq!(batch_machine_id(&b), Some("10.0.0.1".to_string()));

    let b = machine_batch(vec![(
        wf_engine::match_engine::MACHINE_ID,
        vec!["10.0.0.1", "10.0.0.2"],
    )]);
    assert_eq!(batch_machine_id(&b), Some("10.0.0.1".to_string()));
}

// ---------------------------------------------------------------------------
// decode-route-merge: inline route_and_dispatch (source-side, no parse pool)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn route_and_dispatch_commits_inline() {
    let (router, parse_seq) = make_parse_router("events");
    let batch = make_batch(&test_schema(), &[1_000_000_000, 2_000_000_000], &[1, 2]);
    // Sync mode (no mailbox registered): dispatch_parsed falls back to
    // inline commit_window — the single source loop is already ordered.
    route_and_dispatch(&parse_seq, "src", "events", batch, &router, None, None).await;
    wait_for_rows(&router, 2).await;
}

#[tokio::test]
async fn route_and_dispatch_records_receiver_metrics() {
    let (router, parse_seq) = make_parse_router("events");
    let metrics = Arc::new(RuntimeMetrics::new(
        &[],
        &["test_win".to_string()],
        &["src".to_string()],
        BTreeMap::new(),
    ));
    let batch = make_batch(&test_schema(), &[1_000_000_000], &[1]);
    route_and_dispatch(
        &parse_seq,
        "src",
        "events",
        batch,
        &router,
        Some(&metrics),
        None,
    )
    .await;

    let records = metrics.snapshot().to_records();
    let rows = records.iter().find(|r| {
        r.fields
            .iter()
            .any(|(k, v)| k == "name" && v == "rows_total")
            && r.fields.iter().any(|(k, v)| k == "label" && v == "src")
    });
    let Some(rows) = rows else {
        panic!("expected receiver rows_total metric for source 'src'");
    };
    let value: u64 = rows
        .fields
        .iter()
        .find(|(k, _)| k == "value")
        .expect("value field")
        .1
        .parse()
        .expect("numeric value");
    assert_eq!(value, 1);
    wait_for_rows(&router, 1).await;
}

/// decode-route-merge 保序契约：内联 `route_and_dispatch` 必须为每
/// (source, window) 分配无缝递增的 seq——这是 actor 重排游标的输入。
/// 直接订阅 mailbox，断言 `WindowMsg::Append` 的 seq 流。
#[tokio::test]
async fn route_and_dispatch_allocates_contiguous_window_seqs() {
    use wf_engine::window::{WINDOW_CHANNEL_DEPTH, WindowMailbox, WindowMsg};

    let router = make_multi_stream_router();
    let (tx, mut rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
    router.register_mailbox(
        "win_a",
        WindowMailbox {
            tx,
            budget: Arc::new(tokio::sync::Semaphore::new(4 * 1024 * 1024)),
            budget_bytes: 4 * 1024 * 1024,
        },
    );
    let parse_seq = Arc::new(AtomicU64::new(0));
    let schema = test_schema();
    for round in 0..3u64 {
        let batch = make_batch(
            &schema,
            &[(1_000_000_000 + round * 1_000_000) as i64; 2],
            &[round as i64, round as i64],
        );
        route_and_dispatch(&parse_seq, "src", "a", batch, &router, None, None).await;
    }
    for expected in 0..3u64 {
        match rx.recv().await.expect("mailbox message") {
            WindowMsg::Append { source, seq, .. } => {
                assert_eq!(source.as_ref(), "src");
                assert_eq!(seq, expected, "per-(source, window) seq must be gap-free");
            }
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected non-Append WindowMsg"),
        }
    }
}

/// 每 (source, window) 游标相互独立：不同 source 派发同一窗口时各自从 0 起
/// （多 handle 共享一个 seq 计数器的源配置互不串号）。
#[tokio::test]
async fn route_and_dispatch_per_source_window_seqs_are_independent() {
    use wf_engine::window::{WINDOW_CHANNEL_DEPTH, WindowMailbox, WindowMsg};

    let router = make_multi_stream_router();
    let (tx, mut rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
    router.register_mailbox(
        "win_a",
        WindowMailbox {
            tx,
            budget: Arc::new(tokio::sync::Semaphore::new(4 * 1024 * 1024)),
            budget_bytes: 4 * 1024 * 1024,
        },
    );
    let batch = make_batch(&test_schema(), &[1_000_000_000, 2_000_000_000], &[1, 2]);
    route_and_dispatch(
        &AtomicU64::new(0),
        "src_a",
        "a",
        batch.clone(),
        &router,
        None,
        None,
    )
    .await;
    route_and_dispatch(&AtomicU64::new(0), "src_b", "a", batch, &router, None, None).await;
    for expected_source in ["src_a", "src_b"] {
        match rx.recv().await.expect("mailbox message") {
            WindowMsg::Append { source, seq, .. } => {
                assert_eq!(source.as_ref(), expected_source);
                assert_eq!(seq, 0, "each source's (source, window) cursor starts at 0");
            }
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected non-Append WindowMsg"),
        }
    }
}

/// actor mailbox 已死（接收端 drop）：dispatch 记 warn 并继续，不 hang、不
/// panic——"一个死 actor 不得阻塞源任务"在源侧内联后必须仍然成立。
#[tokio::test]
async fn route_and_dispatch_dead_mailbox_does_not_hang() {
    use wf_engine::window::{WINDOW_CHANNEL_DEPTH, WindowMailbox, WindowMsg};

    let (router, parse_seq) = make_parse_router("events");
    let (tx, rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
    drop(rx);
    router.register_mailbox(
        "test_win",
        WindowMailbox {
            tx,
            budget: Arc::new(tokio::sync::Semaphore::new(4 * 1024 * 1024)),
            budget_bytes: 4 * 1024 * 1024,
        },
    );
    let batch = make_batch(&test_schema(), &[1_000_000_000], &[1]);
    // 两次派发都必须返回（内部 warn + 丢该窗副本、释放预算 permits）。
    route_and_dispatch(
        &parse_seq,
        "src",
        "events",
        batch.clone(),
        &router,
        None,
        None,
    )
    .await;
    route_and_dispatch(&parse_seq, "src", "events", batch, &router, None, None).await;
}

/// 未知流（无订阅窗口）：route/dispatch 空转，不 panic、不落任何窗口；
/// 真实流不受影响。
#[tokio::test]
async fn route_and_dispatch_unknown_stream_is_noop() {
    let (router, parse_seq) = make_parse_router("events");
    let batch = make_batch(&test_schema(), &[1_000_000_000], &[1]);
    route_and_dispatch(
        &parse_seq,
        "src",
        "no_such_stream",
        batch,
        &router,
        None,
        None,
    )
    .await;
    let batch = make_batch(&test_schema(), &[2_000_000_000], &[7]);
    route_and_dispatch(&parse_seq, "src", "events", batch, &router, None, None).await;
    wait_for_rows(&router, 1).await;
}

/// IngestLimiter 令牌桶限速：rate=2/s、每次 1 行 → 前两次耗尽初始令牌，
/// 第三次必须等待补票（~0.5s）；限速不丢数据，三批最终全部落窗。
#[tokio::test]
async fn route_and_dispatch_ingest_limiter_throttles() {
    let (router, parse_seq) = make_parse_router("events");
    let limiter = IngestLimiter::new(2);
    let batch = make_batch(&test_schema(), &[1_000_000_000], &[1]);
    route_and_dispatch(
        &parse_seq,
        "src",
        "events",
        batch.clone(),
        &router,
        None,
        Some(&limiter),
    )
    .await;
    route_and_dispatch(
        &parse_seq,
        "src",
        "events",
        batch.clone(),
        &router,
        None,
        Some(&limiter),
    )
    .await;
    let started = std::time::Instant::now();
    route_and_dispatch(
        &parse_seq,
        "src",
        "events",
        batch,
        &router,
        None,
        Some(&limiter),
    )
    .await;
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(400),
        "third dispatch must wait for token refill (~0.5s), got {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "throttle must not over-sleep, got {elapsed:?}"
    );
    wait_for_rows(&router, 3).await;
}

/// blocking IPC replay 端到端走 actor mailbox：spawn_blocking 线程上的
/// `Handle::block_on` 驱动完整 dispatch（含 mailbox 字节预算 acquire），
/// 预算由 runtime 线程上的 actor 消费释放——阻塞路径不能死锁，批次最终落窗。
/// 这是 §4.3 `route_and_dispatch_blocking` 方案的直接回归测试。
#[tokio::test]
async fn file_arrow_ipc_replay_routes_rows_through_actor_mailbox() {
    use wf_engine::window::{
        EvictionGate, WINDOW_CHANNEL_DEPTH, WindowMailbox, WindowMsg, run_window_actor,
    };

    let (router, parse_seq) = make_parse_router("events");
    let gate = Arc::new(EvictionGate::new(usize::MAX));
    let win = router.registry().get_window("test_win").unwrap();
    let notify = router.registry().get_notifier("test_win").unwrap();
    let (tx, rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
    router.register_mailbox(
        "test_win",
        WindowMailbox {
            tx,
            budget: Arc::new(tokio::sync::Semaphore::new(4 * 1024 * 1024)),
            budget_bytes: 4 * 1024 * 1024,
        },
    );
    let name: Arc<str> = Arc::from("test_win");
    let fanout = Arc::clone(router.fanout());
    let actor_cancel = CancellationToken::new().child_token();
    tokio::spawn(async move {
        run_window_actor(name, win, gate, fanout, notify, rx, actor_cancel, None).await;
    });

    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.arrow_ipc");
    let schema = test_schema();
    let batch_a = make_batch(&schema, &[1_000_000_000], &[1]);
    let batch_b = make_batch(&schema, &[2_000_000_000], &[2]);
    {
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch_a).unwrap();
        writer.write(&batch_b).unwrap();
        writer.finish().unwrap();
    }

    replay_arrow_ipc_file(
        &file_path,
        "events",
        "test_source",
        &[wf_lang::WindowSchema {
            name: "test_win".to_string(),
            streams: vec!["events".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        }],
        Arc::clone(&router),
        None,
        Arc::clone(&parse_seq),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    wait_for_rows(&router, 2).await;
}

/// Actor-mode regression for the full-speed pipeline deadlock: a *global*
/// per-source frame seq leaves permanent holes in each window's mailbox
/// sequence (a window only receives its own stream's frames), so the window
/// actor's reorder cursor parked every frame after the first hole and the
/// parked messages' byte-budget permits were never released. With two
/// interleaved streams the second stream's window never appended a single
/// row (and under sustained load the whole pipeline froze). The fix
/// allocates per-(source, window) contiguous seqs at the serialized
/// source-side frame builder — now the inlined `route_and_dispatch` call in
/// the source loop itself (decode-route-merge).
///
/// Before the fix this test timed out waiting for win_b's rows.
#[tokio::test]
async fn actor_mode_interleaved_streams_append_without_deadlock() {
    use tokio_util::sync::CancellationToken;
    use wf_engine::window::{
        EvictionGate, WINDOW_CHANNEL_DEPTH, WindowMailbox, WindowMsg, run_window_actor,
    };

    let router = make_multi_stream_router();
    let gate = Arc::new(EvictionGate::new(usize::MAX));
    for name in ["win_a", "win_b"] {
        let win = router.registry().get_window(name).unwrap();
        let notify = router.registry().get_notifier(name).unwrap();
        let (tx, rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
        router.register_mailbox(
            name,
            WindowMailbox {
                tx,
                budget: Arc::new(tokio::sync::Semaphore::new(4 * 1024 * 1024)),
                budget_bytes: 4 * 1024 * 1024,
            },
        );
        let name: Arc<str> = Arc::from(name);
        let fanout = Arc::clone(router.fanout());
        let gate = Arc::clone(&gate);
        let cancel = CancellationToken::new();
        let cancel = cancel.child_token();
        // Leak the actor task handle: the test runtime reaps it at teardown.
        tokio::spawn(async move {
            run_window_actor(name, win, gate, fanout, notify, rx, cancel, None).await;
        });
    }

    // Interleave the two streams like the nexmark generator (several frames
    // of one stream, then the other, repeatedly) — enough frames to blow
    // past any single-window seq hole.
    let parse_seq = Arc::new(AtomicU64::new(0));
    let schema = test_schema();
    for round in 0..8u64 {
        for stream in ["a", "b"] {
            let batch = make_batch(
                &schema,
                &[(1_000_000_000 + round * 1_000_000) as i64; 2],
                &[round as i64, round as i64],
            );
            route_and_dispatch(&parse_seq, "src", stream, batch, &router, None, None).await;
        }
    }
    // Both windows must receive every one of their 8 frames. Before the fix
    // win_b's actor parked its first frame (global seq 1 ≠ expected 0) and
    // this timed out with 0 rows.
    wait_for_rows_for(&router, "win_a", 16).await;
    wait_for_rows_for(&router, "win_b", 16).await;
}
