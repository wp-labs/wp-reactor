use super::*;
use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
use wf_engine::window::{WindowDef, WindowParams, WindowRegistry};

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
    let reg = WindowRegistry::build(vec![WindowDef {
        params: WindowParams {
            name: "test_win".into(),
            schema: test_schema(),
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
        },
        streams: vec![stream_name.to_string()],
        config: test_config(),
    }])
    .unwrap();
    Arc::new(Router::new(reg))
}

fn make_multi_stream_router() -> Arc<Router> {
    let reg = WindowRegistry::build(vec![
        WindowDef {
            params: WindowParams {
                name: "win_a".into(),
                schema: test_schema(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
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
            },
            streams: vec!["b".to_string()],
            config: test_config(),
        },
    ])
    .unwrap();
    Arc::new(Router::new(reg))
}

/// Count total rows across all batches in the test window snapshot.
fn snapshot_row_count(router: &Router) -> usize {
    snapshot_row_count_for(router, "test_win")
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

#[tokio::test]
async fn file_ndjson_replay_routes_rows() {
    let router = make_router("events");
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
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(snapshot_row_count(&router), 2);
}

#[tokio::test]
async fn file_ndjson_replay_routes_rows_by_row_stream() {
    let router = make_multi_stream_router();
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
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(snapshot_row_count_for(&router, "win_a"), 2);
    assert_eq!(snapshot_row_count_for(&router, "win_b"), 1);
}

#[tokio::test]
async fn file_csv_replay_routes_rows_by_stream_tag_field_column() {
    let router = make_multi_stream_router();
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
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(snapshot_row_count_for(&router, "win_a"), 2);
    assert_eq!(snapshot_row_count_for(&router, "win_b"), 1);
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

#[tokio::test]
async fn file_arrow_framed_replay_routes_rows() {
    let router = make_router("events");
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.arrow_framed");
    let schema = test_schema();
    let batch_a = make_batch(&schema, &[1_000_000_000], &[1]);
    let batch_b = make_batch(&schema, &[2_000_000_000], &[2]);

    {
        let payload_a = wp_arrow::ipc::encode_ipc("events", &batch_a).unwrap();
        let payload_b = wp_arrow::ipc::encode_ipc("events", &batch_b).unwrap();
        let mut body = Vec::new();
        body.extend_from_slice(&(payload_a.len() as u32).to_be_bytes());
        body.extend_from_slice(&payload_a);
        body.extend_from_slice(&(payload_b.len() as u32).to_be_bytes());
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
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(snapshot_row_count(&router), 2);
}

#[tokio::test]
async fn file_arrow_ipc_replay_routes_rows() {
    let router = make_router("events");
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
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(snapshot_row_count(&router), 2);
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
fn coerce_same_type_noop() {
    let arr: arrow::array::ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let result = coerce_column(&arr, &DataType::Utf8, 2);
    // Same type should return the original column (clone)
    let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "a");
    assert_eq!(strings.value(1), "b");
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
