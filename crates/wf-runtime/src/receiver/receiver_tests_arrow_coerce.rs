//! Arrow(framed/ipc) 文件重放与列类型 coerce/路由投影（2026-09-04 自 receiver/tests.rs 拆出，`#[path]`
//! 兄弟子模块，`use super::*` 继承共享 harness）：
//! - replay_arrow_framed_file / replay_arrow_ipc_file 端到端：len-framed / IPC 文件按流路由，未知流
//!   记 window_miss；
//! - coerce_column / coerce_column_for_field：utf8/int64/float64/bool/timestamp 互转、
//!   structured(Object/Array) → Utf8 JSON、同类型 noop、unmatched 落 null；
//! - prepare_batch 路由投影：Utf8 字段带 wfl 元数据（对/错配）投影为窗口目标 Object schema。
use super::*;

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
