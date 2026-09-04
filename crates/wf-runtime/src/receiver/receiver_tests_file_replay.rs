//! 文本文件源（NDJSON/CSV）重放与 JSON 文本行 → 结构化 schema 校验（2026-09-04 自 receiver/tests.rs
//! 拆出，`#[path]` 兄弟子模块，`use super::*` 继承共享 harness）：
//! - replay_ndjson_file / replay_csv_file 端到端：单流路由、行内 wp_oml_name 分窗、未知流 /
//!   missing tag 记 window_miss（metric + miss 窗快照）；
//! - build_record_batch_from_json：epoch millis / RFC3339 时间列、object+array 列序列化 Utf8、
//!   typed array 落 Utf8 存储（文件源 schema）；
//! - validate_batch_schema_for_stream：Object/Array 结构化字段 × List/Struct 输入与 Utf8+wfl
//!   元数据互相接受/拒绝。
use super::*;

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
