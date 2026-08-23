//! receiver/* 第四轮补测（注册于 receiver/mod.rs）。
//!
//! 覆盖点:
//! - ndjson: 空行跳过、非法 JSON / 非对象行报错、文件打开失败、
//!   >2048 行中途 flush（fixed + 动态 schema-cache 命中）、
//!   `flush_ndjson_rows` 空集早退、`normalize_stream_tag_field` 空串默认值。
//! - csv: fixed stream 中途 flush（>2048 行）、文件打开失败。
//! - arrow: framed 畸形前缀（非数字/超长/长度 0）、帧解码失败、
//!   parse 池关闭报错、IPC 文件打开失败、IPC schema 不匹配。
//! - batch: 不支持的字段类型报错; Utf8 字段的字符串数值/布尔/空值分支。
//! - route: `batch_machine_id` 空列、`coerce_column` Utf8→Timestamp 坏值 /
//!   Float64→Utf8、`coerce_column_for_field` 结构化 object 分支与回退、
//!   `prepare_batch` 投影保留 machine_id。
use std::sync::Arc;

use std::sync::atomic::AtomicU64;
use std::time::Duration;

use arrow::array::{Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
use wf_engine::match_engine::{MACHINE_ID, WFL_FIELD_TYPE_METADATA_KEY, WFL_FIELD_TYPE_OBJECT};
use wf_engine::window::{ProviderWindow, Router, WindowDef, WindowParams, WindowRegistry};

// 注意: 不能用 `use super::*;`——`super::arrow` 会遮蔽 arrow crate。
use super::arrow::{replay_arrow_framed_file, replay_arrow_ipc_file};
use super::batch::build_record_batch_from_json;
use super::csv::replay_csv_file;
use super::ndjson::{flush_ndjson_rows, normalize_stream_tag_field, replay_ndjson_file};
use super::route::{batch_machine_id, coerce_column, coerce_column_for_field, prepare_batch};
use super::{DEFAULT_STREAM_TAG_FIELD, ReplayRoute};
use crate::lifecycle::parse_pool::{ParseItem, PrereadBudget, spawn_parse_pool};
use crate::lifecycle::types::TaskGroup;

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("value", DataType::Int64, true),
    ]))
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

fn attach_parse_pool(
    router: Arc<Router>,
) -> (
    Arc<Router>,
    mpsc::Sender<ParseItem>,
    PrereadBudget,
    Arc<AtomicU64>,
) {
    let mut group = TaskGroup::new("test_parse_r4");
    let (parse_tx, preread) = spawn_parse_pool(&router, None, 1, &mut group);
    (router, parse_tx, preread, Arc::new(AtomicU64::new(0)))
}

async fn wait_for_rows(router: &Router, expected: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let rows: usize = router
            .registry()
            .snapshot("test_win")
            .unwrap_or_default()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        if rows >= expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {expected} rows (have {rows})"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn ndjson_schema() -> wf_lang::WindowSchema {
    wf_lang::WindowSchema {
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
    }
}

/// 已关闭的 parse 通道（用于错误路径：push_decoded_batch 返回 false）。
fn closed_parse_channel() -> (mpsc::Sender<ParseItem>, PrereadBudget) {
    let (tx, rx) = mpsc::channel::<ParseItem>(1);
    drop(rx);
    (tx, PrereadBudget::new(1024))
}

fn ndjson_lines(count: usize) -> String {
    let mut out = String::with_capacity(count * 32);
    for i in 0..count {
        out.push_str(&format!("{{\"ts\":{},\"value\":1}}\n", 1_000_000_000 + i));
    }
    out
}

// ---------------------------------------------------------------------------
// ndjson
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ndjson_blank_lines_are_skipped() {
    let (router, parse_tx, preread, parse_seq) = attach_parse_pool(make_router("events"));
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.ndjson");
    std::fs::write(
        &file_path,
        "\n   \n{\"ts\":1000000000,\"value\":1}\n\n{\"ts\":2000000000,\"value\":2}\n",
    )
    .unwrap();

    replay_ndjson_file(
        &file_path,
        ReplayRoute {
            stream_name: "events",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &[ndjson_schema()],
        Arc::clone(&router),
        None,
        parse_tx,
        preread,
        parse_seq,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    wait_for_rows(&router, 2).await;
}

#[tokio::test]
async fn ndjson_invalid_json_line_errors() {
    let (parse_tx, preread) = closed_parse_channel();
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.ndjson");
    std::fs::write(&file_path, "{\"ts\":1,\"value\":1}\nnot-json\n").unwrap();

    let err = replay_ndjson_file(
        &file_path,
        ReplayRoute {
            stream_name: "events",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &[ndjson_schema()],
        make_router("events"),
        None,
        parse_tx,
        preread,
        Arc::new(AtomicU64::new(0)),
        CancellationToken::new(),
    )
    .await
    .expect_err("invalid NDJSON must fail");

    assert!(err.to_string().contains("invalid NDJSON"), "got: {err}");
}

#[tokio::test]
async fn ndjson_non_object_line_errors() {
    let (parse_tx, preread) = closed_parse_channel();
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.ndjson");
    std::fs::write(&file_path, "[1,2,3]\n").unwrap();

    let err = replay_ndjson_file(
        &file_path,
        ReplayRoute {
            stream_name: "events",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &[ndjson_schema()],
        make_router("events"),
        None,
        parse_tx,
        preread,
        Arc::new(AtomicU64::new(0)),
        CancellationToken::new(),
    )
    .await
    .expect_err("non-object NDJSON must fail");

    assert!(
        err.to_string().contains("expected JSON object"),
        "got: {err}"
    );
}

#[tokio::test]
async fn ndjson_missing_file_errors() {
    let (parse_tx, preread) = closed_parse_channel();
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.ndjson");
    let err = replay_ndjson_file(
        &missing,
        ReplayRoute {
            stream_name: "events",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &[ndjson_schema()],
        make_router("events"),
        None,
        parse_tx,
        preread,
        Arc::new(AtomicU64::new(0)),
        CancellationToken::new(),
    )
    .await
    .expect_err("missing file must fail");

    assert!(err.to_string().contains("open file source"), "got: {err}");
}

#[tokio::test]
async fn ndjson_fixed_stream_flushes_mid_file() {
    // >2048 行 → 中途 flush 一次 + 末尾 flush 剩余（覆盖 132-151 与缓存复用）。
    let (router, parse_tx, preread, parse_seq) = attach_parse_pool(make_router("events"));
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.ndjson");
    std::fs::write(&file_path, ndjson_lines(5_000)).unwrap();

    replay_ndjson_file(
        &file_path,
        ReplayRoute {
            stream_name: "events",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &[ndjson_schema()],
        Arc::clone(&router),
        None,
        parse_tx,
        preread,
        parse_seq,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    wait_for_rows(&router, 5_000).await;
}

#[tokio::test]
async fn ndjson_dynamic_stream_schema_cache_hit() {
    // 动态 stream（非 fixed）: 同一 stream 跨两次 flush → 第二次命中 schema_cache。
    let mut schema = ndjson_schema();
    schema.name = "win_a".to_string();
    schema.streams = vec!["a".to_string()];
    let router = {
        let mut reg = WindowRegistry::build(vec![WindowDef {
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
        }])
        .unwrap();
        register_miss_provider(&mut reg);
        Arc::new(Router::new(reg))
    };
    let (router, parse_tx, preread, parse_seq) = attach_parse_pool(router);
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.ndjson");
    // 3000 行全部归 stream "a": 2048 行一次 flush + 952 行一次 flush。
    let mut content = String::new();
    for i in 0..3_000 {
        content.push_str(&format!(
            "{{\"wp_oml_name\":\"a\",\"ts\":{},\"value\":1}}\n",
            1_000_000_000 + i
        ));
    }
    std::fs::write(&file_path, content).unwrap();

    replay_ndjson_file(
        &file_path,
        ReplayRoute {
            stream_name: "",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &[schema],
        Arc::clone(&router),
        None,
        parse_tx,
        preread,
        parse_seq,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let rows: usize = router
            .registry()
            .snapshot("win_a")
            .unwrap_or_default()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        if rows >= 3_000 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for 3000 rows (have {rows})"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn flush_ndjson_rows_empty_is_noop() {
    let (parse_tx, preread) = closed_parse_channel();
    let schema = ndjson_schema();
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
    let mut cache = std::collections::HashMap::new();
    let count = flush_ndjson_rows(
        "events",
        "test_source",
        &[schema],
        None,
        &mut cache,
        rows,
        make_router("events").as_ref(),
        None,
        &parse_tx,
        &preread,
        &AtomicU64::new(0),
        DEFAULT_STREAM_TAG_FIELD,
        "file",
    )
    .await
    .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn normalize_stream_tag_field_empty_uses_default() {
    assert_eq!(normalize_stream_tag_field(""), DEFAULT_STREAM_TAG_FIELD);
    assert_eq!(normalize_stream_tag_field("  "), DEFAULT_STREAM_TAG_FIELD);
    assert_eq!(normalize_stream_tag_field("custom_tag"), "custom_tag");
}

// ---------------------------------------------------------------------------
// csv
// ---------------------------------------------------------------------------

#[tokio::test]
async fn csv_fixed_stream_flushes_mid_file() {
    let (router, parse_tx, preread, parse_seq) = attach_parse_pool(make_router("events"));
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.csv");
    let mut content = String::from("ts,value\n");
    for i in 0..5_000 {
        content.push_str(&format!("{},1\n", 1_000_000_000 + i));
    }
    std::fs::write(&file_path, content).unwrap();

    replay_csv_file(
        &file_path,
        ReplayRoute {
            stream_name: "events",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &[ndjson_schema()],
        Arc::clone(&router),
        None,
        parse_tx,
        preread,
        parse_seq,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    wait_for_rows(&router, 5_000).await;
}

#[tokio::test]
async fn csv_missing_file_errors() {
    let (parse_tx, preread) = closed_parse_channel();
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.csv");
    let err = replay_csv_file(
        &missing,
        ReplayRoute {
            stream_name: "events",
            stream_tag_field: DEFAULT_STREAM_TAG_FIELD,
        },
        "test_source",
        &[ndjson_schema()],
        make_router("events"),
        None,
        parse_tx,
        preread,
        Arc::new(AtomicU64::new(0)),
        CancellationToken::new(),
    )
    .await
    .expect_err("missing csv must fail");

    assert!(err.to_string().contains("open csv source"), "got: {err}");
}

// ---------------------------------------------------------------------------
// arrow（framed + ipc）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn arrow_framed_malformed_prefix_errors() {
    let (parse_tx, preread) = closed_parse_channel();
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("bad.frame");
    std::fs::write(&file_path, "abc").unwrap();
    let err = replay_arrow_framed_file(
        &file_path,
        "events",
        "test_source",
        &[ndjson_schema()],
        make_router("events"),
        None,
        parse_tx,
        preread,
        Arc::new(AtomicU64::new(0)),
        CancellationToken::new(),
        None,
    )
    .await
    .expect_err("malformed prefix must fail");
    // source_err 包装后 to_string 只有外层 detail；root cause（io error 消息）
    // 在 Debug 视图的 source 链里。
    assert!(
        format!("{err:?}").contains("invalid frame length prefix"),
        "got: {err}"
    );
}

#[tokio::test]
async fn arrow_framed_prefix_too_long_errors() {
    let (parse_tx, preread) = closed_parse_channel();
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("long.frame");
    std::fs::write(&file_path, "12345678901234567890 x").unwrap();
    let err = replay_arrow_framed_file(
        &file_path,
        "events",
        "test_source",
        &[ndjson_schema()],
        make_router("events"),
        None,
        parse_tx,
        preread,
        Arc::new(AtomicU64::new(0)),
        CancellationToken::new(),
        None,
    )
    .await
    .expect_err("over-long prefix must fail");
    assert!(
        format!("{err:?}").contains("frame length prefix too long"),
        "got: {err}"
    );
}

#[tokio::test]
async fn arrow_framed_zero_length_frame_errors() {
    let (parse_tx, preread) = closed_parse_channel();
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("zero.frame");
    std::fs::write(&file_path, "0 x").unwrap();
    let err = replay_arrow_framed_file(
        &file_path,
        "events",
        "test_source",
        &[ndjson_schema()],
        make_router("events"),
        None,
        parse_tx,
        preread,
        Arc::new(AtomicU64::new(0)),
        CancellationToken::new(),
        None,
    )
    .await
    .expect_err("zero-length frame must fail");
    assert!(
        format!("{err:?}").contains("unreasonable frame length"),
        "got: {err}"
    );
}

#[tokio::test]
async fn arrow_framed_decode_error() {
    let (parse_tx, preread) = closed_parse_channel();
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("garbage.frame");
    // 长度前缀 5 + 空格 + 5 字节垃圾 → 解码失败。
    std::fs::write(&file_path, "5 xxxxx").unwrap();
    let err = replay_arrow_framed_file(
        &file_path,
        "events",
        "test_source",
        &[ndjson_schema()],
        make_router("events"),
        None,
        parse_tx,
        preread,
        Arc::new(AtomicU64::new(0)),
        CancellationToken::new(),
        None,
    )
    .await
    .expect_err("garbage frame must fail to decode");
    assert!(err.to_string().contains("decode arrow frame"), "got: {err}");
}

#[tokio::test]
async fn arrow_ipc_missing_file_errors() {
    let (parse_tx, preread) = closed_parse_channel();
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.arrow");
    let err = replay_arrow_ipc_file(
        &missing,
        "events",
        "test_source",
        &[ndjson_schema()],
        make_router("events"),
        None,
        parse_tx,
        preread,
        Arc::new(AtomicU64::new(0)),
        CancellationToken::new(),
    )
    .await
    .expect_err("missing ipc file must fail");
    assert!(
        err.to_string().contains("open arrow ipc source"),
        "got: {err}"
    );
}

#[tokio::test]
async fn arrow_ipc_schema_mismatch_errors() {
    let (parse_tx, preread) = closed_parse_channel();
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("mismatch.arrow");
    // IPC 文件的 schema 与目标 stream 不匹配（只有 value 列, 无 ts）。
    let wrong_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&wrong_schema),
        vec![Arc::new(Int64Array::from(vec![1i64])) as _],
    )
    .unwrap();
    {
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = FileWriter::try_new(file, &wrong_schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    let err = replay_arrow_ipc_file(
        &file_path,
        "events",
        "test_source",
        &[ndjson_schema()],
        make_router("events"),
        None,
        parse_tx,
        preread,
        Arc::new(AtomicU64::new(0)),
        CancellationToken::new(),
    )
    .await
    .expect_err("schema mismatch must fail");
    assert!(err.to_string().contains("schema mismatch"), "got: {err}");
}

// ---------------------------------------------------------------------------
// batch（build_record_batch_from_json 分支）
// ---------------------------------------------------------------------------

#[test]
fn build_batch_unsupported_field_type_errors() {
    let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
    let rows = vec![serde_json::json!({"d": 1}).as_object().unwrap().clone()];
    let err = build_record_batch_from_json(&schema, &rows).expect_err("Date32 unsupported");
    assert!(
        err.to_string()
            .contains("unsupported file-source field type"),
        "got: {err}"
    );
}

#[test]
fn build_batch_string_coercions() {
    // Int64 列接字符串数值; Float64 列接字符串; Bool 列接字符串; Utf8 列接 null/数字。
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        serde_json::json!({"i": "42", "f": "1.5", "b": "true", "s": null})
            .as_object()
            .unwrap()
            .clone(),
        serde_json::json!({"i": "bad", "f": "x", "b": "1", "s": 7})
            .as_object()
            .unwrap()
            .clone(),
        serde_json::json!({"i": 1, "f": 2.0, "b": "no", "s": "hi"})
            .as_object()
            .unwrap()
            .clone(),
    ];
    let batch = build_record_batch_from_json(&schema, &rows).expect("build batch");
    let i = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(i.value(0), 42);
    assert!(i.is_null(1));
    assert_eq!(i.value(2), 1);

    let f = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert!((f.value(0) - 1.5).abs() < 1e-9);
    assert!(f.is_null(1));
    assert!((f.value(2) - 2.0).abs() < 1e-9);

    let b = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(b.value(0));
    assert!(b.value(1));
    assert!(b.is_null(2));

    let s = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(s.is_null(0));
    assert_eq!(s.value(1), "7");
    assert_eq!(s.value(2), "hi");
}

// ---------------------------------------------------------------------------
// route
// ---------------------------------------------------------------------------

#[test]
fn batch_machine_id_empty_column_returns_none() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        MACHINE_ID,
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(Vec::<String>::new())) as _],
    )
    .unwrap();
    assert_eq!(batch_machine_id(&batch), None);

    let schema = Arc::new(Schema::new(vec![Field::new(
        MACHINE_ID,
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["m-1".to_string()])) as _],
    )
    .unwrap();
    assert_eq!(batch_machine_id(&batch), Some("m-1".to_string()));
}

#[test]
fn coerce_utf8_to_timestamp_bad_value_is_null() {
    let arr: arrow::array::ArrayRef = Arc::new(StringArray::from(vec!["garbage"]));
    let result = coerce_column(&arr, &DataType::Timestamp(TimeUnit::Nanosecond, None), 1);
    let ts = result
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert!(ts.is_null(0));
}

#[test]
fn coerce_float64_to_utf8() {
    let arr: arrow::array::ArrayRef = Arc::new(arrow::array::Float64Array::from(vec![1.5, 2.0]));
    let result = coerce_column(&arr, &DataType::Utf8, 2);
    let s = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(s.value(0), "1.5");
    assert_eq!(s.value(1), "2");
}

#[test]
fn coerce_column_for_field_structured_object_to_json_strings() {
    // Struct 列 + Utf8(object 元数据) 目标字段 → 结构化转 JSON 字符串。
    let struct_arr: arrow::array::ArrayRef = Arc::new(
        arrow::array::StructArray::try_new(
            arrow::datatypes::Fields::from(vec![Field::new("severity", DataType::Int64, true)]),
            vec![Arc::new(Int64Array::from(vec![10i64])) as _],
            None,
        )
        .expect("struct array"),
    );
    let target =
        Field::new("ext", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            WFL_FIELD_TYPE_OBJECT.to_string(),
        )]));
    let result = coerce_column_for_field(&struct_arr, &target, 1);
    let s = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(s.value(0), r#"{"severity":10}"#);
}

#[test]
fn coerce_column_for_field_falls_through_to_plain_coercion() {
    // 非结构化目标字段 → 直接走 coerce_column。
    let arr: arrow::array::ArrayRef = Arc::new(StringArray::from(vec!["42"]));
    let target = Field::new("i", DataType::Int64, true);
    let result = coerce_column_for_field(&arr, &target, 1);
    let i = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(i.value(0), 42);
}

#[test]
fn prepare_batch_projection_preserves_machine_id() {
    use crate::receiver::schema::field_to_arrow;

    // 窗口 schema 含结构化 extension 字段（触发投影）但没有 machine_id。
    let window_schema = Arc::new(Schema::new(vec![field_to_arrow(
        "extension",
        &wf_lang::FieldType::Object,
    )]));
    let mut reg = WindowRegistry::build(vec![WindowDef {
        params: WindowParams {
            name: "alerts".into(),
            schema: window_schema,
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

    // 源批次: extension 为 Utf8(JSON) + machine_id 列。
    let source_schema = Arc::new(Schema::new(vec![
        Field::new("extension", DataType::Utf8, true),
        Field::new(MACHINE_ID, DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        source_schema,
        vec![
            Arc::new(StringArray::from(vec![r#"{"severity":10}"#])) as _,
            Arc::new(StringArray::from(vec!["m-42".to_string()])) as _,
        ],
    )
    .unwrap();

    let projected = prepare_batch("events", &batch, &router);
    assert_eq!(
        projected.schema().index_of(MACHINE_ID).ok(),
        Some(1),
        "machine_id 列必须保留在投影结果中"
    );
    let mid = projected
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(mid.value(0), "m-42");
    // extension 被投影为 object 元数据的 Utf8 字段。
    assert_eq!(
        projected
            .schema()
            .field(0)
            .metadata()
            .get(WFL_FIELD_TYPE_METADATA_KEY)
            .map(String::as_str),
        Some(WFL_FIELD_TYPE_OBJECT)
    );
}
