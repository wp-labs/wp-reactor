//! event_bridge 内联单测（RecordBatch ↔ Event / JoinRow 桥）——原 `mod tests`
//! 2026-09-04 原样外移（#[path] sibling，`super` = event_bridge 根不变）。

use super::*;
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Int64Type, Schema};
use std::collections::HashSet;
use std::sync::Arc;

use crate::match_engine::cep::{FieldSource, ScopeKey, Value};
use wf_lang::ast::FieldRef;

fn make_schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

#[test]
fn test_batch_to_events_basic() {
    let schema = make_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![42, 99])) as ArrayRef,
            Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
        ],
    )
    .unwrap();

    let events = batch_to_events(&batch);
    assert_eq!(events.len(), 2);

    assert_eq!(events[0].fields["id"], Value::Number(42.0));
    assert_eq!(events[0].fields["name"], Value::Str("alice".into()));
    assert_eq!(events[0].fields["active"], Value::Bool(true));

    assert_eq!(events[1].fields["id"], Value::Number(99.0));
    assert_eq!(events[1].fields["name"], Value::Str("bob".into()));
    assert_eq!(events[1].fields["active"], Value::Bool(false));
}

#[test]
fn test_batch_to_events_timestamp() {
    let schema = make_schema(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )]);
    let nanos: i64 = 1_700_000_000_000_000_000;
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(TimestampNanosecondArray::from(vec![nanos])) as ArrayRef],
    )
    .unwrap();

    let events = batch_to_events(&batch);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].fields["ts"], Value::Number(nanos as f64));
}

#[test]
fn test_batch_event_time_nanos_matches_extract_event_time_roundtrip() {
    // Int64 / Timestamp(Ns) go through an f64 round-trip exactly like the
    // eager `extract_event_time` (Value::Number(n as f64) → `as i64`); only
    // Float64 is a direct `as i64` cast. This is the correctness contract
    // for the L2 deferred scan reading time straight from the column.
    let schema = make_schema(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
    ]);
    // 2^53 + 1 is not representable in f64 — the round-trip collapses it.
    let big: i64 = (1i64 << 53) + 1;
    let nanos: i64 = 1_700_000_000_000_000_000;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(big), None])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.9), None])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![Some(nanos), None])) as ArrayRef,
        ],
    )
    .unwrap();

    let i_idx = batch_time_col_index(&batch, Some("i")).unwrap();
    let f_idx = batch_time_col_index(&batch, Some("f")).unwrap();
    let t_idx = batch_time_col_index(&batch, Some("t")).unwrap();

    // Int64: (value as f64) as i64.
    assert_eq!(
        batch_event_time_nanos_at(&batch, i_idx, 0),
        (big as f64) as i64
    );
    // Float64: direct cast.
    assert_eq!(batch_event_time_nanos_at(&batch, f_idx, 0), 1);
    // Timestamp(Ns): (value as f64) as i64.
    assert_eq!(
        batch_event_time_nanos_at(&batch, t_idx, 0),
        (nanos as f64) as i64
    );
    // Null time → 0 (matching `extract_event_time`'s missing-field fallback).
    assert_eq!(batch_event_time_nanos_at(&batch, i_idx, 1), 0);
    assert_eq!(batch_event_time_nanos_at(&batch, f_idx, 1), 0);
    assert_eq!(batch_event_time_nanos_at(&batch, t_idx, 1), 0);
    // Absent field → 0.
    assert_eq!(batch_event_time_nanos(&batch, Some("missing"), 0), 0);
    assert_eq!(batch_event_time_nanos(&batch, None, 0), 0);
}

#[test]
fn test_batch_to_events_nulls() {
    let schema = make_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![None, Some("bob")])) as ArrayRef,
        ],
    )
    .unwrap();

    let events = batch_to_events(&batch);
    assert_eq!(events.len(), 2);

    // Row 0: id=1, name is null (skipped)
    assert_eq!(events[0].fields["id"], Value::Number(1.0));
    assert!(!events[0].fields.contains_key("name"));

    // Row 1: id is null (skipped), name="bob"
    assert!(!events[1].fields.contains_key("id"));
    assert_eq!(events[1].fields["name"], Value::Str("bob".into()));
}

#[test]
fn test_batch_to_events_empty() {
    let schema = make_schema(vec![Field::new("id", DataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![] as Vec<i64>)) as ArrayRef],
    )
    .unwrap();

    let events = batch_to_events(&batch);
    assert!(events.is_empty());
}

#[test]
fn test_batch_to_events_float64() {
    let schema = make_schema(vec![Field::new("score", DataType::Float64, false)]);
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![3.21, 9.87])) as ArrayRef],
    )
    .unwrap();

    let events = batch_to_events(&batch);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].fields["score"], Value::Number(3.21));
    assert_eq!(events[1].fields["score"], Value::Number(9.87));
}

#[test]
fn test_batch_to_events_struct_and_list() {
    let tags =
        ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(10), Some(20)])]);
    let detection = StructArray::from(vec![(
        Arc::new(Field::new("severity", DataType::Int64, false)),
        Arc::new(Int64Array::from(vec![10])) as ArrayRef,
    )]);
    let extension = StructArray::from(vec![
        (
            Arc::new(Field::new(
                "detection",
                detection.data_type().clone(),
                false,
            )),
            Arc::new(detection) as ArrayRef,
        ),
        (
            Arc::new(Field::new("tags", tags.data_type().clone(), true)),
            Arc::new(tags) as ArrayRef,
        ),
        (
            Arc::new(Field::new("ignored", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        ),
    ]);
    let schema = make_schema(vec![Field::new(
        "extension",
        extension.data_type().clone(),
        false,
    )]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(extension) as ArrayRef]).unwrap();

    let events = batch_to_events(&batch);
    let Value::Object(extension) = &events[0].fields["extension"] else {
        panic!("expected extension object");
    };
    let Some(Value::Object(detection)) = extension.get("detection") else {
        panic!("expected nested detection object, got {extension:?}");
    };
    assert_eq!(detection.get("severity"), Some(&Value::Number(10.0)));
    assert_eq!(
        extension.get("tags"),
        Some(&Value::Array(vec![
            Value::Number(10.0),
            Value::Number(20.0)
        ]))
    );
    assert!(!extension.contains_key("ignored"));
}

#[test]
fn test_batch_to_events_parses_structured_utf8_json_only_with_metadata() {
    let structured_field = Field::new("extension", DataType::Utf8, true).with_metadata(
        std::collections::HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            WFL_FIELD_TYPE_OBJECT.to_string(),
        )]),
    );
    let schema = make_schema(vec![
        structured_field,
        Field::new("plain", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![r#"{"severity":10,"tags":["ssh"]}"#])) as ArrayRef,
            Arc::new(StringArray::from(vec![r#"{"severity":10}"#])) as ArrayRef,
        ],
    )
    .unwrap();

    let events = batch_to_events(&batch);
    let Value::Object(extension) = &events[0].fields["extension"] else {
        panic!("expected extension object");
    };
    assert_eq!(extension.get("severity"), Some(&Value::Number(10.0)));
    assert_eq!(
        extension.get("tags"),
        Some(&Value::Array(vec![Value::Str("ssh".into())]))
    );
    assert_eq!(
        events[0].fields["plain"],
        Value::Str(r#"{"severity":10}"#.into())
    );
}

#[test]
fn test_batch_to_events_parses_structured_array_utf8_json_with_metadata() {
    let structured_field =
        Field::new("ports", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([
            (
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_ARRAY.to_string(),
            ),
        ]));
    let schema = make_schema(vec![
        structured_field,
        Field::new("plain", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![r#"[22,2222]"#])) as ArrayRef,
            Arc::new(StringArray::from(vec![r#"[22,2222]"#])) as ArrayRef,
        ],
    )
    .unwrap();

    let events = batch_to_events(&batch);
    assert_eq!(
        events[0].fields["ports"],
        Value::Array(vec![Value::Number(22.0), Value::Number(2222.0)])
    );
    assert_eq!(events[0].fields["plain"], Value::Str(r#"[22,2222]"#.into()));
}

#[test]
fn test_batch_raw_ts_nanos() {
    // Raw `Timestamp(Ns)` i64 must be preserved exactly (no `as f64 as i64`
    // collapse), null → None, and a non-Timestamp column → None.
    let schema = make_schema(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("i", DataType::Int64, true),
    ]);
    let epoch: i64 = 1_767_225_600_000_000_123;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![Some(epoch), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])) as ArrayRef,
        ],
    )
    .unwrap();

    // Raw value preserved exactly (f64 would round this).
    assert_eq!(batch_raw_ts_nanos(&batch, 0, 0), Some(epoch));
    // Null timestamp → None.
    assert_eq!(batch_raw_ts_nanos(&batch, 0, 1), None);
    // Non-Timestamp(Ns) column → None.
    assert_eq!(batch_raw_ts_nanos(&batch, 1, 0), None);
}

#[test]
fn test_columnar_join_rows_projection() {
    let schema = make_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![42, 99])) as ArrayRef,
            Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![1_000, 2_000])) as ArrayRef,
        ],
    )
    .unwrap();

    let proj: Arc<HashSet<String>> = Arc::new(HashSet::from(["id".to_string(), "ts".to_string()]));
    let rows = columnar_join_rows(vec![batch], Some(proj));

    assert_eq!(rows.len(), 2);
    // `field_names` exposes only the projected columns.
    let mut names: Vec<&str> = rows[0].field_names();
    names.sort_unstable();
    assert_eq!(names, vec!["id", "ts"]);
    // `field_value` still reads non-projected columns (join conditions).
    assert_eq!(
        rows[0].field_value("name"),
        Some(Value::Str("alice".into()))
    );
    assert_eq!(rows[0].field_value("id"), Some(Value::Number(42.0)));
}

#[test]
fn test_columnar_timestamped_join_rows_projection() {
    let schema = make_schema(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_000),
                None,
                Some(3_000),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![42, 99, 7])) as ArrayRef,
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])) as ArrayRef,
        ],
    )
    .unwrap();

    let proj: Arc<HashSet<String>> = Arc::new(HashSet::from(["ts".to_string()]));
    let rows = columnar_timestamped_join_rows(vec![batch], 0, Some(proj));

    // Null-timestamp row (index 1) is skipped.
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 1_000);
    assert_eq!(rows[1].0, 3_000);
    // `field_names` is projected to only "ts".
    assert_eq!(rows[0].1.field_names(), vec!["ts"]);
    // `field_value` still reads non-projected "id" (join conditions).
    assert_eq!(rows[0].1.field_value("id"), Some(Value::Number(42.0)));
    assert_eq!(rows[1].1.field_value("id"), Some(Value::Number(7.0)));
}

#[test]
fn columnar_extract_scope_key_matches_row_based() {
    // 列式直读 `ColumnarEvent::extract_scope_key`（qradar 单 key 热路径）
    // 必须与行式 `extract_key_simple` + `scope_key_from_values` 逐行构造出
    // **同一个** `ScopeKey`——语义锁定（fanout 分片对拍同款 canonicalization）。
    use crate::match_engine::{extract_key_simple, scope_key_from_values};

    let schema = make_schema(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new("dport", DataType::Int64, true),
        Field::new("packet_rate", DataType::Float64, true),
        Field::new("blocked", DataType::Boolean, true),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("10.0.0.1"),
                None,
                Some("10.0.0.2"),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(443), Some(80), Some(80)])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.0), Some(0.0)])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let index = build_field_index(&batch);
    let events = batch_to_events(&batch);

    // 单 key Utf8（含 null 行 → 双路径均 None）
    let keys = [FieldRef::Simple("sip".into())];
    for (row, row_ev) in events.iter().enumerate() {
        let col = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
        assert_eq!(
            col.extract_scope_key(&keys, None, "c"),
            extract_key_simple(row_ev, &keys).map(|v| scope_key_from_values(&v)),
            "sip row {row}"
        );
    }

    // 单 key Int64 / Float64 / Boolean 与行式一致
    for name in ["dport", "packet_rate", "blocked"] {
        let keys = [FieldRef::Simple(name.into())];
        for (row, row_ev) in events.iter().enumerate() {
            let col = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
            assert_eq!(
                col.extract_scope_key(&keys, None, "c"),
                extract_key_simple(row_ev, &keys).map(|v| scope_key_from_values(&v)),
                "{name} row {row}"
            );
        }
    }

    // 多 key：Pair 顺序与行式 `scope_key_from_values` 一致
    let multi = [
        FieldRef::Simple("sip".into()),
        FieldRef::Simple("dport".into()),
    ];
    for (row, row_ev) in events.iter().enumerate() {
        let col = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
        assert_eq!(
            col.extract_scope_key(&multi, None, "c"),
            extract_key_simple(row_ev, &multi).map(|v| scope_key_from_values(&v)),
            "multi row {row}"
        );
    }
}

#[test]
fn columnar_extract_scope_key_fallbacks() {
    // 结构化 object 列 / key_map / 无 index / 列缺失：回退或拒绝路径必须
    // 与行式 `extract_key_simple` 字节一致。
    use crate::match_engine::{extract_key_simple, scope_key_from_values};

    let obj_field = Field::new("conn_info", DataType::Utf8, true).with_metadata(
        std::collections::HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            WFL_FIELD_TYPE_OBJECT.to_string(),
        )]),
    );
    let schema = make_schema(vec![obj_field, Field::new("sip", DataType::Utf8, true)]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some(r#"{"geo":"cn"}"#), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("10.0.0.1"), Some("10.0.0.2")])) as ArrayRef,
        ],
    )
    .unwrap();
    let index = build_field_index(&batch);
    let events = batch_to_events(&batch);

    // object 结构化 key：行式解析 JSON → Value::Object → Str("[object]")，
    // 列式直读会给原始 JSON 串——必须回退行式（语义不变）。
    let keys = [FieldRef::Simple("conn_info".into())];
    let col = ColumnarEvent::with_index(&batch, 0, Arc::clone(&index));
    let expected = extract_key_simple(&events[0], &keys).map(|v| scope_key_from_values(&v));
    assert_eq!(expected, Some(ScopeKey::Str("[object]".into())));
    assert_eq!(col.extract_scope_key(&keys, None, "c"), expected);

    // key_map 别名映射（多事件规则）→ 回退行式
    let keys_sip = [FieldRef::Simple("sip".into())];
    let km = [wf_lang::plan::KeyMapPlan {
        logical_name: "sip".into(),
        source_alias: "c".into(),
        source_field: "sip".into(),
    }];
    let col2 = ColumnarEvent::with_index(&batch, 1, Arc::clone(&index));
    assert_eq!(
        col2.extract_scope_key(&keys_sip, Some(&km), "c"),
        extract_key_simple(&events[1], &keys_sip).map(|v| scope_key_from_values(&v)),
    );

    // 无 index（ColumnarEvent::new）→ schema index_of 路径，结果一致
    let col3 = ColumnarEvent::new(&batch, 0);
    assert_eq!(
        col3.extract_scope_key(&keys_sip, None, "c"),
        extract_key_simple(&events[0], &keys_sip).map(|v| scope_key_from_values(&v)),
    );

    // 列缺失 → None（同行式 key 缺失跳过事件）
    let missing = [FieldRef::Simple("no_such_col".into())];
    assert_eq!(col3.extract_scope_key(&missing, None, "c"), None);
}

#[test]
fn columnar_extract_scope_key_type_lanes() {
    // 类型车道锁定（2026-08-31 review 补）：
    // - Timestamp(Ns) / >2^53 Int64：列式直读 = ScopeKey::Int（精确 i64），
    //   行式 = Float（f64 舍入）——**已知分歧**（fanout 分片
    //   `scope_key_columnar_matches_row_based` 同款：>2^53 行式丢精度），
    //   列式与分片路由一致（本优化的正确方向）；
    // - Struct / List 列：双路径一致 → Str("[object]") / Str("[array]")
    //   （结构化键走 from_value 规范化）；
    // - 空 key 列表 → ScopeKey::Empty（shared instance）。
    use crate::match_engine::{extract_key_simple, scope_key_from_values};
    use arrow::array::TimestampNanosecondArray;
    use arrow::datatypes::TimeUnit;

    // --- Timestamp(Ns) key ---
    let schema = make_schema(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("sip", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec!["10.0.0.1"])) as ArrayRef,
        ],
    )
    .unwrap();
    let index = build_field_index(&batch);
    let keys = [FieldRef::Simple("ts".into())];
    let col = ColumnarEvent::with_index(&batch, 0, Arc::clone(&index));
    let col_key = col.extract_scope_key(&keys, None, "c").unwrap();
    assert_eq!(col_key, ScopeKey::Int(1_700_000_000_000_000_000));
    // 行式（旧路径）在 >2^53 处发散为 Float——分歧被锁定（fanout 同款）。
    let row_key = extract_key_simple(&col, &keys)
        .map(|v| scope_key_from_values(&v))
        .unwrap();
    assert_ne!(col_key, row_key);
    assert!(matches!(row_key, ScopeKey::Float(_)));

    // --- >2^53 Int64 key（同款分歧）---
    let schema = make_schema(vec![
        Field::new("big", DataType::Int64, false),
        Field::new("sip", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![9_007_199_254_740_993])) as ArrayRef, // 2^53+1
            Arc::new(StringArray::from(vec!["10.0.0.1"])) as ArrayRef,
        ],
    )
    .unwrap();
    let index = build_field_index(&batch);
    let keys = [FieldRef::Simple("big".into())];
    let col = ColumnarEvent::with_index(&batch, 0, Arc::clone(&index));
    let col_key = col.extract_scope_key(&keys, None, "c").unwrap();
    assert_eq!(col_key, ScopeKey::Int(9_007_199_254_740_993));
    let row_key = extract_key_simple(&col, &keys)
        .map(|v| scope_key_from_values(&v))
        .unwrap();
    assert_ne!(col_key, row_key, ">2^53 Int64 列式精确 vs 行式 f64 舍入");

    // --- Struct 列 → 双路径均 Str("[object]") ---
    let inner_field = Field::new("geo", DataType::Utf8, false);
    let schema = make_schema(vec![Field::new(
        "obj",
        DataType::Struct(arrow::datatypes::Fields::from(vec![inner_field.clone()])),
        false,
    )]);
    let inner = StringArray::from(vec!["cn"]);
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StructArray::from(vec![(
            Arc::new(inner_field),
            Arc::new(inner) as ArrayRef,
        )])) as ArrayRef],
    )
    .unwrap();
    let index = build_field_index(&batch);
    let keys = [FieldRef::Simple("obj".into())];
    let col = ColumnarEvent::with_index(&batch, 0, Arc::clone(&index));
    assert_eq!(
        col.extract_scope_key(&keys, None, "c"),
        Some(ScopeKey::Str("[object]".into()))
    );

    // --- 空 key 列表 → Empty（shared instance）---
    assert_eq!(col.extract_scope_key(&[], None, "c"), Some(ScopeKey::Empty));
}

#[test]
fn columnar_extract_scope_key_multi_key_gaps() {
    // 多 key 缺口（2026-08-31 review 补）：
    // - 第二个 key 列在批 schema 中缺失 → None（同行式跳过）；
    // - 第二个 key 列为 null → None（同行式缺失语义）。
    use crate::match_engine::{extract_key_simple, scope_key_from_values};
    let schema = make_schema(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["k1", "k2"])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("v1"), None])) as ArrayRef,
        ],
    )
    .unwrap();
    let index = build_field_index(&batch);
    let events = batch_to_events(&batch);

    // 第二个 key 列缺失：a 存在、ghost 不存在 → None（快路径与行式一致）
    let keys = [
        FieldRef::Simple("a".into()),
        FieldRef::Simple("ghost".into()),
    ];
    for (row, row_ev) in events.iter().enumerate() {
        let col = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
        assert_eq!(
            col.extract_scope_key(&keys, None, "c"),
            extract_key_simple(row_ev, &keys).map(|v| scope_key_from_values(&v)),
            "missing second key col row {row}"
        );
    }

    // 第二个 key 列为 null（row 1）：快路径与行式均 None
    let keys = [FieldRef::Simple("a".into()), FieldRef::Simple("b".into())];
    for (row, row_ev) in events.iter().enumerate() {
        let col = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
        assert_eq!(
            col.extract_scope_key(&keys, None, "c"),
            extract_key_simple(row_ev, &keys).map(|v| scope_key_from_values(&v)),
            "null second key col row {row}"
        );
    }
}
