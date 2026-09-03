//! core_coverage 拆出的兄弟子模块（2026-09-04）：值/键/批表示转换覆盖——
//! types.rs（JoinKey / CloseReason / FieldSource / values_equal、WindowLookup
//! 默认实现回退）、key.rs（ScopeKey / ValueKey / scope_key_from_values / 分片 /
//! value_to_string / field_ref_name）与 event_bridge.rs（RecordBatch ↔ Event /
//! JoinRow 列式桥接、结构化字段元数据解析、列式标量字符串）。
//! 共享 harness 与 import 在父模块 core_coverage.rs，此处经 `use super::*` 复用。

use super::*;

// ===========================================================================
// types.rs — JoinKey / CloseReason / FieldSource / values_equal
// ===========================================================================

#[test]
fn join_key_from_value_covers_all_value_variants() {
    // Number → Int with the f64 truncation (matches the hash-index key math).
    assert_eq!(
        JoinKey::from_value(&Value::Number(42.0)),
        Some(JoinKey::Int(42))
    );
    assert_eq!(
        JoinKey::from_value(&Value::Number(1.5)),
        Some(JoinKey::Int(1)),
        "fractional numbers truncate like the join index key"
    );
    assert_eq!(
        JoinKey::from_value(&Value::Number(-7.9)),
        Some(JoinKey::Int(-7))
    );
    assert_eq!(
        JoinKey::from_value(&Value::Str("10.0.0.1".into())),
        Some(JoinKey::Str("10.0.0.1".to_string()))
    );
    assert_eq!(
        JoinKey::from_value(&Value::Bool(true)),
        Some(JoinKey::Bool(true))
    );
    assert_eq!(
        JoinKey::from_value(&Value::Bool(false)),
        Some(JoinKey::Bool(false))
    );
    // Structured values are not joinable scalar keys.
    assert_eq!(JoinKey::from_value(&Value::Array(vec![])), None);
    assert_eq!(
        JoinKey::from_value(&Value::Object(EngineHashMap::default())),
        None
    );
}

#[test]
fn close_reason_as_str_covers_all_variants() {
    assert_eq!(CloseReason::Timeout.as_str(), "timeout");
    assert_eq!(CloseReason::Flush.as_str(), "flush");
    assert_eq!(CloseReason::Eos.as_str(), "eos");
}

#[test]
fn event_field_source_impl_and_default_field_value_str() {
    let ev = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("dport", num(443.0)),
        ("flag", Value::Bool(true)),
    ]);
    // field_value / field_names / to_event on the concrete Event impl.
    assert_eq!(ev.field_value("sip"), Some(str_val("10.0.0.1")));
    assert_eq!(ev.field_value("missing"), None);
    let mut names = ev.field_names();
    names.sort_unstable();
    assert_eq!(names, vec!["dport", "flag", "sip"]);
    assert_eq!(ev.to_event(), ev);

    // Default `field_value_str`: Str → text, anything else → "".
    assert_eq!(ev.field_value_str("sip"), "10.0.0.1");
    assert_eq!(ev.field_value_str("dport"), "");
    assert_eq!(ev.field_value_str("missing"), "");
}

/// A lookup that only overrides `snapshot` / `snapshot_field_values` — the
/// remaining `WindowLookup` methods (join_lookup / asof_candidates /
/// snapshot_with_timestamps / asof_lookup_max) fall back to the default impls.
struct DefaultLookup {
    rows: Vec<HashMap<String, Value>>,
}

impl DefaultLookup {
    fn row(fields: Vec<(&str, Value)>) -> HashMap<String, Value> {
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }
}

impl WindowLookup for DefaultLookup {
    fn snapshot_field_values(&self, _window: &str, _field: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _window: &str) -> Option<Vec<JoinRow>> {
        Some(
            self.rows
                .iter()
                .cloned()
                .map(|r| {
                    JoinRow::Event(Arc::new(Event {
                        fields: r.into_iter().map(|(k, v)| (k.into(), v)).collect(),
                    }))
                })
                .collect(),
        )
    }
}

#[test]
fn window_lookup_default_join_lookup_filters_by_key() {
    let lookup = DefaultLookup {
        rows: vec![
            DefaultLookup::row(vec![("id", num(1.0)), ("v", str_val("a"))]),
            DefaultLookup::row(vec![("id", num(2.0)), ("v", str_val("b"))]),
            DefaultLookup::row(vec![("id", num(2.0)), ("v", str_val("c"))]),
        ],
    };
    let rows = lookup
        .join_lookup("w", "id", &Value::Number(2.0))
        .expect("default join_lookup must scan the snapshot");
    assert_eq!(rows.len(), 2, "default join_lookup is a linear filter");
    // Structured keys never match scalar rows via `values_equal`.
    let none = lookup
        .join_lookup("w", "id", &Value::Array(vec![]))
        .expect("snapshot exists");
    assert!(none.is_empty());
    // Missing key field on a row → filtered out.
    let missing = lookup
        .join_lookup("w", "nope", &Value::Number(1.0))
        .unwrap();
    assert!(missing.is_empty());
}

#[test]
fn window_lookup_default_asof_and_fast_path_fallbacks() {
    let lookup = DefaultLookup { rows: vec![] };
    // Default `snapshot_with_timestamps` → None → `asof_candidates` → None.
    assert!(lookup.snapshot_with_timestamps("w").is_none());
    assert!(
        lookup
            .asof_candidates("w", "id", &Value::Number(1.0))
            .is_none()
    );
    // Default `asof_lookup_max` is always `Fallback`.
    assert!(matches!(
        lookup.asof_lookup_max("w", "id", &Value::Number(1.0), 0, None),
        AsofLookup::Fallback
    ));
}

#[test]
fn values_equal_matches_scalars_and_rejects_structured() {
    assert!(values_equal(&Value::Number(1.0), &Value::Number(1.0)));
    assert!(values_equal(
        &Value::Number(1.0),
        &Value::Number(1.0 + f64::EPSILON / 2.0)
    ));
    assert!(!values_equal(&Value::Number(1.0), &Value::Number(1.5)));
    assert!(values_equal(
        &Value::Str("x".into()),
        &Value::Str("x".into())
    ));
    assert!(!values_equal(
        &Value::Str("x".into()),
        &Value::Str("y".into())
    ));
    assert!(values_equal(&Value::Bool(true), &Value::Bool(true)));
    assert!(!values_equal(&Value::Bool(true), &Value::Bool(false)));
    // Cross-type and structured comparisons are never equal.
    assert!(!values_equal(&Value::Number(1.0), &Value::Str("1".into())));
    assert!(!values_equal(
        &Value::Array(vec![num(1.0)]),
        &Value::Array(vec![num(1.0)])
    ));
    assert!(!values_equal(
        &Value::Object(EngineHashMap::default()),
        &Value::Object(EngineHashMap::default())
    ));
}

// ===========================================================================
// key.rs — ScopeKey / ValueKey / scope_key_from_values / shard / stringify
// ===========================================================================

#[test]
fn scope_key_from_value_covers_all_value_variants() {
    // Integer-valued numbers → Int (including full-precision < 2^53).
    assert_eq!(
        ScopeKey::from_value(&Value::Number(42.0)),
        ScopeKey::Int(42)
    );
    assert_eq!(ScopeKey::from_value(&Value::Number(-0.0)), ScopeKey::Int(0));
    assert_eq!(
        ScopeKey::from_value(&Value::Number(TWO_POW_53 - 1.0)),
        ScopeKey::Int(TWO_POW_53 as i64 - 1)
    );
    // Fractional / huge / non-finite numbers → Float (canonical bits).
    assert!(matches!(
        ScopeKey::from_value(&Value::Number(1.5)),
        ScopeKey::Float(_)
    ));
    assert!(matches!(
        ScopeKey::from_value(&Value::Number(TWO_POW_53)),
        ScopeKey::Float(_)
    ));
    assert!(matches!(
        ScopeKey::from_value(&Value::Number(f64::NAN)),
        ScopeKey::Float(_)
    ));
    assert_eq!(
        ScopeKey::from_value(&Value::Number(1.5)),
        ScopeKey::from_value(&Value::Number(1.5)),
        "same float → same canonical bits"
    );
    // Strings / bools / structured values.
    assert_eq!(
        ScopeKey::from_value(&Value::Str("10.0.0.1".into())),
        ScopeKey::Str("10.0.0.1".into())
    );
    assert_eq!(
        ScopeKey::from_value(&Value::Bool(true)),
        ScopeKey::Str("true".into())
    );
    assert_eq!(
        ScopeKey::from_value(&Value::Bool(false)),
        ScopeKey::Str("false".into())
    );
    assert_eq!(
        ScopeKey::from_value(&Value::Array(vec![])),
        ScopeKey::Str("[array]".into())
    );
    assert_eq!(
        ScopeKey::from_value(&Value::Object(EngineHashMap::default())),
        ScopeKey::Str("[object]".into())
    );
}

const TWO_POW_53: f64 = 9_007_199_254_740_992.0;

#[test]
fn scope_key_from_values_pairs_in_field_order() {
    assert_eq!(scope_key_from_values(&[]), ScopeKey::Empty);
    assert_eq!(scope_key_from_values(&[num(42.0)]), ScopeKey::Int(42));
    let pair = scope_key_from_values(&[num(42.0), str_val("x")]);
    assert_eq!(
        pair,
        ScopeKey::Pair(
            Box::new(ScopeKey::Int(42)),
            Box::new(ScopeKey::Str("x".into()))
        )
    );
    // Three values nest left-deep, preserving order.
    let triple = scope_key_from_values(&[num(1.0), num(2.0), num(3.0)]);
    assert_eq!(
        triple,
        ScopeKey::Pair(
            Box::new(ScopeKey::Pair(
                Box::new(ScopeKey::Int(1)),
                Box::new(ScopeKey::Int(2))
            )),
            Box::new(ScopeKey::Int(3))
        )
    );
}

#[test]
fn scope_key_shard_index_is_bounded_and_deterministic() {
    assert_eq!(scope_key_shard_index(&ScopeKey::Int(1), 0), 0);
    assert_eq!(scope_key_shard_index(&ScopeKey::Int(1), 1), 0);
    let keys = [
        ScopeKey::Empty,
        ScopeKey::Int(42),
        ScopeKey::Float(1.5f64.to_bits()),
        ScopeKey::Str("10.0.0.1".into()),
        ScopeKey::Pair(
            Box::new(ScopeKey::Int(1)),
            Box::new(ScopeKey::Str("x".into())),
        ),
    ];
    for key in &keys {
        let a = scope_key_shard_index(key, 8);
        let b = scope_key_shard_index(key, 8);
        assert_eq!(a, b, "shard index must be deterministic: {key:?}");
        assert!(a < 8, "shard index in range: {key:?}");
    }
    // Pair ordering matters (left/right are not commutative).
    let ab = ScopeKey::Pair(Box::new(ScopeKey::Int(1)), Box::new(ScopeKey::Int(2)));
    let ba = ScopeKey::Pair(Box::new(ScopeKey::Int(2)), Box::new(ScopeKey::Int(1)));
    let shards = (2..16)
        .filter(|n| scope_key_shard_index(&ab, *n) != scope_key_shard_index(&ba, *n))
        .count();
    assert!(
        shards > 0,
        "distinct pair order must shard differently somewhere"
    );
}

#[test]
fn value_key_from_value_canonicalization() {
    // Canonical float keys: -0.0 ≡ +0.0, NaN ≡ NaN.
    assert_eq!(
        ValueKey::from_value(&Value::Number(-0.0)),
        ValueKey::from_value(&Value::Number(0.0))
    );
    assert_eq!(
        ValueKey::from_value(&Value::Number(f64::NAN)),
        ValueKey::from_value(&Value::Number(f64::NAN))
    );
    assert_ne!(
        ValueKey::from_value(&Value::Number(1.0)),
        ValueKey::from_value(&Value::Number(-1.0))
    );
    assert_eq!(
        ValueKey::from_value(&Value::Str("a".into())),
        ValueKey::Str("a".to_string())
    );
    assert_eq!(
        ValueKey::from_value(&Value::Bool(true)),
        ValueKey::Bool(true)
    );
    // Array nests recursively.
    assert_eq!(
        ValueKey::from_value(&Value::Array(vec![num(1.0), str_val("x")])),
        ValueKey::Array(vec![
            ValueKey::Number(1.0f64.to_bits()),
            ValueKey::Str("x".to_string())
        ])
    );
    // Object sorts keys for deterministic keys.
    let mut obj = EngineHashMap::default();
    obj.insert("b".into(), Value::Number(2.0));
    obj.insert("a".into(), Value::Number(1.0));
    assert_eq!(
        ValueKey::from_value(&Value::Object(obj)),
        ValueKey::Object(vec![
            ("a".to_string(), ValueKey::Number(1.0f64.to_bits())),
            ("b".to_string(), ValueKey::Number(2.0f64.to_bits())),
        ])
    );
    // `estimated_bytes` is pub(super) — exercised indirectly by the state
    // machine's memory accounting; `from_value` canonicalization is covered above.
}

#[test]
fn extract_key_simple_reads_flat_fields_and_reports_missing() {
    let ev = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("user", str_val("admin")),
    ]);
    assert_eq!(
        extract_key_simple(&ev, &[simple_key("sip"), simple_key("user")]),
        Some(vec![str_val("10.0.0.1"), str_val("admin")])
    );
    // Qualified refs resolve by their field name.
    assert_eq!(
        extract_key_simple(&ev, &[FieldRef::Qualified("e".into(), "sip".into())]),
        Some(vec![str_val("10.0.0.1")])
    );
    // A missing key field → None (no partial keys).
    assert_eq!(
        extract_key_simple(&ev, &[simple_key("sip"), simple_key("missing")]),
        None
    );
    assert_eq!(extract_key_simple(&ev, &[]), Some(vec![]));
}

#[test]
fn push_i64_exact_decimal_and_value_to_string_variants() {
    let mut s = String::new();
    push_i64_exact_decimal(&mut s, 0);
    assert_eq!(s, "0");
    s.clear();
    push_i64_exact_decimal(&mut s, 123456789);
    assert_eq!(s, "123456789");
    s.clear();
    push_i64_exact_decimal(&mut s, -42);
    assert_eq!(s, "-42");
    s.clear();
    push_i64_exact_decimal(&mut s, i64::MIN);
    assert_eq!(s, "-9223372036854775808");
    s.clear();
    push_i64_exact_decimal(&mut s, i64::MAX);
    assert_eq!(s, "9223372036854775807");
    s.clear();
    push_i64_exact_decimal(&mut s, TWO_POW_53 as i64);
    assert_eq!(s, "9007199254740992");

    // value_to_string: integer fast path / fractional / exponent / -0.0.
    assert_eq!(value_to_string(&Value::Number(1.0)), "1");
    assert_eq!(value_to_string(&Value::Number(-0.0)), "-0");
    assert_eq!(value_to_string(&Value::Number(1.5)), "1.5");
    assert_eq!(
        value_to_string(&Value::Number(1e21)),
        "1000000000000000000000"
    );
    assert_eq!(
        value_to_string(&Value::Number(TWO_POW_53)),
        "9007199254740992"
    );
    assert_eq!(value_to_string(&Value::Str("s".into())), "s");
    assert_eq!(value_to_string(&Value::Bool(true)), "true");
    assert_eq!(value_to_string(&Value::Bool(false)), "false");
    assert_eq!(value_to_string(&Value::Array(vec![])), "[array]");
    assert_eq!(
        value_to_string(&Value::Object(EngineHashMap::default())),
        "[object]"
    );
}

#[test]
fn field_ref_name_covers_flat_qualified_bracketed_and_path() {
    assert_eq!(field_ref_name(&FieldRef::Simple("a".into())), "a");
    assert_eq!(
        field_ref_name(&FieldRef::Qualified("e".into(), "a".into())),
        "a"
    );
    assert_eq!(
        field_ref_name(&FieldRef::Bracketed("e".into(), "a.b".into())),
        "a.b"
    );
    assert_eq!(
        field_ref_name(&FieldRef::Path {
            alias: "e".into(),
            segments: vec![PathSegment::Field("root".into()), PathSegment::Index(0)],
        }),
        "root"
    );
}

// ===========================================================================
// event_bridge.rs — RecordBatch ↔ Event / JoinRow conversions
// ===========================================================================

/// `(id: Int64, name: Utf8, active: Boolean, ts: Timestamp(Ns))` batch.
fn mixed_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("solo", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_000),
                Some(2_000),
                None,
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(9), Some(8), Some(7)])) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn batch_to_events_filtered_materializes_only_requested_fields() {
    let batch = mixed_batch();
    let filtered = batch_to_events_filtered(
        &batch,
        &HashSet::from(["id".to_string(), "name".to_string()]),
    );
    assert_eq!(filtered.len(), 3);
    let e0 = &filtered[0];
    assert_eq!(e0.fields["id"], Value::Number(1.0));
    assert_eq!(e0.fields["name"], Value::Str("a".into()));
    assert!(!e0.fields.contains_key("active"));
    assert!(!e0.fields.contains_key("ts"));
    assert!(!e0.fields.contains_key("solo"));
    // Row 1: null name dropped, active=false still materialized via full path.
    assert_eq!(filtered[1].fields["id"], Value::Number(2.0));
    assert!(!filtered[1].fields.contains_key("name"));

    // Filtering to fields absent from the schema yields empty events.
    let none = batch_to_events_filtered(&batch, &HashSet::from(["ghost".to_string()]));
    assert_eq!(none.len(), 3);
    assert!(none[0].fields.is_empty());
}

#[test]
fn materialize_rows_skips_out_of_range_and_filters_fields() {
    let batch = mixed_batch();
    // Row 99 is out of range → skipped.
    let rows = materialize_rows(&batch, &[0, 1, 99]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].fields["id"], Value::Number(1.0));
    assert_eq!(rows[1].fields["id"], Value::Number(2.0));

    let filtered = materialize_rows_filtered(
        &batch,
        &[0, 1],
        &HashSet::from(["id".to_string(), "ts".to_string()]),
    );
    assert_eq!(filtered.len(), 2);
    assert_eq!(filtered[0].fields["ts"], Value::Number(1_000.0));
    assert!(!filtered[0].fields.contains_key("name"));
}

#[test]
fn batch_event_time_nanos_covers_other_types_and_null() {
    let batch = mixed_batch();
    let id_idx = batch_time_col_index(&batch, Some("id")).unwrap();
    let ts_idx = batch_time_col_index(&batch, Some("ts")).unwrap();
    // Int64 column as time: (value as f64) as i64.
    assert_eq!(batch_event_time_nanos_at(&batch, id_idx, 0), 1);
    // Timestamp column as time.
    assert_eq!(batch_event_time_nanos_at(&batch, ts_idx, 0), 1_000);
    // Null time cell → 0.
    assert_eq!(batch_event_time_nanos_at(&batch, id_idx, 2), 0);
    // Absent / None time field → 0 (batch_time_col_index → None).
    assert_eq!(batch_time_col_index(&batch, Some("ghost")), None);
    assert_eq!(batch_time_col_index(&batch, None), None);
    assert_eq!(batch_event_time_nanos(&batch, Some("ghost"), 0), 0);
    assert_eq!(batch_event_time_nanos(&batch, None, 0), 0);
}

#[test]
fn batch_to_timestamped_rows_requires_timestamp_column_and_skips_nulls() {
    let batch = mixed_batch();
    // A non-Timestamp column index → empty result.
    let id_idx = batch_time_col_index(&batch, Some("id")).unwrap();
    assert!(batch_to_timestamped_rows(&batch, id_idx).is_empty());

    let rows = batch_to_timestamped_rows(&batch, 3);
    // Null timestamp row (index 2) skipped; the ts column itself is retained.
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 1_000);
    assert_eq!(rows[1].0, 2_000);
    assert_eq!(rows[0].1["ts"], Value::Number(1_000.0));
    assert_eq!(rows[0].1["id"], Value::Number(1.0));
    assert_eq!(rows[1].1["id"], Value::Number(2.0));
}

#[test]
fn batch_to_events_covers_list_large_list_and_fixed_size_list() {
    let list =
        ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(10), Some(20)])]);
    let large = LargeListArray::try_new(
        Arc::new(Field::new("item", DataType::Int64, false)),
        OffsetBuffer::new(vec![0i64, 3].into()),
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        None,
    )
    .unwrap();
    let fixed = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int64, false)),
        2,
        Arc::new(Int64Array::from(vec![7, 8])) as ArrayRef,
        None,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("tags", list.data_type().clone(), true),
        Field::new("big", large.data_type().clone(), true),
        Field::new("pair", fixed.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(list) as ArrayRef,
            Arc::new(large) as ArrayRef,
            Arc::new(fixed) as ArrayRef,
        ],
    )
    .unwrap();

    let events = batch_to_events(&batch);
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].fields["tags"],
        Value::Array(vec![Value::Number(10.0), Value::Number(20.0)])
    );
    assert_eq!(
        events[0].fields["big"],
        Value::Array(vec![
            Value::Number(1.0),
            Value::Number(2.0),
            Value::Number(3.0)
        ])
    );
    assert_eq!(
        events[0].fields["pair"],
        Value::Array(vec![Value::Number(7.0), Value::Number(8.0)])
    );
}

#[test]
fn structured_utf8_json_metadata_helpers_and_parse_failures() {
    let obj_field = Field::new("obj", DataType::Utf8, true).with_metadata(HashMap::from([(
        WFL_FIELD_TYPE_METADATA_KEY.to_string(),
        WFL_FIELD_TYPE_OBJECT.to_string(),
    )]));
    let arr_field = Field::new("arr", DataType::Utf8, true).with_metadata(HashMap::from([(
        WFL_FIELD_TYPE_METADATA_KEY.to_string(),
        WFL_FIELD_TYPE_ARRAY.to_string(),
    )]));
    let malformed = Field::new("malformed", DataType::Utf8, true).with_metadata(HashMap::from([(
        WFL_FIELD_TYPE_METADATA_KEY.to_string(),
        WFL_FIELD_TYPE_OBJECT.to_string(),
    )]));
    let plain = Field::new("plain", DataType::Utf8, true);
    let weird = Field::new("weird", DataType::Utf8, true).with_metadata(HashMap::from([(
        WFL_FIELD_TYPE_METADATA_KEY.to_string(),
        "tuple".into(),
    )]));

    assert!(is_wfl_structured_field(&obj_field));
    assert!(is_wfl_structured_field(&arr_field));
    assert!(is_wfl_structured_field(&malformed));
    assert!(!is_wfl_structured_field(&plain));
    assert!(!is_wfl_structured_field(&weird));
    assert_eq!(
        wfl_structured_field_kind(&obj_field),
        Some(WFL_FIELD_TYPE_OBJECT)
    );
    assert_eq!(
        wfl_structured_field_kind(&arr_field),
        Some(WFL_FIELD_TYPE_ARRAY)
    );
    assert_eq!(wfl_structured_field_kind(&plain), None);
    assert_eq!(wfl_structured_field_kind(&weird), None);

    let schema = Arc::new(Schema::new(vec![
        obj_field, arr_field, malformed, weird, plain,
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            // Object kind but array payload → None (field dropped).
            Arc::new(StringArray::from(vec![r#"[1,2]"#])) as ArrayRef,
            // Array kind but object payload → None.
            Arc::new(StringArray::from(vec![r#"{"a":1}"#])) as ArrayRef,
            // Malformed JSON on a structured field → None (parse failure).
            Arc::new(StringArray::from(vec!["{not json"])) as ArrayRef,
            // Unrecognized metadata → plain string pass-through.
            Arc::new(StringArray::from(vec![r#"{"a":1}"#])) as ArrayRef,
            // No metadata → plain string pass-through.
            Arc::new(StringArray::from(vec!["{not json"])) as ArrayRef,
        ],
    )
    .unwrap();

    let events = batch_to_events(&batch);
    let e = &events[0];
    assert!(
        !e.fields.contains_key("obj"),
        "kind/payload mismatch → dropped"
    );
    assert!(
        !e.fields.contains_key("arr"),
        "kind/payload mismatch → dropped"
    );
    assert!(
        !e.fields.contains_key("malformed"),
        "invalid JSON → dropped"
    );
    assert_eq!(e.fields["weird"], Value::Str(r#"{"a":1}"#.into()));
    assert_eq!(e.fields["plain"], Value::Str("{not json".into()));
}

#[test]
fn column_scalar_string_covers_scalar_null_and_structured_cells() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Boolean, true),
        Field::new(
            "obj",
            DataType::Struct(vec![Field::new("x", DataType::Int64, false)].into()),
            true,
        ),
    ]));
    let obj = StructArray::from(vec![(
        Arc::new(Field::new("x", DataType::Int64, false)),
        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
    )]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(42), None])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as ArrayRef,
            Arc::new(obj) as ArrayRef,
        ],
    )
    .unwrap();

    assert_eq!(column_scalar_string(&batch, 0, 0), Some("42".to_string()));
    assert_eq!(column_scalar_string(&batch, 1, 0), Some("1.5".to_string()));
    assert_eq!(column_scalar_string(&batch, 2, 0), Some("a".to_string()));
    assert_eq!(column_scalar_string(&batch, 3, 0), Some("true".to_string()));
    // Null cell → None; structured cell → None.
    assert_eq!(column_scalar_string(&batch, 0, 1), None);
    assert_eq!(column_scalar_string(&batch, 4, 0), None);
}

#[test]
fn columnar_event_constructors_and_join_row_variants() {
    let batch = mixed_batch();
    let index = build_field_index(&batch);
    // build_field_index maps every schema column.
    assert_eq!(index.get("id").copied(), Some(0));
    assert_eq!(index.get("solo").copied(), Some(4));
    assert_eq!(index.get("ghost"), None);

    // `new` (no index) resolves through schema().index_of.
    let ce = ColumnarEvent::new(&batch, 0);
    assert_eq!(ce.field_value("id"), Some(Value::Number(1.0)));
    assert_eq!(ce.field_value("name"), Some(Value::Str("a".into())));
    assert_eq!(ce.field_value_str("name"), "a");
    assert_eq!(ce.field_value_str("id"), "");
    assert_eq!(ce.field_value("ghost"), None);
    assert_eq!(ce.to_event().fields["id"], Value::Number(1.0));

    // `with_index` + projected to_event reproduces the filtered materialization.
    let ce_idx = ColumnarEvent::with_index(&batch, 0, Arc::clone(&index));
    assert_eq!(ce_idx.field_value("solo"), Some(Value::Number(9.0)));
    let proj: Arc<HashSet<String>> =
        Arc::new(HashSet::from(["id".to_string(), "name".to_string()]));
    let projected = ColumnarEvent::with_index_projected(&batch, 0, Arc::clone(&index), Some(proj));
    let ev = projected.to_event();
    assert_eq!(ev.fields["id"], Value::Number(1.0));
    assert_eq!(ev.fields["name"], Value::Str("a".into()));
    assert!(!ev.fields.contains_key("ts"));

    // JoinRow::Event variant: field_value / field_names from a materialized map.
    let row = JoinRow::Event(Arc::new(event(vec![("ip", str_val("10.0.0.1"))])));
    assert_eq!(row.field_value("ip"), Some(str_val("10.0.0.1")));
    assert_eq!(row.field_value("missing"), None);
    assert_eq!(row.field_names(), vec!["ip"]);
}

#[test]
fn columnar_join_rows_and_timestamped_rows_share_batch_state() {
    let batch = mixed_batch();
    let rows = columnar_join_rows(vec![batch], None);
    assert_eq!(rows.len(), 3);
    let mut names = rows[0].field_names();
    names.sort_unstable();
    assert_eq!(names, vec!["active", "id", "name", "solo", "ts"]);
    assert_eq!(rows[0].field_value("id"), Some(Value::Number(1.0)));
    // Null cell reads None on the columnar view.
    assert_eq!(rows[1].field_value("name"), None);
    assert_eq!(rows[2].field_value("name"), Some(Value::Str("c".into())));

    let ts_rows = columnar_timestamped_join_rows(vec![mixed_batch()], 3, None);
    // Null-timestamp row (index 2) skipped.
    assert_eq!(ts_rows.len(), 2);
    assert_eq!(ts_rows[0].0, 1_000);
    assert_eq!(ts_rows[1].0, 2_000);
    assert_eq!(batch_raw_ts_nanos(&mixed_batch(), 3, 0), Some(1_000));
    assert_eq!(batch_raw_ts_nanos(&mixed_batch(), 3, 2), None);
}
