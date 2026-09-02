//! Core-coverage tests: exercise the match_engine building blocks that the
//! feature tests only reach indirectly — Event/Value/JoinKey/ScopeKey type
//! conversions, `WindowLookup` default impls, RecordBatch ↔ Event/JoinRow
//! columnar bridging, `RuleExecutor` query/build interfaces, `execute_joins`
//! mode dispatch (inner/snapshot/asof/anti/interval), close/on-each 收口 paths,
//! conv, and the inline-contract harness failure branches.
//!
//! Only test code lives here — no production logic is modified.
use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float64Array, Int64Array, LargeListArray,
    ListArray, StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int64Type, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{
    BinOp, Bound, BoundVal, CloseMode, Expr, FieldRef, JoinMode, PathSegment, WithinSpec,
};
use wf_lang::plan::{
    BindPlan, BranchPlan, ConvChainPlan, ConvOpPlan, EachPlan, JoinCondPlan, JoinPlan, LetPlan,
    SeqPlan, SeqSkipPlan, SeqStepPlan, SortKeyPlan, YieldField,
};
use wf_lang::{BaseType, FieldType};

use crate::match_engine::executor::{CloseCtxFields, build_eval_context, execute_joins};
use crate::match_engine::match_engine::{
    AsofLookup, BindData, CloseOutput, CloseReason, EngineHashMap, Event, FieldSource, JoinKey,
    MACHINE_ID, ScopeKey, StepData, Value, ValueKey, WindowLookup, eval_expr, field_ref_name,
    push_i64_exact_decimal, scope_key_from_values, scope_key_shard_index, value_to_string,
    values_equal,
};
use crate::match_engine::{
    ColumnarEvent, JoinRow, RuleExecutor, TriggerEvent, WFL_FIELD_TYPE_ARRAY,
    WFL_FIELD_TYPE_METADATA_KEY, WFL_FIELD_TYPE_OBJECT, apply_conv, batch_event_time_nanos,
    batch_event_time_nanos_at, batch_raw_ts_nanos, batch_time_col_index, batch_to_events,
    batch_to_events_filtered, batch_to_timestamped_rows, build_field_index, column_scalar_string,
    columnar_join_rows, columnar_timestamped_join_rows, extract_key_simple,
    is_wfl_structured_field, mask_to_indices, materialize_rows, materialize_rows_filtered,
    wfl_structured_field_kind,
};

use super::helpers::*;

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

// ===========================================================================
// executor/mod.rs — RuleExecutor build / query interfaces
// ===========================================================================

#[test]
fn rule_executor_basic_query_interfaces() {
    let mut plan = simple_rule_plan(
        "r_queries",
        simple_plan(vec![simple_key("sip")], vec![]),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.yield_plan.target = "sink_x".into();
    let exec = RuleExecutor::new(plan.clone());
    assert_eq!(exec.plan().name, "r_queries");
    assert_eq!(&**exec.static_yield_target(), "sink_x");
    assert_eq!(exec.output_config().time_format, "%Y-%m-%d %H:%M:%S%.3f");
    // No `where` → everything passes.
    assert!(exec.where_ok(&event(vec![("sip", str_val("x"))])));
    // machine_id_of extracts the `wp_src_ip` field.
    assert_eq!(
        RuleExecutor::machine_id_of(&event(vec![(MACHINE_ID, str_val("10.0.0.1"))])),
        "10.0.0.1"
    );
    assert_eq!(RuleExecutor::machine_id_of(&event(vec![])), "");
}

#[test]
fn where_ok_is_strict_on_missing_and_false() {
    let mut plan = simple_rule_plan(
        "r_where",
        simple_plan(vec![], vec![]),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".to_string()))),
        right: Box::new(Expr::StringLit("10.0.0.1".to_string())),
    });
    let exec = RuleExecutor::new(plan);
    assert!(exec.where_ok(&event(vec![("sip", str_val("10.0.0.1"))])));
    assert!(!exec.where_ok(&event(vec![("sip", str_val("10.0.0.2"))])));
    assert!(
        !exec.where_ok(&event(vec![])),
        "missing field suppresses output"
    );
}

#[test]
fn cached_emit_time_formats_once_per_nanos() {
    let exec = RuleExecutor::new(simple_rule_plan(
        "r_time",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    ));
    let nanos = 1_700_000_000_000_000_000i64;
    let a = exec.cached_emit_time(nanos);
    let b = exec.cached_emit_time(nanos);
    assert_eq!(a, b);
    assert!(Arc::ptr_eq(&a, &b), "same nanos → same cached Arc");
    let c = exec.cached_emit_time(nanos + 1_000_000_000);
    assert_ne!(a, c, "different nanos → different formatted time");
    assert!(a.contains('T'), "ISO-8601 formatting");
}

#[test]
fn coerce_yield_field_value_covered_for_all_types_and_failures() {
    // Chars: strings pass through; scalars render; structured serialize to JSON.
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Chars)),
            Value::Str("x".into())
        ),
        Ok(Some(Value::Str("x".into())))
    );
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Chars)),
            Value::Number(1.5)
        ),
        Ok(Some(Value::Str("1.5".into())))
    );
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Chars)),
            Value::Bool(true)
        ),
        Ok(Some(Value::Str("true".into())))
    );
    // Array → JSON string; non-finite number → error.
    assert!(matches!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Chars)),
            Value::Array(vec![Value::Number(1.0), Value::Str("x".into())])
        ),
        Ok(Some(Value::Str(s))) if s == r#"[1.0,"x"]"#
    ));
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Chars)),
            Value::Number(f64::NAN)
        )
        .is_err()
    );

    // Empty string degrades to "omit" for non-Chars targets.
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Float)),
            Value::Str("".into())
        ),
        Ok(None)
    );
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Chars)),
            Value::Str("".into())
        ),
        Ok(Some(Value::Str("".into())))
    );

    // Digit: integer numbers pass, fractional / non-number fail.
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Digit)),
            Value::Number(3.0)
        ),
        Ok(Some(Value::Number(3.0)))
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Digit)),
            Value::Number(3.5)
        )
        .is_err()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Digit)),
            Value::Str("3".into())
        )
        .is_err()
    );

    // Float: finite numbers pass; NaN / non-number fail.
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Float)),
            Value::Number(1.5)
        )
        .is_ok()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Float)),
            Value::Number(f64::NAN)
        )
        .is_err()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Float)),
            Value::Bool(true)
        )
        .is_err()
    );

    // Bool: only booleans.
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Bool)),
            Value::Bool(false)
        )
        .is_ok()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Bool)),
            Value::Str("true".into())
        )
        .is_err()
    );

    // Time: epoch numbers normalize; out-of-range / non-number fail.
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Time)),
            Value::Number(1_700_000_000_000_000_000.0)
        )
        .is_ok()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Time)),
            Value::Number(1e300)
        )
        .is_err()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Time)),
            Value::Str("now".into())
        )
        .is_err()
    );

    // Ip: valid literal passes, invalid fails, non-string fails.
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Ip)),
            Value::Str("10.0.0.1".into())
        )
        .is_ok()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Ip)),
            Value::Str("not-an-ip".into())
        )
        .is_err()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Ip)),
            Value::Number(1.0)
        )
        .is_err()
    );

    // Hex: 0x / 0X / bare hex strings and non-negative integers pass.
    for ok in ["0x1F", "0Xff", "ff", "deadBEEF"] {
        assert!(
            RuleExecutor::coerce_yield_field_value_with(
                "f",
                Some(&FieldType::Base(BaseType::Hex)),
                Value::Str(ok.into())
            )
            .is_ok(),
            "valid hex {ok:?} must pass"
        );
    }
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Hex)),
            Value::Number(16.0)
        )
        .is_ok()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Hex)),
            Value::Str("zz".into())
        )
        .is_err()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Hex)),
            Value::Number(-1.0)
        )
        .is_err()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Base(BaseType::Hex)),
            Value::Number(1.5)
        )
        .is_err()
    );

    // Structured field types: array/object values pass, scalars fail.
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Array(BaseType::Chars)),
            Value::Array(vec![Value::Str("a".into())])
        )
        .is_ok()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Array(BaseType::Chars)),
            Value::Number(1.0)
        )
        .is_err()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::ArrayAny),
            Value::Array(vec![])
        )
        .is_ok()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Object),
            Value::Object(EngineHashMap::default())
        )
        .is_ok()
    );
    assert!(
        RuleExecutor::coerce_yield_field_value_with(
            "f",
            Some(&FieldType::Object),
            Value::Number(1.0)
        )
        .is_err()
    );

    // No declared type → value passes through untouched.
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", None, Value::Number(1.0)),
        Ok(Some(Value::Number(1.0)))
    );
}

#[test]
fn yield_kinds_precomputed_per_expression_class() {
    use crate::match_engine::executor::YieldKind;
    let mut plan = simple_rule_plan(
        "r_kinds",
        simple_plan(vec![], vec![]),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "lit_n".into(),
            value: Expr::Number(1.0),
        },
        YieldField {
            name: "lit_s".into(),
            value: Expr::StringLit("s".into()),
        },
        YieldField {
            name: "lit_b".into(),
            value: Expr::Bool(true),
        },
        YieldField {
            name: "fld".into(),
            value: Expr::Field(FieldRef::Simple("sip".into())),
        },
        YieldField {
            name: "gen".into(),
            value: Expr::BinOp {
                op: BinOp::Add,
                left: Box::new(Expr::Number(1.0)),
                right: Box::new(Expr::Number(2.0)),
            },
        },
    ];
    let exec = RuleExecutor::new(plan);
    let kinds = &exec.output_static().yield_kinds;
    assert!(matches!(kinds[0], YieldKind::Lit(Value::Number(1.0))));
    assert!(matches!(kinds[1], YieldKind::Lit(Value::Str(ref s)) if s == "s"));
    assert!(matches!(kinds[2], YieldKind::Lit(Value::Bool(true))));
    assert!(matches!(kinds[3], YieldKind::Field));
    assert!(matches!(kinds[4], YieldKind::General));
    assert_eq!(exec.output_static().score_const, Some(70.0));
    // Constant score is clamped into [0, 100] at construction.
    let plan_hi = simple_rule_plan(
        "r_hi",
        simple_plan(vec![], vec![]),
        Expr::Number(150.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    assert_eq!(
        RuleExecutor::new(plan_hi).output_static().score_const,
        Some(100.0)
    );
    // Non-literal score → no constant.
    let plan_dyn = simple_rule_plan(
        "r_dyn",
        simple_plan(vec![], vec![]),
        Expr::Field(FieldRef::Simple("sip".into())),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    assert_eq!(
        RuleExecutor::new(plan_dyn).output_static().score_const,
        None
    );
}

#[test]
fn event_matches_alias_with_filters_and_many_binds_map_path() {
    // A single bind with a filter: matching event passes, non-matching fails.
    let mut plan = simple_rule_plan(
        "r_bind",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.binds[0].filter = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".to_string()))),
        right: Box::new(Expr::StringLit("10.0.0.1".to_string())),
    });
    let exec = RuleExecutor::new(plan);
    assert!(exec.event_matches_alias("fail", &event(vec![("sip", str_val("10.0.0.1"))]), None));
    assert!(!exec.event_matches_alias("fail", &event(vec![("sip", str_val("10.0.0.2"))]), None));
    // Missing field → filter evaluates to None → rejects.
    assert!(!exec.event_matches_alias("fail", &event(vec![]), None));
    // Unknown alias → no filter → passes.
    assert!(exec.event_matches_alias("ghost", &event(vec![]), None));

    // 25 binds (more than the 24-bind crossover) → the precomputed map path.
    let mut many = simple_rule_plan(
        "r_many",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    many.binds = (0..25)
        .map(|i| BindPlan {
            alias: format!("b{i}"),
            window: "w".into(),
            filter: None,
        })
        .collect();
    let exec_many = RuleExecutor::new(many);
    assert!(exec_many.event_matches_alias("b13", &event(vec![]), None));
    // Unknown alias → no filter → passes (same as the single-bind plan).
    assert!(exec_many.event_matches_alias("b99", &event(vec![]), None));
}

fn eq_str_expr(field: &str, val: &str) -> Expr {
    Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple(field.to_string()))),
        right: Box::new(Expr::StringLit(val.to_string())),
    }
}

fn string_batch(rows: &[(&str, Option<&str>)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new("action", DataType::Utf8, true),
    ]));
    let sip: Vec<Option<&str>> = rows.iter().map(|r| Some(r.0)).collect();
    let action: Vec<Option<&str>> = rows.iter().map(|r| r.1).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(sip)) as ArrayRef,
            Arc::new(StringArray::from(action)) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn bind_filter_columnar_mask_and_safety_gates() {
    let mut plan = simple_rule_plan(
        "r_col",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.binds[0].filter = Some(eq_str_expr("sip", "10.0.0.1"));
    let exec = RuleExecutor::new(plan);
    let batch = string_batch(&[
        ("10.0.0.1", Some("a")),
        ("10.0.0.2", Some("b")),
        ("10.0.0.1", None),
    ]);
    let mask = exec
        .bind_filter_columnar_mask("fail", &batch)
        .expect("columnar filter");
    assert_eq!(mask.len(), 3);
    assert!(mask.value(0));
    assert!(!mask.value(1));
    // Columnar-safe: the filter is columnar.
    assert!(exec.bind_filters_columnar_safe("w"));

    // Non-columnar filter (FuncCall) → mask None, safe = false.
    let mut plan2 = simple_rule_plan(
        "r_col2",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan2.binds[0].filter = Some(Expr::FuncCall {
        qualifier: None,
        name: "len".into(),
        args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
    });
    let exec2 = RuleExecutor::new(plan2);
    assert!(exec2.bind_filter_columnar_mask("fail", &batch).is_none());
    assert!(!exec2.bind_filters_columnar_safe("w"));

    // Window with no binds → trivially safe.
    assert!(exec.bind_filters_columnar_safe("other_window"));
}

#[test]
fn each_filter_columnar_mask_and_branch_guard_masks() {
    // Columnar each filter → mask; non-columnar / absent → None.
    let mut plan = simple_rule_plan(
        "r_each_col",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(eq_str_expr("sip", "10.0.0.1")),
    });
    let exec = RuleExecutor::new(plan);
    let batch = string_batch(&[("10.0.0.1", Some("a")), ("10.0.0.2", Some("b"))]);
    let mask = exec
        .each_filter_columnar_mask(&batch)
        .expect("columnar each filter");
    assert!(mask.value(0));
    assert!(!mask.value(1));

    let mut plan2 = simple_rule_plan(
        "r_each_col2",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan2.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::FuncCall {
            qualifier: None,
            name: "len".into(),
            args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
        }),
    });
    let exec2 = RuleExecutor::new(plan2);
    assert!(exec2.each_filter_columnar_mask(&batch).is_none());
    // No each plan → None.
    let plain = RuleExecutor::new(simple_rule_plan(
        "r_plain",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    ));
    assert!(plain.each_filter_columnar_mask(&batch).is_none());

    // branch_guard_masks: event + close + seq-negation guards.
    let mut mplan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "a".into(),
            field: None,
            guard: Some(eq_str_expr("sip", "10.0.0.1")),
            agg: count_ge(1.0),
        }])],
    );
    mplan.close_steps = vec![step(vec![BranchPlan {
        label: None,
        source: "a".into(),
        field: None,
        guard: Some(eq_str_expr("action", "blocked")),
        agg: count_ge(1.0),
    }])];
    mplan.seq = Some(SeqPlan {
        consec: false,
        skip: SeqSkipPlan::PastLast,
        steps: vec![
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("a", count_ge(1.0)),
            },
            SeqStepPlan {
                neg: true,
                within: None,
                branch: BranchPlan {
                    label: None,
                    source: "c".into(),
                    field: None,
                    guard: Some(eq_str_expr("sip", "10.0.0.2")),
                    agg: count_ge(1.0),
                },
            },
        ],
    });
    let rplan = simple_rule_plan(
        "r_guards",
        mplan,
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec_g = RuleExecutor::new(rplan);
    let batch = string_batch(&[("10.0.0.1", Some("blocked")), ("10.0.0.2", Some("login"))]);
    let masks = exec_g.branch_guard_masks(&batch);
    assert!(!masks.is_empty());
    assert_eq!(masks.event_value(0, 0, 0), Some(true));
    assert_eq!(masks.event_value(0, 0, 1), Some(false));
    assert_eq!(masks.close_value(0, 0, 0), Some(Some(true)));
    assert_eq!(masks.close_value(0, 0, 1), Some(Some(false)));
    assert_eq!(masks.neg_value(0, 0, 1), Some(true));
    assert_eq!(masks.event_value(1, 0, 0), None, "no mask for unknown step");

    // mask_to_indices converts a BooleanArray into row indices.
    let indices = mask_to_indices(&mask);
    assert_eq!(indices, vec![0]);
}

#[test]
fn is_aux_bind_alias_and_build_machine_id_helpers() {
    let plan = simple_rule_plan(
        "r_aux",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    assert!(
        !exec.is_aux_bind_alias("b"),
        "branch source alias is not aux"
    );
    assert!(exec.is_aux_bind_alias("other"), "unused alias is aux");
    // Empty machine id falls back to the rule name.
    assert_eq!(exec.build_machine_id("").as_ref(), "r_aux");
    assert_eq!(exec.build_machine_id("m1").as_ref(), "m1");
}

// ===========================================================================
// executor/context.rs — build_eval_context + execute_joins mode dispatch
// ===========================================================================

fn step_data(label: Option<&str>, measure: f64, field_values: Vec<(&str, Vec<Value>)>) -> StepData {
    StepData {
        satisfied_branch_index: 0,
        label: label.map(|s| s.to_string()),
        measure_value: measure,
        event_first_time_nanos: None,
        event_last_time_nanos: None,
        collected_values: vec![Value::Number(1.0), Value::Number(2.0)],
        field_values: field_values
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    }
}

#[test]
fn build_eval_context_all_mode_materializes_every_synthetic_field() {
    let keys = vec![simple_key("sip")];
    let scope_key = vec![str_val("10.0.0.1")];
    let sd = step_data(
        Some("fail"),
        5.0,
        vec![("user", vec![str_val("a"), str_val("b")])],
    );
    let bd = BindData {
        alias: "w".into(),
        count: 3,
        field_values: [("dport".to_string(), vec![num(80.0)])]
            .into_iter()
            .collect(),
    };
    let step_plans = [step(vec![branch("b", count_ge(1.0))])];
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[sd],
        &[bd],
        &[&step_plans[0]],
        None,
        &CloseCtxFields::All,
        None,
    );

    assert_eq!(ctx.fields["sip"], str_val("10.0.0.1"));
    assert_eq!(ctx.fields["fail"], Value::Number(5.0));
    assert_eq!(
        ctx.fields["_step_0_values"],
        Value::Array(vec![Value::Number(1.0), Value::Number(2.0)])
    );
    assert_eq!(
        ctx.fields["_step_0_field_user"],
        Value::Array(vec![str_val("a"), str_val("b")])
    );
    assert_eq!(
        ctx.fields["user"],
        str_val("b"),
        "last value wins for bare names"
    );
    assert_eq!(ctx.fields["_step_0_measure"], Value::Number(5.0));
    assert_eq!(ctx.fields["_step_0_label"], Value::Str("fail".into()));
    assert_eq!(ctx.fields["_step_0_source"], Value::Str("b".into()));
    assert_eq!(ctx.fields["_bind_w_count"], Value::Number(3.0));
    assert_eq!(
        ctx.fields["_bind_w_field_dport"],
        Value::Array(vec![num(80.0)])
    );
}

#[test]
fn build_eval_context_named_mode_and_trigger_event_precedence() {
    let keys = vec![simple_key("sip")];
    let scope_key = vec![str_val("10.0.0.1")];
    let sd = step_data(
        Some("fail"),
        5.0,
        vec![("user", vec![str_val("a"), str_val("b")])],
    );
    let bd = BindData {
        alias: "w".into(),
        count: 3,
        field_values: [("dport".to_string(), vec![num(80.0)])]
            .into_iter()
            .collect(),
    };
    let step_plans = [step(vec![branch("b", count_ge(1.0))])];
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[sd],
        &[bd],
        &[&step_plans[0]],
        None,
        &CloseCtxFields::Named(HashSet::from(["user".to_string()])),
        None,
    );
    // Only the key + the one requested bare field are present.
    assert_eq!(ctx.fields["sip"], str_val("10.0.0.1"));
    assert_eq!(ctx.fields["user"], str_val("b"));
    assert!(!ctx.fields.contains_key("fail"));
    assert!(!ctx.fields.contains_key("_step_0_measure"));
    assert!(!ctx.fields.contains_key("_bind_w_count"));

    // Trigger event fields inject scalars the history lacks (keys win).
    let trigger = event(vec![("user", str_val("trigger-user")), ("extra", num(7.0))]);
    let ctx2 = build_eval_context(
        &keys,
        &scope_key,
        &[],
        &[],
        &[],
        Some(&TriggerEvent::from_event(Arc::new(trigger.clone()))),
        &CloseCtxFields::All,
        None,
    );
    assert_eq!(
        ctx2.fields["sip"],
        str_val("10.0.0.1"),
        "key wins over trigger"
    );
    assert_eq!(ctx2.fields["extra"], Value::Number(7.0));
    assert_eq!(ctx2.fields["user"], str_val("trigger-user"));

    // A step label colliding with a key field is skipped (key priority).
    let collision = step_data(Some("sip"), 99.0, vec![]);
    let ctx3 = build_eval_context(
        &keys,
        &scope_key,
        &[collision],
        &[],
        &[&step_plans[0]],
        None,
        &CloseCtxFields::All,
        None,
    );
    assert_eq!(
        ctx3.fields["sip"],
        str_val("10.0.0.1"),
        "label must not overwrite key"
    );
}

/// A `WindowLookup` for the `execute_joins` unit tests: `snapshot` /
/// `snapshot_with_timestamps` back the default `join_lookup` / `asof_candidates`
/// impls; `asof_fast` drives the single-condition O(1) fast path.
///
/// `rows`/`ts_rows` 按字符串键索引（多键 join 用）；`asof_fast` 命中时
/// `join_lookup_asof` 走 O(1) 快路径。
type JoinLookupRows = HashMap<String, Vec<HashMap<String, Value>>>;
type JoinLookupTsRows = HashMap<String, Vec<(i64, HashMap<String, Value>)>>;
struct JoinLookup {
    rows: JoinLookupRows,
    ts_rows: JoinLookupTsRows,
    asof_fast: Option<AsofOutcome>,
}

enum AsofOutcome {
    Hit(HashMap<String, Value>),
    Miss,
}

impl JoinLookup {
    fn new() -> Self {
        Self {
            rows: HashMap::new(),
            ts_rows: HashMap::new(),
            asof_fast: None,
        }
    }
    fn row(fields: Vec<(&str, Value)>) -> HashMap<String, Value> {
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }
    fn add_row(&mut self, window: &str, fields: Vec<(&str, Value)>) {
        self.rows
            .entry(window.to_string())
            .or_default()
            .push(Self::row(fields));
    }
    fn add_ts_row(&mut self, window: &str, ts: i64, fields: Vec<(&str, Value)>) {
        self.ts_rows
            .entry(window.to_string())
            .or_default()
            .push((ts, Self::row(fields)));
    }
    fn to_join_row(map: HashMap<String, Value>) -> JoinRow {
        JoinRow::Event(Arc::new(Event {
            fields: map.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }))
    }
}

impl WindowLookup for JoinLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, window: &str) -> Option<Vec<JoinRow>> {
        self.rows
            .get(window)
            .map(|rows| rows.iter().cloned().map(Self::to_join_row).collect())
    }
    fn snapshot_with_timestamps(&self, window: &str) -> Option<Vec<(i64, JoinRow)>> {
        self.ts_rows.get(window).map(|rows| {
            rows.iter()
                .map(|(ts, r)| (*ts, Self::to_join_row(r.clone())))
                .collect()
        })
    }
    fn asof_lookup_max(
        &self,
        _w: &str,
        _kf: &str,
        _k: &Value,
        _event_time_nanos: i64,
        _within: Option<&Duration>,
    ) -> AsofLookup {
        match &self.asof_fast {
            Some(AsofOutcome::Hit(row)) => AsofLookup::Hit(Self::to_join_row(row.clone())),
            Some(AsofOutcome::Miss) => AsofLookup::Miss,
            None => AsofLookup::Fallback,
        }
    }
}

fn join_plan(mode: JoinMode, window: &str, left: &str, right: &str) -> JoinPlan {
    JoinPlan {
        right_window: window.to_string(),
        mode,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple(left.to_string()),
            right: FieldRef::Simple(right.to_string()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }
}

#[test]
fn execute_joins_inner_drops_on_miss_and_enriches_on_hit() {
    // Inner miss (no rows) → drop.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let lookup = JoinLookup::new();
    assert!(!execute_joins(
        &[join_plan(JoinMode::Inner, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        0
    ));
    assert!(!ctx.fields.contains_key("geo.ip"));

    // Inner hit → enriched with qualified + plain names.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.1")), ("country", str_val("US"))],
    );
    assert!(execute_joins(
        &[join_plan(JoinMode::Inner, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        0
    ));
    assert_eq!(ctx.fields["geo.ip"], str_val("10.0.0.1"));
    assert_eq!(ctx.fields["geo.country"], str_val("US"));
    assert_eq!(
        ctx.fields["country"],
        str_val("US"),
        "plain name enriched when absent"
    );

    // Inner with the left key field missing → drop without a lookup.
    let mut ctx = event(vec![]);
    assert!(!execute_joins(
        &[join_plan(JoinMode::Inner, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        0
    ));
}

#[test]
fn execute_joins_snapshot_miss_keeps_event_and_anti_drops_on_match() {
    // Snapshot miss → keep the event, no enrichment.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let lookup = JoinLookup::new();
    assert!(execute_joins(
        &[join_plan(JoinMode::Snapshot, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        0
    ));
    assert!(!ctx.fields.contains_key("geo.ip"));

    // Snapshot miss with rows present but none matching → still kept.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.2")), ("country", str_val("DE"))],
    );
    assert!(execute_joins(
        &[join_plan(JoinMode::Snapshot, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        0
    ));
    assert!(!ctx.fields.contains_key("geo.country"));

    // Anti: matching row → drop.
    let mut lookup_match = JoinLookup::new();
    lookup_match.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.1")), ("country", str_val("US"))],
    );
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(!execute_joins(
        &[join_plan(JoinMode::Anti, "geo", "sip", "ip")],
        &mut ctx,
        &lookup_match,
        0
    ));
    // Anti: no matching row → keep, no enrichment.
    let mut ctx = event(vec![("sip", str_val("10.0.0.9"))]);
    assert!(execute_joins(
        &[join_plan(JoinMode::Anti, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        0
    ));
    assert!(!ctx.fields.contains_key("geo.ip"), "anti never enriches");
}

#[test]
fn execute_joins_asof_fast_path_hit_miss_and_fallback() {
    // Fast-path Hit → enriched with the provided row.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.asof_fast = Some(AsofOutcome::Hit(JoinLookup::row(vec![
        ("ip", str_val("10.0.0.1")),
        ("risk", num(90.0)),
    ])));
    assert!(execute_joins(
        &[join_plan(
            JoinMode::Asof { within: None },
            "ti",
            "sip",
            "ip"
        )],
        &mut ctx,
        &lookup,
        1_000
    ));
    assert_eq!(ctx.fields["ti.risk"], Value::Number(90.0));

    // Fast-path Miss → keep the event without enrichment.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.asof_fast = Some(AsofOutcome::Miss);
    assert!(execute_joins(
        &[join_plan(
            JoinMode::Asof { within: None },
            "ti",
            "sip",
            "ip"
        )],
        &mut ctx,
        &lookup,
        1_000
    ));
    assert!(!ctx.fields.contains_key("ti.risk"));

    // Fallback → timestamped candidate scan picks the latest ts ≤ event time.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "ti",
        200,
        vec![("ip", str_val("10.0.0.1")), ("risk", num(50.0))],
    );
    lookup.add_ts_row(
        "ti",
        800,
        vec![("ip", str_val("10.0.0.1")), ("risk", num(90.0))],
    );
    lookup.add_ts_row(
        "ti",
        2_000,
        vec![("ip", str_val("10.0.0.1")), ("risk", num(99.0))],
    );
    assert!(execute_joins(
        &[join_plan(
            JoinMode::Asof { within: None },
            "ti",
            "sip",
            "ip"
        )],
        &mut ctx,
        &lookup,
        1_000
    ));
    assert_eq!(
        ctx.fields["ti.risk"],
        Value::Number(90.0),
        "latest row ≤ event time"
    );

    // with `within`, rows older than event_time - within are excluded.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "ti",
        100,
        vec![("ip", str_val("10.0.0.1")), ("risk", num(10.0))],
    );
    lookup.add_ts_row(
        "ti",
        900,
        vec![("ip", str_val("10.0.0.1")), ("risk", num(90.0))],
    );
    let within_join = JoinPlan {
        mode: JoinMode::Asof {
            within: Some(Duration::from_millis(500)),
        },
        ..join_plan(JoinMode::Asof { within: None }, "ti", "sip", "ip")
    };
    assert!(execute_joins(&[within_join], &mut ctx, &lookup, 1_000));
    assert_eq!(ctx.fields["ti.risk"], Value::Number(90.0));
}

#[test]
fn execute_joins_asof_multi_condition_uses_candidate_scan() {
    // Two conditions force the full asof_candidates scan (no fast path).
    let mut ctx = event(vec![("sip", str_val("10.0.0.1")), ("zone", num(7.0))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "ti",
        100,
        vec![
            ("ip", str_val("10.0.0.1")),
            ("zone", num(7.0)),
            ("risk", num(10.0)),
        ],
    );
    lookup.add_ts_row(
        "ti",
        500,
        vec![
            ("ip", str_val("10.0.0.1")),
            ("zone", num(8.0)),
            ("risk", num(99.0)),
        ],
    );
    lookup.add_ts_row(
        "ti",
        900,
        vec![
            ("ip", str_val("10.0.0.1")),
            ("zone", num(7.0)),
            ("risk", num(80.0)),
        ],
    );
    let multi = JoinPlan {
        conds: vec![
            JoinCondPlan {
                left: FieldRef::Simple("sip".into()),
                right: FieldRef::Simple("ip".into()),
            },
            JoinCondPlan {
                left: FieldRef::Simple("zone".into()),
                right: FieldRef::Simple("zone".into()),
            },
        ],
        ..join_plan(JoinMode::Asof { within: None }, "ti", "sip", "ip")
    };
    assert!(execute_joins(&[multi], &mut ctx, &lookup, 1_000));
    // The zone=8 row (ts=500, newer than zone=7 ts=100) fails the second cond;
    // the newest matching row is ts=900 (zone=7).
    assert_eq!(ctx.fields["ti.risk"], Value::Number(80.0));
}

fn interval_join(mode: JoinMode, window: &str, left: &str, right: &str) -> JoinPlan {
    JoinPlan {
        right_window: window.to_string(),
        mode,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple(left.to_string()),
            right: FieldRef::Simple(right.to_string()),
        }],
        within: Some(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::from_millis(500),
                    neg: true,
                },
            },
            hi: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::ZERO,
                    neg: false,
                },
            },
        }),
        reduce: None,
        emit_at: None,
    }
}

#[test]
fn execute_joins_interval_inner_hit_miss_and_open_bound() {
    let event_time = 1_000_000_000i64; // 1s

    // Inner hit inside [event-500ms, event] → enriched, kept.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "geo",
        750_000_000,
        vec![("ip", str_val("10.0.0.1")), ("city", str_val("NYC"))],
    );
    assert!(execute_joins(
        &[interval_join(JoinMode::Inner, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        event_time
    ));
    assert_eq!(ctx.fields["geo.city"], str_val("NYC"));

    // Inner miss (row outside the interval) → dropped.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "geo",
        100_000_000,
        vec![("ip", str_val("10.0.0.1")), ("city", str_val("SF"))],
    );
    assert!(!execute_joins(
        &[interval_join(JoinMode::Inner, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        event_time
    ));

    // Inner miss (no candidate rows at all) → dropped.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let lookup = JoinLookup::new();
    assert!(!execute_joins(
        &[interval_join(JoinMode::Inner, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        event_time
    ));

    // Open upper bound: a row exactly at `event_time` is excluded.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "geo",
        event_time,
        vec![("ip", str_val("10.0.0.1")), ("city", str_val("BOS"))],
    );
    let open_hi = JoinPlan {
        within: Some(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::from_millis(500),
                    neg: true,
                },
            },
            hi: Bound {
                open: true,
                val: BoundVal::Dur {
                    dur: Duration::ZERO,
                    neg: false,
                },
            },
        }),
        ..interval_join(JoinMode::Inner, "geo", "sip", "ip")
    };
    assert!(!execute_joins(&[open_hi], &mut ctx, &lookup, event_time));
}

#[test]
fn execute_joins_interval_modes_anti_asof_snapshot_and_bound_expression() {
    let event_time = 1_000_000_000i64;

    // Anti: a row inside the interval → drop; none → keep.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row("geo", 800_000_000, vec![("ip", str_val("10.0.0.1"))]);
    assert!(!execute_joins(
        &[interval_join(JoinMode::Anti, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        event_time
    ));
    let mut ctx = event(vec![("sip", str_val("10.0.0.9"))]);
    assert!(execute_joins(
        &[interval_join(JoinMode::Anti, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        event_time
    ));

    // Asof inside interval → latest ts; Snapshot → earliest ts.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "geo",
        600_000_000,
        vec![("ip", str_val("10.0.0.1")), ("v", num(1.0))],
    );
    lookup.add_ts_row(
        "geo",
        900_000_000,
        vec![("ip", str_val("10.0.0.1")), ("v", num(2.0))],
    );
    assert!(execute_joins(
        &[interval_join(
            JoinMode::Asof { within: None },
            "geo",
            "sip",
            "ip"
        )],
        &mut ctx,
        &lookup,
        event_time
    ));
    assert_eq!(
        ctx.fields["geo.v"],
        Value::Number(2.0),
        "interval asof picks latest"
    );
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(execute_joins(
        &[interval_join(JoinMode::Snapshot, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        event_time
    ));
    assert_eq!(
        ctx.fields["geo.v"],
        Value::Number(1.0),
        "interval snapshot picks earliest"
    );

    // Expr bound: evaluates the left row's absolute time field. A missing
    // field on an Inner join → conservative drop.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row("geo", 800_000_000, vec![("ip", str_val("10.0.0.1"))]);
    let expr_bounds = JoinPlan {
        within: Some(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Expr(Expr::Field(FieldRef::Simple("lo_field".into()))),
            },
            hi: Bound {
                open: false,
                val: BoundVal::Expr(Expr::Field(FieldRef::Simple("hi_field".into()))),
            },
        }),
        ..interval_join(JoinMode::Inner, "geo", "sip", "ip")
    };
    assert!(!execute_joins(
        &[expr_bounds],
        &mut ctx,
        &lookup,
        event_time
    ));

    // Expr bound with a valid numeric field on the left row. Epoch
    // normalization maps 0.8 → 8e8 ns and 1.0 → 1e9 ns, which contains the
    // 8e8-ns candidate row.
    let mut ctx = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("lo_field", num(0.8)),
        ("hi_field", num(1.0)),
    ]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "geo",
        800_000_000,
        vec![("ip", str_val("10.0.0.1")), ("city", str_val("CHI"))],
    );
    let expr_bounds = JoinPlan {
        within: Some(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Expr(Expr::Field(FieldRef::Simple("lo_field".into()))),
            },
            hi: Bound {
                open: false,
                val: BoundVal::Expr(Expr::Field(FieldRef::Simple("hi_field".into()))),
            },
        }),
        ..interval_join(JoinMode::Inner, "geo", "sip", "ip")
    };
    assert!(execute_joins(&[expr_bounds], &mut ctx, &lookup, event_time));
    assert_eq!(ctx.fields["geo.city"], str_val("CHI"));
}

#[test]
fn execute_joins_skips_emit_at_deferred_joins() {
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.1")), ("country", str_val("US"))],
    );
    let deferred = JoinPlan {
        emit_at: Some(Expr::Number(1.0)),
        ..join_plan(JoinMode::Inner, "geo", "sip", "ip")
    };
    // `emit at` joins are handled by the deferred path — eager path skips.
    assert!(execute_joins(&[deferred], &mut ctx, &lookup, 0));
    assert!(!ctx.fields.contains_key("geo.country"));
}

#[test]
fn eval_expr_resolves_event_fields_for_bound_expressions() {
    let ev = event(vec![("x", num(42.0)), ("s", str_val("ok"))]);
    assert_eq!(
        eval_expr(&Expr::Field(FieldRef::Simple("x".into())), &ev),
        Some(Value::Number(42.0))
    );
    assert_eq!(
        eval_expr(
            &Expr::BinOp {
                op: BinOp::Add,
                left: Box::new(Expr::Field(FieldRef::Simple("x".into()))),
                right: Box::new(Expr::Number(8.0)),
            },
            &ev
        ),
        Some(Value::Number(50.0))
    );
    assert_eq!(
        eval_expr(&Expr::Field(FieldRef::Simple("missing".into())), &ev),
        None
    );
}

/// L1 求值器（eval_expr_ext，guard/where 路径）的 match 表达式（issue #79
/// Issue 2）：多模式命中、默认分支、无默认未命中 → None。
#[test]
fn eval_expr_l1_match_expression() {
    use wf_lang::ast::MatchArm;
    let ev = event(vec![("sev", str_val("crit")), ("n", num(2.0))]);
    let sev = Expr::Match {
        expr: Box::new(Expr::Field(FieldRef::Simple("sev".into()))),
        arms: vec![MatchArm {
            patterns: vec![
                Expr::StringLit("crit".into()),
                Expr::StringLit("alert".into()),
            ],
            value: Expr::StringLit("CRITICAL".into()),
        }],
        default: Some(Box::new(Expr::Field(FieldRef::Simple("sev".into())))),
    };
    assert_eq!(
        eval_expr(&sev, &ev),
        Some(Value::Str("CRITICAL".into())),
        "crit | alert → CRITICAL"
    );
    let ev2 = event(vec![("sev", str_val("info"))]);
    assert_eq!(
        eval_expr(&sev, &ev2),
        Some(Value::Str("info".into())),
        "未命中 → 默认分支（原值透传）"
    );
    // 无默认且未命中 → None（guard 语义：filter 不通过）。
    let no_default = Expr::Match {
        expr: Box::new(Expr::Field(FieldRef::Simple("n".into()))),
        arms: vec![MatchArm {
            patterns: vec![Expr::Number(1.0), Expr::Number(2.0)],
            value: Expr::Bool(true),
        }],
        default: None,
    };
    assert_eq!(
        eval_expr(&no_default, &ev),
        Some(Value::Bool(true)),
        "n=2 命中数字模式"
    );
    let ev3 = event(vec![("n", num(9.0))]);
    assert_eq!(eval_expr(&no_default, &ev3), None, "无默认且未命中 → None");
}

// ===========================================================================
// executor/close_exec.rs — close 收口执行
// ===========================================================================

fn sample_close(close_mode: CloseMode, event_ok: bool, close_ok: bool) -> CloseOutput {
    CloseOutput {
        rule_name: "r_close".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok,
        close_ok,
        close_mode,
        event_emitted: false,
        event_step_data: vec![step_data(Some("fail"), 3.0, vec![])],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 1_000,
        machine_id: String::new(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 1_000,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 1_000,
        window_start_time_nanos: 0,
        window_end_time_nanos: 1_000,
        last_event_nanos: 1_000,
        row_fields: None,
        row_field_names: None,
    }
}

#[test]
fn execute_close_unqualified_returns_none() {
    let exec = RuleExecutor::new(simple_rule_plan(
        "r_close",
        simple_plan(vec![simple_key("sip")], vec![]),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    ));
    // Or mode with no close steps → never qualifies (event path owns output).
    let or_no_close = sample_close(CloseMode::Or, true, true);
    assert!(exec.execute_close(&or_no_close).unwrap().is_none());
    // And mode with event_ok=false → suppressed.
    let and_missing_event = sample_close(CloseMode::And, false, true);
    assert!(exec.execute_close(&and_missing_event).unwrap().is_none());
    // And mode with close_ok=false → suppressed.
    let and_missing_close = sample_close(CloseMode::And, true, false);
    assert!(exec.execute_close(&and_missing_close).unwrap().is_none());
}

#[test]
fn execute_close_qualified_builds_alert() {
    let exec = RuleExecutor::new(simple_rule_plan(
        "r_close",
        simple_plan(vec![simple_key("sip")], vec![]),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    ));
    let close = sample_close(CloseMode::And, true, true);
    let alert = exec
        .execute_close(&close)
        .unwrap()
        .expect("qualified close fires");
    assert_eq!(&*alert.rule_name, "r_close");
    assert_eq!(alert.entity_id, "10.0.0.1");
    assert!((alert.score - 70.0).abs() < f64::EPSILON);
    assert_eq!(alert.origin.as_str(), "close:timeout");
    assert!(alert.summary.contains("fail=3.0"));
}

#[test]
fn execute_close_with_joins_suppressed_on_inner_miss() {
    let mut plan = simple_rule_plan(
        "r_close_join",
        simple_plan(vec![simple_key("sip")], vec![]),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.joins = vec![join_plan(JoinMode::Inner, "geo", "sip", "ip")];
    let exec = RuleExecutor::new(plan);
    let close = sample_close(CloseMode::And, true, true);

    // Join miss → close output suppressed (D4 miss → drop).
    let lookup = JoinLookup::new();
    assert!(
        exec.execute_close_with_joins(&close, &lookup)
            .unwrap()
            .is_none()
    );

    // Join hit → enriched close alert, and where passes.
    let mut lookup = JoinLookup::new();
    lookup.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.1")), ("country", str_val("US"))],
    );
    let alert = exec
        .execute_close_with_joins(&close, &lookup)
        .unwrap()
        .unwrap();
    assert_eq!(alert.entity_id, "10.0.0.1");

    // Post-join `where` suppresses when it fails.
    let mut plan2 = simple_rule_plan(
        "r_close_where",
        simple_plan(vec![simple_key("sip")], vec![]),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan2.joins = vec![join_plan(JoinMode::Inner, "geo", "sip", "ip")];
    plan2.r#where = Some(eq_str_expr("country", "US"));
    let exec2 = RuleExecutor::new(plan2);
    let mut lookup_bad = JoinLookup::new();
    lookup_bad.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.1")), ("country", str_val("DE"))],
    );
    assert!(
        exec2
            .execute_close_with_joins(&close, &lookup_bad)
            .unwrap()
            .is_none()
    );
    let mut lookup_good = JoinLookup::new();
    lookup_good.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.1")), ("country", str_val("US"))],
    );
    assert!(
        exec2
            .execute_close_with_joins(&close, &lookup_good)
            .unwrap()
            .is_some()
    );
}

#[test]
fn close_plan_columnar_safe_gate_variants() {
    let base = || {
        simple_rule_plan(
            "r_safe",
            simple_plan(vec![simple_key("sip")], vec![]),
            Expr::Number(70.0),
            "ip",
            Expr::Field(FieldRef::Simple("sip".to_string())),
        )
    };
    assert!(RuleExecutor::new(base()).close_plan_columnar_safe());

    // Non-constant score → unsafe.
    let mut p = base();
    p.score_plan.expr = Expr::Field(FieldRef::Simple("sip".into()));
    assert!(!RuleExecutor::new(p).close_plan_columnar_safe());

    // Entity path ref → unsafe.
    let mut p = base();
    p.entity_plan.entity_id_expr = Expr::Field(FieldRef::Path {
        alias: "e".into(),
        segments: vec![PathSegment::Field("roles_obj".into())],
    });
    assert!(!RuleExecutor::new(p).close_plan_columnar_safe());

    // Entity field with a synthetic `_` prefix → unsafe.
    let mut p = base();
    p.entity_plan.entity_id_expr = Expr::Field(FieldRef::Simple("_step_0_measure".into()));
    assert!(!RuleExecutor::new(p).close_plan_columnar_safe());

    // Yield with a general expression referencing plain fields → safe
    // （2026-08-25 扩展: 列式 close 对 General 走轻量 ctx 求值）。
    let mut p = base();
    p.yield_plan.fields = vec![YieldField {
        name: "f".into(),
        value: Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
            right: Box::new(Expr::Number(2.0)),
        },
    }];
    assert!(RuleExecutor::new(p).close_plan_columnar_safe());
    // General referencing a synthetic `_step_*` field → unsafe（Named 窄化不注入）。
    let mut p = base();
    p.yield_plan.fields = vec![YieldField {
        name: "f".into(),
        value: Expr::Field(FieldRef::Simple("_step_0_measure".into())),
    }];
    assert!(!RuleExecutor::new(p).close_plan_columnar_safe());

    // Joins present → unsafe.
    let mut p = base();
    p.joins = vec![join_plan(JoinMode::Snapshot, "geo", "sip", "ip")];
    assert!(!RuleExecutor::new(p).close_plan_columnar_safe());

    // Literal yields + StringLit entity → safe.
    let mut p = base();
    p.entity_plan.entity_id_expr = Expr::StringLit("fixed-entity".into());
    p.yield_plan.fields = vec![
        YieldField {
            name: "n".into(),
            value: Expr::Number(1.0),
        },
        YieldField {
            name: "s".into(),
            value: Expr::StringLit("x".into()),
        },
        YieldField {
            name: "b".into(),
            value: Expr::Bool(true),
        },
        YieldField {
            name: "f".into(),
            value: Expr::Field(FieldRef::Simple("sip".into())),
        },
    ];
    assert!(RuleExecutor::new(p).close_plan_columnar_safe());
}

// ===========================================================================
// executor/each_exec.rs — on-each 执行
// ===========================================================================

#[test]
fn execute_each_requires_each_plan() {
    let plain = RuleExecutor::new(simple_rule_plan(
        "r_plain",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    ));
    assert!(plain.execute_each(&event(vec![]), 0).is_err());
    assert!(
        plain
            .execute_each_with_joins(&event(vec![]), 0, &JoinLookup::new(), &[], 0)
            .is_err()
    );
}

#[test]
fn execute_each_filter_and_lets_and_where() {
    // Filter rejects non-matching events.
    let mut plan = simple_rule_plan(
        "r_each_f",
        simple_plan(vec![], vec![]),
        Expr::Number(42.5),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(eq_str_expr("sip", "10.0.0.1")),
    });
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.execute_each(&event(vec![("sip", str_val("10.0.0.2"))]), 0)
            .unwrap()
            .is_none()
    );
    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 0)
        .unwrap()
        .unwrap();
    assert!((alert.score - 42.5).abs() < f64::EPSILON);
    assert_eq!(alert.entity_id, "10.0.0.1");

    // `let` bindings inject computed values into the eval context.
    let mut plan = simple_rule_plan(
        "r_each_let",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.lets = vec![LetPlan {
        name: "double".into(),
        expr: Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Field(FieldRef::Simple("x".into()))),
            right: Box::new(Expr::Number(2.0)),
        },
    }];
    plan.yield_plan.fields = vec![YieldField {
        name: "double".into(),
        value: Expr::Field(FieldRef::Simple("double".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("double".into(), FieldType::Base(BaseType::Float))]),
    );
    let alert = exec
        .execute_each(&event(vec![("x", num(21.0))]), 0)
        .unwrap()
        .unwrap();
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| &**n == "double")
            .map(|(_, v)| v.clone()),
        Some(Value::Number(42.0))
    );

    // `where` after the ctx path: with a `let` present the where is evaluated.
    let mut plan = simple_rule_plan(
        "r_each_where",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.lets = vec![LetPlan {
        name: "k".into(),
        expr: Expr::Number(1.0),
    }];
    plan.r#where = Some(eq_str_expr("sip", "10.0.0.1"));
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.execute_each_with_joins(
            &event(vec![("sip", str_val("10.0.0.2"))]),
            0,
            &JoinLookup::new(),
            &[],
            0
        )
        .unwrap()
        .is_none()
    );
    assert!(
        exec.execute_each_with_joins(
            &event(vec![("sip", str_val("10.0.0.1"))]),
            0,
            &JoinLookup::new(),
            &[],
            0
        )
        .unwrap()
        .is_some()
    );
}

// ===========================================================================
// match_engine/conv.rs — apply_conv pipelines
// ===========================================================================

#[test]
fn apply_conv_sort_top_dedup_where_pipelines() {
    let keys = vec![simple_key("sip")];
    fn out(sip: &str, score: f64) -> CloseOutput {
        let mut c = sample_close(CloseMode::And, true, true);
        c.scope_key = vec![str_val(sip)];
        // Encode the score through the step label so conv exprs can read it.
        c.event_step_data = vec![step_data(Some("score"), score, vec![])];
        c
    }
    let a = out("a", 3.0);
    let b = out("b", 1.0);
    let c = out("c", 2.0);
    let dup = out("a", 3.0);

    let score_expr = || Expr::Field(FieldRef::Simple("score".into()));
    // sort(score desc) | top(2) | dedup(score) | where(score >= 2)
    let plan = wf_lang::plan::ConvPlan {
        chains: vec![ConvChainPlan {
            ops: vec![
                ConvOpPlan::Sort(vec![SortKeyPlan {
                    expr: score_expr(),
                    descending: true,
                }]),
                ConvOpPlan::Top(2),
                ConvOpPlan::Dedup(score_expr()),
                ConvOpPlan::Where(Expr::BinOp {
                    op: BinOp::Ge,
                    left: Box::new(score_expr()),
                    right: Box::new(Expr::Number(2.0)),
                }),
            ],
        }],
    };
    let out = apply_conv(&plan, &keys, vec![a, b, c, dup]);
    // sort desc → [3,3,2,1]; top 2 → [3,3]; dedup → [3]; where ≥2 keeps 3.
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].scope_key, vec![str_val("a")]);
    assert_eq!(out[0].event_step_data[0].label.as_deref(), Some("score"));
    assert_eq!(out[0].event_step_data[0].measure_value, 3.0);
}

// ===========================================================================
// contract.rs — inline test harness failure branches
// ===========================================================================

fn auth_events_schema() -> wf_lang::WindowSchema {
    wf_lang::WindowSchema {
        name: "auth_events".to_string(),
        streams: vec!["auth_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            wf_lang::FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            wf_lang::FieldDef {
                name: "action".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            wf_lang::FieldDef {
                name: "count".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            wf_lang::FieldDef {
                name: "event_time".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    }
}

fn security_alerts_schema() -> wf_lang::WindowSchema {
    wf_lang::WindowSchema {
        name: "security_alerts".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![
            wf_lang::FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            wf_lang::FieldDef {
                name: "fail_count".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
        ],
    }
}

fn run_contract_from_source(source: &str) -> crate::match_engine::contract::TestResult {
    run_contract_from_source_with_schemas(
        source,
        vec![auth_events_schema(), security_alerts_schema()],
    )
}

fn run_contract_from_source_with_schemas(
    source: &str,
    schemas: Vec<wf_lang::WindowSchema>,
) -> crate::match_engine::contract::TestResult {
    let wfl_file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plans = wf_lang::compile_wfl(&wfl_file, &schemas).expect("compile should succeed");
    let test = &wfl_file.tests[0];
    let plan = plans
        .iter()
        .find(|p| p.name == test.rule_name)
        .unwrap_or_else(|| panic!("rule `{}` not found", test.rule_name));
    let time_field = schemas
        .iter()
        .find(|s| plan.binds.iter().any(|b| b.window == s.name))
        .and_then(|s| s.time_field.clone());
    crate::match_engine::contract::run_test(test, plan, time_field).expect("run_test succeeds")
}

#[test]
fn contract_hits_mismatch_records_failure() {
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 5)
}

test below_threshold for brute_force {
    input {
        row(e, sip = "10.0.0.1", action = "failed");
        row(e, sip = "10.0.0.1", action = "failed");
        row(e, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits == 1;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(!result.passed);
    assert!(!result.failures.is_empty());
    assert!(
        result
            .failures
            .iter()
            .any(|f| f.contains("hits") && f.contains("expected")),
        "failures: {:?}",
        result.failures
    );
    assert_eq!(result.output_count, 0);
}

#[test]
fn contract_hit_assert_out_of_range_records_failure() {
    let source = r#"
rule brute_force {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test single_hit for brute_force {
    input {
        row(e, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits == 1;
        hit[5].score >= 70;
    }
}
"#;
    let result = run_contract_from_source(source);
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|f| f.contains("hit[5]") && f.contains("index out of range")),
        "failures: {:?}",
        result.failures
    );
    assert_eq!(result.output_count, 1);
}

#[test]
fn contract_rejects_where_referencing_joined_window() {
    // The `where` references a field of the joined window (`geo_lookup.region`)
    // — the inline harness cannot populate joined windows, so the contract
    // harness must reject the rule loudly instead of passing vacuously.
    let geo_lookup = wf_lang::WindowSchema {
        name: "geo_lookup".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![
            wf_lang::FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            wf_lang::FieldDef {
                name: "region".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
        ],
    };
    let source = r#"
rule enriched {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    join geo_lookup snapshot on e.sip == geo_lookup.sip
    where geo_lookup.region == "US"
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 1)
}

test joined_where for enriched {
    input {
        row(e, sip = "10.0.0.1", action = "failed");
    }
    expect {
        hits == 1;
    }
}
"#;
    let result = run_contract_from_source_with_schemas(
        source,
        vec![auth_events_schema(), geo_lookup, security_alerts_schema()],
    );
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|f| f.contains("references joined window")),
        "failures: {:?}",
        result.failures
    );
}
