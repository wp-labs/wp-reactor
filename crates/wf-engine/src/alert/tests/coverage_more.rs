//! Round-2 coverage-fill tests for `alert/types.rs` — the export lanes the
//! in-module `tests` and `alert/tests/coverage_extra.rs` do not reach:
//!
//! - untyped (`field_type == None`) export lanes: Str → Chars, Bool → Bool,
//!   Array → `Array("auto")`, Object → Obj;
//! - typed `Chars` targets fed non-string values (`render_value_as_string`:
//!   Number / Bool, Array / Object → structured JSON string);
//! - typed `Array(base)` target fed a non-array value → error;
//! - `rule_value_to_json` Bool / Array lanes via structured string rendering;
//! - `model_value_to_json` Bool lane in `data_record_to_json_string`;
//! - `OutputRecord`'s derived `serde::Serialize` (skipped fields stay out);
//! - `export_yield_f64` fallbacks to Ip / Hex targets (error lanes);
//! - `to_data_record` with untyped yield values.
use std::sync::Arc;

use wf_lang::{BaseType, FieldType};
use wp_model_core::model::{DataRecord, DataType, Field, FieldStorage, Value as ModelValue};

use crate::alert::data_record_to_json_string;
use crate::alert::types::{AlertOrigin, OutputRecord, export_yield_f64, export_yield_value};
use crate::match_engine::{CloseReason, EngineHashMap, Value};

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn str_val(s: &str) -> Value {
    Value::Str(s.to_string().into())
}

#[test]
fn untyped_export_lanes() {
    // Untyped Str → Chars.
    let (meta, value) = export_yield_value(&str_val("hi"), None).unwrap();
    assert_eq!(meta, DataType::Chars);
    assert_eq!(value, ModelValue::from("hi"));
    // Untyped Bool → Bool.
    let (meta, value) = export_yield_value(&Value::Bool(false), None).unwrap();
    assert_eq!(meta, DataType::Bool);
    assert_eq!(value, ModelValue::from(false));
    // Untyped Array → Array("auto") with per-item auto typing.
    let (meta, value) = export_yield_value(
        &Value::Array(vec![num(1.0), str_val("x"), Value::Bool(true)]),
        None,
    )
    .unwrap();
    assert_eq!(meta, DataType::Array("auto".to_string()));
    let ModelValue::Array(items) = value else {
        panic!("expected array");
    };
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].get_meta(), &DataType::Digit);
    assert_eq!(items[1].get_meta(), &DataType::Chars);
    assert_eq!(items[2].get_meta(), &DataType::Bool);
    // Untyped Object → Obj.
    let mut obj = EngineHashMap::default();
    obj.insert("k".into(), num(1.0));
    let (meta, _) = export_yield_value(&Value::Object(obj), None).unwrap();
    assert_eq!(meta, DataType::Obj);
}

#[test]
fn chars_target_renders_non_string_values() {
    // Number → Display form.
    let (meta, value) =
        export_yield_value(&num(1.5), Some(&FieldType::Base(BaseType::Chars))).unwrap();
    assert_eq!(meta, DataType::Chars);
    assert_eq!(value, ModelValue::from("1.5"));
    // Bool → "true"/"false".
    let (meta, value) =
        export_yield_value(&Value::Bool(true), Some(&FieldType::Base(BaseType::Chars))).unwrap();
    assert_eq!(meta, DataType::Chars);
    assert_eq!(value, ModelValue::from("true"));
    // Array → structured JSON string.
    let (meta, value) = export_yield_value(
        &Value::Array(vec![num(1.0), str_val("b")]),
        Some(&FieldType::Base(BaseType::Chars)),
    )
    .unwrap();
    assert_eq!(meta, DataType::Chars);
    assert_eq!(value, ModelValue::from(r#"[1.0,"b"]"#));
    // Object → structured JSON string with sorted keys.
    let mut obj = EngineHashMap::default();
    obj.insert("z".into(), str_val("last"));
    obj.insert("a".into(), num(1.0));
    obj.insert("flag".into(), Value::Bool(true));
    let (meta, value) =
        export_yield_value(&Value::Object(obj), Some(&FieldType::Base(BaseType::Chars))).unwrap();
    assert_eq!(meta, DataType::Chars);
    assert_eq!(
        value,
        ModelValue::from(r#"{"a":1.0,"flag":true,"z":"last"}"#)
    );
}

#[test]
fn typed_array_target_rejects_non_array_value() {
    let err = export_yield_value(&num(1.0), Some(&FieldType::Array(BaseType::Chars)))
        .expect_err("typed array target requires an array");
    assert!(err.to_string().contains("array export expects"));
}

#[test]
fn export_yield_f64_ip_and_hex_fallbacks_error() {
    // Ip target from a number falls back to the Value path → error.
    let err = export_yield_f64(1.0, Some(&FieldType::Base(BaseType::Ip)))
        .expect_err("ip from a number must fail");
    assert!(err.to_string().contains("ip field requires"));
    // Hex target from a finite number is valid through the Value path.
    let (meta, _) = export_yield_f64(255.0, Some(&FieldType::Base(BaseType::Hex))).unwrap();
    assert_eq!(meta, DataType::Hex);
    // Hex target from a fractional number → error.
    assert!(export_yield_f64(1.5, Some(&FieldType::Base(BaseType::Hex))).is_err());
}

#[test]
fn data_record_json_bool_lane() {
    let mut record = DataRecord::default();
    record.push(FieldStorage::from_owned(Field::new(
        DataType::Bool,
        "active",
        ModelValue::from(true),
    )));
    record.push(FieldStorage::from_owned(Field::new(
        DataType::Float,
        "score",
        ModelValue::from(0.5),
    )));
    let json = data_record_to_json_string(&record).unwrap();
    let json: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(json["active"], true);
    assert_eq!(json["score"], 0.5);
}

#[test]
fn output_record_derive_serialize_skips_internal_fields() {
    let record = OutputRecord {
        wfx_id: "id-9".into(),
        rule_name: Arc::from("rule-b"),
        score: 3.0,
        entity_type: Arc::from("ip"),
        entity_id: "1.1.1.1".into(),
        origin: AlertOrigin::Close {
            reason: CloseReason::Flush,
        },
        fired_at: "2026-03-11T00:00:00.000Z".into(),
        emit_time: Arc::from("2026-03-11T00:00:01.000Z"),
        matched_rows: vec![],
        summary: Arc::from("sum"),
        yield_target: Arc::from("out"),
        yield_fields: vec![("k".into(), num(1.0))],
        yield_field_types: Arc::from([]),
        event_time_nanos: 0,
        machine_id: "m1".into(),
        scope_key: Arc::from("a=1"),
    };
    let json = serde_json::to_value(&record).unwrap();
    // Serde-serialized fields.
    assert_eq!(json["wfx_id"], "id-9");
    assert_eq!(json["rule_name"], "rule-b");
    assert_eq!(json["score"], 3.0);
    assert_eq!(json["origin"], "close:flush");
    assert_eq!(json["entity_id"], "1.1.1.1");
    assert_eq!(json["summary"], "sum");
    // #[serde(skip)] fields must not appear.
    assert!(json.get("matched_rows").is_none());
    assert!(json.get("yield_target").is_none());
    assert!(json.get("yield_fields").is_none());
    assert!(json.get("event_time_nanos").is_none());
    assert!(json.get("machine_id").is_none());
    assert!(json.get("scope_key").is_none());
}

#[test]
fn to_data_record_untyped_yield_lanes() {
    let output = OutputRecord {
        wfx_id: "id-1".into(),
        rule_name: "rule-a".into(),
        score: 1.0,
        entity_type: "ip".into(),
        entity_id: "1.1.1.1".into(),
        origin: AlertOrigin::Event,
        fired_at: "2026-03-11T00:00:00.000Z".into(),
        emit_time: "2026-03-11T00:00:01.000Z".into(),
        matched_rows: vec![],
        summary: "demo".into(),
        yield_target: "out".into(),
        yield_fields: vec![
            ("s".into(), str_val("x")),
            ("b".into(), Value::Bool(true)),
            ("arr".into(), Value::Array(vec![num(1.0)])),
            (
                "o".into(),
                Value::Object(EngineHashMap::from_iter([("k".into(), num(2.0))])),
            ),
        ],
        yield_field_types: Arc::from([]),
        event_time_nanos: 0,
        machine_id: Arc::from(""),
        scope_key: "".into(),
    };
    let record = output.to_data_record().expect("record");
    assert_eq!(record.field("s").unwrap().get_meta(), &DataType::Chars);
    assert_eq!(record.field("b").unwrap().get_meta(), &DataType::Bool);
    assert_eq!(
        record.field("arr").unwrap().get_meta(),
        &DataType::Array("auto".to_string())
    );
    assert_eq!(record.field("o").unwrap().get_meta(), &DataType::Obj);
    // Object member of an untyped object is auto-typed: 2.0 → Digit.
    let ModelValue::Obj(object) = record.field("o").unwrap().get_value() else {
        panic!("expected object");
    };
    assert_eq!(object.get("k").unwrap().get_meta(), &DataType::Digit);
}
