//! Round-4 coverage tests for `alert/types.rs`: the `to_data_record` /
//! `data_record_to_json_string` lanes the earlier suites still miss —
//! `model_value_to_json` over Chars / Obj / Array / Time / Ip / Hex values,
//! the structured-render rejection of non-finite numbers nested in objects,
//! and the naive (`%Y-%m-%dT%H:%M:%S` and `%Y-%m-%d %H:%M:%S%.f`) time-text
//! parse lanes.
//!
//! Only test code lives here — no production logic is modified.

use std::sync::Arc;

use wf_lang::{BaseType, FieldType};
use wp_model_core::model::{DataRecord, DataType, Field, FieldStorage, Value as ModelValue};

use crate::alert::data_record_to_json_string;
use crate::alert::types::{
    AlertOrigin, OutputRecord, WFU_CLOSE_REASON, WFU_ORIGIN, export_yield_value,
};
use crate::match_engine::{CloseReason, EngineHashMap, Value};

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn str_val(s: &str) -> Value {
    Value::Str(s.to_string().into())
}

fn sample_output(origin: AlertOrigin, yield_fields: Vec<(Arc<str>, Value)>) -> OutputRecord {
    OutputRecord {
        wfx_id: "id-r4".into(),
        rule_name: Arc::from("r4_rule"),
        score: 42.0,
        entity_type: Arc::from("ip"),
        entity_id: "10.0.0.1".into(),
        origin,
        fired_at: "2026-08-23T00:00:00Z".into(),
        emit_time: Arc::from("2026-08-23T00:00:01Z"),
        matched_rows: Vec::new(),
        summary: Arc::from("r4"),
        yield_target: Arc::from("out"),
        yield_fields,
        yield_field_types: Arc::from([]),
        event_time_nanos: 0,
        machine_id: String::new(),
        scope_key: Arc::from(""),
    }
}

/// `model_value_to_json` lanes: Chars / Obj / Array / Time / Ip / Hex values
/// serialized through `data_record_to_json_string` (the Time / Ip / Hex values
/// reach the `other` arm via the record produced by `to_data_record`).
#[test]
fn data_record_json_chars_object_array_and_time_ip_hex_lanes() {
    // Chars / Obj / Array built directly on a DataRecord.
    let mut record = DataRecord::default();
    record.push(FieldStorage::from_owned(Field::new(
        DataType::Chars,
        "name",
        ModelValue::from("alice"),
    )));
    let mut obj = wp_model_core::model::types::value::ObjectValue::new();
    obj.insert(
        "k",
        FieldStorage::from_owned(Field::new(DataType::Digit, "k", ModelValue::from(3_i64))),
    );
    record.push(FieldStorage::from_owned(Field::new(
        DataType::Obj,
        "nested",
        ModelValue::Obj(obj),
    )));
    record.push(FieldStorage::from_owned(Field::new(
        DataType::Array("chars".to_string()),
        "tags",
        ModelValue::Array(vec![
            FieldStorage::from_owned(Field::new(DataType::Chars, "item", ModelValue::from("a"))),
            FieldStorage::from_owned(Field::new(DataType::Chars, "item", ModelValue::from("b"))),
        ]),
    )));
    let json = data_record_to_json_string(&record).unwrap();
    let json: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(json["name"], "alice");
    assert_eq!(json["nested"]["k"], 3);
    assert_eq!(json["tags"], serde_json::json!(["a", "b"]));

    // Time / Ip / Hex values via to_data_record → model_value_to_json's
    // `other` arm (they are not JSON primitives).
    let output = sample_output(
        AlertOrigin::Close {
            reason: CloseReason::Eos,
        },
        vec![
            ("seen_at".into(), num(1_710_115_200_000_000_000.0)),
            ("src_ip".into(), str_val("192.168.0.1")),
            ("sha".into(), str_val("0xAB")),
        ],
    );
    let mut output = output;
    output.yield_field_types = Arc::from(vec![
        (Arc::from("seen_at"), FieldType::Base(BaseType::Time)),
        (Arc::from("src_ip"), FieldType::Base(BaseType::Ip)),
        (Arc::from("sha"), FieldType::Base(BaseType::Hex)),
    ]);
    let record = output.to_data_record().expect("record");
    let json = data_record_to_json_string(&record).unwrap();
    let json: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(json["__wfu_origin"], "close:eos");
    assert_eq!(json["__wfu_close_reason"], "eos");
    assert!(json["seen_at"].is_string(), "{json}");
    assert!(json["src_ip"].is_string(), "{json}");
    assert!(json["sha"].is_string(), "{json}");
}

/// Structured values (object / array) nested inside an object export are
/// rendered through `rule_value_to_json`; a non-finite number anywhere in the
/// tree is rejected.
#[test]
fn structured_render_rejects_non_finite_number_nested_in_object() {
    let mut obj = EngineHashMap::default();
    obj.insert("score".into(), Value::Number(f64::NAN));
    let output = sample_output(AlertOrigin::Event, vec![("ctx".into(), Value::Object(obj))]);
    let mut output = output;
    output.yield_field_types =
        Arc::from(vec![(Arc::from("ctx"), FieldType::Base(BaseType::Chars))]);
    let err = output
        .to_data_record()
        .expect_err("non-finite number inside a structured Chars render must fail");
    assert!(
        err.to_string()
            .contains("structured numeric value must be finite"),
        "err: {err}"
    );

    // Same lane through an array.
    let output = sample_output(
        AlertOrigin::Event,
        vec![(
            "items".into(),
            Value::Array(vec![num(1.0), Value::Number(f64::INFINITY)]),
        )],
    );
    let mut output = output;
    output.yield_field_types =
        Arc::from(vec![(Arc::from("items"), FieldType::Base(BaseType::Chars))]);
    assert!(output.to_data_record().is_err());
}

/// `parse_time_text` naive lanes: `%Y-%m-%dT%H:%M:%S` (T-form without
/// fraction) and `%Y-%m-%d %H:%M:%S%.f` (space-form with fraction).
#[test]
fn time_text_naive_t_form_and_space_fraction_lanes() {
    let (meta, _) = export_yield_value(
        &str_val("2024-03-11T08:30:00"),
        Some(&FieldType::Base(BaseType::Time)),
    )
    .unwrap();
    assert_eq!(meta, DataType::Time);

    let (meta, _) = export_yield_value(
        &str_val("2024-03-11 08:30:00.123456"),
        Some(&FieldType::Base(BaseType::Time)),
    )
    .unwrap();
    assert_eq!(meta, DataType::Time);
}

/// `to_data_record` with an `AlertOrigin::Deferred` record (the join-family
/// P3 origin) and a fully untyped yield field — exercises the origin string
/// lane `"deferred"` end-to-end.
#[test]
fn to_data_record_deferred_origin_untyped_fields() {
    let output = sample_output(
        AlertOrigin::Deferred,
        vec![("msg".into(), str_val("hello")), ("n".into(), num(7.0))],
    );
    let record = output.to_data_record().expect("record");
    assert_eq!(
        record.get_value(WFU_ORIGIN),
        Some(&ModelValue::from("deferred"))
    );
    assert_eq!(
        record.get_value(WFU_CLOSE_REASON),
        Some(&ModelValue::from(""))
    );
    assert_eq!(record.get_value("msg"), Some(&ModelValue::from("hello")));
    // untyped 数值恒导出 Float（export_untyped_value：Number → DataType::Float）。
    assert_eq!(record.get_value("n"), Some(&ModelValue::from(7.0_f64)));
}
