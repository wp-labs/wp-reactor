//! Coverage extras for the alert layer: `AlertOrigin` serde round-trips,
//! typed/untyped yield export error branches (Time / Ip / Hex / Bool / digit /
//! float / non-finite), `model_value_to_json` null/digit lanes, and the
//! columnar batch paths the in-module suites skip (`commit_close_rows_batch`,
//! `stage_yield_cell_f64`, `register_yield_column` errors, `reserve_rows`,
//! `take_staged`).
//!
//! Only test code lives here — no production logic is modified.
use std::sync::Arc;


use chrono::Timelike;
use wf_lang::{BaseType, FieldType};
use wp_model_core::model::{DataRecord, DataType, Field, FieldStorage, Value as ModelValue};

use crate::alert::column_batch::{AlertColumnBuilder, EachRowCells};
use crate::alert::types::{AlertOrigin, OutputRecord, export_yield_f64, export_yield_value};
use crate::alert::{WFU_PREFIX, data_record_to_json_string};
use crate::match_engine::{CloseReason, EngineHashMap, Value};

// ===========================================================================
// types.rs — AlertOrigin serde / display
// ===========================================================================

#[test]
fn alert_origin_serde_roundtrip_and_display() {
    let variants = [
        (AlertOrigin::Event, "event"),
        (
            AlertOrigin::Close {
                reason: CloseReason::Timeout,
            },
            "close:timeout",
        ),
        (
            AlertOrigin::Close {
                reason: CloseReason::Flush,
            },
            "close:flush",
        ),
        (
            AlertOrigin::Close {
                reason: CloseReason::Eos,
            },
            "close:eos",
        ),
        (AlertOrigin::Deferred, "deferred"),
    ];
    for (origin, text) in variants {
        assert_eq!(origin.as_str(), text);
        assert_eq!(origin.to_string(), text);
        // Serialize → string; deserialize → equal origin.
        let json = serde_json::to_string(&origin).unwrap();
        assert_eq!(json, format!("\"{text}\""));
        let back: AlertOrigin = serde_json::from_str(&json).unwrap();
        assert_eq!(back, origin);
    }
    // Unknown string → deserialization error.
    assert!(serde_json::from_str::<AlertOrigin>("\"bogus\"").is_err());

    // close_reason(): Close → Some(reason), Event/Deferred → None.
    assert_eq!(
        AlertOrigin::Close {
            reason: CloseReason::Eos
        }
        .close_reason(),
        Some(CloseReason::Eos)
    );
    assert_eq!(AlertOrigin::Event.close_reason(), None);
    assert_eq!(AlertOrigin::Deferred.close_reason(), None);
}

// ===========================================================================
// types.rs — export_yield_f64 fast lanes
// ===========================================================================

#[test]
fn export_yield_f64_fast_lanes_and_fallbacks() {
    use wp_model_core::model::DataType;
    // Digit: integer-valued finite → i64 digit.
    let (meta, value) = export_yield_f64(42.0, Some(&FieldType::Base(BaseType::Digit))).unwrap();
    assert_eq!(meta, DataType::Digit);
    assert_eq!(value, ModelValue::from(42_i64));
    // Float: finite → f64 float.
    let (meta, value) = export_yield_f64(1.5, Some(&FieldType::Base(BaseType::Float))).unwrap();
    assert_eq!(meta, DataType::Float);
    assert_eq!(value, ModelValue::from(1.5));
    // Chars: renders the f64 Display form.
    let (meta, value) = export_yield_f64(1.5, Some(&FieldType::Base(BaseType::Chars))).unwrap();
    assert_eq!(meta, DataType::Chars);
    assert_eq!(value, ModelValue::from("1.5"));
    // Untyped: finite → float.
    let (meta, value) = export_yield_f64(2.0, None).unwrap();
    assert_eq!(meta, DataType::Float);
    assert_eq!(value, ModelValue::from(2.0));
    // Fractional digit → falls back to the Value path → error.
    assert!(export_yield_f64(1.5, Some(&FieldType::Base(BaseType::Digit))).is_err());
    // Non-finite float → falls back → error.
    assert!(export_yield_f64(f64::NAN, Some(&FieldType::Base(BaseType::Float))).is_err());
    assert!(export_yield_f64(f64::INFINITY, None).is_err());
    // Time target from a number → Value path parses epoch nanos.
    let (meta, _) = export_yield_f64(
        1_710_115_200_000_000_000.0,
        Some(&FieldType::Base(BaseType::Time)),
    )
    .unwrap();
    assert_eq!(meta, DataType::Time);
}

// ===========================================================================
// types.rs — typed export errors + untyped lanes
// ===========================================================================

#[test]
fn export_typed_bool_success_and_typed_errors() {
    // Bool success.
    let (meta, value) =
        export_yield_value(&Value::Bool(true), Some(&FieldType::Base(BaseType::Bool))).unwrap();
    assert_eq!(meta, DataType::Bool);
    assert_eq!(value, ModelValue::from(true));
    // Bool target with a number → error.
    let err = export_yield_value(&Value::Number(1.0), Some(&FieldType::Base(BaseType::Bool)))
        .expect_err("bool requires a boolean");
    assert!(err.to_string().contains("bool field requires"));
    // Digit target with a fractional number → error.
    let err = export_yield_value(&Value::Number(1.5), Some(&FieldType::Base(BaseType::Digit)))
        .expect_err("digit requires integer");
    assert!(err.to_string().contains("digit field requires"));
    // Digit target with a non-number → error.
    assert!(
        export_yield_value(
            &Value::Str("x".into()),
            Some(&FieldType::Base(BaseType::Digit)),
        )
        .is_err()
    );
    // Float target with NaN → error.
    let err = export_yield_value(
        &Value::Number(f64::NAN),
        Some(&FieldType::Base(BaseType::Float)),
    )
    .expect_err("float requires finite");
    assert!(err.to_string().contains("float field requires"));
    // Untyped non-finite number → error.
    let err = export_yield_value(&Value::Number(f64::NAN), None)
        .expect_err("untyped non-finite rejected");
    assert!(err.to_string().contains("unsupported untyped yield value"));
    // Array-any target with a non-array → error.
    let err = export_yield_value(&Value::Number(1.0), Some(&FieldType::ArrayAny))
        .expect_err("array export requires an array");
    assert!(err.to_string().contains("array export expects"));
    // Object target with a non-object → error.
    let err = export_yield_value(&Value::Number(1.0), Some(&FieldType::Object))
        .expect_err("object export requires an object");
    assert!(err.to_string().contains("object export expects"));
}

#[test]
fn export_time_ip_hex_via_string_and_number_lanes() {
    // Time from RFC3339 text with offset → naive UTC.
    let (meta, value) = export_yield_value(
        &Value::Str("2024-03-11T08:00:00+08:00".into()),
        Some(&FieldType::Base(BaseType::Time)),
    )
    .unwrap();
    assert_eq!(meta, DataType::Time);
    // Value is a DateTimeValue; sanity: hour should be 00 UTC.
    let dt = match value {
        ModelValue::Time(dt) => dt,
        other => panic!("expected time value, got {other:?}"),
    };
    assert_eq!(dt.hour(), 0);
    // Time from naive text "%Y-%m-%d %H:%M:%S".
    let (meta, _) = export_yield_value(
        &Value::Str("2024-03-11 08:30:00".into()),
        Some(&FieldType::Base(BaseType::Time)),
    )
    .unwrap();
    assert_eq!(meta, DataType::Time);
    // Time from "%Y-%m-%dT%H:%M:%S%.f" (fractional).
    let (meta, _) = export_yield_value(
        &Value::Str("2024-03-11T08:30:00.123".into()),
        Some(&FieldType::Base(BaseType::Time)),
    )
    .unwrap();
    assert_eq!(meta, DataType::Time);
    // Invalid time text → error.
    let err = export_yield_value(
        &Value::Str("not-a-time".into()),
        Some(&FieldType::Base(BaseType::Time)),
    )
    .expect_err("invalid time text rejected");
    assert!(err.to_string().contains("invalid time literal"));
    // Time from a non-numeric, non-string value → error.
    assert!(
        export_yield_value(&Value::Bool(true), Some(&FieldType::Base(BaseType::Time)),).is_err()
    );

    // Ip from a valid string; invalid string → error; non-string → error.
    let (meta, _) = export_yield_value(
        &Value::Str("192.168.0.1".into()),
        Some(&FieldType::Base(BaseType::Ip)),
    )
    .unwrap();
    assert_eq!(meta, DataType::IP);
    assert!(
        export_yield_value(
            &Value::Str("999.1.1.1".into()),
            Some(&FieldType::Base(BaseType::Ip)),
        )
        .is_err()
    );
    assert!(
        export_yield_value(&Value::Number(1.0), Some(&FieldType::Base(BaseType::Ip)),).is_err()
    );

    // Hex: number lane, "0x"/"0X" prefix lanes, invalid lanes.
    let (meta, value) =
        export_yield_value(&Value::Number(255.0), Some(&FieldType::Base(BaseType::Hex))).unwrap();
    assert_eq!(meta, DataType::Hex);
    assert_eq!(value, ModelValue::from(wp_model_core::model::HexT(255)));
    for text in ["0xff", "0XFF", "ff"] {
        let (meta, _) = export_yield_value(
            &Value::Str(text.into()),
            Some(&FieldType::Base(BaseType::Hex)),
        )
        .unwrap();
        assert_eq!(meta, DataType::Hex, "hex parse of {text}");
    }
    assert!(
        export_yield_value(
            &Value::Str("zz".into()),
            Some(&FieldType::Base(BaseType::Hex)),
        )
        .is_err()
    );
    assert!(
        export_yield_value(&Value::Number(-1.0), Some(&FieldType::Base(BaseType::Hex)),).is_err()
    );
    assert!(
        export_yield_value(&Value::Bool(true), Some(&FieldType::Base(BaseType::Hex)),).is_err()
    );
}

#[test]
fn typed_array_element_lanes_for_each_base_type() {
    // Array(Digit) elements; Array(Bool); Array(Time); Array(Hex); Array(Float).
    for (item_type, item_value, expected_meta) in [
        (
            BaseType::Digit,
            Value::Number(7.0),
            DataType::Array("digit".to_string()),
        ),
        (
            BaseType::Float,
            Value::Number(1.5),
            DataType::Array("float".to_string()),
        ),
        (
            BaseType::Bool,
            Value::Bool(true),
            DataType::Array("bool".to_string()),
        ),
        (
            BaseType::Time,
            Value::Number(1_710_115_200_000_000_000.0),
            DataType::Array("time".to_string()),
        ),
        (
            BaseType::Ip,
            Value::Str("10.0.0.1".into()),
            DataType::Array("ip".to_string()),
        ),
        (
            BaseType::Hex,
            Value::Number(1.0),
            DataType::Array("hex".to_string()),
        ),
    ] {
        let (meta, value) = export_yield_value(
            &Value::Array(vec![item_value]),
            Some(&FieldType::Array(item_type)),
        )
        .unwrap();
        assert_eq!(meta, expected_meta);
        assert!(matches!(value, ModelValue::Array(_)));
    }
    // Array(Chars) with a non-string element → error.
    let err = export_yield_value(
        &Value::Array(vec![Value::Number(1.0)]),
        Some(&FieldType::Array(BaseType::Chars)),
    )
    .expect_err("array/chars rejects non-string elements");
    assert!(err.to_string().contains("array/chars field requires"));
    // ArrayAny over mixed elements works.
    let (meta, _) = export_yield_value(
        &Value::Array(vec![Value::Number(1.0), Value::Str("x".into())]),
        Some(&FieldType::ArrayAny),
    )
    .unwrap();
    assert_eq!(meta, DataType::Array("auto".to_string()));
}

#[test]
fn untyped_object_with_digit_bool_and_string_members() {
    // rule_value_to_model_value lanes inside an untyped object:
    // Number(integer) → Digit, Number(fraction) → Float, Bool, Str.
    let mut obj = EngineHashMap::default();
    obj.insert("n".into(), Value::Number(3.0));
    obj.insert("f".into(), Value::Number(1.5));
    obj.insert("b".into(), Value::Bool(true));
    obj.insert("s".into(), Value::Str("x".into()));
    let (meta, value) = export_yield_value(&Value::Object(obj), Some(&FieldType::Object)).unwrap();
    assert_eq!(meta, DataType::Obj);
    let ModelValue::Obj(object) = value else {
        panic!("expected object");
    };
    assert_eq!(object.get("n").unwrap().get_meta(), &DataType::Digit);
    assert_eq!(object.get("f").unwrap().get_meta(), &DataType::Float);
    assert_eq!(object.get("b").unwrap().get_meta(), &DataType::Bool);
    assert_eq!(object.get("s").unwrap().get_meta(), &DataType::Chars);
}

#[test]
fn data_record_json_null_and_digit_lanes() {
    // model_value_to_json: Null → json null, Digit → integer.
    let mut record = DataRecord::default();
    record.push(FieldStorage::from_owned(Field::new(
        DataType::Chars,
        "maybe_null",
        ModelValue::Null,
    )));
    record.push(FieldStorage::from_owned(Field::new(
        DataType::Digit,
        "count",
        ModelValue::from(7_i64),
    )));
    let json = data_record_to_json_string(&record).unwrap();
    let json: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(json["maybe_null"], serde_json::Value::Null);
    assert_eq!(json["count"], 7);
}

// ===========================================================================
// column_batch.rs — direct-write / batched-close paths
// ===========================================================================

fn sample_record(origin: AlertOrigin, yield_fields: Vec<(Arc<str>, Value)>) -> OutputRecord {
    OutputRecord {
        wfx_id: "a1b2c3d4e5f60718".to_string(),
        rule_name: Arc::from("q1_pass"),
        score: 42.5,
        entity_type: Arc::from("ip"),
        entity_id: "10.0.0.1".to_string(),
        origin,
        fired_at: "2026-08-16T00:00:00Z".to_string(),
        emit_time: Arc::from("2026-08-16T00:00:01Z"),
        matched_rows: Vec::new(),
        summary: Arc::from("summary text"),
        yield_target: Arc::from("alerts"),
        yield_fields,
        yield_field_types: Arc::from(vec![
            (Arc::from("auction_id"), FieldType::Base(BaseType::Float)),
            (Arc::from("price"), FieldType::Base(BaseType::Float)),
        ]),
        event_time_nanos: 0,
        machine_id: Arc::from(""),
        scope_key: Arc::from(""),
    }
}

fn assert_records_equal(a: &DataRecord, b: &DataRecord) {
    assert_eq!(a.items.len(), b.items.len(), "field count mismatch");
    for (fa, fb) in a.items.iter().zip(b.items.iter()) {
        assert_eq!(fa.get_name(), fb.get_name());
        assert_eq!(fa.get_meta(), fb.get_meta());
        assert_eq!(fa.get_value(), fb.get_value());
    }
}

#[test]
fn commit_close_rows_batch_matches_record_appended_rows() {
    use crate::alert::types::export_yield_value;
    let target = Arc::from("alerts");
    let ft_float = FieldType::Base(BaseType::Float);
    let rule_name = Arc::from("q1_pass");
    let entity_type = Arc::from("ip");
    let emit_time = Arc::from("2026-08-16T00:00:01Z");
    let n = 3usize;
    // price present in rows {0, 2}, absent in row 1 → mid-segment gap.
    let price_present = [true, false, true];
    let origins = ["close:timeout", "close:flush", "close:eos"];

    // ---- record path (origin = Close{reason}, per-row summary) ----
    let mut via_records = AlertColumnBuilder::new(Arc::clone(&target));
    for (i, &present) in price_present.iter().enumerate() {
        let reason = match i {
            0 => CloseReason::Timeout,
            1 => CloseReason::Flush,
            _ => CloseReason::Eos,
        };
        let mut record = sample_record(
            AlertOrigin::Close { reason },
            vec![
                (Arc::from("auction_id"), Value::Number((1000 + i) as f64)),
                (Arc::from("price"), Value::Number(9.5 + i as f64 * 10.0)),
            ],
        );
        if !present {
            record
                .yield_fields
                .retain(|(name, _)| name.as_ref() != "price");
        }
        record.wfx_id = format!("id{i}");
        record.entity_id = format!("10.0.0.{}", i + 1);
        record.fired_at = format!("ts{i}");
        record.score = 42.5 + i as f64;
        record.summary = Arc::from(format!("summary{i}"));
        via_records.append_record(&record).unwrap();
    }

    // ---- batched close path ----
    // Column indices follow registration order: auction_id = 0, price = 1.
    let mut via_batch = AlertColumnBuilder::new(Arc::clone(&target));
    via_batch
        .register_yield_column(&Arc::from("auction_id"), None)
        .unwrap();
    via_batch
        .register_yield_column(&Arc::from("price"), None)
        .unwrap();
    let auction_col = 0usize;
    let price_col = 1usize;
    let wfx: Vec<String> = (0..n).map(|i| format!("id{i}")).collect();
    let scores: Vec<f64> = (0..n).map(|i| 42.5 + i as f64).collect();
    let eids: Vec<String> = (0..n).map(|i| format!("10.0.0.{}", i + 1)).collect();
    let fats: Vec<String> = (0..n).map(|i| format!("ts{i}")).collect();
    let origins_arc: Vec<Arc<str>> = origins.iter().map(|s| Arc::from(*s)).collect();
    let close_reasons_arc: Vec<Arc<str>> = ["timeout", "flush", "eos"]
        .iter()
        .map(|s| Arc::from(*s))
        .collect();
    let summaries: Vec<Arc<str>> = (0..n).map(|i| Arc::from(format!("summary{i}"))).collect();
    let mut staged_rows = Vec::with_capacity(n);
    for (i, &present) in price_present.iter().enumerate() {
        let a = export_yield_value(&Value::Number((1000 + i) as f64), Some(&ft_float)).unwrap();
        let mut row_cells = vec![(auction_col, a.0, a.1)];
        if present {
            let p =
                export_yield_value(&Value::Number(9.5 + i as f64 * 10.0), Some(&ft_float)).unwrap();
            row_cells.push((price_col, p.0, p.1));
        }
        staged_rows.push(row_cells);
    }
    via_batch.commit_close_rows_batch(
        &wfx,
        &scores,
        &eids,
        &fats,
        &rule_name,
        &entity_type,
        &origins_arc,
        &close_reasons_arc,
        &emit_time,
        &summaries,
        &staged_rows,
    );

    let batch_records = via_records.finish();
    let batch_cols = via_batch.finish();
    assert_eq!(batch_records.len(), batch_cols.len());
    for i in 0..batch_records.len() {
        let a = batch_records.iter_data_records().nth(i).unwrap().unwrap();
        let b = batch_cols.iter_data_records().nth(i).unwrap().unwrap();
        assert_records_equal(&a, &b);
    }
}

#[test]
fn stage_yield_cell_f64_fast_slow_and_error_lanes() {
    let target = Arc::from("alerts");
    let mut builder = AlertColumnBuilder::new(target);
    let ft_digit = FieldType::Base(BaseType::Digit);
    let name = Arc::from("n");
    let name_dup = Arc::from("n");

    // Slow path (first use of the column): reserves + exports the f64.
    builder.begin_row();
    builder
        .stage_yield_cell_f64(&name, Some(&ft_digit), 42.0)
        .unwrap();
    // Fast path (same Arc at the same position).
    builder.begin_row();
    builder
        .stage_yield_cell_f64(&name, Some(&ft_digit), 43.0)
        .unwrap();
    // Duplicate (equal name, different Arc) → error.
    builder.begin_row();
    builder
        .stage_yield_cell_f64(&name, Some(&ft_digit), 44.0)
        .unwrap();
    assert!(
        builder
            .stage_yield_cell_f64(&name_dup, Some(&ft_digit), 45.0)
            .is_err()
    );
    assert_eq!(builder.len(), 0, "failed rows must not touch columns");

    // Reserved prefix → error.
    let bad = Arc::from("__wfu_evil");
    builder.begin_row();
    assert!(builder.stage_yield_cell_f64(&bad, None, 1.0).is_err());

    // Non-finite with a float target → falls back to the Value path → error.
    let ft_float = FieldType::Base(BaseType::Float);
    let fname = Arc::from("f");
    builder.begin_row();
    assert!(
        builder
            .stage_yield_cell_f64(&fname, Some(&ft_float), f64::NAN)
            .is_err()
    );

    // Two clean rows commit and read back as digits.
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    builder.begin_row();
    builder
        .stage_yield_cell_f64(&name, Some(&ft_digit), 1.0)
        .unwrap();
    builder.commit_each_row(EachRowCells {
        wfx_id: "id0".into(),
        score: 1.0,
        entity_id: "e0".into(),
        fired_at: "t0".into(),
        rule_name: &Arc::from("r"),
        entity_type: &Arc::from("ip"),
        origin: &Arc::from("event"),
        close_reason: &Arc::from(""),
        emit_time: &Arc::from("2026-08-16T00:00:01Z"),
        summary: &Arc::from("s"),
    });
    builder.begin_row();
    builder
        .stage_yield_cell_f64(&name, Some(&ft_digit), 2.0)
        .unwrap();
    builder.commit_each_row(EachRowCells {
        wfx_id: "id1".into(),
        score: 1.0,
        entity_id: "e1".into(),
        fired_at: "t1".into(),
        rule_name: &Arc::from("r"),
        entity_type: &Arc::from("ip"),
        origin: &Arc::from("event"),
        close_reason: &Arc::from(""),
        emit_time: &Arc::from("2026-08-16T00:00:01Z"),
        summary: &Arc::from("s"),
    });
    let batch = builder.finish();
    assert_eq!(batch.len(), 2);
    let rows: Vec<_> = batch.iter_data_records().collect();
    assert_eq!(
        rows[0].as_ref().unwrap().field("n").unwrap().get_meta(),
        &DataType::Digit
    );
    assert_eq!(
        rows[1].as_ref().unwrap().field("n").unwrap().get_value(),
        &ModelValue::from(2_i64)
    );
}

#[test]
fn register_yield_column_rejects_reserved_prefix() {
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let err = builder
        .register_yield_column(&Arc::from("__wfu_bad"), None)
        .expect_err("reserved prefix rejected at registration");
    assert!(err.to_string().contains(WFU_PREFIX));
    assert!(builder.is_empty());
}

#[test]
fn register_yield_column_const_value_fills_untouched_rows() {
    // A column registered with a batch-constant cell fills every row that
    // never stages it — including rows appended via append_record.
    let ft_chars = FieldType::Base(BaseType::Chars);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let const_cell = export_yield_value(&Value::Str("q1".into()), Some(&ft_chars)).unwrap();
    builder
        .register_yield_column(&Arc::from("alert_type"), Some(const_cell))
        .unwrap();
    builder
        .append_record(&sample_record(
            AlertOrigin::Event,
            vec![(Arc::from("auction_id"), Value::Number(1.0))],
        ))
        .unwrap();
    builder
        .append_record(&sample_record(
            AlertOrigin::Event,
            vec![(Arc::from("auction_id"), Value::Number(2.0))],
        ))
        .unwrap();
    let batch = builder.finish();
    let rows: Vec<_> = batch.iter_data_records().collect();
    for row in &rows {
        let field = row.as_ref().unwrap().field("alert_type").unwrap();
        assert_eq!(field.get_meta(), &DataType::Chars);
        assert_eq!(field.get_value(), &ModelValue::from("q1"));
    }
}

#[test]
fn reserve_rows_and_take_staged_smoke() {
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    builder.reserve_rows(8);
    assert!(builder.is_empty());

    // begin_row + stage + take_staged: the staged cells are drained.
    builder.begin_row();
    builder
        .stage_yield_cell(
            &Arc::from("x"),
            Some(&FieldType::Base(BaseType::Float)),
            &Value::Number(1.5),
        )
        .unwrap();
    let staged = builder.take_staged();
    assert_eq!(staged.len(), 1);
    assert!(builder.is_empty(), "staged cells are not committed rows");
    // A second take is empty.
    assert!(builder.take_staged().is_empty());
}

#[test]
fn staged_row_with_const_column_matches_append_path() {
    // Cross-check: staged rows + a const column (never staged per row) produce
    // identical row views to the record path where the field is constant.
    use crate::alert::types::export_yield_value;
    let target = Arc::from("alerts");
    let ft_float = FieldType::Base(BaseType::Float);
    let ft_chars = FieldType::Base(BaseType::Chars);
    let rule_name = Arc::from("r");
    let entity_type = Arc::from("ip");
    let origin = Arc::from("event");
    let close_reason = Arc::from("");
    let emit_time = Arc::from("2026-08-16T00:00:01Z");
    let summary = Arc::from("s");

    let mut via_records = AlertColumnBuilder::new(Arc::clone(&target));
    for i in 0..2 {
        let mut record = sample_record(
            AlertOrigin::Event,
            vec![
                (Arc::from("v"), Value::Number((10 + i) as f64)),
                (Arc::from("t"), Value::Str("const".into())),
            ],
        );
        record.wfx_id = format!("id{i}");
        record.entity_id = format!("e{i}");
        record.fired_at = format!("ts{i}");
        record.rule_name = Arc::from("r");
        record.entity_type = Arc::from("ip");
        record.summary = Arc::from("s");
        via_records.append_record(&record).unwrap();
    }

    let mut via_staged = AlertColumnBuilder::new(Arc::clone(&target));
    // Register order must match the record path's first-record yield order
    // ([v, t]) so the row views line up column-for-column.
    via_staged
        .register_yield_column(&Arc::from("v"), None)
        .unwrap();
    via_staged
        .register_yield_column(
            &Arc::from("t"),
            Some(export_yield_value(&Value::Str("const".into()), Some(&ft_chars)).unwrap()),
        )
        .unwrap();
    // `t` is a const column; staging only `v` per row → the const fills `t`.
    let v_name = Arc::from("v");
    for i in 0..2 {
        via_staged.begin_row();
        via_staged
            .stage_yield_cell(&v_name, Some(&ft_float), &Value::Number((10 + i) as f64))
            .unwrap();
        via_staged.commit_each_row(EachRowCells {
            wfx_id: format!("id{i}"),
            score: 42.5,
            entity_id: format!("e{i}"),
            fired_at: format!("ts{i}"),
            rule_name: &rule_name,
            entity_type: &entity_type,
            origin: &origin,
            close_reason: &close_reason,
            emit_time: &emit_time,
            summary: &summary,
        });
    }

    let a = via_records.finish();
    let b = via_staged.finish();
    assert_eq!(a.len(), b.len());
    for i in 0..a.len() {
        let row_a = a.iter_data_records().nth(i).unwrap().unwrap();
        let row_b = b.iter_data_records().nth(i).unwrap().unwrap();
        assert_records_equal(&row_a, &row_b);
    }
}
