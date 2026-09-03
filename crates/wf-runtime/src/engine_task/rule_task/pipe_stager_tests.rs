use super::stager::PIPE_EVENT_TIME_FIELD;
use super::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use wf_engine::alert::AlertOrigin;
use wf_engine::match_engine::Value;

fn record_with(
    target: &str,
    event_time_nanos: i64,
    yield_fields: Vec<(Arc<str>, Value)>,
) -> OutputRecord {
    OutputRecord {
        wfx_id: format!("id-{event_time_nanos}"),
        rule_name: "pipe_s1".into(),
        score: 1.0,
        entity_type: "ip".into(),
        entity_id: "10.0.0.1".to_string(),
        origin: AlertOrigin::Event,
        fired_at: "2026-01-01T00:00:00Z".to_string(),
        emit_time: "2026-01-01T00:00:00Z".into(),
        matched_rows: Vec::new(),
        summary: "".into(),
        yield_target: target.into(),
        yield_fields,
        yield_field_types: Vec::new().into(),
        event_time_nanos,
        machine_id: Arc::from(""),
        scope_key: "".into(),
    }
}

/// Covers every arm of the coercion matrix: the pipe event-time field,
/// the time column (with and without an explicit value), all supported
/// scalar columns, Utf8 coercions of non-string values, type-mismatch
/// rows (-> null), and an unsupported column type (Date32 -> null).
fn stager_schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            PIPE_EVENT_TIME_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("n_i", DataType::Int64, true),
        Field::new("n_f", DataType::Float64, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("blob", DataType::Utf8, true),
        Field::new("unsupported", DataType::Date32, true),
    ]))
}

fn varied_records() -> Vec<OutputRecord> {
    vec![
        // All fields present, happy path.
        record_with(
            "t",
            1_000,
            vec![
                (
                    "event_time".into(),
                    Value::Number(1_700_000_000_000_000_000.0),
                ),
                ("n_i".into(), Value::Number(7.0)),
                ("n_f".into(), Value::Number(1.5)),
                ("flag".into(), Value::Bool(true)),
                ("label".into(), Value::Str("x".into())),
                (
                    "blob".into(),
                    Value::Array(vec![Value::Number(1.0), Value::Str("a".into())]),
                ),
            ],
        ),
        // Missing scalars -> null; time column absent -> event-time
        // fallback; Utf8 coercion of Number.
        record_with(
            "t",
            2_000,
            vec![
                ("n_f".into(), Value::Number(2.0)),
                ("label".into(), Value::Number(42.0)),
            ],
        ),
        // Type mismatches -> null; Utf8 coercion of Bool.
        record_with(
            "t",
            3_000,
            vec![
                ("n_i".into(), Value::Str("zz".into())),
                ("flag".into(), Value::Number(1.0)),
                ("label".into(), Value::Bool(true)),
            ],
        ),
    ]
}

/// Direct semantic assertions on the staging coercion matrix (the old
/// per-row `build_pipeline_batch` path is gone; its behaviour lives on
/// exactly in `push_record`).
#[test]
fn staged_batch_coercion_matrix() {
    let schema = stager_schema();
    let records = varied_records();
    let mut stager = PipeBatchStager::new("t".into(), Arc::clone(&schema), Some(1));
    for record in &records {
        stager.push_record(record).expect("stage row");
    }
    let (_, staged, _) = stager.take_events().unwrap().expect("rows staged");
    assert_eq!(staged.len(), records.len());

    // Row 0 — every field present, happy path.
    let f = &staged[0].fields;
    assert_eq!(f.get(PIPE_EVENT_TIME_FIELD), Some(&Value::Number(1_000.0)));
    assert_eq!(
        f.get("event_time"),
        Some(&Value::Number(1_700_000_000_000_000_000.0))
    );
    assert_eq!(f.get("n_i"), Some(&Value::Number(7.0)));
    assert_eq!(f.get("n_f"), Some(&Value::Number(1.5)));
    assert_eq!(f.get("flag"), Some(&Value::Bool(true)));
    assert_eq!(f.get("label"), Some(&Value::Str("x".into())));
    assert_eq!(f.get("blob"), Some(&Value::Str(r#"[1.0,"a"]"#.into())));
    assert_eq!(f.get("unsupported"), None, "Date32 column stages as null");

    // Row 1 — missing scalars -> null (field absent); Utf8 coercion of
    // Number; the time column falls back to the record event time.
    let f = &staged[1].fields;
    assert_eq!(f.get(PIPE_EVENT_TIME_FIELD), Some(&Value::Number(2_000.0)));
    assert_eq!(
        f.get("event_time"),
        Some(&Value::Number(2_000.0)),
        "missing time-col value must fall back to event_time_nanos"
    );
    assert_eq!(f.get("n_i"), None);
    assert_eq!(f.get("n_f"), Some(&Value::Number(2.0)));
    assert_eq!(f.get("flag"), None);
    assert_eq!(f.get("label"), Some(&Value::Str("42".into())));
    assert_eq!(f.get("blob"), None);

    // Row 2 — type mismatches -> null; Utf8 coercion of Bool; a row
    // without any time value gets its own event_time_nanos.
    let f = &staged[2].fields;
    assert_eq!(f.get(PIPE_EVENT_TIME_FIELD), Some(&Value::Number(3_000.0)));
    assert_eq!(f.get("event_time"), Some(&Value::Number(3_000.0)));
    assert_eq!(f.get("n_i"), None, "Str into Int64 stages as null");
    assert_eq!(f.get("flag"), None, "Number into Bool stages as null");
    assert_eq!(f.get("label"), Some(&Value::Str("true".into())));
}

/// A non-finite number inside a structured (Array/Object) value must
/// fail the row instead of serializing `NaN` into JSON.
#[test]
fn staged_row_rejects_non_finite_number_inside_structured_value() {
    let schema = stager_schema();
    let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));
    let record = record_with(
        "t",
        0,
        vec![(
            "blob".into(),
            Value::Object(
                [("score".into(), Value::Number(f64::NAN))]
                    .into_iter()
                    .collect(),
            ),
        )],
    );
    let err = stager
        .push_record(&record)
        .expect_err("non-finite structured number should fail");
    assert!(
        err.to_string()
            .contains("structured numeric value must be finite")
    );
}

/// An explicit epoch-seconds/millis float yield for a Timestamp column
/// is normalized to epoch nanos.
#[test]
fn staged_timestamp_preserves_time_yield_as_epoch_nanos() {
    let schema = stager_schema();
    let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));
    let ts = 1_700_000_000_123_000_000i64;
    let record = record_with(
        "t",
        0,
        vec![("event_time".into(), Value::Number(1_700_000_000_123.0))],
    );
    stager.push_record(&record).expect("stage row");
    let (_, staged, _) = stager.take_events().unwrap().expect("rows staged");
    assert_eq!(
        staged[0].fields.get("event_time"),
        Some(&Value::Number(ts as f64)),
        "float epoch yield must normalize to exact epoch nanos"
    );
}

/// Flushing empties the buffers: a second flush is a no-op and later
/// rows start a fresh batch (per-input-batch flush boundary).
#[test]
fn stager_take_resets_buffers_between_flushes() {
    let schema = stager_schema();
    let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));

    assert!(
        stager.take_events().unwrap().is_none(),
        "fresh stager flush is a no-op"
    );

    stager
        .push_record(&record_with(
            "t",
            5,
            vec![("label".into(), Value::Str("a".into()))],
        ))
        .unwrap();
    stager
        .push_record(&record_with(
            "t",
            6,
            vec![("label".into(), Value::Str("b".into()))],
        ))
        .unwrap();
    let first = stager.take_events().unwrap().expect("rows staged");
    assert_eq!(first.1.len(), 2);

    assert!(
        stager.take_events().unwrap().is_none(),
        "buffers must reset after take"
    );

    stager
        .push_record(&record_with(
            "t",
            7,
            vec![("label".into(), Value::Str("c".into()))],
        ))
        .unwrap();
    let second = stager
        .take_events()
        .unwrap()
        .expect("row staged after reset");
    assert_eq!(second.1.len(), 1);
    assert_eq!(
        second.1[0].fields.get("label"),
        Some(&Value::Str("c".into()))
    );
}

/// Rows across MANY input batches coalesce only up to the flush point:
/// a long run keeps column alignment (no drift, no cross-contamination).
#[test]
fn stager_column_alignment_holds_over_many_rows() {
    let schema = stager_schema();
    let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));
    let rows = 500usize;
    for i in 0..rows {
        stager
            .push_record(&record_with(
                "t",
                i as i64,
                vec![
                    ("n_i".into(), Value::Number(i as f64)),
                    ("label".into(), Value::Str(format!("row-{i}").into())),
                    ("flag".into(), Value::Bool(i % 2 == 0)),
                ],
            ))
            .unwrap();
    }
    let (_, events, _) = stager.take_events().unwrap().expect("rows staged");
    assert_eq!(events.len(), rows);
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.fields.get("n_i"), Some(&Value::Number(i as f64)));
        assert_eq!(
            event.fields.get("label"),
            Some(&Value::Str(format!("row-{i}").into()))
        );
        assert_eq!(event.fields.get("flag"), Some(&Value::Bool(i % 2 == 0)));
    }
}

/// q4a 形状的含 meta 列 schema（deferred 中间窗 auction_finals + 管道 meta
/// 列——`record_window_fields` 会补的四个 `__wfu_meta_*`）：
/// `__wf_pipe_ts` + id/category/final/dateTime + meta×4。
fn q4a_stager_schema() -> arrow::datatypes::SchemaRef {
    use wf_lang::wfu_meta::WfuIntermediateMetaField;
    Arc::new(Schema::new(vec![
        Field::new(
            PIPE_EVENT_TIME_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("id", DataType::Int64, true),
        Field::new("category", DataType::Int64, true),
        Field::new("final", DataType::Float64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new(
            WfuIntermediateMetaField::RuleName.name(),
            DataType::Utf8,
            true,
        ),
        Field::new(WfuIntermediateMetaField::Score.name(), DataType::Utf8, true),
        Field::new(
            WfuIntermediateMetaField::EntityType.name(),
            DataType::Utf8,
            true,
        ),
        Field::new(
            WfuIntermediateMetaField::EntityId.name(),
            DataType::Utf8,
            true,
        ),
    ]))
}

/// q4a 形状的 OutputRecord 序列（含 meta 依赖：rule_name/score/entity_type/
/// entity_id 与 yield 字段均非空；dateTime 缺失由时间列回退补）。
fn q4a_records() -> Vec<OutputRecord> {
    (0..64)
        .map(|i| {
            let mut r = record_with(
                "auction_finals",
                1_700_000_000_000_000_000 + i * 1_000,
                vec![
                    ("id".into(), Value::Number(i as f64)),
                    ("category".into(), Value::Number((i % 5) as f64)),
                    ("final".into(), Value::Number(10.0 + i as f64)),
                    // dateTime 缺失 → 时间列回退 event_time_nanos
                ],
            );
            r.rule_name = "q4a_auction_finals".into();
            r.score = 20.0;
            r.entity_type = "digit".into();
            r.entity_id = (100_000 + i).to_string();
            r
        })
        .collect()
}

/// 行式 `push_record` 与列式 `push_record_columnar` 必须产出**字节一致**的
/// 批次（2026-08-26 q4a：deferred 中间窗 staging 切列式后，行式路径只留
/// 测试对拍——本条钉死两条路径语义一致，防列式装载漏列/错源）。
///
/// 覆盖：无 meta 列（stager_schema）+ q4a 形状（含 4 个 meta 列、时间列
/// 回退）；meta 值（rule_name/score/entity_type/entity_id）与 yield 字段
/// 逐一落入正确列。
#[test]
#[allow(clippy::type_complexity)] // 测试局部三元组签名，alias 会引入生命周期问题
fn push_record_columnar_matches_row_path() {
    let schemas: &[(Arc<str>, arrow::datatypes::SchemaRef, &[Arc<str>])] = &[
        (
            "t".into(),
            stager_schema(),
            &["event_time", "n_i", "n_f", "flag", "label", "blob"]
                .iter()
                .map(|s| Arc::from(*s))
                .collect::<Vec<_>>(),
        ),
        (
            "auction_finals".into(),
            q4a_stager_schema(),
            &["id", "category", "final", "dateTime"]
                .iter()
                .map(|s| Arc::from(*s))
                .collect::<Vec<_>>(),
        ),
    ];
    for (target, schema, yield_names) in schemas {
        let records: Vec<OutputRecord> = if **target == *"auction_finals" {
            q4a_records()
        } else {
            varied_records()
        };
        let mut row_stager = PipeBatchStager::new(Arc::clone(target), Arc::clone(schema), Some(1));
        let mut col_stager = PipeBatchStager::new_columnar(
            Arc::clone(target),
            Arc::clone(schema),
            Some(1),
            yield_names,
        );
        for r in &records {
            row_stager.push_record(r).expect("row path stage");
            col_stager
                .push_record_columnar(r)
                .expect("columnar path stage");
        }
        let row_batch = row_stager
            .take_batch()
            .expect("build")
            .expect("rows staged")
            .1;
        let col_batch = col_stager
            .take_batch()
            .expect("build")
            .expect("rows staged")
            .1;
        assert_eq!(
            row_batch, col_batch,
            "target={target}: push_record 与 push_record_columnar 批次必须一致"
        );
    }
}
