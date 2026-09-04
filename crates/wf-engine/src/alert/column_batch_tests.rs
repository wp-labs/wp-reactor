//! `AlertColumnBatch`/`AlertColumnBuilder` 测试（2026-09-04 自 column_batch.rs
//! 内联 `mod tests` 拆出；`#[path]` 兄弟子模块）：record/直写/批三路等价、稀疏列
//! gap、常量列折叠、失败路径一致性等行视图断言。

use super::*;
use crate::alert::AlertOrigin;
use crate::alert::types::OutputRecord;
use crate::match_engine::Value;
use wf_lang::FieldType;

fn sample_record(yield_fields: Vec<(Arc<str>, Value)>) -> OutputRecord {
    OutputRecord {
        wfx_id: "a1b2c3d4e5f60718".to_string(),
        rule_name: Arc::from("q1_pass"),
        score: 42.5,
        entity_type: Arc::from("ip"),
        entity_id: "10.0.0.1".to_string(),
        origin: AlertOrigin::Event,
        fired_at: "2026-08-16T00:00:00Z".to_string(),
        emit_time: Arc::from("2026-08-16T00:00:01Z"),
        matched_rows: Vec::new(),
        summary: Arc::from("summary text"),
        yield_target: Arc::from("alerts"),
        yield_fields,
        yield_field_types: Arc::from(vec![
            (
                Arc::from("auction_id"),
                FieldType::Base(wf_lang::BaseType::Float),
            ),
            (
                Arc::from("price"),
                FieldType::Base(wf_lang::BaseType::Float),
            ),
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
        match (fa.get_value(), fb.get_value()) {
            (ModelValue::Float(x), ModelValue::Float(y)) => assert_eq!(x, y),
            (ModelValue::Digit(x), ModelValue::Digit(y)) => assert_eq!(x, y),
            _ => assert_eq!(fa.get_value(), fb.get_value()),
        }
    }
}

/// Assert the field `name` was filled as a **mid-segment gap** at `row`
/// (meta `Ignore`, null) — i.e. it was absent that row rather than a
/// trailing fill. Guards the sparse-in-the-middle placement fix for the
/// batched fill (the pre-fix block-level top-up put such fills at the tail,
/// which is not byte-identical to the per-row path).
fn assert_mid_gap_at(builder: &AlertColumnBuilder, name: &Arc<str>, row: usize) {
    let col = builder.yield_cols.iter().find(|c| c.name == *name).unwrap();
    assert_eq!(
        col.metas[row],
        DataType::Ignore,
        "expected mid-segment fill gap for {name:?} at row {row}"
    );
    assert_eq!(col.values[row], ModelValue::Null);
}

#[test]
fn column_batch_row_view_matches_to_data_record() {
    let records = vec![
        sample_record(vec![
            (Arc::from("auction_id"), Value::Number(1000.0)),
            (Arc::from("price"), Value::Number(99.5)),
        ]),
        sample_record(vec![
            (Arc::from("auction_id"), Value::Number(1001.0)),
            (Arc::from("price"), Value::Number(79.25)),
        ]),
        sample_record(vec![
            (Arc::from("auction_id"), Value::Number(1002.0)),
            (Arc::from("price"), Value::Number(10.0)),
        ]),
    ];
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    for record in &records {
        builder.append_record(record).unwrap();
    }
    let batch = builder.finish();
    assert_eq!(batch.len(), 3);
    for (row, record) in records.iter().enumerate() {
        let via_columns = batch.iter_data_records().nth(row).unwrap().unwrap();
        let via_rows = record.to_data_record().unwrap();
        assert_records_equal(&via_columns, &via_rows);
    }
}

#[test]
fn commit_each_rows_batch_matches_repeated_commit_each_row() {
    // L3 batched commit must produce byte-identical output to committing
    // the same rows one-by-one via `commit_each_row` (constant + field
    // yield columns, block-level gap fill, plan-constant system cols).
    use crate::alert::types::export_yield_value;
    use wf_lang::BaseType;
    let target = Arc::from("alerts");
    let ft_chars = FieldType::Base(BaseType::Chars);
    let ft_float = FieldType::Base(BaseType::Float);
    let rule_name = Arc::from("q1_pass");
    let entity_type = Arc::from("digit");
    let origin = Arc::from("event");
    let close_reason = Arc::from("");
    let emit_time = Arc::from("2026-08-16T00:00:01Z");
    let summary = Arc::from("summary");
    let n = 3usize;
    // `price` present in rows {0, 2} but absent in row 1 → the batched
    // fill must land [real0, fill, real2], not a trailing run; `idle` is a
    // registered column that never gets staged (idle/literal analog).
    let price_present = [true, false, true];

    // ---- row-by-row builder ----
    let mut via_row = AlertColumnBuilder::new(Arc::clone(&target));
    via_row
        .register_yield_column(
            &Arc::from("alert_type"),
            Some(export_yield_value(&Value::Str("q1".into()), Some(&ft_chars)).unwrap()),
        )
        .unwrap();
    via_row
        .register_yield_column(&Arc::from("auction_id"), None)
        .unwrap();
    via_row
        .register_yield_column(&Arc::from("price"), None)
        .unwrap();
    via_row
        .register_yield_column(&Arc::from("idle"), None)
        .unwrap();
    for (i, price_present) in price_present.iter().enumerate() {
        via_row.begin_row();
        via_row
            .stage_yield_cell(
                &Arc::from("auction_id"),
                Some(&ft_float),
                &Value::Number((1000 + i) as f64),
            )
            .unwrap();
        if *price_present {
            via_row
                .stage_yield_cell(
                    &Arc::from("price"),
                    Some(&ft_float),
                    &Value::Number(9.5 + i as f64 * 10.0),
                )
                .unwrap();
        }
        via_row.commit_each_row(EachRowCells {
            wfx_id: format!("id{i}").into(),
            score: 42.0 + i as f64,
            entity_id: format!("10.0.0.{}", i + 1).into(),
            fired_at: format!("ts{i}"),
            rule_name: &rule_name,
            entity_type: &entity_type,
            origin: &origin,
            close_reason: &close_reason,
            emit_time: &emit_time,
            summary: &summary,
        });
    }
    // Sparse-mid-segment `price` placement: [real0, fill, real2].
    assert_mid_gap_at(&via_row, &Arc::from("price"), 1);

    // ---- batched builder ----
    let mut via_batch = AlertColumnBuilder::new(Arc::clone(&target));
    via_batch
        .register_yield_column(
            &Arc::from("alert_type"),
            Some(export_yield_value(&Value::Str("q1".into()), Some(&ft_chars)).unwrap()),
        )
        .unwrap();
    via_batch
        .register_yield_column(&Arc::from("auction_id"), None)
        .unwrap();
    via_batch
        .register_yield_column(&Arc::from("price"), None)
        .unwrap();
    via_batch
        .register_yield_column(&Arc::from("idle"), None)
        .unwrap();
    // Pre-export the field cells for the batch path (same export the
    // per-row `stage_yield_cell` performs), column-major per row.
    let auction_col = via_batch
        .yield_cols
        .iter()
        .position(|c| c.name.as_ref() == "auction_id")
        .unwrap();
    let price_col = via_batch
        .yield_cols
        .iter()
        .position(|c| c.name.as_ref() == "price")
        .unwrap();
    let wfx: Vec<SmolStr> = (0..n).map(|i| format!("id{i}").into()).collect();
    let scores: Vec<f64> = (0..n).map(|i| 42.0 + i as f64).collect();
    let eids: Vec<SmolStr> = (0..n).map(|i| format!("10.0.0.{}", i + 1).into()).collect();
    let fats: Vec<String> = (0..n).map(|i| format!("ts{i}")).collect();
    let mut staged_rows = Vec::with_capacity(n);
    for (i, price_present) in price_present.iter().enumerate() {
        let a = export_yield_value(&Value::Number((1000 + i) as f64), Some(&ft_float)).unwrap();
        let mut row_cells = vec![(auction_col, a.0, a.1)];
        if *price_present {
            let p =
                export_yield_value(&Value::Number(9.5 + i as f64 * 10.0), Some(&ft_float)).unwrap();
            row_cells.push((price_col, p.0, p.1));
        }
        staged_rows.push(row_cells);
    }
    via_batch.commit_each_rows_batch(
        &wfx,
        &scores,
        &eids,
        &fats,
        &rule_name,
        &entity_type,
        &origin,
        &close_reason,
        &emit_time,
        &summary,
        &staged_rows,
    );
    assert_mid_gap_at(&via_batch, &Arc::from("price"), 1);

    let batch_row = via_row.finish();
    let batch_col = via_batch.finish();
    assert_eq!(batch_row.len(), batch_col.len());
    assert_eq!(batch_row.len(), n);
    for i in 0..batch_row.len() {
        let a = batch_row.iter_data_records().nth(i).unwrap().unwrap();
        let b = batch_col.iter_data_records().nth(i).unwrap().unwrap();
        assert_records_equal(&a, &b);
    }
}

#[test]
fn commit_each_rows_batch_dense_all_present() {
    // Regression guard for the L3 default case (Q1: every yield field
    // present every row, no mid-segment gaps): batched commit stays
    // byte-identical to repeated `commit_each_row`.
    use crate::alert::types::export_yield_value;
    use wf_lang::BaseType;
    let target = Arc::from("alerts");
    let ft_chars = FieldType::Base(BaseType::Chars);
    let rule_name = Arc::from("q1_pass");
    let entity_type = Arc::from("digit");
    let origin = Arc::from("event");
    let close_reason = Arc::from("");
    let emit_time = Arc::from("2026-08-16T00:00:01Z");
    let summary = Arc::from("summary");
    let n = 3usize;

    let mut via_row = AlertColumnBuilder::new(Arc::clone(&target));
    via_row
        .register_yield_column(&Arc::from("alert_type"), None)
        .unwrap();
    for i in 0..n {
        via_row.begin_row();
        via_row
            .stage_yield_cell(
                &Arc::from("alert_type"),
                Some(&ft_chars),
                &Value::Str(format!("type{i}").into()),
            )
            .unwrap();
        via_row.commit_each_row(EachRowCells {
            wfx_id: format!("id{i}").into(),
            score: 1.0 + i as f64,
            entity_id: format!("e{i}").into(),
            fired_at: format!("ts{i}"),
            rule_name: &rule_name,
            entity_type: &entity_type,
            origin: &origin,
            close_reason: &close_reason,
            emit_time: &emit_time,
            summary: &summary,
        });
    }

    let mut via_batch = AlertColumnBuilder::new(Arc::clone(&target));
    via_batch
        .register_yield_column(&Arc::from("alert_type"), None)
        .unwrap();
    let type_col = 0usize;
    let wfx: Vec<SmolStr> = (0..n).map(|i| format!("id{i}").into()).collect();
    let scores: Vec<f64> = (0..n).map(|i| 1.0 + i as f64).collect();
    let eids: Vec<SmolStr> = (0..n).map(|i| format!("e{i}").into()).collect();
    let fats: Vec<String> = (0..n).map(|i| format!("ts{i}")).collect();
    let staged_rows: Vec<Vec<(usize, DataType, ModelValue)>> = (0..n)
        .map(|i| {
            let t = export_yield_value(&Value::Str(format!("type{i}").into()), Some(&ft_chars))
                .unwrap();
            vec![(type_col, t.0, t.1)]
        })
        .collect();
    via_batch.commit_each_rows_batch(
        &wfx,
        &scores,
        &eids,
        &fats,
        &rule_name,
        &entity_type,
        &origin,
        &close_reason,
        &emit_time,
        &summary,
        &staged_rows,
    );

    let batch_row = via_row.finish();
    let batch_col = via_batch.finish();
    assert_eq!(batch_row.len(), batch_col.len());
    for i in 0..batch_row.len() {
        let a = batch_row.iter_data_records().nth(i).unwrap().unwrap();
        let b = batch_col.iter_data_records().nth(i).unwrap().unwrap();
        assert_records_equal(&a, &b);
    }
}

#[test]
fn append_rejects_reserved_prefix_and_duplicates() {
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));

    let bad_prefix = sample_record(vec![(Arc::from("__wfu_evil"), Value::Number(1.0))]);
    assert!(builder.append_record(&bad_prefix).is_err());
    assert_eq!(builder.len(), 0, "failed append must not touch columns");

    let dup = sample_record(vec![
        (Arc::from("price"), Value::Number(1.0)),
        (Arc::from("price"), Value::Number(2.0)),
    ]);
    assert!(builder.append_record(&dup).is_err());
    assert_eq!(builder.len(), 0);
}

#[test]
fn emit_time_varies_across_batches_reads_back_per_row() {
    // R1 守护（2026-08-26）：emit_time 跨批变化（cached_emit_time 按 nanos
    // 缓存，不同批不同值）——若被常量列折叠成 Const，builder 跨批累积时
    // 后续批的 emit_time 会错读成第一批的值。必须逐行 Rows。
    let target = Arc::from("alerts");
    let rule_name = Arc::from("r1");
    let entity_type = Arc::from("digit");
    let origin = Arc::from("event");
    let close_reason = Arc::from("");
    let summary = Arc::from("summary");
    let mut builder = AlertColumnBuilder::new(Arc::clone(&target));
    // 批 1：emit_time = T1（两行）；批 2：emit_time = T2（一行）——模拟
    // builder 跨批累积（< ALERT_BATCH_SIZE 不 flush 的场景）。
    let t1 = Arc::from("2026-08-26T10:00:00Z");
    let t2 = Arc::from("2026-08-26T10:00:01Z");
    for _ in 0..2 {
        builder.commit_each_row(EachRowCells {
            wfx_id: SmolStr::from("id"),
            score: 1.0,
            entity_id: SmolStr::from("e"),
            fired_at: String::from("ts"),
            rule_name: &rule_name,
            entity_type: &entity_type,
            origin: &origin,
            close_reason: &close_reason,
            emit_time: &t1,
            summary: &summary,
        });
    }
    builder.commit_each_row(EachRowCells {
        wfx_id: SmolStr::from("id3"),
        score: 1.0,
        entity_id: SmolStr::from("e3"),
        fired_at: String::from("ts3"),
        rule_name: &rule_name,
        entity_type: &entity_type,
        origin: &origin,
        close_reason: &close_reason,
        emit_time: &t2,
        summary: &summary,
    });
    let batch = builder.finish();
    assert_eq!(batch.len(), 3);
    let rows: Vec<_> = batch.iter_data_records().collect();
    for (i, row) in rows.iter().enumerate() {
        let r = row.as_ref().unwrap();
        let et = r
            .field(WFU_EMIT_TIME)
            .expect("emit_time field present")
            .get_value();
        let expected = if i < 2 { t1.as_ref() } else { t2.as_ref() };
        assert_eq!(
            et.to_string(),
            expected,
            "行 {i} 的 emit_time 必须逐行正确（跨批变化不得折叠）"
        );
    }
}

#[test]
fn sparse_yield_columns_read_back_as_ignore_null() {
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    builder
        .append_record(&sample_record(vec![
            (Arc::from("auction_id"), Value::Number(1.0)),
            (Arc::from("price"), Value::Number(2.0)),
        ]))
        .unwrap();
    // A later record with an extra yield field extends the layout.
    builder
        .append_record(&sample_record(vec![
            (Arc::from("auction_id"), Value::Number(3.0)),
            (Arc::from("price"), Value::Number(4.0)),
            (Arc::from("extra"), Value::Str("x".into())),
        ]))
        .unwrap();
    let batch = builder.finish();
    let rows: Vec<_> = batch.iter_data_records().collect();
    let first = rows[0].as_ref().unwrap();
    let extra = first.field("extra").expect("sparse cell present");
    assert_eq!(extra.get_meta(), &DataType::Ignore);
}

#[test]
fn finish_leaves_builder_empty() {
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    builder
        .append_record(&sample_record(vec![(
            Arc::from("auction_id"),
            Value::Number(1.0),
        )]))
        .unwrap();
    let _ = builder.finish();
    assert!(builder.is_empty());
}

fn commit_staged(builder: &mut AlertColumnBuilder, wfx_id: &str, entity_id: &str, fired_at: &str) {
    builder.commit_each_row(EachRowCells {
        wfx_id: wfx_id.to_string().into(),
        score: 42.5,
        entity_id: entity_id.to_string().into(),
        fired_at: fired_at.to_string(),
        rule_name: &Arc::from("q1_pass"),
        entity_type: &Arc::from("ip"),
        origin: &Arc::from("event"),
        close_reason: &Arc::from(""),
        emit_time: &Arc::from("2026-08-16T00:00:01Z"),
        summary: &Arc::from("summary text"),
    });
}

#[test]
fn staged_rows_match_record_appended_rows() {
    // Same three records through both paths must yield identical
    // DataRecord row views (system fields included).
    let rows_spec = [
        ("a1b2c3d4e5f60718", "10.0.0.1", "2026-08-16T00:00:00Z"),
        ("b2c3d4e5f60718a1", "10.0.0.2", "2026-08-16T00:00:01Z"),
        ("c3d4e5f60718a1b2", "10.0.0.3", "2026-08-16T00:00:02Z"),
    ];
    let values = [
        (Value::Number(1000.0), Value::Number(99.5)),
        (Value::Number(1001.0), Value::Number(79.25)),
        (Value::Number(1002.0), Value::Number(10.0)),
    ];

    // Record path: one OutputRecord per row, appended via append_record.
    let mut via_records = AlertColumnBuilder::new(Arc::from("alerts"));
    for ((wfx_id, entity_id, fired_at), vals) in rows_spec.iter().zip(values.iter()) {
        let mut record = sample_record(vec![
            (Arc::from("auction_id"), vals.0.clone()),
            (Arc::from("price"), vals.1.clone()),
        ]);
        record.wfx_id = wfx_id.to_string();
        record.entity_id = entity_id.to_string();
        record.fired_at = fired_at.to_string();
        via_records.append_record(&record).unwrap();
    }

    // Staging path (reuses one Arc per field name, like plan slots).
    let names: [Arc<str>; 2] = [Arc::from("auction_id"), Arc::from("price")];
    let ft = Some(FieldType::Base(wf_lang::BaseType::Float));
    let mut via_staging = AlertColumnBuilder::new(Arc::from("alerts"));
    for ((wfx_id, entity_id, fired_at), vals) in rows_spec.iter().zip(values.iter()) {
        via_staging.begin_row();
        via_staging
            .stage_yield_cell(&names[0], ft.as_ref(), &vals.0)
            .unwrap();
        via_staging
            .stage_yield_cell(&names[1], ft.as_ref(), &vals.1)
            .unwrap();
        commit_staged(&mut via_staging, wfx_id, entity_id, fired_at);
    }

    let record_batch = via_records.finish();
    let staged_batch = via_staging.finish();
    assert_eq!(record_batch.len(), staged_batch.len());
    for row in 0..record_batch.len() {
        let a = record_batch.iter_data_records().nth(row).unwrap().unwrap();
        let b = staged_batch.iter_data_records().nth(row).unwrap().unwrap();
        assert_records_equal(&a, &b);
    }
}

#[test]
fn staged_optional_omission_creates_sparse_cells() {
    // Row 1 omits the middle field (optional input missing, #62): the
    // later column must backfill an Ignore/Null cell for that row.
    let names: [Arc<str>; 3] = [Arc::from("a"), Arc::from("b"), Arc::from("c")];
    let ft = Some(FieldType::Base(wf_lang::BaseType::Float));
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));

    // Row 0: a, c (b omitted).
    builder.begin_row();
    builder
        .stage_yield_cell(&names[0], ft.as_ref(), &Value::Number(1.0))
        .unwrap();
    builder
        .stage_yield_cell(&names[2], ft.as_ref(), &Value::Number(3.0))
        .unwrap();
    commit_staged(&mut builder, "id0", "e0", "t0");

    // Row 1: full a, b, c.
    builder.begin_row();
    builder
        .stage_yield_cell(&names[0], ft.as_ref(), &Value::Number(4.0))
        .unwrap();
    builder
        .stage_yield_cell(&names[1], ft.as_ref(), &Value::Number(5.0))
        .unwrap();
    builder
        .stage_yield_cell(&names[2], ft.as_ref(), &Value::Number(6.0))
        .unwrap();
    commit_staged(&mut builder, "id1", "e1", "t1");

    let batch = builder.finish();
    let rows: Vec<_> = batch.iter_data_records().collect();
    let row0 = rows[0].as_ref().unwrap();
    let b_cell = row0.field("b").expect("sparse cell present");
    assert_eq!(b_cell.get_meta(), &DataType::Ignore);
    let row1 = rows[1].as_ref().unwrap();
    match row1.field("b").unwrap().get_value() {
        ModelValue::Float(n) => assert_eq!(*n, 5.0),
        other => panic!("unexpected value for b: {other:?}"),
    }
}

#[test]
fn stage_rejects_reserved_prefix_and_duplicates_and_keeps_row_clean() {
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let bad = Arc::from("__wfu_evil");
    builder.begin_row();
    assert!(
        builder
            .stage_yield_cell(&bad, None, &Value::Number(1.0))
            .is_err()
    );

    let a = Arc::from("dup");
    let a2 = Arc::from("dup");
    builder.begin_row();
    builder
        .stage_yield_cell(&a, None, &Value::Number(1.0))
        .unwrap();
    // Same name again (different Arc, equal string) → duplicate error.
    assert!(
        builder
            .stage_yield_cell(&a2, None, &Value::Number(2.0))
            .is_err()
    );
    assert_eq!(builder.len(), 0, "failed rows must not touch columns");
}

#[test]
fn failed_staging_then_successful_row_is_consistent() {
    // A row that errors mid-staging leaves no partial state; the next
    // row commits cleanly.
    let n = Arc::from("x");
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    builder.begin_row();
    builder
        .stage_yield_cell(&n, None, &Value::Number(1.0))
        .unwrap();
    let bad = Arc::from("__wfu_bad");
    assert!(
        builder
            .stage_yield_cell(&bad, None, &Value::Number(2.0))
            .is_err()
    );
    // begin_row clears the staged cells; commit must still be balanced.
    builder.begin_row();
    builder
        .stage_yield_cell(&n, None, &Value::Number(3.0))
        .unwrap();
    commit_staged(&mut builder, "id", "e", "t");
    let batch = builder.finish();
    assert_eq!(batch.len(), 1);
    let row = batch.iter_data_records().next().unwrap().unwrap();
    match row.field("x").unwrap().get_value() {
        ModelValue::Float(n) => assert_eq!(*n, 3.0),
        other => panic!("unexpected value for x: {other:?}"),
    }
}
