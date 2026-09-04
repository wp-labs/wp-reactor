//! direct_tests.rs 拆出的行式 Event 路径直发测试（2026-09-04；`#[path]`
//! 子模块，经父模块 `use super::*` 复用其导入）。
//!
//! 覆盖：`execute_each_direct` / 批向量化 `execute_each_direct_batch` 与 record
//! 路径（`execute_each_with_joins` + `append_record`）的逐行一致性，含 filter
//! 拒绝计数、显式 NaN 求值错误、`__wfu_id` 字节流一致、批内缺字段（null）
//! 车道与稀疏列漂移、批中段失败只跳该行、常量/字段/`WfuMeta` 各特化车道。

use super::*;

fn sample_events() -> Vec<Event> {
    vec![
        event(vec![
            ("sip", str_val("10.0.0.1")),
            ("auction_id", num(1000.0)),
            ("price", num(99.5)),
        ]),
        event(vec![
            ("sip", str_val("10.0.0.2")),
            ("auction_id", num(1001.0)),
            ("price", num(79.25)),
        ]),
        // Missing optional `price` → the field must be omitted (#62),
        // exercising the sparse-column layout drift on the direct path.
        event(vec![
            ("sip", str_val("10.0.0.3")),
            ("auction_id", num(1002.0)),
        ]),
    ]
}

#[test]
fn execute_each_direct_matches_record_path_rows() {
    let exec = each_plan_rule();
    let events = sample_events();
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_000_000_000;

    // Record path.
    let mut via_records = AlertColumnBuilder::new(Arc::from("alerts"));
    for ev in &events {
        let record = exec
            .execute_each_with_joins(ev, NANOS, &lookup, &[], NANOS + 1)
            .expect("record path must succeed")
            .expect("filter passes");
        via_records.append_record(&record).unwrap();
    }

    // Direct path.
    let mut via_direct = AlertColumnBuilder::new(Arc::from("alerts"));
    for ev in &events {
        let appended = exec
            .execute_each_direct(ev, NANOS, &lookup, &[], NANOS + 1, &mut via_direct)
            .expect("direct path must succeed");
        assert!(appended, "filter passes on the direct path too");
    }

    let record_batch = via_records.finish();
    let direct_batch = via_direct.finish();
    assert_eq!(record_batch.len(), direct_batch.len());
    for row in 0..record_batch.len() {
        let a = record_batch.iter_data_records().nth(row).unwrap().unwrap();
        let b = direct_batch.iter_data_records().nth(row).unwrap().unwrap();
        assert_eq!(a.items.len(), b.items.len(), "row {row} field count");
        for (fa, fb) in a.items.iter().zip(b.items.iter()) {
            assert_eq!(fa.get_name(), fb.get_name(), "row {row} field name");
            assert_eq!(fa.get_meta(), fb.get_meta(), "row {row} field meta");
            assert_eq!(fa.get_value(), fb.get_value(), "row {row} field value");
        }
    }
}

#[test]
fn execute_each_direct_filter_rejection_appends_nothing() {
    let mut plan = simple_rule_plan(
        "filtered",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::BinOp {
            op: wf_lang::ast::BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        }),
    });
    let exec = RuleExecutor::new(plan);
    let lookup = EmptyLookup;
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let appended = exec
        .execute_each_direct(
            &event(vec![("sip", str_val("10.9.9.9"))]),
            1_000_000,
            &lookup,
            &[],
            1_000_001,
            &mut builder,
        )
        .unwrap();
    assert!(!appended, "where filter rejects the event");
    assert!(builder.is_empty());
}

#[test]
fn execute_each_direct_surfaces_eval_errors() {
    // Explicit NaN against a Float-typed yield must fail identically to the
    // record path (no partial row committed).
    let mut plan = simple_rule_plan(
        "nan_rule",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "lat".into(),
        value: Expr::Number(f64::NAN),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("lat".into(), FieldType::Base(BaseType::Float))]),
    );
    let lookup = EmptyLookup;
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let result = exec.execute_each_direct(
        &event(vec![("sip", str_val("10.0.0.1"))]),
        1_000_000,
        &lookup,
        &[],
        1_000_001,
        &mut builder,
    );
    assert!(result.is_err(), "explicit NaN must fail the direct path");
    assert!(builder.is_empty(), "failed row must not touch columns");
}

#[test]
fn direct_path_wfx_id_matches_record_path() {
    // wfx_id depends on the event fields — the direct path must hash the
    // identical byte stream (spot-check via the row view).
    let exec = each_plan_rule();
    let ev = sample_events().remove(0);
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_123_456_789;

    let record = exec
        .execute_each_with_joins(&ev, NANOS, &lookup, &[], NANOS + 7)
        .unwrap()
        .unwrap();
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    exec.execute_each_direct(&ev, NANOS, &lookup, &[], NANOS + 7, &mut builder)
        .unwrap();
    let batch = builder.finish();
    let direct_row = batch.iter_data_records().next().unwrap().unwrap();
    let record_row = record.to_data_record().unwrap();
    let direct_id = direct_row
        .items
        .iter()
        .find(|f| f.get_name() == "__wfu_id")
        .unwrap();
    let record_id = record_row
        .items
        .iter()
        .find(|f| f.get_name() == "__wfu_id")
        .unwrap();
    assert_eq!(direct_id.get_value(), record_id.get_value());
}

// -- Batched direct path (build_each_direct vectorization) ------------------

#[test]
fn execute_each_direct_batch_matches_per_event_path_rows() {
    // Same mixed event batch (one with a missing optional field) through the
    // per-event direct path and the batched direct path must produce
    // identical rows, appended counts, and appended-index bookkeeping.
    let exec = each_plan_rule();
    let events = sample_events();
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_000_000_000;

    // Per-event path.
    let mut via_per_event = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut per_event_appended = 0usize;
    for (i, ev) in events.iter().enumerate() {
        let appended = exec
            .execute_each_direct(
                ev,
                NANOS + i as i64,
                &lookup,
                &[],
                NANOS,
                &mut via_per_event,
            )
            .expect("per-event direct path must succeed");
        if appended {
            per_event_appended += 1;
        }
    }

    // Batched path.
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_batch = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &rows,
        &lookup,
        &[],
        NANOS,
        &mut via_batch,
        &mut appended_idx,
    );
    assert_eq!(stats.appended, per_event_appended);
    assert_eq!(stats.rejected, 0);
    assert_eq!(stats.failed, 0);
    assert_eq!(appended_idx, (0..events.len()).collect::<Vec<_>>());

    assert_batches_equal_rows(&via_per_event.finish(), &via_batch.finish());
}

#[test]
fn execute_each_direct_batch_lit_and_general_specs_match_record_path() {
    // Const score + StringLit entity + literal/field/general (WfuMeta) yields:
    // every specialization lane must stay row-equivalent to the record path.
    use wf_lang::wfu_meta::WfuMetaField;

    let mut plan = simple_rule_plan(
        "lit_rule",
        simple_plan(vec![], vec![]),
        Expr::Number(7.5),
        "ip",
        Expr::StringLit("fixed-entity".into()),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "const_str".into(),
            value: Expr::StringLit("const-value".into()),
        },
        YieldField {
            name: "const_num".into(),
            value: Expr::Number(1.25),
        },
        YieldField {
            name: "const_bool".into(),
            value: Expr::Bool(true),
        },
        YieldField {
            name: "sip".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
        YieldField {
            name: "fired_at".into(),
            value: Expr::WfuMeta(WfuMetaField::FiredAt),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("const_str".into(), FieldType::Base(BaseType::Chars)),
            ("const_num".into(), FieldType::Base(BaseType::Float)),
            ("const_bool".into(), FieldType::Base(BaseType::Bool)),
            ("sip".into(), FieldType::Base(BaseType::Chars)),
            ("fired_at".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );
    let events = sample_events();
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_000_000_000;

    // Record path.
    let mut via_records = AlertColumnBuilder::new(Arc::from("alerts"));
    for (i, ev) in events.iter().enumerate() {
        let record = exec
            .execute_each_with_joins(ev, NANOS + i as i64, &lookup, &[], NANOS)
            .unwrap()
            .unwrap();
        via_records.append_record(&record).unwrap();
    }

    // Batched path.
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_batch = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &rows,
        &lookup,
        &[],
        NANOS,
        &mut via_batch,
        &mut appended_idx,
    );
    assert_eq!(stats.appended, events.len());
    assert_eq!(stats.failed, 0);
    assert_batches_equal_rows(&via_records.finish(), &via_batch.finish());
}

#[test]
fn execute_each_direct_batch_mid_batch_failure_skips_only_that_row() {
    // Row 2's sip is a non-empty string against a Float yield → conversion
    // error; rows 1/3 lack sip entirely → optional omission. The batch must
    // append 2 rows, fail 1, and match the per-event loop exactly.
    let mut plan = simple_rule_plan(
        "mixed_rule",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "auction_id".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "auction_id".into())),
        },
        YieldField {
            name: "sip_f".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("auction_id".into(), FieldType::Base(BaseType::Float)),
            ("sip_f".into(), FieldType::Base(BaseType::Float)),
        ]),
    );
    let events = [
        event(vec![("auction_id", num(1.0))]),
        event(vec![("sip", str_val("10.0.0.2")), ("auction_id", num(2.0))]),
        event(vec![("auction_id", num(3.0))]),
    ];
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_000_000_000;

    // Per-event path: row 2 errors, rows 1/3 append.
    let mut via_per_event = AlertColumnBuilder::new(Arc::from("alerts"));
    for (i, ev) in events.iter().enumerate() {
        let _ = exec.execute_each_direct(
            ev,
            NANOS + i as i64,
            &lookup,
            &[],
            NANOS,
            &mut via_per_event,
        );
    }

    // Batched path.
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut via_batch = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &rows,
        &lookup,
        &[],
        NANOS,
        &mut via_batch,
        &mut appended_idx,
    );
    assert_eq!(stats.appended, 2, "rows 1 and 3 append");
    assert_eq!(stats.failed, 1, "row 2 conversion error");
    assert_eq!(appended_idx, vec![0, 2]);
    assert_batches_equal_rows(&via_per_event.finish(), &via_batch.finish());
}

#[test]
fn execute_each_direct_batch_filter_rejections_match_per_event_path() {
    // Where-filter rejects must be counted as rejected and produce no rows —
    // identical to the per-event path.
    let mut plan = simple_rule_plan(
        "filtered_batch",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::BinOp {
            op: wf_lang::ast::BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        }),
    });
    let exec = RuleExecutor::new(plan);
    let events = [
        event(vec![("sip", str_val("10.0.0.1")), ("auction_id", num(1.0))]),
        event(vec![("sip", str_val("10.9.9.9")), ("auction_id", num(2.0))]),
        event(vec![("sip", str_val("10.0.0.1")), ("auction_id", num(3.0))]),
    ];
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_000_000_000;

    let rows: Vec<(&Event, i64)> = events.iter().map(|ev| (ev, NANOS)).collect();
    let mut via_batch = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_idx = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &rows,
        &lookup,
        &[],
        NANOS,
        &mut via_batch,
        &mut appended_idx,
    );
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.rejected, 1);
    assert_eq!(appended_idx, vec![0, 2]);
    assert_eq!(via_batch.len(), 2);
}
