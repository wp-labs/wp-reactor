//! coverage_extra 拆出的兄弟子模块（2026-09-04）：`close_exec.rs` 与 `deferred_exec.rs`——
//! close 模式 / 拒路 / step-stage 注解 / columnar-safe gate / direct-batch 直写与失败跳过，
//! deferred join reduce 变体 / error / empty / missing 路径，以及 P4 gap-1 列式挂起对拍
//! （投影遮蔽、let 回退物化、alert origin / yield-meta 构建）。共享 harness 在父模块
//! `coverage_extra.rs`，此处经 `use super::*` 复用。

use super::*;

use crate::alert::AlertColumnBuilder;
use crate::match_engine::cep::BindData;
use crate::match_engine::{
    DeferredLeft, DeferredPending, FieldSource, batch_to_events, batch_to_events_filtered,
    build_field_index,
};
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{
    BinOp, Bound, BoundVal, JoinMode, PathSegment, ReduceClause, ReduceMeasure, TieSpec, WithinSpec,
};
use wf_lang::plan::{JoinCondPlan, JoinPlan, LetPlan};

// ---------------------------------------------------------------------------
// close_exec.rs — close paths
// ---------------------------------------------------------------------------

#[test]
fn execute_close_or_mode_empty_close_steps_not_qualified() {
    // OR mode with no close steps must not produce an alert (event path owns it).
    let plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("x", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan);
    let close = close_output(true, true, CloseMode::Or, vec![], vec![]);
    assert!(exec.execute_close(&close).unwrap().is_none());
    assert!(
        exec.execute_close_with_joins(&close, &EmptyLookup)
            .unwrap()
            .is_none()
    );
    // OR mode WITH close steps qualifies.
    let close = close_output(
        true,
        true,
        CloseMode::Or,
        vec![],
        vec![step_data(Some("c"), 1.0, EngineHashMap::default())],
    );
    assert!(exec.execute_close(&close).unwrap().is_some());
}

#[test]
fn execute_close_with_joins_rejections() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("x", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.joins = vec![JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("sip".into()),
            right: FieldRef::Qualified("w".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    let exec = RuleExecutor::new(plan);
    let mut matched_row_fields = EngineHashMap::default();
    matched_row_fields.insert("id".into(), str_val("10.0.0.1"));
    matched_row_fields.insert("amt".into(), num(5.0));
    let lookup = RowsLookup::new(vec![JoinRow::Event(Arc::new(Event {
        fields: matched_row_fields,
    }))]);
    let mut close = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("x"), 1.0, EngineHashMap::default())],
        vec![],
    );
    // Join miss → suppressed.
    close.scope_key = vec![str_val("10.9.9.9")];
    assert!(
        exec.execute_close_with_joins(&close, &lookup)
            .unwrap()
            .is_none()
    );
    // Join hit → output.
    close.scope_key = vec![str_val("10.0.0.1")];
    assert!(
        exec.execute_close_with_joins(&close, &lookup)
            .unwrap()
            .is_some()
    );

    // Post-join where rejection.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("x", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.r#where = Some(Expr::Bool(false));
    let exec = RuleExecutor::new(plan);
    let close = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("x"), 1.0, EngineHashMap::default())],
        vec![],
    );
    assert!(
        exec.execute_close_with_joins(&close, &EmptyLookup)
            .unwrap()
            .is_none()
    );
}

#[test]
fn execute_close_annotates_step_stages_and_yields() {
    // A rule with one event step + one close step; the close ctx must carry
    // `_step_0_stage = event` / `_step_1_stage = close` and yields can read
    // them (drives `annotate_close_step_stages` and the general yield path).
    let match_plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("e1", count_ge(1.0))])],
        vec![step(vec![branch("c1", count_ge(1.0))])],
    );
    let mut plan = simple_rule_plan(
        "r1",
        match_plan,
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "stage0".into(),
            value: Expr::Field(FieldRef::Simple("_step_0_stage".into())),
        },
        YieldField {
            name: "stage1".into(),
            value: Expr::Field(FieldRef::Simple("_step_1_stage".into())),
        },
        // Function call in a yield forces the `CloseCtxFields::All` build.
        YieldField {
            name: "upper_sip".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "upper".into(),
                args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
            },
        },
    ];
    let exec = RuleExecutor::new(plan);
    let mut fv = EngineHashMap::default();
    fv.insert("price".into(), vec![num(1.0), num(2.0)]);
    let close = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("e1"), 2.0, fv.clone())],
        vec![step_data(Some("c1"), 3.0, EngineHashMap::default())],
    );
    let rec = exec.execute_close(&close).unwrap().unwrap();
    let get = |name: &str| {
        rec.yield_fields
            .iter()
            .find(|(n, _)| &**n == name)
            .map(|(_, v)| v.clone())
            .unwrap()
    };
    assert_eq!(get("stage0"), str_val("event"));
    assert_eq!(get("stage1"), str_val("close"));
    assert_eq!(get("upper_sip"), str_val("10.0.0.1"));

    // Missing `_step_*` key in the ctx → the stage annotator runs over nothing.
    let close = close_output(true, true, CloseMode::And, vec![], vec![]);
    assert!(exec.execute_close(&close).unwrap().is_some());
}

#[test]
fn close_plan_columnar_safe_gate_branches() {
    let base = || {
        simple_rule_plan(
            "r1",
            plan_with_close(
                vec![simple_key("sip")],
                vec![],
                vec![step(vec![branch("c1", count_ge(1.0))])],
            ),
            Expr::Number(70.0),
            "ip",
            Expr::Field(FieldRef::Simple("sip".into())),
        )
    };

    // Baseline shape is safe.
    assert!(RuleExecutor::new(base()).close_plan_columnar_safe());

    // Non-constant score → false.
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::Field(FieldRef::Simple("sip".into())),
    };
    assert!(!RuleExecutor::new(plan).close_plan_columnar_safe());

    // Entity StringLit is fine; Path / synthetic / general → false.
    let mut plan = base();
    plan.entity_plan.entity_id_expr = Expr::StringLit("const".into());
    assert!(RuleExecutor::new(plan).close_plan_columnar_safe());
    let mut plan = base();
    plan.entity_plan.entity_id_expr = Expr::Field(FieldRef::Path {
        alias: "b".into(),
        segments: vec![PathSegment::Field("obj".into())],
    });
    assert!(!RuleExecutor::new(plan).close_plan_columnar_safe());
    let mut plan = base();
    plan.entity_plan.entity_id_expr = Expr::Field(FieldRef::Simple("_step_0_measure".into()));
    assert!(!RuleExecutor::new(plan).close_plan_columnar_safe());
    let mut plan = base();
    plan.entity_plan.entity_id_expr = Expr::BinOp {
        op: BinOp::Add,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::Number(1.0)),
    };
    assert!(!RuleExecutor::new(plan).close_plan_columnar_safe());

    // Yields: literal + flat field ok; Path / synthetic / general → false.
    let mut plan = base();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "a".into(),
            value: Expr::Number(1.0),
        },
        YieldField {
            name: "b".into(),
            value: Expr::Field(FieldRef::Simple("sip".into())),
        },
    ];
    assert!(RuleExecutor::new(plan).close_plan_columnar_safe());
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::Field(FieldRef::Path {
            alias: "b".into(),
            segments: vec![PathSegment::Field("obj".into())],
        }),
    }];
    assert!(!RuleExecutor::new(plan).close_plan_columnar_safe());
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::Field(FieldRef::Simple("_bind_x_count".into())),
    }];
    assert!(!RuleExecutor::new(plan).close_plan_columnar_safe());
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "upper".into(),
            args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
        },
    }];
    // General yield（fmt/strftime/count_char 等）只引用普通字段 → 允许
    // （2026-08-25 扩展: 列式 close 对 General 走轻量 ctx 求值）。
    assert!(RuleExecutor::new(plan).close_plan_columnar_safe());
    // General 引用合成字段（`_bind_*`/`_step_*`, Named 窄化不注入）→ 拒绝。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "upper".into(),
            args: vec![Expr::Field(FieldRef::Simple("_bind_x_count".into()))],
        },
    }];
    assert!(!RuleExecutor::new(plan).close_plan_columnar_safe());

    // Joins present → false.
    let mut plan = base();
    plan.joins = vec![JoinPlan {
        right_window: "w".into(),
        mode: JoinMode::Inner,
        conds: vec![],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    assert!(!RuleExecutor::new(plan).close_plan_columnar_safe());
}

#[test]
fn close_direct_batch_columnar_paths() {
    // Entity const + one field yield resolving from: keys → step label →
    // field_values → bind data; unqualified closes are rejected; coerce
    // failures count as failed.
    let mut plan = simple_rule_plan(
        "r1",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("e1", count_ge(1.0))])],
            vec![step(vec![branch("c1", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::StringLit("const-entity".into()),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "k".into(),
            value: Expr::Field(FieldRef::Simple("sip".into())),
        },
        YieldField {
            name: "l".into(),
            value: Expr::Field(FieldRef::Simple("e1".into())),
        },
        YieldField {
            name: "fv".into(),
            value: Expr::Field(FieldRef::Simple("price".into())),
        },
        YieldField {
            name: "lit".into(),
            value: Expr::Number(9.0),
        },
    ];
    let mut plan = plan;
    // bind data provides `bv`.
    plan.yield_plan.fields.push(YieldField {
        name: "bv".into(),
        value: Expr::Field(FieldRef::Simple("amount".into())),
    });
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("fv".into(), FieldType::Base(BaseType::Float)),
            ("bv".into(), FieldType::Base(BaseType::Float)),
        ]),
    );
    assert!(exec.close_plan_columnar_safe());

    let mut fv = EngineHashMap::default();
    fv.insert("price".into(), vec![num(1.0), num(2.0)]);
    let mut close = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("e1"), 2.0, fv)],
        vec![step_data(Some("c1"), 3.0, EngineHashMap::default())],
    );
    close.bind_data = vec![BindData {
        alias: "w".into(),
        count: 2,
        field_values: EngineHashMap::from_iter([(
            "amount".to_string(),
            vec![num(10.0), num(20.0)],
        )]),
    }];
    let qualified = close.clone();

    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[qualified], &mut builder, 0);
    assert_eq!(stats.appended, 1);
    assert_eq!(stats.rejected, 0);
    assert_eq!(stats.failed, 0);
    assert_eq!(builder.len(), 1);

    // Unqualified close (not event_ok/close_ok) → rejected, nothing appended.
    let mut bad = close.clone();
    bad.close_ok = false;
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[bad], &mut builder, 0);
    assert_eq!(stats.rejected, 1);
    assert_eq!(stats.appended, 0);
    assert!(builder.is_empty());

    // Empty closes slice → no commit, empty stats.
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[], &mut builder, 0);
    assert_eq!(stats, Default::default());

    // Coerce failure (string "10.0.0.1" against a Float yield) → failed.
    let mut plan = simple_rule_plan(
        "r1",
        plan_with_close(
            vec![simple_key("sip")],
            vec![],
            vec![step(vec![branch("c1", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::StringLit("const".into()),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "f".into(),
        value: Expr::Field(FieldRef::Simple("sip".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("f".into(), FieldType::Base(BaseType::Float))]),
    );
    assert!(exec.close_plan_columnar_safe());
    let close = close_output(
        true,
        true,
        CloseMode::And,
        vec![],
        vec![step_data(Some("c1"), 1.0, EngineHashMap::default())],
    );
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats =
        exec.execute_close_direct_batch_columnar(std::slice::from_ref(&close), &mut builder, 0);
    // Per-row coerce failure: counted as failed and the row is **skipped**
    // (no columns touched, not appended) — matches the on-each batch path
    // contract (B1 fix).
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.appended, 0);
    assert!(builder.is_empty());
    // The per-row coerce failure path (non-literal value) is hit above; also
    // exercise the literal-coerce failure path (NaN against Float).
    let mut plan = simple_rule_plan(
        "r1",
        plan_with_close(
            vec![simple_key("sip")],
            vec![],
            vec![step(vec![branch("c1", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::StringLit("const".into()),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "nan".into(),
        value: Expr::Number(f64::NAN),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("nan".into(), FieldType::Base(BaseType::Float))]),
    );
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[close], &mut builder, 0);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.appended, 0);

    // Reserved-prefix yield name → register error on the const lane → failed.
    let mut plan = simple_rule_plan(
        "r1",
        plan_with_close(
            vec![simple_key("sip")],
            vec![],
            vec![step(vec![branch("c1", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::StringLit("const".into()),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "__wfu_evil".into(),
        value: Expr::Number(1.0),
    }];
    let exec = RuleExecutor::new(plan);
    let close = close_output(
        true,
        true,
        CloseMode::And,
        vec![],
        vec![step_data(Some("c1"), 1.0, EngineHashMap::default())],
    );
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[close], &mut builder, 0);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.appended, 0);

    // Missing entity field on the close → empty entity_id, row still appended.
    let plan = simple_rule_plan(
        "r1",
        plan_with_close(
            vec![simple_key("sip")],
            vec![],
            vec![step(vec![branch("c1", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("absent".into())),
    );
    let exec = RuleExecutor::new(plan);
    let mut close = close_output(
        true,
        true,
        CloseMode::And,
        vec![],
        vec![step_data(Some("c1"), 1.0, EngineHashMap::default())],
    );
    close.scope_key = vec![str_val("10.0.0.1")];
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[close], &mut builder, 0);
    assert_eq!(stats.appended, 1);
}

#[test]
fn close_direct_batch_columnar_skips_failed_row_keeps_rest() {
    // B1 回归: 列式 close 中一行 coerce/export 失败（`failed += 1`）必须**跳过
    // 该行**（不提交、不计 appended）——与 on-each 批量路径契约一致。此前
    // `break` 只退出 yield 字段循环, 失败行仍被 push 提交（appended 也 +1）。
    // 本测试用「两行 close: 第一行失败、第二行正常」验证行隔离与数组对齐。
    let mut plan = simple_rule_plan(
        "r1",
        plan_with_close(
            vec![simple_key("sip")],
            vec![],
            vec![step(vec![branch("c1", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::StringLit("const".into()),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "f".into(),
        value: Expr::Field(FieldRef::Simple("sip".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("f".into(), FieldType::Base(BaseType::Float))]),
    );
    assert!(exec.close_plan_columnar_safe());

    let close_ok = |scope: &str| {
        let mut c = close_output(
            true,
            true,
            CloseMode::And,
            vec![],
            vec![step_data(Some("c1"), 1.0, EngineHashMap::default())],
        );
        c.scope_key = vec![str_val(scope)];
        c
    };
    // 第一行: sip 是字符串 "10.0.0.1" 而目标类型是 Float → coerce 失败。
    // 第二行: 同样失败。
    let failing_a = close_ok("10.0.0.1");
    let failing_b = close_ok("10.9.9.9");
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats = exec.execute_close_direct_batch_columnar(
        &[failing_a.clone(), failing_b.clone()],
        &mut builder,
        0,
    );
    assert_eq!(stats.failed, 2, "两行都失败");
    assert_eq!(stats.appended, 0, "失败行不提交");
    assert_eq!(stats.rejected, 0);
    assert!(builder.is_empty(), "无任何列被触碰");

    // 混合: 一行失败 + 一行成功（sip 数字可强转）——验证行隔离与对齐。
    // 成功行需要 sip 为可强转浮点的值: 清空 scope_key（keys 不命中）后用
    // close step 的 field_values 注入数值 sip。
    let mut fv = EngineHashMap::default();
    fv.insert("sip".into(), vec![num(7.0)]);
    let mut ok_close = close_output(
        true,
        true,
        CloseMode::And,
        vec![],
        vec![step_data(Some("c1"), 1.0, fv)],
    );
    ok_close.scope_key = vec![]; // keys 不命中 → 回退 field_values
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats =
        exec.execute_close_direct_batch_columnar(&[failing_a.clone(), ok_close], &mut builder, 0);
    assert_eq!(stats.failed, 1, "仅失败行计入 failed");
    assert_eq!(stats.appended, 1, "成功行正常提交");
    assert_eq!(builder.len(), 1, "批次只含成功行, 列保持对齐");
    let batch = builder.finish();
    let records: Vec<_> = batch.iter_data_records().collect();
    assert_eq!(records.len(), 1);
    let record = records[0].as_ref().expect("record");
    assert_eq!(
        record.get_value("f"),
        Some(&wp_model_core::model::Value::from(7.0_f64))
    );
}

// ---------------------------------------------------------------------------
// deferred_exec.rs — deferred joins
// ---------------------------------------------------------------------------

const T: i64 = 1_700_000_000_000_000_000;

fn within_expires() -> WithinSpec {
    WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Expr(Expr::Field(FieldRef::Qualified(
                "a".into(),
                "dateTime".into(),
            ))),
        },
        hi: Bound {
            open: false,
            val: BoundVal::Expr(Expr::Field(FieldRef::Qualified(
                "a".into(),
                "expires".into(),
            ))),
        },
    }
}

fn deferred_join_plan(reduce: Option<ReduceClause>) -> RulePlan {
    let mut plan = simple_rule_plan(
        "q9_deferred",
        simple_plan(vec![], vec![]),
        Expr::Number(30.0),
        "digit",
        Expr::Field(FieldRef::Simple("id".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "a".into(),
        filter: None,
    });
    plan.joins = vec![JoinPlan {
        right_window: "bid_events".to_string(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("a".into(), "id".into()),
            right: FieldRef::Qualified("bid_events".into(), "auction".into()),
        }],
        within: Some(within_expires()),
        reduce,
        emit_at: Some(Expr::Field(FieldRef::Qualified(
            "a".into(),
            "expires".into(),
        ))),
    }];
    plan
}

fn bid(ts: i64, auction: f64, bidder: f64, price: f64) -> (i64, JoinRow) {
    let mut fields = EngineHashMap::default();
    fields.insert("auction".into(), num(auction));
    fields.insert("bidder".into(), num(bidder));
    fields.insert("price".into(), num(price));
    fields.insert("dateTime".into(), num(ts as f64));
    (ts, JoinRow::Event(Arc::new(Event { fields })))
}

fn auction_event() -> Event {
    event(vec![
        ("id", num(5.0)),
        ("dateTime", num(T as f64)),
        ("expires", num((T + 60_000_000_000) as f64)),
    ])
}

#[test]
fn deferred_pending_for_error_paths() {
    let exec = RuleExecutor::new(deferred_join_plan(None));

    // Join index out of range → None.
    assert!(
        exec.deferred_pending_for(1, &DeferredLeft::Event(auction_event()), T)
            .is_none()
    );

    // Missing key field → None.
    let ev = event(vec![
        ("dateTime", num(T as f64)),
        ("expires", num((T + 60_000_000_000) as f64)),
    ]);
    assert!(
        exec.deferred_pending_for(0, &DeferredLeft::Event(ev), T)
            .is_none()
    );

    // Missing bound field → None.
    let ev = event(vec![
        ("id", num(5.0)),
        ("expires", num((T + 60_000_000_000) as f64)),
    ]);
    assert!(
        exec.deferred_pending_for(0, &DeferredLeft::Event(ev), T)
            .is_none()
    );

    // Non-numeric expiry → None.
    let ev = event(vec![
        ("id", num(5.0)),
        ("dateTime", num(T as f64)),
        ("expires", str_val("soon")),
    ]);
    assert!(
        exec.deferred_pending_for(0, &DeferredLeft::Event(ev), T)
            .is_none()
    );

    // No emit_at on the join → None.
    let mut plan = deferred_join_plan(None);
    plan.joins[0].emit_at = None;
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.deferred_pending_for(0, &DeferredLeft::Event(auction_event()), T)
            .is_none()
    );

    // No within on the join → None.
    let mut plan = deferred_join_plan(None);
    plan.joins[0].within = None;
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.deferred_pending_for(0, &DeferredLeft::Event(auction_event()), T)
            .is_none()
    );

    // Happy path (with lets injected).
    let mut plan = deferred_join_plan(None);
    plan.lets = vec![LetPlan {
        name: "bound_hint".into(),
        expr: Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::Field(FieldRef::Simple("id".into()))),
            right: Box::new(Expr::Number(1.0)),
        },
    }];
    let exec = RuleExecutor::new(plan);
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(auction_event()), T)
        .unwrap();
    assert_eq!(pending.key_field, "auction");
    assert_eq!(pending.key, num(5.0));
    assert_eq!(pending.lo_ns, T);
    assert_eq!(pending.hi_ns, T + 60_000_000_000);
    assert_eq!(pending.expiry_nanos, T + 60_000_000_000);
    assert!(!pending.lo_open && !pending.hi_open);
}

#[test]
fn execute_deferred_join_reduce_variants() {
    // maxrow with tie desc + label injection.
    let plan = deferred_join_plan(Some(ReduceClause {
        measure: ReduceMeasure::Maxrow {
            field: FieldRef::Simple("price".into()),
            tie: Some(TieSpec {
                field: FieldRef::Simple("dateTime".into()),
                desc: true,
            }),
        },
        label: Some("winner".into()),
    }));
    let mut plan = plan;
    plan.yield_plan.fields = vec![
        YieldField {
            name: "winner_bidder".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "winner".into(),
                segments: vec![PathSegment::Field("bidder".into())],
            }),
        },
        YieldField {
            name: "winner_price".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "winner".into(),
                segments: vec![PathSegment::Field("price".into())],
            }),
        },
    ];
    let exec = RuleExecutor::new(plan);
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(auction_event()), T)
        .unwrap();
    // Same price, tie desc → latest dateTime wins (bidder=3).
    let lookup = RowsLookup::with_ts(vec![
        bid(T + 10_000_000_000, 5.0, 1.0, 200.0),
        bid(T + 20_000_000_000, 5.0, 2.0, 200.0),
        bid(T + 30_000_000_000, 5.0, 3.0, 200.0),
        bid(T + 40_000_000_000, 9.0, 4.0, 999.0),
    ]);
    let rec = exec
        .execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("deferred output");
    let get = |name: &str| {
        rec.yield_fields
            .iter()
            .find(|(n, _)| &**n == name)
            .map(|(_, v)| v.clone())
            .unwrap()
    };
    assert_eq!(get("winner_bidder"), num(3.0));
    assert_eq!(get("winner_price"), num(200.0));

    // minrow with tie asc.
    let plan = deferred_join_plan(Some(ReduceClause {
        measure: ReduceMeasure::Minrow {
            field: FieldRef::Simple("price".into()),
            tie: Some(TieSpec {
                field: FieldRef::Simple("dateTime".into()),
                desc: false,
            }),
        },
        label: None,
    }));
    let exec = RuleExecutor::new(plan);
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(auction_event()), T)
        .unwrap();
    let lookup = RowsLookup::with_ts(vec![
        bid(T + 10_000_000_000, 5.0, 1.0, 100.0),
        bid(T + 20_000_000_000, 5.0, 2.0, 100.0),
        bid(T + 30_000_000_000, 5.0, 3.0, 50.0),
    ]);
    // min price = 50 (bidder 3).
    let rec = exec
        .execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("deferred output");
    assert_eq!(rec.origin.as_str(), "deferred");

    // minrow tie: both price=100 → tie asc picks bidder 1 (earliest dateTime).
    let mut plan = deferred_join_plan(Some(ReduceClause {
        measure: ReduceMeasure::Minrow {
            field: FieldRef::Simple("price".into()),
            tie: Some(TieSpec {
                field: FieldRef::Simple("dateTime".into()),
                desc: false,
            }),
        },
        label: None,
    }));
    plan.yield_plan.fields = vec![YieldField {
        name: "bidder".into(),
        value: Expr::Field(FieldRef::Simple("bidder".into())),
    }];
    let exec = RuleExecutor::new(plan);
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(auction_event()), T)
        .unwrap();
    let lookup = RowsLookup::with_ts(vec![
        bid(T + 10_000_000_000, 5.0, 1.0, 100.0),
        bid(T + 20_000_000_000, 5.0, 2.0, 100.0),
    ]);
    let rec = exec
        .execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("deferred output");
    let get = |name: &str| {
        rec.yield_fields
            .iter()
            .find(|(n, _)| &**n == name)
            .map(|(_, v)| v.clone())
            .unwrap()
    };
    assert_eq!(get("bidder"), num(1.0));

    // last: latest ts wins.
    let plan = deferred_join_plan(Some(ReduceClause {
        measure: ReduceMeasure::Last {
            field: FieldRef::Simple("price".into()),
        },
        label: None,
    }));
    let mut plan = plan;
    plan.yield_plan.fields = vec![YieldField {
        name: "price".into(),
        value: Expr::Field(FieldRef::Simple("price".into())),
    }];
    let exec = RuleExecutor::new(plan);
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(auction_event()), T)
        .unwrap();
    let lookup = RowsLookup::with_ts(vec![
        bid(T + 10_000_000_000, 5.0, 1.0, 100.0),
        bid(T + 20_000_000_000, 5.0, 2.0, 200.0),
    ]);
    let rec = exec
        .execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("deferred output");
    let get = |name: &str| {
        rec.yield_fields
            .iter()
            .find(|(n, _)| &**n == name)
            .map(|(_, v)| v.clone())
            .unwrap()
    };
    assert_eq!(get("price"), num(200.0));

    // top: desc order, truncation to N.
    let plan = deferred_join_plan(Some(ReduceClause {
        measure: ReduceMeasure::Top {
            n: 1,
            field: FieldRef::Simple("price".into()),
        },
        label: None,
    }));
    let mut plan = plan;
    plan.yield_plan.fields = vec![YieldField {
        name: "price".into(),
        value: Expr::Field(FieldRef::Simple("price".into())),
    }];
    let exec = RuleExecutor::new(plan);
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(auction_event()), T)
        .unwrap();
    let lookup = RowsLookup::with_ts(vec![
        bid(T + 10_000_000_000, 5.0, 1.0, 100.0),
        bid(T + 20_000_000_000, 5.0, 2.0, 300.0),
        bid(T + 30_000_000_000, 5.0, 3.0, 200.0),
    ]);
    let rec = exec
        .execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("deferred output");
    let get = |name: &str| {
        rec.yield_fields
            .iter()
            .find(|(n, _)| &**n == name)
            .map(|(_, v)| v.clone())
            .unwrap()
    };
    assert_eq!(get("price"), num(300.0));

    // reduce with a missing field in all rows → the row comparator treats
    // every pair as equal, so a deterministic row is still selected (never a
    // hard failure).
    let plan = deferred_join_plan(Some(ReduceClause {
        measure: ReduceMeasure::Maxrow {
            field: FieldRef::Simple("nope".into()),
            tie: None,
        },
        label: None,
    }));
    let exec = RuleExecutor::new(plan);
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(auction_event()), T)
        .unwrap();
    let lookup = RowsLookup::with_ts(vec![bid(T + 10_000_000_000, 5.0, 1.0, 100.0)]);
    assert!(
        exec.execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
            .unwrap()
            .is_some()
    );
}

#[test]
fn execute_deferred_join_empty_and_missing_paths() {
    // No join at index → Ok(None).
    let exec = RuleExecutor::new(deferred_join_plan(None));
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(auction_event()), T)
        .unwrap();
    assert!(
        exec.execute_deferred_join(7, &pending, &EmptyLookup, T)
            .unwrap()
            .is_none()
    );

    // No candidates → Ok(None).
    assert!(
        exec.execute_deferred_join(0, &pending, &EmptyLookup, T)
            .unwrap()
            .is_none()
    );

    // Candidates outside the interval → Ok(None).
    let lookup = RowsLookup::with_ts(vec![
        bid(T - 100_000_000_000, 5.0, 1.0, 100.0),
        bid(T + 100_000_000_000, 5.0, 2.0, 200.0),
    ]);
    assert!(
        exec.execute_deferred_join(0, &pending, &lookup, T)
            .unwrap()
            .is_none()
    );

    // Post-join where rejection.
    let mut plan = deferred_join_plan(None);
    plan.r#where = Some(Expr::Bool(false));
    let exec = RuleExecutor::new(plan);
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(auction_event()), T)
        .unwrap();
    let lookup = RowsLookup::with_ts(vec![bid(T + 10_000_000_000, 5.0, 1.0, 100.0)]);
    assert!(
        exec.execute_deferred_join(0, &pending, &lookup, T)
            .unwrap()
            .is_none()
    );

    // Pure existence (reduce None): earliest row enriches; output has
    // `origin=deferred` and `fired_at` = the pending expiry.
    let exec = RuleExecutor::new(deferred_join_plan(None));
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(auction_event()), T)
        .unwrap();
    let lookup = RowsLookup::with_ts(vec![
        bid(T + 10_000_000_000, 5.0, 1.0, 100.0),
        bid(T + 20_000_000_000, 5.0, 2.0, 200.0),
    ]);
    let rec = exec
        .execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("deferred output");
    assert_eq!(rec.origin.as_str(), "deferred");
    // fired_at = expiry (T+60s in ms → formatted).
    assert_eq!(&*rec.fired_at, "2023-11-14T22:14:20.000Z");
}

// ---------------------------------------------------------------------------
// P4 gap-1（2026-09-02）：deferred 驱动列式挂起——列式视图与 eager Event
// 对拍（oracle identical），投影遮蔽语义，let 回退物化。
// ---------------------------------------------------------------------------

/// 构造 2 行 auction 驱动批：id / dateTime / expires / category（均 i64）。
/// 经 `extract_field_value` → Value::Number(f64)，与 `batch_to_events` 同转换。
fn deferred_auction_batch() -> RecordBatch {
    use arrow::array::Int64Array;
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        ArrowField::new("dateTime", DataType::Int64, false),
        ArrowField::new("expires", DataType::Int64, false),
        ArrowField::new("category", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![5, 9])),
            Arc::new(Int64Array::from(vec![T, T + 10_000_000_000])),
            Arc::new(Int64Array::from(vec![
                T + 60_000_000_000,
                T + 70_000_000_000,
            ])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap()
}

/// 断言两个 pending 的全部标量字段一致（key/界/触发点/开闭）。
fn assert_pending_eq(a: &DeferredPending, b: &DeferredPending) {
    assert_eq!(a.key_field, b.key_field);
    assert_eq!(a.key, b.key);
    assert_eq!(a.lo_ns, b.lo_ns);
    assert_eq!(a.hi_ns, b.hi_ns);
    assert_eq!(a.lo_open, b.lo_open);
    assert_eq!(a.hi_open, b.hi_open);
    assert_eq!(a.expiry_nanos, b.expiry_nanos);
}

#[test]
fn deferred_pending_columnar_matches_eager() {
    let exec = RuleExecutor::new(deferred_join_plan(None));
    let batch = deferred_auction_batch();
    let index = build_field_index(&batch);
    let eager_events = batch_to_events(&batch);

    for (row, eager) in eager_events.iter().enumerate() {
        // 列式视图（无投影）挂起 == eager Event 挂起（标量字段 + 物化结果）。
        let left = DeferredLeft::Columnar(JoinRow::Columnar {
            batch: Arc::new(batch.clone()),
            row,
            index: Arc::clone(&index),
            projection: None,
        });
        let pending_col = exec.deferred_pending_for(0, &left, T).unwrap();
        let pending_eager = exec
            .deferred_pending_for(0, &DeferredLeft::Event(eager.clone()), T)
            .unwrap();
        assert_pending_eq(&pending_col, &pending_eager);
        // 无投影 to_event == batch_to_events 全列 Event（oracle identical）。
        assert_eq!(
            pending_col.left.to_event().fields,
            eager.fields,
            "row {row}: 列式 to_event 必须与 batch_to_events 字节一致"
        );
        // 全链路评估（Q9 纯存在 → 输出 ctx）字节一致。
        let lookup = RowsLookup::with_ts(vec![bid(T + 10_000_000_000, 5.0, 1.0, 100.0)]);
        let out_col = exec
            .evaluate_deferred_join(0, &pending_col, &lookup)
            .unwrap();
        let out_eager = exec
            .evaluate_deferred_join(0, &pending_eager, &lookup)
            .unwrap();
        assert_eq!(out_col, out_eager, "row {row}: 评估输出 ctx 字节一致");
    }
}

#[test]
fn deferred_columnar_projection_shadows_unprojected_fields() {
    // 构造冒烟（exec 本身不参与断言，只验证计划可建）。
    let _exec = RuleExecutor::new(deferred_join_plan(None));
    let batch = deferred_auction_batch();
    let index = build_field_index(&batch);
    let projection: HashSet<String> = ["id", "dateTime", "expires"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let proj = Arc::new(projection);
    // 投影 = 规则读集（缺 category）：与 `batch_to_events_filtered` 裁剪一致。
    let eager_filtered = batch_to_events_filtered(&batch, &proj);
    for (row, eager) in eager_filtered.iter().enumerate() {
        let left = DeferredLeft::Columnar(JoinRow::Columnar {
            batch: Arc::new(batch.clone()),
            row,
            index: Arc::clone(&index),
            projection: Some(Arc::clone(&proj)),
        });
        // 投影外字段读 None（对齐 eager 裁剪 Event——字段不在 map 里）。
        assert_eq!(
            left.field_value("category"),
            None,
            "row {row}: 投影外字段必须读 None"
        );
        assert_eq!(
            left.field_value("id"),
            eager.fields.get("id").cloned(),
            "row {row}: 投影内字段直读"
        );
        assert_eq!(
            left.to_event().fields,
            eager.fields,
            "row {row}: 投影 to_event == batch_to_events_filtered"
        );
    }
}

#[test]
fn deferred_execute_columnar_matches_eager_output() {
    // q9 形态：maxrow + label 注入 + winner 输出字段。
    let plan = deferred_join_plan(Some(ReduceClause {
        measure: ReduceMeasure::Maxrow {
            field: FieldRef::Simple("price".into()),
            tie: None,
        },
        label: Some("winner".into()),
    }));
    let mut plan = plan;
    plan.yield_plan.fields = vec![
        YieldField {
            name: "winner_bidder".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "winner".into(),
                segments: vec![PathSegment::Field("bidder".into())],
            }),
        },
        YieldField {
            name: "winner_price".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "winner".into(),
                segments: vec![PathSegment::Field("price".into())],
            }),
        },
    ];
    let exec = RuleExecutor::new(plan);
    let batch = deferred_auction_batch();
    let index = build_field_index(&batch);
    let eager = &batch_to_events(&batch)[0];
    let pending_col = exec
        .deferred_pending_for(
            0,
            &DeferredLeft::Columnar(JoinRow::Columnar {
                batch: Arc::new(batch.clone()),
                row: 0,
                index: Arc::clone(&index),
                projection: None,
            }),
            T,
        )
        .unwrap();
    let pending_eager = exec
        .deferred_pending_for(0, &DeferredLeft::Event(eager.clone()), T)
        .unwrap();
    let lookup = RowsLookup::with_ts(vec![
        bid(T + 10_000_000_000, 5.0, 1.0, 200.0),
        bid(T + 20_000_000_000, 5.0, 2.0, 400.0),
        bid(T + 30_000_000_000, 9.0, 3.0, 999.0), // 其他 auction
    ]);
    let out_col = exec
        .execute_deferred_join(0, &pending_col, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("columnar deferred output");
    let out_eager = exec
        .execute_deferred_join(0, &pending_eager, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("eager deferred output");
    let get = |rec: &crate::alert::OutputRecord, name: &str| {
        rec.yield_fields
            .iter()
            .find(|(n, _)| &**n == name)
            .map(|(_, v)| v.clone())
            .unwrap()
    };
    assert_eq!(out_col.origin, out_eager.origin);
    assert_eq!(out_col.fired_at, out_eager.fired_at);
    assert_eq!(
        get(&out_col, "winner_bidder"),
        get(&out_eager, "winner_bidder")
    );
    assert_eq!(
        get(&out_col, "winner_price"),
        get(&out_eager, "winner_price")
    );
    assert_eq!(get(&out_col, "winner_bidder"), num(2.0), "max price bidder");
    assert_eq!(get(&out_col, "winner_price"), num(400.0));
}

#[test]
fn deferred_columnar_multi_cond_recheck_reads_left_via_field_source() {
    // 多 join 条件（cond_recheck_redundant = false）：cond 复核经
    // `row_matches_conds(row, conds, &dyn FieldSource)` 读列式 left。
    let mut plan = deferred_join_plan(None);
    plan.joins[0].conds.push(JoinCondPlan {
        left: FieldRef::Qualified("a".into(), "category".into()),
        right: FieldRef::Qualified("bid_events".into(), "channel".into()),
    });
    let exec = RuleExecutor::new(plan);
    let batch = deferred_auction_batch();
    let index = build_field_index(&batch);
    let pending_col = exec
        .deferred_pending_for(
            0,
            &DeferredLeft::Columnar(JoinRow::Columnar {
                batch: Arc::new(batch.clone()),
                row: 0,
                index: Arc::clone(&index),
                projection: None,
            }),
            T,
        )
        .unwrap();
    // 第一个候选 channel 匹配（category=10）、第二个不匹配 → 只命中第一个。
    let lookup = RowsLookup::with_ts(vec![
        {
            let mut f = EngineHashMap::default();
            f.insert("auction".into(), num(5.0));
            f.insert("bidder".into(), num(1.0));
            f.insert("price".into(), num(100.0));
            f.insert("dateTime".into(), num((T + 10_000_000_000) as f64));
            f.insert("channel".into(), num(10.0));
            (
                T + 10_000_000_000,
                JoinRow::Event(Arc::new(Event { fields: f })),
            )
        },
        {
            let mut f = EngineHashMap::default();
            f.insert("auction".into(), num(5.0));
            f.insert("bidder".into(), num(2.0));
            f.insert("price".into(), num(200.0));
            f.insert("dateTime".into(), num((T + 20_000_000_000) as f64));
            f.insert("channel".into(), num(99.0));
            (
                T + 20_000_000_000,
                JoinRow::Event(Arc::new(Event { fields: f })),
            )
        },
    ]);
    let out = exec
        .execute_deferred_join(0, &pending_col, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("deferred output");
    // 纯存在：区间内最早命中行富化（第一个候选 channel 匹配、第二个被复核拒绝）。
    // 计划未声明 yield 字段（空字段集）——断言输出产生且 origin 正确。
    assert_eq!(out.origin.as_str(), "deferred");
    assert_eq!(out.yield_fields.len(), 0);
}

#[test]
fn deferred_pending_columnar_with_lets_materializes_once() {
    // 有 let 绑定的规则：列式 left 在挂起时物化 + apply_lets（回退语义），
    // 物化结果与 eager 路径字节一致。
    let mut plan = deferred_join_plan(None);
    plan.lets = vec![LetPlan {
        name: "bound_hint".into(),
        expr: Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::Field(FieldRef::Simple("id".into()))),
            right: Box::new(Expr::Number(1.0)),
        },
    }];
    let exec = RuleExecutor::new(plan);
    let batch = deferred_auction_batch();
    let index = build_field_index(&batch);
    let pending_col = exec
        .deferred_pending_for(
            0,
            &DeferredLeft::Columnar(JoinRow::Columnar {
                batch: Arc::new(batch.clone()),
                row: 0,
                index: Arc::clone(&index),
                projection: None,
            }),
            T,
        )
        .unwrap();
    // eager 基准 = 同批第 0 行（与列式视图同字段集，字段一致才可比）。
    let eager_row = batch_to_events(&batch)[0].clone();
    let pending_eager = exec
        .deferred_pending_for(0, &DeferredLeft::Event(eager_row), T)
        .unwrap();
    // 列式 + let → 回退物化为 Event（非列式视图）。
    assert!(matches!(pending_col.left, DeferredLeft::Event(_)));
    assert_eq!(
        pending_col.left.to_event().fields,
        pending_eager.left.to_event().fields,
        "let 回退物化必须与 eager 字节一致"
    );
    assert_eq!(
        pending_col.left.to_event().fields.get("bound_hint"),
        Some(&num(6.0)),
        "let 绑定注入（id=5 → 6）"
    );
}

#[test]
fn build_each_alert_with_custom_origin_and_yield_meta() {
    use crate::alert::AlertOrigin;

    let exec = each_plan_rule();
    let ev = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("auction_id", num(1.0)),
        ("price", num(2.0)),
    ]);
    let rec = exec
        .build_each_alert_with(
            &ev,
            123_456,
            AlertOrigin::Close {
                reason: CloseReason::Flush,
            },
            &[],
            789,
        )
        .unwrap()
        .unwrap();
    assert_eq!(rec.origin.as_str(), "close:flush");
    assert_eq!(rec.event_time_nanos, 123_456);
    assert_eq!(rec.yield_fields.len(), 2);
    // Machine id extraction from the event.
    assert_eq!(RuleExecutor::machine_id_of(&ev), "");
}
