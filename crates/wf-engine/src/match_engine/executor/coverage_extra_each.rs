//! coverage_extra 拆出的兄弟子模块（2026-09-04）：`each_exec.rs` on-each 行式路径——
//! `execute_each` 拒路 / lets + joins / direct-batch 拒路与求值失败、each plan / pipe 的
//! columnar-safe gate 分支、dead-join 消除后的 enrich 裁剪。共享 harness 在父模块
//! `coverage_extra.rs`，此处经 `use super::*` 复用。

use super::*;

use crate::alert::AlertColumnBuilder;
use wf_lang::ast::{
    BinOp, Bound, BoundVal, JoinMode, PathSegment, ReduceClause, ReduceMeasure, WithinSpec,
};
use wf_lang::plan::{JoinCondPlan, JoinPlan, LetPlan};

// ---------------------------------------------------------------------------
// each_exec.rs — on-each paths
// ---------------------------------------------------------------------------

#[test]
fn execute_each_non_each_error_and_filter_reject() {
    // execute_each on a non-`on each` rule errors.
    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.execute_each(&event(vec![("sip", str_val("x"))]), 0)
            .is_err()
    );
    assert!(
        exec.execute_each_with_joins(&event(vec![("sip", str_val("x"))]), 0, &EmptyLookup, &[], 0)
            .is_err()
    );

    // Filter rejection → Ok(None).
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        }),
    });
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.execute_each(&event(vec![("sip", str_val("1.1.1.1"))]), 0)
            .unwrap()
            .is_none()
    );
    // Filter on a missing field → None → rejected.
    assert!(exec.execute_each(&event(vec![]), 0).unwrap().is_none());
}

#[test]
fn execute_each_with_lets_and_joins_paths() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    // `let doubled = price * 2` — the lets path clones the event and injects.
    plan.lets = vec![LetPlan {
        name: "doubled".into(),
        expr: Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Field(FieldRef::Simple("price".into()))),
            right: Box::new(Expr::Number(2.0)),
        },
    }];
    plan.yield_plan.fields = vec![YieldField {
        name: "d".into(),
        value: Expr::Field(FieldRef::Simple("doubled".into())),
    }];
    let exec = RuleExecutor::new(plan);
    let ev = event(vec![("sip", str_val("10.0.0.1")), ("price", num(5.0))]);
    let rec = exec.execute_each(&ev, 0).unwrap().unwrap();
    assert_eq!(rec.yield_fields[0].1, num(10.0));

    // A let that fails to evaluate leaves no injected field → yield empty.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.lets = vec![LetPlan {
        name: "bad".into(),
        expr: Expr::Field(FieldRef::Simple("missing".into())),
    }];
    let exec = RuleExecutor::new(plan);
    let rec = exec
        .execute_each(&event(vec![("sip", str_val("x"))]), 0)
        .unwrap()
        .unwrap();
    assert!(rec.yield_fields.is_empty());

    // Join rejection on the with-joins path → Ok(None).
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
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
    let ev = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(
        exec.execute_each_with_joins(&ev, 0, &EmptyLookup, &[], 0)
            .unwrap()
            .is_none()
    );
    // Post-join where rejection.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    // A let forces the slow path so the post-join `where` is actually checked
    // (the no-joins/no-lets fast path skips `where_ok` entirely).
    plan.lets = vec![LetPlan {
        name: "x".into(),
        expr: Expr::Number(1.0),
    }];
    plan.r#where = Some(Expr::Bool(false));
    let exec = RuleExecutor::new(plan);
    let ev = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(
        exec.execute_each_with_joins(&ev, 0, &EmptyLookup, &[], 0)
            .unwrap()
            .is_none()
    );

    // No-joins-no-lets fast path returns a record.
    let exec = each_plan_rule();
    let ev = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("auction_id", num(1.0)),
        ("price", num(2.0)),
    ]);
    let rec = exec
        .execute_each_with_joins(&ev, 123, &EmptyLookup, &[], 456)
        .unwrap()
        .unwrap();
    assert_eq!(rec.yield_fields.len(), 2);
    assert_eq!(rec.event_time_nanos, 123);
}

#[test]
fn execute_each_direct_batch_non_each_and_rejections() {
    // Non-`on each` rule: all rows failed, nothing appended.
    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let ev = event(vec![("sip", str_val("x"))]);
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch(
        &[(&ev, 0)],
        &EmptyLookup,
        &[],
        0,
        &mut builder,
        &mut appended,
    );
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.appended, 0);
    assert!(appended.is_empty());

    // Filter rejection counted as rejected.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        }),
    });
    let exec = RuleExecutor::new(plan);
    let ev_ok = event(vec![("sip", str_val("10.0.0.1"))]);
    let ev_bad = event(vec![("sip", str_val("9.9.9.9"))]);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let rows = vec![(&ev_ok, 0i64), (&ev_bad, 1i64), (&ev_ok, 2i64)];
    let mut appended = Vec::new();
    let stats =
        exec.execute_each_direct_batch(&rows, &EmptyLookup, &[], 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.rejected, 1);
    assert_eq!(appended, vec![0, 2]);
    assert_eq!(builder.len(), 2);

    // Join rejection counted as rejected.
    let mut plan = simple_rule_plan(
        "r1",
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
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let rows = vec![(&ev_ok, 0i64)];
    let mut appended = Vec::new();
    let stats =
        exec.execute_each_direct_batch(&rows, &EmptyLookup, &[], 0, &mut builder, &mut appended);
    assert_eq!(stats.rejected, 1);
    assert_eq!(stats.appended, 0);

    // Post-join where rejection.
    let mut plan = simple_rule_plan(
        "r1",
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
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.lets = vec![LetPlan {
        name: "x".into(),
        expr: Expr::Number(1.0),
    }];
    plan.r#where = Some(Expr::Bool(false));
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let rows = vec![(&ev_ok, 0i64)];
    let mut appended = Vec::new();
    let stats =
        exec.execute_each_direct_batch(&rows, &EmptyLookup, &[], 0, &mut builder, &mut appended);
    assert_eq!(stats.rejected, 1);
    assert_eq!(stats.appended, 0);
}

#[test]
fn execute_each_direct_batch_eval_failures() {
    // General score expression that errors → row failed.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Field(FieldRef::Simple("missing".into())), // eval → None → error
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);
    let ev = event(vec![("sip", str_val("x"))]);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let rows = vec![(&ev, 0i64)];
    let mut appended = Vec::new();
    let stats =
        exec.execute_each_direct_batch(&rows, &EmptyLookup, &[], 0, &mut builder, &mut appended);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.appended, 0);

    // Non-const entity expression that errors → row failed.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "missing".into())), // absent → empty string, no error
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let rows = vec![(&ev, 0i64)];
    let mut appended = Vec::new();
    let stats =
        exec.execute_each_direct_batch(&rows, &EmptyLookup, &[], 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 1);
    assert_eq!(stats.failed, 0);
}

#[test]
fn each_plan_columnar_safe_gate_branches() {
    let base = || {
        let mut plan = simple_rule_plan(
            "r1",
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
        plan
    };

    // Baseline shape is safe.
    assert!(RuleExecutor::new(base()).each_plan_columnar_safe());

    // No each plan → false.
    let mut plan = base();
    plan.each_plan = None;
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // Lets（2026-08-25 层 2）：RHS 可列式编译 + 非 yield 表达式不引用 let →
    // 放行（q22 形态）；非列式 RHS / 引用 let 的 filter → 拒绝。
    let mut plan = base();
    plan.lets = vec![LetPlan {
        name: "x".into(),
        expr: Expr::Number(1.0),
    }];
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());
    let mut plan = base();
    plan.lets = vec![LetPlan {
        name: "x".into(),
        expr: Expr::FuncCall {
            qualifier: None,
            name: "bogus_fn".into(),
            args: vec![],
        },
    }];
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());
    let mut plan = base();
    plan.lets = vec![LetPlan {
        name: "x".into(),
        expr: Expr::Number(1.0),
    }];
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::Field(FieldRef::Simple("x".into()))),
    });
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());
    // gap-3（2026-09-02）：**where 引用 let 变量** → 拒绝（列式 where 掩码无
    // let 视图，引用会静默读空 → 失真；行式 where_ok 在 apply_lets 后生效）。
    let mut plan = base();
    plan.lets = vec![LetPlan {
        name: "x".into(),
        expr: Expr::Number(1.0),
    }];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("x".into()))),
        right: Box::new(Expr::Number(1.0)),
    });
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // Joins → false.
    let mut plan = base();
    plan.joins = vec![JoinPlan {
        right_window: "w".into(),
        mode: JoinMode::Inner,
        conds: vec![],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // 列式 each filter（Bool 字面量——`expr_is_columnar` 形状）→ 放行。
    let mut plan = base();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::Bool(true)),
    });
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());

    // 非列式 each filter（函数调用不在列式清单）→ false。
    let mut plan = base();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::FuncCall {
            qualifier: None,
            name: "upper".into(),
            args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
        }),
    });
    // gap-4（2026-09-02）：非列式 each filter → 放行（逐行解释回退）。
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());

    // 列式 each filter + 活 join → false（列式 join 富化路径未接 filter 求值）。
    let mut plan = base();
    plan.joins = vec![JoinPlan {
        right_window: "w".into(),
        mode: JoinMode::Inner,
        conds: vec![],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::Bool(true)),
    });
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // gap-4：非列式 bind filter → 放行（列式路径逐行 event_matches_alias）。
    let mut plan = base();
    plan.binds[0].filter = Some(Expr::FuncCall {
        qualifier: None,
        name: "upper".into(),
        args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
    });
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());

    // bind filter 引用 let 变量 → 仍拒绝（列式视图无 let 覆盖）。
    let mut plan = base();
    plan.lets = vec![LetPlan {
        name: "x".into(),
        expr: Expr::Number(1.0),
    }];
    plan.binds[0].filter = Some(Expr::Field(FieldRef::Simple("x".into())));
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // Non-constant score：裸 flat 字段（`score(b.price)`）→ 可列式（gap-6
    // 2026-09-02，一般列式表达式）→ safe。
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::Field(FieldRef::Simple("sip".into())),
    };
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());

    // BinOp score: 常量×字段（q1 `0.908 * b.price` 形态）→ safe（无 join）。
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Number(0.908)),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
        },
    };
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());

    // 字段×常量 → safe（顺序无关）。
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            right: Box::new(Expr::Number(0.908)),
        },
    };
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());

    // 其他 BinOp（Add，常量+flat 字段）→ 可列式（gap-6）→ safe。
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::Number(0.5)),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
        },
    };
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());

    // 字段×字段 → 可列式（gap-6）→ safe。
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            right: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        },
    };
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());

    // 常量×list-index 字段（`2.0 * e.tags[0]`）→ 可列式（gap-6 review
    // 2026-09-02：MulConst 快通道 value_at 只读 flat 列 → 归一般 cvec）。
    let list_index = || {
        Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![PathSegment::Field("tags".into()), PathSegment::Index(0)],
        })
    };
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Number(2.0)),
            right: Box::new(list_index()),
        },
    };
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());

    // 常量×深嵌套 Path（`2.0 * e.obj.x[0]`，非列式）→ false。
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Number(2.0)),
            right: Box::new(Expr::Field(FieldRef::Path {
                alias: "e".into(),
                segments: vec![
                    PathSegment::Field("obj".into()),
                    PathSegment::Field("x".into()),
                    PathSegment::Index(0),
                ],
            })),
        },
    };
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // 常量×字段 + 活 join → false（join 列式路径 score 仅允许常量）。
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Number(0.908)),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
        },
    };
    plan.joins = vec![JoinPlan {
        right_window: "w".into(),
        mode: JoinMode::Inner,
        conds: vec![],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // 常量×list-index + 活 join → false（list-index score 只无活 join 放行）。
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Number(2.0)),
            right: Box::new(list_index()),
        },
    };
    plan.joins = vec![JoinPlan {
        right_window: "w".into(),
        mode: JoinMode::Inner,
        conds: vec![],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // Entity = Path（单段 object 根，非列式）→ false；Add 表达式（flat 组件）
    // → 可列式（gap-7）→ true。
    let mut plan = base();
    plan.entity_plan.entity_id_expr = Expr::Field(FieldRef::Path {
        alias: "b".into(),
        segments: vec![PathSegment::Field("obj".into())],
    });
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());
    let mut plan = base();
    plan.entity_plan.entity_id_expr = Expr::BinOp {
        op: BinOp::Add,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::Number(1.0)),
    };
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());

    // General yield expression → false.
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "upper".into(),
            args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
        },
    }];
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // 列式输出函数 yield（fmt/strftime/count_char）→ safe（批量 cell 求值）。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("ip={}".into()),
                Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
            ],
        },
    }];
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "strftime".into(),
            args: vec![Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))],
        },
    }];
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "count_char".into(),
            args: vec![
                Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
                Expr::StringLit("1".into()),
            ],
        },
    }];
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());

    // fmt 模板非字面量 / 参数含函数调用 → false（columnar_output_expr 拒绝）。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
        },
    }];
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("x={}".into()),
                Expr::FuncCall {
                    qualifier: None,
                    name: "lower".into(),
                    args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
                },
            ],
        },
    }];
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // 有活 join + 输出函数 yield → false（列式 join 富化路径未接入批量 cell，
    // 拒绝避免 unreachable panic；回退行式）。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("ip={}".into()),
                Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
            ],
        },
    }];
    plan.joins = vec![JoinPlan {
        right_window: "w".into(),
        mode: JoinMode::Inner,
        conds: vec![],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    assert!(
        !RuleExecutor::new(plan).each_plan_columnar_safe(),
        "有活 join 时输出函数 yield 必须回退行式"
    );

    // Path yield field → false.
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::Field(FieldRef::Path {
            alias: "b".into(),
            segments: vec![PathSegment::Field("obj".into())],
        }),
    }];
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // Literal yields are fine.
    let mut plan = base();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "a".into(),
            value: Expr::Number(1.0),
        },
        YieldField {
            name: "b".into(),
            value: Expr::StringLit("s".into()),
        },
        YieldField {
            name: "c".into(),
            value: Expr::Bool(true),
        },
    ];
    assert!(RuleExecutor::new(plan).each_plan_columnar_safe());
}

/// `each_pipe_columnar_safe` 门控（2026-08-25 q13a 列式化）：pipe 列式路径的
/// 保守形状——无 joins/lets/where/each filter、score 常量、entity 字面量/flat
/// 字段、yield ∈ {字面量, flat 字段, `expr_is_columnar`（BinOp 如 q13a
/// `auction % 10000`）}。sink 门控（each_plan_columnar_safe）放行的形状
/// （each filter / 输出函数 / 活 join）在 pipe 门控下**保守拒绝**（回退行式
/// stage_pipe_record）。
#[test]
fn each_pipe_columnar_safe_gate_branches() {
    let base = || {
        let mut plan = simple_rule_plan(
            "q13a_bench",
            simple_plan(vec![], vec![]),
            Expr::Number(10.0),
            "digit",
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
        );
        plan.binds[0].alias = "b".into();
        plan.each_plan = Some(EachPlan {
            alias: "b".into(),
            filter: None,
        });
        plan
    };

    // q13a 形状：5 Field + 1 `%` BinOp yield → safe（BinOp 编译为批级 cvec）。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "mod_key".into(),
        value: Expr::BinOp {
            op: BinOp::Mod,
            left: Box::new(Expr::Field(FieldRef::Qualified(
                "b".into(),
                "auction".into(),
            ))),
            right: Box::new(Expr::Number(10000.0)),
        },
    }];
    assert!(
        RuleExecutor::new(plan).each_pipe_columnar_safe(),
        "q13a mod BinOp yield 必须通过 pipe 列式门控"
    );

    // 无 each plan → false。
    let mut plan = base();
    plan.each_plan = None;
    assert!(!RuleExecutor::new(plan).each_pipe_columnar_safe());

    // each filter → false（pipe 列式路径未接 filter 求值；sink 门控允许）。
    let mut plan = base();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: Some(Expr::Bool(true)),
    });
    assert!(
        !RuleExecutor::new(plan).each_pipe_columnar_safe(),
        "pipe 门控对 each filter 保守拒绝（sink 门控放行）"
    );

    // lets → false。
    let mut plan = base();
    plan.lets = vec![LetPlan {
        name: "x".into(),
        expr: Expr::Number(1.0),
    }];
    assert!(!RuleExecutor::new(plan).each_pipe_columnar_safe());

    // 活 join → false（pipe 列式路径无 join 富化）。yield 引用右窗字段使
    // join 存活（否则死 join 消除 → live_joins 空 → 误放行）。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "cat".into(),
        value: Expr::Field(FieldRef::Qualified("w".into(), "category".into())),
    }];
    plan.joins = vec![JoinPlan {
        right_window: "w".into(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "bidder".into()),
            right: FieldRef::Qualified("w".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    assert!(!RuleExecutor::new(plan).each_pipe_columnar_safe());

    // 可列式 `where`（gap-3 2026-09-02）→ true（批级守卫掩码）。
    let mut plan = base();
    plan.r#where = Some(Expr::Bool(true));
    assert!(RuleExecutor::new(plan).each_pipe_columnar_safe());

    // 非列式 `where`（函数调用）→ false。
    let mut plan = base();
    plan.r#where = Some(Expr::FuncCall {
        qualifier: None,
        name: "upper".into(),
        args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
    });
    assert!(!RuleExecutor::new(plan).each_pipe_columnar_safe());

    // 非列式 yield（upper 函数调用）→ false。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "upper".into(),
            args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
        },
    }];
    assert!(!RuleExecutor::new(plan).each_pipe_columnar_safe());

    // 输出函数 fmt yield → false（pipe 门控未接批量 cell；sink 门控放行）。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("x={}".into()),
                Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
            ],
        },
    }];
    assert!(
        !RuleExecutor::new(plan).each_pipe_columnar_safe(),
        "fmt yield 保守回退行式（列式装载仅支持 Lit/Field/expr_is_columnar）"
    );

    // Path yield field → false。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "y".into(),
        value: Expr::Field(FieldRef::Path {
            alias: "b".into(),
            segments: vec![PathSegment::Field("obj".into())],
        }),
    }];
    assert!(!RuleExecutor::new(plan).each_pipe_columnar_safe());

    // 非 flat entity（Path）→ false。
    let mut plan = base();
    plan.entity_plan.entity_id_expr = Expr::Field(FieldRef::Path {
        alias: "b".into(),
        segments: vec![PathSegment::Field("obj".into())],
    });
    assert!(!RuleExecutor::new(plan).each_pipe_columnar_safe());

    // 非常量 score → false。
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
    };
    assert!(!RuleExecutor::new(plan).each_pipe_columnar_safe());

    // 非列式 bind filter → false。
    let mut plan = base();
    plan.binds[0].filter = Some(Expr::FuncCall {
        qualifier: None,
        name: "upper".into(),
        args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
    });
    assert!(!RuleExecutor::new(plan).each_pipe_columnar_safe());
}

/// Dead-join elimination (2026-08-23, q13 RSS/EPS): a Snapshot/Asof join whose
/// enrichment no output expression reads is dropped from `live_joins` — the
/// rule then qualifies for the columnar each fast path. Filtering modes
/// (Inner/Anti), `within` intervals, `reduce`/`emit at`, and any plain
/// (unqualified) output field reference keep the join live.
#[test]
fn dead_join_elimination_keeps_only_referenced_enrichment() {
    let snapshot_join = || JoinPlan {
        right_window: "person_events".into(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "bidder".into()),
            right: FieldRef::Qualified("person_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    };
    // Rule whose output reads only the driving event's fields (qualified) +
    // literals — the q13 shape. The person snapshot join is dead.
    let base = || {
        let mut plan = simple_rule_plan(
            "q13_shape",
            simple_plan(vec![], vec![]),
            Expr::Number(10.0),
            "sink",
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
        );
        plan.binds[0].alias = "b".into();
        plan.each_plan = Some(EachPlan {
            alias: "b".into(),
            filter: None,
        });
        plan.joins = vec![snapshot_join()];
        plan.yield_plan.fields = vec![YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
        }];
        plan
    };
    let exec = RuleExecutor::new(base());
    assert!(
        exec.live_joins.is_empty(),
        "unreferenced Snapshot join must be eliminated"
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "dead-join rule must qualify for the columnar each path"
    );

    // `where` reading a right-window field keeps the join live (q20 shape).
    let mut plan = base();
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Qualified(
            "person_events".into(),
            "id".into(),
        ))),
        right: Box::new(Expr::Number(42.0)),
    });
    let exec = RuleExecutor::new(plan);
    assert_eq!(exec.live_joins.len(), 1, "where ref → join live");
    // 右窗 where 简单形状（字段 <cmp> 字面量）→ 列式 join 富化支持
    // （2026-08-23 列式 join 富化——q20 形状）。
    assert!(
        exec.each_join_columnar_ready() && exec.each_plan_columnar_safe(),
        "右窗 where 简单形状必须列式 join 支持"
    );

    // yield reading the right window keeps it live.
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "city".into(),
        value: Expr::Field(FieldRef::Qualified("person_events".into(), "city".into())),
    }];
    let exec = RuleExecutor::new(plan);
    assert_eq!(exec.live_joins.len(), 1, "yield ref → join live");
    // yield 读右窗字段（限定）→ 列式 join 支持（q20 输出形状）。
    assert!(
        exec.each_join_columnar_ready() && exec.each_plan_columnar_safe(),
        "右窗 yield 限定引用必须列式 join 支持"
    );

    // A plain (unqualified) output field ref → conservative: join stays live.
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "city".into(),
        value: Expr::Field(FieldRef::Simple("city".into())),
    }];
    let exec = RuleExecutor::new(plan);
    assert_eq!(
        exec.live_joins.len(),
        1,
        "plain ref → join live (conservative)"
    );

    // Filtering modes are never eliminated (miss/hit drops the event).
    for mode in [JoinMode::Inner, JoinMode::Anti] {
        let mut plan = base();
        plan.joins = vec![JoinPlan {
            mode: mode.clone(),
            ..snapshot_join()
        }];
        let exec = RuleExecutor::new(plan);
        assert_eq!(
            exec.live_joins.len(),
            1,
            "mode {mode:?} must never be eliminated"
        );
    }
    // Asof miss keeps the event (like Snapshot) → dead-eliminable.
    let mut plan = base();
    plan.joins = vec![JoinPlan {
        mode: JoinMode::Asof { within: None },
        ..snapshot_join()
    }];
    assert!(
        RuleExecutor::new(plan).live_joins.is_empty(),
        "unreferenced Asof join must be eliminated"
    );
    // within / reduce / emit_at keep the join live.
    let mut plan = base();
    plan.joins = vec![JoinPlan {
        within: Some(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: std::time::Duration::from_secs(1),
                    neg: false,
                },
            },
            hi: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: std::time::Duration::from_secs(2),
                    neg: false,
                },
            },
        }),
        ..snapshot_join()
    }];
    assert_eq!(RuleExecutor::new(plan).live_joins.len(), 1, "within → live");
    let mut plan = base();
    plan.joins = vec![JoinPlan {
        emit_at: Some(Expr::Field(FieldRef::Qualified(
            "a".into(),
            "expires".into(),
        ))),
        ..snapshot_join()
    }];
    assert_eq!(
        RuleExecutor::new(plan).live_joins.len(),
        1,
        "emit_at → live"
    );
    let mut plan = base();
    plan.joins = vec![JoinPlan {
        reduce: Some(ReduceClause {
            measure: ReduceMeasure::Maxrow {
                field: FieldRef::Simple("price".into()),
                tie: None,
            },
            label: Some("winner".into()),
        }),
        ..snapshot_join()
    }];
    assert_eq!(RuleExecutor::new(plan).live_joins.len(), 1, "reduce → live");
}
