//! coverage_extra 拆出的兄弟子模块（2026-09-04）：`executor/mod.rs` 匹配路径语义——
//! window-lookup bind filter 下 `event_matches_alias`、`apply_lets`（each / close / match
//! 三路 + match-expr yield）、system vars / meta 的 General yield 求值、`trigger_event_needed`
//! 与 fire 触发事件投影（M1：规则读集窄化 / 跨窗裸名不幻影物化）。共享 harness 在父模块
//! `coverage_extra.rs`，此处经 `use super::*` 复用。

use super::*;

use crate::match_engine::cep::{CepStateMachine, StepResult};
use crate::match_engine::event_bridge::ColumnarEvent;
use crate::match_engine::{FieldSource, build_field_index};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{BinOp, MatchArm, PathSegment, SystemVar};
use wf_lang::plan::LetPlan;

// ---------------------------------------------------------------------------
// mod.rs — event_matches_alias through a bind filter with a window lookup
// ---------------------------------------------------------------------------

#[test]
fn event_matches_alias_with_window_lookup() {
    // A bind filter referencing `window.has(...)`-style access is evaluated
    // through eval_bool_expr_with_lookup with the provided windows.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.binds = vec![BindPlan {
        alias: "a".into(),
        window: "w".into(),
        filter: Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified("a".into(), "sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        }),
    }];
    let exec = RuleExecutor::new(plan);
    let ev = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(exec.event_matches_alias("a", &ev, Some(&EmptyLookup)));
    let ev2 = event(vec![("sip", str_val("1.1.1.1"))]);
    assert!(!exec.event_matches_alias("a", &ev2, Some(&EmptyLookup)));
}

// ---------------------------------------------------------------------------
// mod.rs — apply_lets
// ---------------------------------------------------------------------------

#[test]
fn apply_lets_injects_bindings_and_skips_failures() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.lets = vec![
        LetPlan {
            name: "a".into(),
            expr: Expr::BinOp {
                op: BinOp::Add,
                left: Box::new(Expr::Field(FieldRef::Simple("x".into()))),
                right: Box::new(Expr::Number(1.0)),
            },
        },
        // Fails to evaluate (missing field) → no injection.
        LetPlan {
            name: "b".into(),
            expr: Expr::Field(FieldRef::Simple("missing".into())),
        },
        // Later binding references an earlier one.
        LetPlan {
            name: "c".into(),
            expr: Expr::BinOp {
                op: BinOp::Mul,
                left: Box::new(Expr::Field(FieldRef::Simple("a".into()))),
                right: Box::new(Expr::Number(2.0)),
            },
        },
    ];
    let exec = RuleExecutor::new(plan);
    let mut ctx = event(vec![("x", num(5.0))]);
    exec.apply_lets(&mut ctx);
    assert_eq!(ctx.fields.get("a"), Some(&num(6.0)));
    assert!(!ctx.fields.contains_key("b"));
    assert_eq!(ctx.fields.get("c"), Some(&num(12.0)));
}

/// match 表达式（issue #79 Issue 2）：on-each 路径 yield 求值——多模式命中
/// 取分支值、未命中取默认分支。列式 gate 不识别 match → 回落行式。
#[test]
fn execute_each_match_expr_yield() {
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
    plan.binds[0].alias = "e".into();
    plan.yield_plan.fields = vec![YieldField {
        name: "sev".into(),
        value: Expr::Match {
            expr: Box::new(Expr::Field(FieldRef::Qualified(
                "e".into(),
                "action".into(),
            ))),
            arms: vec![MatchArm {
                patterns: vec![
                    Expr::StringLit("crit".into()),
                    Expr::StringLit("alert".into()),
                ],
                value: Expr::StringLit("CRITICAL".into()),
            }],
            default: Some(Box::new(Expr::Field(FieldRef::Qualified(
                "e".into(),
                "action".into(),
            )))),
        },
    }];
    let exec = RuleExecutor::new(plan);
    // crit | alert → CRITICAL
    let rec = exec
        .execute_each(
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("action", str_val("crit")),
            ]),
            0,
        )
        .unwrap()
        .unwrap();
    assert_eq!(rec.yield_fields[0].1, str_val("CRITICAL"));
    // 未命中 → 默认分支（原值透传）
    let rec = exec
        .execute_each(
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("action", str_val("info")),
            ]),
            0,
        )
        .unwrap()
        .unwrap();
    assert_eq!(rec.yield_fields[0].1, str_val("info"));
    // 数字 subject 命中数字模式
    let mut plan = simple_rule_plan(
        "r2",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.binds[0].alias = "e".into();
    plan.yield_plan.fields = vec![YieldField {
        name: "bucket".into(),
        value: Expr::Match {
            expr: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "count".into()))),
            arms: vec![MatchArm {
                patterns: vec![Expr::Number(1.0), Expr::Number(2.0)],
                value: Expr::StringLit("low".into()),
            }],
            default: None,
        },
    }];
    let exec = RuleExecutor::new(plan);
    let rec = exec
        .execute_each(
            &event(vec![("sip", str_val("10.0.0.1")), ("count", num(2.0))]),
            0,
        )
        .unwrap()
        .unwrap();
    assert_eq!(rec.yield_fields[0].1, str_val("low"));
    // 无默认且未命中 → match 求值 None → yield 回退空串（eval_yield_expr_with_meta
    // 语义：None → Value::Str("")）。
    let rec = exec
        .execute_each(
            &event(vec![("sip", str_val("10.0.0.1")), ("count", num(9.0))]),
            0,
        )
        .unwrap()
        .unwrap();
    assert_eq!(rec.yield_fields[0].1, str_val(""));
}

/// match 表达式在 close 输出路径（issue #79 Issue 2）：subject 读 close ctx
/// 字段（键/聚合），分支值做归一化。
#[test]
fn execute_close_match_expr_yield() {
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
    plan.yield_plan.fields = vec![YieldField {
        name: "zone".into(),
        value: Expr::Match {
            expr: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
            arms: vec![MatchArm {
                patterns: vec![Expr::StringLit("10.0.0.1".into())],
                value: Expr::StringLit("dmz".into()),
            }],
            default: Some(Box::new(Expr::StringLit("other".into()))),
        },
    }];
    let exec = RuleExecutor::new(plan);
    let close = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("x"), 1.0, EngineHashMap::default())],
        vec![],
    );
    let rec = exec.execute_close(&close).unwrap().unwrap();
    assert_eq!(
        rec.yield_fields[0].1,
        str_val("dmz"),
        "close ctx 键字段命中 match 分支"
    );
}

/// match 表达式 × let 派生字段协同（issue #79）：let 求值注入 ctx 后，match
/// 的 subject/模式/分支值均可引用 let 名；列式 gate 对含 match 的 yield 回落
/// 行式（apply_lets 逐行注入与 match 求值同路径）。
#[test]
fn match_expr_references_let_bindings() {
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
    plan.binds[0].alias = "e".into();
    plan.lets = vec![LetPlan {
        name: "flag".into(),
        expr: Expr::StringLit("admin".into()),
    }];
    plan.yield_plan.fields = vec![YieldField {
        name: "level".into(),
        value: Expr::Match {
            expr: Box::new(Expr::Field(FieldRef::Simple("flag".into()))),
            arms: vec![MatchArm {
                patterns: vec![Expr::StringLit("admin".into())],
                value: Expr::StringLit("root".into()),
            }],
            default: Some(Box::new(Expr::StringLit("user".into()))),
        },
    }];
    let exec = RuleExecutor::new(plan);
    // subject = let 派生值 "admin" → 命中 admin 分支。
    let rec = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 0)
        .unwrap()
        .unwrap();
    assert_eq!(rec.yield_fields[0].1, str_val("root"));
    // 列式 gate：match yield 不列式（columnar_output_expr=false）→ 回落行式。
    assert!(
        !exec.each_plan_columnar_safe(),
        "含 match 的 yield 回落行式（apply_lets 逐行注入）"
    );
}

/// match 路径的 let 派生字段（2026-08-31，issue #79）：execute_match_at 在
/// ctx 构建后 apply_lets，yield 引用 let 名得到派生值。let 链：`a = sip`（键
/// 字段注入）、`b = a * 2`。
#[test]
fn execute_match_applies_lets_before_alert_build() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.lets = vec![
        LetPlan {
            name: "a".into(),
            expr: Expr::Field(FieldRef::Simple("sip".into())),
        },
        LetPlan {
            name: "b".into(),
            expr: Expr::BinOp {
                op: BinOp::Mul,
                left: Box::new(Expr::Field(FieldRef::Simple("a".into()))),
                right: Box::new(Expr::Number(2.0)),
            },
        },
    ];
    plan.yield_plan.fields = vec![
        YieldField {
            name: "a_out".into(),
            value: Expr::Field(FieldRef::Simple("a".into())),
        },
        YieldField {
            name: "b_out".into(),
            value: Expr::Field(FieldRef::Simple("b".into())),
        },
    ];
    let exec = RuleExecutor::new(plan);
    let mut mc = default_matched_context();
    mc.scope_key = vec![num(5.0)]; // key `sip` = 5
    let rec = exec.execute_match_at(&mc, 0).unwrap();
    assert_eq!(rec.yield_fields.len(), 2);
    assert_eq!(rec.yield_fields[0].1, num(5.0), "a = sip = 5");
    assert_eq!(rec.yield_fields[1].1, num(10.0), "b = a * 2 = 10");
}

/// 有 let 的 match 规则必须禁用 ctx-free 快路径与列式直写（列式视图无 let，
/// 回落行式保证 apply_lets 注入语义一致）；无 let 的对照组仍走快路径。
#[test]
fn match_lets_force_rowwise_paths() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.lets = vec![LetPlan {
        name: "a".into(),
        expr: Expr::Field(FieldRef::Simple("sip".into())),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(
        !exec.output_static().match_ctx_free,
        "let 需要 build_eval_context 注入，ctx-free 快路径必须禁用"
    );
    assert!(
        !exec.match_plan_columnar_safe(),
        "let 规则回落行式（列式视图无 let 视图）"
    );
    // 对照组：无 let 时仍列式安全 + ctx-free。
    let exec2 = RuleExecutor::new(simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    ));
    assert!(exec2.match_plan_columnar_safe());
    assert!(exec2.output_static().match_ctx_free);
}

/// close 路径的 let 派生字段（2026-08-31，issue #79）：execute_close 在 close
/// ctx（键字段 + 窗口聚合）上求值注入。`a = sip`（键注入）、`b = concat(a,
/// "_s")`（字符串派生）。
#[test]
fn execute_close_applies_lets() {
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
    plan.lets = vec![
        LetPlan {
            name: "a".into(),
            expr: Expr::Field(FieldRef::Simple("sip".into())),
        },
        LetPlan {
            name: "b".into(),
            expr: Expr::FuncCall {
                qualifier: None,
                name: "concat".into(),
                args: vec![
                    Expr::Field(FieldRef::Simple("a".into())),
                    Expr::StringLit("_s".into()),
                ],
            },
        },
    ];
    plan.yield_plan.fields = vec![YieldField {
        name: "b_out".into(),
        value: Expr::Field(FieldRef::Simple("b".into())),
    }];
    let exec = RuleExecutor::new(plan);
    let close = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("x"), 1.0, EngineHashMap::default())],
        vec![],
    );
    let rec = exec.execute_close(&close).unwrap().unwrap();
    assert_eq!(rec.yield_fields.len(), 1);
    assert_eq!(
        rec.yield_fields[0].1,
        str_val("10.0.0.1_s"),
        "b = concat(a, _s)"
    );
}

/// 有 let 的 close 规则必须禁用列式 close 直写（回落行式，apply_lets 注入
/// 语义一致）；无 let 对照组仍列式安全。
#[test]
fn close_lets_force_rowwise_paths() {
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
    plan.lets = vec![LetPlan {
        name: "a".into(),
        expr: Expr::Field(FieldRef::Simple("sip".into())),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(
        !exec.close_plan_columnar_safe(),
        "let close 规则回落行式（列式视图无 let 视图）"
    );
    let exec2 = RuleExecutor::new(simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("x", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    ));
    assert!(exec2.close_plan_columnar_safe());
}

// ---------------------------------------------------------------------------
// mod.rs — yield evaluation with system vars / meta (General yield kind)
// ---------------------------------------------------------------------------

#[test]
fn execute_match_general_yield_with_meta_vars() {
    use wf_lang::wfu_meta::WfuMetaField;

    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "rule".into(),
            value: Expr::WfuMeta(WfuMetaField::RuleName),
        },
        YieldField {
            name: "score".into(),
            value: Expr::SystemVar(SystemVar::Score),
        },
        YieldField {
            name: "scored".into(),
            value: Expr::BinOp {
                op: BinOp::Mul,
                left: Box::new(Expr::SystemVar(SystemVar::Score)),
                right: Box::new(Expr::Number(2.0)),
            },
        },
    ];
    let exec = RuleExecutor::new(plan);
    let matched = default_matched_context();
    let rec = exec.execute_match_at(&matched, 123).unwrap();
    let get = |name: &str| {
        rec.yield_fields
            .iter()
            .find(|(n, _)| &**n == name)
            .map(|(_, v)| v.clone())
            .unwrap()
    };
    assert_eq!(get("rule"), str_val("r1"));
    assert_eq!(get("score"), num(70.0));
    assert_eq!(get("scored"), num(140.0));
}

// ---------------------------------------------------------------------------
// trigger_event_needed — fire 路径是否物化触发事件（2026-08 hotpath）
// ---------------------------------------------------------------------------

#[test]
fn fire_skips_trigger_event_when_key_only_yield() {
    // Q5/Q7/Q12/Q13 形状：score/entity/yield 只读 key 字段 → 编译器
    // `trigger_event_needed=false` → fire 的 MatchedContext.trigger_event 为 None
    // （跳过 per-fire `event.to_event()` 全量 clone）。key 字段由
    // build_eval_context 从 scope_key 提供，输出不受影响。
    let mut plan = simple_rule_plan(
        "r",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("e", count_ge(1.0))])],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.binds[0].window = "w".into();
    plan.match_plan.trigger_event_needed = false;

    let mut sm = CepStateMachine::new("r".into(), plan.match_plan.clone(), None);
    let ev = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("failed")),
    ]);
    let StepResult::Matched(ctx) = sm.advance_at("e", &ev, 1_000) else {
        panic!("must fire");
    };
    assert!(
        ctx.trigger_event.is_none(),
        "key-only yield → fire 不物化触发事件"
    );

    // 输出仍正确：entity/yield 的 key 字段从 scope_key 解析。
    let exec = RuleExecutor::new(plan);
    let rec = exec.execute_match(&ctx).expect("record");
    assert_eq!(rec.entity_id, "10.0.0.1");
}

#[test]
fn fire_keeps_trigger_event_when_non_key_yield() {
    // 非 key yield（e.action）→ 编译器 `trigger_event_needed=true` → fire 保留
    // 触发事件（build_eval_context 从 trigger_event 注入 action）。
    let mut plan = simple_rule_plan(
        "r",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("e", count_ge(1.0))])],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.binds[0].window = "w".into();
    plan.match_plan.trigger_event_needed = true;
    plan.yield_plan.fields = vec![YieldField {
        name: "action".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "action".into())),
    }];

    let mut sm = CepStateMachine::new("r".into(), plan.match_plan.clone(), None);
    let ev = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("failed")),
    ]);
    let StepResult::Matched(ctx) = sm.advance_at("e", &ev, 1_000) else {
        panic!("must fire");
    };
    assert!(
        ctx.trigger_event.is_some(),
        "非 key yield → fire 保留触发事件"
    );

    // yield action 从 trigger_event 注入 → 值正确。
    let exec = RuleExecutor::new(plan);
    let rec = exec.execute_match(&ctx).expect("record");
    let action = rec
        .yield_fields
        .iter()
        .find(|(n, _)| n.as_ref() == "action")
        .map(|(_, v)| v.clone());
    assert_eq!(action, Some(Value::Str("failed".into())));
}

// ---------------------------------------------------------------------------
// M1（P4 终态机制 2026-09-02）：规则级 fire 投影——to_event 只物化 ctx 读的
// Named 字段（消除窗口并集里未引用结构化列的每 fire JSON 解析）。
// ---------------------------------------------------------------------------

#[test]
fn fire_trigger_projection_narrows_to_rule_read_set() {
    use crate::match_engine::{WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY};
    // key≠读集 的规则形态（c_dip_3 式）：entity=yield 读 sip，key=dip。
    let mut plan = simple_rule_plan(
        "m1_proj",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Qualified("c".into(), "sip".into())),
    );
    plan.binds[0].alias = "c".into();
    plan.yield_plan.fields = vec![YieldField {
        name: "dip_out".into(),
        value: Expr::Field(FieldRef::Qualified("c".into(), "dip".into())),
    }];
    let exec = RuleExecutor::new(plan);
    let proj = exec
        .fire_trigger_projection()
        .expect("Named 窄化 match 规则必须带规则级 fire 投影");
    assert!(proj.contains("sip") && proj.contains("dip"), "读集入投影");
    assert!(!proj.contains("tags"), "未引用字段不入投影");

    // 投影实际生效：含结构化列的批上 to_event 只物化读集，跳过 tags JSON。
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new("dip", DataType::Utf8, true),
        ArrowField::new("tags", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_ARRAY.to_string(),
            )]),
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("1.1.1.1")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("2.2.2.2")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some(r#"["prod","edge"]"#)])) as ArrayRef,
        ],
    )
    .unwrap();
    let ev = ColumnarEvent::with_index_projected(
        &batch,
        0,
        build_field_index(&batch),
        Some(Arc::clone(&proj)),
    );
    let materialized = ev.to_event();
    assert_eq!(materialized.fields.len(), 2, "只物化读集字段");
    assert_eq!(materialized.fields.get("sip"), Some(&str_val("1.1.1.1")));
    assert_eq!(materialized.fields.get("dip"), Some(&str_val("2.2.2.2")));
    assert!(
        !materialized.fields.contains_key("tags"),
        "tags 不入 fire 物化"
    );
}

#[test]
fn fire_trigger_projection_none_when_ctx_untrackable() {
    // `_step_*`/合成字段引用 → close_ctx_fields=All → 无法窄化 → 投影 None
    // （调用方回退窗口 materialize_fields，行为与现状一致）。
    let mut plan = simple_rule_plan(
        "m1_all",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Qualified("c".into(), "sip".into())),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "measure".into(),
        value: Expr::Field(FieldRef::Simple("_step_0_measure".into())),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.fire_trigger_projection().is_none(),
        "All（合成字段引用）→ 无规则级投影"
    );
}

// ---------------------------------------------------------------------------
// M1 review ①（2026-09-02 审计）：multi-alias / Path 读集与 fire 投影——
// field_ref_name 剥 alias、Path 取 root，Named 含跨 alias 裸名；单窗批上
// to_event 只物化本批列。三不变式：
//   1) Named ⊇ eval 从 ctx 读的每个裸名（visit_expr_fields 穷尽 + force_all 兜底）；
//   2) 跨窗/跨 alias 裸名若不在驱动窗批 schema → 不幻影物化（to_event 跳过）；
//   3) 同名折叠成单裸名（集合语义），投影恒为超集方向（不会漏读）。
// ---------------------------------------------------------------------------

#[test]
fn plan_close_ctx_fields_multi_alias_and_path_collapse_to_bare_roots() {
    use crate::match_engine::executor::{CloseCtxFields, plan_close_ctx_fields};

    // 多 alias + 深 Path + 同名两读：entity=auction（alias b）、yield1=
    // `b.obj.x[0]`（Path root=obj）、yield2/3=`c.sip` 与 `e2.sip`（同裸名两 alias）。
    let mut plan = simple_rule_plan(
        "alias_path_audit",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "c".into();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "deep".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "b".into(),
                segments: vec![
                    PathSegment::Field("obj".into()),
                    PathSegment::Field("x".into()),
                    PathSegment::Index(0),
                ],
            }),
        },
        YieldField {
            name: "sip_out".into(),
            value: Expr::Field(FieldRef::Qualified("c".into(), "sip".into())),
        },
        YieldField {
            name: "sip_again".into(),
            value: Expr::Field(FieldRef::Qualified("e2".into(), "sip".into())),
        },
    ];

    let fields = plan_close_ctx_fields(&plan);
    match &fields {
        CloseCtxFields::Named(set) => {
            // 裸名/root 入集：Path root=obj（非 alias b、非叶子 x），Qualified
            // 剥 alias（auction/sip）。
            assert!(set.contains("auction"), "entity 裸名入集");
            assert!(set.contains("obj"), "Path root 入集（读集需物化整列）");
            assert!(set.contains("sip"), "Qualified 剥 alias 入集");
            // 不误收：alias、Path 中间/叶子段、未引用列。
            assert!(!set.contains("b"), "alias 不入集");
            assert!(!set.contains("c"), "alias 不入集");
            assert!(!set.contains("e2"), "alias 不入集");
            assert!(!set.contains("x"), "Path 非 root 段不入集");
            assert!(!set.contains("tags"), "未引用列不入集");
            assert_eq!(set.len(), 3, "同名跨 alias 折叠：{{obj, sip, auction}}");
        }
        _ => panic!("无函数/合成字段引用应窄化为 Named，got {fields:?}"),
    }
}

#[test]
fn fire_projection_multi_window_bare_names_no_phantom_and_path_root_materialized() {
    use crate::match_engine::{WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY};

    // 读集含跨窗裸名（b.bcol：非驱动窗列）+ Path root（b.obj）+ 驱动窗列
    // （c.sip）；entity=auction。fire 投影 = 该 Named 集。
    let mut plan = simple_rule_plan(
        "m1_multiwin",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "c".into();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "deep".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "b".into(),
                segments: vec![PathSegment::Field("obj".into())],
            }),
        },
        YieldField {
            name: "sip_out".into(),
            value: Expr::Field(FieldRef::Qualified("c".into(), "sip".into())),
        },
        YieldField {
            name: "other".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "bcol".into())),
        },
    ];
    let exec = RuleExecutor::new(plan);
    let proj = exec
        .fire_trigger_projection()
        .expect("Named 窄化 match 规则必须带规则级 fire 投影");
    assert!(proj.contains("obj") && proj.contains("sip") && proj.contains("bcol"));

    // 驱动窗批：sip/obj/auction + 未读 tags（array 结构化列）。bcol 不在本批。
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new("obj", DataType::Utf8, true),
        ArrowField::new("auction", DataType::Utf8, true),
        ArrowField::new("tags", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_ARRAY.to_string(),
            )]),
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("1.1.1.1")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some(r#"{"x":[7]}"#)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("99")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some(r#"["prod"]"#)])) as ArrayRef,
        ],
    )
    .unwrap();
    let ev = ColumnarEvent::with_index_projected(
        &batch,
        0,
        build_field_index(&batch),
        Some(Arc::clone(&proj)),
    );
    let materialized = ev.to_event();

    // 不变式 2/3：本批读集列物化（含 Path root obj）；跨窗裸名 bcol 不是本批
    // 列 → 不幻影物化；未读列 tags 不解析。
    assert_eq!(materialized.fields.len(), 3, "只物化本批读集列");
    assert_eq!(materialized.fields.get("sip"), Some(&str_val("1.1.1.1")));
    assert_eq!(
        materialized.fields.get("obj"),
        Some(&str_val(r#"{"x":[7]}"#)),
        "Path root 整列物化（JSON 文本；深度解析由 eval 按需）"
    );
    assert!(
        !materialized.fields.contains_key("bcol"),
        "跨窗裸名非本批列 → 无幻影键"
    );
    assert!(
        !materialized.fields.contains_key("tags"),
        "未引用结构化列不解析"
    );
}
