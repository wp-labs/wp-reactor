//! core_coverage 拆出的兄弟子模块（2026-09-04）：`executor/mod.rs` 覆盖——
//! RuleExecutor 构建/查询接口（plan / static_yield_target / where_ok /
//! machine_id / cached emit time）、yield 类型强制
//! （coerce_yield_field_value_with 全类型与失败分支）、bind / each /
//! branch-guard 列式掩码与列式安全门、is_aux / 多 bind map 路径。
//! 共享 harness 与 import 在父模块 core_coverage.rs（eq_str_expr 亦在父模块，
//! close/each 子模块共用），此处经 `use super::*` 复用。

use super::*;

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
