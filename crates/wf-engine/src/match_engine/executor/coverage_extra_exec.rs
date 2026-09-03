//! coverage_extra 拆出的兄弟子模块（2026-09-04）：`executor/mod.rs` 行式工具面——
//! `RuleExecutor` 构造 / `output_static` 静态预计算、`cached_emit_time`、`where_ok`、
//! machine-id / scope-key 边角、bind / alias 匹配（linear/map 两路）与列式掩码门控、
//! `coerce_yield_field_value_with` 类型矩阵、`branch_guard_masks` 门控分支。
//! 共享 harness（plan / RowsLookup / event 构造器）在父模块 `coverage_extra.rs`，
//! 此处经 `use super::*` 复用。

use super::*;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{BinOp, PathSegment};
use wf_lang::plan::{SeqPlan, SeqSkipPlan, SeqStepPlan};

// ---------------------------------------------------------------------------
// mod.rs — RuleExecutor construction / static precompute
// ---------------------------------------------------------------------------

#[test]
fn output_static_precomputes_plan_constants() {
    let mut plan = simple_rule_plan(
        "const_rule",
        simple_plan(vec![simple_key("sip")], vec![]),
        Expr::Number(150.0), // clamped to 100 at construction
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "a".into(),
            value: Expr::Number(1.0),
        },
        YieldField {
            name: "b".into(),
            value: Expr::StringLit("lit".into()),
        },
        YieldField {
            name: "c".into(),
            value: Expr::Bool(true),
        },
        YieldField {
            name: "d".into(),
            value: Expr::Field(FieldRef::Simple("sip".into())),
        },
        YieldField {
            name: "e".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "upper".into(),
                args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
            },
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("a".into(), FieldType::Base(BaseType::Digit)),
            ("c".into(), FieldType::Base(BaseType::Bool)),
        ]),
    );

    let statics = exec.output_static();
    assert_eq!(&*statics.rule_name, "const_rule");
    assert_eq!(&*statics.entity_type, "ip");
    assert_eq!(&*statics.yield_target, "alerts");
    assert_eq!(statics.score_const, Some(100.0));
    assert!(statics.each_summary.is_some());
    assert_eq!(&*statics.each_origin, "event");
    assert_eq!(&*statics.each_close_reason, "");
    // Typed fields only (those present in the runtime type map).
    assert_eq!(statics.yield_field_types.len(), 2);
    // Yield kinds: Number→Lit, StringLit→Lit, Bool→Lit, Field→Field, else General.
    use crate::match_engine::executor::YieldKind;
    assert!(matches!(
        statics.yield_kinds[0],
        YieldKind::Lit(Value::Number(1.0))
    ));
    assert!(matches!(
        statics.yield_kinds[1],
        YieldKind::Lit(Value::Str(_))
    ));
    assert!(matches!(
        statics.yield_kinds[2],
        YieldKind::Lit(Value::Bool(true))
    ));
    assert!(matches!(statics.yield_kinds[3], YieldKind::Field));
    assert!(matches!(statics.yield_kinds[4], YieldKind::General));

    assert_eq!(exec.plan().name, "const_rule");
    assert_eq!(&**exec.static_yield_target(), "alerts");
    assert!(exec.output_config().time_format.len() >= 3);
}

#[test]
fn output_static_no_each_plan_has_no_summary() {
    let plan = simple_rule_plan(
        "no_each",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan);
    assert!(exec.output_static().each_summary.is_none());
    assert_eq!(exec.output_static().score_const, Some(50.0));
}

#[test]
fn cached_emit_time_formats_once_and_reuses() {
    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan);
    let a = exec.cached_emit_time(1_700_000_000_123_456_789);
    let b = exec.cached_emit_time(1_700_000_000_123_456_789);
    assert!(Arc::ptr_eq(&a, &b), "same nanos must reuse the cached Arc");
    let c = exec.cached_emit_time(1_700_000_000_999_999_999);
    assert!(!Arc::ptr_eq(&a, &c), "different nanos must reformat");
    // Clones start with a fresh cache (must still be correct).
    let clone = exec.clone();
    let d = clone.cached_emit_time(1_700_000_000_123_456_789);
    assert_eq!(a.as_ref(), d.as_ref());
}

#[test]
fn where_ok_branches() {
    let plan_no_where = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan_no_where);
    assert!(exec.where_ok(&event(vec![("sip", str_val("x"))])));

    let mut plan_where = simple_rule_plan(
        "r2",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan_where.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::StringLit("10.0.0.1".into())),
    });
    let exec = RuleExecutor::new(plan_where);
    assert!(exec.where_ok(&event(vec![("sip", str_val("10.0.0.1"))])));
    assert!(!exec.where_ok(&event(vec![("sip", str_val("10.9.9.9"))])));
    // Missing field → None → suppressed.
    assert!(!exec.where_ok(&event(vec![])));
    // Non-bool expression → None → suppressed.
    let mut plan_bad = simple_rule_plan(
        "r3",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan_bad.r#where = Some(Expr::Field(FieldRef::Simple("sip".into())));
    let exec = RuleExecutor::new(plan_bad);
    assert!(!exec.where_ok(&event(vec![("sip", str_val("x"))])));
}

#[test]
fn build_machine_id_and_scope_key_edge_cases() {
    let plan = simple_rule_plan(
        "empty_mid",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan);
    assert_eq!(exec.build_machine_id("").as_ref(), "empty_mid");
    assert_eq!(exec.build_machine_id("m1").as_ref(), "m1");
    // Zero keys → empty scope key string.
    assert_eq!(exec.build_scope_key(&[], &[]).as_ref(), "");
    // Key with a numeric value renders via value_to_string.
    assert_eq!(
        exec.build_scope_key(&[simple_key("dport")], &[num(443.0)])
            .as_ref(),
        "dport=443"
    );
    // Mismatched lengths zip silently.
    assert_eq!(
        exec.build_scope_key(&[simple_key("a"), simple_key("b")], &[num(1.0)])
            .as_ref(),
        "a=1"
    );
}

#[test]
fn clone_recomputes_emit_time_cache() {
    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan);
    let _ = exec.cached_emit_time(123);
    let clone = exec.clone();
    // The clone's cache is empty; a call must still produce a valid value.
    assert!(!clone.cached_emit_time(123).is_empty());
}

// ---------------------------------------------------------------------------
// mod.rs — bind filter / alias matching
// ---------------------------------------------------------------------------

#[test]
fn event_matches_alias_linear_and_map_paths() {
    // ≤24 binds: linear scan path.
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.binds = vec![
        BindPlan {
            alias: "a".into(),
            window: "w".into(),
            filter: None,
        },
        BindPlan {
            alias: "b".into(),
            window: "w".into(),
            filter: Some(Expr::BinOp {
                op: BinOp::Eq,
                left: Box::new(Expr::Field(FieldRef::Qualified("b".into(), "sip".into()))),
                right: Box::new(Expr::StringLit("10.0.0.1".into())),
            }),
        },
    ];
    let exec = RuleExecutor::new(plan);
    let ev = event(vec![("sip", str_val("10.0.0.1"))]);
    // No filter → passes.
    assert!(exec.event_matches_alias("a", &ev, None));
    // Filter true → passes; filter false → rejected.
    assert!(exec.event_matches_alias("b", &ev, None));
    let ev2 = event(vec![("sip", str_val("10.9.9.9"))]);
    assert!(!exec.event_matches_alias("b", &ev2, None));
    // Unknown alias → filter None → passes (matches `None => filter.is_none()`).
    assert!(exec.event_matches_alias("unknown", &ev, None));

    // >24 binds: the precomputed map path.
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let mut binds: Vec<BindPlan> = (0..25)
        .map(|i| BindPlan {
            alias: format!("a{i}"),
            window: "w".into(),
            filter: None,
        })
        .collect();
    binds[24] = BindPlan {
        alias: "a24".into(),
        window: "w".into(),
        filter: Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified("a24".into(), "sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        }),
    };
    plan.binds = binds;
    let exec = RuleExecutor::new(plan);
    assert!(exec.event_matches_alias("a0", &ev, None));
    assert!(exec.event_matches_alias("a24", &ev, None));
    assert!(!exec.event_matches_alias("a24", &event(vec![("sip", str_val("1.1.1.1"))]), None));
}

#[test]
fn bind_filter_columnar_mask_branches() {
    // No filter → None.
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
        filter: None,
    }];
    let exec = RuleExecutor::new(plan);
    let batch = batch_of(vec![("sip", vec![Some("x"), Some("y")])]);
    assert!(exec.bind_filter_columnar_mask("a", &batch).is_none());

    // Non-columnar filter (function call) → None (fall back per-event).
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
        filter: Some(Expr::FuncCall {
            qualifier: None,
            name: "startswith_any".into(),
            args: vec![
                Expr::Field(FieldRef::Simple("sip".into())),
                Expr::StringLit("10.".into()),
                Expr::StringLit("192.168.".into()),
            ],
        }),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(exec.bind_filter_columnar_mask("a", &batch).is_none());

    // Columnar filter → Some(mask).
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
            left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
            right: Box::new(Expr::StringLit("x".into())),
        }),
    }];
    let exec = RuleExecutor::new(plan);
    let mask = exec
        .bind_filter_columnar_mask("a", &batch)
        .expect("columnar mask");
    assert_eq!(mask.len(), 2);
    assert!(mask.value(0));
    assert!(!mask.value(1));
}

#[test]
fn bind_filters_columnar_safe_branches() {
    // All absent → safe.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.binds = vec![
        BindPlan {
            alias: "a".into(),
            window: "w1".into(),
            filter: None,
        },
        BindPlan {
            alias: "b".into(),
            window: "w1".into(),
            filter: None,
        },
    ];
    let exec = RuleExecutor::new(plan);
    assert!(exec.bind_filters_columnar_safe("w1"));
    // Unknown window (no binds) → vacuously safe.
    assert!(exec.bind_filters_columnar_safe("nope"));

    // Columnar filter → safe.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.binds = vec![
        BindPlan {
            alias: "a".into(),
            window: "w1".into(),
            filter: None,
        },
        BindPlan {
            alias: "c".into(),
            window: "w1".into(),
            filter: Some(Expr::Bool(true)),
        },
    ];
    let exec = RuleExecutor::new(plan);
    assert!(exec.bind_filters_columnar_safe("w1"));

    // Non-columnar filter → unsafe.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.binds = vec![
        BindPlan {
            alias: "a".into(),
            window: "w1".into(),
            filter: None,
        },
        BindPlan {
            alias: "d".into(),
            window: "w1".into(),
            filter: Some(Expr::FuncCall {
                qualifier: None,
                name: "upper".into(),
                args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
            }),
        },
    ];
    let exec = RuleExecutor::new(plan);
    assert!(!exec.bind_filters_columnar_safe("w1"));
}

#[test]
fn each_filter_columnar_mask_branches() {
    // No each plan → None.
    let plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan);
    let batch = batch_of(vec![("sip", vec![Some("x"), Some("y")])]);
    assert!(exec.each_filter_columnar_mask(&batch).is_none());

    // Non-columnar each filter → None.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::FuncCall {
            qualifier: None,
            name: "trim".into(),
            args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
        }),
    });
    let exec = RuleExecutor::new(plan);
    assert!(exec.each_filter_columnar_mask(&batch).is_none());

    // Columnar each filter → Some(mask).
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
            right: Box::new(Expr::StringLit("x".into())),
        }),
    });
    let exec = RuleExecutor::new(plan);
    let mask = exec
        .each_filter_columnar_mask(&batch)
        .expect("columnar each mask");
    assert!(mask.value(0));
    assert!(!mask.value(1));
}

#[test]
fn is_aux_bind_alias_branches() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![step(vec![branch("s1", count_ge(1.0))])]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.match_plan.close_steps = vec![step(vec![branch("s2", count_ge(1.0))])];
    let exec = RuleExecutor::new(plan);
    // "s1"/"s2" are branch sources (event/close steps) → not aux.
    assert!(!exec.is_aux_bind_alias("s1"));
    assert!(!exec.is_aux_bind_alias("s2"));
    // "s3" is not referenced by any branch → aux.
    assert!(exec.is_aux_bind_alias("s3"));
}

// ---------------------------------------------------------------------------
// mod.rs — coerce_yield_field_value_with type matrix
// ---------------------------------------------------------------------------

#[test]
fn coerce_yield_value_type_matrix() {
    fn ft(t: &FieldType) -> Option<&FieldType> {
        Some(t)
    }

    // No type → value passes through untouched.
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", None, num(1.0)).unwrap(),
        Some(num(1.0))
    );

    // Chars: string pass-through, other types render to string.
    let chars = FieldType::Base(BaseType::Chars);
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&chars), str_val("x")).unwrap(),
        Some(str_val("x"))
    );
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&chars), num(3.5)).unwrap(),
        Some(str_val("3.5"))
    );
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&chars), Value::Bool(true)).unwrap(),
        Some(str_val("true"))
    );
    // Array/Object render as JSON strings.
    let arr = Value::Array(vec![num(1.0), num(2.0)]);
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&chars), arr.clone()).unwrap(),
        Some(str_val("[1.0,2.0]"))
    );
    let obj = Value::Object(EngineHashMap::from_iter([("k".into(), num(1.0))]));
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&chars), obj.clone()).unwrap(),
        Some(str_val(r#"{"k":1.0}"#))
    );

    // Empty string for non-chars → omitted (Ok(None)).
    let digit = FieldType::Base(BaseType::Digit);
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&digit), str_val("")).unwrap(),
        None
    );

    // Digit: integer number ok; fraction / NaN / non-number rejected.
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&digit), num(3.0)).unwrap(),
        Some(num(3.0))
    );
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&digit), num(3.5)).is_err());
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&digit), num(f64::NAN)).is_err());
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&digit), str_val("3")).is_err());

    // Float: finite ok; NaN/Inf rejected; non-number rejected.
    let float = FieldType::Base(BaseType::Float);
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&float), num(3.5)).unwrap(),
        Some(num(3.5))
    );
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&float), num(f64::NAN)).is_err());
    assert!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&float), num(f64::INFINITY)).is_err()
    );

    // Bool.
    let bool = FieldType::Base(BaseType::Bool);
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&bool), Value::Bool(false)).unwrap(),
        Some(Value::Bool(false))
    );
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&bool), num(1.0)).is_err());

    // Time: valid epoch nanos ok; invalid / non-number rejected.
    let time = FieldType::Base(BaseType::Time);
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&time), num(1.7e18)).unwrap(),
        Some(num(1.7e18))
    );
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&time), num(f64::NAN)).is_err());
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&time), str_val("x")).is_err());

    // Ip: valid literal ok; invalid literal rejected; non-string rejected.
    let ip = FieldType::Base(BaseType::Ip);
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&ip), str_val("10.0.0.1")).unwrap(),
        Some(str_val("10.0.0.1"))
    );
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&ip), str_val("nope")).is_err());
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&ip), num(1.0)).is_err());

    // Hex: number (non-negative integer) or string literal (with/without 0x).
    let hex = FieldType::Base(BaseType::Hex);
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&hex), num(255.0)).unwrap(),
        Some(num(255.0))
    );
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&hex), str_val("0x1F")).unwrap(),
        Some(str_val("0x1F"))
    );
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&hex), str_val("0Xff")).unwrap(),
        Some(str_val("0Xff"))
    );
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&hex), str_val("ff")).unwrap(),
        Some(str_val("ff"))
    );
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&hex), str_val("0xZZ")).is_err());
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&hex), num(-1.0)).is_err());
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&hex), num(1.5)).is_err());
    // Empty string is never a valid hex literal; the empty-string early return
    // treats it as an omitted optional field (Ok(None)) for non-chars targets.
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&hex), str_val("")).unwrap(),
        None
    );

    // Array / ArrayAny: array ok, non-array rejected.
    let array = FieldType::ArrayAny;
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&array), arr).unwrap(),
        Some(Value::Array(vec![num(1.0), num(2.0)]))
    );
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&array), num(1.0)).is_err());

    // Object: object ok, non-object rejected.
    let object = FieldType::Object;
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft(&object), obj).unwrap(),
        Some(Value::Object(EngineHashMap::from_iter([(
            "k".into(),
            num(1.0),
        )])))
    );
    assert!(RuleExecutor::coerce_yield_field_value_with("f", ft(&object), num(1.0)).is_err());
}

// ---------------------------------------------------------------------------
// mod.rs — branch_guard_masks
// ---------------------------------------------------------------------------

fn batch_of(columns: Vec<(&str, Vec<Option<&str>>)>) -> RecordBatch {
    let fields: Vec<ArrowField> = columns
        .iter()
        .map(|(name, _)| ArrowField::new(*name, DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .map(|(_, vals)| Arc::new(StringArray::from(vals)) as ArrayRef)
        .collect();
    RecordBatch::try_new(schema, arrays).unwrap()
}

#[test]
fn branch_guard_masks_event_close_and_seq_neg() {
    use crate::match_engine::columnar::GuardMasks;

    let guard_col = || Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::StringLit("10.0.0.1".into())),
    };
    let guard_noncol = || Expr::FuncCall {
        qualifier: None,
        name: "startswith_any".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("sip".into())),
            Expr::StringLit("10.".into()),
            Expr::StringLit("192.168.".into()),
        ],
    };

    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![],
            vec![
                step(vec![branch_guard("s1", Some(guard_col()), count_ge(1.0))]),
                // Non-columnar guard: skipped (falls back to interpreted).
                step(vec![branch_guard(
                    "s2",
                    Some(guard_noncol()),
                    count_ge(1.0),
                )]),
            ],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.match_plan.close_steps = vec![step(vec![branch_guard(
        "s3",
        Some(guard_col()),
        count_ge(1.0),
    )])];
    plan.match_plan.seq = Some(SeqPlan {
        consec: false,
        skip: SeqSkipPlan::PastLast,
        steps: vec![
            SeqStepPlan {
                neg: true,
                within: None,
                branch: branch_guard("s4", Some(guard_col()), count_ge(1.0)),
            },
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch_guard("s5", Some(guard_noncol()), count_ge(1.0)),
            },
        ],
    });

    let exec = RuleExecutor::new(plan);
    let batch = batch_of(vec![("sip", vec![Some("10.0.0.1"), Some("10.9.9.9")])]);
    let masks: GuardMasks = exec.branch_guard_masks(&batch);
    // Event step (0,0) columnar guard present; (1,0) non-columnar absent.
    assert_eq!(masks.event_value(0, 0, 0), Some(true));
    assert_eq!(masks.event_value(0, 0, 1), Some(false));
    assert_eq!(masks.event_value(1, 0, 0), None);
    // Close step (0,0) columnar guard present.
    assert_eq!(masks.close_value(0, 0, 0), Some(Some(true)));
    assert_eq!(masks.close_value(0, 0, 1), Some(Some(false)));
    // Negation index counts only neg steps: step0 (neg) columnar → neg(0,0);
    // step1 (non-neg) skipped.
    assert_eq!(masks.neg_value(0, 0, 0), Some(true));
    assert_eq!(masks.neg_value(0, 0, 1), Some(false));
    assert!(!masks.is_empty());
}

#[test]
fn branch_guard_masks_empty_without_guards_or_seq() {
    let plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![step(vec![branch("s1", count_ge(1.0))])]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan);
    let batch = batch_of(vec![("sip", vec![Some("x")])]);
    let masks = exec.branch_guard_masks(&batch);
    assert!(masks.is_empty());
}

#[test]
fn branch_guard_masks_noncolumnar_only_early_returns() {
    // 2026-08-31 lazy 视图：规则**有 guard 但全部非列式**时同样提前返回空掩码
    // （`has_columnar_guard` 为 false）——状态机回退解释求值，语义不变，但
    // 跳过 `ColumnarBatch::from_all_fields` 视图构建（无 guard 规则每批每规则
    // 的浪费，lazy 化目标）。
    let guard_noncol = || Expr::FuncCall {
        qualifier: None,
        name: "startswith_any".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("sip".into())),
            Expr::StringLit("10.".into()),
            Expr::StringLit("192.168.".into()),
        ],
    };
    let plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![],
            vec![step(vec![branch_guard(
                "s1",
                Some(guard_noncol()),
                count_ge(1.0),
            )])],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan);
    let batch = batch_of(vec![("sip", vec![Some("10.0.0.1")])]);
    let masks = exec.branch_guard_masks(&batch);
    assert!(masks.is_empty(), "非列式 guard → 空掩码 → 解释回退");
}

#[test]
fn branch_guard_masks_list_index_path_guard() {
    use crate::match_engine::columnar::GuardMasks;
    use crate::match_engine::{WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY};

    // The qradar `c && c.tags[0] == "prod"` guard: `c` is the step source, so
    // the guard AST is just `c.tags[0] == "prod"` — a list-index Path.
    let guard = || Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Path {
            alias: "c".into(),
            segments: vec![PathSegment::Field("tags".into()), PathSegment::Index(0)],
        })),
        right: Box::new(Expr::StringLit("prod".into())),
    };
    assert!(wf_lang::columnar::expr_is_columnar(&guard()));

    let mut plan = simple_rule_plan(
        "r_list_index",
        simple_plan(
            vec![],
            vec![step(vec![branch_guard("c", Some(guard()), count_ge(1.0))])],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.match_plan.close_steps = vec![step(vec![branch_guard("c", Some(guard()), count_ge(1.0))])];
    let exec = RuleExecutor::new(plan);

    // `tags` is a structured JSON-array column (the frame storage shape): row 0
    // hits, row 1 misses, row 2 is a null cell, row 3 is out of range.
    let tags_col = Arc::new(StringArray::from(vec![
        Some(r#"["prod","edge","dmz"]"#),
        Some(r#"["edge"]"#),
        None,
        Some(r#"[]"#),
    ])) as ArrayRef;
    let field = ArrowField::new("tags", DataType::Utf8, true).with_metadata(HashMap::from([(
        WFL_FIELD_TYPE_METADATA_KEY.to_string(),
        WFL_FIELD_TYPE_ARRAY.to_string(),
    )]));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![tags_col]).unwrap();

    let masks: GuardMasks = exec.branch_guard_masks(&batch);
    // Event step (0,0): row 0 matched; rows 1-3 null / miss → not matched.
    assert_eq!(masks.event_value(0, 0, 0), Some(true));
    assert_eq!(masks.event_value(0, 0, 1), Some(false));
    assert_eq!(masks.event_value(0, 0, 2), Some(false));
    assert_eq!(masks.event_value(0, 0, 3), Some(false));
    // Close step: the matching row is a definite true, a miss is a definite
    // false, and null / out-of-range rows stay permissive (null slot) — the
    // null-vs-definite-false distinction close-step accumulation relies on.
    assert_eq!(masks.close_value(0, 0, 0), Some(Some(true)));
    assert_eq!(masks.close_value(0, 0, 1), Some(Some(false)));
    assert_eq!(masks.close_value(0, 0, 2), Some(None));
    assert_eq!(masks.close_value(0, 0, 3), Some(None));
}
