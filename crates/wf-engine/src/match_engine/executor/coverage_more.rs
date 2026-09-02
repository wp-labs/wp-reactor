//! Round-2 coverage-fill tests for `executor/mod.rs` internals: the
//! `plan_close_ctx_fields` / `visit_expr_fields` branches (underscore-prefixed
//! fields, `$param` preset params, nested expression shapes), the
//! `new_with_yield_field_types_and_output` constructor, and the
//! `render_yield_value_as_string` / `yield_value_to_json` non-finite-number
//! error lanes reachable through `coerce_yield_field_value_with`.

use std::collections::{HashMap, HashSet};

use wf_lang::ast::{CloseMode, CmpOp, Expr, FieldRef, MatchMode, Measure};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StepPlan,
    WindowSpec, YieldPlan,
};
use wf_lang::{BaseType, FieldType};

use super::{CloseCtxFields, plan_close_ctx_fields};
use crate::match_engine::RuleExecutor;
use crate::match_engine::match_engine::{EngineHashMap, Value};

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn str_val(s: &str) -> Value {
    Value::Str(s.to_string().into())
}

fn simple_key(name: &str) -> FieldRef {
    FieldRef::Simple(name.to_string())
}

fn count_ge(n: f64) -> AggPlan {
    AggPlan {
        transforms: vec![],
        measure: Measure::Count,
        cmp: CmpOp::Ge,
        threshold: Expr::Number(n),
    }
}

fn branch(source: &str, agg: AggPlan) -> BranchPlan {
    BranchPlan {
        label: None,
        source: source.to_string(),
        field: None,
        guard: None,
        agg,
    }
}

fn step(branches: Vec<BranchPlan>) -> StepPlan {
    StepPlan { branches }
}

fn match_plan(keys: Vec<FieldRef>, event_steps: Vec<StepPlan>) -> MatchPlan {
    MatchPlan {
        keys,
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(std::time::Duration::from_secs(300)),
        event_steps,
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

fn default_match_plan() -> MatchPlan {
    match_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    )
}

fn simple_rule_plan(
    name: &str,
    match_plan: MatchPlan,
    score_expr: Expr,
    entity_type: &str,
    entity_id_expr: Expr,
) -> RulePlan {
    RulePlan {
        conv_window: None,
        name: name.to_string(),
        binds: vec![BindPlan {
            alias: "fail".to_string(),
            window: "w".to_string(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan,
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: entity_type.to_string(),
            entity_id_expr,
        },
        yield_plan: YieldPlan {
            target: "alerts".to_string(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan { expr: score_expr },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    }
}

// ---------------------------------------------------------------------------
// plan_close_ctx_fields / visit_expr_fields branches
// ---------------------------------------------------------------------------

#[test]
fn close_ctx_fields_plain_fields_narrow_to_named_set() {
    let plan = simple_rule_plan(
        "r",
        default_match_plan(),
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let mut plan = plan;
    plan.yield_plan.fields = vec![wf_lang::plan::YieldField {
        name: "v".into(),
        value: Expr::Field(FieldRef::Simple("dport".into())),
    }];
    match plan_close_ctx_fields(&plan) {
        CloseCtxFields::Named(names) => {
            assert!(names.contains("sip"));
            assert!(names.contains("dport"));
            assert!(!names.contains("_step_0_values"));
        }
        CloseCtxFields::All => panic!("plain fields must narrow to Named"),
    }
}

#[test]
fn close_ctx_fields_underscore_and_preset_param_force_all() {
    // A `_`-prefixed field name is treated as reserved/synthetic → all-fields.
    let plan_underscore = simple_rule_plan(
        "r",
        default_match_plan(),
        Expr::Field(FieldRef::Simple("_internal".into())),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    assert!(
        matches!(plan_close_ctx_fields(&plan_underscore), CloseCtxFields::All),
        "underscore-prefixed field forces the all-fields build"
    );

    // A `$param` preset reference may read anything → all-fields.
    let plan_param = simple_rule_plan(
        "r",
        default_match_plan(),
        Expr::PresetParam("severity".into()),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    assert!(
        matches!(plan_close_ctx_fields(&plan_param), CloseCtxFields::All),
        "PresetParam forces the all-fields build"
    );
}

#[test]
fn close_ctx_fields_nested_expr_shapes_walk_visitors() {
    // BinOp / Neg / Array / Object / InList / IfThenElse recurse through
    // visit_expr_fields; only Field leaves contribute names.
    let nested = Expr::IfThenElse {
        cond: Box::new(Expr::BinOp {
            op: wf_lang::ast::BinOp::Gt,
            left: Box::new(Expr::Field(FieldRef::Simple("score".into()))),
            right: Box::new(Expr::Number(50.0)),
        }),
        then_expr: Box::new(Expr::Array(vec![Expr::Field(FieldRef::Simple("a".into()))])),
        else_expr: Box::new(Expr::InList {
            expr: Box::new(Expr::Neg(Box::new(Expr::Field(FieldRef::Simple(
                "b".into(),
            ))))),
            list: vec![Expr::Object(vec![wf_lang::ast::ObjectItem {
                targets: vec!["k".into()],
                type_hint: None,
                value: Expr::Field(FieldRef::Simple("c".into())),
            }])],
            negated: false,
        }),
    };
    let plan = simple_rule_plan(
        "r",
        default_match_plan(),
        nested,
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    match plan_close_ctx_fields(&plan) {
        CloseCtxFields::Named(names) => {
            for expected in ["score", "a", "b", "c", "sip"] {
                assert!(names.contains(expected), "missing {expected}");
            }
        }
        CloseCtxFields::All => panic!("function-free nested fields must narrow"),
    }
}

// ---------------------------------------------------------------------------
// new_with_yield_field_types_and_output
// ---------------------------------------------------------------------------

#[test]
fn constructor_with_output_config_builds_executor() {
    let plan = simple_rule_plan(
        "r",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new_with_yield_field_types_and_output(
        plan,
        HashMap::from([("v".into(), FieldType::Base(BaseType::Float))]),
        wf_config::OutputConfig::default(),
    );
    assert_eq!(exec.plan().name, "r");
    assert_eq!(exec.static_yield_target().as_ref(), "alerts");
    assert_eq!(exec.output_static().score_const, Some(50.0));
    assert!(exec.output_static().each_summary.is_none());
}

// ---------------------------------------------------------------------------
// render_yield_value_as_string / yield_value_to_json — non-finite errors
// ---------------------------------------------------------------------------

#[test]
fn chars_coercion_rejects_non_finite_numbers_in_structured_values() {
    let chars = FieldType::Base(BaseType::Chars);
    let ft = Some(&chars);

    // A plain non-finite number cannot render to a string.
    let err = RuleExecutor::coerce_yield_field_value_with("f", ft, num(f64::NAN));
    assert!(err.is_err(), "NaN must not stringify");

    // A nested non-finite number inside an Array fails JSON serialization.
    let arr = Value::Array(vec![num(1.0), num(f64::INFINITY)]);
    let err = RuleExecutor::coerce_yield_field_value_with("f", ft, arr);
    assert!(err.is_err(), "nested infinity must fail JSON conversion");

    // A nested non-finite number inside an Object fails too.
    let obj = Value::Object(EngineHashMap::from_iter([(
        "k".into(),
        Value::Array(vec![num(f64::NEG_INFINITY)]),
    )]));
    let err = RuleExecutor::coerce_yield_field_value_with("f", ft, obj);
    assert!(err.is_err(), "nested -infinity must fail JSON conversion");

    // Finite structured values still render.
    let ok = Value::Object(EngineHashMap::from_iter([("k".into(), num(1.0))]));
    assert_eq!(
        RuleExecutor::coerce_yield_field_value_with("f", ft, ok).unwrap(),
        Some(str_val(r#"{"k":1.0}"#))
    );
}
