//! Round-2 coverage-fill tests for `executor/close_exec.rs` — the branches
//! `executor/coverage_extra.rs` does not reach:
//!
//! - `build_close_alert` with a **non-constant score** (eval_score path):
//!   numeric field → Ok; non-numeric → error;
//! - entity id: missing plain field → empty string on the per-record path;
//!   non-field expression (function call) → `eval_entity_id` stringify;
//! - coerce `Ok(None)` → optional missing yield field omitted from the record;
//! - `execute_close_direct_batch_columnar` literal **Bool / StringLit** const
//!   columns, and the per-row `stage_yield_cell` error path (untyped object
//!   with a non-finite number passes coerce, fails at export → `failed++`).
use std::sync::Arc;

use std::collections::HashMap;

use wf_lang::ast::{CloseMode, CmpOp, Expr, FieldRef, MatchMode, Measure};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StepPlan,
    WindowSpec, YieldField, YieldPlan,
};
use wf_lang::{BaseType, FieldType};

use crate::alert::AlertColumnBuilder;
use crate::match_engine::RuleExecutor;
use crate::match_engine::match_engine::{CloseOutput, CloseReason, EngineHashMap, StepData, Value};

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

fn simple_plan(keys: Vec<FieldRef>, steps: Vec<StepPlan>) -> MatchPlan {
    MatchPlan {
        keys,
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(std::time::Duration::from_secs(300)),
        event_steps: steps,
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: std::collections::HashSet::new(),
        tracked_bind_fields: HashMap::new(),
        tracked_plain_fields: std::collections::HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    }
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

fn close_output(event_step_data: Vec<StepData>, close_step_data: Vec<StepData>) -> CloseOutput {
    CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data,
        close_step_data,
        bind_data: vec![],
        watermark_nanos: 0,
        machine_id: String::new(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        last_event_nanos: 123,
        row_fields: None,
        row_field_names: None,
    }
}

fn step_data(
    label: Option<&str>,
    measure_value: f64,
    field_values: EngineHashMap<String, Vec<Value>>,
) -> StepData {
    StepData {
        satisfied_branch_index: 0,
        label: label.map(|s| s.to_string()),
        measure_value,
        event_first_time_nanos: None,
        event_last_time_nanos: None,
        collected_values: Vec::new(),
        field_values,
    }
}

/// One event step `fail count >= 1` (label "fail", measure 1.0) + close data.
fn default_close() -> CloseOutput {
    close_output(
        vec![step_data(Some("fail"), 1.0, EngineHashMap::default())],
        vec![],
    )
}

fn default_plan() -> RulePlan {
    simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    )
}

#[test]
fn close_alert_non_constant_score_lanes() {
    // Score from a numeric field (the step label measure) → eval_score Ok.
    let mut plan = default_plan();
    plan.score_plan = ScorePlan {
        expr: Expr::Field(FieldRef::Simple("fail".into())),
    };
    let exec = RuleExecutor::new(plan);
    let rec = exec.execute_close(&default_close()).unwrap().unwrap();
    assert_eq!(rec.score, 1.0);

    // Score from a function call that yields a non-numeric value → error.
    let mut plan = default_plan();
    plan.score_plan = ScorePlan {
        expr: Expr::FuncCall {
            qualifier: None,
            name: "upper".into(),
            args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
        },
    };
    let exec = RuleExecutor::new(plan);
    let err = exec
        .execute_close(&default_close())
        .expect_err("non-numeric score must error");
    assert!(err.to_string().contains("non-numeric"), "{err}");
}

#[test]
fn close_alert_entity_id_lanes() {
    // Missing plain field → empty string (per-record path).
    let mut plan = default_plan();
    plan.entity_plan.entity_id_expr = Expr::Field(FieldRef::Simple("absent".into()));
    let exec = RuleExecutor::new(plan);
    let rec = exec.execute_close(&default_close()).unwrap().unwrap();
    assert_eq!(rec.entity_id, "");

    // Non-field expression → eval_entity_id stringifies the result.
    let mut plan = default_plan();
    plan.entity_plan.entity_id_expr = Expr::FuncCall {
        qualifier: None,
        name: "upper".into(),
        args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
    };
    let exec = RuleExecutor::new(plan);
    let rec = exec.execute_close(&default_close()).unwrap().unwrap();
    assert_eq!(rec.entity_id, "10.0.0.1");
}

#[test]
fn close_alert_omits_optional_missing_yield_field() {
    // A yield typed Float whose source field is absent everywhere: the eval
    // falls back to an empty string, coerce returns Ok(None) → the field is
    // omitted (not an error, not an empty cell).
    let mut plan = default_plan();
    plan.yield_plan.fields = vec![YieldField {
        name: "fv".into(),
        value: Expr::Field(FieldRef::Simple("missing_field".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("fv".into(), FieldType::Base(BaseType::Float))]),
    );
    let rec = exec.execute_close(&default_close()).unwrap().unwrap();
    assert!(
        rec.yield_fields.is_empty(),
        "optional missing field must be omitted, got {:?}",
        rec.yield_fields
    );
}

#[test]
fn close_direct_batch_columnar_literal_bool_and_string_columns() {
    // Bool / StringLit / Number literal yields become batch-constant columns.
    let mut plan = default_plan();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "b".into(),
            value: Expr::Bool(true),
        },
        YieldField {
            name: "s".into(),
            value: Expr::StringLit("lit".into()),
        },
        YieldField {
            name: "n".into(),
            value: Expr::Number(9.0),
        },
    ];
    let exec = RuleExecutor::new(plan);
    assert!(exec.close_plan_columnar_safe());

    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[default_close()], &mut builder, 0);
    assert_eq!(stats.appended, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(builder.len(), 1);

    let batch = builder.finish();
    let mut records = batch.iter_data_records().collect::<Vec<_>>();
    let record = records.pop().unwrap().unwrap();
    assert_eq!(
        record.get_value("b"),
        Some(&wp_model_core::model::Value::from(true))
    );
    assert_eq!(
        record.get_value("s"),
        Some(&wp_model_core::model::Value::from("lit"))
    );
    assert_eq!(
        record.get_value("n"),
        Some(&wp_model_core::model::Value::from(9.0_f64))
    );
}

#[test]
fn close_direct_batch_columnar_stage_error_marks_row_failed() {
    // An untyped yield whose value is an object containing a non-finite
    // number: coerce passes it through (no type), but export fails at
    // `stage_yield_cell` → `stats.failed` increments and the row is
    // **skipped** (no columns touched, not appended — B1 fix).
    let mut plan = default_plan();
    plan.yield_plan.fields = vec![YieldField {
        name: "obj".into(),
        value: Expr::Field(FieldRef::Simple("risk".into())),
    }];
    // `risk` resolves from close step field_values as an object with NaN.
    let mut fv = EngineHashMap::default();
    fv.insert(
        "risk".to_string(),
        vec![Value::Object(EngineHashMap::from_iter([(
            "score".into(),
            num(f64::NAN),
        )]))],
    );
    let close = close_output(vec![], vec![step_data(Some("c1"), 1.0, fv)]);
    let exec = RuleExecutor::new(plan);

    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[close], &mut builder, 0);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.appended, 0);
    assert!(builder.is_empty());
}
