//! Coverage-fill tests for the executor layer (`executor/mod.rs` internals,
//! `context.rs` join execution, `close_exec.rs`, `each_exec.rs`,
//! `deferred_exec.rs`, `match_exec.rs`).
//!
//! These tests drive the real executor entry points (`RuleExecutor::execute_*`,
//! `execute_joins`, `build_eval_context`, ...) with constructed plans /
//! contexts / events, focusing on the error paths, boundary conditions, and
//! configuration branches that the equivalence-focused tests in
//! `tests/executor/` do not reach.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{
    BinOp, Bound, BoundVal, CloseMode, CmpOp, Expr, FieldRef, JoinMode, MatchMode, Measure,
    PathSegment, ReduceClause, ReduceMeasure, SystemVar, TieSpec, WithinSpec,
};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EachPlan, EntityPlan, JoinCondPlan, JoinPlan, LetPlan,
    MatchPlan, RulePlan, ScorePlan, SeqPlan, SeqSkipPlan, SeqStepPlan, StepPlan, WindowSpec,
    YieldField, YieldPlan,
};
use wf_lang::{BaseType, FieldType};

use crate::alert::AlertColumnBuilder;
use crate::match_engine::event_bridge::ColumnarEvent;
use crate::match_engine::match_engine::{
    AsofLookup, BindData, CepStateMachine, CloseOutput, CloseReason, EngineHashMap, Event,
    MatchedContext, StepData, StepResult, Value, WindowLookup,
};
use crate::match_engine::{JoinRow, RuleExecutor};

// ---------------------------------------------------------------------------
// Helpers (local copies — `tests::helpers` is not reachable from this module)
// ---------------------------------------------------------------------------

fn event(fields: Vec<(&str, Value)>) -> Event {
    Event {
        fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    }
}

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

fn branch_guard(source: &str, guard: Option<Expr>, agg: AggPlan) -> BranchPlan {
    BranchPlan {
        label: None,
        source: source.to_string(),
        field: None,
        guard,
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
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps: steps,
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

fn plan_with_close(
    keys: Vec<FieldRef>,
    event_steps: Vec<StepPlan>,
    close_steps: Vec<StepPlan>,
) -> MatchPlan {
    MatchPlan {
        keys,
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps,
        close_steps,
        close_mode: CloseMode::And,
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

/// Minimal rule plan (mirrors `tests::helpers::simple_rule_plan`).
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

/// Default match plan: single key `sip`, one step `fail count >= 1`.
fn default_match_plan() -> MatchPlan {
    simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    )
}

fn default_matched_context() -> MatchedContext {
    MatchedContext {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 1.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        event_time_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        trigger_event: None,
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

fn close_output(
    event_ok: bool,
    close_ok: bool,
    close_mode: CloseMode,
    event_step_data: Vec<StepData>,
    close_step_data: Vec<StepData>,
) -> CloseOutput {
    CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok,
        close_ok,
        close_mode,
        event_emitted: false,
        event_step_data,
        close_step_data,
        bind_data: vec![],
        watermark_nanos: 0,
        machine_id: String::new(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        last_event_nanos: 123,
    }
}

/// `on each` rule: score 42.5, entity = `e.sip`, two float yields.
fn each_plan_rule() -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "q1_pass",
        simple_plan(vec![], vec![]),
        Expr::Number(42.5),
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
            name: "price".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "price".into())),
        },
    ];
    RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("auction_id".into(), FieldType::Base(BaseType::Float)),
            ("price".into(), FieldType::Base(BaseType::Float)),
        ]),
    )
}

struct EmptyLookup;
impl WindowLookup for EmptyLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        None
    }
}

/// Join lookup holding a fixed candidate set; `asof_lookup_max` outcome is
/// configurable (default `Fallback` → candidate scan).
struct RowsLookup {
    rows: Vec<JoinRow>,
    ts_rows: Vec<(i64, JoinRow)>,
    asof_outcome: Option<AsofLookup>,
}

impl RowsLookup {
    fn new(rows: Vec<JoinRow>) -> Self {
        Self {
            rows,
            ts_rows: Vec::new(),
            asof_outcome: None,
        }
    }
    fn with_ts(ts_rows: Vec<(i64, JoinRow)>) -> Self {
        Self {
            rows: ts_rows.iter().map(|(_, r)| r.clone()).collect(),
            ts_rows,
            asof_outcome: None,
        }
    }
}

impl WindowLookup for RowsLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        Some(self.rows.clone())
    }
    fn snapshot_with_timestamps(&self, _w: &str) -> Option<Vec<(i64, JoinRow)>> {
        Some(self.ts_rows.clone())
    }
    fn join_lookup(&self, _w: &str, key_field: &str, key: &Value) -> Option<Vec<JoinRow>> {
        Some(
            self.rows
                .iter()
                .filter(|row| {
                    row.field_value(key_field)
                        .is_some_and(|v| crate::match_engine::values_equal(&v, key))
                })
                .cloned()
                .collect(),
        )
    }
    fn asof_candidates(&self, _w: &str, _kf: &str, _k: &Value) -> Option<Vec<(i64, JoinRow)>> {
        Some(self.ts_rows.clone())
    }
    fn asof_lookup_max(
        &self,
        _w: &str,
        _kf: &str,
        _k: &Value,
        _t: i64,
        _within: Option<&Duration>,
    ) -> AsofLookup {
        self.asof_outcome.clone().unwrap_or(AsofLookup::Fallback)
    }
}

fn join_row(key: &str, key_val: f64, extra: Vec<(&str, Value)>) -> JoinRow {
    let mut fields = EngineHashMap::default();
    fields.insert(key.into(), num(key_val));
    for (name, value) in extra {
        fields.insert(name.into(), value);
    }
    JoinRow::Event(Arc::new(Event { fields }))
}

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
    assert_eq!(exec.build_machine_id(""), "empty_mid");
    assert_eq!(exec.build_machine_id("m1"), "m1");
    // Zero keys → empty scope key string.
    assert_eq!(exec.build_scope_key(&[], &[]), "");
    // Key with a numeric value renders via value_to_string.
    assert_eq!(
        exec.build_scope_key(&[simple_key("dport")], &[num(443.0)]),
        "dport=443"
    );
    // Mismatched lengths zip silently.
    assert_eq!(
        exec.build_scope_key(&[simple_key("a"), simple_key("b")], &[num(1.0)]),
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
            name: "startswith".into(),
            args: vec![
                Expr::Field(FieldRef::Simple("sip".into())),
                Expr::StringLit("10.".into()),
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
        name: "startswith".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("sip".into())),
            Expr::StringLit("10.".into()),
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

// ---------------------------------------------------------------------------
// context.rs — build_eval_context
// ---------------------------------------------------------------------------

#[test]
fn build_eval_context_all_and_named_synthetic_fields() {
    use crate::match_engine::executor::context::{CloseCtxFields, build_eval_context};

    let keys = vec![simple_key("sip"), simple_key("dport")];
    let scope_key = vec![str_val("10.0.0.1"), num(443.0)];
    let mut fv1 = EngineHashMap::default();
    fv1.insert("price".into(), vec![num(1.0), num(2.0), num(3.0)]);
    let sd1 = step_data(Some("login"), 3.0, fv1);
    let mut fv2 = EngineHashMap::default();
    fv2.insert("price".into(), vec![num(9.0)]);
    let sd2 = step_data(Some("brute"), 1.0, fv2);

    let step_plan_login = StepPlan {
        branches: vec![BranchPlan {
            label: Some("login".into()),
            source: "e".into(),
            field: None,
            guard: None,
            agg: count_ge(1.0),
        }],
    };
    let step_plan_brute = StepPlan {
        branches: vec![BranchPlan {
            label: Some("brute".into()),
            source: "f".into(),
            field: None,
            guard: None,
            agg: count_ge(1.0),
        }],
    };
    let step_plans = vec![&step_plan_login, &step_plan_brute];

    // All build: every synthetic field present.
    let all = CloseCtxFields::All;
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[sd1.clone(), sd2.clone()],
        &[],
        &step_plans,
        None,
        &all,
    );
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));
    assert_eq!(ctx.fields.get("dport"), Some(&num(443.0)));
    assert_eq!(ctx.fields.get("login"), Some(&num(3.0)));
    assert_eq!(ctx.fields.get("brute"), Some(&num(1.0)));
    // L3 collected values / measure / label / source fields.
    assert_eq!(
        ctx.fields.get("_step_0_values"),
        Some(&Value::Array(vec![]))
    );
    assert_eq!(ctx.fields.get("_step_0_measure"), Some(&num(3.0)));
    assert_eq!(ctx.fields.get("_step_0_label"), Some(&str_val("login")));
    assert_eq!(ctx.fields.get("_step_0_source"), Some(&str_val("e")));
    // Per-field history arrays + last-value injection.
    assert_eq!(
        ctx.fields.get("_step_0_field_price"),
        Some(&Value::Array(vec![num(1.0), num(2.0), num(3.0)]))
    );
    assert_eq!(ctx.fields.get("price"), Some(&num(3.0)));
    // Keys are not overwritten by a colliding label.
    // Colliding key/label: label must not overwrite the key.
    let colliding = step_data(Some("sip"), 99.0, EngineHashMap::default());
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[colliding],
        &[],
        &[&StepPlan { branches: vec![] }],
        None,
        &all,
    );
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));
    assert_eq!(ctx.fields.get("_step_0_measure"), Some(&num(99.0)));

    // Trigger event scalars are included (keys win).
    let trigger = event(vec![("sip", str_val("override")), ("raw", num(7.0))]);
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[sd1.clone()],
        &[],
        &[&StepPlan { branches: vec![] }],
        Some(&trigger),
        &all,
    );
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));
    assert_eq!(ctx.fields.get("raw"), Some(&num(7.0)));

    // Named build: only requested names materialized (last values only).
    let named = CloseCtxFields::Named(HashSet::from(["price".to_string()]));
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[sd1, sd2],
        &[],
        &step_plans,
        None,
        &named,
    );
    assert_eq!(ctx.fields.get("price"), Some(&num(3.0)));
    assert!(ctx.fields.get("login").is_none(), "label not requested");
    assert!(ctx.fields.get("_step_0_measure").is_none());
    // Keys are always present.
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));

    // Bind data: _bind_<alias>_count / _bind_<alias>_field_<name> + last value.
    let bd = BindData {
        alias: "win".to_string(),
        count: 2,
        field_values: EngineHashMap::from_iter([(
            "amount".to_string(),
            vec![num(10.0), num(20.0)],
        )]),
    };
    let all = CloseCtxFields::All;
    let ctx = build_eval_context(&keys, &scope_key, &[], &[bd.clone()], &[], None, &all);
    assert_eq!(ctx.fields.get("_bind_win_count"), Some(&num(2.0)));
    assert_eq!(
        ctx.fields.get("_bind_win_field_amount"),
        Some(&Value::Array(vec![num(10.0), num(20.0)]))
    );
    assert_eq!(ctx.fields.get("amount"), Some(&num(20.0)));
    let named = CloseCtxFields::Named(HashSet::from(["amount".to_string()]));
    let ctx = build_eval_context(&keys, &scope_key, &[], &[bd], &[], None, &named);
    assert_eq!(ctx.fields.get("amount"), Some(&num(20.0)));
    assert!(ctx.fields.get("_bind_win_count").is_none());
}

// ---------------------------------------------------------------------------
// context.rs — execute_joins
// ---------------------------------------------------------------------------

#[test]
fn execute_joins_inner_snapshot_anti_modes() {
    use crate::match_engine::executor::context::execute_joins;

    let cond = |left: &str, right: &str| JoinCondPlan {
        left: FieldRef::Simple(left.to_string()),
        right: FieldRef::Qualified("w".into(), right.to_string()),
    };
    let jp = |mode: JoinMode, conds: Vec<JoinCondPlan>| JoinPlan {
        right_window: "w".to_string(),
        mode,
        conds,
        within: None,
        reduce: None,
        emit_at: None,
    };

    // Inner hit: enriches; miss: drops (returns false).
    let lookup = RowsLookup::new(vec![join_row("id", 1.0, vec![("amt", num(5.0))])]);
    let joins = vec![jp(JoinMode::Inner, vec![cond("key", "id")])];
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&joins, &mut ctx, &lookup, 0));
    assert_eq!(ctx.fields.get("w.amt"), Some(&num(5.0)));
    assert_eq!(ctx.fields.get("amt"), Some(&num(5.0)));
    let mut ctx = event(vec![("key", num(9.0))]);
    assert!(!execute_joins(&joins, &mut ctx, &lookup, 0));

    // Inner with missing key field / no rows → drop.
    let mut ctx = event(vec![]);
    assert!(!execute_joins(&joins, &mut ctx, &lookup, 0));
    let mut ctx = event(vec![("key", num(2.0))]);
    assert!(!execute_joins(&joins, &mut ctx, &lookup, 0));

    // Snapshot: miss keeps the event unenriched.
    let joins = vec![jp(JoinMode::Snapshot, vec![cond("key", "id")])];
    let mut ctx = event(vec![("key", num(9.0))]);
    assert!(execute_joins(&joins, &mut ctx, &lookup, 0));
    assert!(ctx.fields.get("amt").is_none());
    let mut ctx = event(vec![]);
    assert!(execute_joins(&joins, &mut ctx, &lookup, 0));

    // Snapshot with no window data → keep.
    let joins = vec![jp(JoinMode::Snapshot, vec![cond("key", "id")])];
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&joins, &mut ctx, &EmptyLookup, 0));

    // Anti: matching row drops; no row keeps.
    let joins = vec![jp(JoinMode::Anti, vec![cond("key", "id")])];
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(!execute_joins(&joins, &mut ctx, &lookup, 0));
    let mut ctx = event(vec![("key", num(7.0))]);
    assert!(execute_joins(&joins, &mut ctx, &lookup, 0));
    // Anti without window data → keep.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&joins, &mut ctx, &EmptyLookup, 0));
    // Anti with missing key → keep (continue).
    let mut ctx = event(vec![]);
    assert!(execute_joins(&joins, &mut ctx, &lookup, 0));

    // Deferred (`emit_at`) joins are skipped on the eager path entirely.
    let joins = vec![JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Inner,
        conds: vec![cond("key", "id")],
        within: None,
        reduce: None,
        emit_at: Some(Expr::Number(1.0)),
    }];
    let mut ctx = event(vec![("key", num(9.0))]);
    assert!(execute_joins(&joins, &mut ctx, &lookup, 0));
    assert!(ctx.fields.get("amt").is_none());
}

#[test]
fn execute_joins_asof_single_cond_hit_miss_fallback() {
    use crate::match_engine::executor::context::execute_joins;

    let cond = JoinCondPlan {
        left: FieldRef::Simple("key".into()),
        right: FieldRef::Qualified("w".into(), "id".into()),
    };
    let jp = |within: Option<Duration>| JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Asof { within },
        conds: vec![cond.clone()],
        within: None,
        reduce: None,
        emit_at: None,
    };
    let row = join_row("id", 1.0, vec![("amt", num(5.0))]);

    // Fast-path Hit.
    let lookup = RowsLookup {
        rows: vec![row.clone()],
        ts_rows: vec![],
        asof_outcome: Some(AsofLookup::Hit(row.clone())),
    };
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&vec![jp(None)], &mut ctx, &lookup, 1_000));
    assert_eq!(ctx.fields.get("amt"), Some(&num(5.0)));

    // Fast-path Miss → None (no enrichment, keep).
    let lookup = RowsLookup {
        rows: vec![row.clone()],
        ts_rows: vec![],
        asof_outcome: Some(AsofLookup::Miss),
    };
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&vec![jp(None)], &mut ctx, &lookup, 1_000));
    assert!(ctx.fields.get("amt").is_none());

    // Fallback → candidate scan; picks the latest ts ≤ event_time.
    let lookup = RowsLookup::with_ts(vec![
        (100, join_row("id", 1.0, vec![("amt", num(1.0))])),
        (200, join_row("id", 1.0, vec![("amt", num(2.0))])),
        (300, join_row("id", 1.0, vec![("amt", num(3.0))])),
        (999, join_row("id", 9.0, vec![("amt", num(99.0))])),
    ]);
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&vec![jp(None)], &mut ctx, &lookup, 250));
    assert_eq!(ctx.fields.get("amt"), Some(&num(2.0)));

    // `within` filters older rows: latest within [250-100, 250] is ts=200.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &vec![jp(Some(Duration::from_secs(100)))],
        &mut ctx,
        &lookup,
        250
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(2.0)));

    // Asof with missing key → keep unenriched (continue).
    let mut ctx = event(vec![]);
    assert!(execute_joins(&vec![jp(None)], &mut ctx, &lookup, 250));

    // Asof with no candidates → keep unenriched.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&vec![jp(None)], &mut ctx, &EmptyLookup, 250));
}

#[test]
fn execute_joins_asof_multi_cond_uses_scan() {
    use crate::match_engine::executor::context::execute_joins;

    let conds = vec![
        JoinCondPlan {
            left: FieldRef::Simple("key".into()),
            right: FieldRef::Qualified("w".into(), "id".into()),
        },
        JoinCondPlan {
            left: FieldRef::Simple("chan".into()),
            right: FieldRef::Qualified("w".into(), "channel".into()),
        },
    ];
    let join = JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Asof { within: None },
        conds,
        within: None,
        reduce: None,
        emit_at: None,
    };
    let lookup = RowsLookup::with_ts(vec![
        (
            100,
            join_row(
                "id",
                1.0,
                vec![("channel", str_val("a")), ("amt", num(1.0))],
            ),
        ),
        (
            200,
            join_row(
                "id",
                1.0,
                vec![("channel", str_val("b")), ("amt", num(2.0))],
            ),
        ),
        (
            300,
            join_row(
                "id",
                1.0,
                vec![("channel", str_val("a")), ("amt", num(3.0))],
            ),
        ),
    ]);
    let mut ctx = event(vec![("key", num(1.0)), ("chan", str_val("a"))]);
    assert!(execute_joins(&vec![join], &mut ctx, &lookup, 1_000));
    // Latest matching both conds = ts=300.
    assert_eq!(ctx.fields.get("amt"), Some(&num(3.0)));
}

#[test]
fn execute_joins_interval_within_modes() {
    use crate::match_engine::executor::context::execute_joins;

    let cond = JoinCondPlan {
        left: FieldRef::Simple("key".into()),
        right: FieldRef::Qualified("w".into(), "id".into()),
    };
    let within = WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::from_secs(100),
                neg: true,
            },
        },
        hi: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::from_secs(100),
                neg: false,
            },
        },
    };
    let jp = |mode: JoinMode| JoinPlan {
        right_window: "w".to_string(),
        mode,
        conds: vec![cond.clone()],
        within: Some(within.clone()),
        reduce: None,
        emit_at: None,
    };
    const T0: i64 = 1_700_000_000_000_000_000;
    let lookup = RowsLookup::with_ts(vec![
        (
            T0 - 200_000_000_000,
            join_row("id", 1.0, vec![("amt", num(1.0))]),
        ),
        (
            T0 - 50_000_000_000,
            join_row("id", 1.0, vec![("amt", num(2.0))]),
        ),
        (
            T0 + 50_000_000_000,
            join_row("id", 1.0, vec![("amt", num(3.0))]),
        ),
        (
            T0 + 400_000_000_000,
            join_row("id", 1.0, vec![("amt", num(4.0))]),
        ),
    ]);
    // Event at T0: interval [T0-100s, T0+100s] → rows at T0-50s and T0+50s.
    // Inner/Snapshot pick the earliest, Asof the latest.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &vec![jp(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        T0
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(2.0)));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &vec![jp(JoinMode::Snapshot)],
        &mut ctx,
        &lookup,
        T0
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(2.0)));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &vec![jp(JoinMode::Asof { within: None })],
        &mut ctx,
        &lookup,
        T0
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(3.0)));
    // Anti within: an interval match drops the event.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(!execute_joins(
        &vec![jp(JoinMode::Anti)],
        &mut ctx,
        &lookup,
        T0
    ));
    // Event at T0+500s: interval [T0+400s, T0+600s] → the T0+400s row qualifies.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &vec![jp(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        T0 + 500_000_000_000
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(4.0)));
    // Event at T0-1000s: interval [T0-1100s, T0-900s] → nothing in range.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(!execute_joins(
        &vec![jp(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        T0 - 1_000_000_000_000
    ));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &vec![jp(JoinMode::Anti)],
        &mut ctx,
        &lookup,
        T0 - 1_000_000_000_000
    ));

    // Interval bound eval failure (missing key field) → inner drops, others keep.
    let jp2 = |mode: JoinMode| JoinPlan {
        right_window: "w".to_string(),
        mode,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("missing".into()),
            right: FieldRef::Qualified("w".into(), "id".into()),
        }],
        within: Some(within.clone()),
        reduce: None,
        emit_at: None,
    };
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(!execute_joins(
        &vec![jp2(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        T0
    ));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &vec![jp2(JoinMode::Snapshot)],
        &mut ctx,
        &lookup,
        T0
    ));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &vec![jp2(JoinMode::Anti)],
        &mut ctx,
        &lookup,
        T0
    ));
}

#[test]
fn in_interval_and_eval_interval_bound() {
    use crate::match_engine::executor::context::{eval_interval_bound, in_interval};

    // Closed bounds include boundaries; open exclude.
    assert!(in_interval(100, 100, 200, false, false));
    assert!(in_interval(200, 100, 200, false, false));
    assert!(!in_interval(99, 100, 200, false, false));
    assert!(!in_interval(100, 100, 200, true, false));
    assert!(!in_interval(200, 100, 200, false, true));
    assert!(in_interval(101, 100, 200, true, false));
    assert!(in_interval(199, 100, 200, false, true));

    // Dur bounds (positive / negative / huge overflow saturates to i64::MAX).
    let dur = |secs: u64, neg: bool, open: bool| Bound {
        open,
        val: BoundVal::Dur {
            dur: Duration::from_secs(secs),
            neg,
        },
    };
    let ctx = event(vec![]);
    assert_eq!(
        eval_interval_bound(&dur(10, true, false), &ctx, 1_000),
        Some(-9_999_999_000i64)
    );
    assert_eq!(
        eval_interval_bound(&dur(10, false, false), &ctx, 1_000),
        Some(10_000_001_000i64)
    );

    // Expr bounds: numeric → epoch nanos; non-numeric / missing → None.
    let expr_bound = |e: Expr| Bound {
        open: false,
        val: BoundVal::Expr(e),
    };
    let ctx = event(vec![
        ("ts", num(1_700_000_000_000_000_000.0)),
        ("s", str_val("x")),
    ]);
    assert_eq!(
        eval_interval_bound(
            &expr_bound(Expr::Field(FieldRef::Simple("ts".into()))),
            &ctx,
            0
        ),
        Some(1_700_000_000_000_000_000)
    );
    assert!(
        eval_interval_bound(
            &expr_bound(Expr::Field(FieldRef::Simple("s".into()))),
            &ctx,
            0
        )
        .is_none()
    );
    assert!(
        eval_interval_bound(
            &expr_bound(Expr::Field(FieldRef::Simple("missing".into()))),
            &ctx,
            0
        )
        .is_none()
    );
    // Direct number literal expr.
    assert_eq!(
        eval_interval_bound(&expr_bound(Expr::Number(1.7e18)), &ctx, 0),
        Some(1_700_000_000_000_000_000)
    );
}

#[test]
fn enrich_join_row_and_first_join_key() {
    use crate::match_engine::executor::context::{
        enrich_join_row, first_join_key, row_matches_conds,
    };

    let join = JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Inner,
        conds: vec![],
        within: None,
        reduce: None,
        emit_at: None,
    };
    let row = join_row("id", 1.0, vec![("amt", num(5.0))]);
    let mut ctx = event(vec![("key", num(1.0)), ("existing", num(1.0))]);
    enrich_join_row(&mut ctx, &join, &row);
    assert_eq!(ctx.fields.get("w.id"), Some(&num(1.0)));
    assert_eq!(ctx.fields.get("id"), Some(&num(1.0)));
    assert_eq!(ctx.fields.get("w.amt"), Some(&num(5.0)));
    // Plain-name insertion does not override an existing field.
    assert_eq!(ctx.fields.get("key"), Some(&num(1.0)));
    assert_eq!(ctx.fields.get("existing"), Some(&num(1.0)));

    // first_join_key: empty conds / missing field → None.
    let conds = vec![JoinCondPlan {
        left: FieldRef::Simple("key".into()),
        right: FieldRef::Qualified("w".into(), "id".into()),
    }];
    assert_eq!(
        first_join_key(&ctx, &conds),
        Some(("id".to_string(), num(1.0)))
    );
    assert_eq!(first_join_key(&ctx, &[]), None);
    assert_eq!(first_join_key(&event(vec![]), &conds), None);

    // row_matches_conds: all conditions must hold; missing side → false.
    let conds = vec![
        JoinCondPlan {
            left: FieldRef::Simple("key".into()),
            right: FieldRef::Qualified("w".into(), "id".into()),
        },
        JoinCondPlan {
            left: FieldRef::Simple("amt".into()),
            right: FieldRef::Qualified("w".into(), "amt".into()),
        },
    ];
    assert!(row_matches_conds(
        &row,
        &conds,
        &event(vec![("key", num(1.0)), ("amt", num(5.0))])
    ));
    assert!(!row_matches_conds(
        &row,
        &conds,
        &event(vec![("key", num(1.0)), ("amt", num(9.0))])
    ));
    assert!(!row_matches_conds(
        &row,
        &conds,
        &event(vec![("key", num(1.0))])
    ));
    assert!(!row_matches_conds(
        &row,
        &conds,
        &event(vec![("amt", num(5.0))])
    ));
    // Row missing the right field → false.
    let partial = join_row("other", 2.0, vec![]);
    assert!(!row_matches_conds(
        &partial,
        &conds,
        &event(vec![("key", num(1.0)), ("amt", num(5.0))])
    ));
}

// ---------------------------------------------------------------------------
// match_exec.rs — execute_match with joins / where
// ---------------------------------------------------------------------------

#[test]
fn execute_match_with_joins_rejections() {
    // Right-window row whose `id` matches the scope value (a string, matching
    // the rule key `sip`).
    let matched_row = || {
        let mut fields = EngineHashMap::default();
        fields.insert("id".into(), str_val("10.0.0.1"));
        fields.insert("amt".into(), num(5.0));
        JoinRow::Event(Arc::new(Event { fields }))
    };

    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
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
    let matched = default_matched_context();
    let lookup = RowsLookup::new(vec![matched_row()]);

    // Join hit → record; join miss → None.
    let rec = exec
        .execute_match_with_joins_at(&matched, &lookup, 123)
        .unwrap()
        .unwrap();
    assert_eq!(rec.score, 50.0);
    let mut missed = matched.clone();
    missed.scope_key = vec![str_val("10.9.9.9")];
    assert!(
        exec.execute_match_with_joins_at(&missed, &lookup, 123)
            .unwrap()
            .is_none()
    );

    // Post-join where reject → None.
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.joins = vec![JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("sip".into()),
            right: FieldRef::Qualified("w".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("amt".into()))),
        right: Box::new(Expr::Number(5.0)),
    });
    let exec = RuleExecutor::new(plan);
    let matched = default_matched_context();
    let lookup = RowsLookup::new(vec![matched_row()]);
    // Snapshot join enriches; where checks the enriched `amt`.
    let rec = exec
        .execute_match_with_joins_at(&matched, &lookup, 123)
        .unwrap();
    assert!(rec.is_some());
    // Where reads an absent field → suppressed.
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("missing".into()))),
        right: Box::new(Expr::Number(5.0)),
    });
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.execute_match_with_joins_at(&matched, &EmptyLookup, 123)
            .unwrap()
            .is_none()
    );
}

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

    // Lets → false.
    let mut plan = base();
    plan.lets = vec![LetPlan {
        name: "x".into(),
        expr: Expr::Number(1.0),
    }];
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

    // Each filter → false.
    let mut plan = base();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::Bool(true)),
    });
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // Non-columnar bind filter → false.
    let mut plan = base();
    plan.binds[0].filter = Some(Expr::FuncCall {
        qualifier: None,
        name: "upper".into(),
        args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
    });
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // Non-constant score → false.
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::Field(FieldRef::Simple("sip".into())),
    };
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // Entity = Path field / general expr → false.
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
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

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

#[test]
fn columnar_each_entity_lanes_and_failure_paths() {
    // Schema: sip=Utf8, id=Int64, ts=Timestamp(Ns), price=Float64, note=structured Utf8.
    let note_field =
        ArrowField::new("note", DataType::Utf8, true).with_metadata(HashMap::from([(
            crate::match_engine::event_bridge::WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            crate::match_engine::event_bridge::WFL_FIELD_TYPE_OBJECT.to_string(),
        )]));
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new("id", DataType::Int64, true),
        ArrowField::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ArrowField::new("price", DataType::Float64, true),
        note_field,
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("10.0.0.1"),
                Some("10.0.0.2"),
                None,
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1000), Some(1001), Some(1002)])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_001_000_000),
                Some(1_700_000_000_002_000_000),
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(r#"{"a":1}"#),
                Some(r#"{"b":2}"#),
                None,
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    // Entity = e.id (Int64) → I64 lane; yield id (same column, Float type) →
    // numeric fast lane.
    let mut plan = simple_rule_plan(
        "i64_entity",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "digit",
        Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "id_copy".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("id_copy".into(), FieldType::Base(BaseType::Float))]),
    );
    assert!(exec.each_plan_columnar_safe());
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .map(|ev| (ev, 1_700_000_000_000_000_000))
        .collect();
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);
    assert_eq!(appended, vec![0, 1, 2]);

    // Entity = e.ts (Timestamp Ns) → TsNanos lane.
    let mut plan = simple_rule_plan(
        "ts_entity",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "time",
        Expr::Field(FieldRef::Qualified("e".into(), "ts".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);

    // Entity = e.sip (Utf8, with a null row) → Utf8 lane + empty-entity fallback.
    let exec = each_plan_rule();
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);

    // Entity = e.price (Float64) → Generic lane (value_at + value_to_string).
    let mut plan = simple_rule_plan(
        "f64_entity",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "price".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);

    // Entity = e.note (structured Utf8) → Generic lane (no fast lane).
    let mut plan = simple_rule_plan(
        "structured_entity",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "note".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);

    // Entity field missing from the batch schema → Generic None → empty pair.
    let mut plan = simple_rule_plan(
        "missing_entity",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "absent".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);

    // Const (literal) yield that fails to coerce → whole batch failed.
    let mut plan = simple_rule_plan(
        "nan_yield",
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
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.failed, 3);
    assert_eq!(stats.appended, 0);

    // Yield name with the reserved `__wfu_` prefix → register error → failed.
    let mut plan = simple_rule_plan(
        "reserved_yield",
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
        name: "__wfu_evil".into(),
        value: Expr::Number(1.0),
    }];
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.failed, 3);

    // Non-`on each` rule → all rows failed.
    let plan = simple_rule_plan(
        "not_each",
        default_match_plan(),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let exec = RuleExecutor::new(plan);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&rows, 0, &mut builder, &mut appended);
    assert_eq!(stats.failed, 3);
    assert_eq!(stats.appended, 0);
}

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
    let stats = exec.execute_close_direct_batch_columnar(&[close.clone()], &mut builder, 0);
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
    assert!(exec.deferred_pending_for(1, &auction_event(), T).is_none());

    // Missing key field → None.
    let ev = event(vec![
        ("dateTime", num(T as f64)),
        ("expires", num((T + 60_000_000_000) as f64)),
    ]);
    assert!(exec.deferred_pending_for(0, &ev, T).is_none());

    // Missing bound field → None.
    let ev = event(vec![
        ("id", num(5.0)),
        ("expires", num((T + 60_000_000_000) as f64)),
    ]);
    assert!(exec.deferred_pending_for(0, &ev, T).is_none());

    // Non-numeric expiry → None.
    let ev = event(vec![
        ("id", num(5.0)),
        ("dateTime", num(T as f64)),
        ("expires", str_val("soon")),
    ]);
    assert!(exec.deferred_pending_for(0, &ev, T).is_none());

    // No emit_at on the join → None.
    let mut plan = deferred_join_plan(None);
    plan.joins[0].emit_at = None;
    let exec = RuleExecutor::new(plan);
    assert!(exec.deferred_pending_for(0, &auction_event(), T).is_none());

    // No within on the join → None.
    let mut plan = deferred_join_plan(None);
    plan.joins[0].within = None;
    let exec = RuleExecutor::new(plan);
    assert!(exec.deferred_pending_for(0, &auction_event(), T).is_none());

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
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
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
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
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
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
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
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
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
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
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
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
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
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
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
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
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
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
    let lookup = RowsLookup::with_ts(vec![bid(T + 10_000_000_000, 5.0, 1.0, 100.0)]);
    assert!(
        exec.execute_deferred_join(0, &pending, &lookup, T)
            .unwrap()
            .is_none()
    );

    // Pure existence (reduce None): earliest row enriches; output has
    // `origin=deferred` and `fired_at` = the pending expiry.
    let exec = RuleExecutor::new(deferred_join_plan(None));
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
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
    assert!(ctx.fields.get("b").is_none());
    assert_eq!(ctx.fields.get("c"), Some(&num(12.0)));
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
