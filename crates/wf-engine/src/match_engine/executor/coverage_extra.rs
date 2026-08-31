//! Coverage-fill tests for the executor layer (`executor/mod.rs` internals,
//! `context.rs` join execution, `close_exec.rs`, `each_exec.rs`,
//! `deferred_exec.rs`, `match_exec.rs`).
//!
//! These tests drive the real executor entry points (`RuleExecutor::execute_*`,
//! `execute_joins`, `build_eval_context`, ...) with constructed plans /
//! contexts / events, focusing on the error paths, boundary conditions, and
//! configuration branches that the equivalence-focused tests in
//! `tests/executor/` do not reach.
use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
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
        row_fields: None,
        row_field_names: None,
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
    fn asof_candidates(
        &self,
        _w: &str,
        key_field: &str,
        key: &Value,
    ) -> Option<Vec<(i64, JoinRow)>> {
        // 契约（types.rs asof_candidates 文档）：候选 = key_field == key 的行。
        // 2026-08-26 q4a 条件复核冗余跳过后，测试 lookup 必须遵守该契约
        //（否则错误 key 的行会绕过复核被 reduce 选中）。
        Some(
            self.ts_rows
                .iter()
                .filter(|(_, row)| {
                    row.field_value(key_field)
                        .is_some_and(|v| crate::match_engine::values_equal(&v, key))
                })
                .cloned()
                .collect(),
        )
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
        None,
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
        None,
    );
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));
    assert_eq!(ctx.fields.get("_step_0_measure"), Some(&num(99.0)));

    // Trigger event scalars are included (keys win).
    let trigger = event(vec![("sip", str_val("override")), ("raw", num(7.0))]);
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        std::slice::from_ref(&sd1),
        &[],
        &[&StepPlan { branches: vec![] }],
        Some(&trigger),
        &all,
        None,
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
        None,
    );
    assert_eq!(ctx.fields.get("price"), Some(&num(3.0)));
    assert!(!ctx.fields.contains_key("login"), "label not requested");
    assert!(!ctx.fields.contains_key("_step_0_measure"));
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
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[],
        std::slice::from_ref(&bd),
        &[],
        None,
        &all,
        None,
    );
    assert_eq!(ctx.fields.get("_bind_win_count"), Some(&num(2.0)));
    assert_eq!(
        ctx.fields.get("_bind_win_field_amount"),
        Some(&Value::Array(vec![num(10.0), num(20.0)]))
    );
    assert_eq!(ctx.fields.get("amount"), Some(&num(20.0)));
    let named = CloseCtxFields::Named(HashSet::from(["amount".to_string()]));
    let ctx = build_eval_context(&keys, &scope_key, &[], &[bd], &[], None, &named, None);
    assert_eq!(ctx.fields.get("amount"), Some(&num(20.0)));
    assert!(!ctx.fields.contains_key("_bind_win_count"));
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
    assert!(!ctx.fields.contains_key("amt"));
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
    assert!(!ctx.fields.contains_key("amt"));
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
    assert!(execute_joins(&[jp(None)], &mut ctx, &lookup, 1_000));
    assert_eq!(ctx.fields.get("amt"), Some(&num(5.0)));

    // Fast-path Miss → None (no enrichment, keep).
    let lookup = RowsLookup {
        rows: vec![row.clone()],
        ts_rows: vec![],
        asof_outcome: Some(AsofLookup::Miss),
    };
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&[jp(None)], &mut ctx, &lookup, 1_000));
    assert!(!ctx.fields.contains_key("amt"));

    // Fallback → candidate scan; picks the latest ts ≤ event_time.
    let lookup = RowsLookup::with_ts(vec![
        (100, join_row("id", 1.0, vec![("amt", num(1.0))])),
        (200, join_row("id", 1.0, vec![("amt", num(2.0))])),
        (300, join_row("id", 1.0, vec![("amt", num(3.0))])),
        (999, join_row("id", 9.0, vec![("amt", num(99.0))])),
    ]);
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&[jp(None)], &mut ctx, &lookup, 250));
    assert_eq!(ctx.fields.get("amt"), Some(&num(2.0)));

    // `within` filters older rows: latest within [250-100, 250] is ts=200.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &[jp(Some(Duration::from_secs(100)))],
        &mut ctx,
        &lookup,
        250
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(2.0)));

    // Asof with missing key → keep unenriched (continue).
    let mut ctx = event(vec![]);
    assert!(execute_joins(&[jp(None)], &mut ctx, &lookup, 250));

    // Asof with no candidates → keep unenriched.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&[jp(None)], &mut ctx, &EmptyLookup, 250));
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
    assert!(execute_joins(&[join], &mut ctx, &lookup, 1_000));
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
    assert!(execute_joins(&[jp(JoinMode::Inner)], &mut ctx, &lookup, T0));
    assert_eq!(ctx.fields.get("amt"), Some(&num(2.0)));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &[jp(JoinMode::Snapshot)],
        &mut ctx,
        &lookup,
        T0
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(2.0)));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &[jp(JoinMode::Asof { within: None })],
        &mut ctx,
        &lookup,
        T0
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(3.0)));
    // Anti within: an interval match drops the event.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(!execute_joins(&[jp(JoinMode::Anti)], &mut ctx, &lookup, T0));
    // Event at T0+500s: interval [T0+400s, T0+600s] → the T0+400s row qualifies.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &[jp(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        T0 + 500_000_000_000
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(4.0)));
    // Event at T0-1000s: interval [T0-1100s, T0-900s] → nothing in range.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(!execute_joins(
        &[jp(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        T0 - 1_000_000_000_000
    ));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &[jp(JoinMode::Anti)],
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
        &[jp2(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        T0
    ));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &[jp2(JoinMode::Snapshot)],
        &mut ctx,
        &lookup,
        T0
    ));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&[jp2(JoinMode::Anti)], &mut ctx, &lookup, T0));
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
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

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

    // 其他 BinOp（Add）→ false。
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::Number(0.5)),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
        },
    };
    assert!(!RuleExecutor::new(plan).each_plan_columnar_safe());

    // 字段×字段 → false。
    let mut plan = base();
    plan.score_plan = ScorePlan {
        expr: Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            right: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
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

    // `where` → false。
    let mut plan = base();
    plan.r#where = Some(Expr::Bool(true));
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

#[test]
fn columnar_each_binop_score_matches_row_path() {
    // q1 形态：score(0.908 * e.price)、entity=e.id、yield 常量 + id 字段。
    // 对拍：行式（Event 物化 + eval_score 解释求值）vs 列式（ColumnarEvent
    // 零物化 + 列读 f64 × 常量）输出字节一致，且 score = clamp(0.908 × price)。
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", DataType::Int64, true),
        ArrowField::new("price", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), Some(100.0)])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut plan = simple_rule_plan(
        "q1_binop_score",
        simple_plan(vec![], vec![]),
        Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Number(0.908)),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "price".into()))),
        },
        "digit",
        Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q1_passthrough".into()),
        },
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("id".into(), FieldType::Base(BaseType::Float))]),
    );
    assert!(exec.each_plan_columnar_safe());

    let t = 1_700_000_000_000_000_000i64;

    // 行式路径（Event 物化 + eval_score 解释求值）。
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    assert_eq!(sr.appended, 3);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 列式路径（ColumnarEvent 零物化 + 列读 f64）。
    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    assert_eq!(sc.appended, 3);
    assert_eq!(sc.failed, 0);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 对拍：两路径逐字段一致；score = clamp(0.908 × price)。
    assert_eq!(out_row, out_col);
    let scores: Vec<f64> = out_col
        .iter()
        .map(|r| {
            r.fields()
                .find(|f| f.get_name() == wf_lang::wfu_meta::WFU_SCORE)
                .and_then(|f| match f.get_value() {
                    ModelValue::Float(v) => Some(*v),
                    _ => None,
                })
                .expect("score field present")
        })
        .collect();
    assert_eq!(scores, vec![0.908 * 1.5, 0.908 * 2.5, 0.908 * 100.0]);
}

#[test]
fn columnar_each_binop_score_null_field_fails_row() {
    // 常量×字段的 score 字段为 null → 整行 failed（与解释路径 eval_score 的
    // None → Err 一致），其余行正常 appended。
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", DataType::Int64, true),
        ArrowField::new("price", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.5), None, Some(3.0)])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut plan = simple_rule_plan(
        "null_score",
        simple_plan(vec![], vec![]),
        Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Number(0.5)),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "price".into()))),
        },
        "digit",
        Expr::Field(FieldRef::Qualified("e".into(), "id".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);
    assert!(exec.each_plan_columnar_safe());

    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events
        .iter()
        .map(|ev| (ev, 1_700_000_000_000_000_000))
        .collect();
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.failed, 1);
    assert_eq!(appended, vec![0, 2]);
}

/// 列式输出函数（fmt/strftime/count_char）yield：行式 vs 列式 each 输出逐字段
/// 对拍（含 null 参数 → 空串的 yield 包装语义）。
#[test]
fn each_columnar_output_funcs_match_row_path() {
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new("count", DataType::Int64, true),
        ArrowField::new("ts", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("10.1.1.1"),
                None,
                Some("192.168.0.2"),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(3), Some(7), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000),
                None,
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut plan = simple_rule_plan(
        "out_funcs",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
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
            name: "label".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "fmt".into(),
                args: vec![
                    Expr::StringLit("ip={}|n={}".into()),
                    Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
                    Expr::Field(FieldRef::Qualified("e".into(), "count".into())),
                ],
            },
        },
        YieldField {
            name: "day".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "strftime".into(),
                args: vec![
                    Expr::Field(FieldRef::Qualified("e".into(), "ts".into())),
                    Expr::StringLit("%Y-%m-%d".into()),
                ],
            },
        },
        YieldField {
            name: "dots".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "count_char".into(),
                args: vec![
                    Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
                    Expr::StringLit(".".into()),
                ],
            },
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("label".into(), FieldType::Base(BaseType::Chars)),
            ("day".into(), FieldType::Base(BaseType::Chars)),
            ("dots".into(), FieldType::Base(BaseType::Digit)),
        ]),
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "fmt/strftime/count_char yield 应列式"
    );

    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    assert_eq!(sr.appended, 3);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    assert_eq!(sc.appended, 3);
    assert_eq!(sc.failed, 0);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(out_row, out_col);
    // 关键语义抽查：null sip（row 1）→ fmt 空串；null count（row 2）→ fmt
    // 空串；count_char 正常行返回数字。
    // 关键语义抽查：null sip（row 1）→ fmt 空串；null count（row 2）→ fmt
    // 空串；count_char 正常行返回数字。
    let label = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "label")
            .and_then(|f| match f.get_value() {
                ModelValue::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect("label field")
    };
    assert_eq!(label(&out_col[0]), "ip=10.1.1.1|n=3", "row 0 fmt");
    assert_eq!(label(&out_col[1]), "", "row 1 fmt null sip → 空串");
    assert_eq!(label(&out_col[2]), "", "row 2 fmt null count → 空串");
}

/// 层 2（2026-08-25，q22 形态）：`let parts = split(e.url, "/")` + yield
/// `concat(mvindex(parts,3), "/", ...)`——列式 each（编译期内联 let + 融合
/// SplitIndex）与行式 each（apply_lets 逐行注入）输出逐字段对拍（含 null /
/// 越界 → 空串）。
#[test]
fn each_columnar_q22_split_mvindex_concat_matches_row_path() {
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "url",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            Some("https://www.nexmark.com/aaaaa/bbbbb/ccccc/item.htm?query=1"),
            None,           // null 行
            Some("short"),  // mvindex 越界 → 空串
            Some("a/b//d"), // 空段
        ])) as ArrayRef],
    )
    .unwrap();

    let mut plan = simple_rule_plan(
        "q22_shape",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "chars",
        Expr::Field(FieldRef::Qualified("e".into(), "url".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.lets = vec![LetPlan {
        name: "parts".into(),
        expr: Expr::FuncCall {
            qualifier: None,
            name: "split".into(),
            args: vec![
                Expr::Field(FieldRef::Qualified("e".into(), "url".into())),
                Expr::StringLit("/".into()),
            ],
        },
    }];
    let mvindex = |idx: f64| Expr::FuncCall {
        qualifier: None,
        name: "mvindex".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("parts".into())),
            Expr::Number(idx),
        ],
    };
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "concat".into(),
            args: vec![
                mvindex(3.0),
                Expr::StringLit("/".into()),
                mvindex(4.0),
                Expr::StringLit("/".into()),
                mvindex(5.0),
            ],
        },
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("detail".into(), FieldType::Base(BaseType::Chars))]),
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "q22 let+split+mvindex+concat 应列式"
    );

    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    assert_eq!(sr.appended, 4);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let col_events: Vec<ColumnarEvent> = (0..4).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    assert_eq!(sc.appended, 4);
    assert_eq!(sc.failed, 0);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(out_row, out_col);
    // 语义抽查：row 0 三段拼接 aaaaa/bbbbb/ccccc；null row 1 与越界 row 2 → 空串。
    let detail = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "detail")
            .and_then(|f| match f.get_value() {
                wp_model_core::model::Value::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect("detail field")
    };
    assert_eq!(detail(&out_col[0]), "aaaaa/bbbbb/ccccc", "row 0 concat");
    assert_eq!(detail(&out_col[1]), "", "row 1 null url → 空串");
    assert_eq!(detail(&out_col[2]), "", "row 2 mvindex 越界 → 空串");
    assert_eq!(detail(&out_col[3]), "", "row 3 段数不足 → 空串");
}

/// 层 2 收口（2026-08-25）：**行式批路径**（`execute_each_direct_batch`，Event
/// 数组——文件源 replay 等非 RecordBatch 源）的 General yield 走列式批级 cell
/// （Event 数组物化 + let 内联），与逐事件 `execute_each` 逐字段字节一致。
#[test]
fn each_direct_batch_general_yield_matches_per_event() {
    let mut plan = simple_rule_plan(
        "each_fmt",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "digit",
        Expr::Field(FieldRef::Qualified("e".into(), "auction".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.lets = vec![LetPlan {
        name: "parts".into(),
        expr: Expr::FuncCall {
            qualifier: None,
            name: "split".into(),
            args: vec![
                Expr::Field(FieldRef::Qualified("e".into(), "url".into())),
                Expr::StringLit("/".into()),
            ],
        },
    }];
    let mvindex = |idx: f64| Expr::FuncCall {
        qualifier: None,
        name: "mvindex".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("parts".into())),
            Expr::Number(idx),
        ],
    };
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "auction".into())),
        },
        YieldField {
            name: "detail".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "concat".into(),
                args: vec![mvindex(3.0), Expr::StringLit("/".into()), mvindex(4.0)],
            },
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Digit)),
            ("detail".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );

    let t = 1_700_000_000_000_000_000i64;
    let events = vec![
        event(vec![
            ("auction", num(1001.0)),
            (
                "url",
                str_val("https://www.nexmark.com/aaaaa/bbbbb/ccccc/item.htm"),
            ),
        ]),
        event(vec![("auction", num(1002.0)), ("url", str_val("short"))]),
        event(vec![("auction", num(1003.0))]), // url 缺失 → mvindex null → 空串
    ];

    // 逐事件（解释路径，apply_lets 注入）。
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    for ev in &events {
        let record = exec.execute_each(ev, t).unwrap().unwrap();
        b_row.append_record(&record).unwrap();
    }
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 行式批路径（Event 数组 → 列式 cell）。
    let rows: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_batch = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app = Vec::new();
    let stats = exec.execute_each_direct_batch(&rows, &EmptyLookup, &[], 0, &mut b_batch, &mut app);
    assert_eq!(stats.appended, 3);
    assert_eq!(stats.failed, 0);
    assert_eq!(app, vec![0, 1, 2]);
    let out_batch: Vec<_> = b_batch
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 逐字段字节一致（`__wfu_emit_time` 除外——批路径用传入的 emit_time，
    // 逐事件用 now()，与列式批路径的文档化差异一致；emit_time 不喂语义）。
    assert_eq!(out_row.len(), out_batch.len());
    for (row, (ra, rb)) in out_row.iter().zip(out_batch.iter()).enumerate() {
        for (fa, fb) in ra.items.iter().zip(rb.items.iter()) {
            if fa.get_name() == wf_lang::wfu_meta::WFU_EMIT_TIME {
                continue;
            }
            assert_eq!(fa.get_name(), fb.get_name(), "row {row} field name");
            assert_eq!(
                fa.get_value(),
                fb.get_value(),
                "row {row} field {} value",
                fa.get_name()
            );
        }
    }
    let detail = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "detail")
            .and_then(|f| match f.get_value() {
                wp_model_core::model::Value::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect("detail field")
    };
    assert_eq!(detail(&out_batch[0]), "aaaaa/bbbbb", "row 0 concat");
    assert_eq!(detail(&out_batch[1]), "", "row 1 越界 → 空串");
    assert_eq!(detail(&out_batch[2]), "", "row 2 url 缺失 → 空串");
}

#[test]
fn close_ctx_fields_narrowed_for_output_funcs() {
    // 层 2 收口 review：列式输出函数（fmt 等）是纯参数函数——
    // `plan_close_ctx_fields` 应窄化为 Named（含引用的普通字段），而非
    // force_all（行式/回退路径的全量 ctx 构建）。合成字段引用仍 force_all。
    use super::CloseCtxFields;
    use super::plan_close_ctx_fields;

    let base = || {
        let mut plan = simple_rule_plan(
            "narrow",
            simple_plan(vec![], vec![]),
            Expr::Number(10.0),
            "digit",
            Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        );
        plan.binds[0].alias = "b".into();
        plan
    };
    let fmt = |args: Vec<Expr>| Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args,
    };
    let f = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));

    // fmt detail（纯参数函数）→ Named，且含引用的字段。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: fmt(vec![
            Expr::StringLit("{} {}".into()),
            f("bidder"),
            f("price"),
        ]),
    }];
    let fields = plan_close_ctx_fields(&plan);
    match &fields {
        CloseCtxFields::Named(set) => {
            assert!(set.contains("bidder"), "fmt 参数 bidder 应收集");
            assert!(set.contains("price"), "fmt 参数 price 应收集");
        }
        _ => panic!("fmt detail 应窄化为 Named，got {fields:?}"),
    }

    // L3 聚合（collect_set 读 `_step_*` 合成字段）→ 仍 force_all。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "agg".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "collect_set".into(),
            args: vec![f("bidder")],
        },
    }];
    assert!(
        matches!(plan_close_ctx_fields(&plan), CloseCtxFields::All),
        "L3 聚合必须 All（读合成字段）"
    );

    // fmt 引用合成字段 → 仍 force_all（Field 的 `_` 前缀检查）。
    let mut plan = base();
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: fmt(vec![
            Expr::StringLit("{}".into()),
            Expr::Field(FieldRef::Simple("_step_0_measure".into())),
        ]),
    }];
    assert!(
        matches!(plan_close_ctx_fields(&plan), CloseCtxFields::All),
        "fmt 引用合成字段必须 All"
    );
}

/// fmt 参数为结构化（object）字段：形状 gate 放行，但编译失败 → 行式回退，
/// 输出与纯行式路径逐字段一致（object 渲染 [object]）。
#[test]
fn each_columnar_fmt_structured_arg_falls_back_matches_row_path() {
    use crate::match_engine::WFL_FIELD_TYPE_METADATA_KEY;
    use crate::match_engine::WFL_FIELD_TYPE_OBJECT;
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("ext", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_OBJECT.to_string(),
            )]),
        ),
        ArrowField::new("sip", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some(r#"{"k":1}"#),
                Some(r#"{"k":2}"#),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("10.0.0.1"), Some("10.0.0.2")])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut plan = simple_rule_plan(
        "obj_fmt",
        simple_plan(vec![], vec![]),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "label".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("x={}".into()),
                Expr::Field(FieldRef::Qualified("e".into(), "ext".into())),
            ],
        },
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("label".into(), FieldType::Base(BaseType::Chars))]),
    );
    // 形状 gate 放行；执行时结构化参数编译失败 → 行式回退（不 panic）。
    assert!(exec.each_plan_columnar_safe());

    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    assert_eq!(sr.appended, 2);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let col_events: Vec<ColumnarEvent> = (0..2).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    assert_eq!(sc.appended, 2);
    assert_eq!(sc.failed, 0);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(out_row, out_col);
    // 行式回退渲染 [object]。
    let label = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "label")
            .and_then(|f| match f.get_value() {
                ModelValue::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect("label field")
    };
    assert_eq!(label(&out_col[0]), "x=[object]", "object 参数渲染 [object]");
}

/// Q14 形态的 on-each 规则：each filter（`0.908*price` 价格区间，列式算术
/// 比较）+ yield fmt（IfThenElse+InList+count_char 递归列式）。行式 vs 列式
/// 批路径统计与输出逐位对拍（含 each filter 拒绝行）。
#[test]
fn each_columnar_q14_filter_matches_row_path() {
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new("price", DataType::Int64, true),
        ArrowField::new("dateTime", DataType::Int64, true),
        ArrowField::new("extra", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                None,
                Some(6),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                // 0.908*price 需 ∈ (1M, 50M)：1M 不过、5M 过、60M 不过、10M 过、null 不过、20M 过。
                Some(1_000_000),
                Some(5_000_000),
                Some(60_000_000),
                Some(10_000_000),
                None,
                Some(20_000_000),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                // 真实 3 档 CASE：22 时 → nightTime；10 时（-12h）→ dayTime；07 时（-15h）→ otherTime。
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000 - 12 * 3_600_000_000_000),
                None,
                Some(1_700_000_000_000_000_000 - 15 * 3_600_000_000_000),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("abc"),
                Some("abc c cc"),
                Some("x"),
                Some("no-c"),
                None,
                Some("zz"),
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    let b_field = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));
    let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
        qualifier: None,
        name: name.into(),
        args,
    };
    let mut plan = simple_rule_plan(
        "q14_filter",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: Some(Expr::BinOp {
            op: BinOp::And,
            left: Box::new(Expr::BinOp {
                op: BinOp::Gt,
                left: Box::new(Expr::BinOp {
                    op: BinOp::Mul,
                    left: Box::new(Expr::Number(0.908)),
                    right: Box::new(b_field("price")),
                }),
                right: Box::new(Expr::Number(1_000_000.0)),
            }),
            right: Box::new(Expr::BinOp {
                op: BinOp::Lt,
                left: Box::new(Expr::BinOp {
                    op: BinOp::Mul,
                    left: Box::new(Expr::Number(0.908)),
                    right: Box::new(b_field("price")),
                }),
                right: Box::new(Expr::Number(50_000_000.0)),
            }),
        }),
    });
    // 真实 q14.wfl：嵌套 3 档 CASE（nightTime/dayTime/otherTime，10/9 项 InList）。
    let in_hours = |hours: &[&str]| Expr::InList {
        expr: Box::new(call(
            "strftime",
            vec![b_field("dateTime"), Expr::StringLit("%H".into())],
        )),
        list: hours.iter().map(|h| Expr::StringLit((*h).into())).collect(),
        negated: false,
    };
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: call(
            "fmt",
            vec![
                Expr::StringLit("{} c={}".into()),
                Expr::IfThenElse {
                    cond: Box::new(in_hours(&[
                        "00", "01", "02", "03", "04", "05", "06", "20", "21", "22", "23",
                    ])),
                    then_expr: Box::new(Expr::StringLit("nightTime".into())),
                    else_expr: Box::new(Expr::IfThenElse {
                        cond: Box::new(in_hours(&[
                            "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18",
                        ])),
                        then_expr: Box::new(Expr::StringLit("dayTime".into())),
                        else_expr: Box::new(Expr::StringLit("otherTime".into())),
                    }),
                },
                call(
                    "count_char",
                    vec![b_field("extra"), Expr::StringLit("c".into())],
                ),
            ],
        ),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("detail".into(), FieldType::Base(BaseType::Chars))]),
    );
    assert!(
        exec.each_plan_columnar_safe(),
        "Q14 each filter + 递归输出函数应列式放行"
    );

    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let col_events: Vec<ColumnarEvent> = (0..6).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);

    // 统计对拍：each filter 拒绝 3 行（1M 低于区间 / 60M 高于区间 / null），
    // 追加 3 行（5M / 10M / 20M）。
    assert_eq!(sr.appended, 3, "行式 appended");
    assert_eq!(sr.rejected, 3, "行式 rejected");
    assert_eq!(sc.appended, 3, "列式 appended");
    assert_eq!(sc.rejected, 3, "列式 rejected");
    assert_eq!(sc.failed, 0);
    assert_eq!(app_row, vec![1usize, 3, 5], "行式 appended 索引");
    assert_eq!(app_col, vec![1usize, 3, 5], "列式 appended 索引");

    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(out_row, out_col, "输出逐位对拍");
    let label = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "detail")
            .and_then(|f| match f.get_value() {
                ModelValue::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect("detail field")
    };
    assert_eq!(
        label(&out_col[0]),
        "nightTime c=4",
        "5M 行：22 时 → nightTime，\"abc c cc\" 含 4 个 c"
    );
    assert_eq!(
        label(&out_col[1]),
        "dayTime c=1",
        "10M 行：10 时 → dayTime，\"no-c\" 含 1 个 c"
    );
    assert_eq!(
        label(&out_col[2]),
        "otherTime c=0",
        "20M 行：07 时 → otherTime，\"zz\" 无 c"
    );
}

/// Q14 变体：fmt 的 IfThenElse 分支 / count_char 参数含 OBJECT 元数据字段。
/// gate 放行（flat FieldRef），但编译期递归 `arg_reads_structured` 拦截 →
/// 整个 yield 行式回退——行式/列式输出必须逐位一致（列式若不回退会渲染原始
/// JSON / 对 JSON 计数，字节分叉）。
#[test]
fn each_columnar_nested_structured_falls_back_matches_row_path() {
    use crate::match_engine::WFL_FIELD_TYPE_METADATA_KEY;
    use crate::match_engine::WFL_FIELD_TYPE_OBJECT;
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new("flag", DataType::Boolean, true),
        ArrowField::new("ext", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_OBJECT.to_string(),
            )]),
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(r#"{"k":1}"#),
                Some(r#"{"c":2}"#),
                None,
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    let b_field = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));
    let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
        qualifier: None,
        name: name.into(),
        args,
    };
    let mut plan = simple_rule_plan(
        "q14_obj",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    // label = fmt("{} {}", if b.flag then b.ext else "x", "y")——结构化藏在分支。
    // cc    = count_char(b.ext, "c")——结构化作 text 参数（解释器 None → 空串）。
    plan.yield_plan.fields = vec![
        YieldField {
            name: "label".into(),
            value: call(
                "fmt",
                vec![
                    Expr::StringLit("{} {}".into()),
                    Expr::IfThenElse {
                        cond: Box::new(b_field("flag")),
                        then_expr: Box::new(b_field("ext")),
                        else_expr: Box::new(Expr::StringLit("x".into())),
                    },
                    Expr::StringLit("y".into()),
                ],
            ),
        },
        YieldField {
            name: "cc".into(),
            value: call(
                "count_char",
                vec![b_field("ext"), Expr::StringLit("c".into())],
            ),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("label".into(), FieldType::Base(BaseType::Chars)),
            ("cc".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );
    // 形状 gate 放行（分支/参数是 flat FieldRef）……
    assert!(exec.each_plan_columnar_safe());

    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);

    assert_eq!(sr.appended, 3, "行式 appended");
    assert_eq!(
        sc.appended, 3,
        "列式 appended（结构化回退仍应产出全部 3 行）"
    );
    assert_eq!(sr.rejected, 0);
    assert_eq!(sc.rejected, 0);
    assert_eq!(sc.failed, 0);
    assert_eq!(app_row, vec![0usize, 1, 2]);
    assert_eq!(app_col, vec![0usize, 1, 2]);

    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(out_row, out_col, "结构化嵌套必须行式回退且输出逐位一致");
    let get = |r: &wp_model_core::model::DataRecord, name: &str| {
        r.fields()
            .find(|f| f.get_name() == name)
            .and_then(|f| match f.get_value() {
                ModelValue::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect(name)
    };
    // label：row 0 true 分支 → [object]；row 1 false 分支 → "x"；row 2 null ext → 空串。
    assert_eq!(
        get(&out_col[0], "label"),
        "[object] y",
        "true 分支渲染 [object]（列式若未回退会渲染原始 JSON）"
    );
    assert_eq!(get(&out_col[1], "label"), "x y", "false 分支渲染 x");
    assert_eq!(
        get(&out_col[2], "label"),
        "",
        "null ext → fmt 参数 None → 空串"
    );
    // cc：count_char(Object) → None → 空串（列式若未回退会对原始 JSON 文本计数）。
    assert_eq!(
        get(&out_col[0], "cc"),
        "",
        "count_char(Object) → None → 空串"
    );
    assert_eq!(
        get(&out_col[1], "cc"),
        "",
        "count_char(Object) → None → 空串"
    );
    assert_eq!(get(&out_col[2], "cc"), "", "count_char(null) → None → 空串");
}

/// 空 rows：wrapper 与 `_with` 都应安全返回零统计（batch 级注册/预留对空批是
/// no-op，循环不执行；`emit_each_direct_batch_columnar` 的空行早退路径同源）。
#[test]
fn each_columnar_empty_rows_is_noop() {
    let b_field = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));
    let mut plan = simple_rule_plan(
        "empty",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        // 列式 filter：即便有 filter，空 rows 也不该有任何求值/拒绝。
        filter: Some(Expr::Bool(true)),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::StringLit("x".into()),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(exec.each_plan_columnar_safe());

    // wrapper（prepare default 路径）。
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = exec.execute_each_direct_batch_columnar(&[], 0, &mut builder, &mut appended);
    assert_eq!(stats.appended, 0);
    assert_eq!(stats.rejected, 0);
    assert_eq!(stats.failed, 0);
    assert!(appended.is_empty());
    assert_eq!(builder.finish().len(), 0, "空批不得产出任何行");

    // _with（真实 prepared + 空 rows）：debug_assert 不得触发，统计为零。
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef],
    )
    .unwrap();
    let prepared = exec.each_batch_prepare(&batch);
    let mut b2 = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app2 = Vec::new();
    let s2 = exec.execute_each_direct_batch_columnar_with(&[], 0, &prepared, &mut b2, &mut app2);
    assert_eq!(s2.appended, 0);
    assert_eq!(s2.rejected, 0);
    assert_eq!(s2.failed, 0);
    assert!(app2.is_empty());
}

/// each filter 引用批 schema 里不存在的列：gate 放行（形状可列式），列式编译
/// 解析成 `ColKind::Null` → 掩码全 None → 全拒绝；行式 `passes_each_filter`
/// 对缺字段求值 None → 同样全拒绝。两路统计与输出必须一致。
#[test]
fn each_columnar_filter_missing_column_rejects_all_parity() {
    let b_field = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));
    let mut plan = simple_rule_plan(
        "missing_filter",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        // b.price 不在下面 batch 的 schema 里。
        filter: Some(Expr::BinOp {
            op: BinOp::Gt,
            left: Box::new(b_field("price")),
            right: Box::new(Expr::Number(1.0)),
        }),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::StringLit("x".into()),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(exec.each_plan_columnar_safe());

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![Some(1), Some(2), None])) as ArrayRef],
    )
    .unwrap();
    let t = 1_700_000_000_000_000_000i64;

    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    assert_eq!(sr.rejected, 3, "行式：缺字段 → None → 全拒绝");
    assert_eq!(sr.appended, 0);

    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    assert_eq!(sc.rejected, 3, "列式：ColKind::Null → 全拒绝");
    assert_eq!(sc.appended, 0);
    assert_eq!(sc.failed, 0);
    assert_eq!(app_row, Vec::<usize>::new());
    assert_eq!(app_col, Vec::<usize>::new());
}

/// 回归：General（列式输出函数）yield **不在字段位 0**——前面有 Field/Lit
/// 字段（真实 q14：id=Field, alert_type=Lit, detail=fmt General, request_count=Lit）。
/// 此前 general_cvecs 用「只数 General 的游标」索引，错位取到 Field/Lit 槽位
/// （None）→ 误走行式回退 + yield_meta 悬空 panic。必须逐位对拍。
#[test]
fn each_columnar_general_yield_not_first_matches_row_path() {
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new("price", DataType::Int64, true),
        ArrowField::new("extra", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(11), Some(22), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(7), Some(8), Some(9)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("ab c"), Some("cc"), None])) as ArrayRef,
        ],
    )
    .unwrap();

    let b_field = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));
    let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
        qualifier: None,
        name: name.into(),
        args,
    };
    let mut plan = simple_rule_plan(
        "mixed_yield_order",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    // 字段顺序刻意让 General 落在第 3 位（前面 Field + Lit）。
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: b_field("auction"),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q14_calc".into()),
        },
        YieldField {
            name: "detail".into(),
            value: call(
                "fmt",
                vec![
                    Expr::StringLit("c={} p={}".into()),
                    call(
                        "count_char",
                        vec![b_field("extra"), Expr::StringLit("c".into())],
                    ),
                    b_field("price"),
                ],
            ),
        },
        YieldField {
            name: "request_count".into(),
            value: Expr::Number(1.0),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Float)),
            ("alert_type".into(), FieldType::Base(BaseType::Chars)),
            ("detail".into(), FieldType::Base(BaseType::Chars)),
            ("request_count".into(), FieldType::Base(BaseType::Float)),
        ]),
    );
    assert!(exec.each_plan_columnar_safe());

    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);

    assert_eq!(sr.appended, 3, "行式 appended");
    assert_eq!(sr.rejected, 0);
    assert_eq!(
        sc.appended, 3,
        "列式 appended（General 不在字段 0 也必须全编译）"
    );
    assert_eq!(sc.rejected, 0);
    assert_eq!(sc.failed, 0);
    assert_eq!(app_row, vec![0usize, 1, 2]);
    assert_eq!(app_col, vec![0usize, 1, 2]);

    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(out_row, out_col, "混合 yield 顺序必须逐位一致");
    let detail = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "detail")
            .and_then(|f| match f.get_value() {
                ModelValue::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect("detail field")
    };
    // 列式必须真的命中 fmt 槽位（错位取 None 会误回退成空串/悬空 panic）。
    assert_eq!(detail(&out_col[0]), "c=1 p=7");
    assert_eq!(detail(&out_col[1]), "c=2 p=8");
    assert_eq!(
        detail(&out_col[2]),
        "",
        "null extra → count_char None → fmt 参数 None → 空串"
    );
}

/// each filter 引用 OBJECT 元数据列：gate 放行（flat FieldRef 形状），但列式
/// 读原始 JSON 文本、解释器解析成 Value::Object——比较可分叉 → filter 槽位
/// 不编译，逐行 `passes_eval_filter` 解释回退。两路必须一致（Object 比较
/// 非 Bool → None → 全拒绝）。
#[test]
fn each_columnar_filter_structured_field_falls_back_parity() {
    use crate::match_engine::WFL_FIELD_TYPE_METADATA_KEY;
    use crate::match_engine::WFL_FIELD_TYPE_OBJECT;

    let b_field = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));
    let mut plan = simple_rule_plan(
        "obj_filter",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        // 原始 JSON 文本恰好等于字面量时，列式会比较命中——解释器是 Object
        // 比较非 Bool → 拒绝；必须走解释回退保持一致。
        filter: Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(b_field("ext")),
            right: Box::new(Expr::StringLit("{\"k\":1}".into())),
        }),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::StringLit("x".into()),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(exec.each_plan_columnar_safe());

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new("ext", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_OBJECT.to_string(),
            )]),
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(r#"{"k":1}"#),
                Some(r#"{"k":2}"#),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let t = 1_700_000_000_000_000_000i64;

    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    assert_eq!(sr.rejected, 2, "行式：Object 比较非 Bool → None → 全拒绝");
    assert_eq!(sr.appended, 0);

    let col_events: Vec<ColumnarEvent> = (0..2).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);
    assert_eq!(
        sc.rejected, 2,
        "列式：结构化 filter 槽位不编译 → 解释回退 → 全拒绝"
    );
    assert_eq!(sc.appended, 0);
    assert_eq!(sc.failed, 0);
    assert_eq!(app_row, Vec::<usize>::new());
    assert_eq!(app_col, Vec::<usize>::new());
}

/// 形状矩阵收口：**多个 General 被 Field/Lit 隔开**（Field, General, Lit,
/// General）——每个 General 槽位按字段位置独立命中，`need_yield_meta` 与
/// 槽位映射必须对齐。若有人把位置索引改回「只数 General 的游标」，此形状
/// 会同时错位两个 General（修复前 general_cvecs 游标 bug 的完整触发面）。
#[test]
fn each_columnar_multiple_generals_interspersed_matches_row_path() {
    use wp_model_core::model::Value as ModelValue;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new("ts", DataType::Int64, true),
        ArrowField::new("extra", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(11), Some(22), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000),
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("ab c"), Some("cc"), None])) as ArrayRef,
        ],
    )
    .unwrap();

    let b_field = |n: &str| Expr::Field(FieldRef::Qualified("b".into(), n.into()));
    let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
        qualifier: None,
        name: name.into(),
        args,
    };
    let mut plan = simple_rule_plan(
        "mixed_interspersed",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        b_field("auction"),
    );
    plan.binds[0].alias = "b".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    // 刻意：Field, General, Lit, General——两个 General 都被非 General 隔开。
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: b_field("auction"),
        },
        YieldField {
            name: "day".into(),
            value: call(
                "strftime",
                vec![b_field("ts"), Expr::StringLit("%Y".into())],
            ),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q14_calc".into()),
        },
        YieldField {
            name: "dots".into(),
            value: call(
                "count_char",
                vec![b_field("extra"), Expr::StringLit("c".into())],
            ),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Float)),
            ("day".into(), FieldType::Base(BaseType::Chars)),
            ("alert_type".into(), FieldType::Base(BaseType::Chars)),
            ("dots".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );
    assert!(exec.each_plan_columnar_safe());

    let t = 1_700_000_000_000_000_000i64;
    let events = crate::match_engine::event_bridge::batch_to_events(&batch);
    let row_refs: Vec<(&Event, i64)> = events.iter().map(|e| (e, t)).collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let sr =
        exec.execute_each_direct_batch(&row_refs, &EmptyLookup, &[], 0, &mut b_row, &mut app_row);
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let col_events: Vec<ColumnarEvent> = (0..3).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_refs: Vec<(&ColumnarEvent, i64)> = col_events.iter().map(|ev| (ev, t)).collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let sc = exec.execute_each_direct_batch_columnar(&col_refs, 0, &mut b_col, &mut app_col);

    assert_eq!(sr.appended, 3);
    assert_eq!(sr.rejected, 0);
    assert_eq!(sc.appended, 3, "两个 General 槽位都必须命中");
    assert_eq!(sc.rejected, 0);
    assert_eq!(sc.failed, 0);
    assert_eq!(app_row, vec![0usize, 1, 2]);
    assert_eq!(app_col, vec![0usize, 1, 2]);

    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(out_row, out_col, "交错多 General 必须逐位一致");
    let get = |r: &wp_model_core::model::DataRecord, name: &str| {
        r.fields()
            .find(|f| f.get_name() == name)
            .and_then(|f| match f.get_value() {
                ModelValue::Chars(v) => Some(v.to_string()),
                _ => None,
            })
            .expect(name)
    };
    // 两个 General 都得真命中各自槽位（错位会取到 Field/Lit 的 None → 空串）。
    assert_eq!(get(&out_col[0], "day"), "2023", "strftime 槽位 1 命中");
    assert_eq!(get(&out_col[1], "day"), "2023");
    assert_eq!(
        get(&out_col[0], "dots"),
        "1",
        "count_char 槽位 3 命中（\"ab c\" 含 1 个 c）"
    );
    assert_eq!(get(&out_col[1], "dots"), "2", "\"cc\" 含 2 个 c");
    assert_eq!(get(&out_col[2], "dots"), "", "null extra → None → 空串");
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
    assert!(!ctx.fields.contains_key("b"));
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
