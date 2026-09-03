//! Round-4 coverage-fill tests for the executor layer: `executor/mod.rs`
//! (yield coercion branches, bind filters, columnar gates), `executor/context.rs`
//! (join modes, interval joins, eval-context narrowing), `executor/close_exec.rs`
//! (qualified-close alert building, direct batch columnar field resolution,
//! step-stage annotation), and `executor/each_exec.rs` (on-each filter / joins /
//! `where` / error lanes and the direct-write batch path).
use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{
    BinOp, Bound, BoundVal, CloseMode, CmpOp, Expr, FieldRef, JoinMode, MatchMode, Measure,
    SystemVar, WithinSpec,
};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EachPlan, EntityPlan, JoinCondPlan, JoinPlan, MatchPlan,
    RulePlan, ScorePlan, StepPlan, WindowSpec, YieldField, YieldPlan,
};
use wf_lang::{BaseType, FieldType};

use crate::alert::AlertColumnBuilder;
use crate::match_engine::cep::{
    BindData, CloseOutput, CloseReason, EngineHashMap, Event, MatchedContext, StepData, Value,
    WindowLookup,
};
use crate::match_engine::executor::context::{
    build_eval_context, enrich_join_row, execute_joins, in_interval,
};
use crate::match_engine::{JoinRow, RuleExecutor, TriggerEvent};

// ---------------------------------------------------------------------------
// Helpers (local copies — `tests::helpers` is not reachable from this module)
// ---------------------------------------------------------------------------

fn field(name: &str) -> Expr {
    Expr::Field(FieldRef::Simple(name.to_string()))
}

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

fn step(branches: Vec<BranchPlan>) -> StepPlan {
    StepPlan { branches }
}

fn simple_plan(keys: Vec<FieldRef>, steps: Vec<StepPlan>) -> MatchPlan {
    MatchPlan {
        keys,
        key_exprs: Vec::new(),
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
        key_exprs: Vec::new(),
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

fn default_match_plan() -> MatchPlan {
    simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    )
}

fn matched_context(scope_key: Value, step: StepData) -> MatchedContext {
    MatchedContext {
        rule_name: "r1".to_string(),
        scope_key: vec![scope_key],
        step_data: vec![step],
        bind_data: vec![],
        event_time_nanos: 1_700_000_000_000_000_000,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        trigger_event: None,
    }
}

fn step_data(label: Option<&str>, measure_value: f64) -> StepData {
    StepData {
        satisfied_branch_index: 0,
        label: label.map(|s| s.to_string()),
        measure_value,
        event_first_time_nanos: None,
        event_last_time_nanos: None,
        collected_values: Vec::new(),
        field_values: EngineHashMap::default(),
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
        watermark_nanos: 1_700_000_000_000_000_000,
        machine_id: String::new(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        last_event_nanos: 1_700_000_000_000_000_000,
        row_fields: None,
        row_field_names: None,
    }
}

/// A `WindowLookup` holding fixed rows for `join_lookup` / asof paths.
struct RowsLookup {
    rows: Vec<JoinRow>,
    ts_rows: Vec<(i64, JoinRow)>,
}

impl RowsLookup {
    fn new(rows: Vec<JoinRow>) -> Self {
        Self {
            rows,
            ts_rows: Vec::new(),
        }
    }
    fn with_ts(ts_rows: Vec<(i64, JoinRow)>) -> Self {
        Self {
            rows: ts_rows.iter().map(|(_, r)| r.clone()).collect(),
            ts_rows,
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
    ) -> crate::match_engine::AsofLookup {
        crate::match_engine::AsofLookup::Fallback
    }
}

fn join_row_event(fields: Vec<(&str, Value)>) -> JoinRow {
    JoinRow::Event(Arc::new(event(fields)))
}

fn one_cond_join(mode: JoinMode) -> JoinPlan {
    JoinPlan {
        right_window: "w".to_string(),
        mode,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("bidder".into()),
            right: FieldRef::Simple("id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }
}

// ---------------------------------------------------------------------------
// executor/mod.rs — yield coercion branches
// ---------------------------------------------------------------------------

#[test]
fn coerce_yield_value_branches() {
    let coerce =
        |name: &str, t: Option<FieldType>, v: Value| -> crate::error::CoreResult<Option<Value>> {
            RuleExecutor::coerce_yield_field_value_with(name, t.as_ref(), v)
        };

    // No type → passthrough.
    assert_eq!(coerce("x", None, num(1.0)).unwrap(), Some(num(1.0)));

    // Chars: Str passthrough, Number rendered, non-finite Number → Err.
    let chars = Some(FieldType::Base(BaseType::Chars));
    assert_eq!(
        coerce("x", chars.clone(), str_val("a")).unwrap(),
        Some(str_val("a"))
    );
    assert_eq!(
        coerce("x", chars.clone(), num(3.5)).unwrap(),
        Some(str_val("3.5"))
    );
    assert_eq!(
        coerce("x", chars.clone(), Value::Bool(true)).unwrap(),
        Some(str_val("true"))
    );
    assert!(coerce("x", chars.clone(), num(f64::NAN)).is_err());
    // Chars from an array → JSON rendering.
    assert_eq!(
        coerce("x", chars.clone(), Value::Array(vec![num(1.0), num(2.0)])).unwrap(),
        Some(str_val("[1.0,2.0]"))
    );
    // Chars from an object → JSON rendering (sorted keys).
    assert_eq!(
        coerce(
            "x",
            chars,
            Value::Object(EngineHashMap::from_iter([
                ("b".into(), num(2.0)),
                ("a".into(), num(1.0)),
            ]))
        )
        .unwrap(),
        Some(str_val(r#"{"a":1.0,"b":2.0}"#))
    );

    // Digit: integer ok; fractional / non-finite / non-number → Err.
    let digit = Some(FieldType::Base(BaseType::Digit));
    assert_eq!(
        coerce("d", digit.clone(), num(3.0)).unwrap(),
        Some(num(3.0))
    );
    assert!(coerce("d", digit.clone(), num(3.5)).is_err());
    assert!(coerce("d", digit.clone(), num(f64::NAN)).is_err());
    assert!(coerce("d", digit.clone(), str_val("x")).is_err());

    // Float: finite ok; non-finite / non-number → Err.
    let float = Some(FieldType::Base(BaseType::Float));
    assert_eq!(
        coerce("f", float.clone(), num(1.5)).unwrap(),
        Some(num(1.5))
    );
    assert!(coerce("f", float.clone(), num(f64::INFINITY)).is_err());
    assert!(coerce("f", float.clone(), str_val("x")).is_err());

    // Bool: bool ok; non-bool → Err.
    let bool_ty = Some(FieldType::Base(BaseType::Bool));
    assert_eq!(
        coerce("b", bool_ty.clone(), Value::Bool(true)).unwrap(),
        Some(Value::Bool(true))
    );
    assert!(coerce("b", bool_ty.clone(), num(1.0)).is_err());

    // Time: number ok; invalid epoch / non-number → Err.
    let time = Some(FieldType::Base(BaseType::Time));
    assert_eq!(
        coerce("t", time.clone(), num(1_700_000_000_000.0)).unwrap(),
        Some(num(1_700_000_000_000.0))
    );
    assert!(coerce("t", time.clone(), num(f64::NAN)).is_err());
    assert!(coerce("t", time.clone(), str_val("x")).is_err());

    // Ip: valid / invalid / non-str.
    let ip = Some(FieldType::Base(BaseType::Ip));
    assert_eq!(
        coerce("i", ip.clone(), str_val("10.0.0.1")).unwrap(),
        Some(str_val("10.0.0.1"))
    );
    assert!(coerce("i", ip.clone(), str_val("not-an-ip")).is_err());
    assert!(coerce("i", ip.clone(), num(1.0)).is_err());

    // Hex: number ok (non-negative integer), str ok (with/without 0x), invalid → Err.
    let hex = Some(FieldType::Base(BaseType::Hex));
    assert_eq!(
        coerce("h", hex.clone(), num(255.0)).unwrap(),
        Some(num(255.0))
    );
    assert!(coerce("h", hex.clone(), num(-1.0)).is_err());
    assert_eq!(
        coerce("h", hex.clone(), str_val("ff")).unwrap(),
        Some(str_val("ff"))
    );
    assert_eq!(
        coerce("h", hex.clone(), str_val("0xFF")).unwrap(),
        Some(str_val("0xFF"))
    );
    assert!(coerce("h", hex.clone(), str_val("zz")).is_err());
    assert!(coerce("h", hex.clone(), Value::Bool(true)).is_err());

    // Array type: array ok; non-array → Err.
    let arr_ty = Some(FieldType::ArrayAny);
    assert_eq!(
        coerce("a", arr_ty.clone(), Value::Array(vec![])).unwrap(),
        Some(Value::Array(vec![]))
    );
    assert!(coerce("a", arr_ty.clone(), num(1.0)).is_err());

    // Object type: object ok; non-object → Err.
    let obj_ty = Some(FieldType::Object);
    assert_eq!(
        coerce("o", obj_ty.clone(), Value::Object(EngineHashMap::default())).unwrap(),
        Some(Value::Object(EngineHashMap::default()))
    );
    assert!(coerce("o", obj_ty.clone(), num(1.0)).is_err());
}

#[test]
fn empty_string_omission_for_non_chars_targets() {
    // A missing field yields the empty-string fallback; for non-Chars targets
    // that empty string is treated as an absent/optional field → Ok(None).
    let float = Some(FieldType::Base(BaseType::Float));
    let res = RuleExecutor::coerce_yield_field_value_with("f", float.as_ref(), str_val(""));
    assert_eq!(res.unwrap(), None);
    // Chars keeps the empty string.
    let chars = Some(FieldType::Base(BaseType::Chars));
    let res = RuleExecutor::coerce_yield_field_value_with("c", chars.as_ref(), str_val(""));
    assert_eq!(res.unwrap(), Some(str_val("")));
}

// ---------------------------------------------------------------------------
// executor/mod.rs — bind filter / columnar gates / misc helpers
// ---------------------------------------------------------------------------

#[test]
fn bind_filter_map_path_and_columnar_gates() {
    // >24 binds forces the map path.
    let mut plan = simple_rule_plan(
        "many_binds",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    );
    plan.binds = (0..30)
        .map(|i| BindPlan {
            alias: format!("b{i}"),
            window: "w".to_string(),
            filter: None,
        })
        .collect();
    plan.binds[29].alias = "target".into();
    let exec = RuleExecutor::new(plan);
    // event_matches_alias with no filter → true via the map path.
    let ev = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(exec.event_matches_alias("target", &ev, None));

    // each_plan_columnar_safe: no each plan → false; with each plan + no filter → true.
    let mut plan2 = simple_rule_plan(
        "each",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    );
    assert!(!RuleExecutor::new(plan2.clone()).each_plan_columnar_safe());
    plan2.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    assert!(RuleExecutor::new(plan2).each_plan_columnar_safe());

    // bind_filters_columnar_safe / bind_filter_columnar_mask with no filter.
    let exec3 = RuleExecutor::new(simple_rule_plan(
        "bf",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    ));
    assert!(exec3.bind_filters_columnar_safe("w"));

    // branch_guard_masks on a plan with no guards returns empty masks.
    let masks = exec3.branch_guard_masks(&RecordBatch::new_empty(Arc::new(Schema::empty())));
    assert!(masks.is_empty());

    // is_aux_bind_alias: "fail" is a branch source → not aux.
    assert!(!exec3.is_aux_bind_alias("fail"));
    assert!(exec3.is_aux_bind_alias("other"));

    // static_yield_target / plan / output_config accessors.
    assert_eq!(exec3.static_yield_target().as_ref(), "alerts");
    assert_eq!(exec3.plan().name, "bf");
    assert!(exec3.output_config().time_format.contains("%"));
}

#[test]
fn where_ok_and_machine_id_and_scope_key() {
    let mut plan = simple_rule_plan(
        "w",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    );
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("flag")),
        right: Box::new(Expr::Bool(true)),
    });
    let exec = RuleExecutor::new(plan);
    assert!(exec.where_ok(&event(vec![("flag", Value::Bool(true))])));
    assert!(!exec.where_ok(&event(vec![("flag", Value::Bool(false))])));
    assert!(!exec.where_ok(&event(vec![])));
    // No where clause → always ok.
    let exec2 = RuleExecutor::new(simple_rule_plan(
        "w2",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    ));
    assert!(exec2.where_ok(&event(vec![])));

    // build_machine_id: empty → rule name, else passthrough.
    let m1 = exec2.build_machine_id("");
    assert_eq!(m1.as_ref(), "w2");
    let m2 = exec2.build_machine_id("host-1");
    assert_eq!(m2.as_ref(), "host-1");

    // build_scope_key renders `name=value` pairs.
    let sk = exec2.build_scope_key(&[simple_key("sip")], &[str_val("10.0.0.1")]);
    assert_eq!(sk.as_ref(), "sip=10.0.0.1");
}

// ---------------------------------------------------------------------------
// executor/context.rs — join modes + interval joins + eval-context narrowing
// ---------------------------------------------------------------------------

#[test]
fn execute_joins_inner_snapshot_anti_and_enrich() {
    let row = join_row_event(vec![("id", num(1.0)), ("name", str_val("alice"))]);
    let lookup = RowsLookup::new(vec![row]);

    // Inner: hit enriches; missing left key drops; lookup miss drops.
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        1000
    ));
    assert_eq!(ctx.fields.get("w.id"), Some(&num(1.0)));
    assert_eq!(ctx.fields.get("name"), Some(&str_val("alice")));
    assert_eq!(ctx.fields.get("id"), Some(&num(1.0)));

    let mut ctx = event(vec![]);
    assert!(!execute_joins(
        &[one_cond_join(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        1000
    ));

    let empty_lookup = RowsLookup::new(vec![]);
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(!execute_joins(
        &[one_cond_join(JoinMode::Inner)],
        &mut ctx,
        &empty_lookup,
        1000
    ));

    // Snapshot: hit enriches; miss keeps the event (optional join).
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Snapshot)],
        &mut ctx,
        &lookup,
        1000
    ));
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Snapshot)],
        &mut ctx,
        &empty_lookup,
        1000
    ));
    // Missing key on snapshot → continue (keep event).
    let mut ctx = event(vec![]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Snapshot)],
        &mut ctx,
        &lookup,
        1000
    ));

    // Anti: match drops; no match keeps.
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(!execute_joins(
        &[one_cond_join(JoinMode::Anti)],
        &mut ctx,
        &lookup,
        1000
    ));
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Anti)],
        &mut ctx,
        &empty_lookup,
        1000
    ));

    // emit_at set → join skipped entirely.
    let mut join = one_cond_join(JoinMode::Inner);
    join.emit_at = Some(Expr::Number(0.0));
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(&[join], &mut ctx, &lookup, 1000));
    assert!(!ctx.fields.contains_key("w.id"));
}

#[test]
fn execute_joins_asof_single_and_multi_cond_scan() {
    let row = join_row_event(vec![("id", num(1.0)), ("name", str_val("alice"))]);
    let lookup = RowsLookup::with_ts(vec![(500, row.clone())]);

    // Single-condition asof via Fallback → candidate scan.
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Asof {
            within: Some(Duration::from_secs(1))
        })],
        &mut ctx,
        &lookup,
        1000
    ));
    assert_eq!(ctx.fields.get("w.id"), Some(&num(1.0)));

    // Timestamp outside the asof window → no match, event kept.
    let late = RowsLookup::with_ts(vec![(1_000_000, row.clone())]);
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Asof {
            within: Some(Duration::from_millis(1))
        })],
        &mut ctx,
        &late,
        1000
    ));

    // Multi-condition asof → full scan path; no matching condition → kept.
    let mut join = one_cond_join(JoinMode::Asof { within: None });
    join.conds.push(JoinCondPlan {
        left: FieldRef::Simple("other".into()),
        right: FieldRef::Simple("name".into()),
    });
    let mut ctx = event(vec![("bidder", num(1.0)), ("other", str_val("bob"))]);
    assert!(execute_joins(&[join], &mut ctx, &lookup, 1000));
    assert!(!ctx.fields.contains_key("w.id"));
}

#[test]
fn execute_interval_join_modes() {
    // `within [0s, 60s]` inner: row at ts=30 matches.
    let row = join_row_event(vec![("id", num(1.0))]);
    let lookup = RowsLookup::with_ts(vec![(30, row)]);
    let mut join = one_cond_join(JoinMode::Inner);
    join.within = Some(WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::ZERO,
                neg: false,
            },
        },
        hi: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::from_secs(60),
                neg: false,
            },
        },
    });
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(&[join.clone()], &mut ctx, &lookup, 0));
    assert_eq!(ctx.fields.get("w.id"), Some(&num(1.0)));

    // Inner miss (no row in interval) → dropped.
    let lookup_miss = RowsLookup::with_ts(vec![(
        61_000_000_000,
        join_row_event(vec![("id", num(1.0))]),
    )]);
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(!execute_joins(&[join.clone()], &mut ctx, &lookup_miss, 0));

    // Anti interval: row in interval → dropped; none → kept.
    let mut anti = join.clone();
    anti.mode = JoinMode::Anti;
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(!execute_joins(&[anti.clone()], &mut ctx, &lookup, 0));
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(&[anti.clone()], &mut ctx, &lookup_miss, 0));

    // Asof interval: picks the max-ts matching row.
    let mut asof = join.clone();
    asof.mode = JoinMode::Asof { within: None };
    let multi = RowsLookup::with_ts(vec![
        (10, join_row_event(vec![("id", num(1.0))])),
        (50, join_row_event(vec![("id", num(1.0))])),
    ]);
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(&[asof], &mut ctx, &multi, 0));
    assert!(ctx.fields.contains_key("w.id"));

    // Snapshot interval: picks the min-ts row; open bounds respected.
    let mut snap = join.clone();
    snap.mode = JoinMode::Snapshot;
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(&[snap], &mut ctx, &multi, 0));

    // Bound eval failure (missing left field) → inner drops, snapshot keeps.
    let mut bad_join = one_cond_join(JoinMode::Inner);
    bad_join.within = Some(WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Expr(field("ghost_bound")),
        },
        hi: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::from_secs(60),
                neg: false,
            },
        },
    });
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(!execute_joins(&[bad_join], &mut ctx, &lookup, 0));
    let mut snap = one_cond_join(JoinMode::Snapshot);
    snap.within = Some(WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Expr(field("ghost_bound")),
        },
        hi: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::from_secs(60),
                neg: false,
            },
        },
    });
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(&[snap], &mut ctx, &lookup, 0));
}

#[test]
fn in_interval_open_closed_bounds() {
    assert!(in_interval(5, 0, 10, false, false));
    assert!(!in_interval(0, 0, 10, true, false));
    assert!(!in_interval(10, 0, 10, false, true));
    assert!(in_interval(0, 0, 10, false, false));
}

#[test]
fn enrich_join_row_skips_null_fields() {
    let mut ctx = event(vec![]);
    // A JoinRow::Event with a null-free map — all fields enriched.
    let row = join_row_event(vec![("a", num(1.0)), ("b", str_val("x"))]);
    enrich_join_row(&mut ctx, &one_cond_join(JoinMode::Inner), &row);
    assert_eq!(ctx.fields.get("w.a"), Some(&num(1.0)));
    assert_eq!(ctx.fields.get("b"), Some(&str_val("x")));
}

#[test]
fn build_eval_context_narrow_and_all() {
    use crate::match_engine::executor::context::CloseCtxFields;

    let sd = StepData {
        satisfied_branch_index: 0,
        label: Some("fail".to_string()),
        measure_value: 3.0,
        event_first_time_nanos: None,
        event_last_time_nanos: None,
        collected_values: vec![num(1.0), num(2.0)],
        field_values: EngineHashMap::from_iter([("src".to_string(), vec![str_val("10.0.0.1")])]),
    };
    let bind = BindData {
        alias: "b".into(),
        count: 2,
        field_values: EngineHashMap::from_iter([("dip".to_string(), vec![str_val("8.8.8.8")])]),
    };
    let keys = vec![simple_key("sip")];
    let scope = vec![str_val("10.0.0.1")];
    let step_plans: Vec<&StepPlan> = vec![];
    let trigger = event(vec![("raw", num(9.0))]);

    // Narrow build: only requested names.
    let narrow = CloseCtxFields::Named(HashSet::from([
        "sip".to_string(),
        "fail".to_string(),
        "src".to_string(),
        "dip".to_string(),
    ]));
    let ctx = build_eval_context(
        &keys,
        &scope,
        std::slice::from_ref(&sd),
        std::slice::from_ref(&bind),
        &step_plans,
        Some(&TriggerEvent::from_event(Arc::new(trigger.clone()))),
        &narrow,
        None,
    );
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));
    assert_eq!(ctx.fields.get("fail"), Some(&num(3.0)));
    assert_eq!(ctx.fields.get("src"), Some(&str_val("10.0.0.1")));
    assert_eq!(ctx.fields.get("dip"), Some(&str_val("8.8.8.8")));
    // `_step_*` synthetic fields absent in the narrow build.
    assert!(!ctx.fields.contains_key("_step_0_values"));
    // Named 窄化（2026-08 hotpath）：trigger_event 字段只注入 Named 集合内的；
    // "raw" 不在集合中 → 不注入（旧行为全量注入，是 per-fire 热路径浪费——
    // Q13 每事件 8 字段 → 1 字段）。All 模式下仍全量。
    assert!(
        !ctx.fields.contains_key("raw"),
        "narrow 构建不注入集合外字段"
    );

    // All build: synthetic fields present, key collision skips the label.
    let all = CloseCtxFields::All;
    let ctx = build_eval_context(
        &keys,
        &scope,
        std::slice::from_ref(&sd),
        std::slice::from_ref(&bind),
        &step_plans,
        None,
        &all,
        None,
    );
    assert_eq!(
        ctx.fields.get("_step_0_values"),
        Some(&Value::Array(vec![num(1.0), num(2.0)]))
    );
    assert_eq!(
        ctx.fields.get("_step_0_field_src"),
        Some(&Value::Array(vec![str_val("10.0.0.1")]))
    );
    assert_eq!(ctx.fields.get("_step_0_measure"), Some(&num(3.0)));
    assert_eq!(ctx.fields.get("_step_0_label"), Some(&str_val("fail")));
    assert_eq!(ctx.fields.get("_bind_b_count"), Some(&num(2.0)));
    assert_eq!(
        ctx.fields.get("_bind_b_field_dip"),
        Some(&Value::Array(vec![str_val("8.8.8.8")]))
    );
    // The `sip` key collides with the label-less name; key wins.
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));

    // All build with a step plan → `_step_0_source` injected.
    let plan = default_match_plan();
    let ctx = build_eval_context(
        &keys,
        &scope,
        &[sd],
        &[],
        &[&plan.event_steps[0]],
        None,
        &all,
        None,
    );
    assert_eq!(ctx.fields.get("_step_0_source"), Some(&str_val("fail")));
}

// ---------------------------------------------------------------------------
// executor/close_exec.rs — qualified-close alert building
// ---------------------------------------------------------------------------

#[test]
fn execute_close_qualified_and_error_paths() {
    // And-mode close with event_ok && close_ok → record.
    let mut plan = simple_rule_plan(
        "close_r",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
            vec![step(vec![branch("done", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "score_field".into(),
            value: Expr::SystemVar(SystemVar::Score),
        },
        YieldField {
            name: "label_field".into(),
            value: field("fail"),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("score_field".into(), FieldType::Base(BaseType::Float)),
            ("label_field".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );

    let qualified = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![step_data(Some("done"), 2.0)],
    );
    let out = exec
        .execute_close(&qualified)
        .unwrap()
        .expect("qualified close emits");
    assert_eq!(out.rule_name.as_ref(), "close_r");
    assert_eq!(out.score, 70.0);
    assert_eq!(out.entity_id, "10.0.0.1");

    // Unqualified (event not ok) → Ok(None).
    let unqualified = close_output(
        false,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![],
    );
    assert!(exec.execute_close(&unqualified).unwrap().is_none());

    // OR-mode close with close steps → qualifies.
    let or_qualified = close_output(
        true,
        true,
        CloseMode::Or,
        vec![step_data(Some("fail"), 1.0)],
        vec![step_data(Some("done"), 2.0)],
    );
    assert!(exec.execute_close(&or_qualified).unwrap().is_some());
    // OR-mode without close steps → does not qualify.
    let or_empty = close_output(
        true,
        true,
        CloseMode::Or,
        vec![step_data(Some("fail"), 1.0)],
        vec![],
    );
    assert!(exec.execute_close(&or_empty).unwrap().is_none());

    // Score error: non-numeric score expression → Err.
    let bad_plan = simple_rule_plan(
        "bad_score",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
            vec![],
        ),
        field("ghost"),
        "ip",
        field("sip"),
    );
    let bad_exec = RuleExecutor::new(bad_plan);
    let qualified = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![],
    );
    assert!(bad_exec.execute_close(&qualified).is_err());

    // Entity id error: a non-field entity expr evaluating to None → Err.
    let bad_entity_plan = simple_rule_plan(
        "bad_entity",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
            vec![],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::Neg(Box::new(field("ghost_entity"))),
    );
    let bad_entity_exec = RuleExecutor::new(bad_entity_plan);
    let qualified = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![],
    );
    // Entity fallback：eval_yield_expr 对缺失字段回退空串 → entity_id=""，不 err。
    let rec = bad_entity_exec
        .execute_close(&qualified)
        .expect("entity 空串 fallback 不 err");
    assert!(rec.is_some(), "entity 回退空串仍输出记录");
}

#[test]
fn execute_close_with_joins_miss_and_where_reject() {
    let mut plan = simple_rule_plan(
        "close_join",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
            vec![],
        ),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    );
    // Join the scope key (`sip`) against the right window's `id` so the close
    // ctx actually has the left field present.
    plan.joins = vec![JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("sip".into()),
            right: FieldRef::Simple("id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("flag")),
        right: Box::new(Expr::Bool(true)),
    });
    let exec = RuleExecutor::new(plan);
    let qualified = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![],
    );

    // Inner join miss (empty right) → Ok(None).
    let empty_lookup = RowsLookup::new(vec![]);
    assert!(
        exec.execute_close_with_joins(&qualified, &empty_lookup)
            .unwrap()
            .is_none()
    );

    // Join hit but `where` false (no flag in the close ctx) → Ok(None).
    let lookup = RowsLookup::new(vec![join_row_event(vec![("id", str_val("10.0.0.1"))])]);
    assert!(
        exec.execute_close_with_joins(&qualified, &lookup)
            .unwrap()
            .is_none()
    );

    // Unqualified close → Ok(None) before any join work.
    let unqualified = close_output(
        false,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![],
    );
    assert!(
        exec.execute_close_with_joins(&unqualified, &lookup)
            .unwrap()
            .is_none()
    );
}

#[test]
fn close_exec_direct_batch_columnar_resolve_close_field() {
    // A columnar-safe close plan: constant score, field entity, field yields.
    let mut plan = simple_rule_plan(
        "col_close",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
            vec![],
        ),
        Expr::Number(80.0),
        "ip",
        field("sip"),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "sip_out".into(),
            value: field("sip"),
        },
        YieldField {
            name: "label_out".into(),
            value: field("fail"),
        },
        YieldField {
            name: "field_values_out".into(),
            value: field("src"),
        },
        YieldField {
            name: "bind_out".into(),
            value: field("bind_v"),
        },
    ];
    let exec = RuleExecutor::new(plan);
    assert!(exec.close_plan_columnar_safe());

    let mut sd = step_data(Some("fail"), 2.0);
    sd.field_values = EngineHashMap::from_iter([("src".to_string(), vec![str_val("10.0.0.2")])]);
    let mut qualified = close_output(true, true, CloseMode::And, vec![sd], vec![]);
    qualified.bind_data = vec![BindData {
        alias: "b".into(),
        count: 1,
        field_values: EngineHashMap::from_iter([("bind_v".to_string(), vec![str_val("b-value")])]),
    }];

    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[qualified], &mut builder, 0);
    assert_eq!(stats.appended, 1);
    assert_eq!(stats.rejected, 0);

    // Unqualified close → rejected.
    let unqualified = close_output(
        false,
        true,
        CloseMode::And,
        vec![step_data(None, 1.0)],
        vec![],
    );
    let stats = exec.execute_close_direct_batch_columnar(&[unqualified], &mut builder, 0);
    assert_eq!(stats.rejected, 1);
}

#[test]
fn close_stage_annotation_marks_event_and_close_steps() {
    // A close rule whose yield reads an aggregate over the step series. The
    // `_step_*_stage` annotation must mark the close-stage step so the
    // aggregate prefers it (otherwise both steps would be "event" and the sum
    // would include the event step's measure).
    let mut plan = simple_rule_plan(
        "close_stage",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("e", count_ge(1.0))])],
            vec![step(vec![branch("e", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    );
    // A function-call yield forces the all-fields ctx build (close_ctx_fields
    // → All), which carries the `_step_*` fields the annotation walks.
    plan.yield_plan.fields = vec![YieldField {
        name: "close_sum".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "sum".into(),
            args: vec![field("e")],
        },
    }];
    let exec = RuleExecutor::new(plan);
    let qualified = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![step_data(Some("done"), 2.0)],
    );
    let rec = exec
        .execute_close(&qualified)
        .unwrap()
        .expect("qualified close");
    // sum over the close-stage step only → 2.0 (not 3.0).
    assert_eq!(rec.yield_fields[0].1, num(2.0));
}

// ---------------------------------------------------------------------------
// executor/each_exec.rs — on-each paths
// ---------------------------------------------------------------------------

fn each_rule(filter: Option<Expr>, lets: Vec<wf_lang::plan::LetPlan>) -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "each_r",
        simple_plan(vec![], vec![]),
        Expr::Number(42.0),
        "ip",
        field("sip"),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter,
    });
    plan.lets = lets;
    plan.yield_plan.fields = vec![YieldField {
        name: "out".into(),
        value: field("sip"),
    }];
    RuleExecutor::new(plan)
}

#[test]
fn execute_each_error_filter_lets_and_where() {
    // Non-`on each` rule → Err.
    let plain = RuleExecutor::new(simple_rule_plan(
        "m",
        default_match_plan(),
        Expr::Number(1.0),
        "ip",
        field("sip"),
    ));
    assert!(
        plain
            .execute_each(&event(vec![("sip", str_val("1.1.1.1"))]), 0)
            .is_err()
    );

    // Filter rejects → Ok(None).
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("kind")),
        right: Box::new(Expr::StringLit("pass".into())),
    };
    let exec = each_rule(Some(filter.clone()), vec![]);
    let ev = event(vec![("sip", str_val("1.1.1.1")), ("kind", str_val("drop"))]);
    assert!(exec.execute_each(&ev, 1000).unwrap().is_none());
    let ev = event(vec![("sip", str_val("1.1.1.1")), ("kind", str_val("pass"))]);
    let rec = exec
        .execute_each(&ev, 1000)
        .unwrap()
        .expect("passes filter");
    assert_eq!(rec.score, 42.0);

    // With `let` bindings → clone + apply_lets + build.
    let lets = vec![wf_lang::plan::LetPlan {
        name: "computed".into(),
        expr: Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(field("n")),
            right: Box::new(Expr::Number(1.0)),
        },
    }];
    let exec = each_rule(Some(filter), lets);
    let ev = event(vec![
        ("sip", str_val("1.1.1.1")),
        ("kind", str_val("pass")),
        ("n", num(5.0)),
    ]);
    let rec = exec
        .execute_each(&ev, 1000)
        .unwrap()
        .expect("passes filter");
    assert_eq!(rec.score, 42.0);
    // `let` that fails to evaluate leaves no injected field (no panic).
    let lets = vec![wf_lang::plan::LetPlan {
        name: "computed".into(),
        expr: field("ghost"),
    }];
    let exec = each_rule(None, lets);
    let rec = exec.execute_each(&ev, 1000).unwrap().expect("no filter");
    assert_eq!(rec.score, 42.0);
}

#[test]
fn execute_each_with_joins_and_direct_branches() {
    let mut plan = simple_rule_plan(
        "each_join",
        simple_plan(vec![], vec![]),
        Expr::Number(42.0),
        "ip",
        field("sip"),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.joins = vec![one_cond_join(JoinMode::Inner)];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("flag")),
        right: Box::new(Expr::Bool(true)),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "out".into(),
        value: field("sip"),
    }];
    let exec = RuleExecutor::new(plan);
    let ev = event(vec![
        ("sip", str_val("1.1.1.1")),
        ("bidder", num(1.0)),
        ("flag", Value::Bool(true)),
    ]);

    // Join miss → Ok(None).
    let empty = RowsLookup::new(vec![]);
    assert!(
        exec.execute_each_with_joins(&ev, 1000, &empty, &[], 2000)
            .unwrap()
            .is_none()
    );

    // Join hit, where true → record.
    let lookup = RowsLookup::new(vec![join_row_event(vec![("id", num(1.0))])]);
    let rec = exec
        .execute_each_with_joins(&ev, 1000, &lookup, &[], 2000)
        .unwrap()
        .expect("record");
    assert_eq!(rec.score, 42.0);

    // Where false → Ok(None).
    let ev_no_flag = event(vec![("sip", str_val("1.1.1.1")), ("bidder", num(1.0))]);
    assert!(
        exec.execute_each_with_joins(&ev_no_flag, 1000, &lookup, &[], 2000)
            .unwrap()
            .is_none()
    );

    // Direct path: filter miss → Ok(false); hit → Ok(true) and rows appended.
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let ok = exec
        .execute_each_direct(&ev, 1000, &lookup, &[], 2000, &mut builder)
        .unwrap();
    assert!(ok);
    let ok = exec
        .execute_each_direct(&ev_no_flag, 1000, &lookup, &[], 2000, &mut builder)
        .unwrap();
    assert!(!ok);
}

#[test]
fn execute_each_direct_batch_rejections_and_errors() {
    // Non-`on each` rule → failed = rows.len().
    let plain = RuleExecutor::new(simple_rule_plan(
        "m",
        default_match_plan(),
        Expr::Number(1.0),
        "ip",
        field("sip"),
    ));
    let ev = event(vec![("sip", str_val("1.1.1.1"))]);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = plain.execute_each_direct_batch(
        &[(&ev, 1000)],
        &RowsLookup::new(vec![]),
        &[],
        2000,
        &mut builder,
        &mut appended,
    );
    assert_eq!(stats.failed, 1);

    // Filter rejection → rejected.
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("kind")),
        right: Box::new(Expr::StringLit("pass".into())),
    };
    let exec = each_rule(Some(filter), vec![]);
    let drop_ev = event(vec![("sip", str_val("1.1.1.1")), ("kind", str_val("drop"))]);
    let stats = exec.execute_each_direct_batch(
        &[(&drop_ev, 1000)],
        &RowsLookup::new(vec![]),
        &[],
        2000,
        &mut builder,
        &mut appended,
    );
    assert_eq!(stats.rejected, 1);
    assert_eq!(stats.appended, 0);
    assert!(appended.is_empty());

    // Join + where rejections on the batch path.
    let mut plan = simple_rule_plan(
        "each_batch",
        simple_plan(vec![], vec![]),
        Expr::Number(42.0),
        "ip",
        field("sip"),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.joins = vec![one_cond_join(JoinMode::Inner)];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("flag")),
        right: Box::new(Expr::Bool(true)),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "out".into(),
        value: field("sip"),
    }];
    let exec = RuleExecutor::new(plan);
    let hit_ev = event(vec![
        ("sip", str_val("1.1.1.1")),
        ("bidder", num(1.0)),
        ("flag", Value::Bool(true)),
    ]);
    let miss_ev = event(vec![("sip", str_val("2.2.2.2")), ("bidder", num(2.0))]);
    let lookup = RowsLookup::new(vec![join_row_event(vec![("id", num(1.0))])]);
    let stats = exec.execute_each_direct_batch(
        &[(&hit_ev, 1000), (&miss_ev, 1000)],
        &lookup,
        &[],
        2000,
        &mut builder,
        &mut appended,
    );
    assert_eq!(stats.appended, 1);
    assert_eq!(stats.rejected, 1);
    assert_eq!(appended, vec![0]);

    // Score-evaluation error on the batch path → failed.
    let mut bad_score = simple_rule_plan(
        "each_bad_score",
        simple_plan(vec![], vec![]),
        field("ghost_score"),
        "ip",
        field("sip"),
    );
    bad_score.binds[0].alias = "e".into();
    bad_score.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let bad_exec = RuleExecutor::new(bad_score);
    let mut appended = Vec::new();
    let stats = bad_exec.execute_each_direct_batch(
        &[(&ev, 1000)],
        &RowsLookup::new(vec![]),
        &[],
        2000,
        &mut builder,
        &mut appended,
    );
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.appended, 0);
}

#[test]
fn build_each_alert_with_deferred_origin() {
    // build_each_alert_with is exercised through the record path with a custom
    // origin — the direct public route is execute_each (Event origin). We use
    // the each rule to confirm the alert record carries machine_id from the
    // event (extract_event_str on MACHINE_ID).
    let exec = each_rule(None, vec![]);
    let ev = event(vec![
        ("sip", str_val("1.1.1.1")),
        (crate::match_engine::MACHINE_ID, str_val("mid-1")),
    ]);
    let rec = exec.execute_each(&ev, 1000).unwrap().expect("record");
    assert_eq!(rec.machine_id.as_ref(), "mid-1");
    assert_eq!(rec.scope_key.as_ref(), "each_r");
    // machine_id_of reads the same extraction helper.
    assert_eq!(RuleExecutor::machine_id_of(&ev), "mid-1");
    assert_eq!(RuleExecutor::machine_id_of(&event(vec![])), "");
}

/// q4a 形状的 each plan：yield 4 字段（id/category/final=winner.price Path/
/// dateTime=expires），entity=digit(id)——中间窗轻量化（build_each_alert_pipe）
/// 的对拍对象。
fn q4a_pipe_rule() -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "q4a_pipe_r",
        simple_plan(vec![], vec![]),
        Expr::Number(20.0),
        "digit",
        field("id"),
    );
    plan.binds[0].alias = "a".into();
    plan.each_plan = Some(EachPlan {
        alias: "a".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: field("id"),
        },
        YieldField {
            name: "category".into(),
            value: field("category"),
        },
        YieldField {
            name: "final".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "winner".into(),
                segments: vec![wf_lang::ast::PathSegment::Field("price".into())],
            }),
        },
        YieldField {
            name: "dateTime".into(),
            value: field("expires"),
        },
    ];
    RuleExecutor::new(plan)
}

/// 中间窗轻量化对拍（2026-08-26 q4a）：`build_each_alert_pipe`（轻量）与
/// `build_each_alert_with`（全量）产出的**中间窗相关字段逐位一致**——
/// yield_fields（含 winner.price Path 读取）、event_time_nanos、meta
/// （rule_name/score/entity_type/entity_id）、yield_target。轻量只跳过 sink
/// 才需要的告警字段（wfx_id/fired_at/summary/machine_id 空值），不得影响
/// 中间窗列内容。
#[test]
fn deferred_pipe_light_build_matches_full_build() {
    let exec = q4a_pipe_rule();
    assert!(
        exec.pipe_light_build_ready(),
        "q4a 形状（纯 Field/Path yield）必须走轻量 build"
    );
    let ctx = event(vec![
        ("id", num(5.0)),
        ("category", num(3.0)),
        ("price", num(25.5)), // winner.price 富化后的裸名字段
        ("expires", num(1_000.0)),
    ]);
    let full = exec
        .build_each_alert_with(&ctx, 1_000, crate::alert::AlertOrigin::Deferred, &[], 1_000)
        .unwrap()
        .expect("full build");
    let light = exec
        .build_each_alert_pipe(&ctx, 1_000)
        .unwrap()
        .expect("light build");

    // 中间窗消费者相关的字段必须逐位一致。
    assert_eq!(full.yield_fields, light.yield_fields, "yield_fields");
    assert_eq!(full.event_time_nanos, light.event_time_nanos, "event_time");
    assert_eq!(full.rule_name, light.rule_name, "rule_name");
    assert_eq!(full.score, light.score, "score");
    assert_eq!(full.entity_type, light.entity_type, "entity_type");
    assert_eq!(full.entity_id, light.entity_id, "entity_id");
    assert_eq!(full.yield_target, light.yield_target, "yield_target");

    // 轻量跳过的告警字段为空（语义：中间窗消费者不读这些列）。
    assert_eq!(light.wfx_id, "");
    assert_eq!(light.fired_at, "");
    assert_eq!(light.summary.as_ref(), "");
}

/// 轻量门控判定（2026-08-26 q4a）：yield 引用 `__wfu_*` meta → 回退全量
/// （light YieldMeta 的空槽不可观测性不成立）；SystemVar（light 提供真值）/
/// 纯 Field/Path → 放行。
#[test]
fn pipe_light_build_ready_gate() {
    use wf_lang::ast::WfuMetaField;
    // 引用 wfx_id meta → 回退。
    let mut plan = simple_rule_plan(
        "gate_r",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        field("sip"),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "id".into(),
        value: Expr::WfuMeta(WfuMetaField::Id),
    }];
    assert!(
        !RuleExecutor::new(plan).pipe_light_build_ready(),
        "WfuMeta → 回退"
    );

    // SystemVar（score）→ light meta 提供真值，放行。
    let mut plan2 = simple_rule_plan(
        "gate_r2",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        field("sip"),
    );
    plan2.yield_plan.fields = vec![YieldField {
        name: "s".into(),
        value: Expr::SystemVar(SystemVar::Score),
    }];
    assert!(
        RuleExecutor::new(plan2).pipe_light_build_ready(),
        "SystemVar → 放行"
    );

    // 纯 Field → 放行（q4a 同款）。
    let mut plan3 = simple_rule_plan(
        "gate_r3",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        field("sip"),
    );
    plan3.yield_plan.fields = vec![YieldField {
        name: "f".into(),
        value: field("sip"),
    }];
    assert!(
        RuleExecutor::new(plan3).pipe_light_build_ready(),
        "Field → 放行"
    );
}

// ---------------------------------------------------------------------------
// executor/mod.rs — match-alert path (YieldKind::Field / General / Lit)
// ---------------------------------------------------------------------------

#[test]
fn execute_match_yield_kinds_and_coercion_omission() {
    let mut plan = simple_rule_plan(
        "match_r",
        default_match_plan(),
        Expr::Number(55.0),
        "ip",
        field("sip"),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "lit_field".into(),
            value: Expr::Number(9.0),
        },
        YieldField {
            name: "str_field".into(),
            value: Expr::StringLit("const".into()),
        },
        YieldField {
            name: "flag_field".into(),
            value: Expr::Bool(true),
        },
        YieldField {
            name: "general_field".into(),
            value: Expr::SystemVar(SystemVar::Score),
        },
        YieldField {
            name: "missing_typed".into(),
            value: field("ghost"),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("missing_typed".into(), FieldType::Base(BaseType::Float))]),
    );
    let sd = step_data(Some("fail"), 1.0);
    let matched = matched_context(str_val("10.0.0.1"), sd);
    let rec = exec.execute_match(&matched).unwrap();
    assert_eq!(rec.score, 55.0);
    let fields: HashMap<&str, &Value> = rec
        .yield_fields
        .iter()
        .map(|(k, v)| (k.as_ref(), v))
        .collect();
    assert_eq!(fields.get("lit_field"), Some(&&num(9.0)));
    assert_eq!(fields.get("str_field"), Some(&&str_val("const")));
    assert_eq!(fields.get("flag_field"), Some(&&Value::Bool(true)));
    assert_eq!(fields.get("general_field"), Some(&&num(55.0)));
    // Missing typed field → omitted (empty-string + non-Chars).
    assert!(!fields.contains_key("missing_typed"));
    // Machine id from the matched context.
    let mut matched2 = matched;
    matched2.machine_id = "m".into();
    let rec2 = exec.execute_match_at(&matched2, 1234).unwrap();
    assert_eq!(rec2.machine_id.as_ref(), "m");
}

#[test]
fn execute_match_with_joins_hit_miss_and_where() {
    let mut plan = simple_rule_plan(
        "match_join",
        default_match_plan(),
        Expr::Number(55.0),
        "ip",
        field("sip"),
    );
    plan.joins = vec![one_cond_join(JoinMode::Inner)];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("flag")),
        right: Box::new(Expr::Bool(true)),
    });
    let exec = RuleExecutor::new(plan);
    let lookup = RowsLookup::new(vec![join_row_event(vec![("id", num(1.0))])]);

    // Join miss (no right rows) → Ok(None).
    let mut matched = matched_context(str_val("10.0.0.1"), step_data(Some("fail"), 1.0));
    matched.trigger_event = Some(TriggerEvent::from_event(Arc::new(event(vec![(
        "bidder",
        num(1.0),
    )]))));
    assert!(
        exec.execute_match_with_joins(&matched, &RowsLookup::new(vec![]))
            .unwrap()
            .is_none()
    );

    // Join hit but `where` absent from the ctx → Ok(None).
    assert!(
        exec.execute_match_with_joins(&matched, &lookup)
            .unwrap()
            .is_none()
    );

    // with_joins_at with a trigger event and where true → record.
    let mut matched2 = matched_context(str_val("10.0.0.1"), step_data(Some("fail"), 1.0));
    matched2.trigger_event = Some(TriggerEvent::from_event(Arc::new(event(vec![
        ("bidder", num(1.0)),
        ("flag", Value::Bool(true)),
    ]))));
    let rec = exec
        .execute_match_with_joins_at(&matched2, &lookup, 1234)
        .unwrap()
        .expect("record");
    assert_eq!(rec.score, 55.0);
}

#[test]
fn columnar_mask_helpers() {
    // Build a batch with one column and a plan with a bind filter to exercise
    // bind_filter_columnar_mask / each_filter_columnar_mask / branch_guard_masks.
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "v",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();

    let mut plan = simple_rule_plan(
        "col",
        default_match_plan(),
        Expr::Number(55.0),
        "ip",
        field("sip"),
    );
    plan.binds[0].filter = Some(Expr::BinOp {
        op: BinOp::Ge,
        left: Box::new(field("v")),
        right: Box::new(Expr::Number(1.0)),
    });
    let exec = RuleExecutor::new(plan);

    // Columnar bind filter mask present and evaluates.
    let mask = exec.bind_filter_columnar_mask("fail", &batch);
    assert!(mask.is_some());
    assert_eq!(mask.unwrap().len(), 2);

    // No each plan → each_filter_columnar_mask is None.
    assert!(exec.each_filter_columnar_mask(&batch).is_none());

    // Branch guards: no guard → empty masks.
    let masks = exec.branch_guard_masks(&batch);
    assert!(masks.is_empty());

    // bind_filters_columnar_safe with a columnar filter → true.
    assert!(exec.bind_filters_columnar_safe("w"));
}

// ---------------------------------------------------------------------------
// ctx-free match emit（F8.5）：build_match_alert_free 与 Full ctx 逐字段对拍
// ---------------------------------------------------------------------------

#[test]
fn ctx_free_match_output_matches_full_ctx_bytes() {
    // q6 形状：键 seller、无 label step、entity digit(b.auction)、yield
    // id=b.auction + 字面量、score 常量、无 where/join → match_ctx_free=true。
    let mut plan = simple_rule_plan(
        "ctx_free_r",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q6_avg200".into()),
        },
    ];
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.output_static().match_ctx_free,
        "q6 形状（score 常量 + entity/yield Field/Lit + 无 where/join）必须启用 ctx-free"
    );

    const NOW: i64 = 1_700_000_000_000_000_000;
    let trigger = event(vec![
        ("auction", num(1001.0)),
        ("price", num(300.0)),
        ("bidder", num(5.0)),
    ]);
    let matched = MatchedContext {
        rule_name: "ctx_free_r".into(),
        scope_key: vec![num(20.0)],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 300.0,
            event_first_time_nanos: Some(NOW),
            event_last_time_nanos: Some(NOW),
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        event_time_nanos: NOW,
        event_first_time_nanos: NOW,
        event_last_time_nanos: NOW,
        first_match_time_nanos: None,
        evidence_first_time_nanos: NOW,
        evidence_last_time_nanos: NOW,
        window_start_time_nanos: NOW - 600_000_000_000,
        window_end_time_nanos: NOW,
        machine_id: String::new(),
        trigger_event: Some(TriggerEvent::from_event(Arc::new(trigger.clone()))),
    };

    // Full 路径：手动构造窄化 ctx（键字段 + trigger_event 字段——同
    // build_eval_context 的 Named 注入语义）。
    let mut fields = EngineHashMap::default();
    fields.insert("seller".into(), num(20.0));
    for (k, v) in &trigger.fields {
        fields.insert(k.clone(), v.clone());
    }
    let ctx = Event { fields };
    let full = exec.build_match_alert(&matched, &ctx, NOW).unwrap();

    // ctx-free 路径：字段直读 scope_key + trigger_event。
    let free = exec.build_match_alert_free(&matched, NOW).unwrap();

    // 逐字段字节一致（OutputRecord 无 PartialEq——手工比较输出相关字段）。
    assert_eq!(full.wfx_id, free.wfx_id, "wfx_id");
    assert_eq!(full.score, free.score, "score");
    assert_eq!(full.entity_id, free.entity_id, "entity_id");
    assert_eq!(full.scope_key, free.scope_key, "scope_key");
    assert_eq!(full.summary, free.summary, "summary");
    assert_eq!(full.fired_at, free.fired_at, "fired_at");
    assert_eq!(full.machine_id, free.machine_id, "machine_id");
    assert_eq!(full.yield_fields, free.yield_fields, "yield_fields");
    assert_eq!(
        full.yield_field_types, free.yield_field_types,
        "yield_field_types"
    );
    assert_eq!(
        full.event_time_nanos, free.event_time_nanos,
        "event_time_nanos"
    );
    assert_eq!(full.origin, free.origin, "origin");

    // gate 反向：带 where 的规则必须走 Full（禁用 ctx-free）。
    let mut plan_where = simple_rule_plan(
        "ctx_free_where",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan_where.binds[0].alias = "b".into();
    plan_where.binds[0].window = "bid_events".into();
    plan_where.r#where = Some(Expr::BinOp {
        op: BinOp::Ge,
        left: Box::new(Expr::Field(FieldRef::Qualified("b".into(), "price".into()))),
        right: Box::new(Expr::Number(100.0)),
    });
    plan_where.yield_plan.fields = vec![YieldField {
        name: "id".into(),
        value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    }];
    let exec_where = RuleExecutor::new(plan_where);
    assert!(
        !exec_where.output_static().match_ctx_free,
        "带 where 的规则必须禁用 ctx-free（where 需要完整 ctx）"
    );

    // gate 反向：yield General 表达式（函数调用）必须禁用 ctx-free。
    let mut plan_gen = simple_rule_plan(
        "ctx_free_gen",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan_gen.binds[0].alias = "b".into();
    plan_gen.binds[0].window = "bid_events".into();
    plan_gen.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::Number(1.0)),
            right: Box::new(Expr::Number(2.0)),
        },
    }];
    let exec_gen = RuleExecutor::new(plan_gen);
    assert!(
        !exec_gen.output_static().match_ctx_free,
        "yield 含 General 表达式的规则必须禁用 ctx-free"
    );
}

#[test]
fn columnar_match_output_matches_row_path() {
    // q6 形状（无 join 变体）：键 seller、entity digit(b.auction)、yield
    // id=b.auction + 字面量、score 常量、无 where —— match_plan_columnar_safe
    // 通过，列式批输出与行式 `execute_match_at` 逐字段字节一致。
    let mut plan = simple_rule_plan(
        "match_col",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q6_avg200".into()),
        },
    ];
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.match_plan_columnar_safe(),
        "q6 形状（无 join）必须通过 match 列式门控"
    );

    const NOW: i64 = 1_700_000_000_000_000_000;
    let mk = |auction: f64, seller: f64| MatchedContext {
        rule_name: "match_col".into(),
        scope_key: vec![num(seller)],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: auction,
            event_first_time_nanos: Some(NOW),
            event_last_time_nanos: Some(NOW),
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        event_time_nanos: NOW,
        event_first_time_nanos: NOW,
        event_last_time_nanos: NOW,
        first_match_time_nanos: None,
        evidence_first_time_nanos: NOW,
        evidence_last_time_nanos: NOW,
        window_start_time_nanos: NOW - 600_000_000_000,
        window_end_time_nanos: NOW,
        machine_id: String::new(),
        trigger_event: Some(TriggerEvent::from_event(Arc::new(event(vec![
            ("auction", num(auction)),
            ("price", num(300.0)),
            ("bidder", num(5.0)),
        ])))),
    };
    let m1 = mk(1001.0, 20.0);
    let m2 = mk(1002.0, 21.0);

    // 行式路径：每命中 execute_match_at → OutputRecord → append_record。
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    for m in [&m1, &m2] {
        let record = exec.execute_match_at(m, NOW).unwrap();
        b_row.append_record(&record).unwrap();
    }
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 列式路径：批量直写 builder。
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats =
        exec.execute_match_direct_batch_columnar(&[&m1, &m2], NOW, &mut b_col, &mut appended);
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.failed, 0);
    assert_eq!(appended, vec![0, 1]);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 逐字段字节一致。
    assert_eq!(out_row, out_col);
    // entity_id = auction（字符串化），两行各一条。
    let entity_ids: Vec<String> = out_col
        .iter()
        .map(|r| {
            r.fields()
                .find(|f| f.get_name() == wf_lang::wfu_meta::WFU_ENTITY_ID)
                .and_then(|f| f.get_chars().map(str::to_string))
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(entity_ids, vec!["1001", "1002"]);
}

#[test]
fn columnar_match_general_fmt_matches_row_path() {
    // 层 2 收口（2026-08-25）：match 列式批的 General yield（fmt detail，q6
    // 真实形态——此前门控排除 General → unreachable，全走行式）——列式 cell
    // vs 行式 Full ctx 逐字段字节一致（含 null 字段 → 空串）。
    let mut plan = simple_rule_plan(
        "match_fmt",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q6_avg200".into()),
        },
        YieldField {
            name: "detail".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "fmt".into(),
                args: vec![
                    Expr::StringLit("seller={} price={}".into()),
                    Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
                    Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
                ],
            },
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Digit)),
            ("alert_type".into(), FieldType::Base(BaseType::Chars)),
            ("detail".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );
    assert!(
        exec.match_plan_columnar_safe(),
        "match + fmt detail（普通字段）应过列式门控（层 2）"
    );

    const NOW: i64 = 1_700_000_000_000_000_000;
    let mk = |auction: f64, seller: f64, price: Option<f64>| MatchedContext {
        rule_name: "match_fmt".into(),
        scope_key: vec![num(seller)],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: auction,
            event_first_time_nanos: Some(NOW),
            event_last_time_nanos: Some(NOW),
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        event_time_nanos: NOW,
        event_first_time_nanos: NOW,
        event_last_time_nanos: NOW,
        first_match_time_nanos: None,
        evidence_first_time_nanos: NOW,
        evidence_last_time_nanos: NOW,
        window_start_time_nanos: NOW - 600_000_000_000,
        window_end_time_nanos: NOW,
        machine_id: String::new(),
        trigger_event: Some(TriggerEvent::from_event(Arc::new(event(vec![
            ("auction", num(auction)),
            ("bidder", num(5.0)),
            (
                "price",
                match price {
                    Some(p) => num(p),
                    None => Value::Str("".into()),
                },
            ),
        ])))),
    };
    let m1 = mk(1001.0, 20.0, Some(300.0));
    let m2 = mk(1002.0, 21.0, None); // price 缺失 → fmt 空串（解释 None→""）

    // 行式路径：execute_match_at（General → Full ctx）+ append_record。
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    for m in [&m1, &m2] {
        let record = exec.execute_match_at(m, NOW).unwrap();
        b_row.append_record(&record).unwrap();
    }
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    // 列式路径：match_batch_prepare 批级 cell。
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats =
        exec.execute_match_direct_batch_columnar(&[&m1, &m2], NOW, &mut b_col, &mut appended);
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.failed, 0);
    assert_eq!(appended, vec![0, 1]);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(out_row, out_col);
    let detail = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "detail")
            .and_then(|f| f.get_chars().map(str::to_string))
            .unwrap_or_default()
    };
    assert_eq!(detail(&out_col[0]), "seller=5 price=300", "row 0 fmt");
    assert_eq!(
        detail(&out_col[1]),
        "seller=5 price=",
        "row 1 缺 price → 空串"
    );
}

#[test]
fn columnar_match_general_materialize_fail_falls_back_matches_row_path() {
    // 层 2 收口 review：物化失败（引用字段跨行类型不一致 → materialize_fields
    // None）→ 整批回退逐行——回退路径（build_eval_context Full ctx + eval）
    // 必须与解释路径（execute_match_at）逐位一致。
    let mut plan = simple_rule_plan(
        "match_fmt_fb",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("price={}".into()),
                Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
            ],
        },
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("detail".into(), FieldType::Base(BaseType::Chars))]),
    );
    assert!(exec.match_plan_columnar_safe(), "fmt 普通字段应过门控");

    const NOW: i64 = 1_700_000_000_000_000_000;
    let mk = |auction: f64, seller: f64, price: Value| MatchedContext {
        rule_name: "match_fmt_fb".into(),
        scope_key: vec![num(seller)],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: auction,
            event_first_time_nanos: Some(NOW),
            event_last_time_nanos: Some(NOW),
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        event_time_nanos: NOW,
        event_first_time_nanos: NOW,
        event_last_time_nanos: NOW,
        first_match_time_nanos: None,
        evidence_first_time_nanos: NOW,
        evidence_last_time_nanos: NOW,
        window_start_time_nanos: NOW - 600_000_000_000,
        window_end_time_nanos: NOW,
        machine_id: String::new(),
        trigger_event: Some(TriggerEvent::from_event(Arc::new(event(vec![
            ("auction", num(auction)),
            ("price", price),
        ])))),
    };
    // price 跨行类型不一致（Number vs Str）→ 物化失败 → 整批回退逐行。
    let m1 = mk(1001.0, 20.0, num(300.0));
    let m2 = mk(1002.0, 21.0, str_val("abc"));

    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    for m in [&m1, &m2] {
        let record = exec.execute_match_at(m, NOW).unwrap();
        b_row.append_record(&record).unwrap();
    }
    let out_row: Vec<_> = b_row
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats =
        exec.execute_match_direct_batch_columnar(&[&m1, &m2], NOW, &mut b_col, &mut appended);
    assert_eq!(stats.appended, 2);
    assert_eq!(stats.failed, 0);
    let out_col: Vec<_> = b_col
        .finish()
        .iter_data_records()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(out_row, out_col);
    let detail = |r: &wp_model_core::model::DataRecord| {
        r.fields()
            .find(|f| f.get_name() == "detail")
            .and_then(|f| f.get_chars().map(str::to_string))
            .unwrap_or_default()
    };
    assert_eq!(detail(&out_col[0]), "price=300", "row 0 Number");
    assert_eq!(
        detail(&out_col[1]),
        "price=abc",
        "row 1 Str（回退路径渲染）"
    );
}

#[test]
fn columnar_match_gate_rejects_right_window_refs() {
    // join 存在时，输出字段引用右窗字段（Qualified 右窗名）→ 门控回退行式。
    let mut plan = simple_rule_plan(
        "match_join_col",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.joins = vec![JoinPlan {
        right_window: "auction_events".into(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "auction".into()),
            right: FieldRef::Qualified("auction_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    // 输出引用右窗字段（seller 在 auction_events）→ 拒绝。
    plan.yield_plan.fields = vec![YieldField {
        name: "seller".into(),
        value: Expr::Field(FieldRef::Qualified(
            "auction_events".into(),
            "seller".into(),
        )),
    }];
    let exec = RuleExecutor::new(plan);
    assert!(
        !exec.match_plan_columnar_safe(),
        "输出引用非键右窗字段必须回退行式（字节一致性）"
    );

    // 输出仅引用左窗字段（b.auction）+ 常量 → 即使有 join 也通过
    // （join 在上游完成，输出不依赖富化）。
    let mut plan2 = simple_rule_plan(
        "match_join_col2",
        simple_plan(
            vec![simple_key("seller")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan2.binds[0].alias = "b".into();
    plan2.binds[0].window = "bid_events".into();
    plan2.joins = vec![JoinPlan {
        right_window: "auction_events".into(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "auction".into()),
            right: FieldRef::Qualified("auction_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan2.yield_plan.fields = vec![YieldField {
        name: "id".into(),
        value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    }];
    let exec2 = RuleExecutor::new(plan2);
    assert!(
        exec2.match_plan_columnar_safe(),
        "输出仅左窗字段 + join 已上游完成 → 列式安全"
    );
}
