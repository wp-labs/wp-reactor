//! Round-2 coverage-fill tests for the `match_engine::match_engine` core.
//!
//! Targets the state-machine branches the earlier suites reach only
//! indirectly: OR/AND-mode short-circuits after an emit (`event_emitted` /
//! `event_ok`), `Any`-mode rate-limit re-fire (throttle × accu), the
//! no-close-path rate-limit `FailRule` latch, OR-mode throttled-emit
//! suppression, fixed-window `close()` oldest-bucket selection, the
//! conv-wrapped close/scan entry points, and the step-level value-based
//! `check_threshold` / no-tracked-fields collection lanes.
//!
//! Lives inside the module so it can reach the private submodules directly.
//! Only test code lives here — no production logic is modified.

use std::collections::HashSet;
use std::time::Duration;

use wf_lang::ast::{CloseMode, CmpOp, Expr, FieldRef, MatchMode, Measure};
use wf_lang::plan::{
    AggPlan, BranchPlan, ConvPlan, ExceedAction, LimitsPlan, MatchPlan, RateSpec, WindowSpec,
};

use super::state::AliasState;
use super::state::BranchState;
use super::step::{
    StepEvaluationInput, check_threshold, collect_alias_event, collect_event_fields,
    evaluate_step_with_progress, update_measure,
};
use super::types::{CloseOutput, CloseReason, Value};
use super::{CepStateMachine, EngineHashMap, Event, StepResult, StepState, close_is_qualified};

fn event(fields: Vec<(&str, Value)>) -> Event {
    Event {
        fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    }
}

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn str_val(s: &str) -> Value {
    Value::Str(s.into())
}

fn count_ge(n: f64) -> AggPlan {
    AggPlan {
        transforms: vec![],
        measure: Measure::Count,
        cmp: CmpOp::Ge,
        threshold: Expr::Number(n),
    }
}

fn simple_key(name: &str) -> FieldRef {
    FieldRef::Simple(name.to_string())
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

fn step(branches: Vec<BranchPlan>) -> wf_lang::plan::StepPlan {
    wf_lang::plan::StepPlan { branches }
}

fn simple_plan(keys: Vec<FieldRef>, steps: Vec<wf_lang::plan::StepPlan>) -> MatchPlan {
    MatchPlan {
        keys,
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps: steps,
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
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
    event_steps: Vec<wf_lang::plan::StepPlan>,
    close_steps: Vec<wf_lang::plan::StepPlan>,
) -> MatchPlan {
    let mut plan = simple_plan(keys, event_steps);
    plan.window_spec = WindowSpec::Sliding(Duration::from_secs(60));
    plan.close_steps = close_steps;
    plan.close_mode = CloseMode::And;
    plan
}

fn throttle_limits(count: u64, on_exceed: ExceedAction) -> LimitsPlan {
    LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(RateSpec {
            count,
            per: Duration::from_secs(60),
        }),
        on_exceed,
        spill: None,
        max_disk_bytes: None,
    }
}

// ===========================================================================
// mod.rs — OR-mode `event_emitted` short-circuit after an event-path emit
// ===========================================================================

#[test]
fn or_mode_with_close_event_emitted_short_circuits_later_events() {
    let mut plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
    );
    plan.close_mode = CloseMode::Or;
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // First event completes the chain → OR mode emits immediately and keeps
    // the instance alive for the close path.
    assert!(matches!(
        sm.advance_at("req", &e, 0),
        StepResult::Matched(_)
    ));
    // Later events hit the `event_emitted` guard → plain accumulate.
    assert_eq!(sm.advance_at("req", &e, 1_000), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 1);
    // A close-source event still accumulates the close step for the close path.
    sm.advance_at("c", &e, 2_000);

    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(
        out.event_emitted,
        "OR-mode event-path emit marks the instance"
    );
    assert!(out.close_ok);
    assert!(close_is_qualified(&out), "OR mode qualifies on close data");
}

// ===========================================================================
// mod.rs — AND-mode `event_ok` short-circuit after a full match
// ===========================================================================

#[test]
fn and_mode_with_close_event_ok_short_circuits_later_events() {
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // First event completes the event chain → AND mode marks event_ok and
    // returns Advance (still accumulating close steps).
    assert_eq!(sm.advance_at("req", &e, 0), StepResult::Advance);
    // Later events hit the `event_ok` guard → plain accumulate.
    assert_eq!(sm.advance_at("req", &e, 1_000), StepResult::Accumulate);
    // A close-source event still accumulates the close step.
    sm.advance_at("c", &e, 2_000);

    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out.event_ok);
    assert!(out.close_ok);
    assert!(close_is_qualified(&out));
}

// ===========================================================================
// mod.rs — `Any`-mode re-fire under a rate limit
// ===========================================================================

#[test]
fn any_mode_throttle_accu_keeps_running_count() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    plan.match_mode = MatchMode::Any;
    plan.accu = true;
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        Some(throttle_limits(1, ExceedAction::Throttle)),
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // First fire within the window consumes the single-slot budget.
    let StepResult::Matched(first) = sm.advance_at("e", &e, 0) else {
        panic!("first Any-mode fire");
    };
    assert_eq!(first.step_data[0].measure_value, 1.0);
    // Second event within the same window: throttled, but `<accu>` rearm keeps
    // the running accumulation (no reset).
    assert_eq!(sm.advance_at("e", &e, 1_000), StepResult::Accumulate);
    // After the window rotates the fire carries the cumulative count (3).
    let StepResult::Matched(third) = sm.advance_at("e", &e, 61_000_000_000) else {
        panic!("Any-mode fire after window rotation");
    };
    assert_eq!(third.step_data[0].measure_value, 3.0);
}

#[test]
fn any_mode_throttle_non_accu_resets_and_survives() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    plan.match_mode = MatchMode::Any;
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        Some(throttle_limits(1, ExceedAction::Throttle)),
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    assert!(matches!(sm.advance_at("e", &e, 0), StepResult::Matched(_)));
    // Throttled re-fire in non-accu mode resets the instance and suppresses
    // the alert, but the instance stays alive for a fresh chain.
    assert_eq!(sm.advance_at("e", &e, 1_000), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 1);
}

// ===========================================================================
// mod.rs — no-close-path rate-limit `FailRule` latch (event emit path)
// ===========================================================================

#[test]
fn seq_no_close_throttle_fail_rule_latches() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        Some(throttle_limits(1, ExceedAction::FailRule)),
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    assert!(matches!(sm.advance_at("e", &e, 0), StepResult::Matched(_)));
    // Second fire within the same window hits the throttle → FailRule latches.
    assert_eq!(sm.advance_at("e", &e, 1_000), StepResult::Accumulate);
    // The latch rejects everything after, even past the window.
    assert_eq!(
        sm.advance_at("e", &e, 61_000_000_000),
        StepResult::Accumulate
    );
}

// ===========================================================================
// mod.rs — OR-mode throttled emit: suppressed but still marks `event_emitted`
// ===========================================================================

#[test]
fn or_mode_with_close_throttle_suppressed_emit_marks_event_emitted() {
    let mut plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(2.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
    );
    plan.close_mode = CloseMode::Or;
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        Some(throttle_limits(1, ExceedAction::Throttle)),
    );

    // key2 consumes the single-slot throttle budget (fires on its 2nd event).
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    sm.advance_at("req", &e2, 0);
    assert!(matches!(
        sm.advance_at("req", &e2, 1_000),
        StepResult::Matched(_)
    ));

    // key1 completes its chain within the same window → the emit is throttled
    // and suppressed, but the OR-mode path still marks the event as emitted so
    // the close output reflects it.
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("req", &e1, 2_000);
    assert_eq!(sm.advance_at("req", &e1, 3_000), StepResult::Accumulate);
    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(
        out.event_emitted,
        "suppressed OR-mode emit still marks the event emitted"
    );
}

#[test]
fn or_mode_with_close_throttle_fail_rule_latches() {
    let mut plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(2.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
    );
    plan.close_mode = CloseMode::Or;
    let mut sm = CepStateMachine::with_limits(
        "r".into(),
        plan,
        None,
        Some(throttle_limits(1, ExceedAction::FailRule)),
    );
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    sm.advance_at("req", &e2, 0);
    assert!(matches!(
        sm.advance_at("req", &e2, 1_000),
        StepResult::Matched(_)
    ));

    // key1's fire is throttled → FailRule latches the whole rule.
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("req", &e1, 2_000);
    assert_eq!(sm.advance_at("req", &e1, 3_000), StepResult::Accumulate);
    // The latch rejects all future events.
    assert_eq!(
        sm.advance_at("req", &e1, 61_000_000_000),
        StepResult::Accumulate
    );
}

// ===========================================================================
// mod.rs — fixed-window `close()` picks the oldest bucket instance
// ===========================================================================

#[test]
fn fixed_window_close_picks_oldest_bucket() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    plan.window_spec = WindowSpec::Fixed(Duration::from_secs(600));
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    sm.advance_at("e", &e, 100); // bucket 0 (t < 600s)
    sm.advance_at("e", &e, 601_000_000_000); // bucket 1
    assert_eq!(sm.instance_count(), 2, "two fixed buckets for the same key");

    let first = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Flush)
        .unwrap();
    assert_eq!(
        first.window_start_time_nanos, 0,
        "oldest bucket closed first"
    );
    assert_eq!(sm.instance_count(), 1);

    let second = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Flush)
        .unwrap();
    assert_eq!(
        second.window_start_time_nanos, 600_000_000_000,
        "remaining bucket closed next"
    );
    assert_eq!(sm.instance_count(), 0);
}

// ===========================================================================
// mod.rs — conv-wrapped close / scan entry points
// ===========================================================================

#[test]
fn conv_wrapped_close_and_scan_variants() {
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
    );
    let conv = ConvPlan { chains: vec![] };
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // close_all_with_conv: qualifying outputs pass through conv (empty chain =
    // identity).
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 1_000);
    let outs = sm.close_all_with_conv(CloseReason::Eos, Some(&conv));
    assert_eq!(outs.len(), 1);
    assert!(close_is_qualified(&outs[0]));
    assert_eq!(sm.instance_count(), 0);

    // close_all (no conv).
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 1_000);
    let outs = sm.close_all(CloseReason::Eos);
    assert_eq!(outs.len(), 1);
    assert_eq!(sm.instance_count(), 0);

    // scan_expired_at_with_conv.
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 1_000);
    let outs = sm.scan_expired_at_with_conv(61_000_000_000, Some(&conv));
    assert_eq!(outs.len(), 1);
    assert_eq!(sm.instance_count(), 0);

    // scan_expired_at_with_conv_skip_non_alerting.
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 1_000);
    let outs = sm.scan_expired_at_with_conv_skip_non_alerting(61_000_000_000, Some(&conv));
    assert_eq!(outs.len(), 1);

    // unbounded skip variant.
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 1_000);
    let outs =
        sm.scan_expired_at_with_conv_skip_non_alerting_unbounded(61_000_000_000, Some(&conv));
    assert_eq!(outs.len(), 1);

    // conv = None → outputs pass through untouched.
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 1_000);
    let outs = sm.scan_expired_at_with_conv(61_000_000_000, None);
    assert_eq!(outs.len(), 1);
}

// ===========================================================================
// step.rs — value-based threshold paths (min/max on non-numeric fields)
// ===========================================================================

#[test]
fn check_threshold_value_path_min_max_and_non_constant() {
    let min_eq_a = AggPlan {
        transforms: vec![],
        measure: Measure::Min,
        cmp: CmpOp::Eq,
        threshold: Expr::StringLit("a".into()),
    };
    let min_eq_b = AggPlan {
        transforms: vec![],
        measure: Measure::Min,
        cmp: CmpOp::Eq,
        threshold: Expr::StringLit("b".into()),
    };
    let min_cross_type = AggPlan {
        transforms: vec![],
        measure: Measure::Min,
        cmp: CmpOp::Eq,
        threshold: Expr::Number(1.0),
    };

    // Min on a string field: the numeric accumulator stays INF, so the check
    // falls back to the value-based path.
    let mut bs = BranchState::new();
    update_measure(&Measure::Min, &Some(str_val("a")), &mut bs);
    assert!(check_threshold(&min_eq_a, &bs), "value equality satisfied");
    assert!(!check_threshold(&min_eq_b, &bs), "different string fails");
    assert!(
        !check_threshold(&min_cross_type, &bs),
        "cross-type comparison is rejected"
    );

    // Max on a string field, Ge comparison against the same string.
    let max_ge_z = AggPlan {
        transforms: vec![],
        measure: Measure::Max,
        cmp: CmpOp::Ge,
        threshold: Expr::StringLit("z".into()),
    };
    let mut bs2 = BranchState::new();
    update_measure(&Measure::Max, &Some(str_val("z")), &mut bs2);
    assert!(check_threshold(&max_ge_z, &bs2));

    // Count with a non-constant threshold (field ref): cannot evaluate the
    // threshold → unsatisfied rather than comparing against 0.
    let count_field_threshold = AggPlan {
        transforms: vec![],
        measure: Measure::Count,
        cmp: CmpOp::Ge,
        threshold: Expr::Field(FieldRef::Simple("sip".into())),
    };
    let mut bs3 = BranchState::new();
    update_measure(&Measure::Count, &None, &mut bs3);
    assert!(!check_threshold(&count_field_threshold, &bs3));

    // Min with a numeric threshold but no field values: numeric path is INF,
    // value path has no min_val → false.
    let min_le_5 = AggPlan {
        transforms: vec![],
        measure: Measure::Min,
        cmp: CmpOp::Le,
        threshold: Expr::Number(5.0),
    };
    let bs4 = BranchState::new();
    assert!(!check_threshold(&min_le_5, &bs4));
}

// ===========================================================================
// step.rs — no-tracked-fields collection lanes
// ===========================================================================

#[test]
fn collect_event_fields_none_tracked_lane_collects_all_non_null_fields() {
    let e = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("n", num(5.0)),
        ("flag", Value::Bool(true)),
    ]);
    let mut bs = BranchState::new();
    collect_event_fields(&e, &mut bs, None, &HashSet::new(), None);
    let fv = bs.field_values.as_deref().expect("field history allocated");
    assert_eq!(fv.len(), 3, "every non-null field is collected");
    assert_eq!(
        fv["sip"].iter().cloned().collect::<Vec<_>>(),
        vec![str_val("10.0.0.1")]
    );
    assert_eq!(fv["n"].iter().cloned().collect::<Vec<_>>(), vec![num(5.0)]);
    assert_eq!(
        fv["flag"].iter().cloned().collect::<Vec<_>>(),
        vec![Value::Bool(true)]
    );
}

#[test]
fn collect_alias_event_none_tracked_counts_and_collects_all_fields() {
    let e = event(vec![("sip", str_val("10.0.0.1")), ("n", num(7.0))]);
    let mut alias_state = AliasState::new();
    collect_alias_event(&e, &mut alias_state, None);
    assert_eq!(alias_state.count, 1);
    let fv = alias_state.field_values.as_deref().expect("allocated");
    assert_eq!(fv.len(), 2);
}

// ===========================================================================
// step.rs — evaluate_step_with_progress skips other-alias branches
// ===========================================================================

#[test]
fn evaluate_step_skips_branches_from_other_aliases() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut step_state = StepState::new(1);
    let mut baselines = EngineHashMap::default();
    let (satisfied, progress) = evaluate_step_with_progress(
        StepEvaluationInput {
            alias: "other",
            event: &e,
            event_time_nanos: 0,
            windows: None,
            progress: None,
            step_index: 0,
            row: 0,
            masks: None,
            collect_step_values: false,
        },
        &plan.event_steps[0],
        &mut step_state,
        &mut baselines,
    );
    assert_eq!(satisfied, None, "no branch matches the alias");
    assert_eq!(progress, None);
    assert_eq!(step_state.branch_states[0].count, 0, "nothing accumulated");
}

// ===========================================================================
// types.rs — CloseOutput helper assertions used across the machine
// ===========================================================================

#[test]
fn close_output_field_defaults_via_state_machine_roundtrip() {
    // A never-matched AND-mode instance expires to a CloseOutput with empty
    // step data and a non-qualifying flag — guards the close-time defaults.
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(5.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("req", &e, 0); // 1 of 5 required events
    let out: CloseOutput = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(!out.event_ok);
    assert!(!out.close_ok, "close threshold unmet");
    assert!(!close_is_qualified(&out));
    assert_eq!(out.event_step_data.len(), 0);
    assert_eq!(out.close_step_data.len(), 1);
    assert_eq!(out.scope_key, vec![str_val("10.0.0.1")]);
}
