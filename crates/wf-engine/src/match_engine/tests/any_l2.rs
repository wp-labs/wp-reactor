//! `on event any` (unordered co-occurrence) engine tests.

use std::time::Duration;
use wf_lang::ast::MatchMode;
use wf_lang::plan::{ExceedAction, LimitsPlan, MatchPlan, RateSpec};

use crate::match_engine::match_engine::{CepStateMachine, StepResult};

use super::helpers::*;

/// Two steps (a | count>=1, b | count>=1) in unordered (`any`) mode.
fn any_plan() -> MatchPlan {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("a", count_ge(1.0))]),
            step(vec![branch("b", count_ge(1.0))]),
        ],
    );
    plan.match_mode = MatchMode::Any;
    plan
}

#[test]
fn any_mode_order_agnostic_fires() {
    let plan = any_plan();
    let mut sm = CepStateMachine::new("any_order".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // b arrives BEFORE a — unordered mode must still fire
    assert_eq!(sm.advance_at("b", &e, 0), StepResult::Accumulate);
    let StepResult::Matched(ctx) = sm.advance_at("a", &e, 10) else {
        panic!("any mode should fire regardless of order");
    };
    assert_eq!(ctx.scope_key, vec![str_val("10.0.0.1")]);
}

#[test]
fn any_mode_missing_step_does_not_fire() {
    let plan = any_plan();
    let mut sm = CepStateMachine::new("any_miss".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("a", &e, 0), StepResult::Accumulate);
    assert_eq!(sm.advance_at("a", &e, 10), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 1); // alive, waiting for b
}

#[test]
fn any_mode_contrast_with_seq_order() {
    // Same steps, but `Seq` (ordered) mode: b before a must NOT fire.
    let mut plan = any_plan();
    plan.match_mode = MatchMode::Seq;
    let mut sm = CepStateMachine::new("seq_vs_any".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("b", &e, 0), StepResult::Accumulate);
    // a completes step 0 → Advance; step 1 (b) remains but the earlier b was
    // consumed at step 0 → no fire.
    assert_eq!(sm.advance_at("a", &e, 10), StepResult::Advance);
    assert_eq!(sm.instance_count(), 1);
}

#[test]
fn any_mode_throttle_fail_rule_trips_failed() {
    let plan = any_plan();
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(RateSpec {
            count: 1,
            per: Duration::from_secs(60),
        }),
        on_exceed: ExceedAction::FailRule,
        spill: None,
        max_disk_bytes: None,
    };
    let mut sm = CepStateMachine::with_limits("any_throttle".into(), plan, None, Some(limits));
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // First match fires (within the 1/s throttle window).
    assert_eq!(sm.advance_at("b", &e, 0), StepResult::Accumulate);
    assert!(matches!(sm.advance_at("a", &e, 1), StepResult::Matched(_)));
    // Second rapid match → throttle hit → FailRule trips `failed`.
    assert_eq!(sm.advance_at("b", &e, 2), StepResult::Accumulate);
    assert_eq!(sm.advance_at("a", &e, 3), StepResult::Accumulate);
    // Once failed, every future event is rejected — never a Matched again.
    assert_eq!(sm.advance_at("b", &e, 4), StepResult::Accumulate);
    assert_eq!(sm.advance_at("a", &e, 5), StepResult::Accumulate);
}

#[test]
fn any_mode_fire_and_reset_repeats() {
    let plan = any_plan();
    let mut sm = CepStateMachine::new("any_repeat".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // Chain 1: a then b → fires, resets.
    assert_eq!(sm.advance_at("a", &e, 0), StepResult::Accumulate);
    assert!(matches!(sm.advance_at("b", &e, 1), StepResult::Matched(_)));
    // Chain 2 (fresh): a then b again → fires again.
    assert_eq!(sm.advance_at("a", &e, 2), StepResult::Accumulate);
    assert!(matches!(sm.advance_at("b", &e, 3), StepResult::Matched(_)));
}
