//! Chain L2 semantics: within / not / consec.

use std::time::Duration;

use wf_lang::ast::MatchMode;
use wf_lang::plan::WindowSpec;
use wf_lang::plan::{MatchPlan, SeqPlan, SeqSkipPlan, SeqStepPlan};

use crate::match_engine::cep::{CepStateMachine, StepResult};

use super::helpers::*;

/// Two use-steps (scan → login); the login step gets `within`.
fn chain_plan(login_within: Option<Duration>) -> MatchPlan {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("scan", count_ge(1.0))]),
            step(vec![branch("login", count_ge(1.0))]),
        ],
    );
    plan.seq = Some(SeqPlan {
        consec: false,
        skip: SeqSkipPlan::PastLast,
        steps: vec![
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("scan", count_ge(1.0)),
            },
            SeqStepPlan {
                neg: false,
                within: login_within,
                branch: branch("login", count_ge(1.0)),
            },
        ],
    });
    plan
}

#[test]
fn within_satisfied_fires() {
    let plan = chain_plan(Some(Duration::from_secs(600))); // login within 10m
    let mut sm = CepStateMachine::new("within_ok".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("scan", &e, 0), StepResult::Advance);
    // login at T0 + 5m → within 10m → fires
    let StepResult::Matched(ctx) = sm.advance_at("login", &e, 300_000_000_000) else {
        panic!("login within 10m should fire");
    };
    assert_eq!(ctx.scope_key, vec![str_val("10.0.0.1")]);
}

#[test]
fn within_violated_suppresses() {
    let plan = chain_plan(Some(Duration::from_secs(600))); // login within 10m
    let mut sm = CepStateMachine::new("within_no".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("scan", &e, 0), StepResult::Advance);
    // login at T0 + 11m → gap exceeds within → suppressed
    assert_eq!(
        sm.advance_at("login", &e, 660_000_000_000),
        StepResult::Accumulate
    );
    assert_eq!(sm.instance_count(), 1); // reset, alive for a fresh chain
}

/// scan (use) → fail (neg, within 5m) → login (use).
fn neg_plan() -> MatchPlan {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("scan", count_ge(1.0))]),
            step(vec![branch("login", count_ge(1.0))]),
        ],
    );
    plan.seq = Some(SeqPlan {
        consec: false,
        skip: SeqSkipPlan::PastLast,
        steps: vec![
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("scan", count_ge(1.0)),
            },
            SeqStepPlan {
                neg: true,
                within: Some(Duration::from_secs(300)),
                branch: branch("fail", count_ge(1.0)),
            },
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("login", count_ge(1.0)),
            },
        ],
    });
    plan
}

#[test]
fn negation_violated_suppresses() {
    let plan = neg_plan();
    let mut sm = CepStateMachine::new("neg_no".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("scan", &e, 0), StepResult::Advance);
    // fail within 5m of scan → violation
    assert_eq!(
        sm.advance_at("fail", &e, 60_000_000_000),
        StepResult::Accumulate
    );
    // login completes the chain → suppressed by negation
    assert_eq!(
        sm.advance_at("login", &e, 120_000_000_000),
        StepResult::Accumulate
    );
    assert_eq!(sm.instance_count(), 1);
}

#[test]
fn negation_clean_fires() {
    let plan = neg_plan();
    let mut sm = CepStateMachine::new("neg_ok".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("scan", &e, 0), StepResult::Advance);
    // no fail → clean chain fires
    let StepResult::Matched(ctx) = sm.advance_at("login", &e, 120_000_000_000) else {
        panic!("clean chain should fire");
    };
    assert_eq!(ctx.scope_key, vec![str_val("10.0.0.1")]);
}

/// consec: scan → login, strict adjacency.
fn consec_plan() -> MatchPlan {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("scan", count_ge(1.0))]),
            step(vec![branch("login", count_ge(1.0))]),
        ],
    );
    plan.seq = Some(SeqPlan {
        consec: true,
        skip: SeqSkipPlan::PastLast,
        steps: vec![
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("scan", count_ge(1.0)),
            },
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("login", count_ge(1.0)),
            },
        ],
    });
    plan
}

#[test]
fn consec_unrelated_event_breaks_chain() {
    let plan = consec_plan();
    let mut sm = CepStateMachine::new("consec_no".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("scan", &e, 0), StepResult::Advance);
    // unrelated event breaks the chain in consec mode
    assert_eq!(sm.advance_at("other", &e, 10), StepResult::Accumulate);
    // login after the break: instance was reset, step 0 is scan → no fire
    assert_eq!(sm.advance_at("login", &e, 20), StepResult::Accumulate);
}

#[test]
fn consec_adjacent_events_fire() {
    let plan = consec_plan();
    let mut sm = CepStateMachine::new("consec_ok".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("scan", &e, 0), StepResult::Advance);
    let StepResult::Matched(ctx) = sm.advance_at("login", &e, 10) else {
        panic!("adjacent scan+login should fire in consec mode");
    };
    assert_eq!(ctx.scope_key, vec![str_val("10.0.0.1")]);
}

#[test]
fn negation_before_prev_step_does_not_violate() {
    let plan = neg_plan();
    let mut sm = CepStateMachine::new("neg_before".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // fail BEFORE scan (the preceding use-step) → negation window inactive → no violation
    assert_eq!(sm.advance_at("fail", &e, 0), StepResult::Accumulate);
    assert_eq!(sm.advance_at("scan", &e, 10), StepResult::Advance);
    // login completes → chain fires (the early fail was not in the neg window)
    let StepResult::Matched(_) = sm.advance_at("login", &e, 20) else {
        panic!("fail before the preceding step should not suppress the chain");
    };
}

/// consec + `not`-first: the negation violation must survive the adjacency break.
fn consec_not_first_plan() -> MatchPlan {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("b", count_ge(1.0))])],
    );
    plan.seq = Some(SeqPlan {
        consec: true,
        skip: SeqSkipPlan::PastLast,
        steps: vec![
            SeqStepPlan {
                neg: true,
                within: Some(Duration::from_secs(300)),
                branch: branch("a", count_ge(1.0)),
            },
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("b", count_ge(1.0)),
            },
        ],
    });
    plan
}

#[test]
fn consec_does_not_bypass_negation() {
    let plan = consec_not_first_plan();
    let mut sm = CepStateMachine::new("consec_neg".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // `a` violates the first-step negation AND breaks adjacency in consec mode.
    assert_eq!(sm.advance_at("a", &e, 0), StepResult::Accumulate);
    // `b` alone must NOT fire: the negation violation must survive the consec break.
    assert_eq!(sm.advance_at("b", &e, 10), StepResult::Accumulate);
}

#[test]
fn within_out_of_order_completion_suppresses() {
    let plan = chain_plan(Some(Duration::from_secs(600))); // login within 10m
    let mut sm = CepStateMachine::new("within_ooo".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // scan completes at t=100.
    assert_eq!(sm.advance_at("scan", &e, 100), StepResult::Advance);
    // login completes at t=50 (out-of-order, before scan) → negative gap → within violated.
    assert_eq!(sm.advance_at("login", &e, 50), StepResult::Accumulate);
}

/// seq: a(use) → c(neg within 5m) → b(use within 10m). A negation violation must
/// survive a later `within` reset so the chain cannot re-fire clean.
fn within_neg_plan() -> MatchPlan {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("a", count_ge(1.0))]),
            step(vec![branch("b", count_ge(1.0))]),
        ],
    );
    plan.window_spec = WindowSpec::Sliding(Duration::from_secs(3600));
    plan.match_mode = MatchMode::Seq;
    plan.seq = Some(SeqPlan {
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
                within: Some(Duration::from_secs(300)),
                branch: branch("c", count_ge(1.0)),
            },
            SeqStepPlan {
                neg: false,
                within: Some(Duration::from_secs(600)),
                branch: branch("b", count_ge(1.0)),
            },
        ],
    });
    plan
}

#[test]
fn within_reset_preserves_negation() {
    let plan = within_neg_plan();
    let mut sm = CepStateMachine::new("within_neg".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    let s = 1_000_000_000i64; // 1 second in nanos
    // a completes step 0 at t=0.
    assert_eq!(sm.advance_at("a", &e, 0), StepResult::Advance);
    // c@60s → negation violation within [a.last=0, 0+300s].
    assert_eq!(sm.advance_at("c", &e, 60 * s), StepResult::Accumulate);
    // b@700s → within(600s) violated (700s > 600s) → reset, negation preserved.
    assert_eq!(sm.advance_at("b", &e, 700 * s), StepResult::Accumulate);
    // Fresh chain: a then b → must NOT fire (negation preserved across the reset).
    assert_eq!(sm.advance_at("a", &e, 800 * s), StepResult::Advance);
    assert_eq!(sm.advance_at("b", &e, 800 * s + 1), StepResult::Accumulate);
}
