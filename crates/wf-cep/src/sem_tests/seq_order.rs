//! Verify the existing `on event` engine's ordering semantics:
//! does step i+1 only evaluate after step i completes?

use crate::cep::{CepStateMachine, StepResult};

use super::helpers::*;

/// Two sequential steps: scan (count>=1) then login (count>=1).
fn two_step_plan() -> Vec<wf_lang::plan::StepPlan> {
    vec![
        step(vec![branch("scan", count_ge(1.0))]),
        step(vec![branch("login", count_ge(1.0))]),
    ]
}

#[test]
fn existing_engine_is_sequential() {
    let plan = simple_plan(vec![simple_key("sip")], two_step_plan());
    let mut sm = CepStateMachine::new("seq_check".to_string(), plan, None);

    let login = event(vec![("sip", str_val("10.0.0.1"))]);
    let scan = event(vec![("sip", str_val("10.0.0.1"))]);

    // login BEFORE scan: step 0 is scan, so login must NOT progress it.
    assert_eq!(sm.advance("login", &login), StepResult::Accumulate);
    // scan completes step 0 → returns Advance (step 1 still pending).
    assert_eq!(sm.advance("scan", &scan), StepResult::Advance);
    // The earlier login was consumed against step 0 and is NOT retained for
    // step 1. A fresh login AFTER scan must fire.
    if let StepResult::Matched(ctx) = sm.advance("login", &login) {
        assert_eq!(ctx.scope_key, vec![str_val("10.0.0.1")]);
    } else {
        panic!("login after scan should fire");
    }
}

#[test]
fn scan_then_login_fires() {
    let plan = simple_plan(vec![simple_key("sip")], two_step_plan());
    let mut sm = CepStateMachine::new("seq_check2".to_string(), plan, None);

    let scan = event(vec![("sip", str_val("10.0.0.1"))]);
    let login = event(vec![("sip", str_val("10.0.0.1"))]);

    // scan completes step 0 → Advance.
    assert_eq!(sm.advance("scan", &scan), StepResult::Advance);
    // login completes step 1 → Matched.
    if let StepResult::Matched(ctx) = sm.advance("login", &login) {
        assert_eq!(ctx.scope_key, vec![str_val("10.0.0.1")]);
    } else {
        panic!("scan then login should have fired");
    }
}
