use super::*;

#[test]
fn compile_seq_rule_emits_event_steps_and_seq_plan() {
    let src = r#"
rule rat_propagation {
    events {
        scan  : fw_events
        login : auth_events
        xfer  : fw_events
    }
    match<sip,dip:30m> {
        on event seq {
            has scan;
            has login within 10m;
            has xfer;
        }
    } -> score(95.0)
    entity(ip, scan.sip)
    yield out (x = scan.sip)
}
"#;
    let plans = compile_with(
        src,
        &[auth_events_window(), fw_events_window(), output_window()],
    );
    let plan = plans
        .iter()
        .find(|p| p.name == "rat_propagation")
        .expect("rule should compile");
    let chain = plan
        .match_plan
        .seq
        .as_ref()
        .expect("chain plan should exist");
    assert_eq!(chain.steps.len(), 3);
    assert!(!chain.consec, "default is gap");
    // event_steps: use-steps emitted for ordered progression
    assert_eq!(plan.match_plan.event_steps.len(), 3);
    assert_eq!(plan.match_plan.event_steps[0].branches[0].source, "scan");
    assert_eq!(plan.match_plan.event_steps[1].branches[0].source, "login");
    assert_eq!(plan.match_plan.event_steps[2].branches[0].source, "xfer");
    // within metadata preserved on chain plan
    assert_eq!(chain.steps[1].within.unwrap().as_secs(), 10 * 60);
    assert!(chain.steps[0].within.is_none());
    assert!(chain.steps[2].within.is_none());
    assert!(!chain.steps[1].neg);
}

#[test]
fn compile_seq_negation_excluded_from_event_steps() {
    let src = r#"
rule seq_neg {
    events {
        fail  : auth_events
        ok    : auth_events
    }
    match<sip:10m> {
        on event seq {
            has ok;
            not has fail within 5m;
        }
    } -> score(70.0)
    entity(ip, ok.sip)
    yield out (x = ok.sip)
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let plan = plans
        .iter()
        .find(|p| p.name == "seq_neg")
        .expect("rule should compile");
    let chain = plan
        .match_plan
        .seq
        .as_ref()
        .expect("chain plan should exist");
    assert_eq!(chain.steps.len(), 2);
    assert!(chain.steps[1].neg, "second step is a negation");
    // event_steps excludes negation steps
    assert_eq!(plan.match_plan.event_steps.len(), 1);
    assert_eq!(plan.match_plan.event_steps[0].branches[0].source, "ok");
    assert_eq!(chain.steps[1].within.unwrap().as_secs(), 5 * 60);
}

#[test]
fn seq_step_label_registers_stat() {
    let src = r#"
rule seq_stat {
    events {
        a : fw_events
        b : fw_events
    }
    match<sip:30m> {
        on event seq {
            spam: a | count >= 5;
            has b;
        }
    } -> score(80.0)
    entity(ip, a.sip)
    yield security_alerts (sip = a.sip, fail_count = stat.count(match_event(spam)), port_count = 1, message = "ok")
}
"#;
    // compile_with asserts parse + compile succeed; without the seq-label
    // stat registration this fails with an unknown-label error.
    compile_with(src, &[fw_events_window(), security_alerts_window()]);
}
