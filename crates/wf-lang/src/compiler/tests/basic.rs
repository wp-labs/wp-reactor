use super::*;

// =========================================================================
// 1. compile_brute_force
// =========================================================================

#[test]
fn compile_brute_force() {
    let schemas = [auth_events_window(), security_alerts_window()];
    let plans = compile_with(
        r#"
rule brute_force {
    events {
        fail : auth_events && action == "failed"
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
        }
    } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} failed", fail.sip)
    )
}
"#,
        &schemas,
    );
    assert_eq!(plans.len(), 1);
    let p = &plans[0];

    // name
    assert_eq!(p.name, "brute_force");

    // 1 bind
    assert_eq!(p.binds.len(), 1);
    assert_eq!(p.binds[0].alias, "fail");
    assert_eq!(p.binds[0].window, "auth_events");
    assert!(p.binds[0].filter.is_some());

    // match: 1 key, Sliding(300s), 1 event step, no close
    assert_eq!(p.match_plan.keys, vec![FieldRef::Simple("sip".into())]);
    assert_eq!(
        p.match_plan.window_spec,
        WindowSpec::Sliding(Duration::from_secs(300))
    );
    assert_eq!(p.match_plan.event_steps.len(), 1);
    assert!(p.match_plan.close_steps.is_empty());

    // event step: 1 branch
    let branch = &p.match_plan.event_steps[0].branches[0];
    assert_eq!(branch.source, "fail");
    assert!(branch.field.is_none());
    assert_eq!(branch.agg.measure, Measure::Count);
    assert_eq!(branch.agg.cmp, CmpOp::Ge);
    assert_eq!(branch.agg.threshold, Expr::Number(3.0));

    // entity
    assert_eq!(p.entity_plan.entity_type, "ip");
    assert_eq!(
        p.entity_plan.entity_id_expr,
        Expr::Field(FieldRef::Qualified("fail".into(), "sip".into()))
    );

    // score
    assert_eq!(p.score_plan.expr, Expr::Number(70.0));

    // yield: 3 fields
    assert_eq!(p.yield_plan.target, "security_alerts");
    assert_eq!(p.yield_plan.fields.len(), 3);
    assert_eq!(p.yield_plan.fields[0].name, "sip");
    assert_eq!(p.yield_plan.fields[1].name, "fail_count");
    assert_eq!(p.yield_plan.fields[2].name, "message");

    // L1 empties
    assert!(p.joins.is_empty());
    assert!(p.conv_plan.is_none());
}

// =========================================================================
// 2. compile_multi_source_multi_step
// =========================================================================

#[test]
fn compile_multi_source_multi_step() {
    let schemas = [auth_events_window(), fw_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule multi {
    events {
        fail : auth_events && action == "failed"
        scan : fw_events
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
            scan.dport | distinct | count > 10;
        }
    } -> score(80.0)
    entity(ip, fail.sip)
    yield out (x = fail.sip)
}
"#,
        &schemas,
    );
    let p = &plans[0];

    // 2 binds
    assert_eq!(p.binds.len(), 2);
    assert_eq!(p.binds[0].alias, "fail");
    assert_eq!(p.binds[1].alias, "scan");

    // 2 event steps
    assert_eq!(p.match_plan.event_steps.len(), 2);

    // step[1]: field = Dot("dport"), transforms = [Distinct]
    let step1 = &p.match_plan.event_steps[1].branches[0];
    assert_eq!(step1.field, Some(FieldSelector::Dot("dport".into())));
    assert_eq!(step1.agg.transforms, vec![Transform::Distinct]);
    assert_eq!(step1.agg.measure, Measure::Count);
    assert_eq!(step1.agg.cmp, CmpOp::Gt);
}

// =========================================================================
// 3. compile_on_close
// =========================================================================

#[test]
fn compile_on_close() {
    let schemas = [dns_query_window(), dns_response_window(), output_window()];
    let plans = compile_with(
        r#"
rule dns_timeout {
    events {
        req : dns_query
        resp : dns_response
    }
    match<query_id:30s> {
        on event {
            req | count >= 1;
        }
        on close {
            resp && close_reason == "timeout" | count == 0;
        }
    } -> score(50.0)
    entity(ip, req.sip)
    yield out (x = req.sip)
}
"#,
        &schemas,
    );
    let p = &plans[0];

    assert_eq!(p.match_plan.event_steps.len(), 1);
    assert_eq!(p.match_plan.close_steps.len(), 1);

    let close_branch = &p.match_plan.close_steps[0].branches[0];
    assert_eq!(close_branch.source, "resp");
    assert!(close_branch.guard.is_some());
    assert_eq!(close_branch.agg.measure, Measure::Count);
    assert_eq!(close_branch.agg.cmp, CmpOp::Eq);
    assert_eq!(close_branch.agg.threshold, Expr::Number(0.0));
}

// =========================================================================
// 4. compile_or_branches
// =========================================================================

#[test]
fn compile_or_branches() {
    let schemas = [generic_window(), generic_window2(), output_window()];
    let plans = compile_with(
        r#"
rule or_rule {
    events { a : win  b : win2 }
    match<sip:5m> {
        on event {
            a | count >= 3 || b | count >= 5;
        }
    } -> score(60.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#,
        &schemas,
    );
    let step = &plans[0].match_plan.event_steps[0];
    assert_eq!(step.branches.len(), 2);
    assert_eq!(step.branches[0].source, "a");
    assert_eq!(step.branches[1].source, "b");
}
