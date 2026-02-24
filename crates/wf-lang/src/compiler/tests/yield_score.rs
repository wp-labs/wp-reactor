use super::*;

// =========================================================================
// 8. compile_multiple_rules
// =========================================================================

#[test]
fn compile_multiple_rules() {
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule r1 {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}

rule r2 {
    events { e : win }
    match<:1h> { on event { e.dport | sum >= 100; } } -> score(30.0)
    entity(host, e.host)
    yield out (y = e.host)
}
"#,
        &schemas,
    );
    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].name, "r1");
    assert_eq!(plans[1].name, "r2");
}

// =========================================================================
// 9. compile_yield_fields
// =========================================================================

#[test]
fn compile_yield_fields() {
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { fail : auth_events }
    match<sip:5m> { on event { fail | count >= 3; } } -> score(70.0)
    entity(ip, fail.sip)
    yield out (
        x = fail.sip,
        n = count(fail)
    )
}
"#,
        &schemas,
    );
    let yp = &plans[0].yield_plan;
    assert_eq!(yp.target, "out");
    assert_eq!(yp.fields.len(), 2);

    assert_eq!(yp.fields[0].name, "x");
    assert_eq!(
        yp.fields[0].value,
        Expr::Field(FieldRef::Qualified("fail".into(), "sip".into()))
    );

    assert_eq!(yp.fields[1].name, "n");
    assert!(matches!(
        &yp.fields[1].value,
        Expr::FuncCall { name, args, .. } if name == "count" && args.len() == 1
    ));
}

// =========================================================================
// 10. compile_score_arithmetic
// =========================================================================

#[test]
fn compile_score_arithmetic() {
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0 + 20.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert!(matches!(
        &plans[0].score_plan.expr,
        Expr::BinOp { op: BinOp::Add, .. }
    ));
}

// =========================================================================
// 11. compile_bracket_field_ref
// =========================================================================

#[test]
fn compile_bracket_field_ref() {
    // The bracket key e["detail.sha256"] is not type-checked for L1 (checker
    // skips bracket-ref field existence), so generic_window suffices.
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { e : win }
    match<e["detail.sha256"]:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    let keys = &plans[0].match_plan.keys;
    assert_eq!(keys.len(), 1);
    assert_eq!(
        keys[0],
        FieldRef::Bracketed("e".into(), "detail.sha256".into())
    );
}

// =========================================================================
// 12. compile_event_filter
// =========================================================================

#[test]
fn compile_event_filter() {
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { e : win && action == "failed" }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    let bind = &plans[0].binds[0];
    assert!(bind.filter.is_some());
    assert!(matches!(
        bind.filter.as_ref().unwrap(),
        Expr::BinOp { op: BinOp::Eq, .. }
    ));
}

// =========================================================================
// 13. compile_labeled_branch
// =========================================================================

#[test]
fn compile_labeled_branch() {
    let schemas = [generic_window(), generic_window2(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { a : win  b : win2 }
    match<sip:5m> {
        on event {
            lbl: a | count >= 3;
        }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#,
        &schemas,
    );
    let branch = &plans[0].match_plan.event_steps[0].branches[0];
    assert_eq!(branch.label, Some("lbl".into()));
}
