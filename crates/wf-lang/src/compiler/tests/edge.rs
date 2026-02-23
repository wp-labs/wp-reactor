use super::*;

// =========================================================================
// 14. compile_empty_file
// =========================================================================

#[test]
fn compile_empty_file() {
    let plans = compile_with("", &[]);
    assert!(plans.is_empty());
}

// =========================================================================
// 15. compile_rejects_semantic_errors
// =========================================================================

#[test]
fn compile_rejects_semantic_errors() {
    let file = parse_wfl(
        r#"
rule r {
    events { e : nonexistent_window }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
    )
    .unwrap();
    let err = compile_wfl(&file, &[output_window()]).unwrap_err();
    assert!(
        err.to_string().contains("nonexistent_window"),
        "error should mention the bad window: {err}",
    );
}

// =========================================================================
// 16. compile_yield_version
// =========================================================================

#[test]
fn compile_yield_version() {
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    meta { contract_version = "2" }
    events { fail : auth_events }
    match<sip:5m> { on event { fail | count >= 3; } } -> score(70.0)
    entity(ip, fail.sip)
    yield out@v2 (x = fail.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans[0].yield_plan.version, Some(2));
}

#[test]
fn compile_yield_no_version() {
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { fail : auth_events }
    match<sip:5m> { on event { fail | count >= 3; } } -> score(70.0)
    entity(ip, fail.sip)
    yield out (x = fail.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans[0].yield_plan.version, None);
}

// =========================================================================
// 17. compile_if_then_else_in_score
// =========================================================================

#[test]
fn compile_if_then_else_in_score() {
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } }
    -> score(if e.action == "failed" then 80.0 else 40.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert!(matches!(&plans[0].score_plan.expr, Expr::IfThenElse { .. }));
}
