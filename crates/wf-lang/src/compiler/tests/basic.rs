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

#[test]
fn compile_yield_presets_expand_into_yield_plan() {
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
yield preset base_alerts (
    y = "base",
    n = 1
)

yield preset override_fields (
    y = "override"
)

rule preset_rule {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts, override_fields (
        x = e.sip,
        n = e.count
    )
}
"#,
        &schemas,
    );
    assert_eq!(plans.len(), 1);
    let fields = &plans[0].yield_plan.fields;
    assert_eq!(fields.len(), 3);
    assert_eq!(fields[0].name, "y");
    assert_eq!(fields[0].value, Expr::StringLit("override".into()));
    assert_eq!(fields[1].name, "n");
    assert_eq!(
        fields[1].value,
        Expr::Field(FieldRef::Qualified("e".into(), "count".into()))
    );
    assert_eq!(fields[2].name, "x");
}

#[test]
fn compile_parameterized_yield_preset_expands_args_and_defaults() {
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
yield preset base_alerts <severity, count = 1> (
    x = e.sip,
    y = $severity,
    n = $count
)

rule preset_rule {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts<"high"> ()
}
"#,
        &schemas,
    );
    assert_eq!(plans.len(), 1);
    let fields = &plans[0].yield_plan.fields;
    assert_eq!(fields.len(), 3);
    assert_eq!(fields[0].name, "x");
    assert_eq!(
        fields[0].value,
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))
    );
    assert_eq!(fields[1].name, "y");
    assert_eq!(fields[1].value, Expr::StringLit("high".into()));
    assert_eq!(fields[2].name, "n");
    assert_eq!(fields[2].value, Expr::Number(1.0));
}

#[test]
fn compile_yield_preset_can_supply_empty_yield_body() {
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
yield preset base_alerts (
    x = e.sip,
    y = "base",
    n = e.count
)

rule preset_rule {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts ()
}
"#,
        &schemas,
    );
    let fields = &plans[0].yield_plan.fields;
    assert_eq!(fields.len(), 3);
    assert_eq!(fields[0].name, "x");
    assert_eq!(fields[1].name, "y");
    assert_eq!(fields[2].name, "n");
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
    assert_eq!(p.match_plan.close_mode, CloseMode::Or);

    let close_branch = &p.match_plan.close_steps[0].branches[0];
    assert_eq!(close_branch.source, "resp");
    assert!(close_branch.guard.is_some());
    assert_eq!(close_branch.agg.measure, Measure::Count);
    assert_eq!(close_branch.agg.cmp, CmpOp::Eq);
    assert_eq!(close_branch.agg.threshold, Expr::Number(0.0));
}

#[test]
fn compile_on_each_rule() {
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule pass_through {
    events { e : auth_events }
    on each e where e.action == "failed" -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    let p = &plans[0];

    let each = p.each_plan.as_ref().expect("missing each plan");
    assert_eq!(each.alias, "e");
    assert_eq!(
        each.filter,
        Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified(
                "e".into(),
                "action".into(),
            ))),
            right: Box::new(Expr::StringLit("failed".into())),
        })
    );
    assert!(p.match_plan.event_steps.is_empty());
    assert!(p.match_plan.close_steps.is_empty());
    assert_eq!(p.score_plan.expr, Expr::Number(1.0));
}

#[test]
fn compile_on_each_chain_uses_auto_wfu_fields() {
    let enriched = make_output_window(
        "enriched_events",
        vec![
            ("event_time", bt(BaseType::Time)),
            ("sip", bt(BaseType::Ip)),
            ("username", bt(BaseType::Chars)),
        ],
    );
    let final_out = make_output_window("final_out", vec![("sip", bt(BaseType::Ip))]);
    let plans = compile_with(
        r#"
rule enrich_each_event {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield enriched_events (
        event_time = e.event_time,
        sip = e.sip,
        username = e.user
    )
}

rule final_risk {
    events { x : enriched_events }
    match<sip:5m> {
        on event {
            x | count >= 1;
        }
    } -> score(avg(x.__wfu_score) + 10.0)
    entity(ip, x.sip)
    yield final_out (sip = x.sip)
}
"#,
        &[auth_events_window(), enriched, final_out],
    );
    assert_eq!(plans.len(), 2);
    assert_eq!(plans[1].name, "final_risk");
}

// =========================================================================
// 3b. compile_and_close
// =========================================================================

#[test]
fn compile_and_close() {
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
        and close {
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
    assert_eq!(p.match_plan.close_mode, CloseMode::And);
}

// =========================================================================
// needs_field_history precision (q12 advance hot path)
// =========================================================================

#[test]
fn close_rule_yield_reads_only_key_skips_field_history() {
    // q12 shape: fixed window + `and close`, score/entity/yields reference
    // only the match key (served from scope_key by build_eval_context) and
    // literals — the per-event field_values collection is pure overhead here.
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule q12_like {
    events { b : auth_events }
    match<sip:10s:fixed> {
        on event { b | count >= 1; }
        and close { n: b | count >= 1; }
    } -> score(10.0)
    entity(digit, b.sip)
    yield out (x = b.sip, y = "q12")
}
"#,
        &schemas,
    );
    let p = &plans[0];
    assert!(!p.match_plan.close_steps.is_empty());
    assert!(
        !p.match_plan.needs_field_history,
        "key-only close yields must not require the field history"
    );
}

#[test]
fn close_rule_yield_reads_non_key_field_needs_field_history() {
    // A yield reading a non-key field resolves from field_values.last() at
    // close time — the history is required.
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule q12_like_nonkey {
    events { b : auth_events }
    match<sip:10s:fixed> {
        on event { b | count >= 1; }
        and close { n: b | count >= 1; }
    } -> score(10.0)
    entity(digit, b.sip)
    yield out (x = b.sip, y = b.action)
}
"#,
        &schemas,
    );
    let p = &plans[0];
    assert!(
        p.match_plan.needs_field_history,
        "non-key field yields need the history"
    );
}

#[test]
fn close_rule_l3_series_still_needs_field_history() {
    // L3 series (avg/collect_set) consume collected values — history stays on.
    let schemas = [auth_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule q12_like_l3 {
    events { b : auth_events }
    match<sip:10s:fixed> {
        on event { b | count >= 1; }
        and close { n: b | count >= 1; }
    } -> score(avg(b.count))
    entity(digit, b.sip)
    yield out (x = b.sip, y = "q12")
}
"#,
        &schemas,
    );
    let p = &plans[0];
    assert!(
        p.match_plan.needs_field_history,
        "L3 series need the history"
    );
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
