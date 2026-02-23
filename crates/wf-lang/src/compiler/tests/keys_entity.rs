use super::*;

// =========================================================================
// 5. compile_no_key
// =========================================================================

#[test]
fn compile_no_key() {
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule nokey {
    events { e : win }
    match<:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert!(plans[0].match_plan.keys.is_empty());
}

// =========================================================================
// 6. compile_compound_keys
// =========================================================================

#[test]
fn compile_compound_keys() {
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule compound {
    events { e : win }
    match<sip,dport:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    let keys = &plans[0].match_plan.keys;
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], FieldRef::Simple("sip".into()));
    assert_eq!(keys[1], FieldRef::Simple("dport".into()));
}

// =========================================================================
// 7. compile_entity_type_normalization
// =========================================================================

#[test]
fn compile_entity_type_normalization() {
    let schemas = [generic_window(), output_window()];

    // Ident form: ip
    let plans_ident = compile_with(
        r#"
rule r1 {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans_ident[0].entity_plan.entity_type, "ip");

    // StringLit form: "ip"
    let plans_str = compile_with(
        r#"
rule r2 {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity("ip", e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans_str[0].entity_plan.entity_type, "ip");

    // Both normalize to the same string
    assert_eq!(
        plans_ident[0].entity_plan.entity_type,
        plans_str[0].entity_plan.entity_type
    );
}

/// Uppercase entity type is lowercased during compilation.
#[test]
fn compile_entity_type_case_normalization() {
    let schemas = [generic_window(), output_window()];

    let plans = compile_with(
        r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(IP, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans[0].entity_plan.entity_type, "ip");

    let plans2 = compile_with(
        r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity("IP", e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans2[0].entity_plan.entity_type, "ip");
}
