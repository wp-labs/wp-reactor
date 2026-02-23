use crate::parse_wfl;

// -----------------------------------------------------------------------
// Error cases
// -----------------------------------------------------------------------

#[test]
fn reject_missing_events() {
    let input = r#"
rule r {
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_missing_score() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } }
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_digit_leading_ident() {
    // rule name starting with digit
    let input = r#"
rule 1bad {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_empty_events() {
    let input = r#"
rule r {
    events { }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_empty_on_event() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_contract_decimal_hit_index() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect {
        hit[0.5].score == 50.0;
    }
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_contract_decimal_hits_count() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect {
        hits == 1.5;
    }
}
"#;
    assert!(parse_wfl(input).is_err());
}
