use super::*;

#[test]
fn sum_on_chars_field() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.action | sum >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires a numeric field",
    );
}

#[test]
fn count_on_column() {
    // count with a field selector (without distinct) should error
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.action | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "use `distinct | count`",
    );
}

#[test]
fn distinct_on_set() {
    // distinct without a field selector should error
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e | distinct | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "distinct requires a field selector",
    );
}

#[test]
fn score_non_numeric() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score("text")
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "score expression must be numeric",
    );
}

#[test]
fn entity_id_bool() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, true)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "scalar identity type",
    );
}

#[test]
fn compare_mismatched_types() {
    // sip is Ip, 42 is Digit — incompatible for ==
    let input = r#"
rule r {
    events { e : auth_events && sip == 42 }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "incompatible types",
    );
}

#[test]
fn logic_on_non_bool() {
    // sip || dip — both are Ip, not Bool
    let input = r#"
rule r {
    events { e : auth_events && sip || dip }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires bool operands",
    );
}
