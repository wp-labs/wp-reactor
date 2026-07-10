use super::*;

#[test]
fn yield_unknown_target() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield nonexistent (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window()],
        "yield target window `nonexistent` does not exist",
    );
}

#[test]
fn yield_target_has_stream() {
    // auth_events has streams, so it shouldn't be a yield target.
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield auth_events (sip = e.sip)
}
"#;
    assert_has_error(input, &[auth_events_window()], "has stream subscriptions");
}

#[test]
fn yield_system_field() {
    let out = make_output_window("out", vec![("score", bt(BaseType::Float))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (score = 50.0)
}
"#;
    assert_has_error(input, &[auth_events_window(), out], "system field");
}

#[test]
fn yield_rejects_wfu_reserved_prefix() {
    let out = make_output_window("out", vec![("__wfu_score", bt(BaseType::Float))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (__wfu_score = 50.0)
}
"#;
    assert_has_error(input, &[auth_events_window(), out], "uses reserved prefix");
}

#[test]
fn yield_unknown_field() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (nonexistent = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not a field in target window",
    );
}

#[test]
fn yield_type_mismatch() {
    // 'x' in out is ip, but we assign a digit
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = 42)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "type mismatch",
    );
}

#[test]
fn yield_allows_score_system_var() {
    let out = make_output_window("out", vec![("risk_score", bt(BaseType::Float))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (risk_score = @score)
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn yield_allows_structured_object_and_array_fields() {
    let out = make_output_window(
        "out",
        vec![
            ("risk_context", FieldType::Object),
            ("tags", FieldType::ArrayAny),
            ("scores", FieldType::Array(BaseType::Float)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        risk_context = object {
            score: float = @score;
            source = e.sip;
            tags: array = array ["bruteforce", e.action];
        },
        tags = array ["bruteforce", "ssh"],
        scores = array [@score, 1.5]
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn yield_allows_typed_array_literal_for_typed_array_field() {
    let out = make_output_window("out", vec![("ports", FieldType::Array(BaseType::Digit))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (ports = array [22, 2222, e.count])
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn yield_allows_digit_and_float_literals_for_float_array_field() {
    let out = make_output_window("out", vec![("scores", FieldType::Array(BaseType::Float))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (scores = array [1, 1.5, @score])
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn yield_allows_digit_array_for_float_array_field() {
    let out = make_output_window("out", vec![("scores", FieldType::Array(BaseType::Float))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (scores = array [1, 2, e.count])
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn yield_allows_empty_array_for_typed_array_field() {
    let out = make_output_window("out", vec![("ports", FieldType::Array(BaseType::Digit))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (ports = array [])
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn yield_rejects_mixed_array_literal_for_typed_array_field() {
    let out = make_output_window("out", vec![("scores", FieldType::Array(BaseType::Float))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (scores = array [@score, "high"])
}
"#;
    assert_has_error(input, &[auth_events_window(), out], "type mismatch");
}

#[test]
fn yield_rejects_nested_array_literal_for_typed_array_field() {
    let out = make_output_window("out", vec![("tags", FieldType::Array(BaseType::Chars))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (tags = array [array ["ssh"]])
}
"#;
    assert_has_error(input, &[auth_events_window(), out], "type mismatch");
}

#[test]
fn yield_rejects_object_item_type_hint_mismatch() {
    let out = make_output_window("out", vec![("risk_context", FieldType::Object)]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        risk_context = object {
            score: digit = "high";
        }
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out],
        "object field type hint",
    );
}

#[test]
fn yield_rejects_duplicate_object_field_assignments() {
    let out = make_output_window("out", vec![("risk_context", FieldType::Object)]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        risk_context = object {
            source = e.sip;
            source = e.dip;
        }
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out],
        "duplicate object field",
    );
}

#[test]
fn yield_rejects_duplicate_object_field_targets() {
    let out = make_output_window("out", vec![("risk_context", FieldType::Object)]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        risk_context = object {
            source, source = e.sip;
        }
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out],
        "duplicate object field",
    );
}

#[test]
fn yield_rejects_structured_object_for_scalar_field() {
    let out = make_output_window("out", vec![("risk_context", bt(BaseType::Chars))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        risk_context = object { source = e.sip; }
    )
}
"#;
    assert_has_error(input, &[auth_events_window(), out], "type mismatch");
}

#[test]
fn score_rejects_score_system_var() {
    let out = make_output_window("out", vec![("risk_score", bt(BaseType::Float))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(@score)
    entity(ip, e.sip)
    yield out (risk_score = 1.0)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out],
        "system variables like `@score` are only allowed in `yield` expressions",
    );
}

#[test]
fn yield_allows_count_set_level_alias() {
    let out = make_output_window("out", vec![("n", bt(BaseType::Digit))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = count(e))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn yield_rejects_avg_set_level_alias() {
    let out = make_output_window("out", vec![("n", bt(BaseType::Float))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = avg(e))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out],
        "avg() requires a field projection like alias.field",
    );
}

#[test]
fn yield_rejects_max_set_level_alias() {
    let out = make_output_window("out", vec![("n", bt(BaseType::Digit))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = max(e))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), out],
        "max() requires a field projection like alias.field",
    );
}

#[test]
fn yield_allows_avg_field_projection() {
    let out = make_output_window("out", vec![("n", bt(BaseType::Float))]);
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = avg(e.count))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}
