use super::*;
use crate::schema::FieldType;

/// auth_events with a nested `roles_obj` object field (issue #64).
fn auth_events_with_roles() -> WindowSchema {
    make_window(
        "auth_events",
        vec!["auth_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("roles_obj", FieldType::Object),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

#[test]
fn nested_path_root_exists_passes() {
    let input = r#"
rule r {
    events { a : auth_events }
    match<:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.roles_obj.source.process.uid)
}
"#;
    assert_no_errors(input, &[auth_events_with_roles(), output_window()]);
}

#[test]
fn nested_path_root_exists_with_array_index_passes() {
    let input = r#"
rule r {
    events { a : auth_events }
    match<:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.roles_obj.related[0].process.name)
}
"#;
    assert_no_errors(input, &[auth_events_with_roles(), output_window()]);
}

#[test]
fn nested_path_root_missing_errors() {
    // `auth_events` has no `roles_obj` field → the root of the path is invalid.
    let input = r#"
rule r {
    events { a : auth_events }
    match<:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.roles_obj.source.process.uid)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "field `roles_obj` not found in window `auth_events`",
    );
}

#[test]
fn nested_path_bad_alias_errors() {
    let input = r#"
rule r {
    events { a : auth_events }
    match<:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = b.roles_obj.source.process.uid)
}
"#;
    assert_has_error(
        input,
        &[auth_events_with_roles(), output_window()],
        "`b` is not a declared alias or step label",
    );
}

#[test]
fn nested_path_in_match_key_is_parse_error_not_silent() {
    // Match keys are single-level; a nested path is rejected at parse time so
    // it can never reach the checker as a FieldRef::Path.
    let input = r#"
rule r {
    events { a : auth_events }
    match<a.roles_obj.source:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    let err = parse_wfl(input)
        .expect_err("nested path in match key must fail to parse");
    assert!(
        format!("{:?}", err).contains("match"),
        "expected a parse error mentioning the match clause, got: {:?}",
        err
    );
}

#[test]
fn nested_path_as_stat_selector_arg_errors() {
    // Stat selectors (first/last/collect_*) aggregate flat columns; a nested
    // path argument is rejected with the column-projection message (the runtime
    // would have no column to fold).
    let input = r#"
rule r {
    events { a : auth_events }
    match<:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (n = first(a.roles_obj.related))
}
"#;
    assert_has_error(
        input,
        &[auth_events_with_roles(), output_window()],
        "first() argument must be a column projection (alias.field)",
    );
}

#[test]
fn nested_path_as_sum_avg_min_max_arg_errors() {
    // sum/avg/min/max reject nested paths too (previously they passed the
    // checker and then silently omitted the field at runtime).
    for (func, message) in [
        ("avg", "avg() argument must be a column projection (alias.field)"),
        ("sum", "sum() argument must be a column projection (alias.field)"),
        ("min", "min() argument must be a column projection (alias.field)"),
        ("max", "max() argument must be a column projection (alias.field)"),
    ] {
        let input = format!(
            r#"
rule r {{
    events {{ a : auth_events }}
    match<:5m> {{ on event {{ a | count >= 1; }} }} -> score(50.0)
    entity(ip, a.sip)
    yield out (n = {func}(a.roles_obj.risk))
}}
"#
        );
        assert_has_error(
            &input,
            &[auth_events_with_roles(), output_window()],
            message,
        );
    }
}

#[test]
fn nested_path_as_count_arg_errors() {
    // count() takes a set-level alias, not a field projection — a nested path
    // is rejected like a flat qualified field (previously it passed the checker
    // and silently omitted the field at runtime).
    let input = r#"
rule r {
    events { a : auth_events }
    match<:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (n = count(a.roles_obj.x))
}
"#;
    assert_has_error(
        input,
        &[auth_events_with_roles(), output_window()],
        "count() expects a set-level argument (alias), not a field projection",
    );
}


