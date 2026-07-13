use super::*;

#[test]
fn mvsort_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvsort(e.action))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "must be an array expression",
    );
}

// L3 Collection functions (M28.2)

#[test]
fn collect_set_valid() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("resources", FieldType::Array(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (resources = collect_set(e.action))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn collect_set_wrong_arg() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = collect_set(42))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "column projection",
    );
}

#[test]
fn mvcount_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("n", bt(BaseType::Digit))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvcount(collect_set(e.action)))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn mvcount_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvcount(e.action))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "must be an array expression",
    );
}

#[test]
fn mvjoin_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("joined", bt(BaseType::Chars))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (joined = mvjoin(collect_list(e.action), ","))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn mvjoin_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvjoin(e.action, ","))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "first argument must be an array expression",
    );
}

#[test]
fn mvdedup_valid() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("uniq", FieldType::Array(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (uniq = mvdedup(collect_list(e.action)))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn mvdedup_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvdedup(e.action))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "mvdedup() argument must be an array expression",
    );
}

#[test]
fn mvindex_valid_scalar_and_range() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("pick", bt(BaseType::Chars)),
            ("slice", FieldType::Array(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        pick = mvindex(collect_list(e.action), 0),
        slice = mvindex(collect_list(e.action), 0, 1)
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn mvindex_wrong_first_arg_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvindex(e.action, 0))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "first argument must be an array expression",
    );
}

#[test]
fn mv_functions_accept_array_any_and_empty_array_literals() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("n", bt(BaseType::Digit)),
            ("joined", bt(BaseType::Chars)),
            ("uniq", FieldType::ArrayAny),
            ("sorted", FieldType::ArrayAny),
            ("reversed", FieldType::ArrayAny),
            ("slice", FieldType::ArrayAny),
            ("appended", FieldType::ArrayAny),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        n = mvcount(array []),
        joined = mvjoin(array ["ssh", 22, e.action], ","),
        uniq = mvdedup(array ["ssh", 22, e.action]),
        sorted = mvsort(array ["ssh", 22, e.action]),
        reversed = mvreverse(array ["ssh", 22, e.action]),
        slice = mvindex(array ["ssh", 22, e.action], 0, 1),
        appended = mvappend(array ["ssh", 22], e.action)
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn mvindex_rejects_scalar_pick_from_array_any_literal() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvindex(array ["ssh", 22], 0))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "cannot infer scalar result type",
    );
}

#[test]
fn mvappend_valid() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("vals", FieldType::Array(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (vals = mvappend(collect_list(e.action), "tail"))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn mvappend_promotes_digit_and_float_elements() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("scalar_vals", FieldType::Array(BaseType::Float)),
            ("array_vals", FieldType::Array(BaseType::Float)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        scalar_vals = mvappend(1.5, 2),
        array_vals = mvappend(array [1, 1.5], 2)
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn mvappend_mixed_type_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = mvappend(collect_list(e.action), e.count))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}
