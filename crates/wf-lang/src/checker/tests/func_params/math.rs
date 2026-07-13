use super::*;

#[test]
fn abs_valid() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("n", bt(BaseType::Digit))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = abs(e.count - 10))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn abs_wrong_type() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = abs(e.action))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "abs() argument must be numeric",
    );
}

#[test]
fn round_valid_with_precision() {
    let out = make_output_window(
        "out",
        vec![("x", bt(BaseType::Ip)), ("n", bt(BaseType::Float))],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = round(12.3456, 2))
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}

#[test]
fn top50_batch4_functions_valid() {
    use crate::schema::FieldType;
    let out = make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("sqrt_v", bt(BaseType::Float)),
            ("pow_v", bt(BaseType::Float)),
            ("log_v", bt(BaseType::Float)),
            ("exp_v", bt(BaseType::Float)),
            ("clamp_v", bt(BaseType::Float)),
            ("sign_v", bt(BaseType::Float)),
            ("trunc_v", bt(BaseType::Float)),
            ("finite_v", bt(BaseType::Bool)),
            ("ltrim_v", bt(BaseType::Chars)),
            ("rtrim_v", bt(BaseType::Chars)),
            ("concat_v", bt(BaseType::Chars)),
            ("index_v", bt(BaseType::Digit)),
            ("replace_plain_v", bt(BaseType::Chars)),
            ("sw_any_v", bt(BaseType::Bool)),
            ("ew_any_v", bt(BaseType::Bool)),
            ("coalesce_v", bt(BaseType::Chars)),
            ("isnull_v", bt(BaseType::Bool)),
            ("isnotnull_v", bt(BaseType::Bool)),
            ("sorted_v", FieldType::Array(BaseType::Chars)),
            ("reversed_v", FieldType::Array(BaseType::Chars)),
        ],
    );
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        sqrt_v = sqrt(16),
        pow_v = pow(2, 8),
        log_v = log(100, 10),
        exp_v = exp(1),
        clamp_v = clamp(e.count, 1, 100),
        sign_v = sign(e.count - 5),
        trunc_v = trunc(12.987),
        finite_v = is_finite(e.count),
        ltrim_v = ltrim("  hello"),
        rtrim_v = rtrim("hello  "),
        concat_v = concat("u=", e.user, "_c=", e.count),
        index_v = indexof(e.action, "fail"),
        replace_plain_v = replace_plain(e.action, "_", "-"),
        sw_any_v = startswith_any(e.action, "fail", "deny"),
        ew_any_v = endswith_any(e.action, "ed", "ied"),
        coalesce_v = coalesce(e.user, e.action, "unknown"),
        isnull_v = isnull(mvindex(split(e.action, "_"), 99)),
        isnotnull_v = isnotnull(e.action),
        sorted_v = mvsort(split(e.action, "_")),
        reversed_v = mvreverse(split(e.action, "_"))
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), out]);
}
