use super::*;

#[test]
fn cidr_match_valid_ip_field() {
    // Ip 类型字段可直接作为第一个参数（network.wfs 的 dip/sip 就是 Ip）。
    let input = r#"
rule r {
    events { e : auth_events && cidr_match(e.sip, "10.0.0.0/8") }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn cidr_match_valid_chars_field() {
    // Chars 类型字段（字符串形式的 IP）同样接受。
    let input = r#"
rule r {
    events { e : auth_events && cidr_match(e.action, "192.168.0.0/16") }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn cidr_match_valid_v6() {
    let input = r#"
rule r {
    events { e : auth_events && cidr_match(e.sip, "fe80::/10") }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn cidr_match_invalid_cidr_literal() {
    let input = r#"
rule r {
    events { e : auth_events && cidr_match(e.sip, "10.0.0.0") }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not a valid CIDR",
    );
}

#[test]
fn cidr_match_prefix_out_of_range() {
    let input = r#"
rule r {
    events { e : auth_events && cidr_match(e.sip, "10.0.0.0/33") }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not a valid CIDR",
    );
}

#[test]
fn cidr_match_wrong_arg_count() {
    let input = r#"
rule r {
    events { e : auth_events && cidr_match(e.sip) }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires exactly 2 arguments",
    );
}

#[test]
fn cidr_match_non_literal_subnet() {
    let input = r#"
rule r {
    events { e : auth_events && cidr_match(e.sip, e.action) }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "must be a string literal CIDR",
    );
}

#[test]
fn cidr_match_first_arg_not_ip() {
    let input = r#"
rule r {
    events { e : auth_events && cidr_match(e.count, "10.0.0.0/8") }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "first argument must be an IP or string field",
    );
}
