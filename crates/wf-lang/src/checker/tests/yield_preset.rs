use super::*;

#[test]
fn yield_preset_fields_are_checked_after_expansion() {
    let input = r#"
yield preset base_alerts (
    y = "wfl",
    n = e.count
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts (
        x = e.sip
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn yield_preset_later_values_are_overridden() {
    let input = r#"
yield preset base_alerts (
    y = 1
)

yield preset chars_override (
    y = "ok"
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts, chars_override (
        x = e.sip
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn yield_preset_explicit_yield_overrides_presets() {
    let input = r#"
yield preset base_alerts (
    y = 1
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts (
        x = e.sip,
        y = "ok"
    )
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn yield_preset_can_supply_all_fields_with_empty_yield_args() {
    let input = r#"
yield preset base_alerts (
    x = e.sip,
    y = "ok",
    n = e.count
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts ()
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn parameterized_yield_preset_expands_args_and_defaults() {
    let input = r#"
yield preset base_alerts <severity, count = 1> (
    x = e.sip,
    y = $severity,
    n = $count
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts<"high"> ()
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn parameterized_yield_preset_accepts_field_expr_args() {
    let input = r#"
yield preset base_alerts <severity> (
    x = e.sip,
    y = $severity,
    n = e.count
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts<e.user> ()
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn parameterized_yield_preset_missing_required_arg_is_rejected() {
    let input = r#"
yield preset base_alerts <severity> (
    y = $severity
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts (
        x = e.sip,
        n = e.count
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "yield preset `base_alerts` missing required argument `severity`",
    );
}

#[test]
fn parameterized_yield_preset_too_many_args_is_rejected() {
    let input = r#"
yield preset base_alerts <severity, source = "wfusion"> (
    y = $severity
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts<"high", "sensor", "extra"> (
        x = e.sip,
        n = e.count
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "yield preset `base_alerts` expects 1..2 arguments, got 3",
    );
}

#[test]
fn parameterized_yield_preset_unknown_param_is_rejected() {
    let input = r#"
yield preset base_alerts <severity> (
    y = $missing
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts<"high"> (
        x = e.sip,
        n = e.count
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "unknown yield preset parameter `$missing` in `base_alerts`",
    );
}

#[test]
fn parameterized_yield_preset_duplicate_param_is_rejected() {
    let input = r#"
yield preset base_alerts <severity, severity = "low"> (
    y = $severity
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts<"high"> (
        x = e.sip,
        n = e.count
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "duplicate yield preset parameter `severity` in `base_alerts`",
    );
}

#[test]
fn parameterized_yield_preset_required_param_after_default_is_rejected() {
    let input = r#"
yield preset base_alerts <source = "wfusion", severity> (
    y = $severity
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts<"sensor", "high"> (
        x = e.sip,
        n = e.count
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "yield preset `base_alerts` required parameter `severity` cannot follow a defaulted parameter",
    );
}

#[test]
fn yield_preset_param_outside_preset_is_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        x = e.sip,
        y = $severity,
        n = e.count
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "yield preset parameter `$severity` can only be used inside a yield preset",
    );
}

#[test]
fn yield_preset_unknown_ref_is_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : missing_preset (
        x = e.sip
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "unknown yield preset `missing_preset`",
    );
}

#[test]
fn yield_preset_unknown_field_is_rejected_after_expansion() {
    let input = r#"
yield preset base_alerts (
    missing = "not in out"
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts (
        x = e.sip
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "yield argument `missing` is not a field in target window `out`",
    );
}

#[test]
fn yield_preset_reserved_output_prefix_is_rejected_after_expansion() {
    let input = r#"
yield preset base_alerts (
    __wfu_internal = "nope"
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts (
        x = e.sip
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "yield argument `__wfu_internal` uses reserved prefix `__wfu_`",
    );
}

#[test]
fn yield_preset_duplicate_decl_is_rejected() {
    let input = r#"
yield preset base_alerts (y = "a")
yield preset base_alerts (y = "b")

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts (
        x = e.sip
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "duplicate yield preset `base_alerts`",
    );
}

#[test]
fn yield_preset_duplicate_decl_does_not_hide_rule_ref_errors() {
    let input = r#"
yield preset base_alerts (y = "a")
yield preset base_alerts (y = "b")

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : missing_preset (
        x = e.sip
    )
}
"#;
    let errs = check_errors(input, &[auth_events_window(), output_window()]);
    assert!(
        errs.iter()
            .any(|e| e.contains("duplicate yield preset `base_alerts`")),
        "expected duplicate preset error, got: {:?}",
        errs
    );
    assert!(
        errs.iter()
            .any(|e| e.contains("unknown yield preset `missing_preset`")),
        "expected unknown preset error, got: {:?}",
        errs
    );
}

#[test]
fn yield_preset_duplicate_ref_is_rejected() {
    let input = r#"
yield preset base_alerts (y = "a")

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts, base_alerts (
        x = e.sip
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "yield preset `base_alerts` is referenced more than once",
    );
}

#[test]
fn yield_preset_type_errors_are_reported_at_use_site() {
    let input = r#"
yield preset base_alerts (
    n = e.user
)

rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts (
        x = e.sip
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "yield argument `n` type mismatch",
    );
}

#[test]
fn yield_preset_on_each_expressions_are_checked_after_expansion() {
    let input = r#"
yield preset base_alerts (
    y = close_reason
)

rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out : base_alerts (
        x = e.sip,
        n = e.count
    )
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "close_reason is not available in `on each`",
    );
}
