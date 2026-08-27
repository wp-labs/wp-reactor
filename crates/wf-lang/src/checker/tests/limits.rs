use super::*;

// ---------------------------------------------------------------------------
// Zero-value rejection
// ---------------------------------------------------------------------------

#[test]
fn check_limits_max_instances_zero_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_instances = 0; on_exceed = throttle; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "max_instances",
    );
}

#[test]
fn check_limits_max_instances_positive_accepted() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_instances = 1; on_exceed = throttle; }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn check_limits_max_memory_zero_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_memory = "0MB"; on_exceed = throttle; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "max_memory",
    );
}

#[test]
fn check_limits_max_throttle_zero_count_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_throttle = "0/min"; on_exceed = throttle; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "max_throttle",
    );
}

// ---------------------------------------------------------------------------
// Overflow rejection
// ---------------------------------------------------------------------------

#[test]
fn check_limits_max_memory_overflow_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_memory = "18446744073709551615GB"; on_exceed = throttle; }
}
"#;
    assert_has_error(input, &[auth_events_window(), output_window()], "overflows");
}

// ---------------------------------------------------------------------------
// spill（M4）
// ---------------------------------------------------------------------------

#[test]
fn check_limits_spill_redb_accepted() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_memory = "1GB"; spill = "redb"; max_disk = "8GB"; }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

/// 旧键 `max_spill_bytes` 保留为兼容别名（2026-08-27 改名 max_disk）:
/// 无 Error（仍生效）, 但产生迁移 Warning。
#[test]
fn check_limits_spill_old_alias_warns_migration() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { spill = "redb"; max_spill_bytes = "8GB"; }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
    // 别名产生迁移 Warning（check_errors 只收 Error, 这里直接收全量）。
    let file = parse_wfl(input).expect("parse");
    let warns = check_wfl(&file, &[auth_events_window(), output_window()])
        .into_iter()
        .filter(|e| {
            e.severity == crate::checker::Severity::Warning && e.message.contains("max_disk")
        })
        .count();
    assert!(warns >= 1, "旧键 max_spill_bytes 应产生 max_disk 迁移警告");
}

#[test]
fn check_limits_spill_unknown_value_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { spill = "rocksdb"; }
}
"#;
    assert_has_error(input, &[auth_events_window(), output_window()], "spill");
}

#[test]
fn check_limits_spill_max_bytes_bad_format_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { spill = "redb"; max_spill_bytes = "lots"; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "max_spill_bytes",
    );
}
