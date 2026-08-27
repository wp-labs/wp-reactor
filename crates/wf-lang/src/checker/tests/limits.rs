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
// disk_provider（状态落盘, 2026-08-27 改名自 spill）
// ---------------------------------------------------------------------------

#[test]
fn check_limits_disk_provider_redb_accepted() {
    // 合法形态: stats 规则 + 非空键 + disk_provider + max_disk
    let input = r#"
rule r {
    events { e : auth_events }
    stats<10s:fixed> group by (e.sip) { e | count as n; }
    entity(digit, e.sip)
    yield out (y = fmt("{}", stat.value(final(n))))
    limits { max_memory = "1GB"; disk_provider = "redb"; max_disk = "8GB"; }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

/// 旧键兼容别名（2026-08-27 改名）: `spill`（→ disk_provider）与 `max_spill_bytes`
/// （→ max_disk）无 Error（仍生效）, 但产生迁移 Warning。
#[test]
fn check_limits_spill_old_alias_warns_migration() {
    let input = r#"
rule r {
    events { e : auth_events }
    stats<10s:fixed> group by (e.sip) { e | count as n; }
    entity(digit, e.sip)
    yield out (y = fmt("{}", stat.value(final(n))))
    limits { spill = "redb"; max_spill_bytes = "8GB"; }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
    // 两个旧键别名各产生一条迁移 Warning（check_errors 只收 Error, 这里直接收全量）。
    let file = parse_wfl(input).expect("parse");
    let warns = check_wfl(&file, &[auth_events_window(), output_window()]);
    assert!(
        warns.iter().any(|e| {
            e.severity == crate::checker::Severity::Warning && e.message.contains("disk_provider")
        }),
        "旧键 spill 应产生 disk_provider 迁移警告"
    );
    assert!(
        warns.iter().any(|e| {
            e.severity == crate::checker::Severity::Warning && e.message.contains("max_disk")
        }),
        "旧键 max_spill_bytes 应产生 max_disk 迁移警告"
    );
}

#[test]
fn check_limits_disk_provider_unknown_value_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    stats<10s:fixed> group by (e.sip) { e | count as n; }
    entity(digit, e.sip)
    yield out (y = fmt("{}", stat.value(final(n))))
    limits { disk_provider = "rocksdb"; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "disk_provider",
    );
}

/// 旧键 `spill` 别名非法值同样报错（拒绝静默忽略）。
#[test]
fn check_limits_spill_alias_unknown_value_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    stats<10s:fixed> group by (e.sip) { e | count as n; }
    entity(digit, e.sip)
    yield out (y = fmt("{}", stat.value(final(n))))
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
    stats<10s:fixed> group by (e.sip) { e | count as n; }
    entity(digit, e.sip)
    yield out (y = fmt("{}", stat.value(final(n))))
    limits { spill = "redb"; max_spill_bytes = "lots"; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "max_spill_bytes",
    );
}

// ---------------------------------------------------------------------------
// 场景静态判定（2026-08-27）: 配了但不生效 → 报错, 不静默忽略
// ---------------------------------------------------------------------------

/// match/on-each 规则无状态落盘路径——`disk_provider` 会被 spawn 静默忽略,
/// 静态报错。
#[test]
fn check_limits_disk_provider_match_rule_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { disk_provider = "redb"; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "仅支持 stats",
    );
}

/// match/on-each 规则 + `max_disk` 同样被静默忽略 → 报错。
#[test]
fn check_limits_max_disk_match_rule_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_disk = "8GB"; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "仅支持 stats",
    );
}

/// 空键 stats（无 group by）单桶无驱逐对象——`disk_provider` 从不生效 → 报错。
#[test]
fn check_limits_disk_provider_empty_key_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    stats<10s:fixed> { e | count as n; }
    entity(digit, 1)
    yield out (y = fmt("{}", stat.value(final(n))))
    limits { disk_provider = "redb"; }
}
"#;
    assert_has_error(input, &[auth_events_window(), output_window()], "group by");
}

/// `max_disk` 配了但没配 `disk_provider` → 落盘未启用, 上限不生效（Warning）。
#[test]
fn check_limits_max_disk_without_provider_warns() {
    let input = r#"
rule r {
    events { e : auth_events }
    stats<10s:fixed> group by (e.sip) { e | count as n; }
    entity(digit, e.sip)
    yield out (y = fmt("{}", stat.value(final(n))))
    limits { max_disk = "8GB"; }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
    let file = parse_wfl(input).expect("parse");
    let warns = check_wfl(&file, &[auth_events_window(), output_window()]);
    assert!(
        warns.iter().any(|e| {
            e.severity == crate::checker::Severity::Warning && e.message.contains("不生效")
        }),
        "max_disk 无 disk_provider 应产生不生效警告"
    );
}

/// 旧键 `spill` 别名 + match 规则 → 同样报「仅支持 stats」（别名不豁免场景检查）。
#[test]
fn check_limits_spill_alias_match_rule_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { spill = "redb"; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "仅支持 stats",
    );
}

/// 旧键 `spill` 别名 + 空键 stats → 同样报「无驱逐对象」。（
#[test]
fn check_limits_spill_alias_empty_key_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    stats<10s:fixed> { e | count as n; }
    entity(digit, 1)
    yield out (y = fmt("{}", stat.value(final(n))))
    limits { spill = "redb"; }
}
"#;
    assert_has_error(input, &[auth_events_window(), output_window()], "group by");
}

/// 旧键 `max_spill_bytes` 别名 + match 规则 → 报「仅支持 stats」。
#[test]
fn check_limits_max_spill_bytes_match_rule_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_spill_bytes = "8GB"; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "仅支持 stats",
    );
}

/// 空键 stats + `max_disk`（无 provider）→ Warning「不生效」（无驱逐对象, 上限无意义）。
#[test]
fn check_limits_max_disk_empty_key_warns_no_provider() {
    let input = r#"
rule r {
    events { e : auth_events }
    stats<10s:fixed> { e | count as n; }
    entity(digit, 1)
    yield out (y = fmt("{}", stat.value(final(n))))
    limits { max_disk = "8GB"; }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
    let file = parse_wfl(input).expect("parse");
    let warns = check_wfl(&file, &[auth_events_window(), output_window()]);
    assert!(
        warns.iter().any(|e| {
            e.severity == crate::checker::Severity::Warning && e.message.contains("不生效")
        }),
        "空键 stats + max_disk 无 provider 应产生不生效警告"
    );
}

/// 新键 `disk_provider` + 旧键 `max_spill_bytes` 组合: has_disk_provider 认新键
/// → max_spill_bytes 不报「未配置 provider」警告（只有迁移警告）。
#[test]
fn check_limits_new_provider_old_disk_key_no_unused_warning() {
    let input = r#"
rule r {
    events { e : auth_events }
    stats<10s:fixed> group by (e.sip) { e | count as n; }
    entity(digit, e.sip)
    yield out (y = fmt("{}", stat.value(final(n))))
    limits { disk_provider = "redb"; max_spill_bytes = "8GB"; }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
    let file = parse_wfl(input).expect("parse");
    let warns = check_wfl(&file, &[auth_events_window(), output_window()]);
    assert!(
        !warns.iter().any(|e| e.message.contains("不生效")),
        "disk_provider 已配置, max_spill_bytes 不应报未配置警告"
    );
}

/// 旧键 `spill` 别名 + 新键 `max_disk` 组合: has_disk_provider 认别名
/// → 无「未配置」警告。
#[test]
fn check_limits_old_provider_alias_new_disk_key_no_unused_warning() {
    let input = r#"
rule r {
    events { e : auth_events }
    stats<10s:fixed> group by (e.sip) { e | count as n; }
    entity(digit, e.sip)
    yield out (y = fmt("{}", stat.value(final(n))))
    limits { spill = "redb"; max_disk = "8GB"; }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
    let file = parse_wfl(input).expect("parse");
    let warns = check_wfl(&file, &[auth_events_window(), output_window()]);
    assert!(
        !warns.iter().any(|e| e.message.contains("不生效")),
        "spill 别名已配置 provider, max_disk 不应报未配置警告"
    );
}

/// R1 修复回归: provider 值非法时, max_disk 不叠加「未配置」警告——键明明配了,
/// 只是值错（值错误已单独报）。
#[test]
fn check_limits_invalid_provider_value_does_not_warn_unused_disk() {
    let input = r#"
rule r {
    events { e : auth_events }
    stats<10s:fixed> group by (e.sip) { e | count as n; }
    entity(digit, e.sip)
    yield out (y = fmt("{}", stat.value(final(n))))
    limits { disk_provider = "rocksdb"; max_disk = "8GB"; }
}
"#;
    // 值非法必须报 Error
    assert_has_error(input, &[auth_events_window(), output_window()], "invalid");
    let file = parse_wfl(input).expect("parse");
    let warns = check_wfl(&file, &[auth_events_window(), output_window()]);
    assert!(
        !warns.iter().any(|e| e.message.contains("不生效")),
        "provider 值非法时不应叠加未配置警告（认键不认值）"
    );
}
