//! rule 级与 rules/ 子模块校验分支：rules/mod.rs（where / on each / step label）、
//! rules/joins.rs、rules/keys.rs、rules/limits.rs（2026-09-04 自 coverage_extra.rs
//! 拆出；经 `use super::*` 复用父模块 window harness 与共享断言）。

use super::*;

// ===========================================================================
// rules/mod.rs — rule-level checks
// ===========================================================================

#[test]
fn where_without_join_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    where e.action == "failed"
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "`where` requires at least one `join` clause",
    );
}

#[test]
fn where_non_bool_rejected() {
    let schemas = vec![auth_events_window(), bid_win(), output_window()];
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.sip == bid_events.bidder
    where e.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(input, &schemas, "`where` expression must be bool");
}

#[test]
fn on_each_filter_non_bool_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e where e.sip -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "`on each where` expression must be bool",
    );
}

#[test]
fn on_each_rejects_set_level_alias_in_score() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(e)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "set-level alias references are not allowed in `on each` expressions",
    );
}

#[test]
fn on_each_rejects_qualified_close_reason() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (y = e.close_reason)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "close_reason is not available in `on each`",
    );
}

#[test]
fn on_each_rejects_stat_funcs() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (n = stat.count(window_event(e)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "function `count` is not allowed in `on each`",
    );
}

#[test]
fn step_label_conflicts_with_match_key_field() {
    let input = r#"
rule r {
    events { e : label_win }
    match<fail:5m> { on event { fail: e | count >= 1; } } -> score(50.0)
    entity(ip, e.fail)
    yield out (n = e.fail)
}
"#;
    assert_has_error(
        input,
        &[label_win(), output_window()],
        "step label `fail` conflicts with match key field of the same name",
    );
}

// ===========================================================================
// rules/joins.rs — join validation branches
// ===========================================================================

#[test]
fn join_target_window_missing() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join nonexistent on e.sip == nonexistent.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "join target window `nonexistent` does not exist in schemas",
    );
}

#[test]
fn join_condition_left_side_unresolved() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.bogus == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), bid_win(), output_window()],
        "join condition left side:",
    );
}

#[test]
fn join_condition_right_side_validation() {
    let schemas = vec![auth_events_window(), bid_win(), output_window()];

    let wrong_qualifier = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.sip == other.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        wrong_qualifier,
        &schemas,
        "join condition right side `other.sip` must be qualified with target window `bid_events`",
    );

    let field_missing = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.sip == bid_events.bogus
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        field_missing,
        &schemas,
        "join condition: field `bogus` not found in window `bid_events`",
    );

    let unqualified = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.sip == bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        unqualified,
        &schemas,
        "join condition right side must be qualified with window name",
    );
}

#[test]
fn join_key_must_be_scalar_base_type() {
    let float_join = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join float_win on e.sip == float_win.f
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        float_join,
        &[auth_events_window(), float_win(), output_window()],
        "join key `float_win.f` must be a scalar base type",
    );

    let object_join = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join obj_win on e.sip == obj_win.obj
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        object_join,
        &[auth_events_window(), obj_win(), output_window()],
        "join key `obj_win.obj` must be a scalar base type",
    );
}

#[test]
fn asof_join_requires_time_field_and_positive_within() {
    let no_time = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join out asof on e.sip == out.x
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        no_time,
        &[auth_events_window(), output_window()],
        "join `out` uses asof mode but target window has no time field",
    );

    let zero_within = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events asof within 0s on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        zero_within,
        &[auth_events_window(), bid_win(), output_window()],
        "join `bid_events` asof within must be > 0",
    );
}

#[test]
fn static_window_join_mode_restrictions() {
    // anti 对静态表**允许**（2026-08-24 放开）：纯键存在性否定不依赖时间，
    // 白名单排除是标准用例（Q21 形状）——不报错。
    let anti = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join prov anti on e.sip == prov.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(
        anti,
        &[auth_events_window(), provider_win(), output_window()],
    );

    let within = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join prov within 10s on e.sip == prov.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        within,
        &[auth_events_window(), provider_win(), output_window()],
        "`within` interval 需要右窗 time 字段，静态表没有",
    );

    let emit = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join prov on e.sip == prov.sip emit at e.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        emit,
        &[auth_events_window(), provider_win(), output_window()],
        "`emit at` deferred 触发需要窗口生命周期，静态表没有",
    );

    let reduce = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join prov reduce last(prov.sip) on e.sip == prov.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        reduce,
        &[auth_events_window(), provider_win(), output_window()],
        "`reduce` 归约对静态表 v1 不支持",
    );
}

#[test]
fn reduce_field_validation() {
    let schemas = vec![auth_events_window(), bid_win(), output_window()];

    let wrong_qualifier = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events reduce maxrow(other.price) on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        wrong_qualifier,
        &schemas,
        "join `bid_events` reduce measure field `other.price` must be qualified with target window `bid_events`",
    );

    let tie_missing = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events reduce maxrow(price) tie(bogus) on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        tie_missing,
        &schemas,
        "join `bid_events` reduce tie field `bogus` not found in window `bid_events`",
    );
}

#[test]
fn reduce_measure_field_must_be_scalar() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join obj_win reduce maxrow(obj_win.obj) on e.sip == obj_win.sip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), obj_win(), output_window()],
        "join `obj_win` reduce measure field `obj` must be scalar (structured type cannot be ordered)",
    );
}

// ===========================================================================
// rules/keys.rs — match key validation branches
// ===========================================================================

#[test]
fn session_gap_must_be_positive() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:session(0s)> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "session(gap) gap must be > 0",
    );
}

#[test]
fn bracketed_and_qualified_key_unknown_alias() {
    let bracketed = r#"
rule r {
    events { e : auth_events }
    match<missing["dip"]:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        bracketed,
        &[auth_events_window(), output_window()],
        "match key `missing[\"dip\"]` references unknown alias `missing`",
    );

    let qualified = r#"
rule r {
    events { e : auth_events }
    match<missing.sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        qualified,
        &[auth_events_window(), output_window()],
        "match key `missing.sip` references unknown alias `missing`",
    );
}

#[test]
fn qualified_key_field_not_found() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<e.bogus:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "match key `e.bogus`: field `bogus` not found in window",
    );
}

#[test]
fn join_window_qualified_key_rejected() {
    let schemas = vec![auth_events_window(), bid_win(), output_window()];
    let input = r#"
rule r {
    events { e : auth_events }
    match<bid_events.price:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &schemas,
        "match key `bid_events.price` references join window `bid_events`; join-side keys must be unqualified",
    );
}

#[test]
fn non_snapshot_join_key_source_rejected() {
    let schemas = vec![auth_events_window(), bid_win(), output_window()];
    let input = r#"
rule r {
    events { e : auth_events }
    match<price:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events asof on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &schemas,
        "match key `price` is only available on non-snapshot join window(s) (bid_events)",
    );
}

#[test]
fn key_mapping_source_field_validation() {
    let unqualified = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        key { user_id = sip; }
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        unqualified,
        &[auth_events_window(), output_window()],
        "key mapping `user_id`: source field must be qualified (alias.field)",
    );

    let unknown_alias = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        key { user_id = zzz.sip; }
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        unknown_alias,
        &[auth_events_window(), output_window()],
        "key mapping `user_id = zzz.sip`: alias `zzz` not declared in events",
    );

    let missing_field = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        key { user_id = e.bogus; }
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        missing_field,
        &[auth_events_window(), output_window()],
        "key mapping `user_id = e.bogus`: field `bogus` not found in window",
    );
}

#[test]
fn simple_key_type_mismatch_across_sources() {
    let schemas = vec![ip_sip_win(), chars_sip_win(), output_window()];
    let input = r#"
rule r {
    events { a : ip_sip  b : chars_sip }
    match<sip:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(input, &schemas, "match key `sip` type mismatch");
}

// ===========================================================================
// rules/limits.rs — limits validation branches
// ===========================================================================

#[test]
fn limits_unknown_key_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { bogus = 1; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "unknown limits key `bogus`",
    );
}

#[test]
fn limits_on_exceed_invalid_value_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { on_exceed = "bogus"; }
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "on_exceed value `bogus` invalid",
    );
}

#[test]
fn limits_max_throttle_invalid_formats() {
    let no_slash = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_throttle = "1000"; }
}
"#;
    assert_has_error(
        no_slash,
        &[auth_events_window(), output_window()],
        "max_throttle value `1000` must be in format count/unit",
    );

    let bad_unit = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_throttle = "1000/fortnight"; }
}
"#;
    assert_has_error(
        bad_unit,
        &[auth_events_window(), output_window()],
        "max_throttle unit `fortnight` invalid",
    );
}

#[test]
fn limits_max_memory_invalid_values() {
    let bad_suffix = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_memory = "100"; }
}
"#;
    assert_has_error(
        bad_suffix,
        &[auth_events_window(), output_window()],
        "max_memory value `100` must end with KB, MB, or GB",
    );

    let bad_prefix = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_memory = "abcMB"; }
}
"#;
    assert_has_error(
        bad_prefix,
        &[auth_events_window(), output_window()],
        "max_memory value `abcMB` must have a positive numeric prefix",
    );
}

#[test]
fn limits_valid_block_accepted() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits {
        max_memory = "512MB";
        max_instances = 2;
        max_throttle = "100/min";
        on_exceed = "drop_oldest";
    }
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}
