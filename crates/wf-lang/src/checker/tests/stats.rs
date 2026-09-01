use super::*;

// =========================================================================
// stats 形态 stat.value(final(label)) 校验（P1 步骤④b）
// =========================================================================

#[test]
fn stats_rule_yield_final_label_matches_measure() {
    let input = r#"
rule stats_r {
    events { a : auth_events }
    stats<10s:fixed> {
        a | count as total;
        a | distinct_count(a.sip) as uniq;
    }
    entity(digit, 1)
    yield out (y = fmt("{} {}", stat.value(final(total)), stat.value(final(uniq))))
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn stats_rule_yield_unknown_final_label_rejected() {
    let input = r#"
rule stats_r {
    events { a : auth_events }
    stats<10s:fixed> {
        a | count as total;
    }
    entity(digit, 1)
    yield out (n = stat.value(final(nope)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "references unknown step label `nope`",
    );
}

#[test]
fn stats_rule_yield_trigger_label_rejected() {
    // trigger(...) 是事件阶段标签; stats 度量全部是 Close 阶段 → 必须报错
    let input = r#"
rule stats_r {
    events { a : auth_events }
    stats<10s:fixed> {
        a | count as total;
    }
    entity(digit, 1)
    yield out (n = stat.value(trigger(total)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "requires step label `total` to come from on event",
    );
}

// =========================================================================
// stats 度量校验（review 补充）: source alias / 字段 / where 类型
// =========================================================================

#[test]
fn stats_measure_unknown_field_rejected() {
    let input = r#"
rule stats_r {
    events { a : auth_events }
    stats<10s:fixed> {
        a | distinct_count(a.nope) as uniq;
    }
    entity(digit, 1)
    yield out (y = "x")
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "stats measure `uniq`",
    );
}

#[test]
fn stats_measure_where_unknown_field_rejected() {
    let input = r#"
rule stats_r {
    events { a : auth_events }
    stats<10s:fixed> {
        a | count as total where a.nope < 100;
    }
    entity(digit, 1)
    yield out (y = "x")
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "stats measure `total` where",
    );
}

#[test]
fn stats_measure_where_non_bool_rejected() {
    let input = r#"
rule stats_r {
    events { a : auth_events }
    stats<10s:fixed> {
        a | count as total where a.count + 1;
    }
    entity(digit, 1)
    yield out (y = "x")
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "where expression must be bool",
    );
}

#[test]
fn stats_measure_unknown_source_alias_rejected() {
    let input = r#"
rule stats_r {
    events { a : auth_events }
    stats<10s:fixed> {
        z | count as total;
    }
    entity(digit, 1)
    yield out (y = "x")
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "source `z` is not a declared event alias",
    );
}

#[test]
fn stats_measure_valid_fields_and_where_ok() {
    // 合法: 字段存在 + where 为 bool 比较 + where 字段存在
    let input = r#"
rule stats_r {
    events { a : auth_events }
    stats<10s:fixed> {
        a | count as total where a.count < 100;
        a | distinct_count(a.sip) as uniq;
        a | sum(a.count) as sum_count where a.action == "login";
    }
    entity(digit, 1)
    yield out (y = "x")
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

/// stats 规则 + let 派生字段（2026-08-31，issue #79）：stats 未接入 per-event
/// let 求值，checker 显式报错而非静默忽略。
#[test]
fn stats_rule_with_lets_rejected() {
    let input = r#"
rule stats_r {
    events { a : auth_events }
    let tenant = first(a.sip)
    stats<10s:fixed> {
        a | count as total;
    }
    entity(digit, 1)
    yield out (y = "x")
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "stats 规则暂不支持 `let` 派生字段",
    );
}

/// match 规则 + let 派生字段（2026-08-31，issue #79）：let 求值已接入
/// match/close 路径，yield 引用 let 名应通过 checker。
#[test]
fn match_rule_with_lets_ok() {
    let input = r#"
rule r {
    events { a : auth_events }
    let tenant = first(a.sip)
    let dedup = join_by("|", tenant, "x")
    match<a.sip:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(chars, tenant)
    yield out (y = dedup)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}
