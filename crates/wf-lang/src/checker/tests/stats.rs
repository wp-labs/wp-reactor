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
