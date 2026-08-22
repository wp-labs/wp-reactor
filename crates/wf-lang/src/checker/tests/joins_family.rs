// P1 join 算子族 checker 校验测试：over ≥ 跨度（D3）、emit_at 触发点、reduce 字段、
// asof within 互斥、reduce 标签冲突与 object 解析。
// 设计文档：docs/design/join-family-design.md §3/§4/§5.4。

use std::time::Duration;

use super::*;
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};

fn auction_events_window() -> WindowSchema {
    WindowSchema {
        name: "auction_events".to_string(),
        streams: vec!["auction".to_string()],
        time_field: Some("dateTime".to_string()),
        over: Duration::from_secs(1800), // 30m
        fields: vec![
            FieldDef {
                name: "id".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "seller".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "dateTime".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
            FieldDef {
                name: "expires".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
            FieldDef {
                name: "category".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
        ],
    }
}

fn bid_events_window() -> WindowSchema {
    WindowSchema {
        name: "bid_events".to_string(),
        streams: vec!["bid".to_string()],
        time_field: Some("dateTime".to_string()),
        over: Duration::from_secs(1800), // 30m
        fields: vec![
            FieldDef {
                name: "auction".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "bidder".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "price".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "dateTime".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
            FieldDef {
                name: "channel".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
        ],
    }
}

fn person_events_window() -> WindowSchema {
    WindowSchema {
        name: "person_events".to_string(),
        streams: vec!["person".to_string()],
        time_field: Some("dateTime".to_string()),
        over: Duration::from_secs(1800), // 30m
        fields: vec![
            FieldDef {
                name: "id".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "dateTime".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    }
}

fn out_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![
            ("id", bt(BaseType::Digit)),
            ("winner_id", bt(BaseType::Digit)),
            ("detail", bt(BaseType::Chars)),
        ],
    )
}

fn schemas() -> Vec<WindowSchema> {
    vec![
        auction_events_window(),
        bid_events_window(),
        person_events_window(),
        out_window(),
    ]
}

/// Q9 形态完整通过：reduce + within 行内界 + as label + emit at + `winner.bidder` 引用。
#[test]
fn q9_shape_checks_clean() {
    let input = r#"
rule q9 {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce maxrow(price) tie(dateTime asc)
        within [a.dateTime, a.expires]
        on a.id == bid_events.auction as winner
        emit at a.expires
    entity(digit, a.id)
    yield out (id = a.id, winner_id = winner.bidder, detail = fmt("w {}", winner.bidder))
}
"#;
    assert_no_errors(input, &schemas());
}

/// Q8 形态完整通过：上开界 + bucket_end 表达式 + emit at 同表达式。
#[test]
fn q8_shape_checks_clean() {
    let input = r#"
rule q8 {
    events { p : auction_events }
    on each p -> score(1.0)
    join bid_events within [p.dateTime, <bucket_end(p.dateTime, 10s)]
        on p.id == bid_events.auction
        emit at bucket_end(p.dateTime, 10s)
    entity(digit, p.id)
    yield out (id = p.id)
}
"#;
    assert_no_errors(input, &schemas());
}

/// 常量界：右窗 over ≥ 跨度通过。
#[test]
fn within_constant_span_within_over_ok() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [1s, 5s] on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_no_errors(input, &schemas());
}

/// 常量界：跨度超右窗 over（D3）→ 错误。
#[test]
fn within_span_exceeds_over_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [10m, 41m] on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas(), "within 区间跨度");
}

/// 常量界：lo > hi → 错误。
#[test]
fn within_lo_gt_hi_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [10s, 1s] on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas(), "下界必须 ≤ 上界");
}

/// lo/hi 类型不一致（时长 vs 表达式）→ 错误。
#[test]
fn within_mixed_bound_types_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [1s, a.expires] on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas(), "必须同为相对时长或同为绝对时间表达式");
}

/// `emit at` 缺 `within` → 错误。
#[test]
fn emit_at_without_within_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events on a.id == bid_events.auction
        emit at a.expires
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas(), "需要 `within` 区间");
}

/// `emit at` + 相对时长上界 → 错误。
#[test]
fn emit_at_with_duration_upper_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [1s, 5s] on a.id == bid_events.auction
        emit at a.expires
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas(), "要求 within 上界为绝对时间表达式");
}

/// `emit at` 字段 ≠ within 上界字段 → 错误。
#[test]
fn emit_at_field_below_upper_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [a.dateTime, a.expires] on a.id == bid_events.auction
        emit at a.dateTime
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas(), "必须 ≥ within 上界");
}

/// reduce 度量字段不在右窗 → 错误。
#[test]
fn reduce_measure_field_not_found_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events reduce maxrow(nonexistent) on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(
        input,
        &schemas(),
        "reduce measure field `nonexistent` not found",
    );
}

/// reduce tie 字段不在右窗 → 错误。
#[test]
fn reduce_tie_field_not_found_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events reduce maxrow(price) tie(nope asc) on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas(), "reduce tie field `nope` not found");
}

/// top(0, ...) → 错误。
#[test]
fn reduce_top_zero_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events reduce top(0, price) on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas(), "top(N) N must be ≥ 1");
}

/// `as label` 与事件别名冲突 → 错误。
#[test]
fn reduce_label_conflicts_with_alias_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events reduce maxrow(price) on a.id == bid_events.auction as a
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(
        input,
        &schemas(),
        "reduce label `a` conflicts with an event/window alias",
    );
}

/// asof `within DUR` 与 interval `within [...]` 互斥 → 错误。
#[test]
fn asof_within_dur_plus_interval_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events asof within 5m within [1s, 2s] on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas(), "同一时间谓词只能声明一次");
}

/// interval 需要右窗 time field。
#[test]
fn within_requires_time_field_rejected() {
    // out 为 yield-only 窗口（无 time_field）——这里用无时间窗做 join 右窗
    let schemas = vec![
        auction_events_window(),
        make_output_window(
            "static_tbl",
            vec![("id", bt(BaseType::Digit)), ("val", bt(BaseType::Chars))],
        ),
        out_window(),
    ];
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join static_tbl within [1s, 5s] on a.id == static_tbl.id
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas, "but target window has no time field");
}

/// review 修复：`emit at expires`（裸名）与 `within [a.dateTime, a.expires]` 上界同字段——
/// 不得误报「必须 ≥ within 上界」。
#[test]
fn emit_at_bare_field_same_as_qualified_upper_ok() {
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events within [a.dateTime, a.expires] on a.id == bid_events.auction
        emit at expires
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_no_errors(input, &schemas());
}

/// review 修复：同一规则内多个 join 的 `as label` 必须唯一。
#[test]
fn duplicate_reduce_label_across_joins_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events reduce maxrow(price) on a.id == bid_events.auction as winner
    join person_events reduce last(id) on a.seller == person_events.id as winner
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas(), "is duplicated across joins");
}

/// review 修复：within 界表达式只能引用驱动事件字段（左行），不能引用 join 右窗。
#[test]
fn within_bound_references_right_window_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [bid_events.dateTime, a.expires] on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas(), "只能引用驱动事件字段（左行）");
}

/// review 修复：emit at 表达式不能引用 join 右窗。
#[test]
fn emit_at_references_right_window_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [a.dateTime, a.expires] on a.id == bid_events.auction
        emit at bid_events.dateTime
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(
        input,
        &schemas(),
        "`emit at` 表达式 只能引用驱动事件字段（左行）",
    );
}

/// review 修复：pipeline 规则 final stage 的 reduce 标签应注册（final_scope 手工建，
/// 不走 build_scope）——`winner.bidder` 在 final yield 须可解析。
#[test]
fn pipeline_final_stage_reduce_label_resolves() {
    let input = r#"
rule pipe_label {
  events { a : auction_events }
  match<id:10m> { on event { a | count >= 1; } }
  |> match<id:10m> { on event { _in | count >= 1; } } -> score(1.0)
  join bid_events reduce maxrow(price) on _in.id == bid_events.auction as winner
  entity(digit, _in.id)
  yield out (id = _in.id, winner_id = winner.bidder)
}
"#;
    assert_no_errors(input, &schemas());
}

/// review 修复（P3）：deferred join（emit at）v1 仅支持 on-each 驱动——
/// match 形态规则带 emit_at 报错（否则 rule_task 无挂起承载点，join 静默无输出）。
#[test]
fn deferred_emit_at_match_shape_rejected() {
    let input = r#"
rule r {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events within [a.dateTime, a.expires] on a.id == bid_events.auction
        emit at a.expires
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    assert_has_error(input, &schemas(), "仅支持 on-each 驱动形态");
}

/// on-each 形态 + emit_at 通过（P3 支持形态）。
#[test]
fn deferred_emit_at_each_shape_ok() {
    let input = r#"
rule r {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce maxrow(price) within [a.dateTime, a.expires]
        on a.id == bid_events.auction as winner
        emit at a.expires
    entity(digit, a.id)
    yield out (id = a.id, winner_id = winner.bidder)
}
"#;
    assert_no_errors(input, &schemas());
}
