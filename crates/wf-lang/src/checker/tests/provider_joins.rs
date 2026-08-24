// P4 side input：provider/静态窗口 join 校验。
// 设计文档：docs/design/join-family-design.md §7/§8 P4——provider 窗口（side input）
// 无 stream/time/over，v1 支持 snapshot（及缺省 inner）与 **anti** join；
// anti 是纯键存在性否定不依赖时间（白名单排除，Q21 形状）；
// asof/interval/reduce/deferred 对无时序静态表无意义。

use std::time::Duration;

use super::*;
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};

/// provider 窗口经 `StaticWindowSchema::to_flow_schema()` 的投影：
/// 无 stream、无 time 字段、over = 0。
fn provider_schema() -> WindowSchema {
    WindowSchema {
        name: "person_table".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::ZERO,
        fields: vec![
            FieldDef {
                name: "id".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "state".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
        ],
    }
}

fn schemas() -> Vec<WindowSchema> {
    vec![
        provider_schema(),
        make_window(
            "bid_events",
            vec!["bid"],
            vec![
                ("bidder", bt(BaseType::Digit)),
                ("price", bt(BaseType::Digit)),
            ],
        ),
        make_output_window("out", vec![("id", bt(BaseType::Digit))]),
    ]
}

/// Q13 形态：bid ⋈ person 静态表 snapshot join 通过（side input 主用例）。
#[test]
fn provider_snapshot_join_clean() {
    let input = r#"
rule r {
    events { b : bid_events }
    on each b -> score(1.0)
    join person_table snapshot on b.bidder == person_table.id
    entity(digit, b.bidder)
    yield out (id = b.bidder)
}
"#;
    assert_no_errors(input, &schemas());
}

/// 缺省 inner（无 mode 关键字）同样允许——静态表上「存在则富化」等价 snapshot。
#[test]
fn provider_inner_join_clean() {
    let input = r#"
rule r {
    events { b : bid_events }
    on each b -> score(1.0)
    join person_table on b.bidder == person_table.id
    entity(digit, b.bidder)
    yield out (id = b.bidder)
}
"#;
    assert_no_errors(input, &schemas());
}

/// asof 需要右窗 time 字段——静态表没有。
#[test]
fn provider_asof_rejected() {
    let input = r#"
rule r {
    events { b : bid_events }
    on each b -> score(1.0)
    join person_table asof within 10s on b.bidder == person_table.id
    entity(digit, b.bidder)
    yield out (id = b.bidder)
}
"#;
    assert_has_error(input, &schemas(), "provider/静态窗口");
}

/// interval（within）需要右窗 time 字段——静态表没有。
#[test]
fn provider_within_rejected() {
    let input = r#"
rule r {
    events { b : bid_events }
    on each b -> score(1.0)
    join person_table within 10s on b.bidder == person_table.id
    entity(digit, b.bidder)
    yield out (id = b.bidder)
}
"#;
    assert_has_error(input, &schemas(), "provider/静态窗口");
}

/// anti 对无时序静态表**有意义**（2026-08-24 放开）：anti 是纯键存在性否定
/// （`join_lookup` → 有匹配丢、无匹配留），不依赖时间——静态表白名单排除是
/// 标准用例（NEXMark Q21 形状），provider `join_lookup` 已有 O(1) 行索引。
#[test]
fn provider_anti_join_clean() {
    let input = r#"
rule r {
    events { b : bid_events }
    on each b -> score(1.0)
    join person_table anti on b.bidder == person_table.id
    entity(digit, b.bidder)
    yield out (id = b.bidder)
}
"#;
    assert_no_errors(input, &schemas());
}

/// reduce 归约 v1 不支持静态表。
#[test]
fn provider_reduce_rejected() {
    let input = r#"
rule r {
    events { b : bid_events }
    on each b -> score(1.0)
    join person_table reduce maxrow(state) on b.bidder == person_table.id
    entity(digit, b.bidder)
    yield out (id = b.bidder)
}
"#;
    assert_has_error(input, &schemas(), "provider/静态窗口");
}

/// emit at（deferred）需要窗口生命周期——静态表没有。
#[test]
fn provider_emit_at_rejected() {
    let input = r#"
rule r {
    events { b : bid_events }
    on each b -> score(1.0)
    join person_table within [b.price, b.price] on b.bidder == person_table.id
        emit at b.price
    entity(digit, b.bidder)
    yield out (id = b.bidder)
}
"#;
    let errors = check_errors(input, &schemas());
    assert!(
        errors.iter().any(|e| e.contains("provider/静态窗口")),
        "expected static-window error, got: {:?}",
        errors
    );
}

/// 流式窗口不受影响：asof/interval/reduce 校验路径保持原行为（回归锚点）。
#[test]
fn flow_window_joins_unaffected() {
    let input = r#"
rule r {
    events { b : bid_events }
    on each b -> score(1.0)
    join person_table asof within 10s on b.bidder == person_table.id
    entity(digit, b.bidder)
    yield out (id = b.bidder)
}
"#;
    // person_table 是静态表——错误信息应指明静态限制而不是泛泛的“无 time field”
    assert_has_error(input, &schemas(), "provider/静态窗口");
}
