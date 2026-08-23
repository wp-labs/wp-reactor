//! stats 子句解析测试。

use crate::ast::{StatsAgg, StatsOutputShape, StatsWindowMode};
use crate::wfl_parser::parse_wfl;

fn parse_stats_rule(src: &str) -> crate::ast::RuleDecl {
    // parse_wfl 只解析 wfl 规则体, use "xxx.wfs" 引用外部 schema
    let wfl = format!("use \"nexmark.wfs\"\n\n{}\n", src);
    let file = parse_wfl(&wfl).unwrap_or_else(|e| panic!("parse failed: {e}"));
    assert_eq!(file.rules.len(), 1, "应恰好一条规则");
    file.rules.into_iter().next().unwrap()
}

#[test]
fn stats_empty_key_count() {
    let rule = parse_stats_rule(
        r#"
rule q15_bidding_stats {
    events { b : bid_events }
    stats<30m:fixed> {
        b | count as total_bids;
    }
    entity(digit, 1)
    yield alerts ( id = 1, alert_type = "q15", detail = "x" )
}
"#,
    );
    let stats = rule.stats_clause.expect("应有 stats_clause");
    assert_eq!(stats.window.mode, StatsWindowMode::Fixed);
    assert_eq!(stats.window.duration.as_secs(), 1800);
    assert!(stats.keys.is_empty(), "空键规则无 keys");
    assert_eq!(stats.output_shape, StatsOutputShape::Rows);
    assert_eq!(stats.measures.len(), 1);
    assert_eq!(stats.measures[0].label, "total_bids");
    assert_eq!(stats.measures[0].agg, StatsAgg::Count);
}

#[test]
fn stats_tier_columns() {
    let rule = parse_stats_rule(
        r#"
rule q15_tier {
    events { b : bid_events }
    stats<30m:fixed> tier b.price [ <10000, <1000000 ] {
        b | count as total_bids;
        b | distinct_count(b.bidder) as total_bidders;
    }
    entity(digit, 1)
    yield alerts ( id = 1, alert_type = "q15", detail = "x" )
}
"#,
    );
    let stats = rule.stats_clause.expect("应有 stats_clause");
    assert_eq!(stats.keys.len(), 1, "tier 生成 1 个桶键");
    let crate::ast::Expr::FuncCall { name, args, .. } = &stats.keys[0] else {
        panic!("tier 应编译为 FuncCall");
    };
    assert_eq!(name, "tier");
    assert_eq!(args.len(), 3, "tier(field, b1, b2)");
    assert_eq!(
        stats.output_shape,
        StatsOutputShape::Columns,
        "tier 隐含列展开"
    );
    assert_eq!(stats.measures.len(), 2);
    assert_eq!(stats.measures[1].agg, StatsAgg::DistinctCount);
}

#[test]
fn stats_group_by() {
    let rule = parse_stats_rule(
        r#"
rule q16_stats {
    events { b : bid_events }
    stats<30m:fixed> group by (b.channel) {
        b | count as bids;
    }
    entity(digit, 1)
    yield alerts ( id = 1, alert_type = "q16", detail = "x" )
}
"#,
    );
    let stats = rule.stats_clause.expect("应有 stats_clause");
    assert_eq!(stats.keys.len(), 1);
    assert_eq!(stats.output_shape, StatsOutputShape::Rows);
}

#[test]
fn stats_where_filter() {
    let rule = parse_stats_rule(
        r#"
rule q15_where {
    events { b : bid_events }
    stats<30m:fixed> {
        b | count as google_bids where b.channel == "google";
    }
    entity(digit, 1)
    yield alerts ( id = 1, alert_type = "q15", detail = "x" )
}
"#,
    );
    let stats = rule.stats_clause.expect("应有 stats_clause");
    assert!(stats.measures[0].where_expr.is_some(), "where 应被解析");
}

#[test]
fn stats_compound_group_by() {
    // Q18 形状: 复合键 group by (b.bidder, b.auction)——修复: 逗号后空白由 item
    // 的 ws_skip 消费（separator 为裸逗号, 闭括号跳空白）。
    let rule = parse_stats_rule(
        r#"
rule q18_stats {
    events { b : bid_events }
    stats<30m:fixed> group by (b.bidder, b.auction) {
        b | count as n;
    }
    entity(digit, 1)
    yield alerts ( id = 1, alert_type = "q18", detail = "x" )
}
"#,
    );
    let stats = rule.stats_clause.expect("应有 stats_clause");
    assert_eq!(stats.keys.len(), 2, "复合键 2 个桶键");
    // 键序保持: bidder, auction
    assert!(matches!(&stats.keys[0], crate::ast::Expr::Field(_)));
    assert!(matches!(&stats.keys[1], crate::ast::Expr::Field(_)));
}

#[test]
fn stats_last_top_measures() {
    // Q18/Q19 形状: last(field) / top(N, field)（逗号后带空白, 修复点）
    let rule = parse_stats_rule(
        r#"
rule q19_stats {
    events { b : bid_events }
    stats<30m:fixed> group by (b.auction) {
        b | top(10, b.price) as top_price;
        b | last(b.price) as last_price;
    }
    entity(digit, 1)
    yield alerts ( id = 1, alert_type = "q19", detail = "x" )
}
"#,
    );
    let stats = rule.stats_clause.expect("应有 stats_clause");
    assert_eq!(stats.measures.len(), 2);
    assert_eq!(stats.measures[0].agg, StatsAgg::Top);
    assert_eq!(stats.measures[0].arg, Some(10), "top N");
    assert_eq!(stats.measures[1].agg, StatsAgg::Last);
    // 字段引用解析为 Qualified
    assert!(stats.measures[0].field.is_some());
    assert!(stats.measures[1].field.is_some());
}

#[test]
fn stats_ws_tolerant_comma_and_parens() {
    // 空格边界回归（P4 review 修复）: 逗号前/后空白、闭括号前空白均须解析——
    // `group by (a , b)` 与 `top(10 , b.price)`、`sum( b.price )` 不再报错。
    let rule = parse_stats_rule(
        r#"
rule q18_ws {
    events { b : bid_events }
    stats<30m:fixed> group by ( b.bidder , b.auction ) {
        b | top( 10 , b.price ) as top_price;
        b | last( b.price ) as last_price;
    }
    entity(digit, 1)
    yield alerts ( id = 1, alert_type = "q18", detail = "x" )
}
"#,
    );
    let stats = rule.stats_clause.expect("应有 stats_clause");
    assert_eq!(stats.keys.len(), 2, "逗号前空格不破坏复合键");
    assert_eq!(stats.measures.len(), 2);
    assert_eq!(stats.measures[0].agg, StatsAgg::Top);
    assert_eq!(stats.measures[0].arg, Some(10));
    assert_eq!(stats.measures[1].agg, StatsAgg::Last);
    // 字段名解析正确（前导/内部空白被吞掉）
    let crate::ast::FieldRef::Qualified(alias, name) = stats.measures[0].field.as_ref().unwrap()
    else {
        panic!("字段应为 Qualified");
    };
    assert_eq!(alias, "b");
    assert_eq!(name, "price");
}

#[test]
fn stats_group_by_then_tier_with_whitespace() {
    // B2 回归: `group by (...)` 与 `tier` 之间必须允许空白（此前仅零空格
    // `)tier` 可解析; `) tier` 因 tier_clause 前缺 ws_skip 而失败）。
    let rule = parse_stats_rule(
        r#"
rule q19_tier_group {
    events { b : bid_events }
    stats<30m:fixed> group by (b.channel) tier b.price [ <10000, <1000000 ] {
        b | count as bids;
    }
    entity(digit, 1)
    yield alerts ( id = 1, alert_type = "q19", detail = "x" )
}
"#,
    );
    let stats = rule.stats_clause.expect("应有 stats_clause");
    assert_eq!(stats.keys.len(), 2, "group-by 键 + tier 键");
    assert_eq!(
        stats.output_shape,
        StatsOutputShape::Columns,
        "tier 隐含列展开"
    );
    assert!(matches!(
        &stats.keys[0],
        crate::ast::Expr::Field(crate::ast::FieldRef::Qualified(a, f)) if a == "b" && f == "channel"
    ));
    let crate::ast::Expr::FuncCall { name, .. } = &stats.keys[1] else {
        panic!("tier 应编译为 FuncCall");
    };
    assert_eq!(name, "tier");
}
