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
