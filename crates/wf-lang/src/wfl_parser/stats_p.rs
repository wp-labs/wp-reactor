use winnow::combinator::{alt, cut_err, delimited, opt, preceded, separated};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{duration_value, ident, kw, number_literal, ws_skip};

use super::expr;

// ---------------------------------------------------------------------------
// stats clause
// ---------------------------------------------------------------------------
// `stats<30m[:fixed|session]> [group by (k1, k2, ...)] [tier f [ <b1, <b2 ... ]]
//  { measure; ... }`

pub(super) fn stats_clause_only(input: &mut &str) -> ModalResult<StatsClause> {
    kw("stats").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("<")).parse_next(input)?;

    let (duration, mode) = cut_err(stats_window_params).parse_next(input)?;
    cut_err(literal(">")).parse_next(input)?;
    ws_skip.parse_next(input)?;

    // Optional `group by (k1, k2, ...)`
    let keys: Vec<Expr> = opt(group_by_clause).parse_next(input)?.unwrap_or_default();

    // Optional `tier f [ <b1, <b2, ... ]` — 数值区间分档, 展开为 tier(f, b...) 桶键
    ws_skip.parse_next(input)?;
    let tier_key: Option<Expr> = opt(tier_clause).parse_next(input)?;
    let has_tier = tier_key.is_some();
    let keys = append_tier_key(keys, tier_key);

    // Output shape: `tier` implies columns (单行多列), else rows.
    let output_shape = if has_tier {
        StatsOutputShape::Columns
    } else {
        StatsOutputShape::Rows
    };

    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let measures = parse_measure_items(input)?;

    Ok(StatsClause {
        window: StatsWindow { duration, mode },
        keys,
        output_shape,
        measures,
    })
}

/// tier 键追加到 group-by 键末尾（`tier(f, b...)` 桶键展开）。
fn append_tier_key(keys: Vec<Expr>, tier_key: Option<Expr>) -> Vec<Expr> {
    match tier_key {
        Some(k) => keys.into_iter().chain(std::iter::once(k)).collect(),
        None => keys,
    }
}

/// `{ m1; m2; ... }` 内的度量列表：每条以 `;` 结尾（允许最后一条缺分号、紧跟 `}`）。
fn parse_measure_items(input: &mut &str) -> ModalResult<Vec<StatsMeasure>> {
    let mut measures: Vec<StatsMeasure> = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break; // 空块或已到结尾
        }
        let m = cut_err(stats_measure).parse_next(input)?;
        measures.push(m);
        ws_skip.parse_next(input)?;
        // 分号分隔; 无分号则要求下一个是 `}`
        let has_semi = opt(literal(";")).parse_next(input)?.is_some();
        ws_skip.parse_next(input)?;
        if !has_semi {
            cut_err(literal("}")).parse_next(input)?;
            break;
        }
    }
    Ok(measures)
}

fn stats_window_params(input: &mut &str) -> ModalResult<(std::time::Duration, StatsWindowMode)> {
    // duration[:mode]  — mode 缺省 fixed
    let dur = cut_err(duration_value).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let mode = opt(preceded(literal(":"), cut_err(window_mode_lit)))
        .parse_next(input)?
        .unwrap_or(StatsWindowMode::Fixed);
    Ok((dur, mode))
}

fn window_mode_lit(input: &mut &str) -> ModalResult<StatsWindowMode> {
    alt((
        kw("fixed").value(StatsWindowMode::Fixed),
        kw("session").value(StatsWindowMode::Session),
    ))
    .parse_next(input)
}

fn group_by_clause(input: &mut &str) -> ModalResult<Vec<Expr>> {
    kw("group").parse_next(input)?;
    ws_skip.parse_next(input)?;
    kw("by").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let keys: Vec<Expr> = cut_err(delimited(
        literal("("),
        separated(
            0..,
            preceded(ws_skip, expr::parse_expr),
            preceded(ws_skip, literal(",")),
        ),
        cut_err(preceded(ws_skip, literal(")"))),
    ))
    .parse_next(input)?;
    Ok(keys)
}

fn tier_clause(input: &mut &str) -> ModalResult<Expr> {
    kw("tier").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let field = cut_err(field_ref_lit).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("[")).parse_next(input)?;

    // bounds: `<b1, <b2, ...` (可空)
    let bounds = parse_tier_bounds(input)?;

    let mut args = vec![Expr::Field(field)];
    for b in bounds {
        args.push(Expr::Number(b));
    }
    Ok(Expr::FuncCall {
        qualifier: None,
        name: "tier".to_string(),
        args,
    })
}

/// `[` 后的区间界 `<b1, <b2 ...`（可空; 无逗号时要求紧接 `]`）。
fn parse_tier_bounds(input: &mut &str) -> ModalResult<Vec<f64>> {
    let mut bounds: Vec<f64> = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("]")).parse_next(input)?.is_some() {
            break;
        }
        let b = cut_err(tier_boundary).parse_next(input)?;
        bounds.push(b);
        ws_skip.parse_next(input)?;
        if opt(literal(",")).parse_next(input)?.is_none() {
            // 无逗号则要求 `]`
            ws_skip.parse_next(input)?;
            cut_err(literal("]")).parse_next(input)?;
            break;
        }
    }
    Ok(bounds)
}

/// 边界字面量：`<10000` / `<=1000000` —— 边界值取正数（`<` 语义；`<=` 由 checker 校验）。
fn tier_boundary(input: &mut &str) -> ModalResult<f64> {
    alt((
        preceded(literal("<="), cut_err(number_literal)),
        preceded(literal("<"), cut_err(number_literal)),
    ))
    .parse_next(input)
}

pub(super) fn stats_measure(input: &mut &str) -> ModalResult<StatsMeasure> {
    // b | agg(field) as label [where expr]
    let source_alias = cut_err(ident).parse_next(input)?.to_string();
    ws_skip.parse_next(input)?;
    cut_err(literal("|")).parse_next(input)?;
    ws_skip.parse_next(input)?;

    let (agg, field, arg) = cut_err(stats_agg).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(kw("as")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let label = cut_err(ident).parse_next(input)?.to_string();

    // Optional `where expr`
    ws_skip.parse_next(input)?;
    let where_expr = opt(preceded(
        kw("where"),
        cut_err(preceded(ws_skip, expr::parse_expr)),
    ))
    .parse_next(input)?;

    Ok(StatsMeasure {
        label,
        source_alias,
        where_expr,
        agg,
        field,
        arg,
    })
}

fn stats_agg(input: &mut &str) -> ModalResult<(StatsAgg, Option<FieldRef>, Option<u64>)> {
    let name = cut_err(ident).parse_next(input)?.to_string();
    match name.as_str() {
        "count" => Ok((StatsAgg::Count, None, None)),
        "sum" | "avg" | "min" | "max" | "distinct_count" => {
            let f = agg_field_parens(input)?;
            let agg = match name.as_str() {
                "sum" => StatsAgg::Sum,
                "avg" => StatsAgg::Avg,
                "min" => StatsAgg::Min,
                "max" => StatsAgg::Max,
                _ => StatsAgg::DistinctCount,
            };
            Ok((agg, Some(f), None))
        }
        "last" => {
            let f = agg_field_parens(input)?;
            Ok((StatsAgg::Last, Some(f), None))
        }
        "top" => {
            let (n, f) = top_arg_parens(input)?;
            Ok((StatsAgg::Top, Some(f), Some(n)))
        }
        _ => Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        )),
    }
}

/// `(field)` — 字段引用参数（sum/avg/min/max/distinct_count/last 共用）。
fn agg_field_parens(input: &mut &str) -> ModalResult<FieldRef> {
    ws_skip.parse_next(input)?;
    cut_err(delimited(
        literal("("),
        field_ref_lit,
        cut_err(preceded(ws_skip, literal(")"))),
    ))
    .parse_next(input)
}

/// `(N, field)` — top 的条数 + 字段参数。
fn top_arg_parens(input: &mut &str) -> ModalResult<(u64, FieldRef)> {
    ws_skip.parse_next(input)?;
    let (n, _, f) = cut_err(delimited(
        literal("("),
        (
            preceded(ws_skip, number_literal),
            preceded(ws_skip, literal(",")),
            cut_err(field_ref_lit),
        ),
        cut_err(preceded(ws_skip, literal(")"))),
    ))
    .parse_next(input)?;
    Ok((n as u64, f))
}

/// 字段引用: `b.price` / `b.bidder`（简版：Qualified）。入口跳前导空白——
/// `top(10, b.price)` 逗号后、`sum( b.price)` 括号内均允许空格。
fn field_ref_lit(input: &mut &str) -> ModalResult<FieldRef> {
    ws_skip.parse_next(input)?;
    let alias = cut_err(ident).parse_next(input)?.to_string();
    ws_skip.parse_next(input)?;
    if opt(literal(".")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let name = cut_err(ident).parse_next(input)?.to_string();
        Ok(FieldRef::Qualified(alias, name))
    } else {
        Ok(FieldRef::Simple(alias))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    fn stats(input: &str) -> StatsClause {
        let mut s = input;
        stats_clause_only
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("stats parse failed for {input:?}: {e:?}"))
    }

    #[test]
    fn stats_clause_basic_measure_variants_and_where() {
        let c = stats(
            "stats<30m:fixed> { b | count as n; b | sum(price) as s where price > 10; b | top(3, bidder) as t; b | last(ts) as l; }",
        );
        assert_eq!(c.window.mode, StatsWindowMode::Fixed);
        assert_eq!(c.measures.len(), 4);
        assert_eq!(c.measures[0].agg, StatsAgg::Count);
        assert!(c.measures[1].where_expr.is_some(), "where 尾随解析");
        assert_eq!(c.measures[2].agg, StatsAgg::Top);
        assert_eq!(c.measures[2].arg, Some(3));
        assert_eq!(c.measures[3].agg, StatsAgg::Last);
        assert_eq!(c.output_shape, StatsOutputShape::Rows);
        assert!(c.keys.is_empty());
    }

    #[test]
    fn stats_group_by_and_tier_shape() {
        let c = stats(
            "stats<10s:session> group by (e.sip, e.ip) tier e.count [ <100, <1000 ] { e | distinct_count(sip) as d; }",
        );
        assert_eq!(c.window.mode, StatsWindowMode::Session);
        assert_eq!(c.keys.len(), 3, "group-by 2 键 + tier 键");
        assert!(
            matches!(&c.keys[2], Expr::FuncCall { name, .. } if name == "tier"),
            "tier 展开为 tier(field, b...) 调用"
        );
        assert_eq!(
            c.output_shape,
            StatsOutputShape::Columns,
            "tier 隐含 Columns"
        );
        // 无 tier → Rows
        assert_eq!(
            stats("stats<10s> { e | count as n; }").output_shape,
            StatsOutputShape::Rows
        );
    }

    #[test]
    fn stats_measure_missing_semi_and_unknown_agg() {
        // 最后一条缺分号、紧跟 } 合法
        let mut s = "stats<10s> { e | count as n }";
        assert!(stats_clause_only.parse_next(&mut s).is_ok());
        // 空块在解析层合法（0 度量; 语义校验由编译层把关）
        let mut s2 = "stats<10s> { }";
        let c = stats_clause_only
            .parse_next(&mut s2)
            .unwrap_or_else(|e| panic!("{e:?}"));
        assert!(c.measures.is_empty(), "空块 → 0 度量");
        // 未知聚合名 → 错误
        let mut s3 = "stats<10s> { e | bogus(x) as n; }";
        assert!(
            stats_clause_only.parse_next(&mut s3).is_err(),
            "未知聚合拒绝"
        );
    }
}
