use winnow::combinator::{alt, cut_err, opt, separated};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{kw, nonneg_integer, ws_skip};

use super::expr;

// ---------------------------------------------------------------------------
// conv clause
// ---------------------------------------------------------------------------

/// `conv { conv_chain; ... }`
pub(super) fn conv_clause(input: &mut &str) -> ModalResult<ConvClause> {
    kw("conv").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'{' after conv",
        )))
        .parse_next(input)?;

    let mut chains = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        chains.push(parse_conv_chain_cut(input)?);
    }

    if chains.is_empty() {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }

    Ok(ConvClause { chains })
}

/// 单链（cut + 诊断上下文）——`conv_clause` 循环体。
fn parse_conv_chain_cut(input: &mut &str) -> ModalResult<ConvChain> {
    cut_err(conv_chain)
        .context(StrContext::Expected(StrContextValue::Description(
            "conv chain",
        )))
        .parse_next(input)
}

/// `conv_step { "|" conv_step } ";"`
fn conv_chain(input: &mut &str) -> ModalResult<ConvChain> {
    let steps: Vec<ConvStep> = separated(
        1..,
        (ws_skip, conv_step).map(|(_, s)| s),
        (ws_skip, literal("|")),
    )
    .parse_next(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal(";"))
        .context(StrContext::Expected(StrContextValue::Description(
            "';' after conv chain",
        )))
        .parse_next(input)?;

    Ok(ConvChain { steps })
}

/// `("sort" | "top" | "top_ties" | "dedup" | "where") "(" args ")"`
/// 关键字分派（尝试序 = 文档序; `kw` 带词边界, `top` 不会误吞 `top_ties`）。
fn conv_step(input: &mut &str) -> ModalResult<ConvStep> {
    ws_skip.parse_next(input)?;
    alt((
        (kw("sort"), parse_sort).map(|(_, s)| s),
        (kw("top"), parse_top).map(|(_, s)| s),
        (kw("top_ties"), parse_top_ties).map(|(_, s)| s),
        (kw("dedup"), parse_dedup).map(|(_, s)| s),
        (kw("where"), parse_where).map(|(_, s)| s),
    ))
    .parse_next(input)
}

/// `"(" sort_key { "," sort_key } ")"`
fn parse_sort(input: &mut &str) -> ModalResult<ConvStep> {
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let keys = parse_sort_keys(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(ConvStep::Sort(keys))
}

/// `sort_key { "," sort_key }`（逗号分隔列表）。
fn parse_sort_keys(input: &mut &str) -> ModalResult<Vec<SortKey>> {
    separated(
        1..,
        (ws_skip, sort_key).map(|(_, k)| k),
        (ws_skip, literal(",")),
    )
    .parse_next(input)
}

/// `["-"] expr`
fn sort_key(input: &mut &str) -> ModalResult<SortKey> {
    ws_skip.parse_next(input)?;
    let descending = opt(literal("-")).parse_next(input)?.is_some();
    ws_skip.parse_next(input)?;
    let e = cut_err(expr::parse_expr).parse_next(input)?;
    Ok(SortKey {
        expr: e,
        descending,
    })
}

/// `"(" integer ")"`
fn parse_top(input: &mut &str) -> ModalResult<ConvStep> {
    let n = parse_paren_n(input, "positive integer for top(N)")?;
    Ok(ConvStep::Top(n))
}

/// `"(" integer ")"` — RANK 语义（并列全输出），要求前导 sort。
fn parse_top_ties(input: &mut &str) -> ModalResult<ConvStep> {
    let n = parse_paren_n(input, "positive integer for top_ties(N)")?;
    Ok(ConvStep::TopTies(n))
}

/// `"(" 非负整数 ")"`（`top(N)`/`top_ties(N)` 共用; 错误描述按调用方传参）。
fn parse_paren_n(input: &mut &str, desc: &'static str) -> ModalResult<u64> {
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let n = cut_err(nonneg_integer)
        .context(StrContext::Expected(StrContextValue::Description(desc)))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(n as u64)
}

/// `"(" expr ")"`
fn parse_dedup(input: &mut &str) -> ModalResult<ConvStep> {
    let e = parse_paren_expr(input)?;
    Ok(ConvStep::Dedup(e))
}

/// `"(" expr ")"`
fn parse_where(input: &mut &str) -> ModalResult<ConvStep> {
    let e = parse_paren_expr(input)?;
    Ok(ConvStep::Where(e))
}

/// `"(" 表达式 ")"`（`dedup(...)`/`where(...)` 共用）。
fn parse_paren_expr(input: &mut &str) -> ModalResult<Expr> {
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let e = cut_err(expr::parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    fn step(input: &str) -> ConvStep {
        let mut s = input;
        conv_step
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("conv_step parse failed for {input:?}: {e:?}"))
    }

    #[test]
    fn conv_step_dispatches_all_keywords() {
        // sort: 降序/升序键列表
        match step("sort(-score, ts)") {
            ConvStep::Sort(keys) => {
                assert_eq!(keys.len(), 2);
                assert!(keys[0].descending);
                assert!(!keys[1].descending);
            }
            other => panic!("期望 Sort, 得到 {other:?}"),
        }
        assert!(matches!(step("top(10)"), ConvStep::Top(10)));
        // top 词边界: 不得误吞 top_ties
        assert!(matches!(step("top_ties(3)"), ConvStep::TopTies(3)));
        assert!(matches!(step("dedup(sip)"), ConvStep::Dedup(_)));
        assert!(matches!(step("where(count > 3)"), ConvStep::Where(_)));
        // 未知关键字 → 错误
        assert!(conv_step.parse_next(&mut "bogus(1)").is_err());
    }

    #[test]
    fn top_and_top_ties_share_paren_integer_with_own_context() {
        // 负数被拒（nonneg_integer + cut）
        assert!(conv_step.parse_next(&mut "top(-1)").is_err());
    }

    #[test]
    fn conv_clause_chains_and_empty_block_rejected() {
        let mut s = "conv { sort(-score) ; where(count > 5) ; }";
        let c = conv_clause
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("conv parse failed: {e:?}"));
        assert_eq!(c.chains.len(), 2);
        assert!(matches!(&c.chains[0].steps[0], ConvStep::Sort(_)));
        assert!(matches!(&c.chains[1].steps[0], ConvStep::Where(_)));
        assert!(s.is_empty());

        // 空块 → 语法错误
        let mut empty = "conv { }";
        assert!(
            conv_clause.parse_next(&mut empty).is_err(),
            "空 conv 块拒绝"
        );
    }
}
