use winnow::combinator::{cut_err, opt, separated};
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
        let chain = cut_err(conv_chain)
            .context(StrContext::Expected(StrContextValue::Description(
                "conv chain",
            )))
            .parse_next(input)?;
        chains.push(chain);
    }

    if chains.is_empty() {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }

    Ok(ConvClause { chains })
}

/// `conv_step { "|" conv_step } ";"`
fn conv_chain(input: &mut &str) -> ModalResult<ConvChain> {
    let steps: Vec<ConvStep> =
        separated(1.., (ws_skip, conv_step).map(|(_, s)| s), (ws_skip, literal("|")))
            .parse_next(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal(";"))
        .context(StrContext::Expected(StrContextValue::Description(
            "';' after conv chain",
        )))
        .parse_next(input)?;

    Ok(ConvChain { steps })
}

/// `("sort" | "top" | "dedup" | "where") "(" args ")"`
fn conv_step(input: &mut &str) -> ModalResult<ConvStep> {
    ws_skip.parse_next(input)?;

    // Try each keyword
    if opt(kw("sort")).parse_next(input)?.is_some() {
        return parse_sort(input);
    }
    if opt(kw("top")).parse_next(input)?.is_some() {
        return parse_top(input);
    }
    if opt(kw("dedup")).parse_next(input)?.is_some() {
        return parse_dedup(input);
    }
    if opt(kw("where")).parse_next(input)?.is_some() {
        return parse_where(input);
    }

    Err(winnow::error::ErrMode::Backtrack(
        winnow::error::ContextError::new(),
    ))
}

/// `"(" sort_key { "," sort_key } ")"`
fn parse_sort(input: &mut &str) -> ModalResult<ConvStep> {
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;

    let keys: Vec<SortKey> =
        separated(1.., (ws_skip, sort_key).map(|(_, k)| k), (ws_skip, literal(",")))
            .parse_next(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(ConvStep::Sort(keys))
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
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let n = cut_err(nonneg_integer)
        .context(StrContext::Expected(StrContextValue::Description(
            "positive integer for top(N)",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(ConvStep::Top(n as u64))
}

/// `"(" expr ")"`
fn parse_dedup(input: &mut &str) -> ModalResult<ConvStep> {
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let e = cut_err(expr::parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(ConvStep::Dedup(e))
}

/// `"(" expr ")"`
fn parse_where(input: &mut &str) -> ModalResult<ConvStep> {
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let e = cut_err(expr::parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(ConvStep::Where(e))
}
