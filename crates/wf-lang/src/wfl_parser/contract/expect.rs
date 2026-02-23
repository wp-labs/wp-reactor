use winnow::combinator::{alt, cut_err};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{kw, nonneg_integer, number_literal, quoted_string, ws_skip};

use super::cmp_op;
use super::super::expr;

// ---------------------------------------------------------------------------
// expect block
// ---------------------------------------------------------------------------

/// `expect { hits cmp NUMBER; ... hit[i].assert; ... }`
pub(super) fn expect_block(input: &mut &str) -> ModalResult<Vec<ExpectStmt>> {
    kw("expect").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let mut stmts = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if input.starts_with('}') {
            break;
        }
        let stmt = cut_err(expect_stmt)
            .context(StrContext::Expected(StrContextValue::Description(
                "expect statement (hits or hit[i].assert)",
            )))
            .parse_next(input)?;
        stmts.push(stmt);
    }

    cut_err(literal("}")).parse_next(input)?;
    Ok(stmts)
}

fn expect_stmt(input: &mut &str) -> ModalResult<ExpectStmt> {
    alt((expect_hits, expect_hit_assert)).parse_next(input)
}

/// `hits cmp_op INTEGER;`
fn expect_hits(input: &mut &str) -> ModalResult<ExpectStmt> {
    kw("hits").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let cmp = cut_err(cmp_op).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let count = cut_err(nonneg_integer)
        .context(StrContext::Expected(StrContextValue::Description(
            "non-negative integer for hits count",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(";")).parse_next(input)?;
    Ok(ExpectStmt::Hits { cmp, count })
}

/// `hit[INTEGER].assert;`
fn expect_hit_assert(input: &mut &str) -> ModalResult<ExpectStmt> {
    kw("hit").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("[")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let index = cut_err(nonneg_integer)
        .context(StrContext::Expected(StrContextValue::Description(
            "non-negative integer index",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("]")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(".")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let assert = cut_err(hit_assert).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(";")).parse_next(input)?;
    Ok(ExpectStmt::HitAssert { index, assert })
}

fn hit_assert(input: &mut &str) -> ModalResult<HitAssert> {
    alt((
        hit_assert_score,
        hit_assert_close_reason,
        hit_assert_entity_type,
        hit_assert_entity_id,
        hit_assert_field,
    ))
    .context(StrContext::Expected(StrContextValue::Description(
        "hit assertion (score|close_reason|entity_type|entity_id|field)",
    )))
    .parse_next(input)
}

/// `score cmp_op NUMBER`
fn hit_assert_score(input: &mut &str) -> ModalResult<HitAssert> {
    kw("score").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let cmp = cut_err(cmp_op).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(number_literal).parse_next(input)?;
    Ok(HitAssert::Score { cmp, value })
}

/// `close_reason == STRING`
fn hit_assert_close_reason(input: &mut &str) -> ModalResult<HitAssert> {
    kw("close_reason").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("==")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(quoted_string).parse_next(input)?;
    Ok(HitAssert::CloseReason { value })
}

/// `entity_type == STRING`
fn hit_assert_entity_type(input: &mut &str) -> ModalResult<HitAssert> {
    kw("entity_type").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("==")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(quoted_string).parse_next(input)?;
    Ok(HitAssert::EntityType { value })
}

/// `entity_id == STRING`
fn hit_assert_entity_id(input: &mut &str) -> ModalResult<HitAssert> {
    kw("entity_id").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("==")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(quoted_string).parse_next(input)?;
    Ok(HitAssert::EntityId { value })
}

/// `field(STRING) cmp_op expr`
fn hit_assert_field(input: &mut &str) -> ModalResult<HitAssert> {
    kw("field").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let name = cut_err(quoted_string).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let cmp = cut_err(cmp_op).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(expr::parse_expr).parse_next(input)?;
    Ok(HitAssert::Field { name, cmp, value })
}
