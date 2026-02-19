use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{
    duration_value, ident, kw, nonneg_integer, number_literal, quoted_string, ws_skip,
};

use super::expr;

// ---------------------------------------------------------------------------
// contract block
// ---------------------------------------------------------------------------

/// `contract NAME for RULE_NAME { given { ... } expect { ... } [options { ... }] }`
pub(super) fn contract_block(input: &mut &str) -> ModalResult<ContractBlock> {
    ws_skip.parse_next(input)?;
    kw("contract").parse_next(input)?;
    ws_skip.parse_next(input)?;

    let name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "contract name",
        )))
        .parse_next(input)?
        .to_string();

    ws_skip.parse_next(input)?;
    cut_err(kw("for"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'for' after contract name",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;

    let rule_name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "rule name after 'for'",
        )))
        .parse_next(input)?
        .to_string();

    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    ws_skip.parse_next(input)?;
    let given = cut_err(given_block)
        .context(StrContext::Expected(StrContextValue::Description(
            "given block",
        )))
        .parse_next(input)?;

    ws_skip.parse_next(input)?;
    let expect = cut_err(expect_block)
        .context(StrContext::Expected(StrContextValue::Description(
            "expect block",
        )))
        .parse_next(input)?;

    ws_skip.parse_next(input)?;
    let options = opt(options_block).parse_next(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal("}")).parse_next(input)?;

    Ok(ContractBlock {
        name,
        rule_name,
        given,
        expect,
        options,
    })
}

// ---------------------------------------------------------------------------
// given block
// ---------------------------------------------------------------------------

/// `given { row(...); ... tick(...); ... }`
fn given_block(input: &mut &str) -> ModalResult<Vec<GivenStmt>> {
    kw("given").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let mut stmts = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if input.starts_with('}') {
            break;
        }
        let stmt = cut_err(given_stmt)
            .context(StrContext::Expected(StrContextValue::Description(
                "given statement (row or tick)",
            )))
            .parse_next(input)?;
        stmts.push(stmt);
    }

    cut_err(literal("}")).parse_next(input)?;
    Ok(stmts)
}

fn given_stmt(input: &mut &str) -> ModalResult<GivenStmt> {
    alt((given_row, given_tick)).parse_next(input)
}

/// `row(IDENT, field = expr, ...);`
fn given_row(input: &mut &str) -> ModalResult<GivenStmt> {
    kw("row").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;

    let alias = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "event alias in row()",
        )))
        .parse_next(input)?
        .to_string();

    ws_skip.parse_next(input)?;
    cut_err(literal(","))
        .context(StrContext::Expected(StrContextValue::Description(
            "',' after alias",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;

    // First field assignment is required
    let first = cut_err(field_assign).parse_next(input)?;
    let mut fields = vec![first];

    loop {
        ws_skip.parse_next(input)?;
        if opt(literal(",")).parse_next(input)?.is_none() {
            break;
        }
        ws_skip.parse_next(input)?;
        // Trailing comma
        if input.starts_with(')') {
            break;
        }
        let f = cut_err(field_assign).parse_next(input)?;
        fields.push(f);
    }

    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(";")).parse_next(input)?;

    Ok(GivenStmt::Row { alias, fields })
}

/// `(IDENT | STRING) = expr`
fn field_assign(input: &mut &str) -> ModalResult<FieldAssign> {
    let name = alt((quoted_string, ident.map(|s: &str| s.to_string()))).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("=")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(expr::parse_expr).parse_next(input)?;
    Ok(FieldAssign { name, value })
}

/// `tick(DURATION);`
fn given_tick(input: &mut &str) -> ModalResult<GivenStmt> {
    kw("tick").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let dur = cut_err(duration_value)
        .context(StrContext::Expected(StrContextValue::Description(
            "duration value in tick()",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(";")).parse_next(input)?;
    Ok(GivenStmt::Tick(dur))
}

// ---------------------------------------------------------------------------
// expect block
// ---------------------------------------------------------------------------

/// `expect { hits cmp NUMBER; ... hit[i].assert; ... }`
fn expect_block(input: &mut &str) -> ModalResult<Vec<ExpectStmt>> {
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

// ---------------------------------------------------------------------------
// options block
// ---------------------------------------------------------------------------

/// `options { [close_trigger = val;] [eval_mode = val;] }`
fn options_block(input: &mut &str) -> ModalResult<ContractOptions> {
    kw("options").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let mut close_trigger = None;
    let mut eval_mode = None;

    loop {
        ws_skip.parse_next(input)?;
        if input.starts_with('}') {
            break;
        }
        if opt(kw("close_trigger")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            cut_err(literal("=")).parse_next(input)?;
            ws_skip.parse_next(input)?;
            close_trigger = Some(cut_err(close_trigger_val).parse_next(input)?);
            ws_skip.parse_next(input)?;
            cut_err(literal(";")).parse_next(input)?;
        } else if opt(kw("eval_mode")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            cut_err(literal("=")).parse_next(input)?;
            ws_skip.parse_next(input)?;
            eval_mode = Some(cut_err(eval_mode_val).parse_next(input)?);
            ws_skip.parse_next(input)?;
            cut_err(literal(";")).parse_next(input)?;
        } else {
            return Err(winnow::error::ErrMode::Cut(
                winnow::error::ContextError::new(),
            ));
        }
    }

    cut_err(literal("}")).parse_next(input)?;

    Ok(ContractOptions {
        close_trigger,
        eval_mode,
    })
}

fn close_trigger_val(input: &mut &str) -> ModalResult<CloseTrigger> {
    alt((
        kw("timeout").map(|_| CloseTrigger::Timeout),
        kw("flush").map(|_| CloseTrigger::Flush),
        kw("eos").map(|_| CloseTrigger::Eos),
    ))
    .context(StrContext::Expected(StrContextValue::Description(
        "close trigger (timeout|flush|eos)",
    )))
    .parse_next(input)
}

fn eval_mode_val(input: &mut &str) -> ModalResult<EvalMode> {
    alt((
        kw("strict").map(|_| EvalMode::Strict),
        kw("lenient").map(|_| EvalMode::Lenient),
    ))
    .context(StrContext::Expected(StrContextValue::Description(
        "eval mode (strict|lenient)",
    )))
    .parse_next(input)
}

// ---------------------------------------------------------------------------
// shared: cmp_op
// ---------------------------------------------------------------------------

fn cmp_op(input: &mut &str) -> ModalResult<CmpOp> {
    alt((
        literal("==").value(CmpOp::Eq),
        literal("!=").value(CmpOp::Ne),
        literal("<=").value(CmpOp::Le),
        literal(">=").value(CmpOp::Ge),
        literal("<").value(CmpOp::Lt),
        literal(">").value(CmpOp::Gt),
    ))
    .parse_next(input)
}
