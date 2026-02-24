mod expect;
mod input;
mod options;

use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, ws_skip};

// ---------------------------------------------------------------------------
// test block
// ---------------------------------------------------------------------------

/// `test NAME for RULE_NAME { input { ... } expect { ... } [options { ... }] }`
pub(super) fn test_block(input_str: &mut &str) -> ModalResult<TestBlock> {
    ws_skip.parse_next(input_str)?;
    kw("test").parse_next(input_str)?;
    ws_skip.parse_next(input_str)?;

    let name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "test name",
        )))
        .parse_next(input_str)?
        .to_string();

    ws_skip.parse_next(input_str)?;
    cut_err(kw("for"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'for' after test name",
        )))
        .parse_next(input_str)?;
    ws_skip.parse_next(input_str)?;

    let rule_name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "rule name after 'for'",
        )))
        .parse_next(input_str)?
        .to_string();

    ws_skip.parse_next(input_str)?;
    cut_err(literal("{")).parse_next(input_str)?;

    ws_skip.parse_next(input_str)?;
    let input_stmts = cut_err(input::input_block)
        .context(StrContext::Expected(StrContextValue::Description(
            "input block",
        )))
        .parse_next(input_str)?;

    ws_skip.parse_next(input_str)?;
    let expect = cut_err(expect::expect_block)
        .context(StrContext::Expected(StrContextValue::Description(
            "expect block",
        )))
        .parse_next(input_str)?;

    ws_skip.parse_next(input_str)?;
    let options = opt(options::options_block).parse_next(input_str)?;

    ws_skip.parse_next(input_str)?;
    cut_err(literal("}")).parse_next(input_str)?;

    Ok(TestBlock {
        name,
        rule_name,
        input: input_stmts,
        expect,
        options,
    })
}

// ---------------------------------------------------------------------------
// shared: cmp_op
// ---------------------------------------------------------------------------

pub(super) fn cmp_op(input: &mut &str) -> ModalResult<CmpOp> {
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
