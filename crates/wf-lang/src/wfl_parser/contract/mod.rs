mod expect;
mod given;
mod options;

use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, ws_skip};

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
    let given = cut_err(given::given_block)
        .context(StrContext::Expected(StrContextValue::Description(
            "given block",
        )))
        .parse_next(input)?;

    ws_skip.parse_next(input)?;
    let expect = cut_err(expect::expect_block)
        .context(StrContext::Expected(StrContextValue::Description(
            "expect block",
        )))
        .parse_next(input)?;

    ws_skip.parse_next(input)?;
    let options = opt(options::options_block).parse_next(input)?;

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
