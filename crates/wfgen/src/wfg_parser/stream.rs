use winnow::combinator::{cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use wf_lang::parse_utils::ident;

use crate::wfg_ast::*;

use super::primitives::{field_name, gen_expr, rate, ws_skip};

// ---------------------------------------------------------------------------
// Stream block
// ---------------------------------------------------------------------------

pub(super) fn stream_block(input: &mut &str) -> ModalResult<StreamBlock> {
    ws_skip(input)?;
    let alias = ident(input)?.to_string();
    ws_skip(input)?;
    cut_err(literal(":"))
        .context(StrContext::Expected(StrContextValue::Description(
            "colon after stream alias",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    let window = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "window name",
        )))
        .parse_next(input)?
        .to_string();
    ws_skip(input)?;
    let r = cut_err(rate)
        .context(StrContext::Expected(StrContextValue::Description("rate")))
        .parse_next(input)?;
    ws_skip(input)?;

    let mut overrides = Vec::new();
    if opt(literal("{")).parse_next(input)?.is_some() {
        loop {
            ws_skip(input)?;
            if opt(literal("}")).parse_next(input)?.is_some() {
                break;
            }
            let fo = field_override(input)?;
            overrides.push(fo);
        }
    }

    Ok(StreamBlock {
        alias,
        window,
        rate: r,
        overrides,
    })
}

pub(super) fn field_override(input: &mut &str) -> ModalResult<FieldOverride> {
    let fname = field_name(input)?;
    ws_skip(input)?;
    cut_err(literal("="))
        .context(StrContext::Expected(StrContextValue::Description(
            "'=' in field override",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    let expr = cut_err(gen_expr)
        .context(StrContext::Expected(StrContextValue::Description(
            "generator expression",
        )))
        .parse_next(input)?;
    Ok(FieldOverride {
        field_name: fname,
        gen_expr: expr,
    })
}
