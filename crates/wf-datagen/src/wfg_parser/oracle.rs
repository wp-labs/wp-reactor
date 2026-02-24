use winnow::combinator::{cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use wf_lang::parse_utils::ident;

use crate::wfg_ast::*;

use super::primitives::{param_value, semi, ws_skip};

// ---------------------------------------------------------------------------
// Oracle block
// ---------------------------------------------------------------------------

pub(super) fn oracle_block(input: &mut &str) -> ModalResult<OracleBlock> {
    ws_skip(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description(
            "opening brace for oracle block",
        )))
        .parse_next(input)?;

    let mut params = Vec::new();
    loop {
        ws_skip(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let p = param_assign(input)?;
        cut_err(semi)
            .context(StrContext::Expected(StrContextValue::Description(
                "trailing ';' after oracle assignment",
            )))
            .parse_next(input)?;
        params.push(p);
    }

    Ok(OracleBlock { params })
}

// ---------------------------------------------------------------------------
// Shared: param_assign
// ---------------------------------------------------------------------------

pub(super) fn param_assign(input: &mut &str) -> ModalResult<ParamAssign> {
    let name = ident(input)?.to_string();
    ws_skip(input)?;
    cut_err(literal("="))
        .context(StrContext::Expected(StrContextValue::Description(
            "'=' in param assignment",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    let value = cut_err(param_value)
        .context(StrContext::Expected(StrContextValue::Description(
            "param value",
        )))
        .parse_next(input)?;
    Ok(ParamAssign { name, value })
}
