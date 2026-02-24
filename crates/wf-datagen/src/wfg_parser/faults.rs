use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::wfg_ast::*;

use super::primitives::{percent, semi, ws_skip};

// ---------------------------------------------------------------------------
// Faults block
// ---------------------------------------------------------------------------

pub(super) fn faults_block(input: &mut &str) -> ModalResult<FaultsBlock> {
    ws_skip(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description(
            "opening brace for faults block",
        )))
        .parse_next(input)?;

    let mut faults = Vec::new();
    loop {
        ws_skip(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let fl = fault_line(input)?;
        cut_err(semi)
            .context(StrContext::Expected(StrContextValue::Description(
                "trailing ';' after faults line",
            )))
            .parse_next(input)?;
        faults.push(fl);
    }

    Ok(FaultsBlock { faults })
}

pub(super) fn fault_line(input: &mut &str) -> ModalResult<FaultLine> {
    let fault_type = alt((
        wf_lang::parse_utils::kw("out_of_order").value(FaultType::OutOfOrder),
        wf_lang::parse_utils::kw("late").value(FaultType::Late),
        wf_lang::parse_utils::kw("duplicate").value(FaultType::Duplicate),
        wf_lang::parse_utils::kw("drop").value(FaultType::Drop),
    ))
    .context(StrContext::Expected(StrContextValue::Description(
        "fault type (out_of_order, late, duplicate, drop)",
    )))
    .parse_next(input)?;
    ws_skip(input)?;
    let pct = cut_err(percent)
        .context(StrContext::Expected(StrContextValue::Description(
            "percent for fault",
        )))
        .parse_next(input)?;
    Ok(FaultLine {
        fault_type,
        percent: pct,
    })
}
