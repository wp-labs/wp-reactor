use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{kw, ws_skip};

// ---------------------------------------------------------------------------
// options block
// ---------------------------------------------------------------------------

/// `options { [close_trigger = val;] [eval_mode = val;] }`
pub(super) fn options_block(input: &mut &str) -> ModalResult<ContractOptions> {
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
