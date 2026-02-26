use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{kw, nonneg_integer, quoted_string, ws_skip};

// ---------------------------------------------------------------------------
// options block
// ---------------------------------------------------------------------------

/// `options { [close_trigger = val;] [eval_mode = val;] [permutation = shuffle;] [runs = N;] }`
pub(super) fn options_block(input: &mut &str) -> ModalResult<TestOptions> {
    kw("options").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let mut close_trigger = None;
    let mut eval_mode = None;
    let mut permutation = None;
    let mut runs = None;

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
        } else if opt(kw("permutation")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            cut_err(literal("=")).parse_next(input)?;
            ws_skip.parse_next(input)?;
            permutation = Some(cut_err(permutation_val).parse_next(input)?);
            ws_skip.parse_next(input)?;
            cut_err(literal(";")).parse_next(input)?;
        } else if opt(kw("runs")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            cut_err(literal("=")).parse_next(input)?;
            ws_skip.parse_next(input)?;
            runs = Some(cut_err(runs_val).parse_next(input)?);
            ws_skip.parse_next(input)?;
            cut_err(literal(";")).parse_next(input)?;
        } else {
            return Err(winnow::error::ErrMode::Cut(
                winnow::error::ContextError::new(),
            ));
        }
    }

    cut_err(literal("}")).parse_next(input)?;

    Ok(TestOptions {
        close_trigger,
        eval_mode,
        permutation,
        runs,
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

fn permutation_val(input: &mut &str) -> ModalResult<PermutationMode> {
    if opt(kw("shuffle")).parse_next(input)?.is_some() {
        return Ok(PermutationMode::Shuffle);
    }

    let mode = quoted_string.parse_next(input)?;
    if mode == "shuffle" {
        Ok(PermutationMode::Shuffle)
    } else {
        Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ))
    }
}

fn runs_val(input: &mut &str) -> ModalResult<usize> {
    let n = nonneg_integer.parse_next(input)?;
    if n == 0 {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }
    Ok(n)
}
