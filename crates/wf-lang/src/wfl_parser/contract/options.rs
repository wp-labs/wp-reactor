use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, nonneg_integer, quoted_string, ws_skip};

// ---------------------------------------------------------------------------
// options block
// ---------------------------------------------------------------------------

/// `options { [close_trigger = val;] [eval_mode = val;] [permutation = shuffle;] [runs = N;] }`
pub(super) fn options_block(input: &mut &str) -> ModalResult<TestOptions> {
    kw("options").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let opts = parse_option_entries(input)?;

    cut_err(literal("}")).parse_next(input)?;
    Ok(opts)
}

/// `key = value; ...` 选项条目循环（`}` 收尾; 未知关键字 → Cut）。
fn parse_option_entries(input: &mut &str) -> ModalResult<TestOptions> {
    let mut close_trigger = None;
    let mut eval_mode = None;
    let mut permutation = None;
    let mut runs = None;

    loop {
        ws_skip.parse_next(input)?;
        if input.starts_with('}') {
            break;
        }
        // 关键字名分派；未知关键字即语法错误（Cut）。
        match cut_err(ident)
            .context(StrContext::Expected(StrContextValue::Description(
                "option key (close_trigger|eval_mode|permutation|runs)",
            )))
            .parse_next(input)?
        {
            "close_trigger" => close_trigger = Some(parse_option_value(input, close_trigger_val)?),
            "eval_mode" => eval_mode = Some(parse_option_value(input, eval_mode_val)?),
            "permutation" => permutation = Some(parse_option_value(input, permutation_val)?),
            "runs" => runs = Some(parse_option_value(input, runs_val)?),
            _ => {
                return Err(winnow::error::ErrMode::Cut(
                    winnow::error::ContextError::new(),
                ));
            }
        }
    }

    Ok(TestOptions {
        close_trigger,
        eval_mode,
        permutation,
        runs,
    })
}

/// `= <val> ;` 选项赋值尾（调用方已消费关键字名与前置空白）。
fn parse_option_value<T>(
    input: &mut &str,
    value: fn(&mut &str) -> ModalResult<T>,
) -> ModalResult<T> {
    ws_skip.parse_next(input)?;
    cut_err(literal("=")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let v = cut_err(value).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(";")).parse_next(input)?;
    Ok(v)
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

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    fn opts(input: &str) -> TestOptions {
        let mut s = input;
        options_block
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("options_block failed for {input:?}: {e:?}"))
    }

    fn opts_err(input: &str) {
        let mut s = input;
        assert!(
            options_block.parse_next(&mut s).is_err(),
            "expected options_block error for {input:?}"
        );
    }

    #[test]
    fn options_block_all_fields() {
        let o = opts(
            "options { close_trigger = timeout; eval_mode = strict; \
             permutation = shuffle; runs = 3; }",
        );
        assert_eq!(o.close_trigger, Some(CloseTrigger::Timeout));
        assert_eq!(o.eval_mode, Some(EvalMode::Strict));
        assert_eq!(o.permutation, Some(PermutationMode::Shuffle));
        assert_eq!(o.runs, Some(3));
    }

    #[test]
    fn options_block_empty_and_partial() {
        let o = opts("options { }");
        assert_eq!(o.close_trigger, None);
        assert_eq!(o.eval_mode, None);
        assert_eq!(o.permutation, None);
        assert_eq!(o.runs, None);

        let o = opts("options { eval_mode = lenient; }");
        assert_eq!(o.eval_mode, Some(EvalMode::Lenient));
        assert!(o.close_trigger.is_none() && o.permutation.is_none() && o.runs.is_none());
    }

    #[test]
    fn option_value_variants() {
        for (val, want) in [("flush", CloseTrigger::Flush), ("eos", CloseTrigger::Eos)] {
            let o = opts(&format!("options {{ close_trigger = {val}; }}"));
            assert_eq!(o.close_trigger, Some(want));
        }
    }

    #[test]
    fn options_block_errors() {
        opts_err("options { bogus = 1; }"); // 未知选项
        opts_err("options { runs = 0; }"); // runs 必须 > 0
        opts_err("options { close_trigger = timeout }"); // 缺 `;`
        opts_err("options { eval_mode = strict;"); // 缺 `}`
    }

    #[test]
    fn permutation_accepts_quoted_shuffle_only() {
        // 兼容引号写法：仅 "shuffle" 合法
        let o = opts("options { permutation = \"shuffle\"; }");
        assert_eq!(o.permutation, Some(PermutationMode::Shuffle));

        let mut s = "options { permutation = \"round_robin\"; }";
        assert!(options_block.parse_next(&mut s).is_err());
    }
}
