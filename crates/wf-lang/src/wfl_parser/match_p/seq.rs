//! seq 子句族（match_p/steps 拆分）：`seq [consec] [skip = …] { not has / step_branch [within] ; }`。
//! `seq_block_body` 由 on-event 主体分派引用; `seq_step`/`parse_seq_steps` 供同模块测试直连。

use winnow::combinator::{alt, cut_err, delimited, opt, preceded};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{duration_value, kw, ws_skip};

use super::branch::{parse_has_branch, step_branch};
use super::steps::collect_until_close;

/// Parse chain body after the `chain` keyword:
///   `[consec] [skip = past_last|to_next] { seq_steps }`
pub(super) fn seq_block_body(input: &mut &str) -> ModalResult<SeqClause> {
    let (consec, skip) = parse_seq_modifiers(input)?;
    let steps = preceded(
        ws_skip,
        delimited(
            cut_err(literal("{")),
            parse_seq_steps,
            cut_err(literal("}")),
        ),
    )
    .parse_next(input)?;
    Ok(SeqClause {
        consec,
        skip,
        steps,
    })
}

/// `[consec] [skip = past_last|to_next]` —— seq 前缀；skip 缺省为 `past_last`。
fn parse_seq_modifiers(input: &mut &str) -> ModalResult<(bool, SeqSkip)> {
    let mut consec = false;
    ws_skip.parse_next(input)?;
    if opt(kw("consec")).parse_next(input)?.is_some() {
        consec = true;
        ws_skip.parse_next(input)?;
    }

    let skip = if opt(kw("skip")).parse_next(input)?.is_some() {
        parse_skip_assignment(input)?
    } else {
        SeqSkip::PastLast
    };
    Ok((consec, skip))
}

/// `skip = past_last | to_next`（调用方已消费 `skip`）。
fn parse_skip_assignment(input: &mut &str) -> ModalResult<SeqSkip> {
    ws_skip.parse_next(input)?;
    cut_err(literal("=")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let policy = cut_err(alt((
        kw("past_last").value(SeqSkip::PastLast),
        kw("to_next").value(SeqSkip::ToNext),
    )))
    .context(StrContext::Expected(StrContextValue::Description(
        "skip policy (past_last|to_next)",
    )))
    .parse_next(input)?;
    ws_skip.parse_next(input)?;
    Ok(policy)
}

/// 解析 `{ step; ... }` 内步骤列表（空列表为语法错误）。
pub(super) fn parse_seq_steps(input: &mut &str) -> ModalResult<Vec<SeqStep>> {
    collect_until_close(input, "chain step", seq_step)
}

/// Parse one chain step: `[not] <body> [within dur] ;`
/// body := `has <alias> [&& guard]` (existential) | step_branch (aggregate).
pub(super) fn seq_step(input: &mut &str) -> ModalResult<SeqStep> {
    ws_skip.parse_next(input)?;
    let neg = opt(kw("not")).parse_next(input)?.is_some();
    ws_skip.parse_next(input)?;
    let branch = parse_seq_step_body(input)?;
    let within = parse_within(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal(";"))
        .context(StrContext::Expected(StrContextValue::Description(
            "';' after chain step",
        )))
        .parse_next(input)?;

    Ok(SeqStep {
        neg,
        within,
        branch,
    })
}

/// 链步骤主体：`has <alias> [&& guard]`（存在性）或普通聚合 `step_branch`。
fn parse_seq_step_body(input: &mut &str) -> ModalResult<StepBranch> {
    if opt(kw("has")).parse_next(input)?.is_some() {
        // `has <alias> [&& guard]` — existential step, implicit `count >= 1`
        parse_has_branch(input)
    } else {
        // Aggregate step: reuse existing step_branch (pipe required)
        cut_err(step_branch)
            .context(StrContext::Expected(StrContextValue::Description(
                "chain step (has <alias> | <alias>.<field> | distinct | count >= N)",
            )))
            .parse_next(input)
    }
}

/// `within <duration>` —— 可选链步骤时限；缺省 None。
fn parse_within(input: &mut &str) -> ModalResult<Option<std::time::Duration>> {
    ws_skip.parse_next(input)?;
    if opt(kw("within")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Ok(Some(
            cut_err(duration_value)
                .context(StrContext::Expected(StrContextValue::Description(
                    "within duration",
                )))
                .parse_next(input)?,
        ))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    #[test]
    fn seq_modifiers_and_steps_roundtrip() {
        // seq 前缀 `consec skip = to_next` + 带 not/within 的步骤列表
        let mut s = "consec skip = to_next { not has a within 2s; has b; }";
        let seq = seq_block_body
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("seq block failed: {e:?}"));
        assert!(seq.consec);
        assert!(matches!(seq.skip, SeqSkip::ToNext));
        assert_eq!(seq.steps.len(), 2);
        assert!(seq.steps[0].neg);
        assert_eq!(seq.steps[0].within, Some(std::time::Duration::from_secs(2)));
        assert_eq!(seq.steps[1].branch.source, "b");
        assert!(s.is_empty());
    }
}
