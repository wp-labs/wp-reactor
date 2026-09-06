//! 步骤分支与 pipe 链（match_p/steps 拆分）：`step_branch`（`has <alias>` 存在性 /
//! `[label ":"] source [field] [&& guard] | pipe`）与 `| transform | measure cmp threshold`
//! 管道。match / seq 步骤入口经 [`parse_or_branches`] / [`step_branch`] / [`parse_has_branch`]
//! 引用（pub(super), 同模块内可见）。

use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, quoted_string, ws_skip};

use super::super::expr;

/// `step_branch { "||" step_branch }` —— `||` 分隔的分支列表（不含结尾 `;`）。
pub(super) fn parse_or_branches(input: &mut &str) -> ModalResult<Vec<StepBranch>> {
    let first = step_branch.parse_next(input)?;
    let mut branches = vec![first];

    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("||")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            let branch = cut_err(step_branch).parse_next(input)?;
            branches.push(branch);
        } else {
            break;
        }
    }
    Ok(branches)
}

/// 解析可选的 `&& <expr>` guard（含前置空白）。
fn parse_optional_guard(input: &mut &str) -> ModalResult<Option<Expr>> {
    ws_skip.parse_next(input)?;
    if opt(literal("&&")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Ok(Some(cut_err(expr::parse_expr).parse_next(input)?))
    } else {
        Ok(None)
    }
}

/// `has <alias> [&& guard]` —— 存在性步骤，隐式 `count >= 1`（调用方已消费 `has`）。
pub(super) fn parse_has_branch(input: &mut &str) -> ModalResult<StepBranch> {
    ws_skip.parse_next(input)?;
    let source = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "event alias after `has`",
        )))
        .parse_next(input)?
        .to_string();
    let guard = parse_optional_guard(input)?;
    Ok(StepBranch {
        label: None,
        source,
        field: None,
        guard,
        pipe: PipeChain {
            transforms: Vec::new(),
            measure: Measure::Count,
            cmp: CmpOp::Ge,
            threshold: Expr::Number(1.0),
        },
    })
}

/// `[label ":"] source [".field" | '"["field"]"'] ["&&" guard] pipe_chain`
/// or `has <alias> [&& guard]` (existential, implicit `count >= 1`).
pub(super) fn step_branch(input: &mut &str) -> ModalResult<StepBranch> {
    ws_skip.parse_next(input)?;

    // `has <alias> [&& guard]` — existential step, implicit `count >= 1`.
    if opt(kw("has")).parse_next(input)?.is_some() {
        return parse_has_branch(input);
    }

    let (label, source) = parse_label_source(input)?;
    let (field, guard, pipe) = parse_branch_tail(input)?;

    Ok(StepBranch {
        label,
        source,
        field,
        guard,
        pipe,
    })
}

/// `[label ":"] source` —— label 可选（`label : source` 或裸 `source`）。
fn parse_label_source(input: &mut &str) -> ModalResult<(Option<String>, String)> {
    alt((
        // label : source
        (ident, ws_skip, literal(":"), ws_skip, ident)
            .map(|(l, _, _, _, s)| (Some(l.to_string()), s.to_string())),
        // just source
        ident.map(|s: &str| (None, s.to_string())),
    ))
    .parse_next(input)
}

/// 聚合形态尾段：`[".field" | '"["field"]"'] ["&&" guard] pipe_chain`。
fn parse_branch_tail(
    input: &mut &str,
) -> ModalResult<(Option<FieldSelector>, Option<Expr>, PipeChain)> {
    // Optional field selector
    let field = opt(field_selector).parse_next(input)?;

    // Optional guard: && expr
    let guard = parse_optional_guard(input)?;

    // Pipe chain
    ws_skip.parse_next(input)?;
    let pipe = cut_err(pipe_chain)
        .context(StrContext::Expected(StrContextValue::Description(
            "pipe chain (| measure cmp value)",
        )))
        .parse_next(input)?;
    Ok((field, guard, pipe))
}

/// Parse `.field` or `["field"]` selector.
fn field_selector(input: &mut &str) -> ModalResult<FieldSelector> {
    alt((
        // .field
        (literal("."), ident).map(|(_, f)| FieldSelector::Dot(f.to_string())),
        // ["field"]
        (literal("["), ws_skip, quoted_string, ws_skip, literal("]"))
            .map(|(_, _, s, _, _)| FieldSelector::Bracket(s)),
    ))
    .parse_next(input)
}

/// `{ "|" transform } "|" measure cmp_op threshold`
pub(super) fn pipe_chain(input: &mut &str) -> ModalResult<PipeChain> {
    let mut transforms = Vec::new();

    // Parse pipes: each is | followed by transform or measure
    // We collect transforms until we hit a measure keyword
    loop {
        ws_skip.parse_next(input)?;
        cut_err(literal("|"))
            .context(StrContext::Expected(StrContextValue::Description("'|'")))
            .parse_next(input)?;
        ws_skip.parse_next(input)?;

        // Try transform first
        if let Some(t) = opt(transform).parse_next(input)? {
            transforms.push(t);
        } else {
            // Must be a measure tail: measure cmp_op threshold
            let (measure, cmp, threshold) = parse_measure_tail(input)?;
            return Ok(PipeChain {
                transforms,
                measure,
                cmp,
                threshold,
            });
        }
    }
}

/// `measure cmp_op threshold`（transform 之后收尾的一段）——
/// 如 `count >= 2`、`sum(x) == 5`。
fn parse_measure_tail(input: &mut &str) -> ModalResult<(Measure, CmpOp, Expr)> {
    (
        cut_err(measure).context(StrContext::Expected(StrContextValue::Description(
            "measure (count|sum|avg|min|max)",
        ))),
        ws_skip,
        cut_err(cmp_op_step),
        ws_skip,
        cut_err(expr::parse_atomic_expr),
    )
        .map(|(m, _, c, _, t)| (m, c, t))
        .parse_next(input)
}

fn transform(input: &mut &str) -> ModalResult<Transform> {
    kw("distinct")
        .map(|_| Transform::Distinct)
        .parse_next(input)
}

fn measure(input: &mut &str) -> ModalResult<Measure> {
    alt((
        kw("count").map(|_| Measure::Count),
        kw("sum").map(|_| Measure::Sum),
        kw("avg").map(|_| Measure::Avg),
        kw("min").map(|_| Measure::Min),
        kw("max").map(|_| Measure::Max),
    ))
    .parse_next(input)
}

fn cmp_op_step(input: &mut &str) -> ModalResult<CmpOp> {
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


#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    #[test]
    fn or_branches_parse_and_stop_before_semicolon() {
        let mut s = "e | count >= 2 || b | sum == 1;";
        let branches = parse_or_branches
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("or branches failed: {e:?}"));
        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0].source, "e");
        assert_eq!(branches[1].source, "b");
        assert_eq!(s, ";", "|| 分支列表不含结尾 ;");
    }

    #[test]
    fn has_branch_defaults_to_count_ge_one() {
        let mut s = "a";
        let b = parse_has_branch
            .parse_next(&mut s)
            .expect("has a");
        assert_eq!(b.source, "a");
        assert!(matches!(b.pipe, PipeChain { measure: Measure::Count, cmp: CmpOp::Ge, .. }));
        assert!(s.is_empty());
    }
}
