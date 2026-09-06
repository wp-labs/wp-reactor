use winnow::combinator::{alt, cut_err, delimited, opt, preceded};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{duration_value, ident, kw, quoted_string, ws_skip};

use super::super::expr;
use super::window::field_ref;

/// on event 主体的解析结果：`(有序步骤, close 块, seq 子句, 模式)`——
/// seq 形态下 `on_event` 为空；`match_p/mod.rs` 直接解构使用。
pub(super) type OnEventBody = (
    Vec<MatchStep>,
    Option<CloseBlock>,
    Option<SeqClause>,
    MatchMode,
);

/// 解析 match body 的三种形态（accu 已消费）：
///   `on event { ... } [close]`        — ordered (default, backward compat)
///   `on event seq [mods] { ... }`     — ordered + within/not/consec/skip
///   `on event any { ... }`            — unordered co-occurrence
pub(super) fn parse_on_event_body(input: &mut &str) -> ModalResult<OnEventBody> {
    ws_skip.parse_next(input)?;

    if opt(kw("seq")).parse_next(input)?.is_some() {
        // `on event seq { ... }` — ordered + within/not/consec/skip
        let seq = cut_err(seq_block_body)
            .context(StrContext::Expected(StrContextValue::Description(
                "seq block body",
            )))
            .parse_next(input)?;
        Ok((Vec::new(), None, Some(seq), MatchMode::Seq))
    } else if opt(kw("any")).parse_next(input)?.is_some() {
        // `on event any { ... }` — unordered co-occurrence
        let steps = parse_any_block(input)?;
        Ok((steps, None, None, MatchMode::Any))
    } else {
        // bare `on event { ... } [close]` — ordered (backward compat)
        let body = parse_ordered_block(input)?;
        Ok(body)
    }
}

/// `any { steps }`（调用方已消费 `any`）——无序共现块。
fn parse_any_block(input: &mut &str) -> ModalResult<Vec<MatchStep>> {
    preceded(
        ws_skip,
        delimited(cut_err(literal("{")), match_steps, cut_err(literal("}"))),
    )
    .parse_next(input)
}

/// 裸 `{ ... } [close]`（backward compat 有序）——块体 + 可选 on/and close。
fn parse_ordered_block(input: &mut &str) -> ModalResult<OnEventBody> {
    let on_event = cut_err(on_event_block)
        .context(StrContext::Expected(StrContextValue::Description(
            "'on event' block",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let on_close = opt(close_block).parse_next(input)?;
    Ok((on_event, on_close, None, MatchMode::Seq))
}

/// 收集 `{ ... }` 内条目直到 `}`（`}` 由调用方消费）：条目为空或解析失败
/// 均报语法错误；条目错误带 `desc` 描述上下文。
fn collect_until_close<T>(
    input: &mut &str,
    desc: &'static str,
    item: fn(&mut &str) -> ModalResult<T>,
) -> ModalResult<Vec<T>> {
    let mut items = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if input.starts_with('}') {
            break;
        }
        let parsed = cut_err(item)
            .context(StrContext::Expected(StrContextValue::Description(desc)))
            .parse_next(input)?;
        items.push(parsed);
    }
    if items.is_empty() {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }
    Ok(items)
}

/// `key { logical = alias.field; ... }`
pub(super) fn key_block(input: &mut &str) -> ModalResult<Vec<KeyMapItem>> {
    kw("key").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;
    let items = collect_until_close(input, "key mapping item", parse_key_mapping_item)?;
    cut_err(literal("}")).parse_next(input)?;
    Ok(items)
}

/// `logical = alias.field ;` —— 单条 key 映射（含 ';' 终结符）。
fn parse_key_mapping_item(input: &mut &str) -> ModalResult<KeyMapItem> {
    (
        cut_err(ident),
        ws_skip,
        cut_err(literal("=")),
        ws_skip,
        cut_err(field_ref),
        ws_skip,
        cut_err(literal(";")).context(StrContext::Expected(StrContextValue::Description(
            "';' after key mapping item",
        ))),
    )
        .map(|(logical, _, _, _, source, _, _)| KeyMapItem {
            logical_name: logical.to_string(),
            source_field: source,
        })
        .parse_next(input)
}

fn on_event_block(input: &mut &str) -> ModalResult<Vec<MatchStep>> {
    delimited(cut_err(literal("{")), match_steps, cut_err(literal("}"))).parse_next(input)
}

fn close_block(input: &mut &str) -> ModalResult<CloseBlock> {
    // Try "and close" first (AND mode), then "on close" (OR mode)
    let mode = alt((
        (kw("and"), ws_skip, kw("close")).map(|_| CloseMode::And),
        (kw("on"), ws_skip, kw("close")).map(|_| CloseMode::Or),
    ))
    .parse_next(input)?;
    let steps = preceded(
        ws_skip,
        delimited(cut_err(literal("{")), match_steps, cut_err(literal("}"))),
    )
    .parse_next(input)?;
    Ok(CloseBlock { mode, steps })
}

fn match_steps(input: &mut &str) -> ModalResult<Vec<MatchStep>> {
    collect_until_close(input, "match step", match_step)
}

/// `step_branch { "||" step_branch } ";"`
fn match_step(input: &mut &str) -> ModalResult<MatchStep> {
    let branches = parse_or_branches(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal(";"))
        .context(StrContext::Expected(StrContextValue::Description(
            "';' after match step",
        )))
        .parse_next(input)?;

    Ok(MatchStep { branches })
}

/// `step_branch { "||" step_branch }` —— `||` 分隔的分支列表（不含结尾 `;`）。
fn parse_or_branches(input: &mut &str) -> ModalResult<Vec<StepBranch>> {
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
fn parse_has_branch(input: &mut &str) -> ModalResult<StepBranch> {
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
fn step_branch(input: &mut &str) -> ModalResult<StepBranch> {
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
fn pipe_chain(input: &mut &str) -> ModalResult<PipeChain> {
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

/// Parse chain body after the `chain` keyword:
///   `[consec] [skip = past_last|to_next] { seq_steps }`
fn seq_block_body(input: &mut &str) -> ModalResult<SeqClause> {
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
fn parse_seq_steps(input: &mut &str) -> ModalResult<Vec<SeqStep>> {
    collect_until_close(input, "chain step", seq_step)
}

/// Parse one chain step: `[not] <body> [within dur] ;`
/// body := `has <alias> [&& guard]` (existential) | step_branch (aggregate).
fn seq_step(input: &mut &str) -> ModalResult<SeqStep> {
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
    use super::super::match_clause_only;
    use super::*;
    use winnow::Parser;

    fn match_only(input: &str) -> MatchClause {
        let mut s = input;
        match_clause_only
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("match_clause_only failed for {input:?}: {e:?}"))
    }

    #[test]
    fn has_branch_implicit_count() {
        let mut s = "a";
        let branch = parse_has_branch(&mut s).expect("has a");
        assert_eq!(branch.source, "a");
        assert!(branch.guard.is_none());
        assert!(matches!(
            branch.pipe,
            PipeChain {
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                ..
            }
        ));

        let mut s = "a && x == 1";
        let branch = parse_has_branch(&mut s).expect("has a && guard");
        assert!(branch.guard.is_some());
    }

    #[test]
    fn match_body_bare_seq_any() {
        // bare on event（backward compat）
        let mc = match_only("match<a:5m> { on event { e | count >= 1; } }");
        assert_eq!(mc.keys, vec![FieldRef::Simple("a".into())]);
        assert_eq!(mc.duration, std::time::Duration::from_secs(300));
        assert!(matches!(mc.match_mode, MatchMode::Seq));
        assert_eq!(mc.on_event.len(), 1);
        assert!(mc.seq.is_none());

        // seq + consec + not has within
        let mc = match_only("match<:5m> { on event seq consec { not has b within 1s; } }");
        assert!(matches!(mc.match_mode, MatchMode::Seq));
        let seq = mc.seq.expect("seq");
        assert!(seq.consec);
        assert_eq!(seq.steps.len(), 1);
        assert!(seq.steps[0].neg);
        assert_eq!(seq.steps[0].within, Some(std::time::Duration::from_secs(1)));

        // any（无序共现）
        let mc = match_only("match<:5m> { on event any { e | count >= 1; } }");
        assert!(matches!(mc.match_mode, MatchMode::Any));
        assert_eq!(mc.on_event.len(), 1);
    }

    #[test]
    fn seq_skip_policy_default_and_explicit() {
        // skip 缺省 = past_last
        let mc = match_only("match<:5m> { on event seq { has a; } }");
        let seq = mc.seq.expect("seq");
        assert!(!seq.consec);
        assert!(matches!(seq.skip, SeqSkip::PastLast));

        // consec + skip = to_next
        let mc = match_only("match<:5m> { on event seq consec skip = to_next { has a; has b; } }");
        let seq = mc.seq.expect("seq");
        assert!(seq.consec);
        assert!(matches!(seq.skip, SeqSkip::ToNext));
        assert_eq!(seq.steps.len(), 2);
    }

    #[test]
    fn key_mapping_block_items() {
        let mc = match_only(
            "match<a:5m> { key { k1 = e.f; k2 = e.obj[0]; } on event { e | count >= 1; } }",
        );
        let items = mc.key_mapping.expect("key mapping");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].logical_name, "k1");
        assert_eq!(
            items[0].source_field,
            FieldRef::Qualified("e".into(), "f".into())
        );
        assert!(matches!(items[1].source_field, FieldRef::Path { .. }));
    }

    fn pipe(input: &str) -> PipeChain {
        let mut s = input;
        pipe_chain
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("pipe_chain failed for {input:?}: {e:?}"))
    }

    #[test]
    fn pipe_chain_transforms_and_measure_tail() {
        // 纯 measure 尾部
        let p = pipe("| count >= 2");
        assert!(p.transforms.is_empty());
        assert_eq!(p.measure, Measure::Count);
        assert_eq!(p.cmp, CmpOp::Ge);
        assert_eq!(p.threshold, Expr::Number(2.0));

        // distinct 变换（可重复）在前，measure 收尾
        let p = pipe("| distinct | distinct | sum != 1");
        assert_eq!(p.transforms, vec![Transform::Distinct, Transform::Distinct]);
        assert_eq!(p.measure, Measure::Sum);
        assert_eq!(p.cmp, CmpOp::Ne);

        // 其余 cmp 走 alt
        assert_eq!(pipe("| avg <= 0").cmp, CmpOp::Le);
        assert_eq!(pipe("| min > 5").measure, Measure::Min);
        assert_eq!(pipe("| max == 9").measure, Measure::Max);
    }

    #[test]
    fn pipe_chain_requires_measure_tail() {
        let mut s = "| distinct";
        assert!(pipe_chain.parse_next(&mut s).is_err(), "transform 不是结尾");
        let mut s = "| 5";
        assert!(pipe_chain.parse_next(&mut s).is_err(), "缺少 measure");
    }

    fn seq_s(input: &str) -> SeqStep {
        let mut s = input;
        seq_step
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("seq_step failed for {input:?}: {e:?}"))
    }

    #[test]
    fn seq_step_has_neg_within_and_aggregate() {
        // not has + within
        let st = seq_s("not has b within 1s;");
        assert!(st.neg);
        assert_eq!(st.branch.source, "b");
        assert!(matches!(
            st.branch.pipe,
            PipeChain {
                measure: Measure::Count,
                ..
            }
        ));
        assert_eq!(st.within, Some(std::time::Duration::from_secs(1)));

        // 聚合步骤：label: source.field + pipe
        let st = seq_s("l: e.f | count >= 3;");
        assert!(!st.neg);
        assert_eq!(st.branch.label.as_deref(), Some("l"));
        assert_eq!(st.branch.source, "e");
        assert!(matches!(st.branch.field, Some(FieldSelector::Dot(ref f)) if f == "f"));
        assert!(st.within.is_none());

        // 缺 `;` 报错
        let mut s = "has b";
        assert!(seq_step.parse_next(&mut s).is_err());
    }

    #[test]
    fn close_block_and_and_or_modes() {
        let mut s = "on close { e | count >= 1; }";
        let cb = close_block.parse_next(&mut s).expect("on close block");
        assert!(matches!(cb.mode, CloseMode::Or));
        assert_eq!(cb.steps.len(), 1);

        let mut s = "and close { t: e.sip | distinct | count >= 1; }";
        let cb = close_block.parse_next(&mut s).expect("and close block");
        assert!(matches!(cb.mode, CloseMode::And));
        assert_eq!(cb.steps.len(), 1);
    }

    #[test]
    fn match_step_or_branches_and_guard() {
        // 两个 `||` 分支 + 字段选择器 + `&&` guard
        let mut s = "e.action && x == 1 | count > 2 || b[\"k\"] | sum >= 2;";
        let step = match_step
            .parse_next(&mut s)
            .expect("match_step with branches");
        assert_eq!(step.branches.len(), 2);
        let (b0, b1) = (&step.branches[0], &step.branches[1]);
        assert_eq!(b0.source, "e");
        assert!(matches!(b0.field, Some(FieldSelector::Dot(ref f)) if f == "action"));
        assert!(matches!(&b0.guard, Some(Expr::BinOp { op: BinOp::Eq, .. })));
        assert!(matches!(b1.field, Some(FieldSelector::Bracket(ref k)) if k == "k"));
        assert_eq!(b1.source, "b");
    }

    #[test]
    fn empty_braced_lists_are_errors() {
        // key / match / chain 三个收集列表均拒绝空体
        let mut s = "key { }";
        assert!(key_block.parse_next(&mut s).is_err());
        let mut s = "}";
        assert!(match_steps.parse_next(&mut s).is_err());
        let mut s = "}";
        assert!(parse_seq_steps.parse_next(&mut s).is_err());
    }

    #[test]
    fn match_step_three_or_branches() {
        // `||` 分支不限两个：三路分支列表 + `;` 终结
        let mut s = "a | count >= 1 || b | count >= 2 || c | sum >= 3;";
        let step = match_step.parse_next(&mut s).expect("three || branches");
        assert_eq!(step.branches.len(), 3);
        assert_eq!(step.branches[0].source, "a");
        assert_eq!(step.branches[2].source, "c");
        assert!(s.is_empty());
    }

    #[test]
    fn match_step_requires_semicolon() {
        let mut s = "a | count >= 1 || b | count >= 2";
        assert!(match_step.parse_next(&mut s).is_err());
    }
}
