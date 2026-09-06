use winnow::combinator::{cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, ws_skip};

use super::expr;

mod steps;
mod window;

use self::steps::{OnEventBody, key_block, parse_on_event_body};
use self::window::match_params;

/// Parse `match<...> { ... } -> score(expr)`
pub(super) fn match_with_score(input: &mut &str) -> ModalResult<(MatchClause, ScoreExpr)> {
    (
        match_clause_only,
        (ws_skip, cut_err(literal("->"))).context(StrContext::Expected(
            StrContextValue::Description("'->' after match block"),
        )),
        (ws_skip, cut_err(score_expr_only)),
    )
        .map(|(mc, _, (_, score))| (mc, score))
        .parse_next(input)
}

pub(super) fn match_clause_only(input: &mut &str) -> ModalResult<MatchClause> {
    // 五段线性流一次解析：头 → `{` → key/on-event 前段 → body 三形态 → `}`
    (
        parse_match_header,
        (ws_skip, cut_err(literal("{"))),
        parse_match_block_header,
        parse_on_event_body,
        (ws_skip, cut_err(literal("}"))),
    )
        .map(|(header, _, block_header, body, _)| build_match_clause(header, block_header, body))
        .parse_next(input)
}

/// 由三段解析结果组装 `MatchClause`。
fn build_match_clause(
    header: (Vec<FieldRef>, std::time::Duration, WindowMode),
    block_header: (Option<Vec<KeyMapItem>>, bool),
    body: OnEventBody,
) -> MatchClause {
    let (keys, duration, window_mode) = header;
    let (key_mapping, accu) = block_header;
    let (on_event, on_close, seq, match_mode) = body;
    MatchClause {
        keys,
        key_mapping,
        duration,
        window_mode,
        on_event,
        on_close,
        match_mode,
        seq,
        accu,
    }
}

/// `match<keys[:dur][:mode]>` 头——`match` 关键字到参数括号闭合。
fn parse_match_header(
    input: &mut &str,
) -> ModalResult<(Vec<FieldRef>, std::time::Duration, WindowMode)> {
    (
        kw("match"),
        ws_skip,
        cut_err(literal("<")),
        cut_err(match_params),
        cut_err(literal(">")),
    )
        .map(|(_, _, _, (keys, duration, window_mode), _)| (keys, duration, window_mode))
        .parse_next(input)
}

/// `{` 内前段：可选 `key { ... }` 映射块 + `on event [<accu>]`（调用方已消费 `{`）。
fn parse_match_block_header(input: &mut &str) -> ModalResult<(Option<Vec<KeyMapItem>>, bool)> {
    (
        ws_skip,
        opt(key_block),
        ws_skip,
        kw("on"),
        ws_skip,
        cut_err(kw("event")).context(StrContext::Expected(StrContextValue::Description(
            "'event'",
        ))),
        ws_skip,
        opt(accu_param),
    )
        .map(|(_, key_mapping, _, _, _, _, _, accu)| (key_mapping, accu.is_some()))
        .parse_next(input)
}

/// `on event<accu>` — the angle-bracket accumulation marker. The opening `<`
/// must backtrack cleanly (no cut) so a plain `on event` parses normally.
fn accu_param(input: &mut &str) -> ModalResult<bool> {
    (
        literal("<"),
        ws_skip,
        cut_err(kw("accu")),
        ws_skip,
        cut_err(literal(">")),
    )
        .map(|_| true)
        .parse_next(input)
}

pub(super) fn each_clause_only(input: &mut &str) -> ModalResult<EachClause> {
    (
        kw("on"),
        ws_skip,
        cut_err(kw("each")).context(StrContext::Expected(StrContextValue::Description("'each'"))),
        ws_skip,
        cut_err(ident).context(StrContext::Expected(StrContextValue::Description(
            "event alias after `on each`",
        ))),
        ws_skip,
        parse_optional_each_where,
    )
        .map(|(_, _, _, _, alias, _, filter)| EachClause {
            alias: alias.to_string(),
            filter,
        })
        .parse_next(input)
}

/// `where <expr>` 可选过滤（调用方已消费 `where` 前的空白；无 `where` 为 None）。
fn parse_optional_each_where(input: &mut &str) -> ModalResult<Option<Expr>> {
    if opt(kw("where")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Ok(Some(cut_err(expr::parse_expr).parse_next(input)?))
    } else {
        Ok(None)
    }
}

/// `score(expr)`
pub(super) fn score_expr_only(input: &mut &str) -> ModalResult<ScoreExpr> {
    (
        kw("score"),
        ws_skip,
        cut_err(literal("(")),
        ws_skip,
        cut_err(expr::parse_expr),
        ws_skip,
        cut_err(literal(")")),
    )
        .map(|(_, _, _, _, e, _, _)| ScoreExpr { expr: e })
        .parse_next(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    fn clause(input: &str) -> MatchClause {
        let mut s = input;
        match_clause_only
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("match_clause_only failed for {input:?}: {e:?}"))
    }

    fn clause_err(input: &str) {
        let mut s = input;
        assert!(
            match_clause_only.parse_next(&mut s).is_err(),
            "expected match_clause_only error for {input:?}"
        );
    }

    #[test]
    fn bare_ordered_clause_defaults() {
        let mc = clause("match<:5m> { on event { e | count >= 1; } }");
        assert!(mc.keys.is_empty());
        assert_eq!(mc.duration, std::time::Duration::from_secs(300));
        assert!(mc.key_mapping.is_none());
        assert_eq!(mc.on_event.len(), 1);
        assert!(mc.on_close.is_none());
        assert!(mc.seq.is_none());
        assert!(matches!(mc.match_mode, MatchMode::Seq));
        assert!(!mc.accu);
    }

    #[test]
    fn key_block_accu_and_close_modes() {
        let mc = clause(
            "match<:5m> { key { k = e.f; } on event<accu> { e | count >= 1; } \
             and close { e | count >= 2; } }",
        );
        let items = mc.key_mapping.expect("key mapping");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].logical_name, "k");
        assert_eq!(
            items[0].source_field,
            FieldRef::Qualified("e".into(), "f".into())
        );
        assert!(mc.accu, "on event<accu> 应置 accu");
        let close = mc.on_close.expect("and close");
        assert!(matches!(close.mode, CloseMode::And));
        assert_eq!(close.steps.len(), 1);
    }

    #[test]
    fn seq_body_with_keys_and_skip() {
        let mc =
            clause("match<sip:5m> { on event seq consec skip = to_next { not has b within 1s; } }");
        assert_eq!(mc.keys, vec![FieldRef::Simple("sip".into())]);
        assert!(mc.on_event.is_empty());
        let seq = mc.seq.expect("seq clause");
        assert!(seq.consec);
        assert!(matches!(seq.skip, SeqSkip::ToNext));
        assert_eq!(seq.steps.len(), 1);
        assert!(seq.steps[0].neg);
        assert_eq!(seq.steps[0].within, Some(std::time::Duration::from_secs(1)));
    }

    #[test]
    fn any_body_and_errors() {
        let mc = clause("match<:5m> { on event any { a | count >= 1; b | sum >= 2; } }");
        assert!(matches!(mc.match_mode, MatchMode::Any));
        assert_eq!(mc.on_event.len(), 2);

        clause_err("match<:5m { on event { e | count >= 1; } }"); // 缺 `>`
        clause_err("match<:5m> { on event }"); // 缺 body
        clause_err("match<:5m> { key { } }"); // 空 key 块
    }

    fn each(input: &str) -> EachClause {
        let mut s = input;
        each_clause_only
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("each_clause_only failed for {input:?}: {e:?}"))
    }

    #[test]
    fn each_clause_alias_and_optional_where() {
        let ec = each("on each e where e.sip == \"1.2.3.4\"");
        assert_eq!(ec.alias, "e");
        assert!(matches!(
            ec.filter,
            Some(Expr::BinOp {
                op: BinOp::Eq,
                ref left,
                ref right,
            }) if matches!(&**left, Expr::Field(FieldRef::Qualified(q, f)) if q == "e" && f == "sip")
                && matches!(&**right, Expr::StringLit(s) if s == "1.2.3.4")
        ));

        let ec = each("on each b");
        assert_eq!(ec.alias, "b");
        assert!(ec.filter.is_none());
    }

    #[test]
    fn match_with_score_end_to_end() {
        let mut s = "match<:5m> { on event { e | count >= 1; } } -> score(50.0)";
        let (mc, score) = match_with_score
            .parse_next(&mut s)
            .expect("match_with_score");
        assert_eq!(mc.on_event.len(), 1);
        assert_eq!(score.expr, Expr::Number(50.0));
        assert!(s.is_empty());
    }

    #[test]
    fn score_expr_accepts_arithmetic() {
        // score(expr) 内为完整表达式（含系统变量与算术）
        let mut s = "match<:5m> { on event { e | count >= 1; } } -> score(10 + @score)";
        let (_, score) = match_with_score
            .parse_next(&mut s)
            .expect("match_with_score with arithmetic");
        assert!(matches!(&score.expr, Expr::BinOp { op: BinOp::Add, .. }));
        assert!(s.is_empty());
    }
}
