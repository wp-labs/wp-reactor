//! `join` 子句族解析（clauses/ 拆分）: mode / within / reduce / on 条件 /
//! `as label` / `emit at` 及 reduce 度量（top/maxrow/minrow/last, tie）解析。
//! 对外入口 `join_clause` 由 clauses/mod 统一 re-export。

use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{ContextError, ErrMode, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{duration_value, ident, kw, nonneg_integer, ws_skip};

use super::super::expr;
use super::parse_cut_error;

// ---------------------------------------------------------------------------
// join clause
// ---------------------------------------------------------------------------

/// 解析 `within` / `reduce` 修饰语：两者可任一顺序、各至多一次。
/// 每个位置尝试失败后回退输入，保证顺序无关（对齐 join_clause 原行为）。
fn parse_within_or_reduce_modifiers(
    input: &mut &str,
) -> ModalResult<(Option<WithinSpec>, Option<ReduceClause>)> {
    let mut within = None;
    let mut reduce = None;
    loop {
        ws_skip.parse_next(input)?;
        let saved = *input;
        if within.is_none()
            && let Ok(w) = within_clause.parse_next(input)
        {
            within = Some(w);
            continue;
        }
        *input = saved;
        if reduce.is_none()
            && let Ok(r) = reduce_clause.parse_next(input)
        {
            reduce = Some(r);
            continue;
        }
        *input = saved;
        break;
    }
    Ok((within, reduce))
}

/// 解析 `&&` 分隔的连接条件（首个条件为 cut）。
fn parse_join_conditions(input: &mut &str) -> ModalResult<Vec<JoinCondition>> {
    let first = cut_err(join_cond).parse_next(input)?;
    let mut conditions = vec![first];
    while let Some(cond) = parse_join_cond_tail(input)? {
        conditions.push(cond);
    }
    Ok(conditions)
}

/// 额外条件：`&& cond`（缺省 None）。
fn parse_join_cond_tail(input: &mut &str) -> ModalResult<Option<JoinCondition>> {
    ws_skip.parse_next(input)?;
    if opt(literal("&&")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Ok(Some(cut_err(join_cond).parse_next(input)?))
    } else {
        Ok(None)
    }
}

/// 把 `as <label>`（可空）挂到 reduce 上；无 reduce 或 label 已存在均为语法错误。
fn attach_join_label(
    reduce: &mut Option<ReduceClause>,
    label: Option<String>,
) -> Result<(), ErrMode<ContextError>> {
    let Some(label) = label else {
        return Ok(());
    };
    match reduce.as_mut() {
        Some(rc) if rc.label.is_none() => {
            rc.label = Some(label);
            Ok(())
        }
        _ => Err(parse_cut_error()),
    }
}

/// `join WINDOW [mode] [within ...] [reduce ...] on cond [&& cond] [as label] [emit at expr]`
///
/// - `within` / `reduce` 位于 mode 与 `on` 之间，两者可任一顺序、各至多一次；
/// - `as label`（Q9 形态）可跟在 `on` 条件之后；BNF 形态 `reduce ... as label` 也可；
/// - `emit at <expr>` 为 deferred 标记 + 触发点（P1 语法）。
pub(crate) fn join_clause(input: &mut &str) -> ModalResult<JoinClause> {
    let (target_window, mode) = parse_join_head(input)?;

    let (within, mut reduce) = parse_within_or_reduce_modifiers(input)?;
    let conditions = parse_join_on_conditions(input)?;

    // `as label`（Q9 形态）与 `emit at <expr>`（deferred 标记）尾部
    let emit_at = parse_join_tail(input, &mut reduce)?;

    Ok(JoinClause {
        target_window,
        mode,
        conditions,
        within,
        reduce,
        emit_at,
    })
}

/// `join WINDOW [mode]`（mode 缺省 = 纯存在 inner, 设计 D4）。
fn parse_join_head(input: &mut &str) -> ModalResult<(String, JoinMode)> {
    ws_skip.parse_next(input)?;
    kw("join").parse_next(input)?;
    ws_skip.parse_next(input)?;

    let target_window = parse_join_target(input)?;

    ws_skip.parse_next(input)?;
    let mode = opt(join_mode).parse_next(input)?.unwrap_or(JoinMode::Inner);
    Ok((target_window, mode))
}

/// `join` 后的目标窗口名（cut + 诊断上下文）。
fn parse_join_target(input: &mut &str) -> ModalResult<String> {
    cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "target window name",
        )))
        .parse_next(input)
        .map(|s: &str| s.to_string())
}

/// `on cond [&& cond ...]`（首个条件为 cut）。
fn parse_join_on_conditions(input: &mut &str) -> ModalResult<Vec<JoinCondition>> {
    ws_skip.parse_next(input)?;
    cut_err(kw("on"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'on' after join mode",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    parse_join_conditions(input)
}

/// 尾部：`as label`（需 reduce 且未重复）+ `emit at <expr>`。
fn parse_join_tail(
    input: &mut &str,
    reduce: &mut Option<ReduceClause>,
) -> ModalResult<Option<Expr>> {
    ws_skip.parse_next(input)?;
    let label = opt(as_label).parse_next(input)?;
    attach_join_label(reduce, label)?;
    ws_skip.parse_next(input)?;
    opt(emit_at_clause).parse_next(input)
}

fn join_mode(input: &mut &str) -> ModalResult<JoinMode> {
    alt((
        (kw("asof"), ws_skip, opt(asof_within)).map(|(_, _, within)| JoinMode::Asof { within }),
        kw("snapshot").map(|_| JoinMode::Snapshot),
        kw("anti").map(|_| JoinMode::Anti),
    ))
    .parse_next(input)
}

fn asof_within(input: &mut &str) -> ModalResult<std::time::Duration> {
    kw("within").parse_next(input)?;
    ws_skip.parse_next(input)?;
    // 非 cut：`asof within [lo, hi]` 时 duration 解析失败须回溯，让位给 interval
    // within 子句（`asof` 降级为无 within 的 mode）。
    duration_value.parse_next(input)
}

/// `within [lo, hi]` 或 `within dur` 糖（≡ `within [-dur, 0s]`）。
fn within_clause(input: &mut &str) -> ModalResult<WithinSpec> {
    kw("within").parse_next(input)?;
    ws_skip.parse_next(input)?;
    if opt(literal("[")).parse_next(input)?.is_some() {
        within_interval(input)
    } else {
        within_duration_sugar(input)
    }
}

/// `within [lo, hi]`（开闭记号由 [`bound_value`] 处理; 方括号已消费）。
fn within_interval(input: &mut &str) -> ModalResult<WithinSpec> {
    ws_skip.parse_next(input)?;
    let lo = cut_err(bound_value).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let hi = cut_err(bound_value).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("]")).parse_next(input)?;
    Ok(WithinSpec { lo, hi })
}

/// `within dur` 糖 ≡ `within [-dur, 0s]`（duration 已消费 `within` 后的词法单元）。
fn within_duration_sugar(input: &mut &str) -> ModalResult<WithinSpec> {
    // `within 10s` 糖 ≡ within [-10s, 0s]
    let dur = cut_err(duration_value).parse_next(input)?;
    Ok(WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Dur { dur, neg: true },
        },
        hi: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: std::time::Duration::ZERO,
                neg: false,
            },
        },
    })
}

/// 区间界：`['<' | '<='] (dur | expr)`。`<` 前缀 = 开区间；`<=`/缺省 = 闭。
fn bound_value(input: &mut &str) -> ModalResult<Bound> {
    let open = parse_bound_marker(input)?;
    let after_marker = *input;
    bound_value_body(input, open, after_marker)
}

/// `['<' | '<=']` 开闭记号（`<=` 闭显式 / `<` 开 / 缺省闭）。
fn parse_bound_marker(input: &mut &str) -> ModalResult<bool> {
    if opt(literal("<=")).parse_next(input)?.is_some() {
        Ok(false)
    } else {
        Ok(opt(literal("<")).parse_next(input)?.is_some())
    }
}

/// 记号之后：先试时长；非时长重置到记号后解析表达式（字段/函数调用）。
fn bound_value_body<'a>(
    input: &mut &'a str,
    open: bool,
    after_marker: &'a str,
) -> ModalResult<Bound> {
    ws_skip.parse_next(input)?;
    if let Ok(dur) = duration_value.parse_next(input) {
        return Ok(Bound {
            open,
            val: BoundVal::Dur { dur, neg: false },
        });
    }
    // 非时长 → 左行绝对时间表达式（字段/函数调用）；重置到记号后解析
    *input = after_marker;
    ws_skip.parse_next(input)?;
    let expr = cut_err(expr::parse_expr).parse_next(input)?;
    Ok(Bound {
        open,
        val: BoundVal::Expr(expr),
    })
}

/// `reduce maxrow(field) [tie(field asc|desc)] | minrow(...) | last(field) | top(N, field)`
fn reduce_clause(input: &mut &str) -> ModalResult<ReduceClause> {
    kw("reduce").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let measure = cut_err(reduce_measure).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let label = opt(as_label).parse_next(input)?;
    Ok(ReduceClause { measure, label })
}

/// 按度量名构造 ReduceMeasure：maxrow/minrow 尾随可选 tie，top 携带 N。
fn finish_reduce_measure(
    name: &str,
    field: FieldRef,
    n: Option<u64>,
    input: &mut &str,
) -> ModalResult<ReduceMeasure> {
    match name {
        "maxrow" => Ok(ReduceMeasure::Maxrow {
            field,
            tie: opt_tie(input)?,
        }),
        "minrow" => Ok(ReduceMeasure::Minrow {
            field,
            tie: opt_tie(input)?,
        }),
        "last" => Ok(ReduceMeasure::Last { field }),
        "top" => Ok(ReduceMeasure::Top {
            n: n.expect("top parsed n"),
            field,
        }),
        _ => Err(parse_cut_error()),
    }
}

fn reduce_measure(input: &mut &str) -> ModalResult<ReduceMeasure> {
    let name = cut_err(ident).parse_next(input)?.to_string();
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let (field, n) = reduce_measure_field(input, &name)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    finish_reduce_measure(&name, field, n, input)
}

/// `(...)` 内的字段/参数：`top(N, field)` 带 N, 其余度量只有字段。
fn reduce_measure_field(input: &mut &str, name: &str) -> ModalResult<(FieldRef, Option<u64>)> {
    if name == "top" {
        let n = cut_err(nonneg_integer).parse_next(input)? as u64;
        ws_skip.parse_next(input)?;
        cut_err(literal(",")).parse_next(input)?;
        ws_skip.parse_next(input)?;
        let field = cut_err(join_field_ref).parse_next(input)?;
        Ok((field, Some(n)))
    } else {
        let field = cut_err(join_field_ref).parse_next(input)?;
        Ok((field, None))
    }
}

/// `tie(field asc|desc)`（闭括号后; 无 tie 关键字 → None）。
fn opt_tie(input: &mut &str) -> ModalResult<Option<TieSpec>> {
    ws_skip.parse_next(input)?;
    if opt(kw("tie")).parse_next(input)?.is_none() {
        return Ok(None);
    }
    ws_skip.parse_next(input)?;
    tie_spec(input).map(Some)
}

/// `tie(...)` 主体（关键字已消费）。
fn tie_spec(input: &mut &str) -> ModalResult<TieSpec> {
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let field = cut_err(join_field_ref).parse_next(input)?;
    let desc = tie_direction(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(TieSpec { field, desc })
}

/// `[, asc | desc]`——tie 方向（缺省 asc; 尾逗号允许）。
fn tie_direction(input: &mut &str) -> ModalResult<bool> {
    ws_skip.parse_next(input)?;
    let _ = opt(literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    Ok(opt(alt((kw("asc").value(false), kw("desc").value(true))))
        .parse_next(input)?
        .unwrap_or(false))
}

/// `as <label>`。
fn as_label(input: &mut &str) -> ModalResult<String> {
    kw("as").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(ident).parse_next(input).map(|s| s.to_string())
}

/// `emit at <expr>`。
fn emit_at_clause(input: &mut &str) -> ModalResult<Expr> {
    kw("emit").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(kw("at")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(expr::parse_expr).parse_next(input)
}

fn join_cond(input: &mut &str) -> ModalResult<JoinCondition> {
    let left = join_field_ref.parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("==")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let right = cut_err(join_field_ref).parse_next(input)?;
    Ok(JoinCondition { left, right })
}

/// Parse a field reference for join conditions: `ident.ident` or `ident`
fn join_field_ref(input: &mut &str) -> ModalResult<FieldRef> {
    let first = ident.parse_next(input)?;
    if opt(literal(".")).parse_next(input)?.is_some() {
        let second = cut_err(ident).parse_next(input)?;
        Ok(FieldRef::Qualified(first.to_string(), second.to_string()))
    } else {
        Ok(FieldRef::Simple(first.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    fn join(input: &str) -> JoinClause {
        let mut s = input;
        join_clause
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("join parse failed for {input:?}: {e:?}"))
    }

    fn join_err(input: &str) {
        let mut s = input;
        assert!(
            join_clause.parse_next(&mut s).is_err(),
            "expected join parse error for {input:?}"
        );
    }

    fn measure(input: &str) -> ReduceMeasure {
        let mut s = input;
        reduce_measure
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("reduce_measure failed for {input:?}: {e:?}"))
    }

    #[test]
    fn join_modifiers_any_order() {
        // within 在前
        let j = join("join w within 10s reduce maxrow(price) on a == w.b");
        assert!(j.within.is_some());
        assert!(matches!(
            j.reduce.as_ref().map(|r| &r.measure),
            Some(ReduceMeasure::Maxrow { .. })
        ));
        // reduce 在前
        let j = join("join w reduce last(x) within 5s on a == w.b");
        assert!(matches!(
            j.reduce.as_ref().map(|r| &r.measure),
            Some(ReduceMeasure::Last { .. })
        ));
        assert!(j.within.is_some());
    }

    #[test]
    fn join_defaults_to_inner_and_accepts_snapshot() {
        let j = join("join w on a.x == w.y");
        assert_eq!(j.mode, JoinMode::Inner);
        assert_eq!(j.conditions.len(), 1);
        assert!(j.within.is_none() && j.reduce.is_none() && j.emit_at.is_none());

        let j = join("join w snapshot on a == w.b");
        assert_eq!(j.mode, JoinMode::Snapshot);
    }

    #[test]
    fn join_parses_multiple_conditions() {
        let j = join("join w on a.x == w.y && a.z == w.q && a.t == w.u");
        assert_eq!(j.conditions.len(), 3);
        assert!(matches!(&j.conditions[1], JoinCondition { .. }));
    }

    #[test]
    fn join_attach_label_requires_reduce() {
        // as label 无 reduce → 语法错误
        join_err("join w on a == w.b as best");
        // reduce 后 as label（BNF 形态）合法
        let j = join("join w reduce minrow(count) as best on a == w.b");
        assert_eq!(
            j.reduce.as_ref().and_then(|r| r.label.as_deref()),
            Some("best")
        );
    }

    #[test]
    fn join_rejects_duplicate_reduce_and_unknown_measure() {
        join_err("join w reduce last(x) reduce last(y) on a == w.b");
        join_err("join w reduce bogus(x) on a == w.b");
    }

    #[test]
    fn reduce_measure_variants() {
        assert!(matches!(measure("last(ts)"), ReduceMeasure::Last { .. }));
        assert!(matches!(
            measure("top(3, dist)"),
            ReduceMeasure::Top { n: 3, .. }
        ));
        match measure("minrow(count) tie(ts desc)") {
            ReduceMeasure::Minrow { tie: Some(tie), .. } => {
                assert!(tie.desc);
                assert_eq!(tie.field, FieldRef::Simple("ts".into()));
            }
            other => panic!("expected Minrow with tie, got {other:?}"),
        }
    }

    #[test]
    fn join_emit_at_and_open_bound_within() {
        let j = join("join w within [a.t, <b.t] on a == w.b emit at a.t");
        assert!(j.emit_at.is_some());
        let w = j.within.expect("within");
        assert!(!w.lo.open && w.hi.open);
    }
}

#[test]
fn within_sugar_interval_and_expr_bounds() {
    let mut s = "within 10s";
    let w = within_clause.parse_next(&mut s).unwrap();
    // `within 10s` 糖 ≡ within [-10s, 0s]
    assert!(matches!(
        (w.lo.val, w.hi.val),
        (
            BoundVal::Dur { neg: true, .. },
            BoundVal::Dur { neg: false, .. }
        )
    ));
    assert!(s.is_empty());

    // 开/闭记号: `<` 开、`<=` 闭（时长界）
    let mut s = "within [<5s, <=10s]";
    let w = within_clause.parse_next(&mut s).unwrap();
    assert!(w.lo.open && !w.hi.open);
    assert!(matches!(w.lo.val, BoundVal::Dur { .. }) && matches!(w.hi.val, BoundVal::Dur { .. }));

    // 非时长界 → 左行绝对时间表达式
    let mut s = "within [a.t, <b.t]";
    let w = within_clause.parse_next(&mut s).unwrap();
    assert!(!w.lo.open && w.hi.open);
    assert!(matches!(w.lo.val, BoundVal::Expr(_)) && matches!(w.hi.val, BoundVal::Expr(_)));
}

#[test]
fn tie_direction_defaults_asc_and_desc_parses() {
    // tie_spec 自 '(' 起解析（'tie' 关键字由 opt_tie 消费）
    let mut s = "(ts)";
    let t = tie_spec.parse_next(&mut s).unwrap();
    assert!(!t.desc, "tie 缺省 asc");
    let mut s = "(ts, desc)";
    let t = tie_spec.parse_next(&mut s).unwrap();
    assert!(t.desc, "尾逗号 + desc");
    assert_eq!(t.field, FieldRef::Simple("ts".into()));
}
