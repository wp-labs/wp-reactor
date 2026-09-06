//! 表达式解析：`expr/` 子模块按职责拆分——本文件保留公共入口与优先级梯
//! （`||` → `&&` → `not` → 比较/`in` → `+-` → `*/%` → 一元负号 → `primary`）；
//! `values.rs`（括号/object/array 字面量）、`ident.rs`（字段引用/函数调用）、
//! `cond.rs`（`if then else` / `case` 模式匹配）各居其位。

mod cond;
mod ident;
mod values;

use winnow::combinator::{alt, cut_err, opt, separated};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, number_literal, quoted_string, ws_skip};

// 父层 `primary` 需要引用的子模块入口。
use self::cond::{case_expr, if_expr};
use self::ident::ident_primary;
use self::values::{array_expr, object_expr, paren_expr};

// ---------------------------------------------------------------------------
// Public entry: full expression
// ---------------------------------------------------------------------------

pub(crate) fn parse_expr(input: &mut &str) -> ModalResult<Expr> {
    or_expr.parse_next(input)
}

/// Parse an expression that stops before `||` and `&&` — used for pipe chain
/// thresholds in match steps where `||` is the branch separator.
pub(crate) fn parse_atomic_expr(input: &mut &str) -> ModalResult<Expr> {
    // Only parse up to additive level (no comparisons or logic)
    // In practice, thresholds are simple values: numbers, field refs, func calls
    unary_expr.parse_next(input)
}

// ---------------------------------------------------------------------------
// Precedence levels (lowest to highest)
// ---------------------------------------------------------------------------

/// `or_expr = and_expr { "||" and_expr }`
fn or_expr(input: &mut &str) -> ModalResult<Expr> {
    let mut left = and_expr.parse_next(input)?;
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("||")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            let right = cut_err(and_expr).parse_next(input)?;
            left = Expr::BinOp {
                op: BinOp::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        } else {
            break;
        }
    }
    Ok(left)
}

/// `and_expr = not_expr { "&&" not_expr }`
fn and_expr(input: &mut &str) -> ModalResult<Expr> {
    let mut left = not_expr.parse_next(input)?;
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("&&")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            let right = cut_err(not_expr).parse_next(input)?;
            left = Expr::BinOp {
                op: BinOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        } else {
            break;
        }
    }
    Ok(left)
}

/// `not_expr = ["not" | "!"] not_expr | cmp_expr`
///
/// 逻辑否定（issue #22）：`not <条件>` / `!<条件>`。放在 `&&`/`||` 之下、
/// 比较之上——`not a == b` 解析为 `not (a == b)`，`not a && not b` 为
/// `(not a) && (not b)`；`x not in (...)` 的 `not in` 仍由 cmp_expr 处理。
fn not_expr(input: &mut &str) -> ModalResult<Expr> {
    ws_skip.parse_next(input)?;
    let negated = opt(kw("not")).parse_next(input)?.is_some()
        || opt(literal("!")).parse_next(input)?.is_some();
    if negated {
        ws_skip.parse_next(input)?;
        let inner = not_expr.parse_next(input)?;
        Ok(Expr::Not(Box::new(inner)))
    } else {
        cmp_expr.parse_next(input)
    }
}

/// `cmp_expr = add_expr [cmp_op add_expr | "in" "(" list ")" | "not" "in" "(" list ")"]`
fn cmp_expr(input: &mut &str) -> ModalResult<Expr> {
    let left = add_expr.parse_next(input)?;
    ws_skip.parse_next(input)?;

    // Try "not in"
    if opt((kw("not"), ws_skip, kw("in")))
        .parse_next(input)?
        .is_some()
    {
        return parse_in_rhs(input, left, true);
    }

    // Try "in"
    if opt(kw("in")).parse_next(input)?.is_some() {
        return parse_in_rhs(input, left, false);
    }

    // Try cmp_op
    if let Some(op) = opt(cmp_op).parse_next(input)? {
        ws_skip.parse_next(input)?;
        let right = cut_err(add_expr).parse_next(input)?;
        return Ok(Expr::BinOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        });
    }

    Ok(left)
}

/// `in` / `not in` 的右侧：裸标识符 = 公共允许列表引用（issue #73，非 cut，
/// 失败回退），否则 `(…)` 字面列表。
fn parse_in_rhs(input: &mut &str, left: Expr, negated: bool) -> ModalResult<Expr> {
    ws_skip.parse_next(input)?;
    let saved = *input;
    if let Ok(name) = ident.parse_next(input)
        && !input.trim_start().starts_with('.')
    {
        // 限定名（`foo.bar`）不是列表引用——回退，让 in_list 报 "expected ("。
        return Ok(Expr::InList {
            expr: Box::new(left),
            list: vec![Expr::ListRef(name.to_string())],
            negated,
        });
    }
    *input = saved;
    let list = in_list.parse_next(input)?;
    Ok(Expr::InList {
        expr: Box::new(left),
        list,
        negated,
    })
}

pub(crate) fn in_list(input: &mut &str) -> ModalResult<Vec<Expr>> {
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let list: Vec<Expr> =
        separated(1.., (ws_skip, parse_expr).map(|(_, e)| e), literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(list)
}

fn cmp_op(input: &mut &str) -> ModalResult<BinOp> {
    alt((
        literal("==").value(BinOp::Eq),
        literal("!=").value(BinOp::Ne),
        literal("<=").value(BinOp::Le),
        literal(">=").value(BinOp::Ge),
        literal("<").value(BinOp::Lt),
        literal(">").value(BinOp::Gt),
    ))
    .parse_next(input)
}

/// `add_expr = mul_expr { ("+" | "-") mul_expr }`
fn add_expr(input: &mut &str) -> ModalResult<Expr> {
    let mut left = mul_expr.parse_next(input)?;
    loop {
        ws_skip.parse_next(input)?;
        if input.starts_with("->") {
            break;
        }
        let op = opt(alt((
            literal("+").value(BinOp::Add),
            literal("-").value(BinOp::Sub),
        )))
        .parse_next(input)?;
        if let Some(op) = op {
            ws_skip.parse_next(input)?;
            let right = cut_err(mul_expr).parse_next(input)?;
            left = Expr::BinOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        } else {
            break;
        }
    }
    Ok(left)
}

/// `mul_expr = unary_expr { ("*" | "/" | "%") unary_expr }`
fn mul_expr(input: &mut &str) -> ModalResult<Expr> {
    let mut left = unary_expr.parse_next(input)?;
    loop {
        ws_skip.parse_next(input)?;
        let op = opt(alt((
            literal("*").value(BinOp::Mul),
            literal("/").value(BinOp::Div),
            literal("%").value(BinOp::Mod),
        )))
        .parse_next(input)?;
        if let Some(op) = op {
            ws_skip.parse_next(input)?;
            let right = cut_err(unary_expr).parse_next(input)?;
            left = Expr::BinOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        } else {
            break;
        }
    }
    Ok(left)
}

/// `unary_expr = ["-"] primary`
fn unary_expr(input: &mut &str) -> ModalResult<Expr> {
    if opt(literal("-")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let inner = primary.parse_next(input)?;
        Ok(Expr::Neg(Box::new(inner)))
    } else {
        primary.parse_next(input)
    }
}

// ---------------------------------------------------------------------------
// Primary
// ---------------------------------------------------------------------------

fn primary(input: &mut &str) -> ModalResult<Expr> {
    alt((
        alt((
            number_literal.map(Expr::Number),
            quoted_string.map(Expr::StringLit),
            kw("true").map(|_| Expr::Bool(true)),
            kw("false").map(|_| Expr::Bool(false)),
            system_var,
            preset_param,
        )),
        alt((
            if_expr,
            case_expr,
            object_expr,
            array_expr,
            paren_expr,
            ident_primary,
        )),
    ))
    .context(StrContext::Expected(StrContextValue::Description(
        "expression",
    )))
    .parse_next(input)
}

fn preset_param(input: &mut &str) -> ModalResult<Expr> {
    literal("$").parse_next(input)?;
    let name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "yield preset parameter name",
        )))
        .parse_next(input)?;
    Ok(Expr::PresetParam(name.to_string()))
}

fn system_var(input: &mut &str) -> ModalResult<Expr> {
    literal("@").parse_next(input)?;
    let name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "system variable name",
        )))
        .parse_next(input)?;
    match name {
        "score" => Ok(Expr::SystemVar(SystemVar::Score)),
        "event_first_time" => Ok(Expr::SystemVar(SystemVar::EventFirstTime)),
        "event_last_time" => Ok(Expr::SystemVar(SystemVar::EventLastTime)),
        "evidence_start_time" => Ok(Expr::SystemVar(SystemVar::EvidenceStartTime)),
        "evidence_end_time" => Ok(Expr::SystemVar(SystemVar::EvidenceEndTime)),
        "window_start_time" => Ok(Expr::SystemVar(SystemVar::WindowStartTime)),
        "window_end_time" => Ok(Expr::SystemVar(SystemVar::WindowEndTime)),
        "emit_time" => Ok(Expr::SystemVar(SystemVar::EmitTime)),
        "first_match_time" => Ok(Expr::SystemVar(SystemVar::FirstMatchTime)),
        _ => crate::wfu_meta::WfuMetaField::from_name(name)
            .filter(|field| field.available_in_yield())
            .map(Expr::WfuMeta)
            .ok_or_else(|| winnow::error::ErrMode::Cut(winnow::error::ContextError::new())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    fn expr_of(input: &str) -> Expr {
        let mut s = input;
        parse_expr
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("parse_expr failed for {input:?}: {e:?}"))
    }

    fn expr_err(input: &str) {
        let mut s = input;
        assert!(
            parse_expr.parse_next(&mut s).is_err(),
            "expected parse_expr error for {input:?}"
        );
    }

    #[test]
    fn comparison_ops_parse() {
        match expr_of("x >= 3") {
            Expr::BinOp {
                op, left, right, ..
            } => {
                assert_eq!(op, BinOp::Ge);
                assert_eq!(*left, Expr::Field(FieldRef::Simple("x".into())));
                assert_eq!(*right, Expr::Number(3.0));
            }
            other => panic!("expected BinOp, got {other:?}"),
        }
    }

    #[test]
    fn in_literal_list_negation() {
        match expr_of("x not in (1, 2)") {
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                assert_eq!(*expr, Expr::Field(FieldRef::Simple("x".into())));
                assert_eq!(list, vec![Expr::Number(1.0), Expr::Number(2.0)]);
                assert!(negated);
            }
            other => panic!("expected InList, got {other:?}"),
        }
    }

    #[test]
    fn in_shared_list_name_is_list_ref_issue_73() {
        // `not in <bare-name>` 公共允许列表引用：解析期产出 ListRef，编译期展开。
        match expr_of("src not in allowed_ips") {
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                assert_eq!(*expr, Expr::Field(FieldRef::Simple("src".into())));
                assert_eq!(list, vec![Expr::ListRef("allowed_ips".into())]);
                assert!(negated);
            }
            other => panic!("expected InList(ListRef), got {other:?}"),
        }
        // 正向 `in` 同样走裸名列表引用。
        match expr_of("src in allow") {
            Expr::InList { list, negated, .. } => {
                assert_eq!(list, vec![Expr::ListRef("allow".into())]);
                assert!(!negated);
            }
            other => panic!("expected InList(ListRef), got {other:?}"),
        }
    }

    #[test]
    fn qualified_name_is_not_a_list_ref() {
        // `a.b` 不是列表名：回退到 `(...)` 字面列表并报错。
        expr_err("x in a.b");
    }

    #[test]
    fn logic_precedence_or_looser_than_and() {
        match expr_of("a || b && c") {
            Expr::BinOp {
                op: BinOp::Or,
                left,
                right,
                ..
            } => {
                assert_eq!(*left, Expr::Field(FieldRef::Simple("a".into())));
                assert_eq!(
                    *right,
                    Expr::BinOp {
                        op: BinOp::And,
                        left: Box::new(Expr::Field(FieldRef::Simple("b".into()))),
                        right: Box::new(Expr::Field(FieldRef::Simple("c".into()))),
                    }
                );
            }
            other => panic!("expected Or at top, got {other:?}"),
        }
    }

    #[test]
    fn arithmetic_and_unary_neg() {
        match expr_of("1 + 2 * 3") {
            Expr::BinOp { op, .. } => assert_eq!(op, BinOp::Add),
            other => panic!("expected Add at top, got {other:?}"),
        }
        match expr_of("-x") {
            Expr::Neg(inner) => {
                assert_eq!(*inner, Expr::Field(FieldRef::Simple("x".into())));
            }
            other => panic!("expected Neg, got {other:?}"),
        }
    }

    #[test]
    fn atomic_expr_stops_before_comparison() {
        let mut s = "x >= 3";
        let e = parse_atomic_expr
            .parse_next(&mut s)
            .expect("parse_atomic_expr should parse the field ref");
        assert_eq!(e, Expr::Field(FieldRef::Simple("x".into())));
        assert_eq!(s, ">= 3");
    }

    #[test]
    fn list_ref_via_in_list_helper() {
        // in_list 是 `expr in (...)` 的共享右值解析器（`pub(crate)`）。
        let mut s = "(\"a\", \"b\")";
        let list = in_list
            .parse_next(&mut s)
            .expect("in_list should parse a parenthesized list");
        assert_eq!(
            list,
            vec![Expr::StringLit("a".into()), Expr::StringLit("b".into())]
        );
        assert!(s.is_empty());
    }
}
