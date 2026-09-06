//! 条件/分支表达式：`if <expr> then <expr> else <expr>` 与
//! `case <expr> { pat1 | pat2 => …, …, _ => default }`。被 `mod.rs` 的
//! `primary` 引用。

use winnow::combinator::{cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{kw, ws_skip};

use super::{and_expr, or_expr, parse_expr};

/// `if expr then expr else expr`
pub(super) fn if_expr(input: &mut &str) -> ModalResult<Expr> {
    kw("if").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let cond = cut_err(parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(kw("then"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'then' after if condition",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let then_e = cut_err(or_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(kw("else"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'else' after then branch",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let else_e = cut_err(or_expr).parse_next(input)?;
    Ok(Expr::IfThenElse {
        cond: Box::new(cond),
        then_expr: Box::new(then_e),
        else_expr: Box::new(else_e),
    })
}

/// case 模式匹配表达式（issue #79 Issue 2；2026-09-01 由 `match` 改名——
/// 避免与规则级 CEP 子句 `match<keys:window> { ... }` 撞名）：
/// `case <expr> { pat1 | pat2 => value, ..., _ => default }`。
///
/// - pattern 用 `and_expr` 以下层级解析——`|` 是分支多模式分隔符，不能被
///   `or_expr` 当作逻辑或吞掉；
/// - arm 以 `,` 分隔，允许尾逗号；`_` 默认分支必须最后（其后不再接受 arm）；
/// - 无匹配且无默认 → 求值 None（与 if 条件非 bool 同语义）。
pub(super) fn case_expr(input: &mut &str) -> ModalResult<Expr> {
    kw("case").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let subject = cut_err(or_expr)
        .context(StrContext::Expected(StrContextValue::Description(
            "case subject expression",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'{' after case subject",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let mut arms = Vec::new();
    let mut default = None;
    // `_` 默认分支（必须最后一个；其后残留内容由 `}` 检查报错）。
    loop {
        ws_skip.parse_next(input)?;
        if input.starts_with('}') {
            break;
        }
        if opt(literal("_")).parse_next(input)?.is_some() {
            default = Some(Box::new(parse_case_default_arm(input)?));
            break;
        }
        arms.push(parse_case_regular_arm(input)?);
        ws_skip.parse_next(input)?;
        if opt(literal(",")).parse_next(input)?.is_some() {
            continue;
        }
        break;
    }
    ws_skip.parse_next(input)?;
    cut_err(literal("}"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'}' closing case block",
        )))
        .parse_next(input)?;
    Ok(Expr::Match {
        expr: Box::new(subject),
        arms,
        default,
    })
}

/// 普通分支：`pat1 | pat2 => <expr>`（不含尾部逗号；逗号由 case_expr 循环消费）。
fn parse_case_regular_arm(input: &mut &str) -> ModalResult<MatchArm> {
    let patterns = parse_case_patterns(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("=>"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'=>' after case patterns",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(parse_expr).parse_next(input)?;
    Ok(MatchArm { patterns, value })
}

/// `_ => <expr> [,]` —— 默认分支值（调用方已消费 `_`）。
fn parse_case_default_arm(input: &mut &str) -> ModalResult<Expr> {
    ws_skip.parse_next(input)?;
    cut_err(literal("=>"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'=>' after case default pattern '_'",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    opt(literal(",")).parse_next(input)?;
    Ok(value)
}

/// 模式列表：`pat1 | pat2 | ...`（and_expr 不含逻辑或）。
fn parse_case_patterns(input: &mut &str) -> ModalResult<Vec<Expr>> {
    let mut patterns = vec![cut_err(and_expr).parse_next(input)?];
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("|")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            patterns.push(cut_err(and_expr).parse_next(input)?);
        } else {
            break;
        }
    }
    Ok(patterns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    fn if_e(input: &str) -> Expr {
        let mut s = input;
        if_expr
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("if_expr failed for {input:?}: {e:?}"))
    }

    fn case_e(input: &str) -> Expr {
        let mut s = input;
        case_expr
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("case_expr failed for {input:?}: {e:?}"))
    }

    fn case_err(input: &str) {
        let mut s = input;
        assert!(
            case_expr.parse_next(&mut s).is_err(),
            "expected case_expr error for {input:?}"
        );
    }

    #[test]
    fn if_then_else_parses_branches() {
        let e = if_e("if score >= 3 then \"high\" else \"low\"");
        match e {
            Expr::IfThenElse {
                cond,
                then_expr,
                else_expr,
            } => {
                assert!(matches!(&*cond, Expr::BinOp { op: BinOp::Ge, .. }));
                assert_eq!(*then_expr, Expr::StringLit("high".into()));
                assert_eq!(*else_expr, Expr::StringLit("low".into()));
            }
            other => panic!("expected IfThenElse, got {other:?}"),
        }
    }

    #[test]
    fn if_missing_then_is_an_error() {
        let mut s = "if x 1 else 2";
        assert!(if_expr.parse_next(&mut s).is_err());
    }

    #[test]
    fn case_multi_pattern_arm_and_default() {
        let e = case_e("case level { 1 | 2 => \"low\", 3 => \"mid\", _ => \"high\" }");
        match e {
            Expr::Match {
                expr,
                arms,
                default,
            } => {
                assert_eq!(*expr, Expr::Field(FieldRef::Simple("level".into())));
                assert_eq!(arms.len(), 2);
                assert_eq!(arms[0].patterns, vec![Expr::Number(1.0), Expr::Number(2.0)]);
                assert_eq!(arms[0].value, Expr::StringLit("low".into()));
                assert_eq!(arms[1].patterns, vec![Expr::Number(3.0)]);
                assert_eq!(
                    *default.expect("case default missing"),
                    Expr::StringLit("high".into())
                );
            }
            other => panic!("expected Match, got {other:?}"),
        }
    }

    #[test]
    fn case_trailing_comma_allowed_after_regular_arm() {
        let e = case_e("case x { 1 => \"a\", 2 => \"b\", }");
        match e {
            Expr::Match { arms, default, .. } => {
                assert_eq!(arms.len(), 2);
                assert!(default.is_none());
            }
            other => panic!("expected Match, got {other:?}"),
        }
    }

    #[test]
    fn case_default_only_arm() {
        let e = case_e("case x { _ => 0 }");
        match e {
            Expr::Match { arms, default, .. } => {
                assert!(arms.is_empty());
                assert_eq!(*default.unwrap(), Expr::Number(0.0));
            }
            other => panic!("expected Match, got {other:?}"),
        }
    }

    #[test]
    fn case_default_must_be_last() {
        case_err("case x { _ => 0, 1 => 2 }");
    }

    #[test]
    fn case_arm_missing_comma_is_an_error() {
        case_err("case x { 1 => \"a\" 2 => \"b\" }");
    }

    #[test]
    fn case_missing_brace_is_an_error() {
        case_err("case x 1 => 2 }");
    }
}
