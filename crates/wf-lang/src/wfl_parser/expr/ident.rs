//! 标识符起点的基础表达式：字段引用（简单/限定/括号/嵌套 path）与函数调用。
//! 被 `mod.rs` 的 `primary` 引用。

use winnow::ascii::dec_uint;
use winnow::combinator::{cut_err, opt, separated};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{duration_value, ident, quoted_string, ws_skip};

use super::parse_expr;

/// Parse an ident-based primary: function call or field reference.
pub(super) fn ident_primary(input: &mut &str) -> ModalResult<Expr> {
    let first = ident.parse_next(input)?;

    // ws 已被消费，按首个后续字符分派（与 kw/literal 解析天然互斥）。
    ws_skip.parse_next(input)?;
    if opt(literal("(")).parse_next(input)?.is_some() {
        // Case 1: first( ... ) → function call
        return parse_func_call_args(None, first.to_string(), input);
    }
    if opt(literal(".")).parse_next(input)?.is_some() {
        // Case 2: first.second → qualified func call / field ref / nested path
        return parse_dot_field_or_call(first, input);
    }
    if opt(literal("[")).parse_next(input)?.is_some() {
        // Case 3: first["key"] → bracket field ref
        return parse_bracketed_field(first, input);
    }
    // Case 4: bare ident → simple field ref
    Ok(Expr::Field(FieldRef::Simple(first.to_string())))
}

/// 点链后续：`first.second[(…)…]`——要么限定函数调用，要么是字段引用
/// （单层 → Qualified；多层含 `[index]` → Path）。调用方已消费 `.`。
fn parse_dot_field_or_call(first: &str, input: &mut &str) -> ModalResult<Expr> {
    ws_skip.parse_next(input)?;
    let second = cut_err(ident).parse_next(input)?;
    ws_skip.parse_next(input)?;

    // first.second( ... ) → qualified function call
    if opt(literal("(")).parse_next(input)?.is_some() {
        return parse_func_call_args(Some(first.to_string()), second.to_string(), input);
    }

    finish_qualified_or_path(first, second, input)
}

/// `.second` 之后的路径段与形状收敛（非函数调用）：单层 → Qualified，
/// 更深（含 `[index]`）→ Path。
fn finish_qualified_or_path(first: &str, second: &str, input: &mut &str) -> ModalResult<Expr> {
    // Consume further segments: `.ident` members and `[integer]` indices.
    let mut segments = vec![PathSegment::Field(second.to_string())];
    while let Some(segment) = parse_field_path_tail(input)? {
        segments.push(segment);
    }

    // Single level → backward-compatible Qualified; deeper → Path.
    if segments.len() == 1 {
        return Ok(Expr::Field(FieldRef::Qualified(
            first.to_string(),
            second.to_string(),
        )));
    }
    Ok(Expr::Field(FieldRef::Path {
        alias: first.to_string(),
        segments,
    }))
}

/// 方括号字段引用：`first["key"]`（key 为带引号的扁平字段名）。
/// 调用方已消费 `[`。
fn parse_bracketed_field(first: &str, input: &mut &str) -> ModalResult<Expr> {
    ws_skip.parse_next(input)?;
    let key = cut_err(quoted_string).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("]")).parse_next(input)?;
    Ok(Expr::Field(FieldRef::Bracketed(first.to_string(), key)))
}

fn parse_func_call_args(
    qualifier: Option<String>,
    name: String,
    input: &mut &str,
) -> ModalResult<Expr> {
    ws_skip.parse_next(input)?;

    // Empty args?
    if opt(literal(")")).parse_next(input)?.is_some() {
        return Ok(Expr::FuncCall {
            qualifier,
            name,
            args: vec![],
        });
    }

    let args: Vec<Expr> =
        separated(1.., (ws_skip, func_arg_expr).map(|(_, e)| e), literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;

    Ok(Expr::FuncCall {
        qualifier,
        name,
        args,
    })
}

/// 限定路径的后续段：`.member` 或 `[index]`；无后续段返回 `None`。
fn parse_field_path_tail(input: &mut &str) -> ModalResult<Option<PathSegment>> {
    if let Some(segment) = parse_field_member_tail(input)? {
        return Ok(Some(segment));
    }
    parse_index_tail(input)
}

/// `.member` 段；不是 `.` 开头则不消费并返回 `None`。
fn parse_field_member_tail(input: &mut &str) -> ModalResult<Option<PathSegment>> {
    if opt(literal(".")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let seg = cut_err(ident).parse_next(input)?;
        ws_skip.parse_next(input)?;
        Ok(Some(PathSegment::Field(seg.to_string())))
    } else {
        Ok(None)
    }
}

/// `[index]` 段；不是 `[` 开头则不消费并返回 `None`。
fn parse_index_tail(input: &mut &str) -> ModalResult<Option<PathSegment>> {
    if opt(literal("[")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        // Parse directly as `usize` so an index that overflows the
        // platform width is a parse error, not a silent truncation.
        let idx: usize = cut_err(dec_uint).parse_next(input)?;
        ws_skip.parse_next(input)?;
        cut_err(literal("]")).parse_next(input)?;
        ws_skip.parse_next(input)?;
        Ok(Some(PathSegment::Index(idx)))
    } else {
        Ok(None)
    }
}

/// Parse a function argument expression.
/// Allows duration literals (e.g. `5m`, `1h`) which are converted to seconds as Number.
fn func_arg_expr(input: &mut &str) -> ModalResult<Expr> {
    // Try duration literal first (before number, since `5m` starts with digit)
    let saved = *input;
    if let Ok(dur) = duration_value.parse_next(input) {
        // Only accept if followed by `)` or `,` or whitespace — not more ident chars
        let trimmed = input.trim_start();
        if trimmed.starts_with(')') || trimmed.starts_with(',') || trimmed.is_empty() {
            return Ok(Expr::Number(dur.as_secs_f64()));
        }
        // Not a duration in this context, backtrack
        *input = saved;
    } else {
        *input = saved;
    }
    parse_expr(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    fn iprim(input: &str) -> Expr {
        let mut s = input;
        ident_primary
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("ident_primary failed for {input:?}: {e:?}"))
    }

    #[test]
    fn bare_ident_is_simple_field() {
        assert_eq!(iprim("sip"), Expr::Field(FieldRef::Simple("sip".into())));
    }

    #[test]
    fn qualified_field_ref() {
        assert_eq!(
            iprim("fail.sip"),
            Expr::Field(FieldRef::Qualified("fail".into(), "sip".into()))
        );
    }

    #[test]
    fn bracket_field_ref_with_dotted_key() {
        assert_eq!(
            iprim("fail[\"detail.sha256\"]"),
            Expr::Field(FieldRef::Bracketed("fail".into(), "detail.sha256".into()))
        );
    }

    #[test]
    fn func_call_plain_and_empty() {
        assert_eq!(
            iprim("count(x, 3)"),
            Expr::FuncCall {
                qualifier: None,
                name: "count".into(),
                args: vec![Expr::Field(FieldRef::Simple("x".into())), Expr::Number(3.0)],
            }
        );
        assert_eq!(
            iprim("count()"),
            Expr::FuncCall {
                qualifier: None,
                name: "count".into(),
                args: vec![],
            }
        );
    }

    #[test]
    fn qualified_func_call_with_duration_arg() {
        match iprim("calc.ago(5m)") {
            Expr::FuncCall {
                qualifier,
                name,
                args,
            } => {
                assert_eq!(qualifier.as_deref(), Some("calc"));
                assert_eq!(name, "ago");
                assert_eq!(args, vec![Expr::Number(300.0)]);
            }
            other => panic!("expected FuncCall, got {other:?}"),
        }
    }

    #[test]
    fn nested_field_path_with_index() {
        assert_eq!(
            iprim("s.roles_obj.related[0].name"),
            Expr::Field(FieldRef::Path {
                alias: "s".into(),
                segments: vec![
                    PathSegment::Field("roles_obj".into()),
                    PathSegment::Field("related".into()),
                    PathSegment::Index(0),
                    PathSegment::Field("name".into()),
                ],
            })
        );
    }

    #[test]
    fn path_tail_index_must_be_integer() {
        let mut s = "s.a[x]";
        assert!(ident_primary.parse_next(&mut s).is_err());
    }

    #[test]
    fn field_member_tail_consumes_dot_ident_only() {
        let mut s = ".b";
        assert_eq!(
            parse_field_member_tail.parse_next(&mut s).unwrap().unwrap(),
            PathSegment::Field("b".into())
        );
        assert!(s.is_empty());

        // 无 `.` 前缀则不消费任何输入
        let mut s = "b";
        assert!(
            parse_field_member_tail
                .parse_next(&mut s)
                .unwrap()
                .is_none()
        );
        assert_eq!(s, "b");
    }

    #[test]
    fn index_tail_accepts_int_rejects_others() {
        let mut s = "[0]";
        assert_eq!(
            parse_index_tail.parse_next(&mut s).unwrap().unwrap(),
            PathSegment::Index(0)
        );
        assert!(s.is_empty());

        // 非 `[` 开头不消费
        let mut s = "x";
        assert!(parse_index_tail.parse_next(&mut s).unwrap().is_none());

        // 非整数下标 / 超出 usize 宽度均报错（不静默截断）
        let mut s = "[x]";
        assert!(parse_index_tail.parse_next(&mut s).is_err());
        let mut s = "[99999999999999999999999999]";
        assert!(parse_index_tail.parse_next(&mut s).is_err());
    }

    #[test]
    fn func_call_with_complex_args() {
        // 实参支持表达式/对象/嵌套调用
        match iprim("make(e.a + 1, obj)") {
            Expr::FuncCall { name, args, .. } => {
                assert_eq!(name, "make");
                assert_eq!(args.len(), 2);
                assert!(matches!(&args[0], Expr::BinOp { op: BinOp::Add, .. }));
                assert_eq!(args[1], Expr::Field(FieldRef::Simple("obj".into())));
            }
            other => panic!("expected FuncCall, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
