//! 值字面量/结构化字面量：括号表达式、`object { … }`、`array [ … ]` 及
//! object 字段类型标注（`array/…`、基础类型）。被 `mod.rs` 的 `primary` 引用。

use winnow::combinator::{alt, cut_err, opt, separated};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, ws_skip};
use crate::schema::{BaseType, FieldType};

use super::parse_expr;

pub(super) fn paren_expr(input: &mut &str) -> ModalResult<Expr> {
    literal("(").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let inner = cut_err(parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(inner)
}

pub(super) fn object_expr(input: &mut &str) -> ModalResult<Expr> {
    kw("object").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;
    let items = object_items(input)?;
    Ok(Expr::Object(items))
}

/// `object { ... }` 内条目列表：条目间空白分隔、`;` 可选；收尾 `}` 由本函数消费。
fn object_items(input: &mut &str) -> ModalResult<Vec<ObjectItem>> {
    let mut items = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let item = cut_err(object_item)
            .context(StrContext::Expected(StrContextValue::Description(
                "object item",
            )))
            .parse_next(input)?;
        items.push(item);
    }
    Ok(items)
}

fn object_item(input: &mut &str) -> ModalResult<ObjectItem> {
    let targets: Vec<String> = separated(1.., preceded_ws_ident, comma_sep).parse_next(input)?;
    let type_hint = parse_optional_type_hint(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("=")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let _ = opt(literal(";")).parse_next(input)?;
    Ok(ObjectItem {
        targets,
        type_hint,
        value,
    })
}

/// `[: field_type]` 可选类型标注（调用方已解析 targets；含前置空白）。
fn parse_optional_type_hint(input: &mut &str) -> ModalResult<Option<FieldType>> {
    ws_skip.parse_next(input)?;
    if opt(literal(":")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Ok(Some(cut_err(object_field_type).parse_next(input)?))
    } else {
        Ok(None)
    }
}

fn object_field_type(input: &mut &str) -> ModalResult<FieldType> {
    alt((
        array_field_type,
        kw("object").value(FieldType::Object),
        base_type_parser.map(FieldType::Base),
    ))
    .parse_next(input)
}

fn array_field_type(input: &mut &str) -> ModalResult<FieldType> {
    kw("array").parse_next(input)?;
    ws_skip.parse_next(input)?;
    if opt(literal("/")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let base = cut_err(base_type_parser).parse_next(input)?;
        Ok(FieldType::Array(base))
    } else {
        Ok(FieldType::ArrayAny)
    }
}

fn base_type_parser(input: &mut &str) -> ModalResult<BaseType> {
    alt((
        kw("chars").value(BaseType::Chars),
        kw("digit").value(BaseType::Digit),
        kw("float").value(BaseType::Float),
        kw("bool").value(BaseType::Bool),
        kw("time").value(BaseType::Time),
        kw("ip").value(BaseType::Ip),
        kw("hex").value(BaseType::Hex),
    ))
    .parse_next(input)
}

pub(super) fn array_expr(input: &mut &str) -> ModalResult<Expr> {
    kw("array").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("[")).parse_next(input)?;
    let items = array_items(input)?;
    Ok(Expr::Array(items))
}

/// `array [ ... ]` 的元素：空 `]` 即空数组；元素逗号分隔、允许尾逗号；
/// 收尾 `]` 由本函数消费。
fn array_items(input: &mut &str) -> ModalResult<Vec<Expr>> {
    ws_skip.parse_next(input)?;
    if opt(literal("]")).parse_next(input)?.is_some() {
        return Ok(Vec::new());
    }

    let items: Vec<Expr> =
        separated(1.., (ws_skip, parse_expr).map(|(_, e)| e), comma_sep).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let _ = opt(literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("]")).parse_next(input)?;
    Ok(items)
}

fn preceded_ws_ident(input: &mut &str) -> ModalResult<String> {
    ws_skip.parse_next(input)?;
    ident.map(ToString::to_string).parse_next(input)
}

fn comma_sep(input: &mut &str) -> ModalResult<()> {
    ws_skip.parse_next(input)?;
    literal(",").parse_next(input)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    fn obj(input: &str) -> Expr {
        let mut s = input;
        object_expr
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("object_expr failed for {input:?}: {e:?}"))
    }

    #[test]
    fn object_literal_with_type_hints() {
        match obj("object { a = 1; b: array/chars = array [\"x\", \"y\"]; }") {
            Expr::Object(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].targets, vec!["a"]);
                assert_eq!(items[0].type_hint, None);
                assert_eq!(items[0].value, Expr::Number(1.0));
                assert_eq!(items[1].targets, vec!["b"]);
                assert_eq!(items[1].type_hint, Some(FieldType::Array(BaseType::Chars)));
                match &items[1].value {
                    Expr::Array(vals) => {
                        assert_eq!(
                            *vals,
                            vec![Expr::StringLit("x".into()), Expr::StringLit("y".into())]
                        );
                    }
                    other => panic!("expected Array value, got {other:?}"),
                }
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn object_item_without_semicolon_is_allowed() {
        match obj("object { a = 1 }") {
            Expr::Object(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].value, Expr::Number(1.0));
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn array_literal_empty_and_trailing_comma() {
        let mut s = "array [1, 2, ]";
        match array_expr.parse_next(&mut s) {
            Ok(Expr::Array(vals)) => assert_eq!(vals, vec![Expr::Number(1.0), Expr::Number(2.0)]),
            Ok(other) => panic!("expected Array, got {other:?}"),
            Err(e) => panic!("array parse failed: {e:?}"),
        }

        let mut s = "array []";
        match array_expr.parse_next(&mut s) {
            Ok(Expr::Array(vals)) => assert!(vals.is_empty()),
            Ok(other) => panic!("expected empty Array, got {other:?}"),
            Err(e) => panic!("empty array parse failed: {e:?}"),
        }
    }

    #[test]
    fn paren_expr_returns_grouped_inner_expr() {
        let mut s = "(1 + 2)";
        match paren_expr.parse_next(&mut s) {
            Ok(Expr::BinOp {
                op, left, right, ..
            }) => {
                assert_eq!(op, BinOp::Add);
                assert_eq!(*left, Expr::Number(1.0));
                assert_eq!(*right, Expr::Number(2.0));
            }
            Ok(other) => panic!("expected grouped BinOp, got {other:?}"),
            Err(e) => panic!("paren parse failed: {e:?}"),
        }
    }

    #[test]
    fn object_item_multi_targets_and_type_hints() {
        // 多个逻辑名共用一个值（`,` 分隔 targets）
        match obj("object { x, y: float = 1; }") {
            Expr::Object(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].targets, vec!["x", "y"]);
                assert_eq!(items[0].type_hint, Some(FieldType::Base(BaseType::Float)));
                assert_eq!(items[0].value, Expr::Number(1.0));
            }
            other => panic!("expected Object, got {other:?}"),
        }

        // 嵌套 object 类型标注 + object 值
        match obj("object { geo: object = object { country = \"CN\"; }; }") {
            Expr::Object(items) => {
                assert_eq!(items[0].type_hint, Some(FieldType::Object));
                match &items[0].value {
                    Expr::Object(inner) => {
                        assert_eq!(inner[0].targets, vec!["country"]);
                        assert_eq!(inner[0].value, Expr::StringLit("CN".into()));
                    }
                    other => panic!("expected inner Object, got {other:?}"),
                }
            }
            other => panic!("expected Object, got {other:?}"),
        }

        // array（无 /base）→ ArrayAny
        match obj("object { t: array = array [1]; }") {
            Expr::Object(items) => {
                assert_eq!(items[0].type_hint, Some(FieldType::ArrayAny));
                assert_eq!(items[0].value, Expr::Array(vec![Expr::Number(1.0)]));
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }
}
