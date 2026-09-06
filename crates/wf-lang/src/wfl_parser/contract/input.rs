use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{duration_value, ident, kw, quoted_string, ws_skip};

use super::super::expr;

// ---------------------------------------------------------------------------
// input block
// ---------------------------------------------------------------------------

/// `input { row(...); ... tick(...); ... }`
pub(super) fn input_block(input: &mut &str) -> ModalResult<Vec<InputStmt>> {
    kw("input").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let mut stmts = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if input.starts_with('}') {
            break;
        }
        let stmt = cut_err(input_stmt)
            .context(StrContext::Expected(StrContextValue::Description(
                "input statement (row or tick)",
            )))
            .parse_next(input)?;
        stmts.push(stmt);
    }

    cut_err(literal("}")).parse_next(input)?;
    Ok(stmts)
}

fn input_stmt(input: &mut &str) -> ModalResult<InputStmt> {
    alt((input_row, input_tick)).parse_next(input)
}

/// `row(IDENT, field = expr, ...);`
fn input_row(input: &mut &str) -> ModalResult<InputStmt> {
    kw("row").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;

    let alias = row_alias_comma(input)?;

    ws_skip.parse_next(input)?;
    let fields = row_fields(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(";")).parse_next(input)?;

    Ok(InputStmt::Row { alias, fields })
}

/// `IDENT,` — `row(` 后的别名至逗号（括号已消费; 前后空白由调用方/本函数处理）。
fn row_alias_comma(input: &mut &str) -> ModalResult<String> {
    ws_skip.parse_next(input)?;
    let alias = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "event alias in row()",
        )))
        .parse_next(input)?
        .to_string();
    ws_skip.parse_next(input)?;
    cut_err(literal(","))
        .context(StrContext::Expected(StrContextValue::Description(
            "',' after alias",
        )))
        .parse_next(input)?;
    Ok(alias)
}

/// `field = expr { "," field = expr } [","]`——row() 内逗号分隔字段列表
/// （首个字段必填；尾逗号允许，随后必须是 `)`）。
fn row_fields(input: &mut &str) -> ModalResult<Vec<FieldAssign>> {
    // First field assignment is required
    let first = cut_err(field_assign).parse_next(input)?;
    let mut fields = vec![first];

    loop {
        ws_skip.parse_next(input)?;
        if opt(literal(",")).parse_next(input)?.is_none() {
            break;
        }
        ws_skip.parse_next(input)?;
        // Trailing comma
        if input.starts_with(')') {
            break;
        }
        let f = cut_err(field_assign).parse_next(input)?;
        fields.push(f);
    }

    Ok(fields)
}

/// `(IDENT | STRING) = expr`
fn field_assign(input: &mut &str) -> ModalResult<FieldAssign> {
    let name = alt((quoted_string, ident.map(|s: &str| s.to_string()))).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("=")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(expr::parse_expr).parse_next(input)?;
    Ok(FieldAssign { name, value })
}

/// `tick(DURATION);`
pub(super) fn input_tick(input: &mut &str) -> ModalResult<InputStmt> {
    kw("tick").parse_next(input)?;
    let dur = tick_duration(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(";")).parse_next(input)?;
    Ok(InputStmt::Tick(dur))
}

/// `(DURATION)` — tick 的括号时长参数（`tick` 关键字已消费）。
fn tick_duration(input: &mut &str) -> ModalResult<std::time::Duration> {
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let dur = cut_err(duration_value)
        .context(StrContext::Expected(StrContextValue::Description(
            "duration value in tick()",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(dur)
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    fn row_of(input: &str) -> InputStmt {
        let mut s = input;
        input_stmt
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("input_stmt failed for {input:?}: {e:?}"))
    }

    fn row_err(input: &str) {
        let mut s = input;
        assert!(
            input_stmt.parse_next(&mut s).is_err(),
            "expected input_stmt error for {input:?}"
        );
    }

    #[test]
    fn row_single_and_multi_fields_with_trailing_comma() {
        let stmt = row_of("row(req, query_id = \"q-1\");");
        match stmt {
            InputStmt::Row { alias, fields } => {
                assert_eq!(alias, "req");
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name, "query_id");
                assert_eq!(fields[0].value, Expr::StringLit("q-1".into()));
            }
            other => panic!("expected Row, got {other:?}"),
        }

        // 多字段 + 尾逗号
        let stmt = row_of("row(e, a = 1, b = \"x\", );");
        match stmt {
            InputStmt::Row { alias, fields } => {
                assert_eq!(alias, "e");
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name, "a");
                assert_eq!(fields[0].value, Expr::Number(1.0));
                assert_eq!(fields[1].name, "b");
                assert_eq!(fields[1].value, Expr::StringLit("x".into()));
            }
            other => panic!("expected Row, got {other:?}"),
        }
    }

    #[test]
    fn row_quoted_field_names_and_expr_values() {
        // 扁平点号字段名用引号；值为表达式（时间字面量经 func_arg_expr? 用字符串即可）
        let stmt = row_of("row(e, \"detail.sha256\" = src.hashes[0], n = -2);");
        match stmt {
            InputStmt::Row { fields, .. } => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name, "detail.sha256");
                assert!(matches!(
                    fields[0].value,
                    Expr::Field(FieldRef::Path { .. })
                ));
                assert_eq!(fields[1].name, "n");
                assert_eq!(fields[1].value, Expr::Neg(Box::new(Expr::Number(2.0))));
            }
            other => panic!("expected Row, got {other:?}"),
        }
    }

    #[test]
    fn tick_duration_stmt() {
        let mut s = "tick(31s);";
        match input_stmt.parse_next(&mut s) {
            Ok(InputStmt::Tick(d)) => assert_eq!(d, std::time::Duration::from_secs(31)),
            Ok(other) => panic!("expected Tick, got {other:?}"),
            Err(e) => panic!("tick parse failed: {e:?}"),
        }
    }

    #[test]
    fn row_errors() {
        row_err("row(req 1);"); // 别名后缺 `,`
        row_err("row(req,);"); // 至少一个字段
        row_err("row(req, a = 1"); // 缺 `)`
        row_err("row(req, a);"); // 缺 `=`
        row_err("tick(5);"); // 非 duration 字面量
    }
}
