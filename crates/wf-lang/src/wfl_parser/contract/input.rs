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
    ws_skip.parse_next(input)?;

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

    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(";")).parse_next(input)?;

    Ok(InputStmt::Row { alias, fields })
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
fn input_tick(input: &mut &str) -> ModalResult<InputStmt> {
    kw("tick").parse_next(input)?;
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
    ws_skip.parse_next(input)?;
    cut_err(literal(";")).parse_next(input)?;
    Ok(InputStmt::Tick(dur))
}
