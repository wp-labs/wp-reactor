use winnow::combinator::{alt, cut_err, opt, separated};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, number_literal, quoted_string, ws_skip};

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

/// `and_expr = cmp_expr { "&&" cmp_expr }`
fn and_expr(input: &mut &str) -> ModalResult<Expr> {
    let mut left = cmp_expr.parse_next(input)?;
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("&&")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            let right = cut_err(cmp_expr).parse_next(input)?;
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

/// `cmp_expr = add_expr [cmp_op add_expr | "in" "(" list ")" | "not" "in" "(" list ")"]`
fn cmp_expr(input: &mut &str) -> ModalResult<Expr> {
    let left = add_expr.parse_next(input)?;
    ws_skip.parse_next(input)?;

    // Try "not in"
    if opt((kw("not"), ws_skip, kw("in")))
        .parse_next(input)?
        .is_some()
    {
        ws_skip.parse_next(input)?;
        let list = in_list.parse_next(input)?;
        return Ok(Expr::InList {
            expr: Box::new(left),
            list,
            negated: true,
        });
    }

    // Try "in"
    if opt(kw("in")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let list = in_list.parse_next(input)?;
        return Ok(Expr::InList {
            expr: Box::new(left),
            list,
            negated: false,
        });
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

fn in_list(input: &mut &str) -> ModalResult<Vec<Expr>> {
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
        // Number literal
        number_literal.map(Expr::Number),
        // String literal
        quoted_string.map(Expr::StringLit),
        // Boolean literals (keyword-checked)
        kw("true").map(|_| Expr::Bool(true)),
        kw("false").map(|_| Expr::Bool(false)),
        // Parenthesized expression
        paren_expr,
        // Ident-based: field ref or function call
        ident_primary,
    ))
    .context(StrContext::Expected(StrContextValue::Description(
        "expression",
    )))
    .parse_next(input)
}

fn paren_expr(input: &mut &str) -> ModalResult<Expr> {
    literal("(").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let inner = cut_err(parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(inner)
}

/// Parse an ident-based primary: function call or field reference.
fn ident_primary(input: &mut &str) -> ModalResult<Expr> {
    let first = ident.parse_next(input)?;

    // Check what follows the ident
    ws_skip.parse_next(input)?;

    // Case 1: first( ... ) → function call
    if opt(literal("(")).parse_next(input)?.is_some() {
        return parse_func_call_args(None, first.to_string(), input);
    }

    // Case 2: first.second → either qualified func call or field ref
    if opt(literal(".")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let second = cut_err(ident).parse_next(input)?;
        ws_skip.parse_next(input)?;

        // first.second( ... ) → qualified function call
        if opt(literal("(")).parse_next(input)?.is_some() {
            return parse_func_call_args(Some(first.to_string()), second.to_string(), input);
        }

        // first.second → qualified field ref
        return Ok(Expr::Field(FieldRef::Qualified(
            first.to_string(),
            second.to_string(),
        )));
    }

    // Case 3: first["key"] → bracket field ref
    if opt(literal("[")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let key = cut_err(quoted_string).parse_next(input)?;
        ws_skip.parse_next(input)?;
        cut_err(literal("]")).parse_next(input)?;
        return Ok(Expr::Field(FieldRef::Bracketed(first.to_string(), key)));
    }

    // Case 4: bare ident → simple field ref
    Ok(Expr::Field(FieldRef::Simple(first.to_string())))
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
        separated(1.., (ws_skip, parse_expr).map(|(_, e)| e), literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;

    Ok(Expr::FuncCall {
        qualifier,
        name,
        args,
    })
}
