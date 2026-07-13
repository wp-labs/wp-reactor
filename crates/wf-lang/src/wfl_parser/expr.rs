use winnow::combinator::{alt, cut_err, opt, separated};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{duration_value, ident, kw, number_literal, quoted_string, ws_skip};
use crate::schema::{BaseType, FieldType};

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
        )),
        alt((if_expr, object_expr, array_expr, paren_expr, ident_primary)),
    ))
    .context(StrContext::Expected(StrContextValue::Description(
        "expression",
    )))
    .parse_next(input)
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
        _ => Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        )),
    }
}

fn paren_expr(input: &mut &str) -> ModalResult<Expr> {
    literal("(").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let inner = cut_err(parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(inner)
}

fn object_expr(input: &mut &str) -> ModalResult<Expr> {
    kw("object").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

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
    Ok(Expr::Object(items))
}

fn object_item(input: &mut &str) -> ModalResult<ObjectItem> {
    let targets: Vec<String> = separated(1.., preceded_ws_ident, comma_sep).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let type_hint = if opt(literal(":")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Some(cut_err(object_field_type).parse_next(input)?)
    } else {
        None
    };
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

fn array_expr(input: &mut &str) -> ModalResult<Expr> {
    kw("array").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("[")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    if opt(literal("]")).parse_next(input)?.is_some() {
        return Ok(Expr::Array(Vec::new()));
    }

    let items: Vec<Expr> =
        separated(1.., (ws_skip, parse_expr).map(|(_, e)| e), comma_sep).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let _ = opt(literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("]")).parse_next(input)?;
    Ok(Expr::Array(items))
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
        separated(1.., (ws_skip, func_arg_expr).map(|(_, e)| e), literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;

    Ok(Expr::FuncCall {
        qualifier,
        name,
        args,
    })
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

// ---------------------------------------------------------------------------
// Conditional expression
// ---------------------------------------------------------------------------

/// `if expr then expr else expr`
fn if_expr(input: &mut &str) -> ModalResult<Expr> {
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
