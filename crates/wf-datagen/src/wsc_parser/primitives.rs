use winnow::ascii::multispace0;
use winnow::combinator::opt;
use winnow::error::{ContextError, ErrMode, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::{literal, take_while};

use wf_lang::parse_utils::{ident, number_literal};

// ---------------------------------------------------------------------------
// Whitespace & comments (// style for .wsc)
// ---------------------------------------------------------------------------

/// Skip whitespace and `// ...` line comments.
pub fn ws_skip(input: &mut &str) -> ModalResult<()> {
    loop {
        let _ = multispace0.parse_next(input)?;
        if opt(literal("//")).parse_next(input)?.is_some() {
            let _ = take_while(0.., |c: char| c != '\n').parse_next(input)?;
        } else {
            break;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Rate: NUMBER "/" ("s"|"m"|"h")
// ---------------------------------------------------------------------------

use crate::wsc_ast::{Rate, RateUnit};

pub fn rate(input: &mut &str) -> ModalResult<Rate> {
    let num = number_literal(input)?;
    let count = num as u64;
    literal("/").parse_next(input)?;
    let unit = winnow::combinator::alt((
        literal("s").value(RateUnit::PerSecond),
        literal("m").value(RateUnit::PerMinute),
        literal("h").value(RateUnit::PerHour),
    ))
    .context(StrContext::Expected(StrContextValue::Description(
        "rate unit (s|m|h)",
    )))
    .parse_next(input)?;
    Ok(Rate { count, unit })
}

// ---------------------------------------------------------------------------
// Percent: NUMBER "%"
// ---------------------------------------------------------------------------

pub fn percent(input: &mut &str) -> ModalResult<f64> {
    let num = number_literal(input)?;
    literal("%").parse_next(input)?;
    Ok(num)
}

// ---------------------------------------------------------------------------
// Required semicolon
// ---------------------------------------------------------------------------

/// Consume a required trailing `;`.
pub fn semi(input: &mut &str) -> ModalResult<()> {
    literal(";").parse_next(input)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// param_value: number (possibly with duration suffix), or quoted string
// ---------------------------------------------------------------------------

use crate::wsc_ast::ParamValue;
use std::time::Duration;

pub fn param_value(input: &mut &str) -> ModalResult<ParamValue> {
    // Try quoted string first
    if let Some(s) = opt(wf_lang::parse_utils::quoted_string).parse_next(input)? {
        return Ok(ParamValue::String(s));
    }

    // Parse a number, then check for duration suffix
    let saved = *input;
    let num = number_literal(input)?;

    // Check for duration suffix
    if let Some(unit) = opt(winnow::combinator::alt((
        literal("s").value(1u64),
        literal("m").value(60u64),
        literal("h").value(3600u64),
        literal("d").value(86400u64),
    )))
    .parse_next(input)?
    {
        // Make sure suffix isn't part of a longer identifier
        if input.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
            *input = saved;
            return Err(ErrMode::Backtrack(ContextError::new()));
        }
        return Ok(ParamValue::Duration(Duration::from_secs(num as u64 * unit)));
    }

    Ok(ParamValue::Number(num))
}

// ---------------------------------------------------------------------------
// Backtick-quoted identifier
// ---------------------------------------------------------------------------

/// Parse a backtick-quoted identifier: `` `field.name` ``
pub fn backtick_ident(input: &mut &str) -> ModalResult<String> {
    literal("`").parse_next(input)?;
    let content = take_while(0.., |c: char| c != '`').parse_next(input)?;
    winnow::combinator::cut_err(literal("`"))
        .context(StrContext::Expected(StrContextValue::Description(
            "closing backtick",
        )))
        .parse_next(input)?;
    Ok(content.to_string())
}

/// Parse a field name: either backtick-quoted or plain identifier.
pub fn field_name(input: &mut &str) -> ModalResult<String> {
    winnow::combinator::alt((backtick_ident, ident.map(|s: &str| s.to_string()))).parse_next(input)
}

// ---------------------------------------------------------------------------
// gen_expr: literal or gen function call (with named arg support)
// ---------------------------------------------------------------------------

use crate::wsc_ast::{GenArg, GenExpr};

pub fn gen_expr(input: &mut &str) -> ModalResult<GenExpr> {
    // Try quoted string
    if let Some(s) = opt(wf_lang::parse_utils::quoted_string).parse_next(input)? {
        return Ok(GenExpr::StringLit(s));
    }

    // Try number literal (before ident to handle negative numbers)
    let saved = *input;
    if let Ok(n) = number_literal(input) {
        // Make sure it's not followed by something that makes it an ident
        if !input.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
            return Ok(GenExpr::NumberLit(n));
        }
        *input = saved;
    }

    // Parse full identifier — handles booleans and gen function names uniformly.
    // This avoids the backtracking bug where `literal("true")` would partially
    // consume identifiers like `true_x` or `false_flag`.
    let name = ident(input)?.to_string();

    // Check for boolean keywords
    if name == "true" {
        return Ok(GenExpr::BoolLit(true));
    }
    if name == "false" {
        return Ok(GenExpr::BoolLit(false));
    }

    // Gen function: name(args...) or bare name
    ws_skip(input)?;
    if opt(literal("(")).parse_next(input)?.is_some() {
        let mut args = Vec::new();
        ws_skip(input)?;
        if opt(literal(")")).parse_next(input)?.is_none() {
            args.push(gen_arg(input)?);
            loop {
                ws_skip(input)?;
                if opt(literal(",")).parse_next(input)?.is_some() {
                    ws_skip(input)?;
                    args.push(gen_arg(input)?);
                } else {
                    break;
                }
            }
            ws_skip(input)?;
            winnow::combinator::cut_err(literal(")"))
                .context(StrContext::Expected(StrContextValue::Description(
                    "closing parenthesis for gen function",
                )))
                .parse_next(input)?;
        }
        Ok(GenExpr::GenFunc { name, args })
    } else {
        // Bare identifier — treat as gen function with no args
        Ok(GenExpr::GenFunc {
            name,
            args: Vec::new(),
        })
    }
}

/// Parse a single gen function argument, optionally with a name: `name: value` or just `value`.
fn gen_arg(input: &mut &str) -> ModalResult<GenArg> {
    // Try named arg: ident ":"
    let saved = *input;
    if let Ok(name) = ident(input) {
        ws_skip(input)?;
        if opt(literal(":")).parse_next(input)?.is_some() {
            ws_skip(input)?;
            let value = gen_expr(input)?;
            return Ok(GenArg::named(name, value));
        }
        // Not a named arg — backtrack
        *input = saved;
    }

    // Positional arg
    let value = gen_expr(input)?;
    Ok(GenArg::positional(value))
}
