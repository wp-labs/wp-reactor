use std::time::Duration;

use winnow::ascii::multispace0;
use winnow::combinator::{alt, opt};
use winnow::error::{ContextError, ErrMode, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::{literal, take_while};

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

pub fn ident<'a>(input: &mut &'a str) -> ModalResult<&'a str> {
    // First character must be alphabetic or underscore (not digit).
    if !input.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
        return Err(ErrMode::Backtrack(ContextError::new()));
    }
    take_while(1.., |c: char| c.is_ascii_alphanumeric() || c == '_').parse_next(input)
}

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

pub fn quoted_string(input: &mut &str) -> ModalResult<String> {
    literal("\"").parse_next(input)?;
    let content = take_while(0.., |c: char| c != '"').parse_next(input)?;
    winnow::combinator::cut_err(literal("\""))
        .context(StrContext::Expected(StrContextValue::Description(
            "closing quote",
        )))
        .parse_next(input)?;
    Ok(content.to_string())
}

// ---------------------------------------------------------------------------
// Duration
// ---------------------------------------------------------------------------

pub fn duration_value(input: &mut &str) -> ModalResult<Duration> {
    let digits = take_while(1.., |c: char| c.is_ascii_digit()).parse_next(input)?;
    let num: u64 = digits
        .parse()
        .map_err(|_| ErrMode::Cut(ContextError::new()))?;

    // "0" with no suffix is valid (static collection)
    if num == 0 {
        let _ = opt(alt((
            literal("s"),
            literal("m"),
            literal("h"),
            literal("d"),
        )))
        .parse_next(input)?;
        return Ok(Duration::ZERO);
    }

    let suffix = alt((
        literal("s").value(1u64),
        literal("m").value(60u64),
        literal("h").value(3600u64),
        literal("d").value(86400u64),
    ))
    .context(StrContext::Expected(StrContextValue::Description(
        "duration suffix (s|m|h|d)",
    )))
    .parse_next(input)?;

    Ok(Duration::from_secs(num * suffix))
}

// ---------------------------------------------------------------------------
// Whitespace & comments
// ---------------------------------------------------------------------------

/// Skip whitespace and `# ...` line comments.
pub fn ws_skip(input: &mut &str) -> ModalResult<()> {
    loop {
        let _ = multispace0.parse_next(input)?;
        if opt(literal("#")).parse_next(input)?.is_some() {
            let _ = take_while(0.., |c: char| c != '\n').parse_next(input)?;
        } else {
            break;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Keyword matching with word boundary
// ---------------------------------------------------------------------------

/// Match an exact keyword string, ensuring it's not a prefix of a longer
/// identifier (i.e. the next character is not alphanumeric or `_`).
pub fn kw<'a>(keyword: &'static str) -> impl FnMut(&mut &'a str) -> ModalResult<()> {
    move |input: &mut &'a str| {
        let saved = *input;
        literal(keyword).parse_next(input)?;
        if input.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
            *input = saved;
            return Err(ErrMode::Backtrack(ContextError::new()));
        }
        Ok(())
    }
}

/// Parse a non-negative integer literal (digits only, no decimal point).
pub fn nonneg_integer(input: &mut &str) -> ModalResult<usize> {
    let saved = *input;
    let digits = take_while(1.., |c: char| c.is_ascii_digit()).parse_next(input)?;
    // Reject if followed by decimal point (float, not integer)
    if input.starts_with('.') {
        *input = saved;
        return Err(ErrMode::Backtrack(ContextError::new()));
    }
    digits.parse::<usize>().map_err(|_| {
        *input = saved;
        ErrMode::Backtrack(ContextError::new())
    })
}

// ---------------------------------------------------------------------------
// Number literal
// ---------------------------------------------------------------------------

/// Parse a number literal: integer or float.
pub fn number_literal(input: &mut &str) -> ModalResult<f64> {
    let integer_part = take_while(1.., |c: char| c.is_ascii_digit()).parse_next(input)?;
    let has_dot = opt(literal(".")).parse_next(input)?.is_some();
    if has_dot {
        let frac_part = take_while(1.., |c: char| c.is_ascii_digit())
            .context(StrContext::Expected(StrContextValue::Description(
                "digits after decimal point",
            )))
            .parse_next(input)?;
        let s = format!("{integer_part}.{frac_part}");
        let v: f64 = s.parse().map_err(|_| ErrMode::Cut(ContextError::new()))?;
        Ok(v)
    } else {
        let v: f64 = integer_part
            .parse()
            .map_err(|_| ErrMode::Cut(ContextError::new()))?;
        Ok(v)
    }
}
