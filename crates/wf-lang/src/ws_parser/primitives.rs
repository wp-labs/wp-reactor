use std::time::Duration;

use winnow::ascii::multispace0;
use winnow::combinator::{alt, cut_err, opt, trace};
use winnow::error::{ContextError, ErrMode, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::{literal, take_while};

use crate::schema::BaseType;

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

pub(super) fn ident<'a>(input: &mut &'a str) -> ModalResult<&'a str> {
    take_while(1.., |c: char| c.is_ascii_alphanumeric() || c == '_').parse_next(input)
}

pub(super) fn dotted_or_plain_ident(input: &mut &str) -> ModalResult<String> {
    let first = ident.parse_next(input)?;
    let mut result = first.to_string();
    while opt(literal(".")).parse_next(input)?.is_some() {
        let next = cut_err(ident).parse_next(input)?;
        result.push('.');
        result.push_str(next);
    }
    Ok(result)
}

pub(super) fn backtick_ident(input: &mut &str) -> ModalResult<String> {
    literal("`").parse_next(input)?;
    let content = take_while(0.., |c: char| c != '`').parse_next(input)?;
    cut_err(literal("`"))
        .context(StrContext::Expected(StrContextValue::Description(
            "closing backtick",
        )))
        .parse_next(input)?;
    Ok(content.to_string())
}

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

pub(super) fn quoted_string(input: &mut &str) -> ModalResult<String> {
    literal("\"").parse_next(input)?;
    let content = take_while(0.., |c: char| c != '"').parse_next(input)?;
    cut_err(literal("\""))
        .context(StrContext::Expected(StrContextValue::Description(
            "closing quote",
        )))
        .parse_next(input)?;
    Ok(content.to_string())
}

// ---------------------------------------------------------------------------
// Duration
// ---------------------------------------------------------------------------

pub(super) fn duration_value(input: &mut &str) -> ModalResult<Duration> {
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
// Types
// ---------------------------------------------------------------------------

pub(super) fn base_type_parser(input: &mut &str) -> ModalResult<BaseType> {
    trace(
        "base_type",
        alt((
            literal("chars").value(BaseType::Chars),
            literal("digit").value(BaseType::Digit),
            literal("float").value(BaseType::Float),
            literal("bool").value(BaseType::Bool),
            literal("time").value(BaseType::Time),
            literal("ip").value(BaseType::Ip),
            literal("hex").value(BaseType::Hex),
        )),
    )
    .context(StrContext::Expected(StrContextValue::Description(
        "type (chars|digit|float|bool|time|ip|hex)",
    )))
    .parse_next(input)
}

// ---------------------------------------------------------------------------
// Whitespace & comments
// ---------------------------------------------------------------------------

/// Skip whitespace and `# ...` comments.
pub(super) fn ws_skip(input: &mut &str) -> ModalResult<()> {
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
