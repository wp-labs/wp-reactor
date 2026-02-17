use winnow::combinator::{alt, cut_err, opt, trace};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::{literal, take_while};

use crate::parse_utils::ident;
use crate::schema::BaseType;

// ---------------------------------------------------------------------------
// .ws-specific: identifiers
// ---------------------------------------------------------------------------

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
// .ws-specific: types
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
