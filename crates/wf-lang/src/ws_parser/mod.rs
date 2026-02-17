use std::time::Duration;

use winnow::ascii::multispace1;
use winnow::combinator::{alt, cut_err, delimited, opt, preceded, repeat, separated};
use winnow::error::{AddContext, ContextError, ErrMode, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

mod primitives;
mod validate;

use crate::schema::{FieldDef, FieldType, WindowSchema};
use primitives::{
    backtick_ident, base_type_parser, dotted_or_plain_ident, duration_value, ident, quoted_string,
    ws_skip,
};

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Parse a `.ws` file containing one or more window declarations.
///
/// Returns a list of parsed [`WindowSchema`] values after semantic validation:
/// - Window names must be unique within the file.
/// - If `over > 0`, a `time` attribute is required and the referenced field
///   must exist and have type `time`.
pub fn parse_ws(input: &str) -> anyhow::Result<Vec<WindowSchema>> {
    let windows = ws_file
        .parse(input)
        .map_err(|e| anyhow::anyhow!("parse error: {e}"))?;
    validate::validate_schemas(&windows)?;
    Ok(windows)
}

// ---------------------------------------------------------------------------
// Top-level grammar
// ---------------------------------------------------------------------------

fn ws_file(input: &mut &str) -> ModalResult<Vec<WindowSchema>> {
    ws_skip.parse_next(input)?;
    let windows: Vec<WindowSchema> = repeat(0.., window_decl).parse_next(input)?;
    ws_skip.parse_next(input)?;
    Ok(windows)
}

fn window_decl(input: &mut &str) -> ModalResult<WindowSchema> {
    ws_skip.parse_next(input)?;
    literal("window")
        .context(StrContext::Label("window keyword"))
        .parse_next(input)?;
    let _ = multispace1.parse_next(input)?;

    let name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "window name",
        )))
        .parse_next(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description("'{'")))
        .parse_next(input)?;

    // Parse attributes and fields block in any order
    let mut streams: Vec<String> = Vec::new();
    let mut time_field: Option<String> = None;
    let mut over: Option<Duration> = None;
    let mut fields: Option<Vec<FieldDef>> = None;

    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        if let Some(s) = opt(stream_attr).parse_next(input)? {
            streams.extend(s);
        } else if let Some(t) = opt(time_attr).parse_next(input)? {
            time_field = Some(t);
        } else if let Some(o) = opt(over_attr).parse_next(input)? {
            over = Some(o);
        } else if let Some(f) = opt(fields_block).parse_next(input)? {
            fields = Some(f);
        } else {
            return Err(ErrMode::Cut(ContextError::new().add_context(
                input,
                &input.checkpoint(),
                StrContext::Expected(StrContextValue::Description(
                    "stream, time, over, or fields",
                )),
            )));
        }
    }

    let fields = fields.unwrap_or_default();
    let over = over.unwrap_or(Duration::ZERO);

    Ok(WindowSchema {
        name: name.to_string(),
        streams,
        time_field,
        over,
        fields,
    })
}

// ---------------------------------------------------------------------------
// Attributes
// ---------------------------------------------------------------------------

/// `stream = "name"` or `stream = ["a", "b"]`
fn stream_attr(input: &mut &str) -> ModalResult<Vec<String>> {
    literal("stream").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("="))
        .context(StrContext::Expected(StrContextValue::Description("'='")))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;

    alt((stream_array, stream_single.map(|s| vec![s]))).parse_next(input)
}

fn stream_single(input: &mut &str) -> ModalResult<String> {
    quoted_string.parse_next(input)
}

fn stream_array(input: &mut &str) -> ModalResult<Vec<String>> {
    delimited(
        literal("["),
        separated(1.., preceded(ws_skip, quoted_string), preceded(ws_skip, literal(","))),
        preceded(ws_skip, literal("]")),
    )
    .parse_next(input)
}

/// `time = field_name`
fn time_attr(input: &mut &str) -> ModalResult<String> {
    literal("time").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("="))
        .context(StrContext::Expected(StrContextValue::Description("'='")))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let name = cut_err(ident).parse_next(input)?;
    Ok(name.to_string())
}

/// `over = 5m` or `over = 0`
fn over_attr(input: &mut &str) -> ModalResult<Duration> {
    literal("over").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("="))
        .context(StrContext::Expected(StrContextValue::Description("'='")))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(duration_value).parse_next(input)
}

// ---------------------------------------------------------------------------
// Fields block
// ---------------------------------------------------------------------------

fn fields_block(input: &mut &str) -> ModalResult<Vec<FieldDef>> {
    literal("fields").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description("'{'")))
        .parse_next(input)?;

    let mut defs = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let field = cut_err(field_decl)
            .context(StrContext::Expected(StrContextValue::Description(
                "field declaration",
            )))
            .parse_next(input)?;
        defs.push(field);
    }
    Ok(defs)
}

fn field_decl(input: &mut &str) -> ModalResult<FieldDef> {
    let name = field_name.parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(":"))
        .context(StrContext::Expected(StrContextValue::Description("':'")))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let ft = cut_err(field_type).parse_next(input)?;
    Ok(FieldDef {
        name,
        field_type: ft,
    })
}

/// Field name: plain ident, dotted ident (`a.b.c`), or backtick-quoted.
fn field_name(input: &mut &str) -> ModalResult<String> {
    alt((backtick_ident, dotted_or_plain_ident)).parse_next(input)
}

fn field_type(input: &mut &str) -> ModalResult<FieldType> {
    alt((array_type, base_type_parser.map(FieldType::Base))).parse_next(input)
}

fn array_type(input: &mut &str) -> ModalResult<FieldType> {
    literal("array").parse_next(input)?;
    cut_err(literal("/"))
        .context(StrContext::Expected(StrContextValue::Description("'/'")))
        .parse_next(input)?;
    let bt = cut_err(base_type_parser).parse_next(input)?;
    Ok(FieldType::Array(bt))
}
