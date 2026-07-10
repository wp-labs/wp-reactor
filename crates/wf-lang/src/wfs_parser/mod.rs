use std::time::Duration;

use crate::parse_utils::duration_value;
use winnow::ascii::multispace1;
use winnow::combinator::{alt, cut_err, delimited, opt, preceded, repeat, separated};
use winnow::error::{AddContext, ContextError, ErrMode, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

pub(crate) mod primitives;
mod validate;

use crate::parse_utils::{ident, quoted_string, ws_skip};
use crate::schema::{FieldDef, FieldType, StaticWindowSchema, WindowSchema};
use crate::{LangReason, LangResult};
use orion_error::conversion::ToStructError;
use primitives::{backtick_ident, base_type_parser, dotted_or_plain_ident};

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Parse a `.wfs` file containing one or more window declarations.
///
/// Returns a list of parsed [`WindowSchema`] values after semantic validation:
/// - Window names must be unique within the file.
/// - If `over > 0`, a `time` attribute is required and the referenced field
///   must exist and have type `time`.
///
/// Parse static (provider) windows from a .wfs file.
/// Only returns windows declared with `window<provider>`.
pub fn parse_static_wfs(input: &str) -> LangResult<Vec<StaticWindowSchema>> {
    let schemas: Vec<StaticWindowSchema> = wfs_file_static.parse(input).map_err(|e| {
        LangReason::Parse
            .to_err()
            .with_detail(format!("parse error: {e}"))
    })?;
    validate::validate_static_schemas(&schemas)?;
    Ok(schemas)
}

pub fn parse_wfs(input: &str) -> LangResult<Vec<WindowSchema>> {
    let parsed = wfs_file.parse(input).map_err(|e| {
        LangReason::Parse
            .to_err()
            .with_detail(format!("parse error: {e}"))
    })?;

    validate::validate_static_schemas(&parsed.static_windows)?;
    validate::validate_schemas(&parsed.windows)?;
    Ok(parsed.windows)
}

// ---------------------------------------------------------------------------
// Top-level grammar
// ---------------------------------------------------------------------------

fn wfs_file_static(input: &mut &str) -> ModalResult<Vec<StaticWindowSchema>> {
    ws_skip.parse_next(input)?;
    let schemas: Vec<StaticWindowSchema> = repeat(0.., static_window_decl).parse_next(input)?;
    ws_skip.parse_next(input)?;
    Ok(schemas)
}

struct ParsedWfs {
    windows: Vec<WindowSchema>,
    static_windows: Vec<StaticWindowSchema>,
}

fn wfs_file(input: &mut &str) -> ModalResult<ParsedWfs> {
    ws_skip.parse_next(input)?;
    let mut windows = Vec::new();
    let mut static_windows = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if input.is_empty() {
            break;
        }
        // Try flow window first; if cut_err triggers, try static window
        let saved = *input;
        if let Ok(w) = window_decl.parse_next(&mut *input) {
            windows.push(w);
            continue;
        }
        *input = saved;
        if let Ok(sw) = static_window_decl.parse_next(&mut *input) {
            windows.push(sw.to_flow_schema());
            static_windows.push(sw);
            continue;
        }
        break;
    }
    Ok(ParsedWfs {
        windows,
        static_windows,
    })
}

/// Parse `window<provider> name { fields { ... } }`
fn static_window_decl(input: &mut &str) -> ModalResult<StaticWindowSchema> {
    ws_skip.parse_next(input)?;
    literal("window").parse_next(input)?;
    literal("<provider>").parse_next(input)?;
    let _ = multispace1.parse_next(input)?;
    let name = cut_err(ident).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let fields = cut_err(fields_block).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("}")).parse_next(input)?;
    Ok(StaticWindowSchema {
        name: name.to_string(),
        fields,
    })
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
    let mut has_stream_tag = false;

    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        if let Some(s) = opt(stream_tag_attr).parse_next(input)? {
            if has_stream_tag {
                return Err(ErrMode::Cut(ContextError::new().add_context(
                    input,
                    &input.checkpoint(),
                    StrContext::Expected(StrContextValue::Description(
                        "duplicate 'stream_tag' attribute",
                    )),
                )));
            }
            has_stream_tag = true;
            streams.extend(s);
        } else if let Some(t) = opt(time_attr).parse_next(input)? {
            if time_field.is_some() {
                return Err(ErrMode::Cut(ContextError::new().add_context(
                    input,
                    &input.checkpoint(),
                    StrContext::Expected(StrContextValue::Description(
                        "duplicate 'time' attribute",
                    )),
                )));
            }
            time_field = Some(t);
        } else if let Some(o) = opt(over_attr).parse_next(input)? {
            if over.is_some() {
                return Err(ErrMode::Cut(ContextError::new().add_context(
                    input,
                    &input.checkpoint(),
                    StrContext::Expected(StrContextValue::Description(
                        "duplicate 'over' attribute",
                    )),
                )));
            }
            over = Some(o);
        } else if let Some(f) = opt(fields_block).parse_next(input)? {
            if fields.is_some() {
                return Err(ErrMode::Cut(ContextError::new().add_context(
                    input,
                    &input.checkpoint(),
                    StrContext::Expected(StrContextValue::Description("duplicate 'fields' block")),
                )));
            }
            fields = Some(f);
        } else {
            return Err(ErrMode::Cut(ContextError::new().add_context(
                input,
                &input.checkpoint(),
                StrContext::Expected(StrContextValue::Description(
                    "stream_tag, time, over, or fields",
                )),
            )));
        }
    }

    let fields = fields.ok_or_else(|| {
        ErrMode::Cut(ContextError::new().add_context(
            input,
            &input.checkpoint(),
            StrContext::Expected(StrContextValue::Description("'fields' block is required")),
        ))
    })?;
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

/// `stream_tag = "name"` or `stream_tag = ["a", "b"]`
fn stream_tag_attr(input: &mut &str) -> ModalResult<Vec<String>> {
    literal("stream_tag").parse_next(input)?;
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
        separated(
            1..,
            preceded(ws_skip, quoted_string),
            preceded(ws_skip, literal(",")),
        ),
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
    alt((
        array_type,
        literal("object").value(FieldType::Object),
        base_type_parser.map(FieldType::Base),
    ))
    .parse_next(input)
}

fn array_type(input: &mut &str) -> ModalResult<FieldType> {
    literal("array").parse_next(input)?;
    if opt(literal("/")).parse_next(input)?.is_some() {
        let bt = cut_err(base_type_parser).parse_next(input)?;
        Ok(FieldType::Array(bt))
    } else {
        Ok(FieldType::ArrayAny)
    }
}
