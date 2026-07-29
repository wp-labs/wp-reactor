use winnow::combinator::{alt, cut_err, opt, separated};
use winnow::error::{ContextError, ErrMode, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{duration_value, ident, kw, nonneg_integer, quoted_string, ws_skip};

use super::expr;

// ---------------------------------------------------------------------------
// entity clause
// ---------------------------------------------------------------------------

pub(super) fn entity_clause(input: &mut &str) -> ModalResult<EntityClause> {
    kw("entity").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;

    // entity_type: ident or string literal
    let entity_type = alt((
        quoted_string.map(EntityTypeVal::StringLit),
        ident.map(|s: &str| EntityTypeVal::Ident(s.to_string())),
    ))
    .parse_next(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let id_expr = cut_err(expr::parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;

    Ok(EntityClause {
        entity_type,
        id_expr,
    })
}

// ---------------------------------------------------------------------------
// yield clause
// ---------------------------------------------------------------------------

pub(super) fn yield_preset_decl(input: &mut &str) -> ModalResult<YieldPresetDecl> {
    ws_skip.parse_next(input)?;
    kw("yield").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(kw("preset")).parse_next(input)?;
    ws_skip.parse_next(input)?;

    let name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "yield preset name",
        )))
        .parse_next(input)?
        .to_string();

    ws_skip.parse_next(input)?;
    let params = opt(yield_preset_params)
        .parse_next(input)?
        .unwrap_or_default();
    ws_skip.parse_next(input)?;
    let args = cut_err(named_args_parens).parse_next(input)?;

    Ok(YieldPresetDecl { name, params, args })
}

pub(super) fn yield_clause(input: &mut &str) -> ModalResult<YieldClause> {
    kw("yield").parse_next(input)?;
    ws_skip.parse_next(input)?;

    let target = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "yield target window name",
        )))
        .parse_next(input)?
        .to_string();

    // Optional version: @vN
    let version = if opt(literal("@")).parse_next(input)?.is_some() {
        cut_err(literal("v")).parse_next(input)?;
        let n = cut_err(nonneg_integer).parse_next(input)?;
        Some(n as u32)
    } else {
        None
    };

    ws_skip.parse_next(input)?;
    let presets = if opt(literal(":")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        separated(1.., yield_preset_ref, (ws_skip, literal(","), ws_skip)).parse_next(input)?
    } else {
        Vec::new()
    };

    ws_skip.parse_next(input)?;
    let args = cut_err(named_args_parens).parse_next(input)?;

    Ok(YieldClause {
        target,
        version,
        presets,
        args,
    })
}

fn named_args_parens(input: &mut &str) -> ModalResult<Vec<NamedArg>> {
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let args: Vec<NamedArg> =
        separated(0.., named_arg, (ws_skip, literal(","), ws_skip)).parse_next(input)?;
    // Allow trailing comma
    ws_skip.parse_next(input)?;
    let _ = opt(literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(args)
}

fn yield_preset_params(input: &mut &str) -> ModalResult<Vec<YieldPresetParam>> {
    let body = parse_angle_body(input, starts_named_args_parens)?;
    parse_preset_param_items(body)
}

fn parse_angle_body<'a>(
    input: &mut &'a str,
    close_is_valid: impl Fn(&str) -> bool,
) -> ModalResult<&'a str> {
    literal("<").parse_next(input)?;
    let body_start = *input;
    let Some(close_idx) = find_angle_close(body_start, close_is_valid) else {
        return Err(parse_cut_error());
    };
    let body = &body_start[..close_idx];
    *input = &body_start[close_idx + 1..];
    Ok(body)
}

fn parse_preset_param_items(body: &str) -> ModalResult<Vec<YieldPresetParam>> {
    let mut rest = body;
    ws_skip.parse_next(&mut rest)?;
    if rest.is_empty() {
        return Ok(Vec::new());
    }

    let params: Vec<YieldPresetParam> = separated(1.., preset_param_item, angle_comma_sep)
        .parse_next(&mut rest)
        .map_err(|_| parse_cut_error())?;
    ws_skip.parse_next(&mut rest)?;
    let _ = opt(angle_comma_sep).parse_next(&mut rest)?;
    ws_skip.parse_next(&mut rest)?;
    if rest.is_empty() {
        Ok(params)
    } else {
        Err(parse_cut_error())
    }
}

fn preset_param_item(input: &mut &str) -> ModalResult<YieldPresetParam> {
    ws_skip.parse_next(input)?;
    let name = ident.parse_next(input)?.to_string();
    ws_skip.parse_next(input)?;
    let default = if opt(literal("=")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Some(cut_err(expr::parse_expr).parse_next(input)?)
    } else {
        None
    };
    Ok(YieldPresetParam { name, default })
}

fn parse_preset_ref_arg_items(body: &str) -> ModalResult<Vec<Expr>> {
    let mut rest = body;
    ws_skip.parse_next(&mut rest)?;
    if rest.is_empty() {
        return Ok(Vec::new());
    }

    let args: Vec<Expr> = separated(1.., preset_ref_arg_item, angle_comma_sep)
        .parse_next(&mut rest)
        .map_err(|_| parse_cut_error())?;
    ws_skip.parse_next(&mut rest)?;
    let _ = opt(angle_comma_sep).parse_next(&mut rest)?;
    ws_skip.parse_next(&mut rest)?;
    if rest.is_empty() {
        Ok(args)
    } else {
        Err(parse_cut_error())
    }
}

fn preset_ref_arg_item(input: &mut &str) -> ModalResult<Expr> {
    ws_skip.parse_next(input)?;
    expr::parse_expr.parse_next(input)
}

fn angle_comma_sep(input: &mut &str) -> ModalResult<()> {
    ws_skip.parse_next(input)?;
    literal(",").parse_next(input)?;
    ws_skip.parse_next(input)?;
    Ok(())
}

fn yield_preset_ref(input: &mut &str) -> ModalResult<YieldPresetRef> {
    let name = ident.map(str::to_string).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let args = opt(yield_preset_ref_args)
        .parse_next(input)?
        .unwrap_or_default();
    Ok(YieldPresetRef { name, args })
}

fn yield_preset_ref_args(input: &mut &str) -> ModalResult<Vec<Expr>> {
    let body = parse_angle_body(input, |after| {
        after.starts_with(',') || starts_named_args_parens(after)
    })?;
    parse_preset_ref_arg_items(body)
}

fn find_angle_close(body_start: &str, close_is_valid: impl Fn(&str) -> bool) -> Option<usize> {
    let bytes = body_start.as_bytes();
    let mut i = 0;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'"' => i = skip_string(body_start, i),
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'/' => i = skip_line_comment(bytes, i),
            b'(' => {
                paren_depth += 1;
                i += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                i += 1;
            }
            b'[' => {
                bracket_depth += 1;
                i += 1;
            }
            b']' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                i += 1;
            }
            b'{' => {
                brace_depth += 1;
                i += 1;
            }
            b'}' => {
                brace_depth = brace_depth.saturating_sub(1);
                i += 1;
            }
            b'>' if paren_depth == 0
                && bracket_depth == 0
                && brace_depth == 0
                && close_is_valid(skip_ws_and_line_comments_str(&body_start[i + 1..])) =>
            {
                return Some(i);
            }
            _ => i += 1,
        }
    }
    None
}

fn skip_ws_and_line_comments_str(value: &str) -> &str {
    let i = skip_ws_and_line_comments(value.as_bytes(), 0);
    &value[i..]
}

fn starts_named_args_parens(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut i = skip_ws_and_line_comments(bytes, 0);
    if i >= bytes.len() || bytes[i] != b'(' {
        return false;
    }
    i += 1;
    i = skip_ws_and_line_comments(bytes, i);
    if i < bytes.len() && bytes[i] == b')' {
        return true;
    }
    if i >= bytes.len() || !is_ident_start_byte(bytes[i]) {
        return false;
    }
    while i < bytes.len() && is_ident_cont_byte(bytes[i]) {
        i += 1;
    }
    i = skip_ws_and_line_comments(bytes, i);
    i < bytes.len() && bytes[i] == b'='
}

fn skip_ws_and_line_comments(bytes: &[u8], mut i: usize) -> usize {
    loop {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            i = skip_line_comment(bytes, i);
        } else {
            return i;
        }
    }
}

fn is_ident_start_byte(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_cont_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn skip_string(source: &str, start: usize) -> usize {
    let bytes = source.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() && bytes[i] != b'"' {
        i += source[i..].chars().next().unwrap().len_utf8();
    }
    if i < bytes.len() { i + 1 } else { i }
}

fn skip_line_comment(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    i
}

fn parse_cut_error() -> ErrMode<ContextError> {
    ErrMode::Cut(ContextError::new())
}

fn named_arg(input: &mut &str) -> ModalResult<NamedArg> {
    ws_skip.parse_next(input)?;
    let name = ident.parse_next(input)?.to_string();
    ws_skip.parse_next(input)?;
    cut_err(literal("=")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(expr::parse_expr).parse_next(input)?;
    Ok(NamedArg { name, value })
}

// ---------------------------------------------------------------------------
// join clause
// ---------------------------------------------------------------------------

/// `join WINDOW snapshot/asof [within DUR] on cond [&& cond]`
pub(super) fn join_clause(input: &mut &str) -> ModalResult<JoinClause> {
    ws_skip.parse_next(input)?;
    kw("join").parse_next(input)?;
    ws_skip.parse_next(input)?;

    let target_window = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "target window name",
        )))
        .parse_next(input)?
        .to_string();

    ws_skip.parse_next(input)?;
    let mode = cut_err(join_mode).parse_next(input)?;

    ws_skip.parse_next(input)?;
    cut_err(kw("on"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'on' after join mode",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;

    // Parse join conditions separated by &&
    let first = cut_err(join_cond).parse_next(input)?;
    let mut conditions = vec![first];
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("&&")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            let cond = cut_err(join_cond).parse_next(input)?;
            conditions.push(cond);
        } else {
            break;
        }
    }

    Ok(JoinClause {
        target_window,
        mode,
        conditions,
    })
}

fn join_mode(input: &mut &str) -> ModalResult<JoinMode> {
    alt((
        (kw("asof"), ws_skip, opt(asof_within)).map(|(_, _, within)| JoinMode::Asof { within }),
        kw("snapshot").map(|_| JoinMode::Snapshot),
        kw("anti").map(|_| JoinMode::Anti),
    ))
    .parse_next(input)
}

fn asof_within(input: &mut &str) -> ModalResult<std::time::Duration> {
    kw("within").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(duration_value).parse_next(input)
}

fn join_cond(input: &mut &str) -> ModalResult<JoinCondition> {
    let left = join_field_ref.parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("==")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let right = cut_err(join_field_ref).parse_next(input)?;
    Ok(JoinCondition { left, right })
}

/// Parse a field reference for join conditions: `ident.ident` or `ident`
fn join_field_ref(input: &mut &str) -> ModalResult<FieldRef> {
    let first = ident.parse_next(input)?;
    if opt(literal(".")).parse_next(input)?.is_some() {
        let second = cut_err(ident).parse_next(input)?;
        Ok(FieldRef::Qualified(first.to_string(), second.to_string()))
    } else {
        Ok(FieldRef::Simple(first.to_string()))
    }
}

// ---------------------------------------------------------------------------
// limits block
// ---------------------------------------------------------------------------

/// `limits { key = value; ... }`
pub(super) fn limits_block(input: &mut &str) -> ModalResult<LimitsBlock> {
    kw("limits").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let mut items = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let key = cut_err(ident).parse_next(input)?.to_string();
        ws_skip.parse_next(input)?;
        cut_err(literal("=")).parse_next(input)?;
        ws_skip.parse_next(input)?;
        // Value can be a quoted string or an integer/ident
        let value = cut_err(limit_value).parse_next(input)?;
        ws_skip.parse_next(input)?;
        // Optional semicolon terminator
        let _ = opt(literal(";")).parse_next(input)?;
        items.push(LimitItem { key, value });
    }
    if items.is_empty() {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }
    Ok(LimitsBlock { items })
}

/// Parse a limit value: quoted string or bare token (digits, ident, slash-separated).
fn limit_value(input: &mut &str) -> ModalResult<String> {
    alt((
        quoted_string,
        // Bare value: digits and/or letters, slashes, etc.
        winnow::token::take_while(1.., |c: char| {
            c.is_ascii_alphanumeric() || c == '_' || c == '/'
        })
        .map(|s: &str| s.to_string()),
    ))
    .parse_next(input)
}
