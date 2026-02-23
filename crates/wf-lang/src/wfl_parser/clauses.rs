use winnow::combinator::{alt, cut_err, opt, separated};
use winnow::error::{StrContext, StrContextValue};
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
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;

    let args: Vec<NamedArg> =
        separated(0.., named_arg, (ws_skip, literal(","), ws_skip)).parse_next(input)?;

    // Allow trailing comma
    ws_skip.parse_next(input)?;
    let _ = opt(literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;

    Ok(YieldClause {
        target,
        version,
        args,
    })
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
