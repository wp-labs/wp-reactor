use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use wf_lang::parse_utils::ident;

use crate::wfg_ast::*;

use super::primitives::{param_value, percent, semi, ws_skip};

// ---------------------------------------------------------------------------
// Inject block
// ---------------------------------------------------------------------------

pub(super) fn inject_block(input: &mut &str) -> ModalResult<InjectBlock> {
    ws_skip(input)?;
    cut_err(wf_lang::parse_utils::kw("for"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'for' keyword",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    let rule = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "rule name",
        )))
        .parse_next(input)?
        .to_string();
    ws_skip(input)?;
    cut_err(wf_lang::parse_utils::kw("on"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'on' keyword",
        )))
        .parse_next(input)?;
    ws_skip(input)?;

    // Parse stream list: [s1, s2, ...]
    cut_err(literal("["))
        .context(StrContext::Expected(StrContextValue::Description(
            "opening bracket for stream list",
        )))
        .parse_next(input)?;
    let mut stream_names = Vec::new();
    ws_skip(input)?;
    if opt(literal("]")).parse_next(input)?.is_none() {
        stream_names.push(
            cut_err(ident)
                .context(StrContext::Expected(StrContextValue::Description(
                    "stream alias in inject",
                )))
                .parse_next(input)?
                .to_string(),
        );
        loop {
            ws_skip(input)?;
            if opt(literal(",")).parse_next(input)?.is_some() {
                ws_skip(input)?;
                stream_names.push(
                    cut_err(ident)
                        .context(StrContext::Expected(StrContextValue::Description(
                            "stream alias",
                        )))
                        .parse_next(input)?
                        .to_string(),
                );
            } else {
                break;
            }
        }
        ws_skip(input)?;
        cut_err(literal("]"))
            .context(StrContext::Expected(StrContextValue::Description(
                "closing bracket for stream list",
            )))
            .parse_next(input)?;
    }

    ws_skip(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description(
            "opening brace for inject block",
        )))
        .parse_next(input)?;

    let mut lines = Vec::new();
    loop {
        ws_skip(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let line = inject_line(input)?;
        lines.push(line);
    }

    Ok(InjectBlock {
        rule,
        streams: stream_names,
        lines,
    })
}

pub(super) fn inject_line(input: &mut &str) -> ModalResult<InjectLine> {
    let mode = alt((
        wf_lang::parse_utils::kw("hit").value(InjectMode::Hit),
        wf_lang::parse_utils::kw("near_miss").value(InjectMode::NearMiss),
        wf_lang::parse_utils::kw("non_hit").value(InjectMode::NonHit),
    ))
    .context(StrContext::Expected(StrContextValue::Description(
        "inject mode (hit, near_miss, non_hit)",
    )))
    .parse_next(input)?;

    ws_skip(input)?;
    let pct = cut_err(percent)
        .context(StrContext::Expected(StrContextValue::Description(
            "percent value",
        )))
        .parse_next(input)?;

    ws_skip(input)?;
    let mut params = Vec::new();

    // Flat params only: hit 20% key=value key2=value2;
    loop {
        let saved = *input;
        ws_skip(input)?;
        if input.is_empty() || input.starts_with(';') {
            *input = saved;
            break;
        }
        if let Ok(name) = ident(input) {
            ws_skip(input)?;
            cut_err(literal("="))
                .context(StrContext::Expected(StrContextValue::Description(
                    "'=' in inject param assignment",
                )))
                .parse_next(input)?;
            ws_skip(input)?;
            let value = cut_err(param_value)
                .context(StrContext::Expected(StrContextValue::Description(
                    "param value",
                )))
                .parse_next(input)?;
            params.push(ParamAssign {
                name: name.to_string(),
                value,
            });
            continue;
        }
        *input = saved;
        break;
    }

    ws_skip(input)?;
    cut_err(semi)
        .context(StrContext::Expected(StrContextValue::Description(
            "trailing ';' after inject line",
        )))
        .parse_next(input)?;

    Ok(InjectLine {
        mode,
        percent: pct,
        params,
    })
}
