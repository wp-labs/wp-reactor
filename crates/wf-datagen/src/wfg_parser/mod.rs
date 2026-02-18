mod primitives;
#[cfg(test)]
mod tests;

use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{AddContext, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use wf_lang::parse_utils::{ident, nonneg_integer, quoted_string};

use crate::wfg_ast::*;

use self::primitives::{field_name, gen_expr, param_value, percent, rate, semi, ws_skip};

// ---------------------------------------------------------------------------
// Top-level
// ---------------------------------------------------------------------------

/// Parse a `.wfg` scenario file from a string.
pub fn parse_wfg(input: &str) -> anyhow::Result<WfgFile> {
    let mut rest = input;
    let result = wfg_file(&mut rest).map_err(|e| anyhow::anyhow!("parse error: {e}"))?;

    ws_skip(&mut rest).map_err(|e| anyhow::anyhow!("parse error: {e}"))?;
    if !rest.is_empty() {
        return Err(anyhow::anyhow!(
            "unexpected trailing content: {:?}",
            &rest[..rest.len().min(60)]
        ));
    }
    Ok(result)
}

fn wfg_file(input: &mut &str) -> ModalResult<WfgFile> {
    let mut uses = Vec::new();
    loop {
        ws_skip(input)?;
        if opt(wf_lang::parse_utils::kw("use"))
            .parse_next(input)?
            .is_some()
        {
            ws_skip(input)?;
            let path = cut_err(quoted_string)
                .context(StrContext::Expected(StrContextValue::Description(
                    "quoted path after 'use'",
                )))
                .parse_next(input)?;
            uses.push(UseDecl { path });
        } else {
            break;
        }
    }

    ws_skip(input)?;
    let scenario = scenario_decl(input)?;

    Ok(WfgFile { uses, scenario })
}

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

fn scenario_decl(input: &mut &str) -> ModalResult<ScenarioDecl> {
    wf_lang::parse_utils::kw("scenario").parse_next(input)?;
    ws_skip(input)?;
    let name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "scenario name",
        )))
        .parse_next(input)?
        .to_string();
    ws_skip(input)?;
    cut_err(wf_lang::parse_utils::kw("seed"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'seed' keyword",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    let seed_val = cut_err(nonneg_integer)
        .context(StrContext::Expected(StrContextValue::Description(
            "seed number",
        )))
        .parse_next(input)? as u64;
    ws_skip(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description(
            "opening brace for scenario",
        )))
        .parse_next(input)?;

    // Parse inner clauses
    ws_skip(input)?;
    let time_clause = cut_err(time_clause_parser)
        .context(StrContext::Expected(StrContextValue::Description(
            "time clause",
        )))
        .parse_next(input)?;

    ws_skip(input)?;
    cut_err(wf_lang::parse_utils::kw("total"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'total' keyword",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    let total = cut_err(nonneg_integer)
        .context(StrContext::Expected(StrContextValue::Description(
            "total event count",
        )))
        .parse_next(input)? as u64;

    // Parse streams, injects, faults, oracle
    let mut streams = Vec::new();
    let mut injects = Vec::new();
    let mut faults = None;
    let mut oracle = None;

    loop {
        ws_skip(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        if opt(wf_lang::parse_utils::kw("stream"))
            .parse_next(input)?
            .is_some()
        {
            let s = cut_err(stream_block).parse_next(input)?;
            streams.push(s);
        } else if opt(wf_lang::parse_utils::kw("inject"))
            .parse_next(input)?
            .is_some()
        {
            let i = cut_err(inject_block).parse_next(input)?;
            injects.push(i);
        } else if opt(wf_lang::parse_utils::kw("faults"))
            .parse_next(input)?
            .is_some()
        {
            let f = cut_err(faults_block).parse_next(input)?;
            faults = Some(f);
        } else if opt(wf_lang::parse_utils::kw("oracle"))
            .parse_next(input)?
            .is_some()
        {
            let o = cut_err(oracle_block).parse_next(input)?;
            oracle = Some(o);
        } else {
            return Err(winnow::error::ErrMode::Cut(
                winnow::error::ContextError::new().add_context(
                    input,
                    &input.checkpoint(),
                    StrContext::Expected(StrContextValue::Description(
                        "stream, inject, faults, oracle, or closing brace",
                    )),
                ),
            ));
        }
    }

    Ok(ScenarioDecl {
        name,
        seed: seed_val,
        time_clause,
        total,
        streams,
        injects,
        faults,
        oracle,
    })
}

// ---------------------------------------------------------------------------
// Time clause (single allowed syntax):
//   time "ISO8601" duration DURATION
// ---------------------------------------------------------------------------

fn time_clause_parser(input: &mut &str) -> ModalResult<TimeClause> {
    wf_lang::parse_utils::kw("time").parse_next(input)?;
    ws_skip(input)?;
    let start = cut_err(quoted_string)
        .context(StrContext::Expected(StrContextValue::Description(
            "start timestamp string",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    cut_err(wf_lang::parse_utils::kw("duration"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'duration' keyword",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    let dur = cut_err(wf_lang::parse_utils::duration_value)
        .context(StrContext::Expected(StrContextValue::Description(
            "duration value",
        )))
        .parse_next(input)?;
    Ok(TimeClause {
        start,
        duration: dur,
    })
}

// ---------------------------------------------------------------------------
// Stream block
// ---------------------------------------------------------------------------

fn stream_block(input: &mut &str) -> ModalResult<StreamBlock> {
    ws_skip(input)?;
    let alias = ident(input)?.to_string();
    ws_skip(input)?;
    cut_err(literal(":"))
        .context(StrContext::Expected(StrContextValue::Description(
            "colon after stream alias",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    let window = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "window name",
        )))
        .parse_next(input)?
        .to_string();
    ws_skip(input)?;
    let r = cut_err(rate)
        .context(StrContext::Expected(StrContextValue::Description("rate")))
        .parse_next(input)?;
    ws_skip(input)?;

    let mut overrides = Vec::new();
    if opt(literal("{")).parse_next(input)?.is_some() {
        loop {
            ws_skip(input)?;
            if opt(literal("}")).parse_next(input)?.is_some() {
                break;
            }
            let fo = field_override(input)?;
            overrides.push(fo);
        }
    }

    Ok(StreamBlock {
        alias,
        window,
        rate: r,
        overrides,
    })
}

fn field_override(input: &mut &str) -> ModalResult<FieldOverride> {
    let fname = field_name(input)?;
    ws_skip(input)?;
    cut_err(literal("="))
        .context(StrContext::Expected(StrContextValue::Description(
            "'=' in field override",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    let expr = cut_err(gen_expr)
        .context(StrContext::Expected(StrContextValue::Description(
            "generator expression",
        )))
        .parse_next(input)?;
    Ok(FieldOverride {
        field_name: fname,
        gen_expr: expr,
    })
}

// ---------------------------------------------------------------------------
// Inject block
// ---------------------------------------------------------------------------

fn inject_block(input: &mut &str) -> ModalResult<InjectBlock> {
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

fn inject_line(input: &mut &str) -> ModalResult<InjectLine> {
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

// ---------------------------------------------------------------------------
// Faults block
// ---------------------------------------------------------------------------

fn faults_block(input: &mut &str) -> ModalResult<FaultsBlock> {
    ws_skip(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description(
            "opening brace for faults block",
        )))
        .parse_next(input)?;

    let mut faults = Vec::new();
    loop {
        ws_skip(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let fl = fault_line(input)?;
        cut_err(semi)
            .context(StrContext::Expected(StrContextValue::Description(
                "trailing ';' after faults line",
            )))
            .parse_next(input)?;
        faults.push(fl);
    }

    Ok(FaultsBlock { faults })
}

fn fault_line(input: &mut &str) -> ModalResult<FaultLine> {
    let name = ident(input)?.to_string();
    ws_skip(input)?;
    let pct = cut_err(percent)
        .context(StrContext::Expected(StrContextValue::Description(
            "percent for fault",
        )))
        .parse_next(input)?;
    Ok(FaultLine { name, percent: pct })
}

// ---------------------------------------------------------------------------
// Oracle block
// ---------------------------------------------------------------------------

fn oracle_block(input: &mut &str) -> ModalResult<OracleBlock> {
    ws_skip(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description(
            "opening brace for oracle block",
        )))
        .parse_next(input)?;

    let mut params = Vec::new();
    loop {
        ws_skip(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let p = param_assign(input)?;
        cut_err(semi)
            .context(StrContext::Expected(StrContextValue::Description(
                "trailing ';' after oracle assignment",
            )))
            .parse_next(input)?;
        params.push(p);
    }

    Ok(OracleBlock { params })
}

// ---------------------------------------------------------------------------
// Shared: param_assign
// ---------------------------------------------------------------------------

fn param_assign(input: &mut &str) -> ModalResult<ParamAssign> {
    let name = ident(input)?.to_string();
    ws_skip(input)?;
    cut_err(literal("="))
        .context(StrContext::Expected(StrContextValue::Description(
            "'=' in param assignment",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    let value = cut_err(param_value)
        .context(StrContext::Expected(StrContextValue::Description(
            "param value",
        )))
        .parse_next(input)?;
    Ok(ParamAssign { name, value })
}
