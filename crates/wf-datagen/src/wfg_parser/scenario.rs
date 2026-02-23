use winnow::combinator::{cut_err, opt};
use winnow::error::{AddContext, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use wf_lang::parse_utils::{ident, nonneg_integer, quoted_string};

use crate::wfg_ast::*;

use super::faults::faults_block;
use super::inject::inject_block;
use super::oracle::oracle_block;
use super::primitives::ws_skip;
use super::stream::stream_block;

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

pub(super) fn scenario_decl(input: &mut &str) -> ModalResult<ScenarioDecl> {
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

pub(super) fn time_clause_parser(input: &mut &str) -> ModalResult<TimeClause> {
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
