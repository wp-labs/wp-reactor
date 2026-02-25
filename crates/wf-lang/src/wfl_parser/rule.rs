use winnow::combinator::{cut_err, opt, repeat};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, quoted_string, ws_skip};

use super::clauses;
use super::conv_p;
use super::events;
use super::match_p;

// ---------------------------------------------------------------------------
// rule declaration
// ---------------------------------------------------------------------------

pub(super) fn rule_decl(input: &mut &str) -> ModalResult<RuleDecl> {
    ws_skip.parse_next(input)?;
    kw("rule").parse_next(input)?;
    ws_skip.parse_next(input)?;

    let name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "rule name",
        )))
        .parse_next(input)?
        .to_string();

    ws_skip.parse_next(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description("'{'")))
        .parse_next(input)?;

    // Optional meta block
    ws_skip.parse_next(input)?;
    let meta = opt(meta_block).parse_next(input)?;

    // Required events block
    ws_skip.parse_next(input)?;
    let events = cut_err(events::events_block)
        .context(StrContext::Expected(StrContextValue::Description(
            "events block",
        )))
        .parse_next(input)?;

    // Required match clause (includes -> score)
    ws_skip.parse_next(input)?;
    let (match_clause, score) = cut_err(match_p::match_with_score)
        .context(StrContext::Expected(StrContextValue::Description(
            "match clause",
        )))
        .parse_next(input)?;

    // Optional join clauses (zero or more)
    ws_skip.parse_next(input)?;
    let joins: Vec<JoinClause> = repeat(0.., clauses::join_clause).parse_next(input)?;

    // Required entity clause
    ws_skip.parse_next(input)?;
    let entity = cut_err(clauses::entity_clause)
        .context(StrContext::Expected(StrContextValue::Description(
            "entity clause",
        )))
        .parse_next(input)?;

    // Required yield clause
    ws_skip.parse_next(input)?;
    let yield_clause = cut_err(clauses::yield_clause)
        .context(StrContext::Expected(StrContextValue::Description(
            "yield clause",
        )))
        .parse_next(input)?;

    // Optional conv block (L3, fixed window only — checker enforces constraint)
    ws_skip.parse_next(input)?;
    let conv = opt(conv_p::conv_clause).parse_next(input)?;

    // Optional limits block
    ws_skip.parse_next(input)?;
    let limits = opt(clauses::limits_block).parse_next(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal("}"))
        .context(StrContext::Expected(StrContextValue::Description(
            "closing '}'",
        )))
        .parse_next(input)?;

    Ok(RuleDecl {
        name,
        meta,
        events,
        match_clause,
        score,
        joins,
        entity,
        yield_clause,
        conv,
        limits,
    })
}

// ---------------------------------------------------------------------------
// meta block
// ---------------------------------------------------------------------------

fn meta_block(input: &mut &str) -> ModalResult<MetaBlock> {
    kw("meta").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let mut entries = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let key = cut_err(ident).parse_next(input)?.to_string();
        ws_skip.parse_next(input)?;
        cut_err(literal("=")).parse_next(input)?;
        ws_skip.parse_next(input)?;
        let value = cut_err(quoted_string).parse_next(input)?;
        entries.push(MetaEntry { key, value });
    }
    Ok(MetaBlock { entries })
}
