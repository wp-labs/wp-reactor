use winnow::combinator::{cut_err, opt, repeat, separated};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, quoted_string, ws_skip};

use super::clauses;
use super::conv_p;
use super::events;
use super::match_p;
use super::pattern_p;

// ---------------------------------------------------------------------------
// rule declaration (with pattern support)
// ---------------------------------------------------------------------------

/// Parse a rule declaration with pattern invocation support.
///
/// When `patterns` is non-empty, the match+score position accepts either a
/// standard `match<...> { ... } -> score(...)` or a pattern invocation
/// `pattern_name(arg1, arg2, ...)` that expands to match+score.
pub(super) fn rule_decl_with_patterns(
    input: &mut &str,
    patterns: &[PatternDecl],
) -> ModalResult<RuleDecl> {
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

    // Parse either:
    // 1) legacy/pattern single-stage rule, or
    // 2) stage chain: stage {|> stage} with implicit _in between stages.
    ws_skip.parse_next(input)?;
    let saved = *input;
    let (match_clause, score, joins, pipeline_stages, pattern_origin) =
        match pattern_invocation(input, patterns) {
            Ok((match_clause, score, pattern_origin)) => {
                ws_skip.parse_next(input)?;
                let joins: Vec<JoinClause> = repeat(0.., clauses::join_clause).parse_next(input)?;
                ws_skip.parse_next(input)?;
                if opt(literal("|>")).parse_next(input)?.is_some() {
                    return Err(winnow::error::ErrMode::Cut(
                        winnow::error::ContextError::new(),
                    ));
                }
                (match_clause, score, joins, Vec::new(), pattern_origin)
            }
            Err(winnow::error::ErrMode::Backtrack(_)) => {
                *input = saved;
                let mut parsed_stage = cut_err(stage_clause)
                    .context(StrContext::Expected(StrContextValue::Description(
                        "match clause",
                    )))
                    .parse_next(input)?;
                let mut pipeline_stages: Vec<PipelineStage> = Vec::new();

                loop {
                    ws_skip.parse_next(input)?;
                    if opt(literal("|>")).parse_next(input)?.is_none() {
                        break;
                    }

                    // Non-final pipeline stage must not carry score.
                    if parsed_stage.score.is_some() {
                        return Err(winnow::error::ErrMode::Cut(
                            winnow::error::ContextError::new(),
                        ));
                    }

                    pipeline_stages.push(PipelineStage {
                        match_clause: parsed_stage.match_clause,
                        joins: parsed_stage.joins,
                    });

                    ws_skip.parse_next(input)?;
                    parsed_stage = cut_err(stage_clause)
                        .context(StrContext::Expected(StrContextValue::Description(
                            "pipeline stage",
                        )))
                        .parse_next(input)?;
                }

                let score = match parsed_stage.score {
                    Some(s) => s,
                    None => {
                        return Err(winnow::error::ErrMode::Cut(
                            winnow::error::ContextError::new(),
                        ));
                    }
                };

                (
                    parsed_stage.match_clause,
                    score,
                    parsed_stage.joins,
                    pipeline_stages,
                    None,
                )
            }
            Err(e) => return Err(e),
        };

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
        pipeline_stages,
        entity,
        yield_clause,
        pattern_origin,
        conv,
        limits,
    })
}

#[derive(Debug)]
struct ParsedStage {
    match_clause: MatchClause,
    score: Option<ScoreExpr>,
    joins: Vec<JoinClause>,
}

/// Parse one stage:
/// `match<...> { ... } [-> score(...)] { join ... }*`
fn stage_clause(input: &mut &str) -> ModalResult<ParsedStage> {
    let match_clause = match_p::match_clause_only.parse_next(input)?;

    ws_skip.parse_next(input)?;
    let score = if opt(literal("->")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Some(cut_err(match_p::score_expr_only).parse_next(input)?)
    } else {
        None
    };

    ws_skip.parse_next(input)?;
    let joins: Vec<JoinClause> = repeat(0.., clauses::join_clause).parse_next(input)?;

    Ok(ParsedStage {
        match_clause,
        score,
        joins,
    })
}

/// Parse a pattern invocation: `ident(arg, arg, ...)` where ident matches a
/// known pattern. Arguments are raw token strings (identifiers, durations,
/// numbers) separated by commas.
fn pattern_invocation(
    input: &mut &str,
    patterns: &[PatternDecl],
) -> ModalResult<(MatchClause, ScoreExpr, Option<PatternOrigin>)> {
    let saved = *input;
    ws_skip.parse_next(input)?;
    let name = ident.parse_next(input)?;

    // Must match a known pattern name
    let pattern = patterns.iter().find(|p| p.name == name);
    let pattern = match pattern {
        Some(p) => p,
        None => {
            *input = saved;
            return Err(winnow::error::ErrMode::Backtrack(
                winnow::error::ContextError::new(),
            ));
        }
    };

    ws_skip.parse_next(input)?;
    cut_err(literal("("))
        .context(StrContext::Expected(StrContextValue::Description(
            "'(' after pattern name",
        )))
        .parse_next(input)?;

    ws_skip.parse_next(input)?;
    let args: Vec<String> = separated(
        1..,
        (ws_skip, pattern_arg).map(|(_, a)| a),
        (ws_skip, literal(",")),
    )
    .parse_next(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal(")"))
        .context(StrContext::Expected(StrContextValue::Description(
            "')' after pattern arguments",
        )))
        .parse_next(input)?;

    // Validate argument count
    if args.len() != pattern.params.len() {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }

    // Expand pattern
    let (mc, score, origin) = pattern_p::expand_pattern(pattern, &args)
        .map_err(|_| winnow::error::ErrMode::Cut(winnow::error::ContextError::new()))?;

    Ok((mc, score, Some(origin)))
}

/// Parse a pattern argument: an identifier-like token, a duration token
/// (digits + suffix), or a number. We capture it as a raw string.
fn pattern_arg(input: &mut &str) -> ModalResult<String> {
    ws_skip.parse_next(input)?;
    // Capture a run of non-separator characters (not ',', ')', whitespace)
    let arg = winnow::token::take_while(1.., |c: char| {
        !c.is_ascii_whitespace() && c != ',' && c != ')'
    })
    .parse_next(input)?;
    Ok(arg.to_string())
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
