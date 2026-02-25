use winnow::combinator::{cut_err, separated};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, ws_skip};

use super::match_p;

// ---------------------------------------------------------------------------
// pattern declaration
// ---------------------------------------------------------------------------

/// Parse `pattern name(param1, param2, ...) { body }`
///
/// The body is captured as raw text (not parsed). Brace-balancing handles
/// nested `{}` inside the body (e.g. `on event { ... }`), strings, and
/// comments.
pub(super) fn pattern_decl(input: &mut &str) -> ModalResult<PatternDecl> {
    ws_skip.parse_next(input)?;
    kw("pattern").parse_next(input)?;
    ws_skip.parse_next(input)?;

    let name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "pattern name",
        )))
        .parse_next(input)?
        .to_string();

    ws_skip.parse_next(input)?;
    cut_err(literal("("))
        .context(StrContext::Expected(StrContextValue::Description(
            "'(' after pattern name",
        )))
        .parse_next(input)?;

    ws_skip.parse_next(input)?;
    let params: Vec<&str> =
        separated(1.., (ws_skip, ident).map(|(_, id)| id), (ws_skip, literal(",")))
            .parse_next(input)?;
    let params: Vec<String> = params.into_iter().map(|s| s.to_string()).collect();

    ws_skip.parse_next(input)?;
    cut_err(literal(")"))
        .context(StrContext::Expected(StrContextValue::Description(
            "')' after pattern parameters",
        )))
        .parse_next(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'{' to start pattern body",
        )))
        .parse_next(input)?;

    // Capture raw body text until balanced closing '}'.
    let body = cut_err(raw_balanced_body)
        .context(StrContext::Expected(StrContextValue::Description(
            "pattern body",
        )))
        .parse_next(input)?;

    Ok(PatternDecl { name, params, body })
}

/// Capture everything up to the matching `}` (which is consumed but not
/// included in the returned string). Handles nested `{}`, `"..."` strings,
/// and `# ...` comments.
fn raw_balanced_body(input: &mut &str) -> ModalResult<String> {
    let bytes = input.as_bytes();
    let len = bytes.len();
    let mut depth: usize = 1;
    let mut i = 0;

    while i < len && depth > 0 {
        match bytes[i] {
            b'{' => {
                depth += 1;
                i += 1;
            }
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
                i += 1;
            }
            b'"' => {
                i += 1;
                while i < len && bytes[i] != b'"' {
                    i += 1;
                }
                if i < len {
                    i += 1; // skip closing '"'
                }
            }
            b'#' => {
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    if depth != 0 {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }

    let body = input[..i].trim().to_string();
    *input = &input[i + 1..]; // skip closing '}'
    Ok(body)
}

// ---------------------------------------------------------------------------
// pattern expansion
// ---------------------------------------------------------------------------

/// Expand a pattern invocation: substitute args into the pattern body
/// and parse the result as `match<...> { ... } -> score(...)`.
///
/// Returns `(MatchClause, ScoreExpr, PatternOrigin)`.
pub(super) fn expand_pattern(
    pattern: &PatternDecl,
    args: &[String],
) -> Result<(MatchClause, ScoreExpr, PatternOrigin), String> {
    if args.len() != pattern.params.len() {
        return Err(format!(
            "pattern '{}' expects {} arguments, got {}",
            pattern.name,
            pattern.params.len(),
            args.len()
        ));
    }

    // Text-level substitution: replace ${param} with arg value.
    let mut expanded = pattern.body.clone();
    for (param, arg) in pattern.params.iter().zip(args.iter()) {
        let placeholder = format!("${{{}}}", param);
        expanded = expanded.replace(&placeholder, arg);
    }

    // Parse expanded text as match_with_score.
    let mut text = expanded.as_str();
    let (mc, score) = match_p::match_with_score(&mut text) .map_err(|e| {
        format!(
            "failed to parse expanded pattern '{}': {e}",
            pattern.name
        )
    })?;

    let origin = PatternOrigin {
        pattern_name: pattern.name.clone(),
        args: args.to_vec(),
    };

    Ok((mc, score, origin))
}
