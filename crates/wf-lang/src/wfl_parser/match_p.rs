use winnow::combinator::{alt, cut_err, opt, separated};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{duration_value, ident, kw, quoted_string, ws_skip};

use super::expr;

// ---------------------------------------------------------------------------
// match clause + -> score
// ---------------------------------------------------------------------------

/// Parse `match<...> { ... } -> score(expr)`
pub(super) fn match_with_score(input: &mut &str) -> ModalResult<(MatchClause, ScoreExpr)> {
    let mc = match_clause_only.parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("->"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'->' after match block",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let score = cut_err(score_expr_only).parse_next(input)?;
    Ok((mc, score))
}

// ---------------------------------------------------------------------------
// match clause
// ---------------------------------------------------------------------------

pub(super) fn match_clause_only(input: &mut &str) -> ModalResult<MatchClause> {
    kw("match").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("<")).parse_next(input)?;

    // Parse keys (may be empty), duration, and optional window mode
    let (keys, duration, window_mode) = cut_err(match_params).parse_next(input)?;

    cut_err(literal(">")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    // Optional key mapping block
    ws_skip.parse_next(input)?;
    let key_mapping = opt(key_block).parse_next(input)?;

    // Match body:
    //   `on event { ... } [close]`        — ordered (default, backward compat)
    //   `on event seq [mods] { ... }`     — ordered + within/not/consec/skip
    //   `on event any { ... }`            — unordered co-occurrence
    ws_skip.parse_next(input)?;
    kw("on").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(kw("event"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'event'",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;

    // `on event<accu>` — within-window accumulation (orthogonal to seq/any).
    let accu = opt(accu_param).parse_next(input)?.is_some();
    ws_skip.parse_next(input)?;

    let (on_event, on_close, seq, match_mode) = if opt(kw("seq")).parse_next(input)?.is_some() {
        // `on event seq { ... }` — ordered + within/not/consec/skip
        let seq = cut_err(seq_block_body)
            .context(StrContext::Expected(StrContextValue::Description(
                "seq block body",
            )))
            .parse_next(input)?;
        (Vec::new(), None, Some(seq), MatchMode::Seq)
    } else if opt(kw("any")).parse_next(input)?.is_some() {
        // `on event any { ... }` — unordered co-occurrence
        ws_skip.parse_next(input)?;
        cut_err(literal("{")).parse_next(input)?;
        let steps = cut_err(match_steps).parse_next(input)?;
        cut_err(literal("}")).parse_next(input)?;
        (steps, None, None, MatchMode::Any)
    } else {
        // bare `on event { ... } [close]` — ordered (backward compat)
        let on_event = cut_err(on_event_block)
            .context(StrContext::Expected(StrContextValue::Description(
                "'on event' block",
            )))
            .parse_next(input)?;
        ws_skip.parse_next(input)?;
        let on_close = opt(close_block).parse_next(input)?;
        (on_event, on_close, None, MatchMode::Seq)
    };

    ws_skip.parse_next(input)?;
    cut_err(literal("}")).parse_next(input)?;

    Ok(MatchClause {
        keys,
        key_mapping,
        duration,
        window_mode,
        on_event,
        on_close,
        match_mode,
        seq,
        accu,
    })
}

/// `on event<accu>` — the angle-bracket accumulation marker. The opening `<`
/// must backtrack cleanly (no cut) so a plain `on event` parses normally.
fn accu_param(input: &mut &str) -> ModalResult<bool> {
    literal("<").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(kw("accu")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(">")).parse_next(input)?;
    Ok(true)
}

pub(super) fn each_clause_only(input: &mut &str) -> ModalResult<EachClause> {
    kw("on").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(kw("each"))
        .context(StrContext::Expected(StrContextValue::Description("'each'")))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let alias = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "event alias after `on each`",
        )))
        .parse_next(input)?
        .to_string();
    ws_skip.parse_next(input)?;
    let filter = if opt(kw("where")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Some(cut_err(expr::parse_expr).parse_next(input)?)
    } else {
        None
    };
    Ok(EachClause { alias, filter })
}

/// Parse match params:
///   `[key, key, ...] : duration`               (sliding window)
///   `[key, key, ...] : duration : fixed`       (fixed window)
///   `[key, key, ...] : session(gap)`           (session window, L3)
fn match_params(input: &mut &str) -> ModalResult<(Vec<FieldRef>, std::time::Duration, WindowMode)> {
    ws_skip.parse_next(input)?;

    // If starts with ':', no keys
    let keys = if opt(literal(":")).parse_next(input)?.is_some() {
        vec![]
    } else {
        // Parse comma-separated field refs, then ':'
        let keys: Vec<FieldRef> =
            separated(1.., field_ref, (ws_skip, literal(","), ws_skip)).parse_next(input)?;
        ws_skip.parse_next(input)?;
        cut_err(literal(":")).parse_next(input)?;
        keys
    };

    ws_skip.parse_next(input)?;

    // Check for session(gap) first (L3 session window)
    if opt(kw("session")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        cut_err(literal("("))
            .context(StrContext::Expected(StrContextValue::Description(
                "'(' after 'session'",
            )))
            .parse_next(input)?;
        ws_skip.parse_next(input)?;
        let gap = cut_err(duration_value)
            .context(StrContext::Expected(StrContextValue::Description(
                "gap duration in session(gap)",
            )))
            .parse_next(input)?;
        ws_skip.parse_next(input)?;
        cut_err(literal(")"))
            .context(StrContext::Expected(StrContextValue::Description(
                "')' after session gap",
            )))
            .parse_next(input)?;
        ws_skip.parse_next(input)?;
        return Ok((keys, gap, WindowMode::Session(gap)));
    }

    // Parse duration for sliding/fixed window
    let dur = cut_err(duration_value)
        .context(StrContext::Expected(StrContextValue::Description(
            "duration value",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;

    // Optional :fixed suffix
    let window_mode = if opt(literal(":")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        cut_err(kw("fixed"))
            .context(StrContext::Expected(StrContextValue::Description(
                "'fixed'",
            )))
            .parse_next(input)?;
        ws_skip.parse_next(input)?;
        WindowMode::Fixed
    } else {
        WindowMode::Sliding
    };

    Ok((keys, dur, window_mode))
}

// ---------------------------------------------------------------------------
// key mapping block
// ---------------------------------------------------------------------------

/// `key { logical = alias.field; ... }`
fn key_block(input: &mut &str) -> ModalResult<Vec<KeyMapItem>> {
    kw("key").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let mut items = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let logical = cut_err(ident).parse_next(input)?.to_string();
        ws_skip.parse_next(input)?;
        cut_err(literal("=")).parse_next(input)?;
        ws_skip.parse_next(input)?;
        let source_field = cut_err(field_ref).parse_next(input)?;
        ws_skip.parse_next(input)?;
        // Semicolon terminator
        cut_err(literal(";"))
            .context(StrContext::Expected(StrContextValue::Description(
                "';' after key mapping item",
            )))
            .parse_next(input)?;
        items.push(KeyMapItem {
            logical_name: logical,
            source_field,
        });
    }
    if items.is_empty() {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }
    Ok(items)
}

/// Parse a field reference for match keys: `ident`, `ident.ident`, or `ident["string"]`
fn field_ref(input: &mut &str) -> ModalResult<FieldRef> {
    ws_skip.parse_next(input)?;
    let first = ident.parse_next(input)?;
    if opt(literal(".")).parse_next(input)?.is_some() {
        let second = cut_err(ident).parse_next(input)?;
        Ok(FieldRef::Qualified(first.to_string(), second.to_string()))
    } else if opt(literal("[")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let key = cut_err(quoted_string).parse_next(input)?;
        ws_skip.parse_next(input)?;
        cut_err(literal("]")).parse_next(input)?;
        Ok(FieldRef::Bracketed(first.to_string(), key))
    } else {
        Ok(FieldRef::Simple(first.to_string()))
    }
}

// ---------------------------------------------------------------------------
// on event / on close blocks
// ---------------------------------------------------------------------------

fn on_event_block(input: &mut &str) -> ModalResult<Vec<MatchStep>> {
    cut_err(literal("{")).parse_next(input)?;
    let steps = match_steps(input)?;
    cut_err(literal("}")).parse_next(input)?;
    Ok(steps)
}

fn close_block(input: &mut &str) -> ModalResult<CloseBlock> {
    // Try "and close" first (AND mode), then "on close" (OR mode)
    let mode = alt((
        (kw("and"), ws_skip, kw("close")).map(|_| CloseMode::And),
        (kw("on"), ws_skip, kw("close")).map(|_| CloseMode::Or),
    ))
    .parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;
    let steps = match_steps(input)?;
    cut_err(literal("}")).parse_next(input)?;
    Ok(CloseBlock { mode, steps })
}

fn match_steps(input: &mut &str) -> ModalResult<Vec<MatchStep>> {
    let mut steps = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if input.starts_with('}') {
            break;
        }
        let step = cut_err(match_step)
            .context(StrContext::Expected(StrContextValue::Description(
                "match step",
            )))
            .parse_next(input)?;
        steps.push(step);
    }
    if steps.is_empty() {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }
    Ok(steps)
}

// ---------------------------------------------------------------------------
// match step (with OR branches)
// ---------------------------------------------------------------------------

/// `step_branch { "||" step_branch } ";"`
fn match_step(input: &mut &str) -> ModalResult<MatchStep> {
    let first = step_branch.parse_next(input)?;
    let mut branches = vec![first];

    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("||")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            let branch = cut_err(step_branch).parse_next(input)?;
            branches.push(branch);
        } else {
            break;
        }
    }

    ws_skip.parse_next(input)?;
    cut_err(literal(";"))
        .context(StrContext::Expected(StrContextValue::Description(
            "';' after match step",
        )))
        .parse_next(input)?;

    Ok(MatchStep { branches })
}

/// `[label ":"] source [".field" | '["field"]'] ["&&" guard] pipe_chain`
/// or `has <alias> [&& guard]` (existential, implicit `count >= 1`).
fn step_branch(input: &mut &str) -> ModalResult<StepBranch> {
    ws_skip.parse_next(input)?;

    // `has <alias> [&& guard]` — existential step, implicit `count >= 1`.
    if opt(kw("has")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let source = cut_err(ident)
            .context(StrContext::Expected(StrContextValue::Description(
                "event alias after `has`",
            )))
            .parse_next(input)?
            .to_string();
        ws_skip.parse_next(input)?;
        let guard = if opt(literal("&&")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            Some(cut_err(expr::parse_expr).parse_next(input)?)
        } else {
            None
        };
        return Ok(StepBranch {
            label: None,
            source,
            field: None,
            guard,
            pipe: PipeChain {
                transforms: Vec::new(),
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold: Expr::Number(1.0),
            },
        });
    }

    // Try label: source or just source
    let (label, source) = alt((
        // label : source
        (ident, ws_skip, literal(":"), ws_skip, ident)
            .map(|(l, _, _, _, s)| (Some(l.to_string()), s.to_string())),
        // just source
        ident.map(|s: &str| (None, s.to_string())),
    ))
    .parse_next(input)?;

    // Optional field selector
    let field = opt(field_selector).parse_next(input)?;

    // Optional guard: && expr
    ws_skip.parse_next(input)?;
    let guard = if opt(literal("&&")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Some(cut_err(expr::parse_expr).parse_next(input)?)
    } else {
        None
    };

    // Pipe chain
    ws_skip.parse_next(input)?;
    let pipe = cut_err(pipe_chain)
        .context(StrContext::Expected(StrContextValue::Description(
            "pipe chain (| measure cmp value)",
        )))
        .parse_next(input)?;

    Ok(StepBranch {
        label,
        source,
        field,
        guard,
        pipe,
    })
}

/// Parse `.field` or `["field"]` selector.
fn field_selector(input: &mut &str) -> ModalResult<FieldSelector> {
    alt((
        // .field
        (literal("."), ident).map(|(_, f)| FieldSelector::Dot(f.to_string())),
        // ["field"]
        (literal("["), ws_skip, quoted_string, ws_skip, literal("]"))
            .map(|(_, _, s, _, _)| FieldSelector::Bracket(s)),
    ))
    .parse_next(input)
}

// ---------------------------------------------------------------------------
// pipe chain
// ---------------------------------------------------------------------------

/// `{ "|" transform } "|" measure cmp_op threshold`
fn pipe_chain(input: &mut &str) -> ModalResult<PipeChain> {
    let mut transforms = Vec::new();

    // Parse pipes: each is | followed by transform or measure
    // We collect transforms until we hit a measure keyword
    loop {
        ws_skip.parse_next(input)?;
        cut_err(literal("|"))
            .context(StrContext::Expected(StrContextValue::Description("'|'")))
            .parse_next(input)?;
        ws_skip.parse_next(input)?;

        // Try transform first
        if let Some(t) = opt(transform).parse_next(input)? {
            transforms.push(t);
        } else {
            // Must be a measure
            let measure = cut_err(measure)
                .context(StrContext::Expected(StrContextValue::Description(
                    "measure (count|sum|avg|min|max)",
                )))
                .parse_next(input)?;

            ws_skip.parse_next(input)?;
            let cmp = cut_err(cmp_op_step).parse_next(input)?;
            ws_skip.parse_next(input)?;
            let threshold = cut_err(expr::parse_atomic_expr).parse_next(input)?;

            return Ok(PipeChain {
                transforms,
                measure,
                cmp,
                threshold,
            });
        }
    }
}

fn transform(input: &mut &str) -> ModalResult<Transform> {
    kw("distinct")
        .map(|_| Transform::Distinct)
        .parse_next(input)
}

fn measure(input: &mut &str) -> ModalResult<Measure> {
    alt((
        kw("count").map(|_| Measure::Count),
        kw("sum").map(|_| Measure::Sum),
        kw("avg").map(|_| Measure::Avg),
        kw("min").map(|_| Measure::Min),
        kw("max").map(|_| Measure::Max),
    ))
    .parse_next(input)
}

fn cmp_op_step(input: &mut &str) -> ModalResult<CmpOp> {
    alt((
        literal("==").value(CmpOp::Eq),
        literal("!=").value(CmpOp::Ne),
        literal("<=").value(CmpOp::Le),
        literal(">=").value(CmpOp::Ge),
        literal("<").value(CmpOp::Lt),
        literal(">").value(CmpOp::Gt),
    ))
    .parse_next(input)
}

// ---------------------------------------------------------------------------
// chain block — ordered sequence matching (L1/L2)
// ---------------------------------------------------------------------------

/// Parse chain body after the `chain` keyword:
///   `[consec] [skip = past_last|to_next] { seq_steps }`
fn seq_block_body(input: &mut &str) -> ModalResult<SeqClause> {
    let mut consec = false;
    ws_skip.parse_next(input)?;
    if opt(kw("consec")).parse_next(input)?.is_some() {
        consec = true;
        ws_skip.parse_next(input)?;
    }

    let skip = if opt(kw("skip")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        cut_err(literal("=")).parse_next(input)?;
        ws_skip.parse_next(input)?;
        let s = cut_err(alt((
            kw("past_last").value(SeqSkip::PastLast),
            kw("to_next").value(SeqSkip::ToNext),
        )))
        .context(StrContext::Expected(StrContextValue::Description(
            "skip policy (past_last|to_next)",
        )))
        .parse_next(input)?;
        ws_skip.parse_next(input)?;
        s
    } else {
        SeqSkip::PastLast
    };

    cut_err(literal("{")).parse_next(input)?;

    let mut steps = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if input.starts_with('}') {
            break;
        }
        let step = cut_err(seq_step)
            .context(StrContext::Expected(StrContextValue::Description(
                "chain step",
            )))
            .parse_next(input)?;
        steps.push(step);
    }
    if steps.is_empty() {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }

    ws_skip.parse_next(input)?;
    cut_err(literal("}")).parse_next(input)?;

    Ok(SeqClause {
        consec,
        skip,
        steps,
    })
}

/// Parse one chain step: `[not] <body> [within dur] ;`
/// body := `has <alias> [&& guard]` (existential) | step_branch (aggregate).
fn seq_step(input: &mut &str) -> ModalResult<SeqStep> {
    ws_skip.parse_next(input)?;
    let neg = opt(kw("not")).parse_next(input)?.is_some();
    ws_skip.parse_next(input)?;

    let branch = if opt(kw("has")).parse_next(input)?.is_some() {
        // `has <alias> [&& guard]` — existential step, implicit `count >= 1`
        ws_skip.parse_next(input)?;
        let source = cut_err(ident)
            .context(StrContext::Expected(StrContextValue::Description(
                "event alias after `has`",
            )))
            .parse_next(input)?
            .to_string();
        ws_skip.parse_next(input)?;
        let guard = if opt(literal("&&")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            Some(cut_err(expr::parse_expr).parse_next(input)?)
        } else {
            None
        };
        StepBranch {
            label: None,
            source,
            field: None,
            guard,
            pipe: PipeChain {
                transforms: Vec::new(),
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold: Expr::Number(1.0),
            },
        }
    } else {
        // Aggregate step: reuse existing step_branch (pipe required)
        cut_err(step_branch)
            .context(StrContext::Expected(StrContextValue::Description(
                "chain step (has <alias> | <alias>.<field> | distinct | count >= N)",
            )))
            .parse_next(input)?
    };

    ws_skip.parse_next(input)?;
    let within = if opt(kw("within")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Some(
            cut_err(duration_value)
                .context(StrContext::Expected(StrContextValue::Description(
                    "within duration",
                )))
                .parse_next(input)?,
        )
    } else {
        None
    };

    ws_skip.parse_next(input)?;
    cut_err(literal(";"))
        .context(StrContext::Expected(StrContextValue::Description(
            "';' after chain step",
        )))
        .parse_next(input)?;

    Ok(SeqStep {
        neg,
        within,
        branch,
    })
}

// ---------------------------------------------------------------------------
// score expression
// ---------------------------------------------------------------------------

/// `score(expr)`
pub(super) fn score_expr_only(input: &mut &str) -> ModalResult<ScoreExpr> {
    kw("score").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let e = cut_err(expr::parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(ScoreExpr { expr: e })
}
