use winnow::combinator::{cut_err, opt};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, ws_skip};

use super::expr;

// ---------------------------------------------------------------------------
// events block
// ---------------------------------------------------------------------------

pub(super) fn events_block(input: &mut &str) -> ModalResult<EventsBlock> {
    kw("events").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let mut decls = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let decl = cut_err(event_decl).parse_next(input)?;
        decls.push(decl);
    }
    if decls.is_empty() {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }
    Ok(EventsBlock { decls })
}

fn event_decl(input: &mut &str) -> ModalResult<EventDecl> {
    let alias = ident.parse_next(input)?.to_string();
    ws_skip.parse_next(input)?;
    cut_err(literal(":")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let window = cut_err(ident).parse_next(input)?.to_string();

    // Optional filter: && expr
    ws_skip.parse_next(input)?;
    let filter = if opt(literal("&&")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Some(cut_err(expr::parse_expr).parse_next(input)?)
    } else {
        None
    };
    Ok(EventDecl {
        alias,
        window,
        filter,
    })
}
