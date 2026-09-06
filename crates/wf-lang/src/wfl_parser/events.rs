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

    let decls = parse_event_decls(input)?;
    if decls.is_empty() {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }
    Ok(EventsBlock { decls })
}

/// `{ alias: window [&& filter]; ... }` 内的声明列表（`}` 收尾）。
fn parse_event_decls(input: &mut &str) -> ModalResult<Vec<EventDecl>> {
    let mut decls = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let decl = cut_err(event_decl).parse_next(input)?;
        decls.push(decl);
    }
    Ok(decls)
}

fn event_decl(input: &mut &str) -> ModalResult<EventDecl> {
    let alias = ident.parse_next(input)?.to_string();
    ws_skip.parse_next(input)?;
    cut_err(literal(":")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let window = cut_err(ident).parse_next(input)?.to_string();

    // Optional filter: && expr
    let filter = event_decl_filter(input)?;
    Ok(EventDecl {
        alias,
        window,
        filter,
    })
}

/// 可选的 `&& expr` 事件过滤（缺省 None）。
fn event_decl_filter(input: &mut &str) -> ModalResult<Option<Expr>> {
    ws_skip.parse_next(input)?;
    if opt(literal("&&")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Ok(Some(cut_err(expr::parse_expr).parse_next(input)?))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    #[test]
    fn events_block_decls_with_and_without_filter() {
        let mut s = "events { scan : conn_events && action == \"syn\" login : ok_events }";
        let b = events_block
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("events parse failed: {e:?}"));
        assert_eq!(b.decls.len(), 2);
        assert_eq!(b.decls[0].alias, "scan");
        assert_eq!(b.decls[0].window, "conn_events");
        assert!(b.decls[0].filter.is_some(), "&& 过滤解析");
        assert_eq!(b.decls[1].alias, "login");
        assert_eq!(b.decls[1].window, "ok_events");
        assert!(b.decls[1].filter.is_none());
        assert!(s.is_empty());
    }

    #[test]
    fn events_block_empty_rejected() {
        let mut s = "events { }";
        assert!(events_block.parse_next(&mut s).is_err(), "空 events 块拒绝");
    }
}
