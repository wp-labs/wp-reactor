use winnow::combinator::{cut_err, repeat};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;

mod clauses;
mod contract;
mod events;
mod expr;
mod match_p;
mod rule;

use crate::ast::*;
use crate::parse_utils::{kw, quoted_string, ws_skip};

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Parse a `.wfl` file containing `use` declarations and `rule` definitions.
pub fn parse_wfl(input: &str) -> anyhow::Result<WflFile> {
    wfl_file
        .parse(input)
        .map_err(|e| anyhow::anyhow!("parse error: {e}"))
}

// ---------------------------------------------------------------------------
// Top-level grammar
// ---------------------------------------------------------------------------

fn wfl_file(input: &mut &str) -> ModalResult<WflFile> {
    ws_skip.parse_next(input)?;
    let uses: Vec<UseDecl> = repeat(0.., use_decl).parse_next(input)?;
    let rules: Vec<RuleDecl> = repeat(0.., rule::rule_decl).parse_next(input)?;
    let tests: Vec<TestBlock> = repeat(0.., contract::test_block).parse_next(input)?;
    ws_skip.parse_next(input)?;
    Ok(WflFile {
        uses,
        rules,
        tests,
    })
}

// ---------------------------------------------------------------------------
// use declaration
// ---------------------------------------------------------------------------

fn use_decl(input: &mut &str) -> ModalResult<UseDecl> {
    ws_skip.parse_next(input)?;
    kw("use").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let path = cut_err(quoted_string)
        .context(StrContext::Expected(StrContextValue::Description(
            "string path after 'use'",
        )))
        .parse_next(input)?;
    Ok(UseDecl { path })
}
