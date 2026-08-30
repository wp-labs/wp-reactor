use winnow::combinator::{alt, cut_err, repeat};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;

mod clauses;
mod contract;
mod conv_p;
mod events;
mod expr;
mod match_p;
mod pattern_p;
mod rule;
mod stats_p;

use crate::ast::*;
use crate::parse_utils::{ident, kw, quoted_string, ws_skip};
use crate::{LangReason, LangResult};
use orion_error::conversion::ToStructError;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Parse a `.wfl` file containing `use` declarations and `rule` definitions.
pub fn parse_wfl(input: &str) -> LangResult<WflFile> {
    wfl_file.parse(input).map_err(|e| {
        LangReason::Parse
            .to_err()
            .with_detail(crate::diagnostics::parse_error_detail(input, &e))
    })
}

// ---------------------------------------------------------------------------
// Top-level grammar
// ---------------------------------------------------------------------------

fn wfl_file(input: &mut &str) -> ModalResult<WflFile> {
    ws_skip.parse_next(input)?;
    let uses: Vec<UseDecl> = repeat(0.., use_decl).parse_next(input)?;
    let top_decls: Vec<TopDecl> = repeat(0.., top_decl).parse_next(input)?;
    let mut patterns = Vec::new();
    let mut yield_presets = Vec::new();
    let mut lists = Vec::new();
    for decl in top_decls {
        match decl {
            TopDecl::Pattern(pattern) => patterns.push(pattern),
            TopDecl::YieldPreset(preset) => yield_presets.push(preset),
            TopDecl::List(list) => lists.push(list),
        }
    }
    let rules: Vec<RuleDecl> = repeat(0.., |input: &mut &str| {
        rule::rule_decl_with_patterns(input, &patterns)
    })
    .parse_next(input)?;
    let tests: Vec<TestBlock> = repeat(0.., contract::test_block).parse_next(input)?;
    ws_skip.parse_next(input)?;
    Ok(WflFile {
        uses,
        patterns,
        yield_presets,
        lists,
        rules,
        tests,
    })
}

enum TopDecl {
    Pattern(PatternDecl),
    YieldPreset(YieldPresetDecl),
    List(ListDecl),
}

fn top_decl(input: &mut &str) -> ModalResult<TopDecl> {
    alt((
        pattern_p::pattern_decl.map(TopDecl::Pattern),
        clauses::yield_preset_decl.map(TopDecl::YieldPreset),
        list_decl.map(TopDecl::List),
    ))
    .parse_next(input)
}

/// `name = (item, ...)`——顶层命名字面列表声明（issue #73）。裸绑定（无
/// 关键字/修饰符, WFL 规模小不做可见性控制）; 元素与 `in (...)` 列表同文法,
/// 编译期类型检查。与 `rule`/`pattern`/`yield preset` 关键字引导区分: 裸
/// `ident` 后跟 `=`。
fn list_decl(input: &mut &str) -> ModalResult<ListDecl> {
    ws_skip.parse_next(input)?;
    let name = ident
        .context(StrContext::Expected(StrContextValue::Description(
            "list name",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    // `=` 不用 cut: 裸绑定会先把 `rule`/`pattern` 等关键字当 ident 吞掉,
    // `=` 失败须可回溯, 让 alt 失败、由规则解析接管。
    kw("=")
        .context(StrContext::Expected(StrContextValue::Description(
            "'=' after list name",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let items = expr::in_list(input)?;
    Ok(ListDecl {
        name: name.to_string(),
        items,
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
