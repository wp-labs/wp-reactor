//! 规则子句解析（`clauses/` 拆分）：`let` / `entity` / `yield` / `limits` 子句与
//! 命名参数；`join` 子句族见 [`join`]，yield preset 参数/引用的尖括号扫描见
//! [`preset`]。对外（rule.rs / wfl_parser::mod）仅经 `pub(super)` 入口访问。

mod join;
mod preset;

pub(crate) use join::join_clause;
use preset::{yield_preset_params, yield_preset_ref};

use winnow::combinator::{alt, cut_err, opt, separated};
use winnow::error::{ContextError, ErrMode, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, nonneg_integer, quoted_string, ws_skip};

use super::expr;

// ---------------------------------------------------------------------------
// let clause
// ---------------------------------------------------------------------------

/// `let <ident> = <expr>` — per-event binding, referenced by bare name later.
pub(super) fn let_clause(input: &mut &str) -> ModalResult<LetDecl> {
    kw("let").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "let binding name",
        )))
        .parse_next(input)?
        .to_string();
    ws_skip.parse_next(input)?;
    cut_err(literal("="))
        .context(StrContext::Expected(StrContextValue::Description("'='")))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let expr = cut_err(expr::parse_expr)
        .context(StrContext::Expected(StrContextValue::Description(
            "let binding expression",
        )))
        .parse_next(input)?;
    Ok(LetDecl { name, expr })
}

// ---------------------------------------------------------------------------
// entity clause
// ---------------------------------------------------------------------------

pub(super) fn entity_clause(input: &mut &str) -> ModalResult<EntityClause> {
    kw("entity").parse_next(input)?;
    let entity_type = parse_entity_head(input)?;
    ws_skip.parse_next(input)?;
    let id_expr = cut_err(expr::parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;

    Ok(EntityClause {
        entity_type,
        id_expr,
    })
}

/// `( <type> ,` — `entity` 关键字后的括号头（类型 + 逗号）。
fn parse_entity_head(input: &mut &str) -> ModalResult<EntityTypeVal> {
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let entity_type = parse_entity_type(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(",")).parse_next(input)?;
    Ok(entity_type)
}

/// entity 类型：标识符或字符串字面量。
fn parse_entity_type(input: &mut &str) -> ModalResult<EntityTypeVal> {
    alt((
        quoted_string.map(EntityTypeVal::StringLit),
        ident.map(|s: &str| EntityTypeVal::Ident(s.to_string())),
    ))
    .parse_next(input)
}

// ---------------------------------------------------------------------------
// yield clause
// ---------------------------------------------------------------------------

pub(super) fn yield_preset_decl(input: &mut &str) -> ModalResult<YieldPresetDecl> {
    ws_skip.parse_next(input)?;
    kw("yield").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(kw("preset")).parse_next(input)?;
    ws_skip.parse_next(input)?;

    let (name, params) = parse_preset_decl_head(input)?;

    ws_skip.parse_next(input)?;
    let args = cut_err(named_args_parens).parse_next(input)?;

    Ok(YieldPresetDecl { name, params, args })
}

/// `NAME [<params>]`（`yield preset` 已消费）——preset 名 + 可选形参。
fn parse_preset_decl_head(input: &mut &str) -> ModalResult<(String, Vec<YieldPresetParam>)> {
    let name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "yield preset name",
        )))
        .parse_next(input)?
        .to_string();
    ws_skip.parse_next(input)?;
    let params = opt(yield_preset_params)
        .parse_next(input)?
        .unwrap_or_default();
    Ok((name, params))
}

pub(super) fn yield_clause(input: &mut &str) -> ModalResult<YieldClause> {
    kw("yield").parse_next(input)?;
    ws_skip.parse_next(input)?;

    let target = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "yield target window name",
        )))
        .parse_next(input)?
        .to_string();

    // Optional version: @vN
    let version = yield_version(input)?;
    // Optional presets: `: preset...`（去重引用）
    let presets = yield_preset_list(input)?;

    ws_skip.parse_next(input)?;
    let args = cut_err(named_args_parens).parse_next(input)?;

    Ok(YieldClause {
        target,
        version,
        presets,
        args,
    })
}

/// `@vN` 版本标记（缺省 None）。
fn yield_version(input: &mut &str) -> ModalResult<Option<u32>> {
    if opt(literal("@")).parse_next(input)?.is_some() {
        cut_err(literal("v")).parse_next(input)?;
        let n = cut_err(nonneg_integer).parse_next(input)?;
        Ok(Some(n as u32))
    } else {
        Ok(None)
    }
}

/// `: preset1, preset2` 引用列表（缺省空）。
fn yield_preset_list(input: &mut &str) -> ModalResult<Vec<YieldPresetRef>> {
    ws_skip.parse_next(input)?;
    if opt(literal(":")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        separated(1.., yield_preset_ref, (ws_skip, literal(","), ws_skip)).parse_next(input)
    } else {
        Ok(Vec::new())
    }
}

fn named_args_parens(input: &mut &str) -> ModalResult<Vec<NamedArg>> {
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let args: Vec<NamedArg> =
        separated(0.., named_arg, (ws_skip, literal(","), ws_skip)).parse_next(input)?;
    // Allow trailing comma
    ws_skip.parse_next(input)?;
    let _ = opt(literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(args)
}

fn parse_cut_error() -> ErrMode<ContextError> {
    ErrMode::Cut(ContextError::new())
}

fn named_arg(input: &mut &str) -> ModalResult<NamedArg> {
    ws_skip.parse_next(input)?;
    let name = ident.parse_next(input)?.to_string();
    ws_skip.parse_next(input)?;
    cut_err(literal("=")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(expr::parse_expr).parse_next(input)?;
    Ok(NamedArg { name, value })
}

// ---------------------------------------------------------------------------
// limits block
// ---------------------------------------------------------------------------

/// `limits { key = value; ... }`
pub(super) fn limits_block(input: &mut &str) -> ModalResult<LimitsBlock> {
    kw("limits").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let items = parse_limit_items(input)?;
    if items.is_empty() {
        return Err(parse_cut_error());
    }
    Ok(LimitsBlock { items })
}

/// `key = value; ...` 条目循环（`}` 收尾）。
fn parse_limit_items(input: &mut &str) -> ModalResult<Vec<LimitItem>> {
    let mut items = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        items.push(limit_item(input)?);
    }
    Ok(items)
}

/// 单条 `key = value;`（值可为引号串或裸 token; 分号可选）。
fn limit_item(input: &mut &str) -> ModalResult<LimitItem> {
    ws_skip.parse_next(input)?;
    let key = cut_err(ident).parse_next(input)?.to_string();
    ws_skip.parse_next(input)?;
    cut_err(literal("=")).parse_next(input)?;
    let value = limit_value_tail(input)?;
    Ok(LimitItem { key, value })
}

/// `value [;]`（`=` 已消费; 值可为引号串或裸 token, 分号可选）。
fn limit_value_tail(input: &mut &str) -> ModalResult<String> {
    ws_skip.parse_next(input)?;
    // Value can be a quoted string or an integer/ident
    let value = cut_err(limit_value).parse_next(input)?;
    ws_skip.parse_next(input)?;
    // Optional semicolon terminator
    let _ = opt(literal(";")).parse_next(input)?;
    Ok(value)
}

/// Parse a limit value: quoted string or bare token (digits, ident, slash-separated).
fn limit_value(input: &mut &str) -> ModalResult<String> {
    alt((
        quoted_string,
        // Bare value: digits and/or letters, slashes, etc.
        winnow::token::take_while(1.., |c: char| {
            c.is_ascii_alphanumeric() || c == '_' || c == '/'
        })
        .map(|s: &str| s.to_string()),
    ))
    .parse_next(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    #[test]
    fn limits_block_parses_items_and_requires_nonempty() {
        let mut s = "limits { max_memory = \"1g\"; disk = 100 }";
        let l = limits_block
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("limits parse failed: {e:?}"));
        assert_eq!(l.items.len(), 2);
        assert_eq!(l.items[0].key, "max_memory");
        assert_eq!(l.items[0].value, "1g");
        assert_eq!(l.items[1].key, "disk");
        assert_eq!(l.items[1].value, "100");
        // 空块 = 语法错误
        let mut s2 = "limits {}";
        assert!(
            limits_block.parse_next(&mut s2).is_err(),
            "空 limits 块拒绝"
        );
    }

    #[test]
    fn yield_version_and_preset_list_split_keeps_parse() {
        // @vN（紧跟目标名）+ `: preset1<args>, preset2<args>`（引用参数为尖括号 body）
        let mut s = "yield w@v2 : base_alerts<\"high\">, tags<2> (x = 1)";
        let y = yield_clause
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("yield parse failed: {e:?}"));
        assert_eq!(y.target, "w");
        assert_eq!(y.version, Some(2));
        assert_eq!(y.presets.len(), 2);
        assert_eq!(y.presets[0].name, "base_alerts");
        assert_eq!(y.presets[0].args.len(), 1, "base_alerts<\"high\">");
        assert_eq!(y.presets[1].name, "tags");
        assert_eq!(y.presets[1].args.len(), 1, "tags<2>");
        assert_eq!(y.args.len(), 1, "尾部命名参数 (x = 1)");
        assert!(s.is_empty());
    }
}

#[test]
fn entity_clause_ident_and_string_lit_types() {
    let mut s = "entity(ip, e.sip)";
    let e = entity_clause
        .parse_next(&mut s)
        .unwrap_or_else(|e| panic!("entity parse failed: {e:?}"));
    assert_eq!(e.entity_type, EntityTypeVal::Ident("ip".into()));
    assert_eq!(
        e.id_expr,
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))
    );
    assert!(s.is_empty());

    // 字符串字面量实体类型
    let mut s = "entity(\"flow.dns\", e.qname)";
    let e = entity_clause.parse_next(&mut s).unwrap();
    assert_eq!(e.entity_type, EntityTypeVal::StringLit("flow.dns".into()));
}

#[test]
fn yield_preset_decl_with_params_and_args() {
    let mut s = "yield preset base_alerts <severity, source = \"wfusion\"> (y = $severity)";
    let d = yield_preset_decl
        .parse_next(&mut s)
        .unwrap_or_else(|e| panic!("preset decl parse failed: {e:?}"));
    assert_eq!(d.name, "base_alerts");
    assert_eq!(d.params.len(), 2);
    assert_eq!(d.params[0].name, "severity");
    assert_eq!(d.params[1].name, "source");
    assert!(d.params[1].default.is_some());
    assert_eq!(d.args.len(), 1);
    assert_eq!(d.args[0].name, "y");
    assert!(s.is_empty());
}
