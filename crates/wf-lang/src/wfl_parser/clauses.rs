use winnow::combinator::{alt, cut_err, opt, separated};
use winnow::error::{ContextError, ErrMode, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{duration_value, ident, kw, nonneg_integer, quoted_string, ws_skip};

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
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;

    // entity_type: ident or string literal
    let entity_type = alt((
        quoted_string.map(EntityTypeVal::StringLit),
        ident.map(|s: &str| EntityTypeVal::Ident(s.to_string())),
    ))
    .parse_next(input)?;

    ws_skip.parse_next(input)?;
    cut_err(literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let id_expr = cut_err(expr::parse_expr).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;

    Ok(EntityClause {
        entity_type,
        id_expr,
    })
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
    ws_skip.parse_next(input)?;
    let args = cut_err(named_args_parens).parse_next(input)?;

    Ok(YieldPresetDecl { name, params, args })
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
    let version = if opt(literal("@")).parse_next(input)?.is_some() {
        cut_err(literal("v")).parse_next(input)?;
        let n = cut_err(nonneg_integer).parse_next(input)?;
        Some(n as u32)
    } else {
        None
    };

    ws_skip.parse_next(input)?;
    let presets = if opt(literal(":")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        separated(1.., yield_preset_ref, (ws_skip, literal(","), ws_skip)).parse_next(input)?
    } else {
        Vec::new()
    };

    ws_skip.parse_next(input)?;
    let args = cut_err(named_args_parens).parse_next(input)?;

    Ok(YieldClause {
        target,
        version,
        presets,
        args,
    })
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

fn yield_preset_params(input: &mut &str) -> ModalResult<Vec<YieldPresetParam>> {
    let body = parse_angle_body(input, starts_named_args_parens)?;
    parse_preset_param_items(body)
}

fn parse_angle_body<'a>(
    input: &mut &'a str,
    close_is_valid: impl Fn(&str) -> bool,
) -> ModalResult<&'a str> {
    literal("<").parse_next(input)?;
    let body_start = *input;
    let Some(close_idx) = find_angle_close(body_start, close_is_valid) else {
        return Err(parse_cut_error());
    };
    let body = &body_start[..close_idx];
    *input = &body_start[close_idx + 1..];
    Ok(body)
}

fn parse_preset_param_items(body: &str) -> ModalResult<Vec<YieldPresetParam>> {
    let mut rest = body;
    ws_skip.parse_next(&mut rest)?;
    if rest.is_empty() {
        return Ok(Vec::new());
    }

    let params: Vec<YieldPresetParam> = separated(1.., preset_param_item, angle_comma_sep)
        .parse_next(&mut rest)
        .map_err(|_| parse_cut_error())?;
    ws_skip.parse_next(&mut rest)?;
    let _ = opt(angle_comma_sep).parse_next(&mut rest)?;
    ws_skip.parse_next(&mut rest)?;
    if rest.is_empty() {
        Ok(params)
    } else {
        Err(parse_cut_error())
    }
}

fn preset_param_item(input: &mut &str) -> ModalResult<YieldPresetParam> {
    ws_skip.parse_next(input)?;
    let name = ident.parse_next(input)?.to_string();
    ws_skip.parse_next(input)?;
    let default = if opt(literal("=")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Some(cut_err(expr::parse_expr).parse_next(input)?)
    } else {
        None
    };
    Ok(YieldPresetParam { name, default })
}

fn parse_preset_ref_arg_items(body: &str) -> ModalResult<Vec<Expr>> {
    let mut rest = body;
    ws_skip.parse_next(&mut rest)?;
    if rest.is_empty() {
        return Ok(Vec::new());
    }

    let args: Vec<Expr> = separated(1.., preset_ref_arg_item, angle_comma_sep)
        .parse_next(&mut rest)
        .map_err(|_| parse_cut_error())?;
    ws_skip.parse_next(&mut rest)?;
    let _ = opt(angle_comma_sep).parse_next(&mut rest)?;
    ws_skip.parse_next(&mut rest)?;
    if rest.is_empty() {
        Ok(args)
    } else {
        Err(parse_cut_error())
    }
}

fn preset_ref_arg_item(input: &mut &str) -> ModalResult<Expr> {
    ws_skip.parse_next(input)?;
    expr::parse_expr.parse_next(input)
}

fn angle_comma_sep(input: &mut &str) -> ModalResult<()> {
    ws_skip.parse_next(input)?;
    literal(",").parse_next(input)?;
    ws_skip.parse_next(input)?;
    Ok(())
}

fn yield_preset_ref(input: &mut &str) -> ModalResult<YieldPresetRef> {
    let name = ident.map(str::to_string).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let args = opt(yield_preset_ref_args)
        .parse_next(input)?
        .unwrap_or_default();
    Ok(YieldPresetRef { name, args })
}

fn yield_preset_ref_args(input: &mut &str) -> ModalResult<Vec<Expr>> {
    let body = parse_angle_body(input, |after| {
        after.starts_with(',') || starts_named_args_parens(after)
    })?;
    parse_preset_ref_arg_items(body)
}

fn find_angle_close(body_start: &str, close_is_valid: impl Fn(&str) -> bool) -> Option<usize> {
    let bytes = body_start.as_bytes();
    let mut i = 0;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'"' => i = skip_string(body_start, i),
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'/' => i = skip_line_comment(bytes, i),
            b'(' => {
                paren_depth += 1;
                i += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                i += 1;
            }
            b'[' => {
                bracket_depth += 1;
                i += 1;
            }
            b']' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                i += 1;
            }
            b'{' => {
                brace_depth += 1;
                i += 1;
            }
            b'}' => {
                brace_depth = brace_depth.saturating_sub(1);
                i += 1;
            }
            b'>' if paren_depth == 0
                && bracket_depth == 0
                && brace_depth == 0
                && close_is_valid(skip_ws_and_line_comments_str(&body_start[i + 1..])) =>
            {
                return Some(i);
            }
            _ => i += 1,
        }
    }
    None
}

fn skip_ws_and_line_comments_str(value: &str) -> &str {
    let i = skip_ws_and_line_comments(value.as_bytes(), 0);
    &value[i..]
}

fn starts_named_args_parens(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut i = skip_ws_and_line_comments(bytes, 0);
    if i >= bytes.len() || bytes[i] != b'(' {
        return false;
    }
    i += 1;
    i = skip_ws_and_line_comments(bytes, i);
    if i < bytes.len() && bytes[i] == b')' {
        return true;
    }
    if i >= bytes.len() || !is_ident_start_byte(bytes[i]) {
        return false;
    }
    while i < bytes.len() && is_ident_cont_byte(bytes[i]) {
        i += 1;
    }
    i = skip_ws_and_line_comments(bytes, i);
    i < bytes.len() && bytes[i] == b'='
}

fn skip_ws_and_line_comments(bytes: &[u8], mut i: usize) -> usize {
    loop {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            i = skip_line_comment(bytes, i);
        } else {
            return i;
        }
    }
}

fn is_ident_start_byte(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_cont_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn skip_string(source: &str, start: usize) -> usize {
    let bytes = source.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() && bytes[i] != b'"' {
        i += source[i..].chars().next().unwrap().len_utf8();
    }
    if i < bytes.len() { i + 1 } else { i }
}

fn skip_line_comment(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    i
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
// join clause
// ---------------------------------------------------------------------------

/// 解析 `within` / `reduce` 修饰语：两者可任一顺序、各至多一次。
/// 每个位置尝试失败后回退输入，保证顺序无关（对齐 join_clause 原行为）。
fn parse_within_or_reduce_modifiers(
    input: &mut &str,
) -> ModalResult<(Option<WithinSpec>, Option<ReduceClause>)> {
    let mut within = None;
    let mut reduce = None;
    loop {
        ws_skip.parse_next(input)?;
        let saved = *input;
        if within.is_none()
            && let Ok(w) = within_clause.parse_next(input)
        {
            within = Some(w);
            continue;
        }
        *input = saved;
        if reduce.is_none()
            && let Ok(r) = reduce_clause.parse_next(input)
        {
            reduce = Some(r);
            continue;
        }
        *input = saved;
        break;
    }
    Ok((within, reduce))
}

/// 解析 `&&` 分隔的连接条件（首个条件为 cut）。
fn parse_join_conditions(input: &mut &str) -> ModalResult<Vec<JoinCondition>> {
    let first = cut_err(join_cond).parse_next(input)?;
    let mut conditions = vec![first];
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("&&")).parse_next(input)?.is_some() {
            ws_skip.parse_next(input)?;
            let cond = cut_err(join_cond).parse_next(input)?;
            conditions.push(cond);
        } else {
            break;
        }
    }
    Ok(conditions)
}

/// 把 `as <label>`（可空）挂到 reduce 上；无 reduce 或 label 已存在均为语法错误。
fn attach_join_label(
    reduce: &mut Option<ReduceClause>,
    label: Option<String>,
) -> Result<(), ErrMode<ContextError>> {
    let Some(label) = label else {
        return Ok(());
    };
    match reduce.as_mut() {
        Some(rc) if rc.label.is_none() => {
            rc.label = Some(label);
            Ok(())
        }
        _ => Err(parse_cut_error()),
    }
}

/// `join WINDOW [mode] [within ...] [reduce ...] on cond [&& cond] [as label] [emit at expr]`
///
/// - `within` / `reduce` 位于 mode 与 `on` 之间，两者可任一顺序、各至多一次；
/// - `as label`（Q9 形态）可跟在 `on` 条件之后；BNF 形态 `reduce ... as label` 也可；
/// - `emit at <expr>` 为 deferred 标记 + 触发点（P1 语法）。
pub(super) fn join_clause(input: &mut &str) -> ModalResult<JoinClause> {
    ws_skip.parse_next(input)?;
    kw("join").parse_next(input)?;
    ws_skip.parse_next(input)?;

    let target_window = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "target window name",
        )))
        .parse_next(input)?
        .to_string();

    // mode 缺省 = 纯存在 inner（设计 D4）
    ws_skip.parse_next(input)?;
    let mode = opt(join_mode).parse_next(input)?.unwrap_or(JoinMode::Inner);

    let (within, mut reduce) = parse_within_or_reduce_modifiers(input)?;

    ws_skip.parse_next(input)?;
    cut_err(kw("on"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'on' after join mode",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;

    let conditions = parse_join_conditions(input)?;

    // `as label`（Q9 形态：跟在 on 条件之后；需 reduce 且未重复）
    ws_skip.parse_next(input)?;
    let label = opt(as_label).parse_next(input)?;
    attach_join_label(&mut reduce, label)?;

    // `emit at <expr>`（deferred 标记）
    ws_skip.parse_next(input)?;
    let emit_at = opt(emit_at_clause).parse_next(input)?;

    Ok(JoinClause {
        target_window,
        mode,
        conditions,
        within,
        reduce,
        emit_at,
    })
}

fn join_mode(input: &mut &str) -> ModalResult<JoinMode> {
    alt((
        (kw("asof"), ws_skip, opt(asof_within)).map(|(_, _, within)| JoinMode::Asof { within }),
        kw("snapshot").map(|_| JoinMode::Snapshot),
        kw("anti").map(|_| JoinMode::Anti),
    ))
    .parse_next(input)
}

fn asof_within(input: &mut &str) -> ModalResult<std::time::Duration> {
    kw("within").parse_next(input)?;
    ws_skip.parse_next(input)?;
    // 非 cut：`asof within [lo, hi]` 时 duration 解析失败须回溯，让位给 interval
    // within 子句（`asof` 降级为无 within 的 mode）。
    duration_value.parse_next(input)
}

/// `within [lo, hi]` 或 `within dur` 糖（≡ `within [-dur, 0s]`）。
fn within_clause(input: &mut &str) -> ModalResult<WithinSpec> {
    kw("within").parse_next(input)?;
    ws_skip.parse_next(input)?;
    if opt(literal("[")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let lo = cut_err(bound_value).parse_next(input)?;
        ws_skip.parse_next(input)?;
        cut_err(literal(",")).parse_next(input)?;
        ws_skip.parse_next(input)?;
        let hi = cut_err(bound_value).parse_next(input)?;
        ws_skip.parse_next(input)?;
        cut_err(literal("]")).parse_next(input)?;
        Ok(WithinSpec { lo, hi })
    } else {
        // `within 10s` 糖 ≡ within [-10s, 0s]
        let dur = cut_err(duration_value).parse_next(input)?;
        Ok(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Dur { dur, neg: true },
            },
            hi: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: std::time::Duration::ZERO,
                    neg: false,
                },
            },
        })
    }
}

/// 区间界：`['<' | '<='] (dur | expr)`。`<` 前缀 = 开区间；`<=`/缺省 = 闭。
fn bound_value(input: &mut &str) -> ModalResult<Bound> {
    // 开闭记号：`<=`（闭，显式）| `<`（开）| 缺省（闭）
    let open = if opt(literal("<=")).parse_next(input)?.is_some() {
        false
    } else {
        opt(literal("<")).parse_next(input)?.is_some()
    };
    let after_marker = *input;
    ws_skip.parse_next(input)?;
    if let Ok(dur) = duration_value.parse_next(input) {
        return Ok(Bound {
            open,
            val: BoundVal::Dur { dur, neg: false },
        });
    }
    // 非时长 → 左行绝对时间表达式（字段/函数调用）；重置到记号后解析
    *input = after_marker;
    ws_skip.parse_next(input)?;
    let expr = cut_err(expr::parse_expr).parse_next(input)?;
    Ok(Bound {
        open,
        val: BoundVal::Expr(expr),
    })
}

/// `reduce maxrow(field) [tie(field asc|desc)] | minrow(...) | last(field) | top(N, field)`
fn reduce_clause(input: &mut &str) -> ModalResult<ReduceClause> {
    kw("reduce").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let measure = cut_err(reduce_measure).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let label = opt(as_label).parse_next(input)?;
    Ok(ReduceClause { measure, label })
}

/// 按度量名构造 ReduceMeasure：maxrow/minrow 尾随可选 tie，top 携带 N。
fn finish_reduce_measure(
    name: &str,
    field: FieldRef,
    n: Option<u64>,
    input: &mut &str,
) -> ModalResult<ReduceMeasure> {
    match name {
        "maxrow" => Ok(ReduceMeasure::Maxrow {
            field,
            tie: opt_tie(input)?,
        }),
        "minrow" => Ok(ReduceMeasure::Minrow {
            field,
            tie: opt_tie(input)?,
        }),
        "last" => Ok(ReduceMeasure::Last { field }),
        "top" => Ok(ReduceMeasure::Top {
            n: n.expect("top parsed n"),
            field,
        }),
        _ => Err(parse_cut_error()),
    }
}

fn reduce_measure(input: &mut &str) -> ModalResult<ReduceMeasure> {
    let name = cut_err(ident).parse_next(input)?.to_string();
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let (field, n) = if name == "top" {
        let n = cut_err(nonneg_integer).parse_next(input)? as u64;
        ws_skip.parse_next(input)?;
        cut_err(literal(",")).parse_next(input)?;
        ws_skip.parse_next(input)?;
        let field = cut_err(join_field_ref).parse_next(input)?;
        (field, Some(n))
    } else {
        let field = cut_err(join_field_ref).parse_next(input)?;
        (field, None)
    };
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    finish_reduce_measure(&name, field, n, input)
}

/// `tie(field asc|desc)`（闭括号后）。
fn opt_tie(input: &mut &str) -> ModalResult<Option<TieSpec>> {
    ws_skip.parse_next(input)?;
    if opt(kw("tie")).parse_next(input)?.is_none() {
        return Ok(None);
    }
    ws_skip.parse_next(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let field = cut_err(join_field_ref).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let _ = opt(literal(",")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let desc = opt(alt((kw("asc").value(false), kw("desc").value(true))))
        .parse_next(input)?
        .unwrap_or(false);
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(Some(TieSpec { field, desc }))
}

/// `as <label>`。
fn as_label(input: &mut &str) -> ModalResult<String> {
    kw("as").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(ident).parse_next(input).map(|s| s.to_string())
}

/// `emit at <expr>`。
fn emit_at_clause(input: &mut &str) -> ModalResult<Expr> {
    kw("emit").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(kw("at")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(expr::parse_expr).parse_next(input)
}

fn join_cond(input: &mut &str) -> ModalResult<JoinCondition> {
    let left = join_field_ref.parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("==")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let right = cut_err(join_field_ref).parse_next(input)?;
    Ok(JoinCondition { left, right })
}

/// Parse a field reference for join conditions: `ident.ident` or `ident`
fn join_field_ref(input: &mut &str) -> ModalResult<FieldRef> {
    let first = ident.parse_next(input)?;
    if opt(literal(".")).parse_next(input)?.is_some() {
        let second = cut_err(ident).parse_next(input)?;
        Ok(FieldRef::Qualified(first.to_string(), second.to_string()))
    } else {
        Ok(FieldRef::Simple(first.to_string()))
    }
}

// ---------------------------------------------------------------------------
// limits block
// ---------------------------------------------------------------------------

/// `limits { key = value; ... }`
pub(super) fn limits_block(input: &mut &str) -> ModalResult<LimitsBlock> {
    kw("limits").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let mut items = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let key = cut_err(ident).parse_next(input)?.to_string();
        ws_skip.parse_next(input)?;
        cut_err(literal("=")).parse_next(input)?;
        ws_skip.parse_next(input)?;
        // Value can be a quoted string or an integer/ident
        let value = cut_err(limit_value).parse_next(input)?;
        ws_skip.parse_next(input)?;
        // Optional semicolon terminator
        let _ = opt(literal(";")).parse_next(input)?;
        items.push(LimitItem { key, value });
    }
    if items.is_empty() {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new(),
        ));
    }
    Ok(LimitsBlock { items })
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

    fn join(input: &str) -> JoinClause {
        let mut s = input;
        join_clause
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("join parse failed for {input:?}: {e:?}"))
    }

    fn join_err(input: &str) {
        let mut s = input;
        assert!(
            join_clause.parse_next(&mut s).is_err(),
            "expected join parse error for {input:?}"
        );
    }

    fn measure(input: &str) -> ReduceMeasure {
        let mut s = input;
        reduce_measure
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("reduce_measure failed for {input:?}: {e:?}"))
    }

    #[test]
    fn join_modifiers_any_order() {
        // within 在前
        let j = join("join w within 10s reduce maxrow(price) on a == w.b");
        assert!(j.within.is_some());
        assert!(matches!(
            j.reduce.as_ref().map(|r| &r.measure),
            Some(ReduceMeasure::Maxrow { .. })
        ));
        // reduce 在前
        let j = join("join w reduce last(x) within 5s on a == w.b");
        assert!(matches!(
            j.reduce.as_ref().map(|r| &r.measure),
            Some(ReduceMeasure::Last { .. })
        ));
        assert!(j.within.is_some());
    }

    #[test]
    fn join_defaults_to_inner_and_accepts_snapshot() {
        let j = join("join w on a.x == w.y");
        assert_eq!(j.mode, JoinMode::Inner);
        assert_eq!(j.conditions.len(), 1);
        assert!(j.within.is_none() && j.reduce.is_none() && j.emit_at.is_none());

        let j = join("join w snapshot on a == w.b");
        assert_eq!(j.mode, JoinMode::Snapshot);
    }

    #[test]
    fn join_parses_multiple_conditions() {
        let j = join("join w on a.x == w.y && a.z == w.q && a.t == w.u");
        assert_eq!(j.conditions.len(), 3);
        assert!(matches!(&j.conditions[1], JoinCondition { .. }));
    }

    #[test]
    fn join_attach_label_requires_reduce() {
        // as label 无 reduce → 语法错误
        join_err("join w on a == w.b as best");
        // reduce 后 as label（BNF 形态）合法
        let j = join("join w reduce minrow(count) as best on a == w.b");
        assert_eq!(
            j.reduce.as_ref().and_then(|r| r.label.as_deref()),
            Some("best")
        );
    }

    #[test]
    fn join_rejects_duplicate_reduce_and_unknown_measure() {
        join_err("join w reduce last(x) reduce last(y) on a == w.b");
        join_err("join w reduce bogus(x) on a == w.b");
    }

    #[test]
    fn reduce_measure_variants() {
        assert!(matches!(measure("last(ts)"), ReduceMeasure::Last { .. }));
        assert!(matches!(
            measure("top(3, dist)"),
            ReduceMeasure::Top { n: 3, .. }
        ));
        match measure("minrow(count) tie(ts desc)") {
            ReduceMeasure::Minrow { tie: Some(tie), .. } => {
                assert!(tie.desc);
                assert_eq!(tie.field, FieldRef::Simple("ts".into()));
            }
            other => panic!("expected Minrow with tie, got {other:?}"),
        }
    }

    #[test]
    fn join_emit_at_and_open_bound_within() {
        let j = join("join w within [a.t, <b.t] on a == w.b emit at a.t");
        assert!(j.emit_at.is_some());
        let w = j.within.expect("within");
        assert!(!w.lo.open && w.hi.open);
    }
}
