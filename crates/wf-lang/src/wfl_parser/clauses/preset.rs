//! yield preset 参数/引用解析与尖括号 body 扫描（clauses/ 拆分）: preset 形参
//! 与引用参数都写在 `<...>` 内（body 由 [`find_angle_close`] 用深度/字符串/
//! 注释感知扫描找出, 不受内部 `<>`/括号干扰）。

use winnow::combinator::{cut_err, opt, separated};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, ws_skip};

use super::super::expr;
use super::parse_cut_error;

/// `<...>` 内的 preset 形参（`name[ = default]` 列表）。
pub(super) fn yield_preset_params(input: &mut &str) -> ModalResult<Vec<YieldPresetParam>> {
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
    parse_angle_items(body, preset_param_item)
}

fn parse_preset_ref_arg_items(body: &str) -> ModalResult<Vec<Expr>> {
    parse_angle_items(body, preset_ref_arg_item)
}

fn preset_ref_arg_item(input: &mut &str) -> ModalResult<Expr> {
    ws_skip.parse_next(input)?;
    expr::parse_expr.parse_next(input)
}

/// `<...>` body 内逗号分隔条目（允许尾逗号; body 需恰好消费完）——
/// preset 形参与引用参数共用同一形状。空 body → 空列表。
fn parse_angle_items<T>(body: &str, item: fn(&mut &str) -> ModalResult<T>) -> ModalResult<Vec<T>> {
    let mut rest = body;
    ws_skip.parse_next(&mut rest)?;
    if rest.is_empty() {
        return Ok(Vec::new());
    }

    let items: Vec<T> = separated(1.., item, angle_comma_sep)
        .parse_next(&mut rest)
        .map_err(|_| parse_cut_error())?;
    ws_skip.parse_next(&mut rest)?;
    let _ = opt(angle_comma_sep).parse_next(&mut rest)?;
    ws_skip.parse_next(&mut rest)?;
    if rest.is_empty() {
        Ok(items)
    } else {
        Err(parse_cut_error())
    }
}

fn preset_param_item(input: &mut &str) -> ModalResult<YieldPresetParam> {
    ws_skip.parse_next(input)?;
    let name = ident.parse_next(input)?.to_string();
    let default = parse_param_default(input)?;
    Ok(YieldPresetParam { name, default })
}

/// `[= <expr>]` 形参默认值（缺省 None）。
fn parse_param_default(input: &mut &str) -> ModalResult<Option<Expr>> {
    ws_skip.parse_next(input)?;
    if opt(literal("=")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        Ok(Some(cut_err(expr::parse_expr).parse_next(input)?))
    } else {
        Ok(None)
    }
}

fn angle_comma_sep(input: &mut &str) -> ModalResult<()> {
    ws_skip.parse_next(input)?;
    literal(",").parse_next(input)?;
    ws_skip.parse_next(input)?;
    Ok(())
}

/// `<...>` 内的 preset 引用（`name` + 可选 `<args>`）。
pub(super) fn yield_preset_ref(input: &mut &str) -> ModalResult<YieldPresetRef> {
    let name = ident.map(str::to_string).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let args = opt(yield_preset_ref_args)
        .parse_next(input)?
        .unwrap_or_default();
    Ok(YieldPresetRef { name, args })
}

/// `yield preset(<name>, args...)` 引用参数（`<...>` 内为 Expr 列表）。
pub(super) fn yield_preset_ref_args(input: &mut &str) -> ModalResult<Vec<Expr>> {
    let body = parse_angle_body(input, |after| {
        after.starts_with(',') || starts_named_args_parens(after)
    })?;
    parse_preset_ref_arg_items(body)
}

fn find_angle_close(body_start: &str, close_is_valid: impl Fn(&str) -> bool) -> Option<usize> {
    let bytes = body_start.as_bytes();
    let mut i = 0;
    // 嵌套深度（括号/方括号/花括号; 归并到数组, 顶层 = 全零）。
    let mut depth = [0usize; 3];

    while i < bytes.len() {
        match delimit_kind(bytes[i]) {
            Some((k, true)) => depth[k] += 1,
            Some((k, false)) => depth[k] = depth[k].saturating_sub(1),
            None => match bytes[i] {
                b'"' => {
                    i = skip_string(body_start, i);
                    continue;
                }
                b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'/' => {
                    i = skip_line_comment(bytes, i);
                    continue;
                }
                b'>' if depth.iter().all(|&d| d == 0)
                    && close_is_valid(skip_ws_and_line_comments_str(&body_start[i + 1..])) =>
                {
                    return Some(i);
                }
                _ => {}
            },
        }
        i += 1;
    }
    None
}

/// 界定符字节 → `(深度槽, 是否为开括号)`; 非界定符 → None。
fn delimit_kind(b: u8) -> Option<(usize, bool)> {
    Some(match b {
        b'(' => (0, true),
        b')' => (0, false),
        b'[' => (1, true),
        b']' => (1, false),
        b'{' => (2, true),
        b'}' => (2, false),
        _ => return None,
    })
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
    looks_like_named_arg_tail(bytes, i)
}

/// `(` 之后：空 `()` 或 `ident =` 形态才视作命名参数（否则非）。
fn looks_like_named_arg_tail(bytes: &[u8], mut i: usize) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    #[test]
    fn preset_params_defaults_and_plain_names() {
        // 形参 `<n = 1, u>` 之后必须有命名参数括号（starts_named_args_parens 校验）。
        let mut s = "<n = 1, u>(a = 1)";
        let params = yield_preset_params
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("preset params parse failed: {e:?}"));
        assert_eq!(params.len(), 2, "n + u");
        assert_eq!(params[0].name, "n");
        assert!(matches!(&params[0].default, Some(Expr::Number(v)) if *v == 1.0));
        assert_eq!(params[1].name, "u");
        assert!(params[1].default.is_none(), "无默认值形参");
        assert_eq!(s, "(a = 1)", "只消费 <...> body");
    }

    #[test]
    fn preset_ref_name_only_and_with_args() {
        // 纯名引用（列表内需逗号/命名参数后缀才能闭合）。
        let mut s = "p1, p2";
        let r = yield_preset_ref.parse_next(&mut s).unwrap();
        assert_eq!(r.name, "p1");
        assert!(r.args.is_empty());
        assert_eq!(s, ", p2");

        // 带 `<args>` 的引用：body 逗号后的 '>' 是闭合点。
        let mut s = "p2<1, 2>, p3";
        let r = yield_preset_ref.parse_next(&mut s).unwrap();
        assert_eq!(r.name, "p2");
        assert_eq!(r.args.len(), 2);
        assert!(matches!(&r.args[1], Expr::Number(v) if *v == 2.0));
        assert_eq!(s, ", p3", "参数 body 消费到闭合 >");
    }

    #[test]
    fn angle_close_respects_strings_and_line_comments() {
        // 字符串内的 '>' 不参与闭合判定；注释同理。
        let body = r#""1>2" >x"#;
        let idx = find_angle_close(body, |after| after.starts_with('x')).unwrap();
        assert_eq!(&body[idx..], ">x", "跳过字符串后首个 > 闭合");
        let body = "v // > 注释\n>x";
        let idx = find_angle_close(body, |after| after.starts_with('x')).unwrap();
        assert_eq!(&body[idx..], ">x", "行注释内含 > 被跳过");
    }
}

    #[test]
    fn angle_close_respects_nested_parens_brackets() {
        // 括号内的 '>' 不闭合; 深度归零后的 '>' 才有效
        let body = "f(a > 1) >x";
        let idx = find_angle_close(body, |after| after.starts_with('x')).unwrap();
        assert_eq!(&body[idx..], ">x", "括号内 > 被跳过");
        let body = "arr[0 > 1] >x";
        let idx = find_angle_close(body, |after| after.starts_with('x')).unwrap();
        assert_eq!(&body[idx..], ">x", "方括号内 > 被跳过");
    }

    #[test]
    fn angle_items_reject_unconsumed_tail() {
        // 形参列表后残留非法内容 → 语法错误（parse_angle_items 的 rest 空校验）
        let mut s = "<n = 1; junk>(a = 1)";
        assert!(yield_preset_params.parse_next(&mut s).is_err());
    }
