//! Variable preprocessing for WFL source text.
//!
//! Performs `$VAR` / `${VAR:default}` text substitution as a preprocessing
//! step before WFL parsing (compilation pipeline step 1).
//!
//! Context-aware: `$` references inside line comments (`// ...`) and `"..."`
//! string literals are passed through verbatim. Use `$$` to produce a literal
//! `$` in code positions.

use std::collections::{HashMap, HashSet};
use std::fmt;

#[cfg(test)]
mod tests;

/// Error during variable preprocessing.
#[derive(Debug, Clone)]
pub struct PreprocessError {
    /// Byte offset in the original source where the error occurred.
    pub position: usize,
    /// Human-readable description of the error.
    pub message: String,
}

impl fmt::Display for PreprocessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "preprocess error at position {}: {}",
            self.position, self.message
        )
    }
}

impl std::error::Error for PreprocessError {}

/// Perform variable substitution on WFL source text.
///
/// Scans `source` left-to-right and replaces variable references:
/// - `$IDENT` — replaced by value from `vars`; error if undefined.
/// - `${IDENT}` — same as `$IDENT` (braces for disambiguation).
/// - `${IDENT:default}` — replaced by value if defined, otherwise by `default`.
/// - `$$` — produces a literal `$`.
///
/// IDENT matches `[A-Za-z_][A-Za-z0-9_]*`.
///
/// Variable references inside `// ...` line comments and `"..."` string
/// literals are **not** processed — the text is copied verbatim.
/// Note: `#` is reserved for future annotation syntax and is not treated as a comment.
///
/// A bare `$` not followed by IDENT, `{`, or `$` is left as-is.
/// An unterminated `${...` (missing `}`) is an error.
pub fn preprocess_vars(
    source: &str,
    vars: &HashMap<String, String>,
) -> Result<String, PreprocessError> {
    preprocess_impl(source, vars, false)
}

/// Like [`preprocess_vars`], but falls back to environment variables for
/// any variable not found in `vars`. Useful for project tools (explain,
/// lint) where variables may come from the shell environment rather than
/// a config file.
pub fn preprocess_vars_with_env(
    source: &str,
    vars: &HashMap<String, String>,
) -> Result<String, PreprocessError> {
    preprocess_impl(source, vars, true)
}

fn preprocess_impl(
    source: &str,
    vars: &HashMap<String, String>,
    env_fallback: bool,
) -> Result<String, PreprocessError> {
    preprocess_impl_with_preserved_bare_vars(source, vars, env_fallback, 0, None)
}

fn preprocess_impl_with_preserved_bare_vars(
    source: &str,
    vars: &HashMap<String, String>,
    env_fallback: bool,
    base_pos: usize,
    preserve_bare_vars: Option<&HashSet<String>>,
) -> Result<String, PreprocessError> {
    let bytes = source.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len);
    let mut i = 0;

    while i < len {
        match bytes[i] {
            // --- Line comment: pass through until newline ---
            b'/' if i + 1 < len && bytes[i + 1] == b'/' => {
                copy_line_comment(source, &mut i, &mut out);
            }

            // --- String literal: pass through including $ ---
            b'"' => copy_string_literal(source, &mut i, &mut out),

            // --- Dollar: variable reference or escape ---
            b'$' => {
                let dollar_pos = i;
                i += 1;

                if i < len && bytes[i] == b'$' {
                    // $$ → literal $
                    out.push('$');
                    i += 1;
                } else if i < len && bytes[i] == b'{' {
                    // Braced form: ${IDENT} or ${IDENT:default}
                    i = preprocess_braced_var(
                        source,
                        i,
                        vars,
                        env_fallback,
                        base_pos + dollar_pos,
                        &mut out,
                    )?;
                } else if i < len && is_ident_start(bytes[i]) {
                    // Bare form: $IDENT
                    i = preprocess_bare_var(
                        source,
                        i,
                        vars,
                        env_fallback,
                        preserve_bare_vars,
                        base_pos + dollar_pos,
                        &mut out,
                    )?;
                } else {
                    // Bare '$' not followed by IDENT, '{', or '$' — leave as-is
                    out.push('$');
                }
            }

            // --- Normal character ---
            _ => {
                if preserve_bare_vars.is_none()
                    && bytes[i] == b'y'
                    && try_preprocess_yield_preset_decl(
                        source,
                        &mut i,
                        &mut out,
                        vars,
                        env_fallback,
                        base_pos,
                    )?
                {
                    continue;
                }

                // --- Pattern block: skip verbatim (avoid ${param} conflict) ---
                if bytes[i] == b'p' && try_skip_pattern_block(source, &mut i, &mut out) {
                    continue;
                }

                let ch = source[i..].chars().next().unwrap();
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }

    Ok(out)
}

/// 逐字节复制 `// ...` 行注释直到换行（含），原样保留注释内 `$`。
fn copy_line_comment(source: &str, i: &mut usize, out: &mut String) {
    let bytes = source.as_bytes();
    while *i < bytes.len() && bytes[*i] != b'\n' {
        out.push(bytes[*i] as char);
        *i += 1;
    }
}

/// 逐字符复制 `"..."` 字符串字面量（含闭合引号；未闭合则复制到末尾），
/// 原样保留字符串内 `$`。
fn copy_string_literal(source: &str, i: &mut usize, out: &mut String) {
    let bytes = source.as_bytes();
    out.push('"');
    *i += 1;
    while *i < bytes.len() && bytes[*i] != b'"' {
        let ch = source[*i..].chars().next().unwrap();
        out.push(ch);
        *i += ch.len_utf8();
    }
    if *i < bytes.len() {
        out.push('"');
        *i += 1;
    }
}

/// 处理 `${IDENT}` / `${IDENT:default}`；`i` 指向 `{` 本身。返回下一个未消费位置。
fn preprocess_braced_var(
    source: &str,
    mut i: usize,
    vars: &HashMap<String, String>,
    env_fallback: bool,
    dollar_pos: usize,
    out: &mut String,
) -> Result<usize, PreprocessError> {
    let bytes = source.as_bytes();
    let len = bytes.len();
    i += 1; // skip '{'
    let ident_start = i;

    // Read IDENT
    if i >= len || !is_ident_start(bytes[i]) {
        return Err(PreprocessError {
            position: dollar_pos,
            message: "expected variable name after ${".to_string(),
        });
    }
    while i < len && is_ident_cont(bytes[i]) {
        i += 1;
    }
    let ident = &source[ident_start..i];

    // Check for ':default' or '}'
    let default_val = if i < len && bytes[i] == b':' {
        i += 1; // skip ':'
        let default_start = i;
        while i < len && bytes[i] != b'}' {
            i += 1;
        }
        Some(&source[default_start..i])
    } else {
        None
    };

    // Expect closing '}'
    if i >= len || bytes[i] != b'}' {
        return Err(PreprocessError {
            position: dollar_pos,
            message: format!(
                "unterminated variable reference '${{{}' — missing '}}'",
                ident
            ),
        });
    }
    i += 1; // skip '}'
    out.push_str(&resolve_variable(
        ident,
        default_val,
        vars,
        env_fallback,
        dollar_pos,
    )?);
    Ok(i)
}

/// 处理裸 `$IDENT`；`i` 指向 IDENT 首字符。命中 preserve 名单时原样保留 `$ident`。
fn preprocess_bare_var(
    source: &str,
    mut i: usize,
    vars: &HashMap<String, String>,
    env_fallback: bool,
    preserve_bare_vars: Option<&HashSet<String>>,
    dollar_pos: usize,
    out: &mut String,
) -> Result<usize, PreprocessError> {
    let bytes = source.as_bytes();
    let len = bytes.len();
    let ident_start = i;
    while i < len && is_ident_cont(bytes[i]) {
        i += 1;
    }
    let ident = &source[ident_start..i];

    if preserve_bare_vars.is_some_and(|names| names.contains(ident)) {
        out.push('$');
        out.push_str(ident);
    } else {
        out.push_str(&resolve_variable(
            ident,
            None,
            vars,
            env_fallback,
            dollar_pos,
        )?);
    }
    Ok(i)
}

/// 变量解析优先级：显式 vars → `:default` → 环境变量(可选) → 未定义错误。
fn resolve_variable(
    ident: &str,
    default_val: Option<&str>,
    vars: &HashMap<String, String>,
    env_fallback: bool,
    position: usize,
) -> Result<String, PreprocessError> {
    if let Some(val) = vars.get(ident) {
        return Ok(val.clone());
    }
    if let Some(def) = default_val {
        return Ok(def.to_string());
    }
    if env_fallback {
        if let Ok(val) = std::env::var(ident) {
            return Ok(val);
        }
        return Err(PreprocessError {
            position,
            message: format!(
                "undefined variable '{}' (not in --var or environment)",
                ident
            ),
        });
    }
    Err(PreprocessError {
        position,
        message: format!("undefined variable '{}'", ident),
    })
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn try_preprocess_yield_preset_decl(
    source: &str,
    pos: &mut usize,
    out: &mut String,
    vars: &HashMap<String, String>,
    env_fallback: bool,
    base_pos: usize,
) -> Result<bool, PreprocessError> {
    let start = *pos;
    let Some((params_start, params_end, body_end)) = yield_preset_decl_range(source, start) else {
        return Ok(false);
    };

    let params = if let Some((params_start, params_end)) = params_start.zip(params_end) {
        extract_yield_preset_param_names(&source[params_start..params_end])
    } else {
        HashSet::new()
    };
    let segment = &source[start..body_end];
    let processed = preprocess_impl_with_preserved_bare_vars(
        segment,
        vars,
        env_fallback,
        base_pos + start,
        Some(&params),
    )?;
    out.push_str(&processed);
    *pos = body_end;
    Ok(true)
}

/// `kw` 从 `start` 起且两侧都是标识符边界时返回末尾位置。
fn kw_boundary(bytes: &[u8], start: usize, kw: &[u8]) -> Option<usize> {
    let end = start.checked_add(kw.len())?;
    if end > bytes.len() || &bytes[start..end] != kw {
        return None;
    }
    if (start > 0 && is_ident_cont(bytes[start - 1]))
        || (end < bytes.len() && is_ident_cont(bytes[end]))
    {
        return None;
    }
    Some(end)
}

fn yield_preset_decl_range(
    source: &str,
    start: usize,
) -> Option<(Option<usize>, Option<usize>, usize)> {
    let bytes = source.as_bytes();
    let len = bytes.len();

    let mut i = kw_boundary(bytes, start, b"yield")?;
    i = skip_ws_and_line_comments(bytes, i);
    i = kw_boundary(bytes, i, b"preset")?;
    i = skip_ws_and_line_comments(bytes, i);

    if i >= len || !is_ident_start(bytes[i]) {
        return None;
    }
    while i < len && is_ident_cont(bytes[i]) {
        i += 1;
    }
    i = skip_ws_and_line_comments(bytes, i);

    let (params_start, params_end) = if i < len && bytes[i] == b'<' {
        let inner_start = i + 1;
        let inner_end = find_matching_angle(source, i)?;
        i = skip_ws_and_line_comments(bytes, inner_end + 1);
        (Some(inner_start), Some(inner_end))
    } else {
        (None, None)
    };

    if i >= len || bytes[i] != b'(' {
        return None;
    }
    let body_end = find_matching_paren(source, i)? + 1;
    Some((params_start, params_end, body_end))
}

fn extract_yield_preset_param_names(params: &str) -> HashSet<String> {
    let bytes = params.as_bytes();
    let mut names = HashSet::new();
    let mut i = 0;
    let len = bytes.len();

    while i < len {
        i = skip_ws_and_line_comments(bytes, i);
        if i >= len {
            break;
        }
        if is_ident_start(bytes[i]) {
            let ident_start = i;
            i += 1;
            while i < len && is_ident_cont(bytes[i]) {
                i += 1;
            }
            names.insert(params[ident_start..i].to_string());
        }
        i = skip_param_default_or_separator(params, i);
        if i < len && bytes[i] == b',' {
            i += 1;
        }
    }

    names
}

fn skip_param_default_or_separator(source: &str, mut i: usize) -> usize {
    let bytes = source.as_bytes();
    let len = bytes.len();
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;

    while i < len {
        match bytes[i] {
            b'"' => i = skip_string(source, i),
            b'/' if i + 1 < len && bytes[i + 1] == b'/' => i = skip_line_comment(bytes, i),
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
            b',' if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => break,
            _ => i += 1,
        }
    }

    i
}

fn find_matching_angle(source: &str, open: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let len = bytes.len();
    let mut i = open + 1;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;
    while i < len {
        match bytes[i] {
            b'"' => i = skip_string(source, i),
            b'/' if i + 1 < len && bytes[i + 1] == b'/' => i = skip_line_comment(bytes, i),
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
                && starts_named_args_parens(&source[i + 1..]) =>
            {
                return Some(i);
            }
            _ => i += 1,
        }
    }
    None
}

fn find_matching_paren(source: &str, open: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let len = bytes.len();
    let mut i = open + 1;
    let mut depth = 1usize;
    while i < len {
        match bytes[i] {
            b'"' => i = skip_string(source, i),
            b'/' if i + 1 < len && bytes[i + 1] == b'/' => i = skip_line_comment(bytes, i),
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    None
}

fn skip_ws(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

fn skip_ws_and_line_comments(bytes: &[u8], mut i: usize) -> usize {
    loop {
        i = skip_ws(bytes, i);
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            i = skip_line_comment(bytes, i);
        } else {
            return i;
        }
    }
}

fn starts_named_args_parens(source: &str) -> bool {
    let bytes = source.as_bytes();
    let mut i = skip_ws_and_line_comments(bytes, 0);
    if i >= bytes.len() || bytes[i] != b'(' {
        return false;
    }
    i += 1;
    i = skip_ws_and_line_comments(bytes, i);
    if i < bytes.len() && bytes[i] == b')' {
        return true;
    }
    if i >= bytes.len() || !is_ident_start(bytes[i]) {
        return false;
    }
    while i < bytes.len() && is_ident_cont(bytes[i]) {
        i += 1;
    }
    i = skip_ws_and_line_comments(bytes, i);
    i < bytes.len() && bytes[i] == b'='
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

/// Detect a `pattern name(...) { ... }` block at position `i`.
///
/// If confirmed, copies the entire block (including the closing `}`) verbatim
/// into `out`, advances `*pos` past the block, and returns `true`.
/// If the text at `*pos` doesn't look like a pattern declaration, returns
/// `false` without modifying `*pos` or `out`.
fn try_skip_pattern_block(source: &str, pos: &mut usize, out: &mut String) -> bool {
    let bytes = source.as_bytes();
    let len = bytes.len();
    let start = *pos;

    // Confirmed: this is a pattern block. Now copy everything verbatim
    // until we find the opening '{' of the body, then copy the balanced body.
    let Some(mut k) = pattern_header_end(source, start) else {
        return false;
    };

    // Skip whitespace to find '{'
    while k < len && bytes[k].is_ascii_whitespace() {
        k += 1;
    }
    if k >= len || bytes[k] != b'{' {
        return false;
    }
    k += 1; // past '{'

    // Now copy verbatim while tracking brace depth inside the body.
    let mut brace_depth = 1;
    while k < len && brace_depth > 0 {
        match bytes[k] {
            b'{' => brace_depth += 1,
            b'}' => brace_depth -= 1,
            b'"' => {
                // Skip string literal inside body.
                k += 1;
                while k < len && bytes[k] != b'"' {
                    k += 1;
                }
                // k now points at closing '"' or end; the loop increment below handles it.
            }
            b'/' if k + 1 < len && bytes[k + 1] == b'/' => {
                // Skip // comment inside body.
                while k < len && bytes[k] != b'\n' {
                    k += 1;
                }
                // k now points at '\n' or end; the loop increment below handles it.
                continue; // don't double-advance
            }
            _ => {}
        }
        k += 1;
    }

    // Copy the entire block verbatim [start..k).
    out.push_str(&source[start..k]);
    *pos = k;
    true
}

/// 校验 `pattern name(` 头部并跳过参数括号，返回 `)` 之后的位置。
fn pattern_header_end(source: &str, start: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let len = bytes.len();

    // Must start with "pattern" keyword and be at a word boundary.
    let kw = b"pattern";
    if bytes.get(start..start + kw.len()) != Some(kw) {
        return None;
    }
    // Ensure it's not in the middle of / a prefix of a longer identifier.
    let after_kw = start + kw.len();
    if (start > 0 && is_ident_cont(bytes[start - 1]))
        || (after_kw < len && is_ident_cont(bytes[after_kw]))
    {
        return None;
    }

    // Look ahead: skip whitespace, expect ident, skip whitespace, expect '('
    let mut j = after_kw;
    // skip whitespace
    while j < len && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    // expect ident
    if j >= len || !is_ident_start(bytes[j]) {
        return None;
    }
    while j < len && is_ident_cont(bytes[j]) {
        j += 1;
    }
    // skip whitespace
    while j < len && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    // expect '('
    if j >= len || bytes[j] != b'(' {
        return None;
    }

    // Skip past the '(' params ')' section.
    let mut k = j + 1; // past '('
    let mut paren_depth = 1;
    while k < len && paren_depth > 0 {
        match bytes[k] {
            b'(' => paren_depth += 1,
            b')' => paren_depth -= 1,
            _ => {}
        }
        k += 1;
    }
    Some(k)
}

#[cfg(test)]
mod helper_tests {
    use super::*;

    #[test]
    fn kw_boundary_respects_ident_edges() {
        // 词边界命中
        assert_eq!(kw_boundary(b"yield preset", 0, b"yield"), Some(5));
        assert_eq!(kw_boundary(b"  yield", 2, b"yield"), Some(7));
        // 前字符是标识符继续符 → 不命中（在单词中间）
        assert_eq!(kw_boundary(b"xyield", 1, b"yield"), None);
        // 后字符是标识符继续符 → 不命中（长标识符前缀）
        assert_eq!(kw_boundary(b"yielder", 0, b"yield"), None);
        assert_eq!(kw_boundary(b"yield_preset", 0, b"yield"), None);
        // 越界 / 不匹配
        assert_eq!(kw_boundary(b"yie", 0, b"yield"), None);
        assert_eq!(kw_boundary(b"xxx", 0, b"yield"), None);
    }

    #[test]
    fn resolve_variable_priority_and_errors() {
        let vars = HashMap::from([("A".to_string(), "1".to_string())]);
        // vars 优先于 default
        assert_eq!(
            resolve_variable("A", Some("d"), &vars, false, 7).unwrap(),
            "1"
        );
        // 未定义 → default
        assert_eq!(
            resolve_variable("B", Some("fallback"), &vars, false, 7).unwrap(),
            "fallback"
        );
        // 未定义且无 default → 错误（携带位置）；env_fallback 时文案不同
        let err = resolve_variable("B", None, &vars, false, 42).unwrap_err();
        assert_eq!(err.position, 42);
        assert!(err.message.contains("undefined variable 'B'"));
        assert!(!err.message.contains("environment"));
        let err_env = resolve_variable("B", None, &vars, true, 1).unwrap_err();
        assert!(err_env.message.contains("not in --var or environment"));
    }
}
