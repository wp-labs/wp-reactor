//! 规则检查错误诊断/源码定位组（2026-09-03 自 compile.rs 拆出）：
//! `format_rule_check_error` 家族把 `CheckError` 格式化为带源码行/列与 `yield`
//! 预置定位的可读诊断（backtick/文本扫描工具同属）。纯文本无状态。

use super::ParsedRuleFile;


pub(super) fn format_rule_check_error(
    error: &wf_lang::CheckError,
    parsed: &ParsedRuleFile,
    prelude: Option<&ParsedRuleFile>,
) -> String {
    if let Some(prelude) = prelude
        && let Some(diagnostic) = format_prelude_yield_preset_error(error, parsed, prelude)
    {
        return diagnostic;
    }
    wf_lang::diagnostics::format_check_error_with_source(
        error,
        &parsed.file,
        &parsed.source,
        &parsed.path,
    )
}

pub(super) fn format_prelude_yield_preset_error(
    error: &wf_lang::CheckError,
    parsed: &ParsedRuleFile,
    prelude: &ParsedRuleFile,
) -> Option<String> {
    let rule_name = error.rule.as_deref()?;
    let rule = parsed
        .file
        .rules
        .iter()
        .find(|rule| rule.name == rule_name)?;

    let (line, column) = if let Some(arg_name) =
        extract_backtick_token_after(&error.message, "argument")
    {
        if rule
            .yield_clause
            .args
            .iter()
            .any(|arg| arg.name == arg_name)
        {
            return None;
        }
        let preset_ref = rule.yield_clause.presets.iter().rev().find(|preset_ref| {
            prelude.file.yield_presets.iter().any(|preset| {
                preset.name == preset_ref.name && preset.args.iter().any(|arg| arg.name == arg_name)
            })
        })?;
        find_prelude_yield_preset_arg_location(&prelude.source, &preset_ref.name, &arg_name)?
    } else {
        let tokens = backtick_tokens(&error.message);
        if tokens.is_empty() {
            return None;
        }
        find_referenced_prelude_yield_preset_token_location(
            &prelude.source,
            rule,
            &prelude.file.yield_presets,
            &tokens,
        )?
    };
    let mut out = format!(
        "file: {}\ncategory: yield\n{}\nrule: {}\nlocation: line {}, column {}",
        prelude.path.display(),
        error,
        rule_name,
        line,
        column
    );
    let snippet = source_line_snippet(&prelude.source, line, column);
    if !snippet.is_empty() {
        out.push('\n');
        out.push_str(&snippet);
    }
    Some(out)
}

pub(super) fn backtick_tokens(message: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut rest = message;
    while let Some(start) = rest.find('`') {
        let after_start = &rest[start + 1..];
        let Some(end) = after_start.find('`') else {
            break;
        };
        let token = &after_start[..end];
        if !token.is_empty() {
            tokens.push(token.to_string());
        }
        rest = &after_start[end + 1..];
    }
    tokens
}

pub(super) fn extract_backtick_token_after(message: &str, label: &str) -> Option<String> {
    let idx = message.find(label)?;
    let after_label = &message[idx + label.len()..];
    let start = after_label.find('`')?;
    let after_start = &after_label[start + 1..];
    let end = after_start.find('`')?;
    Some(after_start[..end].to_string())
}

pub(super) fn find_prelude_yield_preset_arg_location(
    source: &str,
    preset_name: &str,
    arg_name: &str,
) -> Option<(usize, usize)> {
    let lines: Vec<&str> = source.lines().collect();
    let decls = yield_preset_decl_locations(source);
    let start_idx = decls
        .iter()
        .find(|decl| decl.name == preset_name)
        .map(|decl| decl.line.saturating_sub(1))
        .or_else(|| {
            lines
                .iter()
                .position(|line| line_declares_yield_preset(line, preset_name))
        })?;
    let end_idx = yield_preset_source_end(&lines, &decls, start_idx);
    lines
        .iter()
        .enumerate()
        .take(end_idx)
        .skip(start_idx)
        .find_map(|(idx, line)| {
            find_named_arg_column(line, arg_name).map(|column| (idx + 1, column))
        })
}

pub(super) fn find_referenced_prelude_yield_preset_token_location(
    source: &str,
    rule: &wf_lang::ast::RuleDecl,
    prelude_presets: &[wf_lang::ast::YieldPresetDecl],
    tokens: &[String],
) -> Option<(usize, usize)> {
    for preset_ref in rule.yield_clause.presets.iter().rev() {
        if !prelude_presets
            .iter()
            .any(|preset| preset.name == preset_ref.name)
        {
            continue;
        }
        if let Some(location) =
            find_prelude_yield_preset_token_location(source, &preset_ref.name, tokens)
        {
            return Some(location);
        }
    }
    None
}

pub(super) fn find_prelude_yield_preset_token_location(
    source: &str,
    preset_name: &str,
    tokens: &[String],
) -> Option<(usize, usize)> {
    let lines: Vec<&str> = source.lines().collect();
    let decls = yield_preset_decl_locations(source);
    let start_idx = decls
        .iter()
        .find(|decl| decl.name == preset_name)
        .map(|decl| decl.line.saturating_sub(1))
        .or_else(|| {
            lines
                .iter()
                .position(|line| line_declares_yield_preset(line, preset_name))
        })?;
    let end_idx = yield_preset_source_end(&lines, &decls, start_idx);
    for (idx, line) in lines.iter().enumerate().take(end_idx).skip(start_idx) {
        if let Some(column) = tokens
            .iter()
            .find_map(|token| find_token_column(line, token))
        {
            return Some((idx + 1, column));
        }
    }
    None
}

pub(super) fn yield_preset_source_end(
    lines: &[&str],
    decls: &[YieldPresetDeclLocation],
    start_idx: usize,
) -> usize {
    lines
        .iter()
        .enumerate()
        .skip(start_idx + 1)
        .find_map(|(idx, line)| {
            let trimmed = line.trim_start();
            (decls.iter().any(|decl| decl.line == idx + 1)
                || line_starts_yield_preset_decl(line)
                || trimmed.starts_with("rule ")
                || trimmed.starts_with("test ")
                || trimmed.starts_with("pattern "))
            .then_some(idx)
        })
        .unwrap_or(lines.len())
}

pub(super) fn find_yield_preset_decl_location(source: &str, preset_name: &str) -> Option<(usize, usize)> {
    find_nth_yield_preset_decl_location(source, preset_name, 1)
}

pub(super) fn find_nth_yield_preset_decl_location(
    source: &str,
    preset_name: &str,
    occurrence: usize,
) -> Option<(usize, usize)> {
    if occurrence == 0 {
        return None;
    }
    yield_preset_decl_locations(source)
        .into_iter()
        .filter(|decl| decl.name == preset_name)
        .map(|decl| (decl.line, decl.column))
        .nth(occurrence - 1)
}

pub(super) fn line_declares_yield_preset(line: &str, preset_name: &str) -> bool {
    let Some(rest) = yield_preset_decl_rest(line) else {
        return false;
    };
    let rest = rest.trim_start();
    let Some(after_name) = rest.strip_prefix(preset_name) else {
        return false;
    };
    !after_name.chars().next().is_some_and(is_ident_char)
}

pub(super) fn line_starts_yield_preset_decl(line: &str) -> bool {
    yield_preset_decl_rest(line)
        .map(str::trim_start)
        .is_some_and(|rest| rest.starts_with(|ch: char| ch.is_ascii_alphabetic() || ch == '_'))
}

pub(super) fn yield_preset_decl_rest(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    let rest = trimmed.strip_prefix("yield")?;
    if !rest.chars().next().is_some_and(char::is_whitespace) {
        return None;
    }
    let rest = rest.trim_start();
    let rest = rest.strip_prefix("preset")?;
    if !rest.chars().next().is_some_and(char::is_whitespace) {
        return None;
    }
    Some(rest)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct YieldPresetDeclLocation {
    pub(super) name: String,
    pub(super) line: usize,
    pub(super) column: usize,
}

pub(super) fn yield_preset_decl_locations(source: &str) -> Vec<YieldPresetDeclLocation> {
    let bytes = source.as_bytes();
    let mut locations = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => i = skip_quoted_string(source, i),
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'/' => {
                i = skip_line_comment(bytes, i);
            }
            b'y' => {
                if let Some((name, _after_name)) = parse_yield_preset_decl_at(source, i) {
                    let (line, column) = source_line_column(source, i);
                    locations.push(YieldPresetDeclLocation { name, line, column });
                    i += "yield".len();
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }
    locations
}

pub(super) fn parse_yield_preset_decl_at(source: &str, start: usize) -> Option<(String, usize)> {
    let bytes = source.as_bytes();
    let after_yield = keyword_at(bytes, start, b"yield")?;
    let mut i = skip_ws_and_line_comments(bytes, after_yield);
    i = keyword_at(bytes, i, b"preset")?;
    i = skip_ws_and_line_comments(bytes, i);
    if i >= bytes.len() || !is_ident_start_byte(bytes[i]) {
        return None;
    }
    let name_start = i;
    i += 1;
    while i < bytes.len() && is_ident_byte(bytes[i]) {
        i += 1;
    }
    Some((source[name_start..i].to_string(), i))
}

pub(super) fn keyword_at(bytes: &[u8], start: usize, keyword: &[u8]) -> Option<usize> {
    let end = start.checked_add(keyword.len())?;
    if end > bytes.len() || &bytes[start..end] != keyword {
        return None;
    }
    if (start > 0 && is_ident_byte(bytes[start - 1]))
        || (end < bytes.len() && is_ident_byte(bytes[end]))
    {
        return None;
    }
    Some(end)
}

pub(super) fn skip_ws_and_line_comments(bytes: &[u8], mut i: usize) -> usize {
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

pub(super) fn skip_quoted_string(source: &str, start: usize) -> usize {
    let bytes = source.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() && bytes[i] != b'"' {
        i += source[i..].chars().next().unwrap().len_utf8();
    }
    if i < bytes.len() { i + 1 } else { i }
}

pub(super) fn skip_line_comment(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    i
}

pub(super) fn source_line_column(source: &str, offset: usize) -> (usize, usize) {
    let mut line = 1;
    let mut line_start = 0;
    for (idx, byte) in source.bytes().enumerate().take(offset) {
        if byte == b'\n' {
            line += 1;
            line_start = idx + 1;
        }
    }
    (line, offset - line_start + 1)
}

pub(super) fn is_ident_start_byte(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

pub(super) fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

pub(super) fn find_named_arg_column(line: &str, arg_name: &str) -> Option<usize> {
    find_named_token_column(line, arg_name, true)
}

pub(super) fn find_token_column(line: &str, token: &str) -> Option<usize> {
    find_named_token_column(line, token, false)
}

pub(super) fn find_named_token_column(line: &str, token: &str, require_assignment: bool) -> Option<usize> {
    let mut search_from = 0;
    while let Some(relative_idx) = line[search_from..].find(token) {
        let idx = search_from + relative_idx;
        let before_ok = line[..idx]
            .chars()
            .next_back()
            .is_none_or(|ch| !is_ident_char(ch));
        let after_name_idx = idx + token.len();
        let after_name = &line[after_name_idx..];
        let after_ok = after_name
            .chars()
            .next()
            .is_none_or(|ch| !is_ident_char(ch));
        let has_equals = !require_assignment || after_name.trim_start().starts_with('=');
        if before_ok && after_ok && has_equals {
            return Some(idx + 1);
        }
        search_from = after_name_idx;
    }
    None
}

pub(super) fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

pub(super) fn source_line_snippet(source: &str, line: usize, column: usize) -> String {
    let Some(text) = source.lines().nth(line.saturating_sub(1)) else {
        return String::new();
    };
    format!("  {}\n  {}^", text, " ".repeat(column.saturating_sub(1)))
}
