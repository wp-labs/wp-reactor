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
    let (line, column) = prelude_error_location(&error.message, rule, prelude)?;
    Some(render_prelude_diagnostic(
        prelude, error, rule_name, line, column,
    ))
}

/// 把错误定位到 prelude `yield preset` 声明体内部：
/// - 消息带 `argument `name`` → 去 prelude 里命中该参数（规则自身已给出则与 prelude 无关）；
/// - 否则按消息中的 backtick token 在 prelude 声明体中定位。
fn prelude_error_location(
    message: &str,
    rule: &wf_lang::ast::RuleDecl,
    prelude: &ParsedRuleFile,
) -> Option<(usize, usize)> {
    if let Some(arg_name) = extract_backtick_token_after(message, "argument") {
        prelude_arg_location(rule, prelude, &arg_name)
    } else {
        let tokens = backtick_tokens(message);
        if tokens.is_empty() {
            return None;
        }
        find_referenced_prelude_yield_preset_token_location(
            &prelude.source,
            rule,
            &prelude.file.yield_presets,
            &tokens,
        )
    }
}

/// 在规则引用的 prelude preset 声明中查找 `arg_name` 出现位置；规则自身
/// yield 参数已含该名时视为非 prelude 错误返回 `None`。
fn prelude_arg_location(
    rule: &wf_lang::ast::RuleDecl,
    prelude: &ParsedRuleFile,
    arg_name: &str,
) -> Option<(usize, usize)> {
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
    find_prelude_yield_preset_arg_location(&prelude.source, &preset_ref.name, arg_name)
}

/// 组装指向 prelude 文件的诊断文本（含源码行与脱字符片段）。
fn render_prelude_diagnostic(
    prelude: &ParsedRuleFile,
    error: &wf_lang::CheckError,
    rule_name: &str,
    line: usize,
    column: usize,
) -> String {
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
    out
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

/// 计算 preset 声明所在行（0-based `start`）与声明体结束行（0-based、
/// 不含边界行）的扫描区间；找不到声明则 `None`。
fn preset_decl_scan_range(
    source: &str,
    preset_name: &str,
    lines: &[&str],
) -> Option<(usize, usize)> {
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
    let end_idx = yield_preset_source_end(lines, &decls, start_idx);
    Some((start_idx, end_idx))
}

pub(super) fn find_prelude_yield_preset_arg_location(
    source: &str,
    preset_name: &str,
    arg_name: &str,
) -> Option<(usize, usize)> {
    let lines: Vec<&str> = source.lines().collect();
    let (start_idx, end_idx) = preset_decl_scan_range(source, preset_name, &lines)?;
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
    let (start_idx, end_idx) = preset_decl_scan_range(source, preset_name, &lines)?;
    lines
        .iter()
        .enumerate()
        .take(end_idx)
        .skip(start_idx)
        .find_map(|(idx, line)| {
            tokens
                .iter()
                .find_map(|token| find_token_column(line, token))
                .map(|column| (idx + 1, column))
        })
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

pub(super) fn find_yield_preset_decl_location(
    source: &str,
    preset_name: &str,
) -> Option<(usize, usize)> {
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

pub(super) fn find_named_token_column(
    line: &str,
    token: &str,
    require_assignment: bool,
) -> Option<usize> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use wf_lang::{CheckError, Severity};

    fn parsed_file(path: &str, source: &str) -> ParsedRuleFile {
        ParsedRuleFile {
            path: PathBuf::from(path),
            source: source.to_string(),
            file: wf_lang::parse_wfl(source).expect("wfl source should parse"),
        }
    }

    fn rule_error(rule: &str, message: &str) -> CheckError {
        CheckError {
            severity: Severity::Error,
            rule: Some(rule.to_string()),
            test: None,
            message: message.to_string(),
        }
    }

    const PRELUDE: &str =
        "yield preset base_alerts (\n    y = \"base\",\n    n = 1,\n    port = 80\n)\n";
    const RULE: &str = "rule preset_rule {\n    events { e : auth_events }\n    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)\n    entity(ip, e.sip)\n    yield out : base_alerts (port = e.dport)\n}\n";

    #[test]
    fn prelude_diag_locates_missing_preset_argument() {
        let prelude = parsed_file("_global.wfl", PRELUDE);
        let rule = parsed_file("rules.wfl", RULE);
        // 规则自身给了 port，缺的是 base_alerts 的必填参数 y → 定位到 prelude 声明
        let error = rule_error(
            "preset_rule",
            "yield preset `base_alerts` missing required argument `y`",
        );
        let diag = format_rule_check_error(&error, &rule, Some(&prelude));
        assert!(diag.contains("file: _global.wfl"), "{diag}");
        assert!(diag.contains("category: yield"), "{diag}");
        assert!(diag.contains("rule: preset_rule"), "{diag}");
        assert!(diag.contains("location: line 2, column 5"), "{diag}");
        assert!(diag.contains("    y = \"base\","), "{diag}");
    }

    #[test]
    fn prelude_diag_locates_backtick_token_in_preset_body() {
        let prelude = parsed_file("_global.wfl", PRELUDE);
        let rule = parsed_file("rules.wfl", RULE);
        // 无 `argument` 标记：按消息 backtick token 在 prelude 声明体中定位
        let error = rule_error(
            "preset_rule",
            "conflicting field `port` for yield preset usage",
        );
        let diag = format_rule_check_error(&error, &rule, Some(&prelude));
        assert!(diag.contains("category: yield"), "{diag}");
        assert!(diag.contains("location: line 4, column 5"), "{diag}");
        assert!(diag.contains("    port = 80"), "{diag}");
    }

    #[test]
    fn prelude_diag_falls_back_when_rule_supplies_the_argument() {
        let prelude = parsed_file("_global.wfl", PRELUDE);
        let rule = parsed_file("rules.wfl", RULE);
        // port 已在规则自身 yield 参数中给出 → 不是 prelude 引用错误，走常规格式
        let error = rule_error(
            "preset_rule",
            "yield preset `base_alerts` missing required argument `port`",
        );
        let diag = format_rule_check_error(&error, &rule, Some(&prelude));
        // port 已在规则自身 yield 参数中给出 → 不是 prelude 引用错误，走常规格式（指回 rules.wfl）
        assert!(!diag.contains("file: _global.wfl"), "{diag}");
        assert!(diag.contains("missing required argument `port`"), "{diag}");
    }

    #[test]
    fn format_without_prelude_uses_plain_check_format() {
        let rule = parsed_file("rules.wfl", RULE);
        let error = rule_error(
            "preset_rule",
            "yield preset `base_alerts` missing required argument `y`",
        );
        let diag = format_rule_check_error(&error, &rule, None);
        // 无 prelude：一律走常规格式并指回规则文件自身
        assert!(!diag.contains("file: _global.wfl"), "{diag}");
        assert!(diag.contains("file: rules.wfl"), "{diag}");
        assert!(diag.contains("preset_rule"), "{diag}");
    }

    #[test]
    fn preset_decl_locations_skip_comments_and_quoted_text() {
        let src = "// yield preset hidden (\nx = \"yield preset quoted (nope)\"\nyield preset target (\n    a = 1\n)\nyield preset target (\n    b = 2\n)\n";
        let locs = yield_preset_decl_locations(src);
        assert_eq!(locs.len(), 2, "comments/quoted text must not count");
        assert_eq!(locs[0].name, "target");
        assert_eq!((locs[0].line, locs[0].column), (3, 1));
        assert_eq!((locs[1].line, locs[1].column), (6, 1));
        assert_eq!(find_yield_preset_decl_location(src, "target"), Some((3, 1)));
        assert_eq!(
            find_nth_yield_preset_decl_location(src, "target", 2),
            Some((6, 1))
        );
        assert_eq!(find_nth_yield_preset_decl_location(src, "target", 0), None);
        assert_eq!(find_nth_yield_preset_decl_location(src, "target", 3), None);
    }

    #[test]
    fn keyword_scan_respects_ident_boundaries() {
        assert_eq!(keyword_at(b"yield preset alpha (", 0, b"yield"), Some(5));
        assert_eq!(keyword_at(b"yielder ", 0, b"yield"), None);
        assert_eq!(keyword_at(b"myield ", 1, b"yield"), None);
        assert_eq!(keyword_at(b"a yield preset", 2, b"yield"), Some(7));
        assert_eq!(keyword_at(b"yie", 0, b"yield"), None);
    }

    #[test]
    fn parse_preset_decl_and_skip_helpers() {
        // 名称可含下划线，关键字之间允许空白与 // 行注释
        let (name, after) = parse_yield_preset_decl_at("yield preset alpha_x (\n", 0).unwrap();
        assert_eq!(name, "alpha_x");
        assert_eq!(&"yield preset alpha_x (\n"[after..after + 1], " ");
        let with_comment = "yield // c\n preset beta (\n";
        assert_eq!(
            parse_yield_preset_decl_at(with_comment, 0).map(|(n, _)| n),
            Some("beta".to_string())
        );
        let skip = "  // comment\n  rest";
        let rest = skip_ws_and_line_comments(skip.as_bytes(), 0);
        assert_eq!(&skip[rest..], "rest");
        assert!(line_declares_yield_preset("yield preset aaa (", "aaa"));
        assert!(!line_declares_yield_preset("yield preset aaa2 (", "aaa"));
        assert!(!line_declares_yield_preset("yield preset aa (", "aaa"));
        assert!(!line_declares_yield_preset("rule x {", "x"));
    }

    #[test]
    fn named_token_columns_require_boundaries_and_assignment() {
        assert_eq!(find_named_arg_column("y = 1, yy = 2", "y"), Some(1));
        assert_eq!(find_named_arg_column("y = 1, yy = 2", "yy"), Some(8));
        assert_eq!(find_named_arg_column("y = 1, yyy = 3", "yy"), None);
        assert_eq!(find_token_column("yield preset x (", "preset"), Some(7));
        // 未闭合/空的 backtick 不产生 token
        assert_eq!(
            backtick_tokens("a `x` b `y`"),
            vec!["x".to_string(), "y".to_string()]
        );
        assert_eq!(backtick_tokens("unclosed `x"), Vec::<String>::new());
        assert_eq!(backtick_tokens("empty `` skips"), Vec::<String>::new());
        assert_eq!(
            extract_backtick_token_after("missing required argument `port`", "argument"),
            Some("port".to_string())
        );
        assert_eq!(
            extract_backtick_token_after("no backtick after argument", "argument"),
            None
        );
    }

    #[test]
    fn source_snippet_caret_and_scan_ranges() {
        assert_eq!(
            source_line_snippet("a\nhello world\nc", 2, 7),
            "  hello world\n        ^"
        );
        assert_eq!(source_line_snippet("a\nhello world\nc", 5, 1), "");
        // 直接在 prelude 文本上验证声明体扫描区间
        assert_eq!(
            find_prelude_yield_preset_arg_location(PRELUDE, "base_alerts", "port"),
            Some((4, 5))
        );
        assert_eq!(
            find_prelude_yield_preset_token_location(PRELUDE, "base_alerts", &["port".to_string()]),
            Some((4, 5))
        );
        assert_eq!(
            find_prelude_yield_preset_token_location(
                PRELUDE,
                "base_alerts",
                &["absent".to_string()]
            ),
            None
        );
    }
}
