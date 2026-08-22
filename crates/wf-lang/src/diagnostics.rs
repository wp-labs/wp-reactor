use std::path::Path;

use winnow::error::{ContextError, ParseError};

use crate::ast::WflFile;
use crate::checker::{CheckError, Severity, check_wfl};
use crate::plan::RulePlan;
use crate::schema::WindowSchema;
use crate::{LangReason, LangResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceLocation {
    line: usize,
    column: usize,
}

/// Return a 1-based line/column location for a byte offset in `source`.
pub fn translate_position(source: &str, offset: usize) -> (usize, usize) {
    let capped = offset.min(source.len());
    let mut line = 1usize;
    let mut column = 1usize;
    for (idx, ch) in source.char_indices() {
        if idx >= capped {
            break;
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    (line, column)
}

pub(crate) fn parse_error_detail(source: &str, err: &ParseError<&str, ContextError>) -> String {
    let (line, column) = translate_position(source, err.offset());
    let mut out = format!("category: syntax\nparse error at line {line}, column {column}");
    let snippet = source_snippet(source, line, column);
    if !snippet.is_empty() {
        out.push('\n');
        out.push_str(&snippet);
    }
    let context = err.inner().to_string();
    if !context.trim().is_empty() {
        out.push('\n');
        out.push_str(context.trim());
    }
    out
}

/// Parse WFL and return a source-aware diagnostic on failure.
pub fn parse_wfl_with_diagnostics(source: &str, path: impl AsRef<Path>) -> LangResult<WflFile> {
    crate::parse_wfl(source).map_err(|err| {
        crate::error::error(
            LangReason::Parse,
            format!(
                "file: {}\n{}",
                path.as_ref().display(),
                err.detail().clone().unwrap_or_else(|| err.to_string())
            ),
        )
    })
}

/// Run semantic checks and return source-aware diagnostics on failure.
pub(crate) fn check_wfl_diagnostics(
    file: &WflFile,
    schemas: &[WindowSchema],
    source: &str,
    path: impl AsRef<Path>,
) -> Vec<String> {
    check_wfl_errors(file, schemas)
        .into_iter()
        .map(|error| format_check_error_with_source(&error, file, source, path.as_ref()))
        .collect()
}

pub(crate) fn check_wfl_errors(file: &WflFile, schemas: &[WindowSchema]) -> Vec<CheckError> {
    check_wfl(file, schemas)
        .into_iter()
        .filter(|error| error.severity == Severity::Error)
        .collect()
}

/// Validate a parsed WFL file and return source-aware diagnostics on failure.
pub fn validate_wfl_with_diagnostics(
    file: &WflFile,
    schemas: &[WindowSchema],
    source: &str,
    path: impl AsRef<Path>,
) -> LangResult<()> {
    let diagnostics = check_wfl_diagnostics(file, schemas, source, path);
    if diagnostics.is_empty() {
        Ok(())
    } else {
        crate::error::fail(
            LangReason::Compile,
            format!("semantic errors:\n{}", diagnostics.join("\n\n")),
        )
    }
}

/// Compile WFL after one semantic check pass, returning source-aware diagnostics on failure.
pub fn compile_wfl_with_diagnostics(
    file: &WflFile,
    schemas: &[WindowSchema],
    source: &str,
    path: impl AsRef<Path>,
) -> LangResult<Vec<RulePlan>> {
    let errors = check_wfl_errors(file, schemas);
    if !errors.is_empty() {
        let diagnostics: Vec<String> = errors
            .iter()
            .map(|error| format_check_error_with_source(error, file, source, path.as_ref()))
            .collect();
        return crate::error::fail(
            LangReason::Compile,
            format!("semantic errors:\n{}", diagnostics.join("\n\n")),
        );
    }
    crate::compiler::compile_wfl_after_semantic_checks(file, schemas).map_err(|error| {
        crate::error::error(
            LangReason::Compile,
            format!(
                "file: {}\ncategory: compile\n{}",
                path.as_ref().display(),
                error.detail().clone().unwrap_or_else(|| error.to_string())
            ),
        )
    })
}

pub fn format_check_error_with_source(
    error: &CheckError,
    file: &WflFile,
    source: &str,
    path: &Path,
) -> String {
    let category = classify_check_error(error);
    let mut out = format!(
        "file: {}\ncategory: {}\n{}",
        path.display(),
        category,
        error
    );

    if let Some(rule_name) = &error.rule {
        out.push('\n');
        out.push_str(&format!("rule: {rule_name}"));
        if let Some(location) = find_rule_error_location(file, source, rule_name, error, category) {
            out.push('\n');
            out.push_str(&format!(
                "location: line {}, column {}",
                location.line, location.column
            ));
            let snippet = source_snippet(source, location.line, location.column);
            if !snippet.is_empty() {
                out.push('\n');
                out.push_str(&snippet);
            }
        } else if file.rules.iter().any(|rule| rule.name == *rule_name) {
            out.push('\n');
            out.push_str("location: rule declaration not found in source");
        }
    } else if let Some(test_name) = &error.test {
        out.push('\n');
        out.push_str(&format!("test: {test_name}"));
        if let Some(location) = find_named_block_location(source, "test", test_name) {
            out.push('\n');
            out.push_str(&format!(
                "location: line {}, column {}",
                location.line, location.column
            ));
            let snippet = source_snippet(source, location.line, location.column);
            if !snippet.is_empty() {
                out.push('\n');
                out.push_str(&snippet);
            }
        }
    }

    out
}

fn classify_check_error(error: &CheckError) -> &'static str {
    let msg = error.message.as_str();
    if error.test.is_some() {
        "test"
    } else if msg.contains("pipeline")
        || msg.contains("intermediate")
        || msg.contains("topology")
        || msg.contains("acyclic")
        || msg.contains("cycle")
        || msg.contains("downstream")
    {
        "topology"
    } else if msg.contains("yield") {
        "yield"
    } else if msg.contains("sink") {
        "sink"
    } else if msg.contains("type")
        || msg.contains("operand")
        || msg.contains("numeric")
        || msg.contains("compatible")
        || msg.contains("unknown field")
        || msg.contains("unknown alias")
        || (msg.contains("field `") && msg.contains("not found"))
        || msg.contains("not found in window")
        || msg.contains("references unknown alias")
        || msg.contains("not a declared event alias")
        || msg.contains("set-level alias")
        || msg.contains("requires a field projection")
    {
        "type"
    } else {
        "rule"
    }
}

fn find_rule_location(source: &str, rule_name: &str) -> Option<SourceLocation> {
    find_named_block_location(source, "rule", rule_name)
}

fn find_rule_error_location(
    file: &WflFile,
    source: &str,
    rule_name: &str,
    error: &CheckError,
    category: &str,
) -> Option<SourceLocation> {
    let lines: Vec<&str> = source.lines().collect();
    let (start_idx, end_idx, rule_location) = find_rule_source_range(source, rule_name, &lines)?;

    let token_needles = primary_backtick_tokens(&error.message, category);
    if category == "yield" {
        if let Some(argument) = extract_token_after_label(&error.message, "argument") {
            if rule_has_explicit_yield_arg(file, rule_name, &argument) {
                if let Some(yield_location) =
                    find_keyword_location(&lines, start_idx, end_idx, "yield")
                    && let Some(location) =
                        find_named_arg_location_after(&lines, yield_location, end_idx, &argument)
                {
                    return Some(location);
                }
            } else if let Some(location) = find_referenced_yield_preset_location(
                file,
                source,
                rule_name,
                std::slice::from_ref(&argument),
            ) {
                return Some(location);
            }
        } else {
            if let Some(yield_location) = find_keyword_location(&lines, start_idx, end_idx, "yield")
                && let Some(location) =
                    find_token_location_after(&lines, yield_location, end_idx, &token_needles)
            {
                return Some(location);
            }
            if let Some(location) =
                find_referenced_yield_preset_location(file, source, rule_name, &token_needles)
            {
                return Some(location);
            }
        }
    }

    if let Some(location) = find_token_location(&lines, start_idx + 1, end_idx, &token_needles) {
        return Some(location);
    }

    let needles = heuristic_location_needles(error, category);
    for (idx, line) in lines.iter().enumerate().take(end_idx).skip(start_idx) {
        let trimmed = line.trim_start();
        if let Some(needle) = needles
            .iter()
            .find(|needle| (idx != start_idx || **needle == "rule") && trimmed.contains(**needle))
        {
            let column = line.find(*needle).unwrap_or(0) + 1;
            return Some(SourceLocation {
                line: idx + 1,
                column,
            });
        }
    }

    Some(rule_location)
}

fn find_token_location(
    lines: &[&str],
    start_idx: usize,
    end_idx: usize,
    needles: &[String],
) -> Option<SourceLocation> {
    for needle in needles.iter().filter(|needle| !needle.is_empty()) {
        for (idx, line) in lines.iter().enumerate().take(end_idx).skip(start_idx) {
            if line.contains(needle.as_str()) {
                let column = line.find(needle.as_str()).unwrap_or(0) + 1;
                return Some(SourceLocation {
                    line: idx + 1,
                    column,
                });
            }
        }
    }
    None
}

fn find_named_arg_location_after(
    lines: &[&str],
    start: SourceLocation,
    end_idx: usize,
    arg_name: &str,
) -> Option<SourceLocation> {
    for (idx, line) in lines
        .iter()
        .enumerate()
        .take(end_idx)
        .skip(start.line.saturating_sub(1))
    {
        let search_start = if idx + 1 == start.line {
            start.column.saturating_sub(1).min(line.len())
        } else {
            0
        };
        if let Some(column) = find_named_arg_column(&line[search_start..], arg_name) {
            return Some(SourceLocation {
                line: idx + 1,
                column: search_start + column,
            });
        }
    }
    None
}

fn find_named_arg_column(line: &str, arg_name: &str) -> Option<usize> {
    let mut rest = line;
    let mut offset = 0usize;
    while let Some(pos) = rest.find(arg_name) {
        let absolute = offset + pos;
        if is_named_arg_start(line, absolute, arg_name.len())
            && line[absolute + arg_name.len()..]
                .trim_start()
                .starts_with('=')
        {
            return Some(absolute + 1);
        }
        let next = pos + arg_name.len();
        offset += next;
        rest = &rest[next..];
    }
    None
}

fn is_named_arg_start(line: &str, start: usize, len: usize) -> bool {
    is_keyword_boundary(line, start, len)
        && previous_non_ws_char(line, start).is_none_or(|ch| ch == '(' || ch == ',')
}

fn previous_non_ws_char(line: &str, start: usize) -> Option<char> {
    line[..start].chars().rev().find(|ch| !ch.is_whitespace())
}

fn find_keyword_location(
    lines: &[&str],
    start_idx: usize,
    end_idx: usize,
    keyword: &str,
) -> Option<SourceLocation> {
    lines
        .iter()
        .enumerate()
        .take(end_idx)
        .skip(start_idx)
        .find_map(|(idx, line)| {
            find_keyword_column(line, keyword).map(|column| SourceLocation {
                line: idx + 1,
                column,
            })
        })
}

fn find_keyword_column(line: &str, keyword: &str) -> Option<usize> {
    let mut rest = line;
    let mut offset = 0usize;
    while let Some(pos) = rest.find(keyword) {
        let absolute = offset + pos;
        if is_keyword_boundary(line, absolute, keyword.len()) {
            return Some(absolute + 1);
        }
        let next = pos + keyword.len();
        offset += next;
        rest = &rest[next..];
    }
    None
}

fn is_keyword_boundary(line: &str, start: usize, len: usize) -> bool {
    let before = line[..start].chars().next_back();
    let after = line[start + len..].chars().next();
    !before.is_some_and(is_ident_char) && !after.is_some_and(is_ident_char)
}

fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn find_token_location_after(
    lines: &[&str],
    start: SourceLocation,
    end_idx: usize,
    needles: &[String],
) -> Option<SourceLocation> {
    for needle in needles.iter().filter(|needle| !needle.is_empty()) {
        for (idx, line) in lines
            .iter()
            .enumerate()
            .take(end_idx)
            .skip(start.line.saturating_sub(1))
        {
            let search_start = if idx + 1 == start.line {
                start.column.saturating_sub(1).min(line.len())
            } else {
                0
            };
            let slice = &line[search_start..];
            if let Some(pos) = slice.find(needle.as_str()) {
                return Some(SourceLocation {
                    line: idx + 1,
                    column: search_start + pos + 1,
                });
            }
        }
    }
    None
}

fn message_backtick_tokens(message: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut rest = message;
    while let Some(start) = rest.find('`') {
        let after_start = &rest[start + 1..];
        let Some(end) = after_start.find('`') else {
            break;
        };
        tokens.push(after_start[..end].to_string());
        rest = &after_start[end + 1..];
    }
    tokens
}

fn primary_backtick_tokens(message: &str, category: &str) -> Vec<String> {
    // Best-effort location hint until the AST carries spans. Prefer the token
    // that describes the failing object, then fall back to all quoted tokens.
    let tokens = message_backtick_tokens(message);
    if category == "yield"
        && let Some(argument) = extract_token_after_label(message, "argument")
    {
        return vec![argument];
    }
    if let Some(field) = extract_token_after_label(message, "field") {
        return vec![field];
    }
    if let Some(argument) = extract_token_after_label(message, "argument") {
        return vec![argument];
    }
    if let Some(source) = extract_token_after_label(message, "source") {
        return vec![source];
    }
    if let Some(target) = extract_token_after_label(message, "target window") {
        return vec![target];
    }
    if let Some(target) = extract_token_after_label(message, "target") {
        return vec![target];
    }
    tokens
}

fn extract_token_after_label(message: &str, label: &str) -> Option<String> {
    let idx = message.find(label)?;
    let after_label = &message[idx + label.len()..];
    let start = after_label.find('`')?;
    let after_start = &after_label[start + 1..];
    let end = after_start.find('`')?;
    Some(after_start[..end].to_string())
}

fn find_referenced_yield_preset_location(
    file: &WflFile,
    source: &str,
    rule_name: &str,
    token_needles: &[String],
) -> Option<SourceLocation> {
    let rule = file.rules.iter().find(|rule| rule.name == rule_name)?;
    let lines: Vec<&str> = source.lines().collect();
    for preset_ref in rule.yield_clause.presets.iter().rev() {
        if let Some((start_idx, end_idx)) =
            find_yield_preset_source_range(source, &lines, &preset_ref.name)
            && let Some(location) =
                find_named_arg_location(&lines, start_idx, end_idx, token_needles)
        {
            return Some(location);
        }
    }
    None
}

fn find_yield_preset_source_range(
    source: &str,
    lines: &[&str],
    preset_name: &str,
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
    let end_idx = lines
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
        .unwrap_or(lines.len());
    Some((start_idx, end_idx))
}

fn find_named_arg_location(
    lines: &[&str],
    start_idx: usize,
    end_idx: usize,
    arg_names: &[String],
) -> Option<SourceLocation> {
    for arg_name in arg_names.iter().filter(|arg_name| !arg_name.is_empty()) {
        for (idx, line) in lines.iter().enumerate().take(end_idx).skip(start_idx) {
            if let Some(column) = find_named_arg_column(line, arg_name) {
                return Some(SourceLocation {
                    line: idx + 1,
                    column,
                });
            }
        }
    }
    None
}

fn line_declares_yield_preset(line: &str, preset_name: &str) -> bool {
    let Some(rest) = yield_preset_decl_rest(line) else {
        return false;
    };
    let rest = rest.trim_start();
    let Some(after_name) = rest.strip_prefix(preset_name) else {
        return false;
    };
    !after_name.chars().next().is_some_and(is_ident_char)
}

fn line_starts_yield_preset_decl(line: &str) -> bool {
    yield_preset_decl_rest(line)
        .map(str::trim_start)
        .is_some_and(|rest| rest.starts_with(|ch: char| ch.is_ascii_alphabetic() || ch == '_'))
}

fn yield_preset_decl_rest(line: &str) -> Option<&str> {
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
struct YieldPresetDeclLocation {
    name: String,
    line: usize,
}

fn yield_preset_decl_locations(source: &str) -> Vec<YieldPresetDeclLocation> {
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
                    let line = source_line(source, i);
                    locations.push(YieldPresetDeclLocation { name, line });
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

fn parse_yield_preset_decl_at(source: &str, start: usize) -> Option<(String, usize)> {
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

fn keyword_at(bytes: &[u8], start: usize, keyword: &[u8]) -> Option<usize> {
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

fn skip_quoted_string(source: &str, start: usize) -> usize {
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

fn source_line(source: &str, offset: usize) -> usize {
    let mut line = 1;
    for byte in source.bytes().take(offset) {
        if byte == b'\n' {
            line += 1;
        }
    }
    line
}

fn is_ident_start_byte(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn rule_has_explicit_yield_arg(file: &WflFile, rule_name: &str, arg_name: &str) -> bool {
    file.rules
        .iter()
        .find(|rule| rule.name == rule_name)
        .is_some_and(|rule| {
            rule.yield_clause
                .args
                .iter()
                .any(|arg| arg.name == arg_name)
        })
}

fn heuristic_location_needles(error: &CheckError, category: &str) -> Vec<&'static str> {
    let msg = error.message.as_str();
    if msg.contains("not a declared event alias") {
        vec!["match"]
    } else if category == "yield" {
        vec!["yield"]
    } else if msg.contains("score expression") || msg.contains("score") {
        vec!["score"]
    } else if msg.contains("entity") {
        vec!["entity"]
    } else if msg.contains("event alias") || msg.contains("window") {
        vec!["events"]
    } else if msg.contains("key") || msg.contains("step") || msg.contains("threshold") {
        vec!["match"]
    } else {
        vec!["rule"]
    }
}

fn find_named_block_location(source: &str, keyword: &str, name: &str) -> Option<SourceLocation> {
    for (line_idx, line) in source.lines().enumerate() {
        let trimmed = line.trim_start();
        let leading = line.len() - trimmed.len();
        let Some(rest) = trimmed.strip_prefix(keyword) else {
            continue;
        };
        if !rest.starts_with(char::is_whitespace) {
            continue;
        }
        let rest = rest.trim_start();
        let Some(after_name) = rest.strip_prefix(name) else {
            continue;
        };
        if after_name
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            continue;
        }
        let name_column_offset = line[leading..].find(name).unwrap_or(0);
        return Some(SourceLocation {
            line: line_idx + 1,
            column: leading + name_column_offset + 1,
        });
    }
    None
}

fn find_rule_source_range(
    source: &str,
    rule_name: &str,
    lines: &[&str],
) -> Option<(usize, usize, SourceLocation)> {
    let rule_location = find_rule_location(source, rule_name)?;
    let start_idx = rule_location.line.saturating_sub(1);
    let end_idx = lines
        .iter()
        .enumerate()
        .skip(start_idx + 1)
        .find_map(|(idx, line)| {
            let trimmed = line.trim_start();
            (trimmed.starts_with("rule ")
                || trimmed.starts_with("test ")
                || trimmed.starts_with("pattern ")
                || line_starts_yield_preset_decl(line))
            .then_some(idx)
        })
        .unwrap_or(lines.len());
    Some((start_idx, end_idx, rule_location))
}

fn source_snippet(source: &str, line: usize, column: usize) -> String {
    let Some(line_text) = source.lines().nth(line.saturating_sub(1)) else {
        return String::new();
    };
    let gutter = line.to_string();
    let gutter_width = gutter.len();
    let caret_pad = " ".repeat(column.saturating_sub(1));
    format!(
        "{:gutter_width$} |\n{} | {}\n{:gutter_width$} | {}^",
        "",
        gutter,
        line_text.trim_end(),
        "",
        caret_pad
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::compile_wfl;
    use crate::parse_wfl;
    use crate::{BaseType, FieldDef, FieldType, WindowSchema};

    fn window(name: &str, streams: Vec<&str>, fields: Vec<(&str, FieldType)>) -> WindowSchema {
        WindowSchema {
            name: name.to_string(),
            streams: streams.into_iter().map(ToString::to_string).collect(),
            time_field: None,
            over: std::time::Duration::ZERO,
            fields: fields
                .into_iter()
                .map(|(name, field_type)| FieldDef {
                    name: name.to_string(),
                    field_type,
                })
                .collect(),
        }
    }

    fn auth_events_window() -> WindowSchema {
        window(
            "auth_events",
            vec!["auth"],
            vec![
                ("sip", FieldType::Base(BaseType::Ip)),
                ("action", FieldType::Base(BaseType::Chars)),
            ],
        )
    }

    fn output_window() -> WindowSchema {
        window("out", vec![], vec![("x", FieldType::Base(BaseType::Ip))])
    }

    #[test]
    fn parse_diagnostic_includes_line_column_and_snippet() {
        let source = r#"
rule bad {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= ; } }
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        let err = parse_wfl_with_diagnostics(source, "rules/bad.wfl").unwrap_err();
        let text = err.to_string();
        assert!(text.contains("rules/bad.wfl"), "{text}");
        assert!(text.contains("category: syntax"), "{text}");
        assert!(text.contains("line 4, column"), "{text}");
        assert!(text.contains("count >="), "{text}");
        assert!(text.contains("^"), "{text}");
        assert_eq!(text.matches("parse error at line").count(), 1, "{text}");
    }

    #[test]
    fn source_snippet_aligns_multi_digit_line_numbers() {
        let source = [
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "    match<sip:5m> { on event { e | count >= ; } }",
        ]
        .join("\n");

        let snippet = source_snippet(&source, 10, 45);

        assert!(snippet.contains("   |"), "{snippet}");
        assert!(snippet.contains("10 |     match<sip:5m>"), "{snippet}");
        assert!(
            snippet.contains("   |                                             ^"),
            "{snippet}"
        );
    }

    #[test]
    fn compile_diagnostic_includes_rule_category_and_snippet() {
        let source = r#"
rule bad_yield {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield missing_out (x = e.sip)
}
"#;
        let file = parse_wfl(source).unwrap();
        let err = validate_wfl_with_diagnostics(
            &file,
            &[auth_events_window(), output_window()],
            source,
            "rules/bad_yield.wfl",
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("rules/bad_yield.wfl"), "{text}");
        assert!(text.contains("category: yield"), "{text}");
        assert!(text.contains("rule: bad_yield"), "{text}");
        assert!(text.contains("location: line 6, column 11"), "{text}");
        assert!(text.contains("yield missing_out"), "{text}");
    }

    #[test]
    fn yield_diagnostic_prefers_yield_clause_when_token_appears_earlier() {
        let source = r#"
rule bad_yield_arg {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (sip = e.sip)
}
"#;
        let file = parse_wfl(source).unwrap();
        let err = validate_wfl_with_diagnostics(
            &file,
            &[auth_events_window(), output_window()],
            source,
            "rules/bad_yield_arg.wfl",
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("category: yield"), "{text}");
        assert!(text.contains("location: line 6, column 16"), "{text}");
        assert!(text.contains("yield out (sip = e.sip)"), "{text}");
        assert!(text.contains("6 |     yield out (sip = e.sip)"), "{text}");
    }

    #[test]
    fn yield_diagnostic_prefers_inline_yield_clause() {
        let source = r#"
rule inline_bad { events { e : auth_events } match<sip:5m> { on event { e | count >= 1; } } -> score(50.0) entity(ip, e.sip) yield out (sip = e.sip) }
"#;
        let file = parse_wfl(source).unwrap();
        let err = validate_wfl_with_diagnostics(
            &file,
            &[auth_events_window(), output_window()],
            source,
            "rules/inline_bad.wfl",
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("category: yield"), "{text}");
        assert!(text.contains("location: line 2, column 137"), "{text}");
        assert!(
            text.contains(
                "2 | rule inline_bad { events { e : auth_events } match<sip:5m> { on event { e | count >= 1; } } -> score(50.0) entity(ip, e.sip) yield out (sip = e.sip) }"
            ),
            "{text}"
        );
    }

    #[test]
    fn compile_diagnostic_matches_compile_wfl_errors() {
        let source = r#"
rule bad_type {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(e.action)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        let file = parse_wfl(source).unwrap();
        let schemas = [auth_events_window(), output_window()];
        assert!(compile_wfl(&file, &schemas).is_err());
        let err = validate_wfl_with_diagnostics(&file, &schemas, source, "rules/bad_type.wfl")
            .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("category: type"), "{text}");
        assert!(text.contains("rule: bad_type"), "{text}");
        assert!(text.contains("score expression must be numeric"), "{text}");
        assert!(text.contains("score(e.action)"), "{text}");
    }

    #[test]
    fn compile_diagnostic_classifies_field_not_found_as_type() {
        let source = r#"
rule bad_field {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.missing)
    yield out (x = e.sip)
}
"#;
        let file = parse_wfl(source).unwrap();
        let err = validate_wfl_with_diagnostics(
            &file,
            &[auth_events_window(), output_window()],
            source,
            "rules/bad_field.wfl",
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("category: type"), "{text}");
        assert!(text.contains("field `missing` not found"), "{text}");
        assert!(text.contains("entity(ip, e.missing)"), "{text}");
    }

    #[test]
    fn compile_diagnostic_classifies_unknown_alias_as_type() {
        let source = r#"
rule bad_alias {
    events { e : auth_events }
    match<sip:5m> { on event { x | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        let file = parse_wfl(source).unwrap();
        let err = validate_wfl_with_diagnostics(
            &file,
            &[auth_events_window(), output_window()],
            source,
            "rules/bad_alias.wfl",
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("category: type"), "{text}");
        assert!(text.contains("not a declared event alias"), "{text}");
        assert!(text.contains("match<sip:5m>"), "{text}");
    }

    #[test]
    fn rule_location_does_not_cross_into_following_test_block() {
        let source = r#"
rule bad_field {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.missing)
    yield out (x = e.sip)
}

test missing_case for bad_field {
    input {
        row(e, missing = "not here");
    }
    expect {
        hits == 0;
    }
}
"#;
        let file = parse_wfl(source).unwrap();
        let err = validate_wfl_with_diagnostics(
            &file,
            &[auth_events_window(), output_window()],
            source,
            "rules/bad_field.wfl",
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("entity(ip, e.missing)"), "{text}");
        assert!(!text.contains("test missing"), "{text}");
    }

    #[test]
    fn primary_error_token_is_preferred_over_window_token() {
        let error = CheckError {
            severity: Severity::Error,
            rule: Some("bad_field".to_string()),
            test: None,
            message: "field `missing` not found in window `auth_events`".to_string(),
        };
        let source = r#"
rule bad_field {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.missing)
    yield out (x = e.sip)
}
"#;
        let file = parse_wfl(source).unwrap();
        let text =
            format_check_error_with_source(&error, &file, source, "rules/bad_field.wfl".as_ref());
        assert!(text.contains("location: line 5, column 18"), "{text}");
        assert!(text.contains("entity(ip, e.missing)"), "{text}");
    }

    #[test]
    fn yield_preset_error_location_falls_back_to_preset_definition() {
        let source = r#"
yield preset base_alerts (
    n = "missing"
)

rule preset_rule {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts (
        x = e.sip
    )
}
"#;
        let file = parse_wfl(source).unwrap();
        let err = validate_wfl_with_diagnostics(
            &file,
            &[auth_events_window(), output_window()],
            source,
            "rules/preset_bad_type.wfl",
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("yield argument `n` is not a field in target window `out`"),
            "{text}"
        );
        assert!(text.contains("location: line 3, column 5"), "{text}");
        assert!(text.contains("n = \"missing\""), "{text}");
    }

    #[test]
    fn split_yield_preset_error_location_falls_back_to_preset_definition() {
        let source = r#"
yield // split header
    preset
    base_alerts (
    n = "missing"
)

rule preset_rule {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : base_alerts (
        x = e.sip
    )
}
"#;
        let file = parse_wfl(source).unwrap();
        let err = validate_wfl_with_diagnostics(
            &file,
            &[auth_events_window(), output_window()],
            source,
            "rules/preset_bad_type.wfl",
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("yield argument `n` is not a field in target window `out`"),
            "{text}"
        );
        assert!(text.contains("location: line 5, column 5"), "{text}");
        assert!(text.contains("n = \"missing\""), "{text}");
    }

    #[test]
    fn yield_preset_short_arg_does_not_match_rule_yield_line_substring() {
        let source = r#"
yield preset reusable (
    u = "missing"
)

rule preset_rule {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out : reusable (
        x = e.sip
    )
}
"#;
        let file = parse_wfl(source).unwrap();
        let err = validate_wfl_with_diagnostics(
            &file,
            &[auth_events_window(), output_window()],
            source,
            "rules/preset_short_arg.wfl",
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("yield argument `u` is not a field in target window `out`"),
            "{text}"
        );
        assert!(text.contains("location: line 3, column 5"), "{text}");
        assert!(text.contains("u = \"missing\""), "{text}");
        assert!(!text.contains("location: line 10"), "{text}");
    }

    #[test]
    fn yield_preset_decl_line_allows_flexible_whitespace() {
        assert!(line_starts_yield_preset_decl("yield preset base (x = 1)"));
        assert!(line_starts_yield_preset_decl(
            "  yield    preset\tbase (x = 1)"
        ));
        assert!(!line_starts_yield_preset_decl("yield preset (x = 1)"));
        assert!(!line_starts_yield_preset_decl("yield preset1 base (x = 1)"));
        assert!(line_declares_yield_preset(
            "yield\tpreset base (x = 1)",
            "base"
        ));
        assert!(!line_declares_yield_preset(
            "yield preset base_extra (x = 1)",
            "base"
        ));
    }

    #[test]
    fn named_arg_column_ignores_string_and_comment_matches() {
        let line = r#"msg = "u =", u = "missing""#;
        let column = find_named_arg_column(line, "u").unwrap();
        assert_eq!(&line[column - 1..], r#"u = "missing""#);

        assert_eq!(find_named_arg_column(r#"msg = "u =""#, "u"), None);
        assert_eq!(find_named_arg_column("// u = commented", "u"), None);
        assert_eq!(
            find_named_arg_column(r#"msg = "ok", // u = commented"#, "u"),
            None
        );
    }
    // Extra coverage: entry-point diagnostics, categories, locations
    // -----------------------------------------------------------------------

    #[test]
    fn parse_wfl_with_diagnostics_succeeds_on_valid_input() {
        let source = r#"
rule ok {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        let file = parse_wfl_with_diagnostics(source, "rules/ok.wfl").unwrap();
        assert_eq!(file.rules.len(), 1);
    }

    #[test]
    fn validate_wfl_with_diagnostics_succeeds_on_valid_input() {
        let source = r#"
rule ok {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        let file = parse_wfl(source).unwrap();
        validate_wfl_with_diagnostics(
            &file,
            &[auth_events_window(), output_window()],
            source,
            "rules/ok.wfl",
        )
        .unwrap();
    }

    #[test]
    fn compile_wfl_with_diagnostics_returns_plans_and_reports_errors() {
        let ok_source = r#"
rule ok {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        let file = parse_wfl(ok_source).unwrap();
        let plans = compile_wfl_with_diagnostics(
            &file,
            &[auth_events_window(), output_window()],
            ok_source,
            "rules/ok.wfl",
        )
        .unwrap();
        assert_eq!(plans.len(), 1);

        // Semantic error path returns a compile failure with the rule category.
        let bad_source = r#"
rule bad {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.bogus)
}
"#;
        let file = parse_wfl(bad_source).unwrap();
        let err = compile_wfl_with_diagnostics(
            &file,
            &[auth_events_window(), output_window()],
            bad_source,
            "rules/bad.wfl",
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("category: type"), "{text}");
        assert!(text.contains("rule: bad"), "{text}");
        assert!(text.contains("location: line"), "{text}");
    }

    #[test]
    fn compile_diagnostic_classifies_topology_and_sink() {
        let classify = |msg: &str| {
            classify_check_error(&CheckError {
                severity: Severity::Error,
                rule: Some("r".into()),
                test: None,
                message: msg.to_string(),
            })
        };
        assert_eq!(classify("pipeline stage topology issue"), "topology");
        assert_eq!(classify("intermediate window cycle"), "topology");
        assert_eq!(classify("output sink not found"), "sink");
        assert_eq!(classify("yield window mismatch"), "yield");
        assert_eq!(classify("unknown field `x`"), "type");
        assert_eq!(classify("set-level alias in where"), "type");
        assert_eq!(classify("something else entirely"), "rule");
        // test-bound errors classify as "test" regardless of message.
        let test_err = CheckError {
            severity: Severity::Error,
            rule: None,
            test: Some("t".into()),
            message: "boom".to_string(),
        };
        assert_eq!(classify_check_error(&test_err), "test");
    }

    #[test]
    fn test_block_error_uses_test_location() {
        let source = r#"
rule ok {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}

test t for missing_rule {
    input { row(e, sip = "x"); }
    expect { hits == 1; }
}
"#;
        let file = parse_wfl(source).unwrap();
        let errors = check_wfl_errors(&file, &[auth_events_window(), output_window()]);
        let test_error = errors
            .iter()
            .find(|e| e.test.is_some())
            .cloned()
            .expect("test block errors carry test context");
        let formatted = format_check_error_with_source(
            &test_error,
            &file,
            source,
            std::path::Path::new("rules/t.wfl"),
        );
        assert!(formatted.contains("test: t"), "{formatted}");
        assert!(formatted.contains("location: line 9"), "{formatted}");
    }

    #[test]
    fn heuristic_location_needles_pick_section_keywords() {
        let mk = |msg: &str| CheckError {
            severity: Severity::Error,
            rule: Some("r".into()),
            test: None,
            message: msg.to_string(),
        };
        assert_eq!(
            heuristic_location_needles(&mk("alias `x` is not a declared event alias"), "rule"),
            vec!["match"]
        );
        assert_eq!(
            heuristic_location_needles(&mk("yield arg missing"), "yield"),
            vec!["yield"]
        );
        assert_eq!(
            heuristic_location_needles(&mk("score expression must be numeric"), "rule"),
            vec!["score"]
        );
        assert_eq!(
            heuristic_location_needles(&mk("entity id type mismatch"), "rule"),
            vec!["entity"]
        );
        assert_eq!(
            heuristic_location_needles(&mk("event alias `x` conflicts"), "rule"),
            vec!["events"]
        );
        assert_eq!(
            heuristic_location_needles(&mk("match key missing"), "rule"),
            vec!["match"]
        );
        assert_eq!(
            heuristic_location_needles(&mk("misc error"), "rule"),
            vec!["rule"]
        );
    }

    #[test]
    fn message_backtick_token_extraction() {
        assert_eq!(
            message_backtick_tokens("field `a.b` not found in window `w`"),
            vec!["a.b", "w"]
        );
        // Unterminated backtick stops extraction.
        assert_eq!(
            message_backtick_tokens("dangling `token"),
            Vec::<String>::new()
        );
        assert_eq!(
            primary_backtick_tokens("field `x` unknown", "type"),
            vec!["x"]
        );
        assert_eq!(
            primary_backtick_tokens("argument `a` wrong", "type"),
            vec!["a"]
        );
        assert_eq!(
            primary_backtick_tokens("source `s` missing", "type"),
            vec!["s"]
        );
        assert_eq!(
            primary_backtick_tokens("target window `w` unknown", "type"),
            vec!["w"]
        );
        assert_eq!(
            primary_backtick_tokens("target `t` missing", "type"),
            vec!["t"]
        );
    }

    #[test]
    fn translate_position_caps_out_of_range_offset() {
        assert_eq!(translate_position("ab\ncd", 1000), (2, 3));
        assert_eq!(translate_position("", 0), (1, 1));
        assert_eq!(translate_position("a\nb\nc", 3), (2, 2));
    }
}
