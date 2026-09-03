#![allow(clippy::items_after_test_module)]

use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use orion_error::conversion::ToStructError;
use orion_error::prelude::*;

use wf_config::ConfigVarContext;
use wf_config::project::load_wfl_with_context;
use wf_config::resolve_glob;
use wf_config::window::WindowDefaults;
use wf_config::{DistMode, FusionConfig, WindowConfig};
use wf_engine::match_engine::RuleExecutor;
use wf_lang::ast::{FieldRef, Measure};
use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

use crate::error::{RuntimeReason, RuntimeResult};

use super::types::{RunRule, RunRuleKind};

const PIPE_WINDOW_PREFIX: &str = "__wf_pipe_";
const PIPE_EVENT_TIME_FIELD: &str = "__wf_pipe_ts";
const RULE_PRELUDE_FILE: &str = "_global.wfl";

#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.ReactorLifecycle")]
struct ParsedRuleFile {
    path: PathBuf,
    source: String,
    file: wf_lang::ast::WflFile,
}

// ---------------------------------------------------------------------------
// Compile-phase helpers — pure data transforms extracted from start()
// ---------------------------------------------------------------------------

/// Load all `.wfs` schema files matching `glob_pattern` under `base_dir`.
pub(crate) fn load_schemas(
    glob_pattern: &str,
    base_dir: &Path,
) -> RuntimeResult<Vec<wf_lang::WindowSchema>> {
    let wfs_paths = resolve_glob(glob_pattern, base_dir)
        .source_err(RuntimeReason::core_conf(), "resolve schema glob")?;
    let mut all_schemas = Vec::new();
    for full_path in &wfs_paths {
        let content = std::fs::read_to_string(full_path)
            .source_err(RuntimeReason::system_error(), "read schema file")
            .position(full_path.display().to_string())?;
        let schemas = wf_lang::parse_wfs(&content)
            .source_err(RuntimeReason::Bootstrap, "parse schema file")
            .position(full_path.display().to_string())?;
        wf_debug!(conf, file = %full_path.display(), schemas = schemas.len(), "loaded schema file");
        all_schemas.extend(schemas);
    }
    Ok(all_schemas)
}

/// Load, preprocess, parse, and compile all `.wfl` rule files matching
/// `glob_pattern` under `base_dir`, substituting `vars` and validating
/// against the given `schemas`.
pub(crate) fn compile_rules(
    glob_pattern: &str,
    base_dir: &Path,
    ctx: &ConfigVarContext,
    schemas: &[wf_lang::WindowSchema],
) -> RuntimeResult<(Vec<wf_lang::plan::RulePlan>, Vec<wf_lang::WindowSchema>)> {
    let wfl_paths = resolve_glob(glob_pattern, base_dir)
        .source_err(RuntimeReason::core_conf(), "resolve rule glob")?;
    let prelude_path = rule_prelude_path(glob_pattern, base_dir);
    let prelude = load_rule_prelude(&prelude_path, ctx, base_dir)?;
    let mut parsed_files = Vec::new();
    let mut all_rules = Vec::new();
    for full_path in &wfl_paths {
        if same_path(full_path, &prelude_path) {
            continue;
        }
        let preprocessed = load_wfl_with_context(full_path, ctx, Some(base_dir))
            .source_err(RuntimeReason::data_error(), "load rule file")
            .position(full_path.display().to_string())?;
        let mut wfl_file = wf_lang::parse_wfl_with_diagnostics(&preprocessed, full_path)
            .map_err(lang_diagnostic)?;
        validate_rule_prelude_conflicts(&wfl_file, &preprocessed, full_path, prelude.as_ref())?;
        apply_rule_prelude(&mut wfl_file, prelude.as_ref());
        // issue #73: `use "file.wfl"` 导入顶层列表（include 语义, 递归/循环/重名报错）。
        wfl_file =
            wf_lang::compiler::lists::resolve_imports(&wfl_file, full_path, &mut |import_path| {
                load_wfl_with_context(import_path, ctx, Some(base_dir)).map_err(|e| {
                    wf_lang::error::error(
                        wf_lang::LangReason::Compile,
                        e.detail().clone().unwrap_or_else(|| e.to_string()),
                    )
                })
            })
            .map_err(lang_diagnostic)?;
        all_rules.extend(wfl_file.rules.iter().cloned());
        parsed_files.push(ParsedRuleFile {
            path: full_path.clone(),
            source: preprocessed,
            file: wfl_file,
        });
    }

    let effective_schemas = wf_lang::effective_schemas_for_rules(&all_rules, schemas);
    let mut topology_errors = Vec::new();
    wf_lang::check_intermediate_target_graph(&all_rules, &mut topology_errors);
    let topology_hard_errors: Vec<_> = topology_errors
        .into_iter()
        .filter(|error| error.severity == wf_lang::Severity::Error)
        .collect();
    if !topology_hard_errors.is_empty() {
        let msgs: Vec<String> = topology_hard_errors
            .into_iter()
            .map(|error| format_topology_error(&error, &parsed_files))
            .collect();
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail(msgs.join("\n"))
            .err();
    }

    let mut all_rule_plans = Vec::new();
    for parsed in &parsed_files {
        let plans = compile_rule_file_with_prelude_diagnostics(
            parsed,
            prelude.as_ref(),
            &effective_schemas,
        )?;
        wf_debug!(conf, file = %parsed.path.display(), rules = plans.len(), "compiled rule file");
        all_rule_plans.extend(plans);
    }
    Ok((all_rule_plans, effective_schemas))
}

fn compile_rule_file_with_prelude_diagnostics(
    parsed: &ParsedRuleFile,
    prelude: Option<&ParsedRuleFile>,
    schemas: &[wf_lang::WindowSchema],
) -> RuntimeResult<Vec<wf_lang::plan::RulePlan>> {
    let errors: Vec<_> = wf_lang::check_wfl(&parsed.file, schemas)
        .into_iter()
        .filter(|error| error.severity == wf_lang::Severity::Error)
        .collect();
    if !errors.is_empty() {
        let diagnostics: Vec<String> = errors
            .iter()
            .map(|error| format_rule_check_error(error, parsed, prelude))
            .collect();
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail(format!("semantic errors:\n{}", diagnostics.join("\n\n")))
            .err();
    }

    wf_lang::compile_wfl_with_diagnostics(&parsed.file, schemas, &parsed.source, &parsed.path)
        .map_err(lang_diagnostic)
}

fn format_rule_check_error(
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

fn format_prelude_yield_preset_error(
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

fn backtick_tokens(message: &str) -> Vec<String> {
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

fn extract_backtick_token_after(message: &str, label: &str) -> Option<String> {
    let idx = message.find(label)?;
    let after_label = &message[idx + label.len()..];
    let start = after_label.find('`')?;
    let after_start = &after_label[start + 1..];
    let end = after_start.find('`')?;
    Some(after_start[..end].to_string())
}

fn find_prelude_yield_preset_arg_location(
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

fn find_referenced_prelude_yield_preset_token_location(
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

fn find_prelude_yield_preset_token_location(
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

fn yield_preset_source_end(
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

fn find_yield_preset_decl_location(source: &str, preset_name: &str) -> Option<(usize, usize)> {
    find_nth_yield_preset_decl_location(source, preset_name, 1)
}

fn find_nth_yield_preset_decl_location(
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
    column: usize,
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

fn source_line_column(source: &str, offset: usize) -> (usize, usize) {
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

fn is_ident_start_byte(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn find_named_arg_column(line: &str, arg_name: &str) -> Option<usize> {
    find_named_token_column(line, arg_name, true)
}

fn find_token_column(line: &str, token: &str) -> Option<usize> {
    find_named_token_column(line, token, false)
}

fn find_named_token_column(line: &str, token: &str, require_assignment: bool) -> Option<usize> {
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

fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn source_line_snippet(source: &str, line: usize, column: usize) -> String {
    let Some(text) = source.lines().nth(line.saturating_sub(1)) else {
        return String::new();
    };
    format!("  {}\n  {}^", text, " ".repeat(column.saturating_sub(1)))
}

fn rule_prelude_path(glob_pattern: &str, base_dir: &Path) -> PathBuf {
    rule_glob_root(glob_pattern, base_dir).join(RULE_PRELUDE_FILE)
}

fn rule_glob_root(glob_pattern: &str, base_dir: &Path) -> PathBuf {
    if !contains_glob_meta(glob_pattern) {
        return base_dir
            .join(glob_pattern)
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| base_dir.to_path_buf());
    }

    let mut root = base_dir.to_path_buf();
    for component in Path::new(glob_pattern).components() {
        match component {
            Component::Normal(part) => {
                if contains_glob_meta(&part.to_string_lossy()) {
                    break;
                }
                root.push(part);
            }
            Component::CurDir => {}
            Component::ParentDir => root.push(".."),
            Component::RootDir | Component::Prefix(_) => {
                root.push(component.as_os_str());
            }
        }
    }
    root
}

fn contains_glob_meta(value: &str) -> bool {
    value.contains('*') || value.contains('?') || value.contains('[')
}

fn same_path(left: &Path, right: &Path) -> bool {
    left == right
        || left
            .canonicalize()
            .ok()
            .zip(right.canonicalize().ok())
            .is_some_and(|(left, right)| left == right)
}

fn load_rule_prelude(
    prelude_path: &Path,
    ctx: &ConfigVarContext,
    base_dir: &Path,
) -> RuntimeResult<Option<ParsedRuleFile>> {
    if !prelude_path.exists() {
        return Ok(None);
    }

    let source = load_wfl_with_context(prelude_path, ctx, Some(base_dir))
        .source_err(RuntimeReason::data_error(), "load rule prelude")
        .position(prelude_path.display().to_string())?;
    let file =
        wf_lang::parse_wfl_with_diagnostics(&source, prelude_path).map_err(lang_diagnostic)?;
    validate_rule_prelude(&file, &source, prelude_path)?;
    Ok(Some(ParsedRuleFile {
        path: prelude_path.to_path_buf(),
        source,
        file,
    }))
}

fn validate_rule_prelude(
    file: &wf_lang::ast::WflFile,
    source: &str,
    path: &Path,
) -> RuntimeResult<()> {
    let invalid = if !file.uses.is_empty() {
        Some("use declarations")
    } else if !file.patterns.is_empty() {
        Some("pattern declarations")
    } else if !file.lists.is_empty() {
        // issue #73 定稿: 列表走 `use` 导入, prelude 只管 yield preset。
        Some("list declarations (declare lists in a separate file and `use` it)")
    } else if !file.rules.is_empty() {
        Some("rule declarations")
    } else if !file.tests.is_empty() {
        Some("test blocks")
    } else {
        None
    };

    if let Some(kind) = invalid {
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail(format!(
                "{} is a rule prelude and only allows `yield preset` declarations; found {}",
                path.display(),
                kind
            ))
            .err();
    }
    validate_unique_yield_presets(file, source, path, "rule prelude")?;
    Ok(())
}

fn validate_rule_prelude_conflicts(
    file: &wf_lang::ast::WflFile,
    source: &str,
    path: &Path,
    prelude: Option<&ParsedRuleFile>,
) -> RuntimeResult<()> {
    validate_unique_yield_presets(file, source, path, "rule file")?;
    let Some(prelude) = prelude else {
        return Ok(());
    };

    for preset in &file.yield_presets {
        if prelude
            .file
            .yield_presets
            .iter()
            .any(|prelude_preset| prelude_preset.name == preset.name)
        {
            let (line, column) =
                find_yield_preset_decl_location(source, &preset.name).unwrap_or((1, 1));
            return RuntimeReason::Bootstrap
                .to_err()
                .with_detail(format!(
                    "{} defines yield preset `{}` that already exists in prelude {}\nlocation: line {}, column {}\n{}",
                    path.display(),
                    preset.name,
                    prelude.path.display(),
                    line,
                    column,
                    source_line_snippet(source, line, column)
                ))
                .err();
        }
    }
    Ok(())
}

fn validate_unique_yield_presets(
    file: &wf_lang::ast::WflFile,
    source: &str,
    path: &Path,
    scope: &str,
) -> RuntimeResult<()> {
    let mut seen: HashMap<&str, usize> = HashMap::new();
    for preset in &file.yield_presets {
        let count = seen.entry(preset.name.as_str()).or_insert(0);
        *count += 1;
        if *count > 1 {
            let (line, column) =
                find_nth_yield_preset_decl_location(source, &preset.name, *count).unwrap_or((1, 1));
            return RuntimeReason::Bootstrap
                .to_err()
                .with_detail(format!(
                    "{} duplicate yield preset `{}` in {}\nlocation: line {}, column {}\n{}",
                    path.display(),
                    preset.name,
                    scope,
                    line,
                    column,
                    source_line_snippet(source, line, column)
                ))
                .err();
        }
    }
    Ok(())
}

fn apply_rule_prelude(file: &mut wf_lang::ast::WflFile, prelude: Option<&ParsedRuleFile>) {
    let Some(prelude) = prelude else {
        return;
    };
    let mut yield_presets = prelude.file.yield_presets.clone();
    yield_presets.extend(file.yield_presets.clone());
    file.yield_presets = yield_presets;
}

fn lang_diagnostic(error: wf_lang::LangError) -> crate::error::RuntimeError {
    RuntimeReason::Bootstrap
        .to_err()
        .with_detail(error.detail().clone().unwrap_or_else(|| error.to_string()))
}

fn format_topology_error(error: &wf_lang::CheckError, parsed_files: &[ParsedRuleFile]) -> String {
    if let Some(rule_name) = error.rule.as_deref()
        && let Some(parsed) = parsed_files
            .iter()
            .find(|parsed| parsed.file.rules.iter().any(|rule| rule.name == rule_name))
    {
        return wf_lang::diagnostics::format_check_error_with_source(
            error,
            &parsed.file,
            &parsed.source,
            &parsed.path,
        );
    }
    error.to_string()
}

pub(crate) fn build_runtime_var_context(
    config: &FusionConfig,
    base_dir: &Path,
) -> ConfigVarContext {
    let mut vars = config.vars.clone();
    vars.entry("WORK_DIR".to_string())
        .or_insert_with(|| base_dir.to_string_lossy().to_string());
    vars.entry("WORK_ROOT".to_string()).or_insert_with(|| {
        resolve_work_root(config, base_dir)
            .to_string_lossy()
            .to_string()
    });
    ConfigVarContext::from_explicit_vars(vars)
}

pub(crate) fn resolve_work_root(config: &FusionConfig, base_dir: &Path) -> std::path::PathBuf {
    config
        .work_root
        .as_ref()
        .map(|path| base_dir.join(path))
        .unwrap_or_else(|| base_dir.to_path_buf())
}

/// Build synthetic schemas/configs for internal pipeline windows (`|>` desugar).
pub(crate) fn build_pipeline_internal_windows(
    plans: &[wf_lang::plan::RulePlan],
    base_schemas: &[WindowSchema],
    defaults: &WindowDefaults,
) -> (Vec<WindowSchema>, Vec<WindowConfig>) {
    let mut known_schemas: HashMap<String, WindowSchema> = base_schemas
        .iter()
        .map(|s| (s.name.clone(), s.clone()))
        .collect();

    let mut derived = Vec::new();
    for plan in plans {
        let target = &plan.yield_plan.target;
        if !is_pipeline_window_name(target) || known_schemas.contains_key(target) {
            continue;
        }

        let Some(over) = find_pipeline_window_over(plans, target) else {
            wf_warn!(
                conf,
                window = %target,
                "skip internal pipeline window without downstream consumer"
            );
            continue;
        };

        let mut fields = vec![FieldDef {
            name: PIPE_EVENT_TIME_FIELD.to_string(),
            field_type: FieldType::Base(BaseType::Time),
        }];
        fields.extend(infer_pipeline_output_fields(plan, &known_schemas));

        let ws = WindowSchema {
            name: target.clone(),
            // Bind stream alias routing uses schema streams; subscribe this internal
            // window on a synthetic stream equal to its own name.
            streams: vec![target.clone()],
            time_field: Some(PIPE_EVENT_TIME_FIELD.to_string()),
            over,
            fields,
        };
        known_schemas.insert(ws.name.clone(), ws.clone());
        derived.push(ws);
    }

    let configs = derived
        .iter()
        .map(|ws| WindowConfig {
            name: ws.name.clone(),
            mode: DistMode::Local,
            max_window_bytes: defaults.max_window_bytes,
            over_cap: ws.over.into(),
            evict_policy: defaults.evict_policy,
            watermark: defaults.watermark,
            allowed_lateness: defaults.allowed_lateness,
            late_policy: defaults.late_policy,
            table: None,
        })
        .collect();

    (derived, configs)
}

/// Build [`RunRule`] instances from compiled plans, pre-computing stream
/// alias routing and constructing the CEP state machines.
pub(crate) fn build_run_rules(
    plans: &[wf_lang::plan::RulePlan],
    schemas: &[wf_lang::WindowSchema],
    output: &wf_config::OutputConfig,
) -> Vec<RunRule> {
    let mut rules = Vec::with_capacity(plans.len());
    for plan in plans {
        let window_aliases = build_window_aliases(&plan.binds);
        let executor = RuleExecutor::new_with_options(
            plan.clone(),
            wf_engine::match_engine::RuleExecutorOptions {
                yield_field_types: resolve_yield_field_types(plan, schemas),
                output: output.clone(),
            },
        );
        let kind = if let Some(stats_plan) = &plan.stats_plan {
            RunRuleKind::Stats {
                stats_plan: stats_plan.clone(),
                time_field: resolve_time_field(&plan.binds, schemas),
            }
        } else if let Some(each_plan) = &plan.each_plan {
            RunRuleKind::Each {
                alias: each_plan.alias.clone(),
                time_field: resolve_alias_time_field(&plan.binds, schemas, &each_plan.alias),
            }
        } else {
            RunRuleKind::Match {
                match_plan: plan.match_plan.clone(),
                time_field: resolve_time_field(&plan.binds, schemas),
                limits: plan.limits_plan.clone(),
            }
        };
        rules.push(RunRule {
            kind,
            executor,
            window_aliases,
        });
    }
    rules
}

pub(crate) fn collect_intermediate_targets(plans: &[wf_lang::plan::RulePlan]) -> HashSet<String> {
    let consumed_windows: HashSet<&str> = plans
        .iter()
        .flat_map(|plan| plan.binds.iter().map(|bind| bind.window.as_str()))
        .collect();

    plans
        .iter()
        .map(|plan| plan.yield_plan.target.as_str())
        .filter(|target| consumed_windows.contains(*target))
        .map(str::to_string)
        .collect()
}

fn resolve_yield_field_types(
    plan: &wf_lang::plan::RulePlan,
    schemas: &[WindowSchema],
) -> HashMap<String, FieldType> {
    let Some(target_schema) = schemas.iter().find(|ws| ws.name == plan.yield_plan.target) else {
        return HashMap::new();
    };
    let schema_fields: HashMap<&str, &FieldType> = target_schema
        .fields
        .iter()
        .map(|field| (field.name.as_str(), &field.field_type))
        .collect();

    plan.yield_plan
        .fields
        .iter()
        .filter_map(|field| {
            schema_fields
                .get(field.name.as_str())
                .map(|field_type| (field.name.clone(), (*field_type).clone()))
        })
        .collect()
}

/// Resolve the event-time field name for a rule from its first bind's window schema.
pub(crate) fn resolve_time_field(
    binds: &[wf_lang::plan::BindPlan],
    schemas: &[wf_lang::WindowSchema],
) -> Option<String> {
    binds.first().and_then(|bind| {
        schemas
            .iter()
            .find(|ws| ws.name == bind.window)
            .and_then(|ws| ws.time_field.clone())
    })
}

fn resolve_alias_time_field(
    binds: &[wf_lang::plan::BindPlan],
    schemas: &[wf_lang::WindowSchema],
    alias: &str,
) -> Option<String> {
    let bind = binds.iter().find(|bind| bind.alias == alias)?;
    schemas
        .iter()
        .find(|ws| ws.name == bind.window)
        .and_then(|ws| ws.time_field.clone())
}

/// Build window_name → alias routing for a rule from its binds.
fn build_window_aliases(binds: &[wf_lang::plan::BindPlan]) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for bind in binds {
        map.entry(bind.window.clone())
            .or_default()
            .push(bind.alias.clone());
    }
    map
}

fn is_pipeline_window_name(name: &str) -> bool {
    name.starts_with(PIPE_WINDOW_PREFIX)
}

fn find_pipeline_window_over(plans: &[wf_lang::plan::RulePlan], window: &str) -> Option<Duration> {
    plans
        .iter()
        .find(|p| p.binds.iter().any(|b| b.window == window))
        .map(|p| match p.match_plan.window_spec {
            wf_lang::plan::WindowSpec::Sliding(d)
            | wf_lang::plan::WindowSpec::Fixed(d)
            | wf_lang::plan::WindowSpec::Session(d) => d,
            // Hop 的管道 over = 窗口大小（下游需保留整窗数据）。
            wf_lang::plan::WindowSpec::Hop { size, .. } => size,
        })
}

fn infer_pipeline_output_fields(
    plan: &wf_lang::plan::RulePlan,
    schemas: &HashMap<String, WindowSchema>,
) -> Vec<FieldDef> {
    let key_types = infer_key_field_types(plan, schemas);
    let branch_types = infer_branch_output_types(plan);

    plan.yield_plan
        .fields
        .iter()
        .filter_map(|f| {
            if f.name == PIPE_EVENT_TIME_FIELD {
                return None;
            }
            let field_type = key_types
                .get(&f.name)
                .cloned()
                .or_else(|| branch_types.get(&f.name).cloned())
                .unwrap_or(FieldType::Base(BaseType::Chars));
            Some(FieldDef {
                name: f.name.clone(),
                field_type,
            })
        })
        .collect()
}

fn infer_key_field_types(
    plan: &wf_lang::plan::RulePlan,
    schemas: &HashMap<String, WindowSchema>,
) -> HashMap<String, FieldType> {
    let mut out = HashMap::new();

    if let Some(key_map) = &plan.match_plan.key_map {
        for item in key_map {
            if let Some(field_type) = resolve_bind_field_type(
                &plan.binds,
                schemas,
                &item.source_alias,
                &item.source_field,
            ) {
                out.insert(item.logical_name.clone(), field_type);
            }
        }
        return out;
    }

    for key in &plan.match_plan.keys {
        let name = key_output_name(key);
        if let Some(field_type) = resolve_key_field_type(plan, schemas, key) {
            out.insert(name, field_type);
        }
    }

    out
}

fn resolve_key_field_type(
    plan: &wf_lang::plan::RulePlan,
    schemas: &HashMap<String, WindowSchema>,
    key: &FieldRef,
) -> Option<FieldType> {
    match key {
        FieldRef::Qualified(alias, field) | FieldRef::Bracketed(alias, field) => {
            resolve_bind_field_type(&plan.binds, schemas, alias, field)
        }
        FieldRef::Simple(field) => {
            let mut found: Vec<FieldType> = Vec::new();
            for bind in &plan.binds {
                let Some(ws) = schemas.get(&bind.window) else {
                    continue;
                };
                let Some(field_type) = ws
                    .fields
                    .iter()
                    .find(|f| f.name == *field)
                    .map(|f| f.field_type.clone())
                else {
                    continue;
                };
                if !found.contains(&field_type) {
                    found.push(field_type);
                }
            }
            if found.len() == 1 {
                Some(found.remove(0))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn resolve_bind_field_type(
    binds: &[wf_lang::plan::BindPlan],
    schemas: &HashMap<String, WindowSchema>,
    alias: &str,
    field: &str,
) -> Option<FieldType> {
    let bind = binds.iter().find(|b| b.alias == alias)?;
    let ws = schemas.get(&bind.window)?;
    ws.fields
        .iter()
        .find(|f| f.name == field)
        .map(|f| f.field_type.clone())
}

fn infer_branch_output_types(plan: &wf_lang::plan::RulePlan) -> HashMap<String, FieldType> {
    let mut map = HashMap::new();
    for step in plan
        .match_plan
        .event_steps
        .iter()
        .chain(plan.match_plan.close_steps.iter())
    {
        for branch in &step.branches {
            let name = branch
                .label
                .clone()
                .unwrap_or_else(|| measure_output_name(branch.agg.measure).to_string());
            let field_type = match branch.agg.measure {
                Measure::Count => FieldType::Base(BaseType::Digit),
                Measure::Sum | Measure::Avg | Measure::Min | Measure::Max => {
                    FieldType::Base(BaseType::Float)
                }
                _ => FieldType::Base(BaseType::Float),
            };
            map.insert(name, field_type);
        }
    }
    map
}

fn key_output_name(key: &FieldRef) -> String {
    match key {
        FieldRef::Simple(name) => name.clone(),
        FieldRef::Qualified(_, field) | FieldRef::Bracketed(_, field) => field.clone(),
        _ => "__unknown_key".to_string(),
    }
}

fn measure_output_name(measure: Measure) -> &'static str {
    match measure {
        Measure::Count => "count",
        Measure::Sum => "sum",
        Measure::Avg => "avg",
        Measure::Min => "min",
        Measure::Max => "max",
        _ => "measure",
    }
}


#[cfg(test)]
#[path = "compile_tests.rs"]
mod compile_tests;

pub(crate) fn load_static_schemas(
    glob_pattern: &str,
    base_dir: &Path,
) -> RuntimeResult<Vec<wf_lang::StaticWindowSchema>> {
    let mut schemas = Vec::new();
    for path in resolve_glob(glob_pattern, base_dir).map_err(|e| {
        RuntimeReason::Bootstrap
            .to_err()
            .with_detail(format!("glob: {}", e))
    })? {
        let source = std::fs::read_to_string(&path)
            .source_err(RuntimeReason::Bootstrap, format!("read schema {:?}", path))?;
        let parsed = wf_lang::parse_static_wfs(&source).map_err(|e| {
            RuntimeReason::Bootstrap
                .to_err()
                .with_detail(format!("parse static schemas from {:?}: {}", path, e))
        })?;
        schemas.extend(parsed);
    }
    Ok(schemas)
}
