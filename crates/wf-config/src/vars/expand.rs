use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

use orion_error::conversion::{SourceRawErr, ToStructError};
use toml::Value as TomlValue;

use crate::vars::{
    ConfigVarContext, ExpandedToml, SourceAtom, TracedValue, VarsReason, VarsResult,
};

pub fn preprocess_toml(
    source: &str,
    ctx: &ConfigVarContext,
    strip_vars: bool,
) -> VarsResult<String> {
    let mut value: TomlValue =
        toml::from_str(source).source_raw_err(VarsReason::Toml, "parse TOML for preprocessing")?;
    let effective_vars = resolve_effective_vars_in_value(&value, ctx)?;
    expand_toml_strings(&mut value, &effective_vars, ctx)?;
    if strip_vars && let Some(table) = value.as_table_mut() {
        table.remove("vars");
    }
    toml::to_string(&value).source_raw_err(VarsReason::Toml, "serialize preprocessed TOML")
}

pub fn resolve_value_vars(
    value: &TomlValue,
    ctx: &ConfigVarContext,
) -> VarsResult<HashMap<String, String>> {
    resolve_effective_vars_in_value(value, ctx)
}

pub fn resolve_toml_vars(
    source: &str,
    ctx: &ConfigVarContext,
) -> VarsResult<HashMap<String, String>> {
    let value: TomlValue =
        toml::from_str(source).source_raw_err(VarsReason::Toml, "parse TOML variables")?;
    resolve_effective_vars_in_value(&value, ctx)
}

pub fn expand_value(value: &TomlValue, ctx: &ConfigVarContext) -> VarsResult<TomlValue> {
    let effective_vars = resolve_effective_vars_in_value(value, ctx)?;
    let mut expanded = value.clone();
    expand_toml_strings(&mut expanded, &effective_vars, ctx)?;
    Ok(expanded)
}

pub fn expand_toml(source: &str, ctx: &ConfigVarContext, strip_vars: bool) -> VarsResult<String> {
    preprocess_toml(source, ctx, strip_vars)
}

pub fn resolve_value_vars_with_sources<F>(
    value: &TomlValue,
    ctx: &ConfigVarContext,
    mut origin_for_path: F,
) -> VarsResult<HashMap<String, TracedValue>>
where
    F: FnMut(&str) -> Option<PathBuf>,
{
    let raw_vars = extract_config_vars(value)?;
    let raw_origins: HashMap<String, Option<PathBuf>> = raw_vars
        .keys()
        .map(|key| (key.clone(), origin_for_path(&format!("vars.{key}"))))
        .collect();
    let mut resolver = ConfigVarResolver::new(&raw_vars, &raw_origins, ctx);
    let mut out = resolver.resolve_all()?;

    for (key, value) in ctx.explicit_vars() {
        out.insert(
            key.clone(),
            TracedValue::with_source(value.clone(), SourceAtom::Explicit(key.clone())),
        );
    }

    Ok(out)
}

pub fn resolve_toml_vars_with_sources<F>(
    source: &str,
    ctx: &ConfigVarContext,
    origin_for_path: F,
) -> VarsResult<HashMap<String, TracedValue>>
where
    F: FnMut(&str) -> Option<PathBuf>,
{
    let value: TomlValue = toml::from_str(source)
        .source_raw_err(VarsReason::Toml, "parse TOML variables with sources")?;
    resolve_value_vars_with_sources(&value, ctx, origin_for_path)
}

pub fn expand_value_with_sources<F>(
    value: &TomlValue,
    ctx: &ConfigVarContext,
    mut origin_for_path: F,
) -> VarsResult<ExpandedToml>
where
    F: FnMut(&str) -> Option<PathBuf>,
{
    let effective_vars = resolve_value_vars_with_sources(value, ctx, |path| origin_for_path(path))?;
    let mut expanded = value.clone();
    let mut sources = HashMap::new();
    let _ = expand_toml_strings_with_sources(
        &mut expanded,
        None,
        &effective_vars,
        ctx,
        &mut origin_for_path,
        &mut sources,
    )?;
    Ok(ExpandedToml {
        value: expanded,
        sources,
    })
}

pub fn expand_toml_with_sources<F>(
    source: &str,
    ctx: &ConfigVarContext,
    strip_vars: bool,
    origin_for_path: F,
) -> VarsResult<ExpandedToml>
where
    F: FnMut(&str) -> Option<PathBuf>,
{
    let value: TomlValue =
        toml::from_str(source).source_raw_err(VarsReason::Toml, "parse TOML for expansion")?;
    let mut expanded = expand_value_with_sources(&value, ctx, origin_for_path)?;
    if strip_vars && let Some(table) = expanded.value.as_table_mut() {
        table.remove("vars");
        expanded.sources.retain(|path, _| {
            path != "vars"
                && !path
                    .strip_prefix("vars")
                    .is_some_and(|rest| rest.starts_with('.') || rest.starts_with('['))
        });
    }
    Ok(expanded)
}

pub fn collect_active_external_sources(
    value: &TomlValue,
    vars: &HashMap<String, TracedValue>,
    ctx: &ConfigVarContext,
) -> VarsResult<BTreeSet<SourceAtom>> {
    let mut out = BTreeSet::new();
    collect_active_external_sources_in_value(value, None, vars, ctx, &mut out)?;
    Ok(out)
}

pub fn external_value_with_source(ident: &str, ctx: &ConfigVarContext) -> Option<TracedValue> {
    if let Some(value) = ctx.explicit_vars().get(ident) {
        return Some(TracedValue::with_source(
            value.clone(),
            SourceAtom::Explicit(ident.to_string()),
        ));
    }
    ctx.env_var_value(ident)
        .map(|value| TracedValue::with_source(value, SourceAtom::Env(ident.to_string())))
}

fn extract_config_vars(value: &TomlValue) -> VarsResult<HashMap<String, String>> {
    let Some(vars) = value.get("vars") else {
        return Ok(HashMap::new());
    };
    let Some(table) = vars.as_table() else {
        return VarsReason::Resolve
            .to_err()
            .with_detail("vars must be a TOML table")
            .err();
    };

    let mut out = HashMap::with_capacity(table.len());
    for (key, value) in table {
        let Some(val) = value.as_str() else {
            return VarsReason::Resolve
                .to_err()
                .with_detail(format!("vars.{key} must be a string"))
                .err();
        };
        out.insert(key.clone(), val.to_string());
    }
    Ok(out)
}

fn resolve_effective_vars_in_value(
    value: &TomlValue,
    ctx: &ConfigVarContext,
) -> VarsResult<HashMap<String, String>> {
    let traced = resolve_value_vars_with_sources(value, ctx, |_| None)?;
    Ok(traced
        .into_iter()
        .map(|(key, traced)| (key, traced.value))
        .collect())
}

fn expand_toml_strings(
    value: &mut TomlValue,
    vars: &HashMap<String, String>,
    ctx: &ConfigVarContext,
) -> VarsResult<()> {
    match value {
        TomlValue::String(s) => {
            *s = expand_template(s, |ident| {
                Ok(vars
                    .get(ident)
                    .cloned()
                    .or_else(|| ctx.resolve_external_var(ident)))
            })?;
        }
        TomlValue::Array(items) => {
            for item in items {
                expand_toml_strings(item, vars, ctx)?;
            }
        }
        TomlValue::Table(table) => {
            for (_, value) in table.iter_mut() {
                expand_toml_strings(value, vars, ctx)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn expand_toml_strings_with_sources<F>(
    value: &mut TomlValue,
    path: Option<&str>,
    vars: &HashMap<String, TracedValue>,
    ctx: &ConfigVarContext,
    origin_for_path: &mut F,
    sources: &mut HashMap<String, BTreeSet<SourceAtom>>,
) -> VarsResult<BTreeSet<SourceAtom>>
where
    F: FnMut(&str) -> Option<PathBuf>,
{
    match value {
        TomlValue::String(s) => {
            let literal_source = path.and_then(origin_for_path).map(SourceAtom::File);
            let traced = expand_template_with_trace(
                s,
                |ident| {
                    Ok(vars
                        .get(ident)
                        .cloned()
                        .or_else(|| external_value_with_source(ident, ctx)))
                },
                literal_source,
            )?;
            *s = traced.value.clone();
            if let Some(path) = path
                && !traced.sources.is_empty()
            {
                sources.insert(path.to_string(), traced.sources.clone());
            }
            Ok(traced.sources)
        }
        TomlValue::Array(items) => {
            let mut combined = BTreeSet::new();
            for (idx, item) in items.iter_mut().enumerate() {
                let child_path = join_indexed_path(path, idx);
                combined.extend(expand_toml_strings_with_sources(
                    item,
                    Some(&child_path),
                    vars,
                    ctx,
                    origin_for_path,
                    sources,
                )?);
            }
            if let Some(path) = path
                && !combined.is_empty()
            {
                sources.insert(path.to_string(), combined.clone());
            }
            Ok(combined)
        }
        TomlValue::Table(table) => {
            let mut combined = BTreeSet::new();
            for (key, value) in table.iter_mut() {
                let child_path = join_path(path, key);
                combined.extend(expand_toml_strings_with_sources(
                    value,
                    Some(&child_path),
                    vars,
                    ctx,
                    origin_for_path,
                    sources,
                )?);
            }
            if let Some(path) = path
                && !combined.is_empty()
            {
                sources.insert(path.to_string(), combined.clone());
            }
            Ok(combined)
        }
        _ => Ok(BTreeSet::new()),
    }
}

fn collect_active_external_sources_in_value(
    value: &TomlValue,
    path: Option<&str>,
    vars: &HashMap<String, TracedValue>,
    ctx: &ConfigVarContext,
    out: &mut BTreeSet<SourceAtom>,
) -> VarsResult<()> {
    match value {
        TomlValue::String(s) => {
            let traced = expand_template_with_trace(
                s,
                |ident| {
                    Ok(vars
                        .get(ident)
                        .cloned()
                        .or_else(|| external_value_with_source(ident, ctx)))
                },
                None,
            )?;
            for source in traced.sources {
                match source {
                    SourceAtom::Explicit(_) | SourceAtom::Env(_) | SourceAtom::Default(_) => {
                        out.insert(source);
                    }
                    SourceAtom::File(_) => {}
                }
            }
        }
        TomlValue::Array(items) => {
            for (idx, item) in items.iter().enumerate() {
                let child_path = join_indexed_path(path, idx);
                collect_active_external_sources_in_value(item, Some(&child_path), vars, ctx, out)?;
            }
        }
        TomlValue::Table(table) => {
            for (key, value) in table {
                if path.is_none() && key == "vars" {
                    continue;
                }
                let child_path = join_path(path, key);
                collect_active_external_sources_in_value(value, Some(&child_path), vars, ctx, out)?;
            }
        }
        _ => {}
    }
    Ok(())
}

struct ConfigVarResolver<'a> {
    raw: &'a HashMap<String, String>,
    raw_origins: &'a HashMap<String, Option<PathBuf>>,
    ctx: &'a ConfigVarContext,
    resolved: HashMap<String, TracedValue>,
    stack: Vec<String>,
}

impl<'a> ConfigVarResolver<'a> {
    fn new(
        raw: &'a HashMap<String, String>,
        raw_origins: &'a HashMap<String, Option<PathBuf>>,
        ctx: &'a ConfigVarContext,
    ) -> Self {
        Self {
            raw,
            raw_origins,
            ctx,
            resolved: HashMap::with_capacity(raw.len()),
            stack: Vec::new(),
        }
    }

    fn resolve_all(&mut self) -> VarsResult<HashMap<String, TracedValue>> {
        let keys: Vec<String> = self.raw.keys().cloned().collect();
        for key in keys {
            let _ = self.resolve_key(&key)?;
        }
        Ok(std::mem::take(&mut self.resolved))
    }

    fn resolve_key(&mut self, key: &str) -> VarsResult<TracedValue> {
        if let Some(value) = self.resolved.get(key) {
            return Ok(value.clone());
        }
        if self.stack.iter().any(|item| item == key) {
            let mut chain = self.stack.clone();
            chain.push(key.to_string());
            return VarsReason::Resolve
                .to_err()
                .with_detail(format!(
                    "cyclic variable reference in [vars]: {}",
                    chain.join(" -> ")
                ))
                .err();
        }

        let Some(raw_value) = self.raw.get(key) else {
            return VarsReason::Resolve
                .to_err()
                .with_detail(format!("unknown config variable '{key}'"))
                .err();
        };
        let raw_value = raw_value.clone();
        let literal_source = self
            .raw_origins
            .get(key)
            .and_then(|path| path.clone())
            .map(SourceAtom::File);

        self.stack.push(key.to_string());
        let expanded = match expand_template_with_trace(
            &raw_value,
            |ident| self.lookup_ident(ident),
            literal_source,
        ) {
            Ok(value) => value,
            Err(err) => {
                self.stack.pop();
                return Err(err);
            }
        };
        self.stack.pop();

        self.resolved.insert(key.to_string(), expanded.clone());
        Ok(expanded)
    }

    fn lookup_ident(&mut self, ident: &str) -> VarsResult<Option<TracedValue>> {
        if let Some(value) = external_value_with_source(ident, self.ctx) {
            return Ok(Some(value));
        }
        if self.raw.contains_key(ident) {
            return self.resolve_key(ident).map(Some);
        }
        Ok(None)
    }
}

fn expand_template<F>(input: &str, mut resolve_ident: F) -> VarsResult<String>
where
    F: FnMut(&str) -> VarsResult<Option<String>>,
{
    let mut out = String::with_capacity(input.len());
    for part in parse_template_parts(input)? {
        match part {
            TemplatePart::Literal(fragment) => out.push_str(fragment),
            TemplatePart::Var { ident, default } => {
                if let Some(value) = resolve_ident(ident)? {
                    out.push_str(&value);
                } else if let Some(default) = default {
                    out.push_str(default);
                } else {
                    return VarsReason::Template
                        .to_err()
                        .with_detail(format!(
                            "undefined variable '{}' in configuration value {:?}",
                            ident, input
                        ))
                        .err();
                }
            }
        }
    }
    Ok(out)
}

fn expand_template_with_trace<F>(
    input: &str,
    mut resolve_ident: F,
    literal_source: Option<SourceAtom>,
) -> VarsResult<TracedValue>
where
    F: FnMut(&str) -> VarsResult<Option<TracedValue>>,
{
    let mut traced = TracedValue::new(String::with_capacity(input.len()));
    let mut used_literal = false;

    for part in parse_template_parts(input)? {
        match part {
            TemplatePart::Literal(fragment) => {
                traced.value.push_str(fragment);
                used_literal = true;
            }
            TemplatePart::Var { ident, default } => {
                if let Some(value) = resolve_ident(ident)? {
                    traced.value.push_str(&value.value);
                    traced.sources.extend(value.sources);
                } else if let Some(default) = default {
                    traced.value.push_str(default);
                    traced
                        .sources
                        .insert(SourceAtom::Default(ident.to_string()));
                } else {
                    return VarsReason::Template
                        .to_err()
                        .with_detail(format!(
                            "undefined variable '{}' in configuration value {:?}",
                            ident, input
                        ))
                        .err();
                }
            }
        }
    }

    if used_literal && let Some(source) = literal_source {
        traced.sources.insert(source);
    }

    Ok(traced)
}

#[derive(Debug, Clone, Copy)]
enum TemplatePart<'a> {
    Literal(&'a str),
    Var {
        ident: &'a str,
        default: Option<&'a str>,
    },
}

fn parse_template_parts(input: &str) -> VarsResult<Vec<TemplatePart<'_>>> {
    let bytes = input.as_bytes();
    let len = bytes.len();
    let mut parts = Vec::new();
    let mut literal_start = 0usize;
    let mut i = 0usize;

    while i < len {
        if bytes[i] != b'$' {
            let ch = input[i..].chars().next().unwrap();
            i += ch.len_utf8();
            continue;
        }

        if literal_start < i {
            parts.push(TemplatePart::Literal(&input[literal_start..i]));
        }

        let dollar_pos = i;
        i += 1;

        if i < len && bytes[i] == b'$' {
            parts.push(TemplatePart::Literal("$"));
            i += 1;
            literal_start = i;
            continue;
        }

        if i < len && bytes[i] == b'{' {
            i += 1;
            let ident_start = i;
            if i >= len || !is_ident_start(bytes[i]) {
                return VarsReason::Template
                    .to_err()
                    .with_detail(format!("expected variable name after '${{' in {:?}", input))
                    .err();
            }
            while i < len && is_ident_cont(bytes[i]) {
                i += 1;
            }
            let ident = &input[ident_start..i];

            let default = if i < len && bytes[i] == b':' {
                i += 1;
                let default_start = i;
                while i < len && bytes[i] != b'}' {
                    i += 1;
                }
                Some(&input[default_start..i])
            } else {
                None
            };

            if i >= len || bytes[i] != b'}' {
                return VarsReason::Template
                    .to_err()
                    .with_detail(format!(
                        "unterminated variable reference starting at byte {} in {:?}",
                        dollar_pos, input
                    ))
                    .err();
            }
            i += 1;
            parts.push(TemplatePart::Var { ident, default });
            literal_start = i;
            continue;
        }

        if i < len && is_ident_start(bytes[i]) {
            let ident_start = i;
            while i < len && is_ident_cont(bytes[i]) {
                i += 1;
            }
            let ident = &input[ident_start..i];
            parts.push(TemplatePart::Var {
                ident,
                default: None,
            });
            literal_start = i;
            continue;
        }

        parts.push(TemplatePart::Literal("$"));
        literal_start = i;
    }

    if literal_start < len {
        parts.push(TemplatePart::Literal(&input[literal_start..]));
    } else if input.is_empty() {
        parts.push(TemplatePart::Literal(""));
    }

    Ok(parts)
}

fn join_path(parent: Option<&str>, key: &str) -> String {
    match parent {
        Some(parent) if !parent.is_empty() => format!("{parent}.{key}"),
        _ => key.to_string(),
    }
}

fn join_indexed_path(parent: Option<&str>, idx: usize) -> String {
    match parent {
        Some(parent) if !parent.is_empty() => format!("{parent}[{idx}]"),
        _ => format!("[{idx}]"),
    }
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_expand_and_resolve_apis_return_plain_values() {
        let value: TomlValue = toml::from_str(
            r#"
sinks = "${CASE_PATH}/sinks"

[vars]
CASE_PATH = "/tmp/from-file"
"#,
        )
        .expect("parse value");
        let ctx = ConfigVarContext::new();

        let vars = resolve_value_vars(&value, &ctx).expect("resolve plain vars");
        assert_eq!(
            vars.get("CASE_PATH").map(String::as_str),
            Some("/tmp/from-file")
        );

        let expanded = expand_value(&value, &ctx).expect("expand plain value");
        assert_eq!(
            expanded.get("sinks").and_then(TomlValue::as_str),
            Some("/tmp/from-file/sinks")
        );
    }

    #[test]
    fn public_expand_and_resolve_apis_report_sources() {
        let value: TomlValue = toml::from_str(
            r#"
mode = "batch"
sinks = "${CASE_PATH}/sinks"

[[sources]]
type = "file"
path = "${CASE_PATH}/data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "${CASE_PATH}/schemas/base/*.wfs"
rules = "${CASE_PATH}/rules/base/*.wfl"

[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.base_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"
"#,
        )
        .expect("parse value");

        let mut explicit = HashMap::new();
        explicit.insert("CASE_PATH".to_string(), "/tmp/from-cli".to_string());
        let ctx = ConfigVarContext::from_explicit_vars(explicit);

        let vars = resolve_value_vars_with_sources(&value, &ctx, |_| None).expect("resolve vars");
        assert_eq!(
            vars.get("CASE_PATH").expect("case path").rendered_sources(),
            "<cli:CASE_PATH>"
        );

        let expanded = expand_value_with_sources(&value, &ctx, |_| None).expect("expand value");
        assert_eq!(
            expanded
                .rendered_source_for("sources")
                .expect("sources provenance"),
            "<cli:CASE_PATH>"
        );
        assert_eq!(
            expanded
                .value
                .get("sinks")
                .and_then(TomlValue::as_str)
                .expect("sinks value"),
            "/tmp/from-cli/sinks"
        );
    }
}
