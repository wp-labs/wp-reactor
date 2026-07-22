use std::collections::{HashMap, HashSet};

use crate::ast::{NamedArg, RuleDecl, YieldClause, YieldPresetDecl};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum YieldPresetError {
    DuplicatePreset(String),
    UnknownPreset(String),
    DuplicatePresetRef(String),
}

impl YieldPresetError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::DuplicatePreset(name) => format!("duplicate yield preset `{name}`"),
            Self::UnknownPreset(name) => format!("unknown yield preset `{name}`"),
            Self::DuplicatePresetRef(name) => {
                format!("yield preset `{name}` is referenced more than once")
            }
        }
    }
}

pub(crate) fn validate_yield_presets(presets: &[YieldPresetDecl]) -> Vec<YieldPresetError> {
    let mut seen = HashSet::new();
    let mut errors = Vec::new();
    for preset in presets {
        if !seen.insert(preset.name.as_str()) {
            errors.push(YieldPresetError::DuplicatePreset(preset.name.clone()));
        }
    }
    errors
}

pub(crate) fn expand_yield_args(
    presets: &[YieldPresetDecl],
    yield_clause: &YieldClause,
) -> Result<Vec<NamedArg>, YieldPresetError> {
    let preset_map = preset_map(presets);
    let mut seen_refs = HashSet::new();
    let mut merged = Vec::new();

    for name in &yield_clause.presets {
        if !seen_refs.insert(name.as_str()) {
            return Err(YieldPresetError::DuplicatePresetRef(name.clone()));
        }
        let preset = preset_map
            .get(name.as_str())
            .ok_or_else(|| YieldPresetError::UnknownPreset(name.clone()))?;
        merge_args(&mut merged, &preset.args);
    }

    merge_args(&mut merged, &yield_clause.args);
    Ok(merged)
}

pub(crate) fn expand_rule_yield_presets(
    rule: &RuleDecl,
    presets: &[YieldPresetDecl],
) -> Result<RuleDecl, YieldPresetError> {
    let mut rule = rule.clone();
    rule.yield_clause.args = expand_yield_args(presets, &rule.yield_clause)?;
    rule.yield_clause.presets.clear();
    Ok(rule)
}

fn preset_map(presets: &[YieldPresetDecl]) -> HashMap<&str, &YieldPresetDecl> {
    let mut map = HashMap::new();
    for preset in presets {
        map.entry(preset.name.as_str()).or_insert(preset);
    }
    map
}

fn merge_args(target: &mut Vec<NamedArg>, incoming: &[NamedArg]) {
    for arg in incoming {
        if let Some(existing) = target.iter_mut().find(|existing| existing.name == arg.name) {
            *existing = arg.clone();
        } else {
            target.push(arg.clone());
        }
    }
}
