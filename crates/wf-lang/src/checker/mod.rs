mod contracts;
mod intermediate;
pub mod lint;
mod rules;
mod scope;
mod types;

use crate::ast::{RuleDecl, WflFile};
use crate::schema::WindowSchema;
use crate::yield_preset::{expand_rule_yield_presets, validate_yield_presets};

/// Severity level for semantic check diagnostics.
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangChecker")]
pub enum Severity {
    Error,
    Warning,
}

/// Semantic check error with optional rule/test context.
#[derive(::moju_derive::MoJu, Debug, Clone)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangChecker")]
pub struct CheckError {
    pub severity: Severity,
    pub rule: Option<String>,
    pub test: Option<String>,
    pub message: String,
}

impl std::fmt::Display for CheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
        };
        match (&self.rule, &self.test) {
            (Some(r), _) => write!(f, "{}: rule `{}`: {}", prefix, r, self.message),
            (_, Some(t)) => write!(f, "{}: test `{}`: {}", prefix, t, self.message),
            _ => write!(f, "{}: {}", prefix, self.message),
        }
    }
}

/// Perform L1 semantic checks on a parsed WflFile against the given window schemas.
/// Returns an empty Vec when all checks pass.
pub fn check_wfl(file: &WflFile, schemas: &[WindowSchema]) -> Vec<CheckError> {
    let mut errors = Vec::new();
    let effective_schemas = intermediate::effective_schemas_for_rules(&file.rules, schemas);
    let preset_errors = validate_yield_presets(&file.yield_presets);

    for error in preset_errors {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: None,
            test: None,
            message: error.message(),
        });
    }

    for rule in &file.rules {
        match expand_rule_yield_presets(rule, &file.yield_presets) {
            Ok(rule) => rules::check_rule(&rule, &effective_schemas, &mut errors),
            Err(error) => errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule.name.clone()),
                test: None,
                message: error.message(),
            }),
        }
    }

    intermediate::check_intermediate_target_graph(&file.rules, &mut errors, None);
    contracts::check_tests(file, &mut errors);

    rules::yield_version::check_yield_versions(file, &mut errors);

    errors
}

pub fn effective_schemas_for_rules(
    rules: &[RuleDecl],
    schemas: &[WindowSchema],
) -> Vec<WindowSchema> {
    intermediate::effective_schemas_for_rules(rules, schemas)
}

pub fn check_intermediate_target_graph(rules: &[RuleDecl], errors: &mut Vec<CheckError>) {
    intermediate::check_intermediate_target_graph(rules, errors, None);
}

#[cfg(test)]
mod tests;
