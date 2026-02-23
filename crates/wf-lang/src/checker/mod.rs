mod contracts;
pub mod lint;
mod rules;
mod scope;
mod types;

use crate::ast::WflFile;
use crate::schema::WindowSchema;

/// Severity level for semantic check diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

/// Semantic check error with optional rule/contract context.
#[derive(Debug, Clone)]
pub struct CheckError {
    pub severity: Severity,
    pub rule: Option<String>,
    pub contract: Option<String>,
    pub message: String,
}

impl std::fmt::Display for CheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
        };
        match (&self.rule, &self.contract) {
            (Some(r), _) => write!(f, "{}: rule `{}`: {}", prefix, r, self.message),
            (_, Some(c)) => write!(f, "{}: contract `{}`: {}", prefix, c, self.message),
            _ => write!(f, "{}: {}", prefix, self.message),
        }
    }
}

/// Perform L1 semantic checks on a parsed WflFile against the given window schemas.
/// Returns an empty Vec when all checks pass.
pub fn check_wfl(file: &WflFile, schemas: &[WindowSchema]) -> Vec<CheckError> {
    let mut errors = Vec::new();

    for rule in &file.rules {
        rules::check_rule(rule, schemas, &mut errors);
    }

    contracts::check_contracts(file, &mut errors);

    rules::yield_version::check_yield_versions(file, &mut errors);

    errors
}

#[cfg(test)]
mod tests;
