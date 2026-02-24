use std::collections::HashSet;

use crate::ast::{FieldSelector, MatchStep};

use crate::checker::scope::Scope;
use crate::checker::types::check_pipe_chain;
use crate::checker::{CheckError, Severity};

pub fn check_match_steps<'a>(
    steps: &'a [MatchStep],
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
    labels_seen: &mut HashSet<&'a str>,
) {
    for step in steps {
        for branch in &step.branches {
            // R5: source must be a declared alias
            if !scope.aliases.contains_key(branch.source.as_str()) {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!(
                        "match step source `{}` is not a declared event alias",
                        branch.source
                    ),
                });
            }

            // R1: label uniqueness within this match block
            if let Some(ref label) = branch.label
                && !labels_seen.insert(label.as_str())
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!("duplicate step label `{}`", label),
                });
            }

            // Validate field selector resolves against source's window
            if let Some(ref fs) = branch.field {
                let field_name = match fs {
                    FieldSelector::Dot(n) | FieldSelector::Bracket(n) => n.as_str(),
                };
                if scope.aliases.contains_key(branch.source.as_str())
                    && !scope.alias_has_field(&branch.source, field_name)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "field `{}` not found in source `{}`'s window",
                            field_name, branch.source
                        ),
                    });
                }
            }

            // Type-check the pipe chain
            check_pipe_chain(branch, scope, rule_name, errors);
        }
    }
}
