use std::collections::HashSet;

use crate::ast::RuleDecl;
use crate::schema::WindowSchema;

use crate::checker::scope::Scope;
use crate::checker::types::check_expr_type;
use crate::checker::{CheckError, Severity};

pub fn build_scope<'a>(
    rule: &'a RuleDecl,
    schemas: &'a [WindowSchema],
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) -> Scope<'a> {
    let mut scope = Scope::new();
    let mut seen_aliases = HashSet::new();

    for decl in &rule.events.decls {
        // EV1: alias uniqueness
        if !seen_aliases.insert(decl.alias.as_str()) {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!("duplicate event alias `{}`", decl.alias),
            });
        }

        // EV2: window must exist in schemas
        match schemas.iter().find(|s| s.name == decl.window) {
            Some(ws) => {
                scope.aliases.insert(&decl.alias, ws);
            }
            None => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "event alias `{}` references unknown window `{}`",
                        decl.alias, decl.window
                    ),
                });
            }
        }

        // Check filter expression if present
        if let Some(ref filter) = decl.filter {
            check_expr_type(filter, &scope, rule_name, errors);
        }
    }

    scope
}
