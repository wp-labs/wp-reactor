use crate::ast::{FieldRef, JoinMode};
use crate::schema::WindowSchema;

use crate::checker::scope::Scope;
use crate::checker::{CheckError, Severity};

pub fn check_joins_list(
    joins: &[crate::ast::JoinClause],
    schemas: &[WindowSchema],
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    for join in joins {
        // Target window must exist in schemas
        let target = schemas.iter().find(|s| s.name == join.target_window);
        match target {
            None => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "join target window `{}` does not exist in schemas",
                        join.target_window
                    ),
                });
            }
            Some(target_schema) => {
                // Validate conditions
                for cond in &join.conditions {
                    // Left side must resolve in scope
                    if let Err(msg) = scope.resolve_field_ref(&cond.left) {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!("join condition left side: {}", msg),
                        });
                    }

                    // Right side must be qualified with target window name
                    match &cond.right {
                        FieldRef::Qualified(qualifier, field) => {
                            if qualifier != &join.target_window {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(rule_name.to_string()),
                                    test: None,
                                    message: format!(
                                        "join condition right side `{}.{}` must be qualified with target window `{}`",
                                        qualifier, field, join.target_window
                                    ),
                                });
                            } else if !target_schema.fields.iter().any(|f| f.name == *field) {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(rule_name.to_string()),
                                    test: None,
                                    message: format!(
                                        "join condition: field `{}` not found in window `{}`",
                                        field, join.target_window
                                    ),
                                });
                            }
                        }
                        _ => {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: format!(
                                    "join condition right side must be qualified with window name (e.g. `{}.field`)",
                                    join.target_window
                                ),
                            });
                        }
                    }
                }

                // T49: asof mode requires time field on right table
                if let JoinMode::Asof { within } = &join.mode {
                    if target_schema.time_field.is_none() {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "join `{}` uses asof mode but target window has no time field",
                                join.target_window
                            ),
                        });
                    }
                    if let Some(dur) = within
                        && dur.is_zero()
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "join `{}` asof within must be > 0",
                                join.target_window
                            ),
                        });
                    }
                }
            }
        }
    }
}
