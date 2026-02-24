use crate::ast::{InputStmt, WflFile};

use super::{CheckError, Severity};

/// Check all test blocks in a WflFile.
pub fn check_tests(file: &WflFile, errors: &mut Vec<CheckError>) {
    for test in &file.tests {
        let tname = &test.name;

        // CT1: target rule must exist in the same file
        let target_rule = file.rules.iter().find(|r| r.name == test.rule_name);
        match target_rule {
            None => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: None,
                    test: Some(tname.to_string()),
                    message: format!(
                        "target rule `{}` not found in this file",
                        test.rule_name
                    ),
                });
                // Can't check row aliases without the target rule
                continue;
            }
            Some(rule) => {
                // CT2: row alias must be declared in the target rule's events
                let event_aliases: Vec<&str> =
                    rule.events.decls.iter().map(|d| d.alias.as_str()).collect();

                for stmt in &test.input {
                    if let InputStmt::Row { alias, .. } = stmt
                        && !event_aliases.contains(&alias.as_str())
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: None,
                            test: Some(tname.to_string()),
                            message: format!(
                                "row alias `{}` is not declared in rule `{}`'s events",
                                alias, test.rule_name
                            ),
                        });
                    }
                }
            }
        }
    }
}
