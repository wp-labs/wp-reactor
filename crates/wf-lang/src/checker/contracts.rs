use crate::ast::{GivenStmt, WflFile};

use super::CheckError;

/// Check all contract blocks in a WflFile.
pub fn check_contracts(file: &WflFile, errors: &mut Vec<CheckError>) {
    for contract in &file.contracts {
        let cname = &contract.name;

        // CT1: target rule must exist in the same file
        let target_rule = file.rules.iter().find(|r| r.name == contract.rule_name);
        match target_rule {
            None => {
                errors.push(CheckError {
                    rule: None,
                    contract: Some(cname.to_string()),
                    message: format!(
                        "target rule `{}` not found in this file",
                        contract.rule_name
                    ),
                });
                // Can't check row aliases without the target rule
                continue;
            }
            Some(rule) => {
                // CT2: row alias must be declared in the target rule's events
                let event_aliases: Vec<&str> =
                    rule.events.decls.iter().map(|d| d.alias.as_str()).collect();

                for stmt in &contract.given {
                    if let GivenStmt::Row { alias, .. } = stmt
                        && !event_aliases.contains(&alias.as_str()) {
                            errors.push(CheckError {
                                rule: None,
                                contract: Some(cname.to_string()),
                                message: format!(
                                    "row alias `{}` is not declared in rule `{}`'s events",
                                    alias, contract.rule_name
                                ),
                            });
                        }
                }
            }
        }
    }
}
