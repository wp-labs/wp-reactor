use crate::ast::{FieldRef, RuleDecl};

use crate::checker::scope::{self, Scope};
use crate::checker::types::{ValType, compatible};
use crate::checker::{CheckError, Severity};

pub fn check_match_keys(
    rule: &RuleDecl,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    for key in &rule.match_clause.keys {
        match key {
            FieldRef::Simple(field) => {
                // K1: unqualified key must exist in ALL event sources
                for (alias, schema) in &scope.aliases {
                    if !schema.fields.iter().any(|f| f.name == *field) {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!(
                                "match key `{}` not found in event source `{}` (window `{}`)",
                                field, alias, schema.name
                            ),
                        });
                    }
                }
                // K4: types must be consistent across sources
                check_key_type_consistency(field, scope, rule_name, errors);
            }
            FieldRef::Qualified(alias, field) => {
                // K2: qualified key
                if !scope.aliases.contains_key(alias.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "match key `{}.{}` references unknown alias `{}`",
                            alias, field, alias
                        ),
                    });
                } else if !scope.alias_has_field(alias, field) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "match key `{}.{}`: field `{}` not found in window",
                            alias, field, field
                        ),
                    });
                }
            }
            FieldRef::Bracketed(alias, key) => {
                if !scope.aliases.contains_key(alias.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "match key `{}[\"{}\"]` references unknown alias `{}`",
                            alias, key, alias
                        ),
                    });
                } else if !scope.alias_has_field(alias, key) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "match key `{}[\"{}\"]`: field `{}` not found in window",
                            alias, key, key
                        ),
                    });
                }
            }
        }
    }
}

/// K4: check that a simple key field has the same type across all event sources.
fn check_key_type_consistency(
    field: &str,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let mut found_type: Option<(ValType, String)> = None;
    for (alias, schema) in &scope.aliases {
        if let Some(fd) = schema.fields.iter().find(|f| f.name == field) {
            let vt = scope::field_type_to_val(&fd.field_type);
            if let Some((ref prev_type, ref prev_alias)) = found_type {
                if !compatible(prev_type, &vt) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "match key `{}` type mismatch: {:?} in `{}` vs {:?} in `{}`",
                            field, prev_type, prev_alias, vt, alias
                        ),
                    });
                }
            } else {
                found_type = Some((vt, alias.to_string()));
            }
        }
    }
}

pub fn check_key_mapping(
    rule: &RuleDecl,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let mapping = match &rule.match_clause.key_mapping {
        Some(m) => m,
        None => return,
    };

    // K4: source field alias must exist in events, field must exist
    for item in mapping {
        match &item.source_field {
            FieldRef::Qualified(alias, field) => {
                if !scope.aliases.contains_key(alias.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "key mapping `{} = {}.{}`: alias `{}` not declared in events",
                            item.logical_name, alias, field, alias
                        ),
                    });
                } else if !scope.alias_has_field(alias, field) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "key mapping `{} = {}.{}`: field `{}` not found in window",
                            item.logical_name, alias, field, field
                        ),
                    });
                }
            }
            _ => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!(
                        "key mapping `{}`: source field must be qualified (alias.field)",
                        item.logical_name
                    ),
                });
            }
        }
    }

    // K4: check type consistency for same logical key name across sources
    let mut logical_types: std::collections::HashMap<&str, (ValType, String)> =
        std::collections::HashMap::new();
    for item in mapping {
        if let FieldRef::Qualified(alias, field) = &item.source_field
            && scope.aliases.contains_key(alias.as_str())
            && let Some(vt) = scope.get_field_type_for_alias(alias, field)
        {
            if let Some((prev_type, prev_source)) = logical_types.get(item.logical_name.as_str()) {
                if !compatible(prev_type, &vt) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "key mapping `{}` type mismatch: {:?} (from {}) vs {:?} (from {}.{})",
                            item.logical_name, prev_type, prev_source, vt, alias, field
                        ),
                    });
                }
            } else {
                logical_types.insert(&item.logical_name, (vt, format!("{}.{}", alias, field)));
            }
        }
    }
}
