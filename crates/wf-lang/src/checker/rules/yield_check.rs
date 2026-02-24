use crate::ast::RuleDecl;
use crate::schema::WindowSchema;

use crate::checker::scope::{self, Scope};
use crate::checker::types::{check_expr_type, compatible, infer_type};
use crate::checker::{CheckError, Severity};

use super::SYSTEM_FIELDS;

pub fn check_yield(
    rule: &RuleDecl,
    schemas: &[WindowSchema],
    scope: &Scope<'_>,
    errors: &mut Vec<CheckError>,
) {
    let name = &rule.name;
    let yc = &rule.yield_clause;

    // Y1: target window must exist
    let target_schema = schemas.iter().find(|s| s.name == yc.target);

    // T51: yield version must match meta.contract_version
    if let Some(yield_ver) = yc.version {
        let meta_ver = rule.meta.as_ref().and_then(|m| {
            m.entries
                .iter()
                .find(|e| e.key == "contract_version")
                .and_then(|e| e.value.parse::<u32>().ok())
        });
        match meta_ver {
            Some(mv) if mv == yield_ver => {} // OK
            Some(mv) => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(name.to_string()),
                    test: None,
                    message: format!(
                        "yield version @v{} does not match meta contract_version = {}",
                        yield_ver, mv
                    ),
                });
            }
            None => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(name.to_string()),
                    test: None,
                    message: format!(
                        "yield specifies @v{} but no contract_version in meta block",
                        yield_ver
                    ),
                });
            }
        }
    }

    match target_schema {
        None => {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(name.to_string()),
                test: None,
                message: format!("yield target window `{}` does not exist", yc.target),
            });
        }
        Some(ws) => {
            // Y1: target window must be an output window (stream is empty)
            if !ws.streams.is_empty() {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(name.to_string()),
                    test: None,
                    message: format!(
                        "yield target `{}` has stream subscriptions; it must be an output-only window",
                        yc.target
                    ),
                });
            }

            for arg in &yc.args {
                // T36/Y8: no system fields in yield arguments
                if SYSTEM_FIELDS.contains(&arg.name.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(name.to_string()),
                        test: None,
                        message: format!(
                            "yield argument `{}` is a system field and cannot be manually assigned",
                            arg.name
                        ),
                    });
                    continue;
                }

                // Y2/Y3: argument name must be a field in the target window
                let target_field = ws.fields.iter().find(|f| f.name == arg.name);
                match target_field {
                    None => {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(name.to_string()),
                            test: None,
                            message: format!(
                                "yield argument `{}` is not a field in target window `{}`",
                                arg.name, yc.target
                            ),
                        });
                    }
                    Some(fd) => {
                        // T10: type must match
                        check_expr_type(&arg.value, scope, name, errors);
                        if let Some(val_type) = infer_type(&arg.value, scope) {
                            let expected = scope::field_type_to_val(&fd.field_type);
                            if !compatible(&expected, &val_type) {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(name.to_string()),
                                    test: None,
                                    message: format!(
                                        "yield argument `{}` type mismatch: expected {:?}, got {:?}",
                                        arg.name, expected, val_type
                                    ),
                                });
                            }
                        }
                    }
                }
            }
        }
    }
}
