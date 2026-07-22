use crate::ast::{CloseMode, Expr, RuleDecl};
use crate::schema::WindowSchema;

use crate::checker::scope::{self, Scope};
use crate::checker::types::{check_expr_type_with_system_vars, infer_type, yield_assignable};
use crate::checker::{CheckError, Severity};

use super::WFU_PREFIX;

pub fn check_yield(
    rule: &RuleDecl,
    schemas: &[WindowSchema],
    scope: &Scope<'_>,
    errors: &mut Vec<CheckError>,
) {
    let name = &rule.name;
    let yc = &rule.yield_clause;

    if rule
        .match_clause
        .on_close
        .as_ref()
        .is_some_and(|close| close.mode == CloseMode::Or)
    {
        for arg in &yc.args {
            if contains_stat_final_selector(&arg.value) {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(name.to_string()),
                    test: None,
                    message: format!(
                        "yield argument `{}` uses stat.value(final(...)) with `on close`; use `and close` so the final close label is available for every output path",
                        arg.name
                    ),
                });
            }
        }
    }

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
                // T36/Y8: no wfusion-managed fields in yield arguments.
                if arg.name.starts_with(WFU_PREFIX) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(name.to_string()),
                        test: None,
                        message: format!(
                            "yield argument `{}` uses reserved prefix `{}`",
                            arg.name, WFU_PREFIX
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
                        check_expr_type_with_system_vars(&arg.value, scope, name, errors);
                        if let Some(val_type) = infer_type(&arg.value, scope) {
                            let expected = scope::field_type_to_val(&fd.field_type);
                            if !yield_assignable(&expected, &val_type) {
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

fn contains_stat_final_selector(expr: &Expr) -> bool {
    match expr {
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => {
            let is_stat_final_value = qualifier.as_deref() == Some("stat")
                && name == "value"
                && args.first().is_some_and(is_final_selector);
            is_stat_final_value || args.iter().any(contains_stat_final_selector)
        }
        Expr::BinOp { left, right, .. } => {
            contains_stat_final_selector(left) || contains_stat_final_selector(right)
        }
        Expr::Neg(inner) => contains_stat_final_selector(inner),
        Expr::Object(items) => items
            .iter()
            .any(|item| contains_stat_final_selector(&item.value)),
        Expr::Array(items) => items.iter().any(contains_stat_final_selector),
        Expr::InList { expr, list, .. } => {
            contains_stat_final_selector(expr) || list.iter().any(contains_stat_final_selector)
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            contains_stat_final_selector(cond)
                || contains_stat_final_selector(then_expr)
                || contains_stat_final_selector(else_expr)
        }
        _ => false,
    }
}

fn is_final_selector(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::FuncCall {
            qualifier: None,
            name,
            ..
        } if name == "final"
    )
}
