use crate::ast::{Expr, FieldRef};
use crate::schema::BaseType;

use super::infer::infer_type;
use super::{ValType, compatible, is_numeric, is_orderable};
use crate::checker::scope::Scope;
use crate::checker::{CheckError, Severity};

pub fn check_func_call(
    name: &str,
    args: &[Expr],
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    match name {
        "count" => {
            // T4: argument should be a set-level reference (bare alias), not a field projection
            if let Some(Expr::Field(FieldRef::Qualified(..)))
            | Some(Expr::Field(FieldRef::Bracketed(..))) = args.first()
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "count() expects a set-level argument (alias), not a field projection"
                        .to_string(),
                });
            }
        }
        "sum" | "avg" => {
            // T1: field must be digit or float
            if let Some(arg) = args.first()
                && let Some(t) = infer_type(arg, scope)
                && !is_numeric(&t)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("{}() requires a numeric field, got {:?}", name, t),
                });
            }
        }
        "min" | "max" => {
            // T2: field must be orderable
            if let Some(arg) = args.first()
                && let Some(t) = infer_type(arg, scope)
                && !is_orderable(&t)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("{}() requires an orderable field, got {:?}", name, t),
                });
            }
        }
        "has" => {
            // T11-T13: window.has() checks
            if args.is_empty() || args.len() > 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "has() expects 1 or 2 arguments".to_string(),
                });
            }
            // T12: second argument must be a string literal
            if args.len() == 2 && !matches!(args[1], Expr::StringLit(_)) {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "has() second argument must be a string literal (field name)"
                        .to_string(),
                });
            }
        }
        "baseline" => {
            // T26: baseline(expr, dur) or baseline(expr, dur, method)
            if args.len() != 2 && args.len() != 3 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "baseline() requires 2 or 3 arguments: (expr, duration, [method])"
                        .to_string(),
                });
            } else {
                // First argument must be numeric
                if let Some(t) = infer_type(&args[0], scope)
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("baseline() first argument must be numeric, got {:?}", t),
                    });
                }
                // Second argument must be a positive number (duration in seconds)
                match &args[1] {
                    Expr::Number(n) if *n > 0.0 => {} // OK
                    _ => {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: "baseline() second argument must be a positive duration"
                                .to_string(),
                        });
                    }
                }
                // Third argument (if present) must be a string literal: "mean", "ewma", or "median"
                if args.len() == 3 {
                    match &args[2] {
                        Expr::StringLit(method) => {
                            let valid_methods = ["mean", "ewma", "median"];
                            if !valid_methods.contains(&method.as_str()) {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(rule_name.to_string()),
                                    test: None,
                                    message: format!(
                                        "baseline() method must be one of: mean, ewma, median, got '{}'",
                                        method
                                    ),
                                });
                            }
                        }
                        _ => {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: "baseline() method must be a string literal: \"mean\", \"ewma\", or \"median\""
                                    .to_string(),
                            });
                        }
                    }
                }
            }
        }
        "regex_match" => {
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "regex_match() requires exactly 2 arguments: (field, pattern)"
                        .to_string(),
                });
            } else {
                // First argument should be Chars
                if let Some(t) = infer_type(&args[0], scope)
                    && !compatible(&t, &ValType::Base(BaseType::Chars))
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("regex_match() first argument must be chars, got {:?}", t),
                    });
                }
                // Second argument should be a string literal (compile-time regex check)
                match &args[1] {
                    Expr::StringLit(pat) => {
                        if regex_syntax::Parser::new().parse(pat).is_err() {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: format!(
                                    "regex_match() pattern \"{}\" is not valid regex",
                                    pat
                                ),
                            });
                        }
                    }
                    _ => {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message:
                                "regex_match() second argument must be a string literal pattern"
                                    .to_string(),
                        });
                    }
                }
            }
        }
        "time_diff" => {
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "time_diff() requires exactly 2 arguments: (t1, t2)".to_string(),
                });
            } else {
                for (i, arg) in args.iter().enumerate() {
                    if let Some(t) = infer_type(arg, scope)
                        && !compatible(&t, &ValType::Base(BaseType::Time))
                        && !is_numeric(&t)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "time_diff() argument {} must be time or numeric, got {:?}",
                                i + 1,
                                t
                            ),
                        });
                    }
                }
            }
        }
        "time_bucket" => {
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "time_bucket() requires exactly 2 arguments: (time, interval_seconds)"
                        .to_string(),
                });
            } else {
                // First argument must be time or numeric
                if let Some(t) = infer_type(&args[0], scope)
                    && !compatible(&t, &ValType::Base(BaseType::Time))
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "time_bucket() first argument must be time or numeric, got {:?}",
                            t
                        ),
                    });
                }
                // Second argument must be numeric (duration in seconds)
                if let Some(t) = infer_type(&args[1], scope)
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "time_bucket() second argument must be numeric (interval seconds), got {:?}",
                            t
                        ),
                    });
                }
            }
        }
        "contains" => {
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "contains() requires exactly 2 arguments: (haystack, needle)"
                        .to_string(),
                });
            } else {
                for (i, arg) in args.iter().enumerate() {
                    if let Some(t) = infer_type(arg, scope)
                        && !compatible(&t, &ValType::Base(BaseType::Chars))
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "contains() argument {} must be chars, got {:?}",
                                i + 1,
                                t
                            ),
                        });
                    }
                }
            }
        }
        "lower" | "upper" => {
            if args.len() != 1 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("{}() requires exactly 1 argument", name),
                });
            } else if let Some(t) = infer_type(&args[0], scope)
                && !compatible(&t, &ValType::Base(BaseType::Chars))
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("{}() argument must be chars, got {:?}", name, t),
                });
            }
        }
        "len" => {
            if args.len() != 1 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "len() requires exactly 1 argument".to_string(),
                });
            } else if let Some(t) = infer_type(&args[0], scope)
                && !compatible(&t, &ValType::Base(BaseType::Chars))
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("len() argument must be chars, got {:?}", t),
                });
            }
        }
        // L3 Collection functions (M28.2)
        "collect_set" | "collect_list" => {
            // T22: argument must be Column projection (alias.field)
            if args.len() != 1 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("{}() requires exactly 1 argument: alias.field", name),
                });
            } else if !matches!(args[0], Expr::Field(FieldRef::Qualified(..)) | Expr::Field(FieldRef::Bracketed(..)))
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "{}() argument must be a column projection (alias.field)",
                        name
                    ),
                });
            }
        }
        "first" | "last" => {
            // T23: argument must be Column projection (alias.field)
            if args.len() != 1 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("{}() requires exactly 1 argument: alias.field", name),
                });
            } else if !matches!(args[0], Expr::Field(FieldRef::Qualified(..)) | Expr::Field(FieldRef::Bracketed(..)))
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "{}() argument must be a column projection (alias.field)",
                        name
                    ),
                });
            }
        }
        // L3 Statistical functions (M28.3)
        "stddev" => {
            // T24: field must be digit or float
            if args.len() != 1 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "stddev() requires exactly 1 argument: alias.field".to_string(),
                });
            } else if let Some(arg) = args.first() {
                if let Some(t) = infer_type(arg, scope) && !is_numeric(&t) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("stddev() requires a numeric field, got {:?}", t),
                    });
                }
                // Also check it's a column projection
                if !matches!(args[0], Expr::Field(FieldRef::Qualified(..)) | Expr::Field(FieldRef::Bracketed(..)))
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: "stddev() argument must be a column projection (alias.field)"
                            .to_string(),
                    });
                }
            }
        }
        "percentile" => {
            // T25: percentile(field, p) where field is numeric, p is 0-100
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "percentile() requires exactly 2 arguments: (field, p)".to_string(),
                });
            } else {
                // First arg must be numeric column
                if let Some(t) = infer_type(&args[0], scope) && !is_numeric(&t) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("percentile() field must be numeric, got {:?}", t),
                    });
                }
                if !matches!(args[0], Expr::Field(FieldRef::Qualified(..)) | Expr::Field(FieldRef::Bracketed(..)))
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: "percentile() field must be a column projection (alias.field)"
                            .to_string(),
                    });
                }
                // Second arg must be digit literal 0-100
                match &args[1] {
                    Expr::Number(p) if *p >= 0.0 && *p <= 100.0 => {} // OK
                    _ => {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: "percentile() p must be a number literal 0-100".to_string(),
                        });
                    }
                }
            }
        }
        _ => {}
    }
}
