use crate::ast::{Expr, FieldRef, Measure};
use crate::schema::BaseType;

use super::infer::infer_type;
use super::{ValType, compatible, is_numeric, is_orderable, unify_array_element_type};
use crate::checker::scope::{Scope, StatLabelStage};
use crate::checker::{CheckError, Severity};

fn is_scalar_stringable_type(t: &ValType) -> bool {
    matches!(
        t,
        ValType::Base(
            BaseType::Chars
                | BaseType::Digit
                | BaseType::Float
                | BaseType::Bool
                | BaseType::Time
                | BaseType::Ip
                | BaseType::Hex
        ) | ValType::Bool
            | ValType::Numeric
    )
}

fn is_array_like(t: &ValType) -> bool {
    matches!(
        t,
        ValType::Array(_) | ValType::ArrayAny | ValType::EmptyArray
    )
}

fn check_join_values(
    name: &str,
    args: &[Expr],
    start: usize,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    for (i, arg) in args.iter().enumerate().skip(start) {
        if let Some(t) = infer_type(arg, scope)
            && !is_scalar_stringable_type(&t)
        {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!("{}() argument {} must be scalar, got {:?}", name, i + 1, t),
            });
        }
    }
}

fn unify_mvappend_element_type(existing: &ValType, incoming: &ValType) -> Option<ValType> {
    match (existing, incoming) {
        (ValType::ArrayAny, _) | (_, ValType::ArrayAny) => Some(ValType::ArrayAny),
        (ValType::Base(left), ValType::Base(right)) => {
            unify_array_element_type(left, right).map(ValType::Base)
        }
        _ if compatible(existing, incoming) => Some(existing.clone()),
        _ => None,
    }
}

pub(super) struct FuncCheckCtx<'a, 'scope> {
    pub scope: &'a Scope<'scope>,
    pub rule_name: &'a str,
    pub allow_l3_funcs: bool,
    pub allow_mixed_coalesce: bool,
}

pub fn check_func_call(
    qualifier: Option<&str>,
    name: &str,
    args: &[Expr],
    ctx: FuncCheckCtx<'_, '_>,
    errors: &mut Vec<CheckError>,
) {
    let scope = ctx.scope;
    let rule_name = ctx.rule_name;
    let allow_l3_funcs = ctx.allow_l3_funcs;
    let allow_mixed_coalesce = ctx.allow_mixed_coalesce;

    if qualifier == Some("stat") && matches!(name, "count" | "value") {
        check_stat_func(name, args, scope, rule_name, errors);
        return;
    }
    if qualifier.is_none() && is_stat_selector_name(name) {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "stat selector `{name}(...)` can only be used inside stat.count(...) or stat.value(...)"
            ),
        });
        return;
    }

    if !allow_l3_funcs
        && matches!(
            name,
            "collect_set" | "collect_list" | "first" | "last" | "stddev" | "percentile"
        )
    {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "{}() is not allowed in guard expressions; use it in score/entity/yield instead",
                name
            ),
        });
        return;
    }

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
            if let Some(Expr::Field(FieldRef::Simple(alias_name))) = args.first()
                && matches!(
                    scope.resolve_field_ref(&FieldRef::Simple(alias_name.clone())),
                    Ok(None)
                )
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "{}() requires a field projection like alias.field; set-level alias `{}` is not allowed",
                        name,
                        alias_name
                    ),
                });
            }
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
            if let Some(Expr::Field(FieldRef::Simple(alias_name))) = args.first()
                && matches!(
                    scope.resolve_field_ref(&FieldRef::Simple(alias_name.clone())),
                    Ok(None)
                )
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "{}() requires a field projection like alias.field; set-level alias `{}` is not allowed",
                        name,
                        alias_name
                    ),
                });
            }
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
        "abs" | "ceil" | "floor" => {
            if args.len() != 1 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("{}() requires exactly 1 numeric argument", name),
                });
            } else if let Some(t) = infer_type(&args[0], scope)
                && !is_numeric(&t)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("{}() argument must be numeric, got {:?}", name, t),
                });
            }
        }
        "round" => {
            if args.len() != 1 && args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "round() requires 1 or 2 arguments: (value, [precision])".to_string(),
                });
            } else {
                if let Some(t) = infer_type(&args[0], scope)
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("round() first argument must be numeric, got {:?}", t),
                    });
                }
                if args.len() == 2
                    && let Some(t) = infer_type(&args[1], scope)
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("round() second argument must be numeric, got {:?}", t),
                    });
                }
            }
        }
        "sqrt" | "exp" | "sign" | "trunc" | "is_finite" => {
            if args.len() != 1 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("{}() requires exactly 1 numeric argument", name),
                });
            } else if let Some(t) = infer_type(&args[0], scope)
                && !is_numeric(&t)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("{}() argument must be numeric, got {:?}", name, t),
                });
            }
        }
        "pow" => {
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "pow() requires exactly 2 numeric arguments: (x, y)".to_string(),
                });
            } else {
                for (i, arg) in args.iter().enumerate() {
                    if let Some(t) = infer_type(arg, scope)
                        && !is_numeric(&t)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "pow() argument {} must be numeric, got {:?}",
                                i + 1,
                                t
                            ),
                        });
                    }
                }
            }
        }
        "log" => {
            if args.len() != 1 && args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "log() requires 1 or 2 numeric arguments: (x, [base])".to_string(),
                });
            } else {
                for (i, arg) in args.iter().enumerate() {
                    if let Some(t) = infer_type(arg, scope)
                        && !is_numeric(&t)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "log() argument {} must be numeric, got {:?}",
                                i + 1,
                                t
                            ),
                        });
                    }
                }
            }
        }
        "clamp" => {
            if args.len() != 3 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "clamp() requires exactly 3 numeric arguments: (x, min, max)"
                        .to_string(),
                });
            } else {
                for (i, arg) in args.iter().enumerate() {
                    if let Some(t) = infer_type(arg, scope)
                        && !is_numeric(&t)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "clamp() argument {} must be numeric, got {:?}",
                                i + 1,
                                t
                            ),
                        });
                    }
                }
            }
        }
        "coalesce" => {
            if args.is_empty() {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "coalesce() requires at least 1 argument".to_string(),
                });
            } else if !allow_mixed_coalesce {
                let mut first_type: Option<ValType> = None;
                for (idx, arg) in args.iter().enumerate() {
                    let Some(inferred) = infer_type(arg, scope) else {
                        continue;
                    };
                    if let Some(existing) = &first_type {
                        if !(compatible(existing, &inferred)
                            || is_numeric(existing) && is_numeric(&inferred))
                        {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: format!(
                                    "coalesce() argument {} type {:?} is not compatible with {:?}",
                                    idx + 1,
                                    inferred,
                                    existing
                                ),
                            });
                        }
                    } else {
                        first_type = Some(inferred);
                    }
                }
            }
        }
        "merge" => {
            if args.is_empty() {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "merge() requires at least 1 argument".to_string(),
                });
            } else {
                for (idx, arg) in args.iter().enumerate() {
                    if let Some(t) = infer_type(arg, scope)
                        && !compatible(&ValType::Object, &t)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "merge() argument {} must be object, got {:?}",
                                idx + 1,
                                t
                            ),
                        });
                    }
                }
            }
        }
        "isnull" | "isnotnull" if args.len() != 1 => {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!("{}() requires exactly 1 argument", name),
            });
        }
        "isnull" | "isnotnull" => {}
        "is_blank" | "null_if_blank" => {
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
        "default_if_blank" => {
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "default_if_blank() requires exactly 2 arguments: (text, default)"
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
                                "default_if_blank() argument {} must be chars, got {:?}",
                                i + 1,
                                t
                            ),
                        });
                    }
                }
            }
        }
        "now" | "now_s" | "now_ms" | "now_us" | "now_ns" if !args.is_empty() => {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!("{}() requires no arguments", name),
            });
        }
        "now" | "now_s" | "now_ms" | "now_us" | "now_ns" => {}
        "strftime" => {
            if args.len() != 1 && args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "strftime() requires 1 or 2 arguments: (time[, format])".to_string(),
                });
            } else {
                if let Some(t) = infer_type(&args[0], scope)
                    && !compatible(&t, &ValType::Base(BaseType::Time))
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "strftime() first argument must be time or numeric, got {:?}",
                            t
                        ),
                    });
                }
                if args.len() == 2
                    && let Some(t) = infer_type(&args[1], scope)
                    && !compatible(&t, &ValType::Base(BaseType::Chars))
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("strftime() second argument must be chars, got {:?}", t),
                    });
                }
            }
        }
        "strptime" => {
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "strptime() requires exactly 2 arguments: (text, format)".to_string(),
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
                                "strptime() argument {} must be chars, got {:?}",
                                i + 1,
                                t
                            ),
                        });
                    }
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
        "startswith" | "endswith" => {
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "{}() requires exactly 2 arguments: (text, prefix_or_suffix)",
                        name
                    ),
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
                                "{}() argument {} must be chars, got {:?}",
                                name,
                                i + 1,
                                t
                            ),
                        });
                    }
                }
            }
        }
        "startswith_any" | "endswith_any" => {
            if args.len() < 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "{}() requires at least 2 arguments: (text, prefix_or_suffix, ...)",
                        name
                    ),
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
                                "{}() argument {} must be chars, got {:?}",
                                name,
                                i + 1,
                                t
                            ),
                        });
                    }
                }
            }
        }
        "md5" | "sha1" | "sha256" | "hex" => {
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
        "sha1_n" => {
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "sha1_n() requires exactly 2 arguments: (text, length)".to_string(),
                });
            } else {
                if let Some(t) = infer_type(&args[0], scope)
                    && !compatible(&t, &ValType::Base(BaseType::Chars))
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("sha1_n() first argument must be chars, got {:?}", t),
                    });
                }
                if let Some(t) = infer_type(&args[1], scope)
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("sha1_n() second argument must be numeric, got {:?}", t),
                    });
                }
                match &args[1] {
                    Expr::Number(n)
                        if n.is_finite() && n.fract() == 0.0 && *n >= 1.0 && *n <= 40.0 => {}
                    Expr::Number(_) => errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: "sha1_n() length must be an integer from 1 to 40".to_string(),
                    }),
                    _ => {}
                }
            }
        }
        "stable_id" => {
            if args.len() < 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "stable_id() requires at least 2 arguments: (prefix, value, ...)"
                        .to_string(),
                });
            } else {
                if let Some(t) = infer_type(&args[0], scope)
                    && !compatible(&t, &ValType::Base(BaseType::Chars))
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("stable_id() prefix must be chars, got {:?}", t),
                    });
                }
                for (i, arg) in args.iter().enumerate().skip(1) {
                    if let Some(t) = infer_type(arg, scope)
                        && !is_scalar_stringable_type(&t)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "stable_id() argument {} must be scalar, got {:?}",
                                i + 1,
                                t
                            ),
                        });
                    }
                }
            }
        }
        "join" => {
            if args.is_empty() {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "join() requires at least 1 argument".to_string(),
                });
            } else {
                check_join_values("join", args, 0, scope, rule_name, errors);
            }
        }
        "join_by" => {
            if args.len() < 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "join_by() requires at least 2 arguments: (separator, value, ...)"
                        .to_string(),
                });
            } else {
                if let Some(t) = infer_type(&args[0], scope)
                    && !compatible(&t, &ValType::Base(BaseType::Chars))
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("join_by() separator must be chars, got {:?}", t),
                    });
                }
                check_join_values("join_by", args, 1, scope, rule_name, errors);
            }
        }
        "substr" => {
            if args.len() != 2 && args.len() != 3 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "substr() requires 2 or 3 arguments: (text, start, [length])"
                        .to_string(),
                });
            } else {
                if let Some(t) = infer_type(&args[0], scope)
                    && !compatible(&t, &ValType::Base(BaseType::Chars))
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("substr() first argument must be chars, got {:?}", t),
                    });
                }
                if let Some(t) = infer_type(&args[1], scope)
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("substr() second argument must be numeric, got {:?}", t),
                    });
                }
                if args.len() == 3
                    && let Some(t) = infer_type(&args[2], scope)
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("substr() third argument must be numeric, got {:?}", t),
                    });
                }
            }
        }
        "replace" => {
            if args.len() != 3 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "replace() requires exactly 3 arguments: (text, pattern, replacement)"
                        .to_string(),
                });
            } else {
                // text + replacement must be chars
                for (i, arg) in [0usize, 2usize].iter().copied().enumerate() {
                    if let Some(t) = infer_type(&args[arg], scope)
                        && !compatible(&t, &ValType::Base(BaseType::Chars))
                    {
                        let pos = if i == 0 { 1 } else { 3 };
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "replace() argument {} must be chars, got {:?}",
                                pos, t
                            ),
                        });
                    }
                }
                // pattern should be a valid regex string literal
                match &args[1] {
                    Expr::StringLit(pat) => {
                        if regex_syntax::Parser::new().parse(pat).is_err() {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: format!(
                                    "replace() pattern \"{}\" is not valid regex",
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
                                "replace() second argument must be a string literal regex pattern"
                                    .to_string(),
                        });
                    }
                }
            }
        }
        "replace_plain" => {
            if args.len() != 3 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "replace_plain() requires exactly 3 arguments: (text, from, to)"
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
                                "replace_plain() argument {} must be chars, got {:?}",
                                i + 1,
                                t
                            ),
                        });
                    }
                }
            }
        }
        "trim" => {
            if args.len() != 1 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "trim() requires exactly 1 argument".to_string(),
                });
            } else if let Some(t) = infer_type(&args[0], scope)
                && !compatible(&t, &ValType::Base(BaseType::Chars))
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("trim() argument must be chars, got {:?}", t),
                });
            }
        }
        "ltrim" | "rtrim" => {
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
        "concat" if args.is_empty() => {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: "concat() requires at least 1 argument".to_string(),
            });
        }
        "concat" => {}
        "indexof" => {
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "indexof() requires exactly 2 arguments: (text, needle)".to_string(),
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
                                "indexof() argument {} must be chars, got {:?}",
                                i + 1,
                                t
                            ),
                        });
                    }
                }
            }
        }
        "mvcount" => {
            if args.len() != 1 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "mvcount() requires exactly 1 argument".to_string(),
                });
            } else if let Some(t) = infer_type(&args[0], scope)
                && !is_array_like(&t)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "mvcount() argument must be an array expression, got {:?}",
                        t
                    ),
                });
            }
        }
        "mvjoin" => {
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "mvjoin() requires exactly 2 arguments: (array_expr, separator)"
                        .to_string(),
                });
            } else {
                if let Some(t) = infer_type(&args[0], scope)
                    && !is_array_like(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "mvjoin() first argument must be an array expression, got {:?}",
                            t
                        ),
                    });
                }
                if let Some(t) = infer_type(&args[1], scope)
                    && !compatible(&t, &ValType::Base(BaseType::Chars))
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "mvjoin() second argument must be chars separator, got {:?}",
                            t
                        ),
                    });
                }
            }
        }
        "split" => {
            if args.len() != 2 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "split() requires exactly 2 arguments: (text, separator)".to_string(),
                });
            } else {
                if let Some(t) = infer_type(&args[0], scope)
                    && !compatible(&t, &ValType::Base(BaseType::Chars))
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("split() first argument must be chars, got {:?}", t),
                    });
                }
                if let Some(t) = infer_type(&args[1], scope)
                    && !compatible(&t, &ValType::Base(BaseType::Chars))
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("split() second argument must be chars, got {:?}", t),
                    });
                }
            }
        }
        "mvdedup" => {
            if args.len() != 1 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "mvdedup() requires exactly 1 argument".to_string(),
                });
            } else if let Some(t) = infer_type(&args[0], scope)
                && !is_array_like(&t)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "mvdedup() argument must be an array expression, got {:?}",
                        t
                    ),
                });
            }
        }
        "mvsort" | "mvreverse" => {
            if args.len() != 1 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("{}() requires exactly 1 argument", name),
                });
            } else if let Some(t) = infer_type(&args[0], scope)
                && !is_array_like(&t)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "{}() argument must be an array expression, got {:?}",
                        name, t
                    ),
                });
            }
        }
        "mvindex" => {
            if args.len() != 2 && args.len() != 3 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message:
                        "mvindex() requires 2 or 3 arguments: (array_expr, index, [end_index])"
                            .to_string(),
                });
            } else {
                if let Some(t) = infer_type(&args[0], scope) {
                    if !is_array_like(&t) {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "mvindex() first argument must be an array expression, got {:?}",
                                t
                            ),
                        });
                    } else if args.len() == 2
                        && matches!(t, ValType::ArrayAny | ValType::EmptyArray)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "mvindex() cannot infer scalar result type from {:?}; use a typed array or the 3-argument slice form",
                                t
                            ),
                        });
                    }
                }
                if let Some(t) = infer_type(&args[1], scope)
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "mvindex() second argument must be numeric index, got {:?}",
                            t
                        ),
                    });
                }
                if args.len() == 3
                    && let Some(t) = infer_type(&args[2], scope)
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "mvindex() third argument must be numeric index, got {:?}",
                            t
                        ),
                    });
                }
            }
        }
        "mvappend" => {
            if args.is_empty() {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "mvappend() requires at least 1 argument".to_string(),
                });
            } else {
                let mut element_type: Option<ValType> = None;
                for (idx, arg) in args.iter().enumerate() {
                    let Some(inferred) = infer_type(arg, scope) else {
                        continue;
                    };
                    let arg_element_type = match inferred {
                        ValType::Array(bt) | ValType::Base(bt) => ValType::Base(bt),
                        ValType::ArrayAny => ValType::ArrayAny,
                        ValType::EmptyArray => continue,
                        ValType::Bool => ValType::Base(BaseType::Bool),
                        other => {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: format!(
                                    "mvappend() argument {} must be scalar or array expression, got {:?}",
                                    idx + 1,
                                    other
                                ),
                            });
                            continue;
                        }
                    };
                    if let Some(existing) = &element_type {
                        if let Some(unified) =
                            unify_mvappend_element_type(existing, &arg_element_type)
                        {
                            element_type = Some(unified);
                        } else {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: format!(
                                    "mvappend() argument {} type {:?} is not compatible with {:?}",
                                    idx + 1,
                                    arg_element_type,
                                    existing
                                ),
                            });
                        }
                    } else {
                        element_type = Some(arg_element_type);
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
            } else if !matches!(
                args[0],
                Expr::Field(FieldRef::Qualified(..)) | Expr::Field(FieldRef::Bracketed(..))
            ) {
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
            } else if !matches!(
                args[0],
                Expr::Field(FieldRef::Qualified(..)) | Expr::Field(FieldRef::Bracketed(..))
            ) {
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
                if let Some(t) = infer_type(arg, scope)
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("stddev() requires a numeric field, got {:?}", t),
                    });
                }
                // Also check it's a column projection
                if !matches!(
                    args[0],
                    Expr::Field(FieldRef::Qualified(..)) | Expr::Field(FieldRef::Bracketed(..))
                ) {
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
                if let Some(t) = infer_type(&args[0], scope)
                    && !is_numeric(&t)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("percentile() field must be numeric, got {:?}", t),
                    });
                }
                if !matches!(
                    args[0],
                    Expr::Field(FieldRef::Qualified(..)) | Expr::Field(FieldRef::Bracketed(..))
                ) {
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

fn is_stat_selector_name(name: &str) -> bool {
    matches!(
        name,
        "window_event" | "match_event" | "match_distinct" | "trigger" | "final"
    )
}

fn check_stat_func(
    name: &str,
    args: &[Expr],
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    if args.len() != 1 {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!("stat.{name}() requires exactly 1 stat selector argument"),
        });
        return;
    }

    let Some((selector, symbol)) = parse_stat_selector(&args[0], rule_name, errors) else {
        return;
    };

    match (name, selector) {
        ("count", "window_event")
            if !scope.aliases.contains_key(symbol)
                || scope.join_windows.iter().any(|alias| alias == &symbol) =>
        {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "stat.count(window_event({symbol})) references unknown event alias `{symbol}`"
                ),
            });
        }
        ("count", "window_event") => {}
        ("count", "match_event") => {
            check_label_stage(
                scope,
                rule_name,
                symbol,
                StatLabelStage::Event,
                "stat.count(match_event(...))",
                errors,
            );
        }
        ("count", "match_distinct") => {
            match scope.stat_labels.get(symbol) {
                Some(info) if info.stage != StatLabelStage::Event => errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "stat.count(match_distinct({symbol})) requires step label `{symbol}` to come from on event"
                    ),
                }),
                Some(info) if info.uses_distinct && matches!(info.measure, Measure::Count) => {}
                Some(info) if info.uses_distinct => errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "stat.count(match_distinct({symbol})) requires step label `{symbol}` to use distinct | count"
                    ),
                }),
                Some(_) => errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "stat.count(match_distinct({symbol})) requires step label `{symbol}` to use distinct"
                    ),
                }),
                None => errors.push(unknown_label_error(
                    rule_name,
                    symbol,
                    "stat.count(match_distinct(...))",
                )),
            }
        }
        ("value", "trigger") => {
            check_label_stage(
                scope,
                rule_name,
                symbol,
                StatLabelStage::Event,
                "stat.value(trigger(...))",
                errors,
            );
        }
        ("value", "final") => {
            check_label_stage(
                scope,
                rule_name,
                symbol,
                StatLabelStage::Close,
                "stat.value(final(...))",
                errors,
            );
        }
        ("count", _) => errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "stat.count() accepts window_event(...), match_event(...), or match_distinct(...), got {selector}(...)"
            ),
        }),
        ("value", _) => errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "stat.value() accepts trigger(...) or final(...), got {selector}(...)"
            ),
        }),
        _ => {}
    }

    if matches!((name, selector), ("count", "match_event"))
        && let Some(info) = scope.stat_labels.get(symbol)
        && !matches!(info.measure, Measure::Count)
    {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "stat.count(match_event({symbol})) requires step label `{symbol}` to use count"
            ),
        });
    }
}

fn parse_stat_selector<'a>(
    expr: &'a Expr,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) -> Option<(&'a str, &'a str)> {
    let Expr::FuncCall {
        qualifier: None,
        name,
        args,
    } = expr
    else {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message:
                "stat functions require a selector such as window_event(alias) or trigger(label)"
                    .to_string(),
        });
        return None;
    };

    if !is_stat_selector_name(name) {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!("unknown stat selector `{name}(...)`"),
        });
        return None;
    }

    if args.len() != 1 {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!("stat selector `{name}(...)` requires exactly 1 symbol argument"),
        });
        return None;
    }

    let Expr::Field(FieldRef::Simple(symbol)) = &args[0] else {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "stat selector `{name}(...)` requires a static symbol argument, without quotes"
            ),
        });
        return None;
    };

    Some((name.as_str(), symbol.as_str()))
}

fn check_label_stage(
    scope: &Scope<'_>,
    rule_name: &str,
    label: &str,
    expected_stage: StatLabelStage,
    context: &str,
    errors: &mut Vec<CheckError>,
) {
    match scope.stat_labels.get(label) {
        Some(info) if info.stage == expected_stage => {}
        Some(_) => errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "{context} requires step label `{label}` to come from {}",
                stat_label_stage_name(expected_stage)
            ),
        }),
        None => errors.push(unknown_label_error(rule_name, label, context)),
    }
}

fn stat_label_stage_name(stage: StatLabelStage) -> &'static str {
    match stage {
        StatLabelStage::Event => "on event",
        StatLabelStage::Close => "on close",
    }
}

fn unknown_label_error(rule_name: &str, label: &str, context: &str) -> CheckError {
    CheckError {
        severity: Severity::Error,
        rule: Some(rule_name.to_string()),
        test: None,
        message: format!("{context} references unknown step label `{label}`"),
    }
}
