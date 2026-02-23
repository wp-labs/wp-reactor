use crate::ast::{BinOp, Expr, FieldRef, FieldSelector, Measure, StepBranch, Transform};
use crate::schema::BaseType;

use super::scope::Scope;
use super::{CheckError, Severity};

// ---------------------------------------------------------------------------
// ValType — lightweight type representation for semantic checks
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValType {
    /// A known scalar base type (Chars, Digit, Float, Time, Ip, Hex).
    Base(BaseType),
    /// Array of a base type.
    Array(BaseType),
    /// Numeric literal — compatible with Digit and Float.
    Numeric,
    /// Boolean value.
    Bool,
}

// ---------------------------------------------------------------------------
// Type compatibility helpers
// ---------------------------------------------------------------------------

pub fn compatible(expected: &ValType, actual: &ValType) -> bool {
    match (expected, actual) {
        (ValType::Base(a), ValType::Base(b)) => a == b,
        (ValType::Array(a), ValType::Array(b)) => a == b,
        (ValType::Base(BaseType::Digit), ValType::Numeric)
        | (ValType::Numeric, ValType::Base(BaseType::Digit)) => true,
        (ValType::Base(BaseType::Float), ValType::Numeric)
        | (ValType::Numeric, ValType::Base(BaseType::Float)) => true,
        (ValType::Numeric, ValType::Numeric) => true,
        (ValType::Bool, ValType::Bool) => true,
        (ValType::Bool, ValType::Base(BaseType::Bool))
        | (ValType::Base(BaseType::Bool), ValType::Bool) => true,
        _ => false,
    }
}

pub fn is_numeric(t: &ValType) -> bool {
    matches!(
        t,
        ValType::Base(BaseType::Digit) | ValType::Base(BaseType::Float) | ValType::Numeric
    )
}

pub fn is_orderable(t: &ValType) -> bool {
    matches!(
        t,
        ValType::Base(BaseType::Digit)
            | ValType::Base(BaseType::Float)
            | ValType::Base(BaseType::Time)
            | ValType::Base(BaseType::Chars)
            | ValType::Numeric
    )
}

/// Whether a type is a scalar identity type usable as entity id (T33).
pub fn is_scalar_identity(t: &ValType) -> bool {
    matches!(
        t,
        ValType::Base(BaseType::Chars)
            | ValType::Base(BaseType::Ip)
            | ValType::Base(BaseType::Hex)
            | ValType::Base(BaseType::Digit)
            | ValType::Numeric
    )
}

/// Numeric promotion: if both sides are numeric, compute the result type.
pub fn numeric_promote(a: &ValType, b: &ValType) -> Option<ValType> {
    if !is_numeric(a) || !is_numeric(b) {
        return None;
    }
    // Float wins over Digit; Numeric is compatible with both.
    if *a == ValType::Base(BaseType::Float) || *b == ValType::Base(BaseType::Float) {
        Some(ValType::Base(BaseType::Float))
    } else if *a == ValType::Base(BaseType::Digit) || *b == ValType::Base(BaseType::Digit) {
        Some(ValType::Base(BaseType::Digit))
    } else {
        Some(ValType::Numeric)
    }
}

// ---------------------------------------------------------------------------
// Expression type inference
// ---------------------------------------------------------------------------

/// Infer the type of an expression within the given scope.
/// Returns None for expressions whose type cannot be statically determined.
pub fn infer_type(expr: &Expr, scope: &Scope<'_>) -> Option<ValType> {
    match expr {
        Expr::Number(n) => {
            if n.fract() == 0.0 {
                Some(ValType::Base(BaseType::Digit))
            } else {
                Some(ValType::Base(BaseType::Float))
            }
        }
        Expr::StringLit(_) => Some(ValType::Base(BaseType::Chars)),
        Expr::Bool(_) => Some(ValType::Bool),
        Expr::Field(fref) => scope.resolve_field_ref(fref).ok().flatten(),
        Expr::BinOp { op, left, right } => infer_binop(*op, left, right, scope),
        Expr::Neg(inner) => {
            let t = infer_type(inner, scope)?;
            if is_numeric(&t) { Some(t) } else { None }
        }
        Expr::FuncCall { name, args, .. } => infer_func_call(name, args, scope),
        Expr::InList { .. } => Some(ValType::Bool),
    }
}

fn infer_binop(op: BinOp, left: &Expr, right: &Expr, scope: &Scope<'_>) -> Option<ValType> {
    match op {
        BinOp::And | BinOp::Or => Some(ValType::Bool),
        BinOp::Eq | BinOp::Ne => Some(ValType::Bool),
        BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => Some(ValType::Bool),
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            let lt = infer_type(left, scope)?;
            let rt = infer_type(right, scope)?;
            numeric_promote(&lt, &rt)
        }
    }
}

fn infer_func_call(name: &str, args: &[Expr], scope: &Scope<'_>) -> Option<ValType> {
    match name {
        "count" => Some(ValType::Base(BaseType::Digit)),
        "sum" | "min" | "max" => {
            // Result type follows the argument type.
            args.first().and_then(|a| infer_type(a, scope))
        }
        "avg" => Some(ValType::Base(BaseType::Float)),
        "distinct" => Some(ValType::Base(BaseType::Digit)),
        "fmt" => Some(ValType::Base(BaseType::Chars)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Expression type checking (emitting errors)
// ---------------------------------------------------------------------------

/// Type-check an expression, emitting errors into `errors`.
pub fn check_expr_type(
    expr: &Expr,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    match expr {
        Expr::BinOp { op, left, right } => {
            check_expr_type(left, scope, rule_name, errors);
            check_expr_type(right, scope, rule_name, errors);

            let lt = infer_type(left, scope);
            let rt = infer_type(right, scope);

            match op {
                BinOp::And | BinOp::Or => {
                    // T9: both sides must be bool
                    if let Some(ref t) = lt
                        && !compatible(t, &ValType::Bool)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!(
                                "logical `{}` requires bool operands, left side is {:?}",
                                op_symbol(*op),
                                t
                            ),
                        });
                    }
                    if let Some(ref t) = rt
                        && !compatible(t, &ValType::Bool)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!(
                                "logical `{}` requires bool operands, right side is {:?}",
                                op_symbol(*op),
                                t
                            ),
                        });
                    }
                }
                BinOp::Eq | BinOp::Ne => {
                    // T7: both sides must have compatible types
                    if let (Some(l), Some(r)) = (&lt, &rt)
                        && !compatible(l, r)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!(
                                "`{}` comparison between incompatible types {:?} and {:?}",
                                op_symbol(*op),
                                l,
                                r
                            ),
                        });
                    }
                }
                BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
                    // T8: both sides must be numeric
                    if let Some(ref t) = lt
                        && !is_numeric(t)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!(
                                "ordering `{}` requires numeric operands, left side is {:?}",
                                op_symbol(*op),
                                t
                            ),
                        });
                    }
                    if let Some(ref t) = rt
                        && !is_numeric(t)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!(
                                "ordering `{}` requires numeric operands, right side is {:?}",
                                op_symbol(*op),
                                t
                            ),
                        });
                    }
                }
                BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
                    if let Some(ref t) = lt
                        && !is_numeric(t)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!(
                                "arithmetic `{}` requires numeric operands, left side is {:?}",
                                op_symbol(*op),
                                t
                            ),
                        });
                    }
                    if let Some(ref t) = rt
                        && !is_numeric(t)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!(
                                "arithmetic `{}` requires numeric operands, right side is {:?}",
                                op_symbol(*op),
                                t
                            ),
                        });
                    }
                }
            }
        }
        Expr::Neg(inner) => {
            check_expr_type(inner, scope, rule_name, errors);
            if let Some(ref t) = infer_type(inner, scope)
                && !is_numeric(t)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!("unary negation requires numeric operand, got {:?}", t),
                });
            }
        }
        Expr::FuncCall { name, args, .. } => {
            for arg in args {
                check_expr_type(arg, scope, rule_name, errors);
            }
            check_func_call(name, args, scope, rule_name, errors);
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            check_expr_type(inner, scope, rule_name, errors);
            for item in list {
                check_expr_type(item, scope, rule_name, errors);
            }
        }
        Expr::Field(fref) => {
            // Just verify the field resolves.
            if let Err(msg) = scope.resolve_field_ref(fref) {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: msg,
                });
            }
        }
        Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => {}
    }
}

fn check_func_call(
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
                    contract: None,
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
                    contract: None,
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
                    contract: None,
                    message: format!("{}() requires an orderable field, got {:?}", name, t),
                });
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Pipe chain type checking
// ---------------------------------------------------------------------------

/// Type-check a match step branch's pipe chain.
pub fn check_pipe_chain(
    branch: &StepBranch,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let has_field = branch.field.is_some();

    // Determine the field type if there is a field selector
    let field_val_type: Option<ValType> = branch.field.as_ref().and_then(|fs| {
        let field_name = match fs {
            FieldSelector::Dot(n) | FieldSelector::Bracket(n) => n.as_str(),
        };
        scope.get_field_type_for_alias(&branch.source, field_name)
    });

    // Check transforms
    for transform in &branch.pipe.transforms {
        match transform {
            Transform::Distinct => {
                // T3: distinct requires a column projection (field selector)
                if !has_field {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "distinct requires a field selector (column projection), but step source `{}` has none",
                            branch.source
                        ),
                    });
                }
            }
        }
    }

    // Check measure
    match branch.pipe.measure {
        Measure::Count => {
            // T4: count operates on a set level. If there's a field but no distinct, it's an error.
            if has_field && !branch.pipe.transforms.contains(&Transform::Distinct) {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!(
                        "count operates on sets; use `distinct | count` for column `{}`",
                        field_selector_name(branch.field.as_ref().unwrap())
                    ),
                });
            }
        }
        Measure::Sum | Measure::Avg => {
            // T1: field must be numeric
            if let Some(ref vt) = field_val_type
                && !is_numeric(vt)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!(
                        "{}() requires a numeric field, `{}` is {:?}",
                        measure_name(branch.pipe.measure),
                        field_selector_name(branch.field.as_ref().unwrap()),
                        vt
                    ),
                });
            }
            if !has_field {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!(
                        "{}() requires a field selector",
                        measure_name(branch.pipe.measure)
                    ),
                });
            }
        }
        Measure::Min | Measure::Max => {
            // T2: field must be orderable
            if let Some(ref vt) = field_val_type
                && !is_orderable(vt)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!(
                        "{}() requires an orderable field, `{}` is {:?}",
                        measure_name(branch.pipe.measure),
                        field_selector_name(branch.field.as_ref().unwrap()),
                        vt
                    ),
                });
            }
            if !has_field {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!(
                        "{}() requires a field selector",
                        measure_name(branch.pipe.measure)
                    ),
                });
            }
        }
    }

    // Check threshold expression type
    check_expr_type(&branch.pipe.threshold, scope, rule_name, errors);

    // T5: threshold type must be compatible with measure result type
    // Note: numeric types (Digit, Float, Numeric) are all interchangeable at runtime
    // (all become Value::Number), so we allow any numeric-to-numeric pairing.
    if let Some(result_type) = measure_result_type(branch.pipe.measure, &field_val_type)
        && let Some(threshold_type) = infer_type(&branch.pipe.threshold, scope)
        && !compatible(&result_type, &threshold_type)
        && !(is_numeric(&result_type) && is_numeric(&threshold_type))
    {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            contract: None,
            message: format!(
                "threshold type {:?} is not compatible with {}() result type {:?}",
                threshold_type,
                measure_name(branch.pipe.measure),
                result_type
            ),
        });
    }

    // Check guard expression if present
    if let Some(ref guard) = branch.guard {
        check_expr_type(guard, scope, rule_name, errors);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn op_symbol(op: BinOp) -> &'static str {
    match op {
        BinOp::And => "&&",
        BinOp::Or => "||",
        BinOp::Eq => "==",
        BinOp::Ne => "!=",
        BinOp::Lt => "<",
        BinOp::Gt => ">",
        BinOp::Le => "<=",
        BinOp::Ge => ">=",
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
    }
}

fn field_selector_name(fs: &FieldSelector) -> &str {
    match fs {
        FieldSelector::Dot(n) | FieldSelector::Bracket(n) => n.as_str(),
    }
}

fn measure_name(m: Measure) -> &'static str {
    match m {
        Measure::Count => "count",
        Measure::Sum => "sum",
        Measure::Avg => "avg",
        Measure::Min => "min",
        Measure::Max => "max",
    }
}

/// Infer the result type of a measure given its field type.
fn measure_result_type(measure: Measure, field_val_type: &Option<ValType>) -> Option<ValType> {
    match measure {
        Measure::Count => Some(ValType::Base(BaseType::Digit)),
        Measure::Sum => field_val_type.clone(),
        Measure::Avg => Some(ValType::Base(BaseType::Float)),
        Measure::Min | Measure::Max => field_val_type.clone(),
    }
}
