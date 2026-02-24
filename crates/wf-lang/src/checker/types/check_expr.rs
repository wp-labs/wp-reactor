use crate::ast::{BinOp, Expr};

use super::infer::infer_type;
use super::{ValType, compatible, is_numeric, op_symbol};
use crate::checker::scope::Scope;
use crate::checker::{CheckError, Severity};

use super::check_funcs::check_func_call;

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
                            test: None,
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
                            test: None,
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
                            test: None,
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
                            test: None,
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
                            test: None,
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
                            test: None,
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
                            test: None,
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
                    test: None,
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
                    test: None,
                    message: msg,
                });
            }
        }
        Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => {}
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            check_expr_type(cond, scope, rule_name, errors);
            check_expr_type(then_expr, scope, rule_name, errors);
            check_expr_type(else_expr, scope, rule_name, errors);

            // T14: cond must be Bool
            if let Some(ref t) = infer_type(cond, scope)
                && !compatible(t, &ValType::Bool)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("if-then-else condition must be bool, got {:?}", t),
                });
            }

            // T14: then/else types must be compatible
            if let (Some(ref tt), Some(ref et)) =
                (infer_type(then_expr, scope), infer_type(else_expr, scope))
                && !(compatible(tt, et) || is_numeric(tt) && is_numeric(et))
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "if-then-else branches have incompatible types: then={:?}, else={:?}",
                        tt, et
                    ),
                });
            }
        }
    }
}
