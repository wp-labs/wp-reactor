use crate::ast::{BinOp, Expr};
use crate::schema::BaseType;

use super::{ValType, is_numeric, numeric_promote};
use crate::checker::scope::Scope;

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
        Expr::IfThenElse { then_expr, .. } => infer_type(then_expr, scope),
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
        "has" | "contains" | "regex_match" | "startswith" | "endswith" => Some(ValType::Bool),
        "substr" => Some(ValType::Base(BaseType::Chars)),
        "mvcount" => Some(ValType::Base(BaseType::Digit)),
        "mvjoin" => Some(ValType::Base(BaseType::Chars)),
        "split" => Some(ValType::Array(BaseType::Chars)),
        "mvdedup" => args.first().and_then(|a| match infer_type(a, scope) {
            Some(ValType::Array(bt)) => Some(ValType::Array(bt)),
            _ => None,
        }),
        "mvindex" => args.first().and_then(|a| match infer_type(a, scope) {
            Some(ValType::Array(bt)) => {
                if args.len() == 3 {
                    Some(ValType::Array(bt))
                } else {
                    Some(ValType::Base(bt))
                }
            }
            _ => None,
        }),
        "mvappend" => infer_mvappend_type(args, scope),
        "baseline" | "time_diff" => Some(ValType::Base(BaseType::Float)),
        "lower" | "upper" | "replace" | "trim" => Some(ValType::Base(BaseType::Chars)),
        "len" => Some(ValType::Base(BaseType::Digit)),
        "time_bucket" => Some(ValType::Base(BaseType::Time)),
        // L3 Collection functions (M28)
        "collect_set" | "collect_list" => {
            // Returns Array<T> where T is the field type
            args.first().and_then(|a| {
                // Extract base type from field and wrap in Array
                infer_type(a, scope).and_then(|t| match t {
                    ValType::Base(bt) => Some(ValType::Array(bt)),
                    _ => None,
                })
            })
        }
        "first" | "last" => {
            // Returns the base field type (unwrap from Field ref)
            args.first().and_then(|a| match a {
                Expr::Field(_) => {
                    // Get the type of the field, which should be BaseType
                    infer_type(a, scope).and_then(|t| match t {
                        ValType::Array(bt) => Some(ValType::Base(bt)),
                        ValType::Base(_) => Some(t),
                        _ => None,
                    })
                }
                _ => None,
            })
        }
        // L3 Statistical functions (M28)
        "stddev" | "percentile" => Some(ValType::Base(BaseType::Float)),
        _ => None,
    }
}

fn infer_mvappend_type(args: &[Expr], scope: &Scope<'_>) -> Option<ValType> {
    let mut element_type: Option<BaseType> = None;
    for arg in args {
        let Some(arg_type) = infer_type(arg, scope) else {
            continue;
        };
        let Some(arg_element_type) = element_base_type(&arg_type) else {
            return None;
        };
        if let Some(existing) = &element_type {
            if *existing != arg_element_type {
                return None;
            }
        } else {
            element_type = Some(arg_element_type);
        }
    }
    element_type.map(ValType::Array)
}

fn element_base_type(t: &ValType) -> Option<BaseType> {
    match t {
        ValType::Array(bt) | ValType::Base(bt) => Some(bt.clone()),
        ValType::Bool => Some(BaseType::Bool),
        _ => None,
    }
}
