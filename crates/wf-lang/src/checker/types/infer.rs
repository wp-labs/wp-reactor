use crate::ast::{BinOp, Expr, FieldRef, SystemVar};
use crate::schema::BaseType;

use super::{ValType, is_numeric, numeric_promote, unify_array_element_type};
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
        Expr::SystemVar(SystemVar::Score) => Some(ValType::Base(BaseType::Float)),
        Expr::SystemVar(
            SystemVar::EventFirstTime
            | SystemVar::EventLastTime
            | SystemVar::EvidenceStartTime
            | SystemVar::EvidenceEndTime
            | SystemVar::WindowStartTime
            | SystemVar::WindowEndTime
            | SystemVar::EmitTime,
        ) => Some(ValType::Base(BaseType::Time)),
        Expr::WfuMeta(field) => Some(ValType::Base(field.base_type())),
        Expr::Field(fref) => match fref {
            // Nested path leaf type is runtime-determined (object/array carry no
            // nested schema), so treat as unknown → yield assignability is relaxed.
            FieldRef::Path { .. } => None,
            _ => scope.resolve_field_ref(fref).ok().flatten(),
        },
        Expr::PresetParam(_) => None,
        // 编译期已展开（resolve_shared_list_refs 在 checker 前）。
        Expr::ListRef(_) => None,
        Expr::Object(_) => Some(ValType::Object),
        Expr::Array(items) => infer_array_type(items, scope),
        Expr::BinOp { op, left, right } => infer_binop(*op, left, right, scope),
        Expr::Neg(inner) => {
            let t = infer_type(inner, scope)?;
            if is_numeric(&t) { Some(t) } else { None }
        }
        Expr::Not(inner) => {
            if matches!(infer_type(inner, scope), Some(ValType::Bool)) {
                Some(ValType::Bool)
            } else {
                None
            }
        }
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => infer_func_call(qualifier.as_deref(), name, args, scope),
        Expr::InList { .. } => Some(ValType::Bool),
        Expr::IfThenElse { then_expr, .. } => infer_type(then_expr, scope),
    }
}

fn infer_array_type(items: &[Expr], scope: &Scope<'_>) -> Option<ValType> {
    if items.is_empty() {
        return Some(ValType::EmptyArray);
    }

    let mut element_type: Option<BaseType> = None;
    for item in items {
        let Some(item_type) = infer_type(item, scope) else {
            return Some(ValType::ArrayAny);
        };
        let Some(base_type) = element_base_type(&item_type) else {
            return Some(ValType::ArrayAny);
        };
        if let Some(existing) = &element_type {
            if let Some(unified) = unify_array_element_type(existing, &base_type) {
                element_type = Some(unified);
            } else {
                return Some(ValType::ArrayAny);
            }
        } else {
            element_type = Some(base_type);
        }
    }
    element_type.map(ValType::Array).or(Some(ValType::ArrayAny))
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

fn infer_func_call(
    qualifier: Option<&str>,
    name: &str,
    args: &[Expr],
    scope: &Scope<'_>,
) -> Option<ValType> {
    if qualifier == Some("stat") {
        return match name {
            "count" => Some(ValType::Base(BaseType::Digit)),
            "value" => Some(ValType::Base(BaseType::Float)),
            _ => None,
        };
    }
    match name {
        "count" => Some(ValType::Base(BaseType::Digit)),
        "sum" | "min" | "max" => {
            // Result type follows the argument type.
            args.first().and_then(|a| infer_type(a, scope))
        }
        "avg" => Some(ValType::Base(BaseType::Float)),
        "distinct" => Some(ValType::Base(BaseType::Digit)),
        "fmt" => Some(ValType::Base(BaseType::Chars)),
        "has" | "contains" | "regex_match" | "startswith" | "endswith" | "startswith_any"
        | "endswith_any" | "is_finite" | "isnull" | "isnotnull" | "is_blank" | "cidr_match" => {
            Some(ValType::Bool)
        }
        "substr" => Some(ValType::Base(BaseType::Chars)),
        "abs" => args.first().and_then(|a| infer_type(a, scope)),
        "ceil" | "floor" | "round" => Some(ValType::Base(BaseType::Float)),
        "sqrt" | "pow" | "log" | "exp" | "clamp" | "sign" | "trunc" => {
            Some(ValType::Base(BaseType::Float))
        }
        "mvcount" => Some(ValType::Base(BaseType::Digit)),
        "mvjoin" => Some(ValType::Base(BaseType::Chars)),
        "split" => Some(ValType::Array(BaseType::Chars)),
        "mvdedup" => args.first().and_then(|a| match infer_type(a, scope) {
            Some(ValType::Array(bt)) => Some(ValType::Array(bt)),
            Some(ValType::ArrayAny) => Some(ValType::ArrayAny),
            Some(ValType::EmptyArray) => Some(ValType::EmptyArray),
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
            Some(ValType::ArrayAny) => {
                if args.len() == 3 {
                    Some(ValType::ArrayAny)
                } else {
                    None
                }
            }
            Some(ValType::EmptyArray) => {
                if args.len() == 3 {
                    Some(ValType::EmptyArray)
                } else {
                    None
                }
            }
            _ => None,
        }),
        "mvappend" => infer_mvappend_type(args, scope),
        "baseline" | "time_diff" => Some(ValType::Base(BaseType::Float)),
        "now" => Some(ValType::Base(BaseType::Time)),
        "now_s" | "now_ms" | "now_us" | "now_ns" => Some(ValType::Base(BaseType::Digit)),
        "strftime" => Some(ValType::Base(BaseType::Chars)),
        "strptime" => Some(ValType::Base(BaseType::Time)),
        "lower" | "upper" | "replace" | "trim" | "ltrim" | "rtrim" | "concat" | "join"
        | "join_by" | "replace_plain" | "null_if_blank" | "default_if_blank" | "md5" | "sha1"
        | "sha1_n" | "sha256" | "stable_id" => Some(ValType::Base(BaseType::Chars)),
        "hex" => Some(ValType::Base(BaseType::Hex)),
        "indexof" => Some(ValType::Base(BaseType::Digit)),
        "count_char" => Some(ValType::Base(BaseType::Digit)),
        "coalesce" => args.first().and_then(|a| infer_type(a, scope)),
        "merge" => Some(ValType::Object),
        "len" => Some(ValType::Base(BaseType::Digit)),
        "time_bucket" | "bucket_end" => Some(ValType::Base(BaseType::Time)),
        "mvsort" | "mvreverse" => args.first().and_then(|a| match infer_type(a, scope) {
            Some(ValType::Array(bt)) => Some(ValType::Array(bt)),
            Some(ValType::ArrayAny) => Some(ValType::ArrayAny),
            Some(ValType::EmptyArray) => Some(ValType::EmptyArray),
            _ => None,
        }),
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
    let mut saw_empty_array = false;
    for arg in args {
        let Some(arg_type) = infer_type(arg, scope) else {
            continue;
        };
        match arg_type {
            ValType::ArrayAny => return Some(ValType::ArrayAny),
            ValType::EmptyArray => {
                saw_empty_array = true;
                continue;
            }
            _ => {}
        }
        let arg_element_type = element_base_type(&arg_type)?;
        if let Some(existing) = &element_type {
            let unified = unify_array_element_type(existing, &arg_element_type)?;
            element_type = Some(unified);
        } else {
            element_type = Some(arg_element_type);
        }
    }
    element_type
        .map(ValType::Array)
        .or_else(|| saw_empty_array.then_some(ValType::EmptyArray))
}

fn element_base_type(t: &ValType) -> Option<BaseType> {
    match t {
        ValType::Base(bt) => Some(bt.clone()),
        ValType::Bool => Some(BaseType::Bool),
        _ => None,
    }
}
