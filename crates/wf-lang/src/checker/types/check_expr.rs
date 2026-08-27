use std::collections::HashSet;

use crate::ast::{BinOp, Expr};

use super::infer::infer_type;
use super::{ValType, compatible, is_numeric, op_symbol};
use crate::checker::scope::Scope;
use crate::checker::{CheckError, Severity};

use super::check_funcs::{FuncCheckCtx, check_func_call};

/// Type-check an expression, emitting errors into `errors`.
pub fn check_expr_type(
    expr: &Expr,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    check_expr_type_inner(expr, scope, rule_name, true, false, false, errors);
}

/// Type-check a yield expression while allowing target-field coercion to
/// resolve mixed `coalesce(...)` result types.
pub fn check_yield_expr_type_with_system_vars(
    expr: &Expr,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    check_expr_type_inner(expr, scope, rule_name, true, true, true, errors);
}

/// Type-check a guard expression. Guard context does not allow L3 functions.
pub fn check_guard_expr_type(
    expr: &Expr,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    check_expr_type_inner(expr, scope, rule_name, false, false, false, errors);
}

fn check_expr_type_inner(
    expr: &Expr,
    scope: &Scope<'_>,
    rule_name: &str,
    allow_l3_funcs: bool,
    allow_yield_context: bool,
    allow_mixed_coalesce: bool,
    errors: &mut Vec<CheckError>,
) {
    match expr {
        Expr::BinOp { op, left, right } => {
            check_expr_type_inner(
                left,
                scope,
                rule_name,
                allow_l3_funcs,
                allow_yield_context,
                false,
                errors,
            );
            check_expr_type_inner(
                right,
                scope,
                rule_name,
                allow_l3_funcs,
                allow_yield_context,
                false,
                errors,
            );

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
            check_expr_type_inner(
                inner,
                scope,
                rule_name,
                allow_l3_funcs,
                allow_yield_context,
                false,
                errors,
            );
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
        Expr::Not(inner) => {
            check_expr_type_inner(
                inner,
                scope,
                rule_name,
                allow_l3_funcs,
                allow_yield_context,
                false,
                errors,
            );
            if let Some(ref t) = infer_type(inner, scope)
                && !compatible(t, &ValType::Bool)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("logical `not` requires a bool operand, got {:?}", t),
                });
            }
        }
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => {
            let is_stat = is_stat_func(qualifier.as_deref(), name);
            let is_stat_selector = is_stat_selector_func(qualifier.as_deref(), name);
            if (is_stat || is_stat_selector) && !allow_yield_context {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "stat functions are only allowed in `yield` expressions".to_string(),
                });
                return;
            }

            if !is_stat && !is_stat_selector {
                for arg in args {
                    check_expr_type_inner(
                        arg,
                        scope,
                        rule_name,
                        allow_l3_funcs,
                        allow_yield_context,
                        false,
                        errors,
                    );
                }
            }
            check_func_call(
                qualifier.as_deref(),
                name,
                args,
                FuncCheckCtx {
                    scope,
                    rule_name,
                    allow_l3_funcs,
                    allow_mixed_coalesce,
                },
                errors,
            );
        }
        Expr::Object(items) => {
            let mut seen = HashSet::new();
            for item in items {
                check_expr_type_inner(
                    &item.value,
                    scope,
                    rule_name,
                    allow_l3_funcs,
                    allow_yield_context,
                    false,
                    errors,
                );
                if let Some(type_hint) = &item.type_hint {
                    let expected = crate::checker::scope::field_type_to_val(type_hint);
                    if let Some(actual) = infer_type(&item.value, scope)
                        && !compatible(&expected, &actual)
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "object field type hint {:?} is incompatible with value type {:?}",
                                type_hint, actual
                            ),
                        });
                    }
                }
                for target in &item.targets {
                    if !seen.insert(target) {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!("duplicate object field `{target}`"),
                        });
                    }
                }
            }
        }
        Expr::Array(items) => {
            for item in items {
                check_expr_type_inner(
                    item,
                    scope,
                    rule_name,
                    allow_l3_funcs,
                    allow_yield_context,
                    false,
                    errors,
                );
            }
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            check_expr_type_inner(
                inner,
                scope,
                rule_name,
                allow_l3_funcs,
                allow_yield_context,
                false,
                errors,
            );
            for item in list {
                check_expr_type_inner(
                    item,
                    scope,
                    rule_name,
                    allow_l3_funcs,
                    allow_yield_context,
                    false,
                    errors,
                );
            }

            // issue #73 评审: 元素-左值类型比对。命名列表已在编译期展开为
            // 字面 InList——字面与命名列表走同一条检查路径（行为一致）。
            // 推断不出的元素（函数调用/字段引用/空列表）跳过, 不误报。
            let mut elem_types: Vec<ValType> = Vec::new();
            for item in list {
                if let Some(t) = infer_type(item, scope) {
                    elem_types.push(t);
                }
            }
            let mut mixed = false;
            for i in 1..elem_types.len() {
                if !compatible(&elem_types[0], &elem_types[i]) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "`in (...)` list mixes incompatible element types ({} and {})",
                            format_type(&elem_types[0]),
                            format_type(&elem_types[i])
                        ),
                    });
                    mixed = true;
                    break;
                }
            }
            // 混类型已报错则跳过左值比对（避免同列表双报）; 列表元素无代表类型
            // （全推断不出）时同样跳过。
            if !mixed
                && let Some(lv) = infer_type(inner, scope)
                && let Some(first) = elem_types.first()
                && !compatible(&lv, first)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "`in (...)` left-hand value type {} is not compatible with list element type {}",
                        format_type(&lv),
                        format_type(first)
                    ),
                });
            }
        }
        Expr::SystemVar(_) if !allow_yield_context => {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: "system variables are only allowed in `yield` expressions".to_string(),
            });
        }
        Expr::WfuMeta(field) if !allow_yield_context => {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "wfusion meta field `{}` is only allowed in `yield` expressions",
                    field.name()
                ),
            });
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
        Expr::PresetParam(name) => {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "yield preset parameter `${name}` can only be used inside a yield preset"
                ),
            });
        }
        Expr::Number(_)
        | Expr::StringLit(_)
        | Expr::Bool(_)
        | Expr::SystemVar(_)
        | Expr::WfuMeta(_)
        // 编译期已展开（resolve_list_refs 在 checker 前）。
        | Expr::ListRef(_) => {}
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            check_expr_type_inner(
                cond,
                scope,
                rule_name,
                allow_l3_funcs,
                allow_yield_context,
                false,
                errors,
            );
            check_expr_type_inner(
                then_expr,
                scope,
                rule_name,
                allow_l3_funcs,
                allow_yield_context,
                false,
                errors,
            );
            check_expr_type_inner(
                else_expr,
                scope,
                rule_name,
                allow_l3_funcs,
                allow_yield_context,
                false,
                errors,
            );

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

fn is_stat_func(qualifier: Option<&str>, name: &str) -> bool {
    qualifier == Some("stat") && matches!(name, "count" | "value")
}

fn is_stat_selector_func(qualifier: Option<&str>, name: &str) -> bool {
    qualifier.is_none()
        && matches!(
            name,
            "window_event" | "match_event" | "match_distinct" | "trigger" | "final"
        )
}

/// 类型显示（错误消息用）。
pub(crate) fn format_type(t: &ValType) -> String {
    match t {
        ValType::Base(b) => format!("{b:?}").to_lowercase(),
        ValType::Bool => "bool".to_string(),
        ValType::Numeric => "number".to_string(),
        ValType::Object => "object".to_string(),
        ValType::ArrayAny => "array".to_string(),
        ValType::Array(b) => format!("array of {b:?}").to_lowercase(),
        ValType::EmptyArray => "empty array".to_string(),
    }
}
