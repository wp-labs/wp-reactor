//! 表达式树遍历：Expr 中是否含某类调用（原 eval/mod.rs 中 `contains_l3_func` /
//! `contains_eval_time_func` / `contains_stat_selector` / `contains_aggregate_func`
//! 四份同构递归收敛为一份泛型遍历 + 谓词包装；2026-09-06 code.split_reduce_complexity）。
//!
//! 与 builtins.rs 的 `contains_system_var` 语义并列：本组只认 **调用形态**
//! （qualifier/name），不含系统变量。

use wf_lang::ast::Expr;

pub(super) fn is_l3_func(name: &str) -> bool {
    matches!(
        name,
        "collect_set" | "collect_list" | "first" | "last" | "stddev" | "percentile"
    )
}

pub(super) fn is_aggregate_func(name: &str) -> bool {
    matches!(name, "count" | "sum" | "avg" | "min" | "max")
}

pub(super) fn is_eval_time_func(name: &str) -> bool {
    // now 系列 + 时间转换（time_to_s/time_to_ms，issue #69）都需要 L3 时间
    // 工具（current_time_nanos / normalize_epoch_timestamp_float_nanos）。
    matches!(
        name,
        "now" | "now_s" | "now_ms" | "now_us" | "now_ns" | "time_to_s" | "time_to_ms"
    )
}

/// 通用树遍历：expr 任一节点上的 FuncCall 满足谓词即短路 `true`（含其子表达式）。
fn any_call(expr: &Expr, hit: &dyn Fn(&str, Option<&str>) -> bool) -> bool {
    match expr {
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => hit(name.as_str(), qualifier.as_deref()) || args.iter().any(|arg| any_call(arg, hit)),
        Expr::BinOp { left, right, .. } => any_call(left, hit) || any_call(right, hit),
        Expr::Neg(inner) | Expr::Not(inner) => any_call(inner, hit),
        Expr::Object(items) => items.iter().any(|item| any_call(&item.value, hit)),
        Expr::Array(items) => items.iter().any(|item| any_call(item, hit)),
        Expr::InList { expr, list, .. } => {
            any_call(expr, hit) || list.iter().any(|item| any_call(item, hit))
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => any_call(cond, hit) || any_call(then_expr, hit) || any_call(else_expr, hit),
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            any_call(expr, hit)
                || arms.iter().any(|arm| {
                    arm.patterns.iter().any(|p| any_call(p, hit)) || any_call(&arm.value, hit)
                })
                || default.as_ref().is_some_and(|d| any_call(d, hit))
        }
        _ => false,
    }
}

pub(super) fn contains_l3_func(expr: &Expr) -> bool {
    any_call(expr, &|name, _qualifier| is_l3_func(name))
}

pub(super) fn contains_aggregate_func(expr: &Expr) -> bool {
    any_call(expr, &|name, _qualifier| is_aggregate_func(name))
}

pub(super) fn contains_eval_time_func(expr: &Expr) -> bool {
    any_call(expr, &|name, _qualifier| is_eval_time_func(name))
}

/// Whether `expr` contains a `stat.count/stat.value` selector call.
///
/// Wrapper functions like `fmt("{}", stat.value(final(x)))` must stay on the
/// L3 eval path when any argument references stat selectors — the plain
/// match-engine eval has no stat support and would evaluate them to `None`,
/// making the whole expression `None` (q15/q16/q17 close-path stats output).
pub(super) fn contains_stat_selector(expr: &Expr) -> bool {
    any_call(expr, &|_name, qualifier| qualifier == Some("stat"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wf_lang::ast::{BinOp, Expr, FieldRef};

    fn call(qualifier: Option<&str>, name: &str, args: Vec<Expr>) -> Expr {
        Expr::FuncCall {
            qualifier: qualifier.map(|q| q.to_string()),
            name: name.to_string(),
            args,
        }
    }

    fn binop(left: Expr, right: Expr) -> Expr {
        Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    #[test]
    fn name_predicates() {
        assert!(is_l3_func("collect_set"));
        assert!(is_aggregate_func("avg"));
        assert!(is_eval_time_func("now_ns"));
        assert!(!is_l3_func("avg"));
        assert!(!is_aggregate_func("now"));
        assert!(!is_eval_time_func("contains"));
    }

    #[test]
    fn walkers_find_calls_anywhere_in_tree() {
        // 嵌套在二元运算子树的聚合调用
        let field = Expr::Field(FieldRef::Simple("x".into()));
        let expr = binop(field.clone(), call(None, "avg", vec![field.clone()]));
        assert!(contains_aggregate_func(&expr));
        assert!(!contains_l3_func(&expr));
        assert!(!contains_stat_selector(&expr));
        // L3 函数调用被 `fmt` 包装时仍可命中
        let wrapped = call(
            None,
            "fmt",
            vec![call(None, "collect_list", vec![field.clone()])],
        );
        assert!(contains_l3_func(&wrapped));
        // stat 选择器按 qualifier 命中（与名字无关）
        let stat_call = call(
            Some("stat"),
            "value",
            vec![call(None, "final", vec![field.clone()])],
        );
        assert!(contains_stat_selector(&stat_call));
        assert!(!contains_aggregate_func(&stat_call));
        // 普通字段/字面量不命中
        assert!(!contains_l3_func(&Expr::Number(1.0)));
        assert!(!contains_eval_time_func(&field));
    }
}
