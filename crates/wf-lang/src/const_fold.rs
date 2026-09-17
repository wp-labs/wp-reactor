//! 阈值表达式的**常量折叠**——语言侧与求值侧共用的单一真源。
//!
//! 匹配步骤的触发判定（阈值比较）不参与逐事件求值：`wf-cep` 的 `check_threshold`
//! 只对阈值表达式做常量折叠，折叠不出结果即判为「不满足」（分支永不触发）。因此
//! 「阈值是否为编译期常量」的判定必须与这里的折叠规则**逐项一致**——历史上这段
//! 规则在 checker 里被重新推导过一遍，导致除零 / 模零这类退化常量在编译期被放行
//! 但运行期永不触发（warp-fusion#101）。
//!
//! 本模块只依赖 `wf_lang::ast::Expr`，`wf-cep` 的 `try_eval_expr_to_f64` /
//! `try_eval_expr_to_value` 直接委托到这里，避免两处规则漂移。

use crate::ast::{BinOp, Expr};

/// 把常量表达式折叠为 `f64`。
///
/// 支持：数字字面量、取负、以及算术算子（`+ - * / %`）作用于可折叠的子表达式。
/// 返回 `None` 表示**不可折叠**：非常量形态（字段引用、函数调用等），或退化常量
/// （除 / 模零、非算术算子）。
pub fn try_eval_expr_to_f64(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Number(n) => Some(*n),
        Expr::Neg(inner) => try_eval_expr_to_f64(inner).map(|v| -v),
        Expr::BinOp { op, left, right } => {
            let l = try_eval_expr_to_f64(left)?;
            let r = try_eval_expr_to_f64(right)?;
            fold_f64_binop(op, l, r)
        }
        _ => None,
    }
}

/// 常量折叠的 f64 算术（除/模零 → None; 非算术算子 → None）。
fn fold_f64_binop(op: &BinOp, l: f64, r: f64) -> Option<f64> {
    match op {
        BinOp::Add => Some(l + r),
        BinOp::Sub => Some(l - r),
        BinOp::Mul => Some(l * r),
        BinOp::Div => {
            if r == 0.0 {
                None
            } else {
                Some(l / r)
            }
        }
        BinOp::Mod => {
            if r == 0.0 {
                None
            } else {
                Some(l % r)
            }
        }
        _ => None,
    }
}

/// 阈值是否可被触发判定求值（"是否为编译期常量"的判定口径）。
///
/// 与 `wf-cep` 的 `try_eval_expr_to_value` 等价：字面量（数字 / 字符串 / 布尔）
/// 直接可求值，其余形态交给 [`try_eval_expr_to_f64`]。checker 用它拒绝不可求值的
/// 阈值（warp-fusion#101）。
pub fn is_foldable_threshold(expr: &Expr) -> bool {
    matches!(expr, Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_))
        || try_eval_expr_to_f64(expr).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn num(n: f64) -> Expr {
        Expr::Number(n)
    }

    fn binop(op: BinOp, l: Expr, r: Expr) -> Expr {
        Expr::BinOp {
            op,
            left: Box::new(l),
            right: Box::new(r),
        }
    }

    #[test]
    fn folds_literals_negation_and_arithmetic() {
        assert_eq!(try_eval_expr_to_f64(&num(5.0)), Some(5.0));
        assert_eq!(
            try_eval_expr_to_f64(&Expr::Neg(Box::new(num(2.0)))),
            Some(-2.0)
        );
        assert_eq!(
            try_eval_expr_to_f64(&binop(BinOp::Add, num(1.0), num(2.0))),
            Some(3.0)
        );
        assert_eq!(
            try_eval_expr_to_f64(&binop(
                BinOp::Mul,
                binop(BinOp::Add, num(1.0), num(2.0)),
                Expr::Neg(Box::new(num(2.0)))
            )),
            Some(-6.0)
        );
    }

    #[test]
    fn rejects_degenerate_constants_and_non_constants() {
        // 除 / 模零：折叠不出结果 → 阈值永不满足（wf-cep sem_tests 同口径）。
        assert_eq!(
            try_eval_expr_to_f64(&binop(BinOp::Div, num(1.0), num(0.0))),
            None
        );
        assert_eq!(
            try_eval_expr_to_f64(&binop(BinOp::Mod, num(1.0), num(0.0))),
            None
        );
        // 非算术算子：Bool 结果不是阈值可用的标量折叠。
        assert_eq!(
            try_eval_expr_to_f64(&binop(BinOp::Eq, num(1.0), num(1.0))),
            None
        );
        // 字段引用 / 取负的非数字字面量。
        assert_eq!(
            try_eval_expr_to_f64(&Expr::Field(crate::ast::FieldRef::Simple("x".into()))),
            None
        );
        assert_eq!(
            try_eval_expr_to_f64(&Expr::Neg(Box::new(Expr::Bool(true)))),
            None
        );
    }

    #[test]
    fn foldable_threshold_covers_literals_and_folded_arithmetic() {
        assert!(is_foldable_threshold(&num(1.0)));
        assert!(is_foldable_threshold(&Expr::StringLit("a".into())));
        assert!(is_foldable_threshold(&Expr::Bool(true)));
        assert!(is_foldable_threshold(&Expr::Neg(Box::new(num(1.0)))));
        assert!(is_foldable_threshold(&binop(
            BinOp::Add,
            num(1.0),
            num(2.0)
        )));
        // 退化常量与非常量形态一律不可折叠。
        assert!(!is_foldable_threshold(&binop(
            BinOp::Div,
            num(1.0),
            num(0.0)
        )));
        assert!(!is_foldable_threshold(&Expr::Field(
            crate::ast::FieldRef::Simple("x".into())
        )));
        assert!(!is_foldable_threshold(&Expr::SystemVar(
            crate::ast::SystemVar::EmitTime
        )));
    }
}
