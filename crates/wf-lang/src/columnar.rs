//! Columnar-expression classification — the dual-track execution gate.
//!
//! `expr_is_columnar` is a **static** predicate: rules are immutable, so it is
//! evaluated once at plan build / subscription time and never per batch. It
//! returns `true` only for the pure field-arithmetic / comparison / constant
//! subset that the columnar guard evaluator can compute bit-for-bit identically
//! to the interpreted evaluator.
//!
//! Anything requiring meta context (`SystemVar` / `WfuMeta` / `PresetParam`),
//! a function call, a window lookup (those expressions never reach here — they
//! are structurally rejected by `FuncCall`), structured literals, or nested
//! field traversal (`FieldRef::Path`) falls back to the interpreted path.

use crate::ast::{BinOp, Expr, FieldRef};

/// Whether `expr` can be evaluated columnar (per batch) with results identical
/// to the row-wise interpreted evaluator.
///
/// Conservative by construction: the predicate only accepts the subset that the
/// columnar evaluator implements exactly. `false` never means "wrong to try",
/// only "fall back to interpreted".
pub fn expr_is_columnar(expr: &Expr) -> bool {
    match expr {
        // Literals evaluate identically on both tracks.
        Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => true,

        // Flat field references map to a single Arrow column. Nested paths
        // (`FieldRef::Path`) need per-row object/array traversal and fall back.
        Expr::Field(field) => field_ref_is_columnar(field),

        // Unary arithmetic negation is a per-element column op.
        Expr::Neg(inner) => expr_is_columnar(inner),

        // Binary ops: logic and arithmetic/comparison are all per-element
        // column operations over the operands.
        Expr::BinOp { op, left, right } => {
            binop_is_columnar(*op) && expr_is_columnar(left) && expr_is_columnar(right)
        }

        // Everything else needs meta / function / window / structured handling.
        Expr::SystemVar(_)
        | Expr::WfuMeta(_)
        | Expr::PresetParam(_)
        | Expr::FuncCall { .. }
        | Expr::Object(_)
        | Expr::Array(_)
        | Expr::InList { .. }
        | Expr::IfThenElse { .. } => false,
    }
}

/// Flat `FieldRef` variants reference one column; nested paths do not.
fn field_ref_is_columnar(field: &FieldRef) -> bool {
    matches!(
        field,
        FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _)
    )
}

/// Whether a binary operator is supported by the columnar evaluator.
fn binop_is_columnar(op: BinOp) -> bool {
    matches!(
        op,
        BinOp::And
            | BinOp::Or
            | BinOp::Eq
            | BinOp::Ne
            | BinOp::Lt
            | BinOp::Gt
            | BinOp::Le
            | BinOp::Ge
            | BinOp::Add
            | BinOp::Sub
            | BinOp::Mul
            | BinOp::Div
            | BinOp::Mod
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str) -> Expr {
        Expr::Field(FieldRef::Simple(name.to_string()))
    }

    fn qualified(alias: &str, name: &str) -> Expr {
        Expr::Field(FieldRef::Qualified(alias.to_string(), name.to_string()))
    }

    fn nested_path() -> Expr {
        Expr::Field(FieldRef::Path {
            alias: "b".to_string(),
            segments: vec![crate::ast::PathSegment::Field("obj".to_string())],
        })
    }

    fn num(n: f64) -> Expr {
        Expr::Number(n)
    }

    fn cmp(op: BinOp, left: Expr, right: Expr) -> Expr {
        Expr::BinOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn func(name: &str) -> Expr {
        Expr::FuncCall {
            qualifier: None,
            name: name.to_string(),
            args: vec![],
        }
    }

    #[test]
    fn literals_are_columnar() {
        assert!(expr_is_columnar(&Expr::Number(1.0)));
        assert!(expr_is_columnar(&Expr::StringLit("x".into())));
        assert!(expr_is_columnar(&Expr::Bool(true)));
    }

    #[test]
    fn flat_field_refs_are_columnar() {
        assert!(expr_is_columnar(&field("auction")));
        assert!(expr_is_columnar(&qualified("b", "auction")));
        assert!(expr_is_columnar(&Expr::Field(FieldRef::Bracketed(
            "b".into(),
            "detail.sha256".into()
        ))));
    }

    #[test]
    fn nested_paths_fall_back() {
        assert!(!expr_is_columnar(&nested_path()));
    }

    #[test]
    fn comparisons_and_arithmetic_are_columnar() {
        // The Q2 guard: auction % 123 == 0.
        let q2 = cmp(
            BinOp::Eq,
            cmp(BinOp::Mod, field("auction"), num(123.0)),
            num(0.0),
        );
        assert!(expr_is_columnar(&q2));

        // Field > const.
        assert!(expr_is_columnar(&cmp(BinOp::Gt, field("price"), num(7.0))));

        // Field == Field.
        assert!(expr_is_columnar(&cmp(
            BinOp::Eq,
            field("a"),
            qualified("b", "a")
        )));
    }

    #[test]
    fn logic_short_circuits_are_columnar_when_both_sides_are() {
        let and = cmp(
            BinOp::And,
            cmp(BinOp::Gt, field("a"), num(0.0)),
            Expr::Bool(true),
        );
        assert!(expr_is_columnar(&and));

        let or = cmp(
            BinOp::Or,
            cmp(BinOp::Gt, field("a"), num(0.0)),
            Expr::Bool(false),
        );
        assert!(expr_is_columnar(&or));
    }

    #[test]
    fn negation_is_columnar() {
        assert!(expr_is_columnar(&Expr::Neg(Box::new(field("price")))));
        assert!(!expr_is_columnar(&Expr::Neg(Box::new(func("f")))));
    }

    #[test]
    fn func_calls_fall_back_even_with_columnar_args() {
        let with_args = Expr::FuncCall {
            qualifier: None,
            name: "concat".to_string(),
            args: vec![field("a"), field("b")],
        };
        assert!(!expr_is_columnar(&with_args));
        assert!(!expr_is_columnar(&func("strftime")));
    }

    #[test]
    fn structured_literals_fall_back() {
        assert!(!expr_is_columnar(&Expr::Object(vec![])));
        assert!(!expr_is_columnar(&Expr::Array(vec![field("a")])));
    }

    #[test]
    fn in_list_and_if_fall_back() {
        assert!(!expr_is_columnar(&Expr::InList {
            expr: Box::new(field("a")),
            list: vec![num(1.0), num(2.0)],
            negated: false,
        }));
        assert!(!expr_is_columnar(&Expr::IfThenElse {
            cond: Box::new(Expr::Bool(true)),
            then_expr: Box::new(num(1.0)),
            else_expr: Box::new(num(2.0)),
        }));
    }

    #[test]
    fn system_and_meta_vars_fall_back() {
        use crate::ast::{SystemVar, WfuMetaField};
        assert!(!expr_is_columnar(&Expr::SystemVar(SystemVar::Score)));
        assert!(!expr_is_columnar(&Expr::WfuMeta(WfuMetaField::RuleName)));
        assert!(!expr_is_columnar(&Expr::PresetParam("severity".into())));
    }

    #[test]
    fn mixed_expression_falls_back_when_any_subterm_is_not_columnar() {
        let mixed = cmp(BinOp::And, cmp(BinOp::Gt, field("a"), num(0.0)), func("f"));
        assert!(!expr_is_columnar(&mixed));
    }

    #[test]
    fn nested_columnar_expression_recurses() {
        // a % 3 == 0 && b > 5 — pure column subset, fully nested.
        let expr = cmp(
            BinOp::And,
            cmp(BinOp::Eq, cmp(BinOp::Mod, field("a"), num(3.0)), num(0.0)),
            cmp(BinOp::Gt, field("b"), num(5.0)),
        );
        assert!(expr_is_columnar(&expr));
    }
}
