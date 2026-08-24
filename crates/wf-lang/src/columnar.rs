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
//! object traversal falls back to the interpreted path.
//!
//! The one nested shape the columnar evaluator handles natively is the
//! **list-index path** `c.tags[0]` — a root field followed by exactly one
//! constant array index. It compiles to an offset read of the array column (a
//! structured JSON-array `Utf8` cell or a native Arrow `List` cell) instead of
//! the interpreted per-row `Value::Array` reconstruction.

use crate::ast::{BinOp, Expr, FieldRef, PathSegment};

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

        // Flat field references map to a single Arrow column; the list-index
        // path (`c.tags[0]`) maps to an offset read of the array column.
        // Anything deeper needs per-row object/array traversal and falls back.
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

/// Flat `FieldRef` variants reference one column. Nested paths do not, with one
/// exception: the list-index path `root[i]` is a columnar offset read.
fn field_ref_is_columnar(field: &FieldRef) -> bool {
    match field {
        FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _) => true,
        FieldRef::Path { segments, .. } => path_is_list_index(segments),
    }
}

/// `root[i]` — a root field followed by exactly one constant array index.
/// Anything deeper (`a.b[0]`, `a[0].b`, `a[0][1]`) falls back to the
/// interpreted per-row object/array traversal.
fn path_is_list_index(segments: &[PathSegment]) -> bool {
    matches!(segments, [PathSegment::Field(_), PathSegment::Index(_)])
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

    fn list_index(alias: &str, root: &str, index: usize) -> Expr {
        Expr::Field(FieldRef::Path {
            alias: alias.to_string(),
            segments: vec![
                crate::ast::PathSegment::Field(root.to_string()),
                crate::ast::PathSegment::Index(index),
            ],
        })
    }

    #[test]
    fn list_index_path_is_columnar() {
        assert!(expr_is_columnar(&list_index("c", "tags", 0)));
        assert!(expr_is_columnar(&list_index("c", "tags", 3)));
        // And compositions over it are columnar too.
        let cmp = cmp(
            BinOp::Eq,
            list_index("c", "tags", 0),
            Expr::StringLit("prod".into()),
        );
        assert!(expr_is_columnar(&cmp));
    }

    #[test]
    fn deeper_paths_fall_back() {
        // member then index: a.b[0]
        let member_index = Expr::Field(FieldRef::Path {
            alias: "s".into(),
            segments: vec![
                crate::ast::PathSegment::Field("obj".into()),
                crate::ast::PathSegment::Field("arr".into()),
                crate::ast::PathSegment::Index(0),
            ],
        });
        assert!(!expr_is_columnar(&member_index));

        // index then member: a[0].b
        let index_member = Expr::Field(FieldRef::Path {
            alias: "s".into(),
            segments: vec![
                crate::ast::PathSegment::Field("arr".into()),
                crate::ast::PathSegment::Index(0),
                crate::ast::PathSegment::Field("b".into()),
            ],
        });
        assert!(!expr_is_columnar(&index_member));

        // double index: a[0][1]
        let double_index = Expr::Field(FieldRef::Path {
            alias: "s".into(),
            segments: vec![
                crate::ast::PathSegment::Field("arr".into()),
                crate::ast::PathSegment::Index(0),
                crate::ast::PathSegment::Index(1),
            ],
        });
        assert!(!expr_is_columnar(&double_index));

        // a path rooted on a bare field (no member/index) still falls back.
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
