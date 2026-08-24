//! Columnar-expression classification — the dual-track execution gate.
//!
//! `expr_is_columnar` is a **static** predicate: rules are immutable, so it is
//! evaluated once at plan build / subscription time and never per batch. It
//! returns `true` only for the pure field-arithmetic / comparison / constant
//! subset that the columnar guard evaluator can compute bit-for-bit identically
//! to the interpreted evaluator.
//!
//! Anything requiring meta context (`SystemVar` / `WfuMeta` / `PresetParam`),
//! a function call other than the natively-lowered string/IP predicates
//! (`cidr_match(field, "cidr")`, `regex_match(field, "pattern")`, `contains` /
//! `startswith` / `endswith` with a flat-field or literal second operand),
//! a window lookup (those expressions never reach here — they
//! are structurally rejected by `FuncCall`), structured literals, or nested
//! object traversal falls back to the interpreted path.
//!
//! The one nested shape the columnar evaluator handles natively is the
//! **list-index path** `c.tags[0]` — a root field followed by exactly one
//! constant array index. It compiles to an offset read of the array column (a
//! structured JSON-array `Utf8` cell or a native Arrow `List` cell) instead of
//! the interpreted per-row `Value::Array` reconstruction.

use crate::ast::{BinOp, Expr, FieldRef, PathSegment};

/// 列式执行器原生支持的内置函数分类 —— **单一权威清单**。
///
/// 门控（[`expr_is_columnar`]）与 wf-engine 的 `compile_expr` 都基于此枚举
/// 判断函数是否可列式化及其参数形态，避免函数名清单在两处各自维护而 drift：
/// 新增可列式函数只需在这里加一个分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnarFunc {
    /// `cidr_match(field, "addr/prefix")` — 第二参数必须是 STRING 字面量。
    CidrMatch,
    /// `regex_match(field, "pattern")` — 第二参数必须是 STRING 字面量。
    RegexMatch,
    /// `contains` / `startswith` / `endswith` — 第二参数是字面量或 flat 字段。
    StrSearch,
}

/// 返回 `name` 对应的列式函数分类（`None` = 非原生列式函数，回落解释器）。
pub fn columnar_func(name: &str) -> Option<ColumnarFunc> {
    match name {
        "cidr_match" => Some(ColumnarFunc::CidrMatch),
        "regex_match" => Some(ColumnarFunc::RegexMatch),
        "contains" | "startswith" | "endswith" => Some(ColumnarFunc::StrSearch),
        _ => None,
    }
}

fn is_flat_field(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Field(FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _))
    )
}

/// 某个列式函数的参数形态是否可列式：第一操作数必须是 flat 字段；第二操作数
/// 依分类而定（`CidrMatch`/`RegexMatch` 必须字面量，`StrSearch` 字面量或 flat
/// 字段）。`false` 只表示回落解释器，不等于错误。
pub fn columnar_func_args_ok(func: ColumnarFunc, args: &[Expr]) -> bool {
    if args.len() != 2 || !is_flat_field(&args[0]) {
        return false;
    }
    match func {
        ColumnarFunc::CidrMatch | ColumnarFunc::RegexMatch => {
            matches!(&args[1], Expr::StringLit(_))
        }
        ColumnarFunc::StrSearch => {
            matches!(&args[1], Expr::StringLit(_)) || is_flat_field(&args[1])
        }
    }
}

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

        // Logical negation is a per-element column op over the bool column.
        Expr::Not(inner) => expr_is_columnar(inner),

        // Binary ops: logic and arithmetic/comparison are all per-element
        // column operations over the operands.
        Expr::BinOp { op, left, right } => {
            binop_is_columnar(*op) && expr_is_columnar(left) && expr_is_columnar(right)
        }

        // 原生列式函数：单一权威清单（`columnar_func`）判定分类，形态由
        // `columnar_func_args_ok` 校验。其余函数调用回落解释器。
        Expr::FuncCall {
            qualifier: None,
            name,
            args,
        } => columnar_func(name).is_some_and(|func| columnar_func_args_ok(func, args)),

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
    fn logical_not_gate_mirrors_inner() {
        // `not (auction == 1)`：inner 列式 → Not 列式。
        assert!(expr_is_columnar(&Expr::Not(Box::new(cmp(
            BinOp::Eq,
            field("auction"),
            num(1.0)
        )))));
        // `not <函数调用>`：inner 非列式 → Not 也非列式（回落解释器）。
        assert!(!expr_is_columnar(&Expr::Not(Box::new(func("now_s")))));
        // 双层 not 仍列式。
        assert!(expr_is_columnar(&Expr::Not(Box::new(Expr::Not(Box::new(
            field("flag")
        ))))));
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
    fn cidr_match_is_columnar_when_literal_subnet() {
        let cm = |arg0: Expr, arg1: Expr| Expr::FuncCall {
            qualifier: None,
            name: "cidr_match".to_string(),
            args: vec![arg0, arg1],
        };
        // 字段 + 字面量子网 → 列式。
        assert!(expr_is_columnar(&cm(
            field("sip"),
            Expr::StringLit("10.0.0.0/8".into())
        )));
        assert!(expr_is_columnar(&cm(
            qualified("e", "sip"),
            Expr::StringLit("fe80::/10".into())
        )));
        // 非字面量子网 → 回落解释器。
        assert!(!expr_is_columnar(&cm(field("sip"), field("subnet"))));
        // 参数个数不符 → 回落。
        assert!(!expr_is_columnar(&Expr::FuncCall {
            qualifier: None,
            name: "cidr_match".to_string(),
            args: vec![field("sip")],
        }));
        // 非字段首参（如函数/字面量 IP）→ 回落。
        assert!(!expr_is_columnar(&cm(
            Expr::StringLit("10.0.0.1".into()),
            Expr::StringLit("10.0.0.0/8".into())
        )));
        // 嵌套路径首参 → 回落。
        assert!(!expr_is_columnar(&cm(
            nested_path(),
            Expr::StringLit("10.0.0.0/8".into())
        )));
        // 其他函数仍非列式。
        assert!(!expr_is_columnar(&func("cidr_match")));
    }

    #[test]
    fn cidr_match_composes_columnar() {
        // `cidr_match(...) && count > 3` 整体列式。
        let cm = Expr::FuncCall {
            qualifier: None,
            name: "cidr_match".to_string(),
            args: vec![field("sip"), Expr::StringLit("10.0.0.0/8".into())],
        };
        let and = cmp(BinOp::And, cm, cmp(BinOp::Gt, field("count"), num(3.0)));
        assert!(expr_is_columnar(&and));
        // not 包住也列式。
        assert!(expr_is_columnar(&Expr::Not(Box::new(and))));
    }

    #[test]
    fn regex_match_is_columnar_when_literal_pattern() {
        let rm = |arg0: Expr, arg1: Expr| Expr::FuncCall {
            qualifier: None,
            name: "regex_match".to_string(),
            args: vec![arg0, arg1],
        };
        // 字段 + 字面量 pattern → 列式。
        assert!(expr_is_columnar(&rm(
            field("action"),
            Expr::StringLit("fail.*".into())
        )));
        assert!(expr_is_columnar(&rm(
            qualified("e", "action"),
            Expr::StringLit("^\\.exe$".into())
        )));
        // 非字面量 pattern → 回落解释器。
        assert!(!expr_is_columnar(&rm(field("action"), field("pat"))));
        // 非字段首参 → 回落。
        assert!(!expr_is_columnar(&rm(
            Expr::StringLit("x".into()),
            Expr::StringLit("y".into())
        )));
        // 参数个数不符 → 回落。
        assert!(!expr_is_columnar(&Expr::FuncCall {
            qualifier: None,
            name: "regex_match".to_string(),
            args: vec![field("action")],
        }));
        // 组合：regex_match && contains(...)（后者非列式）→ 整体回落。
        let mixed = cmp(
            BinOp::And,
            rm(field("action"), Expr::StringLit("fail.*".into())),
            func("contains"),
        );
        assert!(!expr_is_columnar(&mixed));
    }

    #[test]
    fn str_search_funcs_are_columnar_with_literal_or_field_needle() {
        let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
            qualifier: None,
            name: name.to_string(),
            args,
        };
        let lit_nd = Expr::StringLit("fail".into());
        for name in ["contains", "startswith", "endswith"] {
            // func(field, "literal") → 列式。
            assert!(
                expr_is_columnar(&call(name, vec![field("action"), lit_nd.clone()])),
                "{name} lit"
            );
            // func(field, field2) → 列式（needle 为字段）。
            assert!(
                expr_is_columnar(&call(name, vec![field("action"), field("pat")])),
                "{name} field"
            );
            assert!(
                expr_is_columnar(&call(name, vec![qualified("e", "action"), lit_nd.clone()])),
                "{name} qualified"
            );
            // func(literal, field) → 首参非字段 → 回落。
            assert!(!expr_is_columnar(&call(
                name,
                vec![lit_nd.clone(), field("pat")]
            )));
            // func(field, func(...)) → 次参非字段/字面量 → 回落。
            assert!(!expr_is_columnar(&call(
                name,
                vec![field("action"), func("lower")]
            )));
            // 嵌套路径首参 → 回落。
            assert!(!expr_is_columnar(&call(
                name,
                vec![nested_path(), lit_nd.clone()]
            )));
            // 参数个数不符 → 回落。
            assert!(!expr_is_columnar(&call(name, vec![field("action")])));
        }
    }

    #[test]
    fn str_search_funcs_compose_columnar() {
        let contains = Expr::FuncCall {
            qualifier: None,
            name: "contains".to_string(),
            args: vec![field("action"), Expr::StringLit("fail".into())],
        };
        // contains(...) && count > 3 → 整体列式。
        let and = cmp(
            BinOp::And,
            contains,
            cmp(BinOp::Gt, field("count"), num(3.0)),
        );
        assert!(expr_is_columnar(&and));
        // 字段 needle 的 startswith 与 regex_match 组合 → 列式。
        let sw = Expr::FuncCall {
            qualifier: None,
            name: "startswith".to_string(),
            args: vec![field("action"), field("prefix")],
        };
        assert!(expr_is_columnar(&cmp(
            BinOp::Or,
            sw,
            cmp(BinOp::Gt, field("count"), num(1.0))
        )));
        // not 包住也列式。
        assert!(expr_is_columnar(&Expr::Not(Box::new(and))));
    }

    #[test]
    fn columnar_func_is_the_single_authoritative_list() {
        // 清单：三个 StrSearch + 两个常量类。
        for name in ["contains", "startswith", "endswith"] {
            assert_eq!(columnar_func(name), Some(ColumnarFunc::StrSearch), "{name}");
        }
        assert_eq!(columnar_func("cidr_match"), Some(ColumnarFunc::CidrMatch));
        assert_eq!(columnar_func("regex_match"), Some(ColumnarFunc::RegexMatch));
        // 非列式函数不在清单。
        for name in [
            "lower",
            "concat",
            "startswith_any",
            "strftime",
            "len",
            "bogus",
        ] {
            assert_eq!(columnar_func(name), None, "{name} 不应在列式清单");
        }
    }

    #[test]
    fn columnar_func_args_ok_shape_matrix() {
        let flat = field("sip");
        let lit = Expr::StringLit("10.0.0.0/8".into());
        let func_call = func("lower");
        for func in [
            ColumnarFunc::CidrMatch,
            ColumnarFunc::RegexMatch,
            ColumnarFunc::StrSearch,
        ] {
            // 字段 + 字面量：三种分类都接受。
            assert!(columnar_func_args_ok(func, &[flat.clone(), lit.clone()]));
            // 字段 + 字段：仅 StrSearch 接受（cidr/regex 要求字面量）。
            assert_eq!(
                columnar_func_args_ok(func, &[flat.clone(), flat.clone()]),
                func == ColumnarFunc::StrSearch
            );
            // 字面量 + 字段：首参非字段 → 都不接受。
            assert!(!columnar_func_args_ok(func, &[lit.clone(), flat.clone()]));
            // 字段 + 函数：次参非字面量/字段 → 都不接受。
            assert!(!columnar_func_args_ok(
                func,
                &[flat.clone(), func_call.clone()]
            ));
            // 字段 + 嵌套路径：次参非 flat → 都不接受。
            assert!(!columnar_func_args_ok(func, &[flat.clone(), nested_path()]));
            // 参数个数：1 个 → 不接受。
            assert!(!columnar_func_args_ok(func, std::slice::from_ref(&flat)));
        }
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
