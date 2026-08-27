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

fn is_flat_field_ref(field: &FieldRef) -> bool {
    matches!(
        field,
        FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _)
    )
}

fn is_flat_field(expr: &Expr) -> bool {
    matches!(expr, Expr::Field(field) if is_flat_field_ref(field))
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

/// 列式**输出**（yield cell）原生支持的内置函数分类 —— 与 [`ColumnarFunc`]
/// （守卫）并列的单一权威清单。这些函数产生字符串/数值 cell（而非布尔守卫），
/// 供列式输出路径（on-each / match / close 的 yield 批量 cell 求值）编译。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnarOutputFunc {
    /// `fmt(template, v1, ...)` — 字面量模板 + 列式标量参数，批量渲染 `{}`。
    Fmt,
    /// `strftime(ts, [fmt])` — 数值时间列 → 格式化字符串（fmt 字面量或默认）。
    Strftime,
    /// `count_char(text, ch)` — 数字符出现次数 → 数值。
    CountChar,
    /// `split(text, sep)` — 字符串分割（只作为 `mvindex` 的 list 参数融合为
    /// `SplitIndex`；列式无列表值类型，独立使用回落解释器）。
    Split,
    /// `mvindex(list, idx)` — 列表元素（list 为 `split` 时融合求值，q22 形态）。
    MvIndex,
    /// `concat(a, b, ...)` — 字符串拼接（`value_to_string` 渲染）。
    Concat,
}

pub fn columnar_output_func(name: &str) -> Option<ColumnarOutputFunc> {
    match name {
        "fmt" => Some(ColumnarOutputFunc::Fmt),
        "strftime" => Some(ColumnarOutputFunc::Strftime),
        "count_char" => Some(ColumnarOutputFunc::CountChar),
        "split" => Some(ColumnarOutputFunc::Split),
        "mvindex" => Some(ColumnarOutputFunc::MvIndex),
        "concat" => Some(ColumnarOutputFunc::Concat),
        _ => None,
    }
}

/// InList 列表项：字面量（编译期折叠成 Value）。
fn is_output_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_))
}

/// IfThenElse 条件：可列式 Bool 表达式——守卫门控（比较/逻辑/守卫函数）或
/// 值可列式的 InList（产生 Bool）。
fn columnar_output_cond(expr: &Expr) -> bool {
    match expr {
        Expr::InList { .. } => columnar_output_expr(expr),
        other => expr_is_columnar(other),
    }
}

/// yield 输出表达式是否可列式批量求值——**递归**的值表达式分类：字面量 /
/// flat 字段 / 原生列式输出函数（`fmt`/`strftime`/`count_char`，参数递归）/ /
/// `IfThenElse`（条件列式 Bool + 分支递归）/ `InList`（expr 递归 + 列表字面量）。
/// 覆盖 Q14 形态：`fmt("{} c={}", if strftime(ts,"%H") in (...), count_char(...))`。
/// `false` 只表示该输出 cell 走行式解释，不等于错误。
pub fn columnar_output_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => true,
        Expr::Field(field) => is_flat_field_ref(field),
        Expr::FuncCall {
            qualifier: None,
            name,
            args,
        } => match columnar_output_func(name) {
            Some(ColumnarOutputFunc::Fmt) => {
                !args.is_empty()
                    && matches!(&args[0], Expr::StringLit(_))
                    && args[1..].iter().all(columnar_output_expr)
            }
            Some(ColumnarOutputFunc::Strftime) => {
                !args.is_empty()
                    && args.len() <= 2
                    && columnar_output_expr(&args[0])
                    && args.get(1).is_none_or(|a| matches!(a, Expr::StringLit(_)))
            }
            Some(ColumnarOutputFunc::CountChar) => {
                args.len() == 2 && columnar_output_expr(&args[0]) && columnar_output_expr(&args[1])
            }
            Some(ColumnarOutputFunc::Split) => {
                // split(text, sep)：text 列式可求值（flat 字段/let 内联），sep 字面量。
                args.len() == 2
                    && columnar_output_expr(&args[0])
                    && matches!(&args[1], Expr::StringLit(_))
            }
            Some(ColumnarOutputFunc::MvIndex) => {
                // mvindex(list, idx)：2 参（q22 形态），list 列式可求值（split 或
                // let 内联），idx 字面量数字（负数语义由 normalize_index 支持）。
                args.len() == 2
                    && columnar_output_expr(&args[0])
                    && matches!(&args[1], Expr::Number(_))
            }
            Some(ColumnarOutputFunc::Concat) => {
                !args.is_empty() && args.iter().all(columnar_output_expr)
            }
            None => false,
        },
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            columnar_output_cond(cond)
                && columnar_output_expr(then_expr)
                && columnar_output_expr(else_expr)
        }
        Expr::InList { expr, list, .. } => {
            columnar_output_expr(expr) && list.iter().all(is_output_literal)
        }
        _ => false,
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
    fn columnar_output_func_is_single_authoritative_list() {
        for name in [
            "fmt",
            "strftime",
            "count_char",
            "split",
            "mvindex",
            "concat",
        ] {
            assert!(columnar_output_func(name).is_some(), "{name} 应在输出清单");
        }
        for name in ["lower", "contains", "cidr_match", "bogus"] {
            assert_eq!(columnar_output_func(name), None, "{name} 不应在输出清单");
        }
    }

    #[test]
    fn columnar_output_expr_shape_matrix() {
        let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
            qualifier: None,
            name: name.to_string(),
            args,
        };
        let lit_s = Expr::StringLit("x".into());
        let lit_n = num(1.0);
        let fld = field("action");
        let fld2 = field("count");
        let fcall = func("lower");

        // fmt(字面量模板, 字面量/字段参数...) → 可列式。
        assert!(columnar_output_expr(&call(
            "fmt",
            vec![lit_s.clone(), fld.clone()]
        )));
        assert!(columnar_output_expr(&call(
            "fmt",
            vec![lit_s.clone(), fld.clone(), lit_n.clone()]
        )));
        // fmt 模板非字面量 → 否。
        assert!(!columnar_output_expr(&call("fmt", vec![fld.clone()])));
        // fmt 参数含函数调用 → 否。
        assert!(!columnar_output_expr(&call(
            "fmt",
            vec![lit_s.clone(), fcall.clone()]
        )));
        // fmt 空参数 → 否。
        assert!(!columnar_output_expr(&call("fmt", vec![])));

        // strftime(字段, [字面量]) → 可列式；fmt 非字面量 → 否；参数超 2 → 否。
        assert!(columnar_output_expr(&call("strftime", vec![fld.clone()])));
        assert!(columnar_output_expr(&call(
            "strftime",
            vec![fld.clone(), lit_s.clone()]
        )));
        assert!(!columnar_output_expr(&call(
            "strftime",
            vec![fld.clone(), fld2.clone()]
        )));
        assert!(!columnar_output_expr(&call(
            "strftime",
            vec![fld.clone(), lit_s.clone(), lit_n]
        )));
        assert!(!columnar_output_expr(&call("strftime", vec![])));

        // count_char(字段, 字段/字面量) → 可列式；个数/形状不符 → 否。
        assert!(columnar_output_expr(&call(
            "count_char",
            vec![fld.clone(), lit_s.clone()]
        )));
        assert!(columnar_output_expr(&call(
            "count_char",
            vec![fld.clone(), fld2.clone()]
        )));
        assert!(!columnar_output_expr(&call(
            "count_char",
            vec![fld.clone()]
        )));
        assert!(!columnar_output_expr(&call(
            "count_char",
            vec![fcall.clone(), fld.clone()]
        )));

        // 字面量 / flat 字段本身可列式输出；嵌套路径否。
        assert!(columnar_output_expr(&lit_s));
        assert!(columnar_output_expr(&fld));
        assert!(!columnar_output_expr(&nested_path()));

        // IfThenElse：条件列式 Bool + 分支递归 → 可列式（Q14 形态）。
        assert!(columnar_output_expr(&Expr::IfThenElse {
            cond: Box::new(Expr::Bool(true)),
            then_expr: Box::new(num(1.0)),
            else_expr: Box::new(num(2.0)),
        }));
        // 条件非列式（系统变量）→ 否。
        use crate::ast::SystemVar;
        assert!(!columnar_output_expr(&Expr::IfThenElse {
            cond: Box::new(Expr::SystemVar(SystemVar::Score)),
            then_expr: Box::new(num(1.0)),
            else_expr: Box::new(num(2.0)),
        }));
        // InList：expr 递归 + 列表项字面量 → 可列式；非字面量项 → 否。
        assert!(columnar_output_expr(&Expr::InList {
            expr: Box::new(fld.clone()),
            list: vec![lit_s.clone()],
            negated: false,
        }));
        assert!(!columnar_output_expr(&Expr::InList {
            expr: Box::new(fld.clone()),
            list: vec![fld2.clone()],
            negated: false,
        }));
    }

    /// Q14 全形态：`fmt("{} c={}", if strftime(ts,"%H") in ("00","01","02")
    /// then "nightTime" else "dayTime", count_char(extra,"c"))` → 递归可列式。
    #[test]
    fn q14_fmt_ifthenelse_inlist_shape_is_columnar() {
        let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
            qualifier: None,
            name: name.to_string(),
            args,
        };
        let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
        let hour = call(
            "strftime",
            vec![f("dateTime"), Expr::StringLit("%H".into())],
        );
        let is_night = Expr::InList {
            expr: Box::new(hour),
            list: vec![
                Expr::StringLit("00".into()),
                Expr::StringLit("01".into()),
                Expr::StringLit("02".into()),
            ],
            negated: false,
        };
        let label = Expr::IfThenElse {
            cond: Box::new(is_night),
            then_expr: Box::new(Expr::StringLit("nightTime".into())),
            else_expr: Box::new(Expr::StringLit("dayTime".into())),
        };
        let detail = call(
            "fmt",
            vec![
                Expr::StringLit("{} c={}".into()),
                label,
                call("count_char", vec![f("extra"), Expr::StringLit("c".into())]),
            ],
        );
        assert!(
            columnar_output_expr(&detail),
            "Q14 detail 应可列式：{detail:?}"
        );

        // 真实 q14.wfl 形状：嵌套 3 档 CASE（else 分支再嵌 IfThenElse，10/9 项
        // InList）→ 递归放行。
        let in_hours = |hours: &[&str]| Expr::InList {
            expr: Box::new(call(
                "strftime",
                vec![f("dateTime"), Expr::StringLit("%H".into())],
            )),
            list: hours.iter().map(|h| Expr::StringLit((*h).into())).collect(),
            negated: false,
        };
        let three_way = call(
            "fmt",
            vec![
                Expr::StringLit("{} c={}".into()),
                Expr::IfThenElse {
                    cond: Box::new(in_hours(&[
                        "00", "01", "02", "03", "04", "05", "06", "20", "21", "22", "23",
                    ])),
                    then_expr: Box::new(Expr::StringLit("nightTime".into())),
                    else_expr: Box::new(Expr::IfThenElse {
                        cond: Box::new(in_hours(&[
                            "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18",
                        ])),
                        then_expr: Box::new(Expr::StringLit("dayTime".into())),
                        else_expr: Box::new(Expr::StringLit("otherTime".into())),
                    }),
                },
                call("count_char", vec![f("extra"), Expr::StringLit("c".into())]),
            ],
        );
        assert!(
            columnar_output_expr(&three_way),
            "真实 q14 嵌套 3 档 CASE 应可列式"
        );

        // InList 非字面量项 → 整体否。
        let bad_list = call(
            "fmt",
            vec![
                Expr::StringLit("{} c={}".into()),
                Expr::IfThenElse {
                    cond: Box::new(Expr::InList {
                        expr: Box::new(call(
                            "strftime",
                            vec![f("dateTime"), Expr::StringLit("%H".into())],
                        )),
                        list: vec![f("extra")],
                        negated: false,
                    }),
                    then_expr: Box::new(Expr::StringLit("nightTime".into())),
                    else_expr: Box::new(Expr::StringLit("dayTime".into())),
                },
                call("count_char", vec![f("extra"), Expr::StringLit("c".into())]),
            ],
        );
        assert!(
            !columnar_output_expr(&bad_list),
            "非字面量列表项应否：{bad_list:?}"
        );
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
