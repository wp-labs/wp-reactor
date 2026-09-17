// ---------------------------------------------------------------------------
// Field references
// ---------------------------------------------------------------------------

/// Field selector within a step branch: `.ident` or `["string"]`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::jumo_derive::Jumo)]
#[jumo(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum FieldSelector {
    Dot(String),
    Bracket(String),
}

/// Field reference in expressions.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::jumo_derive::Jumo)]
#[jumo(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum FieldRef {
    /// Bare identifier, e.g. `sip`.
    Simple(String),
    /// Qualified, e.g. `fail.sip`.
    Qualified(String, String),
    /// Bracket notation, e.g. `fail["detail.sha256"]` (flat dotted field name).
    Bracketed(String, String),
    /// Multi-level nested access into `object` / `array` fields,
    /// e.g. `s.roles_obj.source.process.uid` or `s.roles_obj.related[0].name`.
    ///
    /// Invariant: `alias` names the event/set alias whose schema contains
    /// `segments[0]` (always a [`PathSegment::Field`]). At evaluation time the
    /// flat field map is keyed by field name, so the traversal starts from the
    /// root segment — `alias` is only used by the compiler for bind tracking and
    /// by the checker for root resolution.
    Path {
        alias: String,
        segments: Vec<PathSegment>,
    },
}

/// One step of a nested field path: a member name or an array index.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, ::jumo_derive::Jumo)]
#[jumo(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum PathSegment {
    Field(String),
    Index(usize),
}

// ---------------------------------------------------------------------------
// Operators
// ---------------------------------------------------------------------------

#[non_exhaustive]
#[derive(::jumo_derive::Jumo, Debug, Clone, Copy, PartialEq, Eq)]
#[jumo(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

#[non_exhaustive]
#[derive(::jumo_derive::Jumo, Debug, Clone, Copy, PartialEq, Eq)]
#[jumo(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum BinOp {
    And,
    Or,
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[non_exhaustive]
#[derive(::jumo_derive::Jumo, Debug, Clone, Copy, PartialEq, Eq)]
#[jumo(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum SystemVar {
    Score,
    EventFirstTime,
    EventLastTime,
    EvidenceStartTime,
    EvidenceEndTime,
    WindowStartTime,
    WindowEndTime,
    EmitTime,
    /// 实例首次完整命中的引擎处理墙钟（issue #82）：accu 重复 fire 保持不变、
    /// 新实例/新窗口重置、未命中无值。
    FirstMatchTime,
}

pub use crate::wfu_meta::WfuMetaField;

#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq)]
#[jumo(kind = "struct", domain = "Lang", module = "Lang.LangExpr")]
pub struct ObjectItem {
    pub targets: Vec<String>,
    pub type_hint: Option<crate::schema::FieldType>,
    pub value: Expr,
}

/// 模式匹配分支（issue #79 Issue 2）：`pat1 | pat2 => value`。
#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq)]
#[jumo(kind = "struct", domain = "Lang", module = "Lang.LangExpr")]
pub struct MatchArm {
    /// 本分支的模式值表达式（求值后与 subject 比较；`|` 表示多个模式）。
    pub patterns: Vec<Expr>,
    /// 命中本分支时的结果表达式。
    pub value: Expr,
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

#[non_exhaustive]
#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq)]
#[jumo(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum Expr {
    /// Number literal (integer or float).
    Number(f64),
    /// String literal.
    StringLit(String),
    /// Boolean literal.
    Bool(bool),
    /// System variable reference, e.g. `@score`.
    SystemVar(SystemVar),
    /// wfusion-managed metadata field reference, e.g. `@__wfu_rule_name`.
    WfuMeta(WfuMetaField),
    /// Field reference.
    Field(FieldRef),
    /// Parameter reference inside a parameterized yield preset body, e.g. `$severity`.
    PresetParam(String),
    /// Binary operation.
    BinOp {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary negation.
    Neg(Box<Expr>),
    /// Logical negation (`not <cond>` / `!<cond>`), Sigma 条件取反（issue #22）。
    Not(Box<Expr>),
    /// Function call: `name(args...)` or `qualifier.name(args...)`.
    FuncCall {
        qualifier: Option<String>,
        name: String,
        args: Vec<Expr>,
    },
    /// Structured object literal: `object { key = expr; }`.
    Object(Vec<ObjectItem>),
    /// Structured array literal: `array [expr, ...]`.
    Array(Vec<Expr>),
    /// `expr in (v1, v2, ...)` or `expr not in (v1, v2, ...)`.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// 公共允许列表引用（issue #73）: `expr in <shared_name>` 的右值——解析期
    /// 产出, 编译期由 `resolve_list_refs` 展开为字面列表; checker/运行时
    /// 见不到本变体（展开后仅剩字面 InList）。
    ListRef(String),
    /// Conditional expression: `if cond then yes else no`.
    IfThenElse {
        cond: Box<Expr>,
        then_expr: Box<Expr>,
        else_expr: Box<Expr>,
    },
    /// 模式匹配表达式（issue #79 Issue 2）：
    /// `match <expr> { pat1 | pat2 => arm, ..., _ => default }`。
    /// 模式求值后与 subject 按值比较（同 `in` 的相等语义）；短路：命中即
    /// 返回对应 arm，未命中继续下一分支；`_` 默认分支兜底；无默认且全部
    /// 未命中 → None。
    Match {
        expr: Box<Expr>,
        arms: Vec<MatchArm>,
        default: Option<Box<Expr>>,
    },
}

// ---------------------------------------------------------------------------
// 规则级 `let` 常量字符串内联（issue #90）
// ---------------------------------------------------------------------------
// `events` 条件（bind filter）在绑定阶段求值，早于 per-event `let` 注入，因此
// 其中只能引用「规则级 let 且 RHS 解析为字符串字面量」的值（正则复用场景：
// `regex_match(f, re)`，re 为 let 声明）。此类引用在 type-check 与 plan 编译前
// 内联为字面量——行/列两条求值路径看到的与手写内联正则完全一致。
// 非字面量 let 的引用保持原样（Simple(let 名)），由 checker 显式报错。
/// 内联模式：只认字符串字面量（events 条件正则复用，issue #90），或任意可折叠
/// 的常量标量（阈值常量，issue #101）。
#[derive(Clone, Copy, PartialEq, Eq)]
enum ConstLetMode {
    /// 仅 `StringLit`（其余形态保留原引用，由 checker 报错）。
    StringOnly,
    /// 常量标量：`Number` / `StringLit` / `Bool` 原样，常量算术折叠为数字字面量。
    Scalar,
}

/// 递归展开引用：`FieldRef::Simple(let 名)` 且该 let 的 RHS（经 let 链递归）解析为
/// 字符串字面量时，就地替换为 `StringLit`；否则（非字面量 let / 事件字段 /
/// Qualified / Path / 自引用）保留原引用。`visiting` 防 let 自引用死循环。
pub(crate) fn inline_const_string_lets(
    expr: &Expr,
    lets: &[crate::ast::LetDecl],
    visiting: &mut Vec<String>,
) -> Expr {
    inline_const_lets(expr, lets, visiting, ConstLetMode::StringOnly)
}

/// 同 [`inline_const_string_lets`]，但放宽到任何**可折叠的常量标量**（阈值位置用，
/// issue #101）：`let THRESHOLD = 5` / `let RE = "..."` / 常量算术都能内联为字面量，
/// 使阈值满足「触发判定可求值」的约束；不可折叠的引用保持原样，交给 checker 拒绝。
pub(crate) fn inline_const_scalar_lets(
    expr: &Expr,
    lets: &[crate::ast::LetDecl],
    visiting: &mut Vec<String>,
) -> Expr {
    inline_const_lets(expr, lets, visiting, ConstLetMode::Scalar)
}

fn inline_const_lets(
    expr: &Expr,
    lets: &[crate::ast::LetDecl],
    visiting: &mut Vec<String>,
    mode: ConstLetMode,
) -> Expr {
    // 递归时统一带上 mode，避免两套遍历逻辑漂移。
    let rec = |e: &Expr, visiting: &mut Vec<String>| inline_const_lets(e, lets, visiting, mode);
    match expr {
        Expr::Field(FieldRef::Simple(name)) => {
            if !visiting.iter().any(|v| v == name) && lets.iter().any(|l| &l.name == name) {
                visiting.push(name.clone());
                let expanded = rec(
                    &lets.iter().find(|l| &l.name == name).unwrap().expr,
                    visiting,
                );
                visiting.pop();
                if let Some(literal) = const_let_literal(&expanded, mode) {
                    return literal;
                }
            }
            expr.clone()
        }
        Expr::BinOp { op, left, right } => Expr::BinOp {
            op: *op,
            left: Box::new(rec(left, visiting)),
            right: Box::new(rec(right, visiting)),
        },
        Expr::Neg(inner) => Expr::Neg(Box::new(rec(inner, visiting))),
        Expr::Not(inner) => Expr::Not(Box::new(rec(inner, visiting))),
        Expr::Array(items) => Expr::Array(items.iter().map(|i| rec(i, visiting)).collect()),
        Expr::InList {
            expr: target,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(rec(target, visiting)),
            list: list.iter().map(|i| rec(i, visiting)).collect(),
            negated: *negated,
        },
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => Expr::IfThenElse {
            cond: Box::new(rec(cond, visiting)),
            then_expr: Box::new(rec(then_expr, visiting)),
            else_expr: Box::new(rec(else_expr, visiting)),
        },
        Expr::Match {
            expr: subject,
            arms,
            default,
        } => Expr::Match {
            expr: Box::new(rec(subject, visiting)),
            arms: arms
                .iter()
                .map(|arm| crate::ast::MatchArm {
                    patterns: arm.patterns.iter().map(|p| rec(p, visiting)).collect(),
                    value: rec(&arm.value, visiting),
                })
                .collect(),
            default: default.as_ref().map(|d| Box::new(rec(d, visiting))),
        },
        Expr::Object(items) => Expr::Object(
            items
                .iter()
                .map(|it| crate::ast::ObjectItem {
                    targets: it.targets.clone(),
                    type_hint: it.type_hint.clone(),
                    value: rec(&it.value, visiting),
                })
                .collect(),
        ),
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => Expr::FuncCall {
            qualifier: qualifier.clone(),
            name: name.clone(),
            args: args.iter().map(|a| rec(a, visiting)).collect(),
        },
        // 叶子：非 Simple 字段引用与字面量保持原样。
        Expr::Field(_)
        | Expr::Number(_)
        | Expr::StringLit(_)
        | Expr::Bool(_)
        | Expr::SystemVar(_)
        | Expr::WfuMeta(_)
        | Expr::PresetParam(_)
        | Expr::ListRef(_) => expr.clone(),
    }
}

/// 内联替换值：`mode` 允许的常量字面量；不可折叠 → `None`（调用方保留原引用）。
fn const_let_literal(expanded: &Expr, mode: ConstLetMode) -> Option<Expr> {
    match expanded {
        Expr::StringLit(_) => Some(expanded.clone()),
        Expr::Number(_) | Expr::Bool(_) if mode == ConstLetMode::Scalar => Some(expanded.clone()),
        _ if mode == ConstLetMode::Scalar => {
            crate::const_fold::try_eval_expr_to_f64(expanded).map(Expr::Number)
        }
        _ => None,
    }
}

/// 收集表达式中仍引用规则级 `let` 的裸名（内联后剩余者必为非字面量 let）。
pub(crate) fn collect_rule_let_refs(expr: &Expr, lets: &[crate::ast::LetDecl]) -> Vec<String> {
    fn go(e: &Expr, lets: &[crate::ast::LetDecl], out: &mut Vec<String>) {
        match e {
            Expr::Field(FieldRef::Simple(name))
                if lets.iter().any(|l| &l.name == name) && !out.iter().any(|o| o == name) =>
            {
                out.push(name.clone());
            }
            Expr::BinOp { left, right, .. } => {
                go(left, lets, out);
                go(right, lets, out);
            }
            Expr::Neg(i) | Expr::Not(i) => go(i, lets, out),
            Expr::Array(items) => {
                for i in items {
                    go(i, lets, out);
                }
            }
            Expr::InList { expr, list, .. } => {
                go(expr, lets, out);
                for i in list {
                    go(i, lets, out);
                }
            }
            Expr::IfThenElse {
                cond,
                then_expr,
                else_expr,
            } => {
                go(cond, lets, out);
                go(then_expr, lets, out);
                go(else_expr, lets, out);
            }
            Expr::Match {
                expr: subject,
                arms,
                default,
            } => {
                go(subject, lets, out);
                for arm in arms {
                    for p in &arm.patterns {
                        go(p, lets, out);
                    }
                    go(&arm.value, lets, out);
                }
                if let Some(d) = default {
                    go(d, lets, out);
                }
            }
            Expr::Object(items) => {
                for it in items {
                    go(&it.value, lets, out);
                }
            }
            Expr::FuncCall { args, .. } => {
                for a in args {
                    go(a, lets, out);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    go(expr, lets, &mut out);
    out
}

#[cfg(test)]
mod inline_const_lets_tests {
    use super::*;
    use crate::ast::{BinOp, Expr, FieldRef, LetDecl};

    fn num(v: f64) -> Expr {
        Expr::Number(v)
    }
    fn field(name: &str) -> Expr {
        Expr::Field(FieldRef::Simple(name.into()))
    }
    fn binop(op: BinOp, l: Expr, r: Expr) -> Expr {
        Expr::BinOp {
            op,
            left: Box::new(l),
            right: Box::new(r),
        }
    }
    fn leto(name: &str, expr: Expr) -> LetDecl {
        LetDecl {
            name: name.into(),
            expr,
        }
    }

    fn inline_string(e: &Expr, lets: &[LetDecl]) -> Expr {
        inline_const_string_lets(e, lets, &mut Vec::new())
    }
    fn inline_scalar(e: &Expr, lets: &[LetDecl]) -> Expr {
        inline_const_scalar_lets(e, lets, &mut Vec::new())
    }

    // ---- StringOnly：既有行为不得变化（events 条件正则复用，issue #90） ----

    #[test]
    fn string_mode_inlines_only_string_literals() {
        let lets = vec![
            leto("RE", Expr::StringLit("^a.*$".into())),
            leto("N", num(5.0)),
            leto("B", Expr::Bool(true)),
            leto("CHAIN", field("RE")),
        ];
        assert_eq!(
            inline_string(&field("RE"), &lets),
            Expr::StringLit("^a.*$".into())
        );
        assert_eq!(
            inline_string(&field("CHAIN"), &lets),
            Expr::StringLit("^a.*$".into())
        );
        // 非字符串 let 与事件字段保持原引用（由 checker 报错）。
        assert_eq!(inline_string(&field("N"), &lets), field("N"));
        assert_eq!(inline_string(&field("B"), &lets), field("B"));
        assert_eq!(inline_string(&field("sip"), &lets), field("sip"));
    }

    #[test]
    fn string_mode_traverses_nested_expressions() {
        let lets = vec![leto("RE", Expr::StringLit("^a$".into()))];
        let call = Expr::FuncCall {
            qualifier: None,
            name: "regex_match".into(),
            args: vec![field("sip"), field("RE")],
        };
        let Expr::FuncCall { args, .. } = inline_string(&call, &lets) else {
            panic!("应是函数调用");
        };
        assert_eq!(args[1], Expr::StringLit("^a$".into()));
    }

    // ---- Scalar：阈值常量（issue #101） ----

    #[test]
    fn scalar_mode_inlines_literals_and_constant_arithmetic() {
        let lets = vec![
            leto("T", num(3.0)),
            leto("S", Expr::StringLit("abc".into())),
            leto("B", Expr::Bool(true)),
            leto("ARITH", binop(BinOp::Add, num(2.0), num(1.0))),
            leto("NEG", Expr::Neg(Box::new(num(4.0)))),
            leto("CHAIN", field("ARITH")),
        ];
        assert_eq!(inline_scalar(&field("T"), &lets), num(3.0));
        assert_eq!(
            inline_scalar(&field("S"), &lets),
            Expr::StringLit("abc".into())
        );
        assert_eq!(inline_scalar(&field("B"), &lets), Expr::Bool(true));
        // 常量算术 / 取负折叠为单个数字字面量（触发判定只做常量折叠）。
        assert_eq!(inline_scalar(&field("ARITH"), &lets), num(3.0));
        assert_eq!(inline_scalar(&field("NEG"), &lets), num(-4.0));
        assert_eq!(inline_scalar(&field("CHAIN"), &lets), num(3.0));
    }

    #[test]
    fn scalar_mode_keeps_unfoldable_references() {
        let lets = vec![
            leto("DEP", field("e.count")),
            leto(
                "CALL",
                Expr::FuncCall {
                    qualifier: None,
                    name: "abs".into(),
                    args: vec![num(-3.0)],
                },
            ),
            leto("DEGEN", binop(BinOp::Div, num(1.0), num(0.0))),
        ];
        // 依赖事件字段 / 函数调用 / 退化常量都不可折叠 → 保留原引用（由 checker 拒绝）。
        assert_eq!(inline_scalar(&field("DEP"), &lets), field("DEP"));
        assert_eq!(inline_scalar(&field("CALL"), &lets), field("CALL"));
        assert_eq!(inline_scalar(&field("DEGEN"), &lets), field("DEGEN"));
        assert_eq!(inline_scalar(&field("sip"), &lets), field("sip"));
    }

    #[test]
    fn cycles_terminate_and_keep_original_reference() {
        // 自引用与互引用都不得死循环；无法解析为常量时保留原引用。
        let self_ref = vec![leto("A", field("A"))];
        assert_eq!(inline_scalar(&field("A"), &self_ref), field("A"));

        let mutual = vec![leto("A", field("B")), leto("B", field("A"))];
        assert_eq!(inline_scalar(&field("A"), &mutual), field("A"));
        assert_eq!(inline_scalar(&field("B"), &mutual), field("B"));
    }

    #[test]
    fn deep_let_chain_does_not_overflow_stack() {
        // 深链（50 层）必须能解析且不爆栈。
        let mut lets: Vec<LetDecl> = vec![leto("L0", num(7.0))];
        for i in 1..50 {
            lets.push(leto(&format!("L{i}"), field(&format!("L{}", i - 1))));
        }
        assert_eq!(inline_scalar(&field("L49"), &lets), num(7.0));
    }

    #[test]
    fn scalar_mode_traverses_binop_and_call_args() {
        let lets = vec![leto("T", num(3.0))];
        // `(T + 1)` 中的 let 引用被内联 → 常量算术进而可折叠为 4。
        let expr = binop(BinOp::Add, field("T"), num(1.0));
        assert_eq!(
            inline_scalar(&expr, &lets),
            binop(BinOp::Add, num(3.0), num(1.0))
        );
        assert_eq!(
            crate::const_fold::try_eval_expr_to_f64(&inline_scalar(&expr, &lets)),
            Some(4.0)
        );
    }
}
