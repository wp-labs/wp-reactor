//! 位置相关的函数可用性闸门（warp-fusion#101 及同类位置）。
//!
//! 部分内建函数的求值依赖某项**运行期上下文**，写在缺乏该上下文的位置时恒求值为
//! 空 → 规则静默不触发 / 事件被静默过滤 / 输出失真，且编译期与运行期都无信号。
//! 本模块按「表达式求值位置」在语义检查阶段直接拒绝。
//!
//! 依赖的能力（[`Capability`]）：
//! - **instance 收集序列**：`collect_set` / `collect_list` / `first` / `last` /
//!   `stddev` / `percentile`，只有 yield/derive 上下文提供（wf-engine
//!   `eval_l3_func` 读 `_step_*` / `_bind_*`）；
//! - **窗口查找**：`window.has(...)`，wf-cep 的逐行求值（`eval_expr`）不传窗口表；
//! - **滚动基线状态**：`baseline(...)` 依赖调用方持有的 `RollingStats` 表——该状态
//!   只在 CEP 状态机的 event guard 路径上跨事件累积，其它位置每次求值都从空表
//!   开始，于是 `deviation()` 恒为 **`0.0`**（空表的 `stddev()==0` 直接短路）——
//!   不是 `None`：拿常量去比较照样能过编译，`> k` 会恒假（`baseline_dev(...)` 走
//!   全局 `wf_cep::baseline::store()`，不受此限）。
//!
//! 位置矩阵（运行期求值路径见 `wf-engine` / `wf-cep`）：
//! - match/close 规则的 score / entity / yield / post-join `where` / 规则级 `let`：
//!   引擎在 instance 上下文求值（`build_eval_context` 注入 `_step_*`）——L3 可用，
//!   但该求值路径不传窗口表与滚动状态，故 `window.has` / `baseline` 仍不可用；
//! - `events` bind filter（`&&`）：wf-cep 逐事件求值，**有窗口查找**，无实例序列、
//!   无滚动基线；
//! - `join ... within` 的表达式界、`emit at`：wf-cep 逐行求值，三者皆无；
//! - `on each` 规则的 `where` filter / score / entity / yield / 规则级 `let` /
//!   post-join `where`（含 deferred）：引擎逐事件求值，三者皆无；
//! - stats 度量的 `where`：引擎逐行求值（`stats_exec` 每行建 ctx 后
//!   `eval_bool_expr`），三者皆无；
//! - `conv` 链的 `sort` / `dedup` / `where`：wf-cep 在收口批上按 output 逐行求值
//!   （`conv.rs` 只注入 key 与 step label），三者皆无。
//!
//! 本闸门管的是“**能调但不能用**”。另一类“能写但引擎不实现”的死形态由各自入口的
//! 白名单管，不在此处：stats 桶键（只实现 `Field` / `bucket` / `tier`，见
//! `rules::check_stats_keys`）、无限定 `has(...)`（`types/check_funcs.rs`）。

use crate::ast::{Expr, RuleDecl};
use crate::checker::{CheckError, Severity};

/// L3 集合/统计函数：需要 instance 收集序列。
///
/// 唯一事实源——checker 内各处（guard 拒绝、on-each 拒绝、本闸门）共用。
pub(crate) const L3_INSTANCE_FUNCS: &[&str] = &[
    "collect_set",
    "collect_list",
    "first",
    "last",
    "stddev",
    "percentile",
];

pub(crate) fn is_l3_instance_func(name: &str) -> bool {
    L3_INSTANCE_FUNCS.contains(&name)
}

/// 函数求值依赖的运行期上下文。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Capability {
    /// 本 rule instance 收集到的事件序列（`_step_*` / `_bind_*`）。
    InstanceSeries,
    /// 窗口查找表（`window.has(...)`）。
    WindowLookup,
    /// 实例级滚动基线状态（`baseline(...)` 的 `RollingStats` 表）。
    RollingBaselines,
}

/// 表达式求值位置——决定运行期能提供哪些 [`Capability`]。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExprPosition {
    /// match/close 规则的 score / entity / yield / 规则级 `let` / post-join `where`：
    /// 引擎在 instance 上下文求值（有实例序列，无窗口表 / 滚动状态）。
    Instance,
    /// `events { alias : W && <expr> }`：绑定阶段逐事件由 wf-cep 求值。
    BindFilter,
    /// `join ... within [lo, hi]` 的表达式界：wf-cep 逐行求值。
    JoinBound,
    /// `join ... emit at <expr>`：wf-cep 逐行求值。
    EmitAt,
    /// `on each` 规则内（规则级 `let` / post-join `where`）：引擎在单事件上求值。
    OnEach,
    /// stats 度量的 `where`：引擎 `stats_exec` 逐行求值（单事件上下文）。
    StatsWhere,
    /// `conv` 链的 `sort` / `dedup` / `where`：wf-cep 在收口批上按 output 逐行求值。
    ConvExpr,
}

impl ExprPosition {
    /// 本位置是否提供该能力。
    fn provides(self, cap: Capability) -> bool {
        match (self, cap) {
            // bind filter 由 `eval_bool_expr_with_lookup` 带着窗口表求值（`window.has`
            // 在 `events` 条件里是受支持写法）。
            (ExprPosition::BindFilter, Capability::WindowLookup) => true,
            // instance 上下文（引擎 `build_eval_context`）提供实例收集序列；
            // 它不传窗口表与滚动状态表，所以 `window.has` / `baseline` 仍不可用。
            (ExprPosition::Instance, Capability::InstanceSeries) => true,
            _ => false,
        }
    }

    fn label(self) -> &'static str {
        match self {
            ExprPosition::Instance => {
                "instance-context expressions (score/entity/yield, rule-level `let`, \
                 post-join `where`)"
            }
            ExprPosition::BindFilter => "`events` bind filter (`&&`) expressions",
            ExprPosition::JoinBound => "join `within` bound expressions",
            ExprPosition::EmitAt => "`emit at` expressions",
            ExprPosition::OnEach => "`on each` expressions",
            ExprPosition::StatsWhere => "stats measure `where` expressions",
            ExprPosition::ConvExpr => "`conv` expressions",
        }
    }

    fn hint(self, cap: Capability) -> &'static str {
        match cap {
            Capability::InstanceSeries if self == ExprPosition::OnEach => {
                "`on each` has no window instance, so L3 collection functions cannot be \
                 evaluated; use them in a `match` / `close` rule instead"
            }
            Capability::InstanceSeries => {
                "it needs this rule instance's collected events, which are not available at \
                 this position (evaluated per event / per row); use it in score/entity/yield \
                 instead"
            }
            Capability::WindowLookup => {
                "window lookups are not available at this position; use it in a bind filter \
                 (`events` `&&`) or a branch guard instead"
            }
            Capability::RollingBaselines => {
                "the rolling baseline state only accumulates across events in event guards, \
                 not at this position"
            }
        }
    }
}

/// 规则级 `let` 与 post-join `where` 的运行期求值位置：`on each`（含 deferred）规则
/// 在单事件上求值（无任何动态上下文）；match/close 规则在 instance 上下文求值
/// （有实例序列，无窗口表 / 滚动状态）。
pub(crate) fn rule_expr_position(rule: &RuleDecl) -> ExprPosition {
    if rule.each_clause.is_some() {
        ExprPosition::OnEach
    } else {
        ExprPosition::Instance
    }
}

/// 函数名 → 其求值所需的运行期能力（不需要动态上下文的函数返回 `None`）。
fn required_capability(name: &str) -> Option<Capability> {
    if is_l3_instance_func(name) {
        Some(Capability::InstanceSeries)
    } else if name == "has" {
        // `window.has(...)`
        Some(Capability::WindowLookup)
    } else if name == "baseline" {
        // 注意 `baseline_dev(...)` 是另一个函数（全局 store），不在此列。
        Some(Capability::RollingBaselines)
    } else {
        None
    }
}

/// 递归查找该位置第一个无法求值的调用，返回 `(函数名, 缺失能力)`。
///
/// 表达式深度已由解析器上限约束，递归深度同界。
fn find_unavailable_call(expr: &Expr, position: ExprPosition) -> Option<(&str, Capability)> {
    match expr {
        Expr::FuncCall { name, args, .. } => {
            if let Some(cap) = required_capability(name)
                && !position.provides(cap)
            {
                return Some((name.as_str(), cap));
            }
            args.iter()
                .find_map(|arg| find_unavailable_call(arg, position))
        }
        Expr::BinOp { left, right, .. } => {
            find_unavailable_call(left, position).or_else(|| find_unavailable_call(right, position))
        }
        Expr::Neg(inner) | Expr::Not(inner) => find_unavailable_call(inner, position),
        Expr::Object(items) => items
            .iter()
            .find_map(|it| find_unavailable_call(&it.value, position)),
        Expr::Array(items) => items
            .iter()
            .find_map(|item| find_unavailable_call(item, position)),
        Expr::InList { expr, list, .. } => find_unavailable_call(expr, position)
            .or_else(|| list.iter().find_map(|i| find_unavailable_call(i, position))),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => find_unavailable_call(cond, position)
            .or_else(|| find_unavailable_call(then_expr, position))
            .or_else(|| find_unavailable_call(else_expr, position)),
        Expr::Match {
            expr,
            arms,
            default,
        } => find_unavailable_call(expr, position)
            .or_else(|| {
                arms.iter().find_map(|arm| {
                    find_unavailable_call(&arm.value, position).or_else(|| {
                        arm.patterns
                            .iter()
                            .find_map(|p| find_unavailable_call(p, position))
                    })
                })
            })
            .or_else(|| {
                default
                    .as_deref()
                    .and_then(|d| find_unavailable_call(d, position))
            }),
        // 叶子（字面量 / 字段 / 系统变量 / 预设参数 / ListRef）及其余变体：无调用。
        _ => None,
    }
}

/// 位置闸门：`expr` 在该位置含“缺上下文”的函数时报错。返回是否命中。
pub(crate) fn check_expr_position(
    expr: &Expr,
    position: ExprPosition,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) -> bool {
    let Some((name, cap)) = find_unavailable_call(expr, position) else {
        return false;
    };
    errors.push(CheckError {
        severity: Severity::Error,
        rule: Some(rule_name.to_string()),
        test: None,
        message: format!(
            "{name}() is not allowed in {}; {}",
            position.label(),
            position.hint(cap)
        ),
    });
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BinOp, FieldRef};

    fn call(name: &str, args: Vec<Expr>) -> Expr {
        Expr::FuncCall {
            qualifier: None,
            name: name.to_string(),
            args,
        }
    }

    fn qualified(qualifier: &str, name: &str, args: Vec<Expr>) -> Expr {
        Expr::FuncCall {
            qualifier: Some(qualifier.to_string()),
            name: name.to_string(),
            args,
        }
    }

    fn field(name: &str) -> Expr {
        Expr::Field(FieldRef::Simple(name.to_string()))
    }

    #[test]
    fn finds_direct_l3_call() {
        let e = call("first", vec![field("x")]);
        assert_eq!(
            find_unavailable_call(&e, ExprPosition::BindFilter),
            Some(("first", Capability::InstanceSeries))
        );
    }

    #[test]
    fn finds_nested_l3_call_and_ignores_other_funcs() {
        let e = Expr::BinOp {
            op: BinOp::Gt,
            left: Box::new(call("mvcount", vec![call("collect_set", vec![field("a")])])),
            right: Box::new(call("len", vec![field("b")])),
        };
        assert_eq!(
            find_unavailable_call(&e, ExprPosition::OnEach),
            Some(("collect_set", Capability::InstanceSeries))
        );
    }

    #[test]
    fn plain_expression_has_no_unavailable_call() {
        let e = Expr::BinOp {
            op: BinOp::Gt,
            left: Box::new(call("len", vec![field("a")])),
            right: Box::new(Expr::Number(0.0)),
        };
        assert_eq!(find_unavailable_call(&e, ExprPosition::OnEach), None);
    }

    #[test]
    fn finds_unavailable_call_in_match_arm() {
        let e = Expr::Match {
            expr: Box::new(field("x")),
            arms: vec![crate::ast::MatchArm {
                patterns: vec![Expr::StringLit("a".into())],
                value: call("last", vec![field("y")]),
            }],
            default: None,
        };
        assert_eq!(
            find_unavailable_call(&e, ExprPosition::EmitAt),
            Some(("last", Capability::InstanceSeries))
        );
    }

    /// 除 BindFilter（带窗口表）外，其余位置都没有窗口查找能力。
    const NO_WINDOW_LOOKUP: &[ExprPosition] = &[
        ExprPosition::Instance,
        ExprPosition::JoinBound,
        ExprPosition::EmitAt,
        ExprPosition::OnEach,
        ExprPosition::StatsWhere,
        ExprPosition::ConvExpr,
    ];

    #[test]
    fn window_lookup_available_only_in_bind_filter() {
        let e = qualified("threat_list", "has", vec![field("domain")]);
        assert_eq!(find_unavailable_call(&e, ExprPosition::BindFilter), None);
        for position in NO_WINDOW_LOOKUP {
            assert_eq!(
                find_unavailable_call(&e, *position),
                Some(("has", Capability::WindowLookup)),
                "has 在 {position:?} 应被拒绝"
            );
        }
    }

    #[test]
    fn rolling_baselines_unavailable_everywhere_gated() {
        let e = call("baseline", vec![field("x"), Expr::Number(300.0)]);
        for position in NO_WINDOW_LOOKUP {
            assert_eq!(
                find_unavailable_call(&e, *position),
                Some(("baseline", Capability::RollingBaselines)),
                "baseline 在 {position:?} 应被拒绝"
            );
        }
        // bind filter 同样无滚动状态（每次调用新建空表）。
        assert_eq!(
            find_unavailable_call(&e, ExprPosition::BindFilter),
            Some(("baseline", Capability::RollingBaselines))
        );
    }

    #[test]
    fn l3_allowed_in_instance_position() {
        for name in L3_INSTANCE_FUNCS {
            let e = call(name, vec![field("x")]);
            assert_eq!(
                find_unavailable_call(&e, ExprPosition::Instance),
                None,
                "{name} 在 instance 上下文应放行"
            );
        }
    }

    #[test]
    fn baseline_dev_is_not_gated() {
        // baseline_dev 走全局 store，与滚动状态表无关。
        let e = call("baseline_dev", vec![field("e"), field("m"), field("v")]);
        assert_eq!(find_unavailable_call(&e, ExprPosition::OnEach), None);
        assert_eq!(find_unavailable_call(&e, ExprPosition::Instance), None);
    }
}
