//! 公共允许列表展开（issue #73）。
//!
//! 语法：`shared <name> = ("a", "b", ...)` 顶层声明 + 规则内 `expr in <name>`
//! （或 `expr not in <name>`）引用。解析器产出 `Expr::ListRef(name)` 占位，
//! 本模块在编译期把它展开为 shared 声明的字面元素——**checker 与运行时见不到
//! `ListRef`**（展开后只剩字面 `InList`，既有类型检查/求值路径原样生效）。
//!
//! 错误面：
//! - 引用未声明的 shared 列表 → 编译错误（带规则名 + 列表名，可定位）；
//! - `ListRef` 出现在非 `in` 右值单元素位置（如 `x in (a, "b")` 混合、或作为
//!   普通表达式）→ 编译错误；
//! - shared 列表元素自身引用 shared 列表（嵌套）→ 编译错误（不支持嵌套）。

use std::collections::HashMap;

use crate::ast::{
    BoundVal, CloseBlock, ConvClause, ConvStep, Expr, MatchClause, MatchStep, PipeChain, RuleDecl,
    SeqClause, SeqStep, SharedListDecl, SortKey, StepBranch, WflFile, WithinSpec, YieldClause,
    YieldPresetRef,
};
use crate::{LangReason, LangResult};

/// 展开文件中全部规则的 shared 列表引用；返回展开后的文件（规则外字段原样）。
pub fn resolve_shared_list_refs(file: &WflFile) -> LangResult<WflFile> {
    let mut lists: HashMap<&str, &SharedListDecl> = HashMap::new();
    for decl in &file.shared_lists {
        if lists.insert(decl.name.as_str(), decl).is_some() {
            return fail(format!(
                "shared list `{}` declared more than once",
                decl.name
            ));
        }
    }
    let mut out = file.clone();
    for rule in &mut out.rules {
        *rule = resolve_rule(rule, &lists)?;
    }
    Ok(out)
}

fn resolve_rule(rule: &RuleDecl, lists: &HashMap<&str, &SharedListDecl>) -> LangResult<RuleDecl> {
    let mut out = rule.clone();

    for decl in &mut out.events.decls {
        if let Some(f) = &decl.filter {
            decl.filter = Some(ok(resolve_expr(f, lists), &rule.name)?);
        }
    }
    for l in &mut out.lets {
        l.expr = ok(resolve_expr(&l.expr, lists), &rule.name)?;
    }
    out.match_clause = ok(resolve_match(&out.match_clause, lists), &rule.name)?;
    if let Some(each) = &mut out.each_clause {
        if let Some(f) = &each.filter {
            each.filter = Some(ok(resolve_expr(f, lists), &rule.name)?);
        }
    }
    if let Some(stats) = &mut out.stats_clause {
        for key in &mut stats.keys {
            *key = ok(resolve_expr(key, lists), &rule.name)?;
        }
        for m in &mut stats.measures {
            if let Some(w) = &m.where_expr {
                m.where_expr = Some(ok(resolve_expr(w, lists), &rule.name)?);
            }
        }
    }
    out.score.expr = ok(resolve_expr(&out.score.expr, lists), &rule.name)?;
    for join in &mut out.joins {
        if let Some(w) = &join.within {
            join.within = Some(ok(resolve_within(w, lists), &rule.name)?);
        }
        if let Some(e) = &join.emit_at {
            join.emit_at = Some(ok(resolve_expr(e, lists), &rule.name)?);
        }
    }
    if let Some(w) = &out.r#where {
        out.r#where = Some(ok(resolve_expr(w, lists), &rule.name)?);
    }
    for stage in &mut out.pipeline_stages {
        stage.match_clause = ok(resolve_match(&stage.match_clause, lists), &rule.name)?;
        if let Some(each) = &mut stage.each_clause {
            if let Some(f) = &each.filter {
                each.filter = Some(ok(resolve_expr(f, lists), &rule.name)?);
            }
        }
        for join in &mut stage.joins {
            if let Some(w) = &join.within {
                join.within = Some(ok(resolve_within(w, lists), &rule.name)?);
            }
            if let Some(e) = &join.emit_at {
                join.emit_at = Some(ok(resolve_expr(e, lists), &rule.name)?);
            }
        }
    }
    out.entity.id_expr = ok(resolve_expr(&out.entity.id_expr, lists), &rule.name)?;
    out.yield_clause = ok(resolve_yield(&out.yield_clause, lists), &rule.name)?;
    if let Some(conv) = &mut out.conv {
        *conv = ok(resolve_conv(conv, lists), &rule.name)?;
    }
    Ok(out)
}

/// 内部 `Result<_, String>` → 带规则名定位的 `LangError`。
fn ok<T>(r: Result<T, String>, rule: &str) -> LangResult<T> {
    r.map_err(|e| crate::error::error(LangReason::Compile, format!("rule `{rule}`: {e}")))
}

fn resolve_match(
    m: &MatchClause,
    lists: &HashMap<&str, &SharedListDecl>,
) -> Result<MatchClause, String> {
    let mut out = m.clone();
    out.on_event = resolve_steps(&m.on_event, lists)?;
    if let Some(close) = &m.on_close {
        out.on_close = Some(CloseBlock {
            mode: close.mode,
            steps: resolve_steps(&close.steps, lists)?,
        });
    }
    if let Some(seq) = &m.seq {
        let steps = seq
            .steps
            .iter()
            .map(|s| {
                Ok(SeqStep {
                    neg: s.neg,
                    within: s.within,
                    branch: resolve_branch(&s.branch, lists)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        out.seq = Some(SeqClause {
            consec: seq.consec,
            skip: seq.skip,
            steps,
        });
    }
    Ok(out)
}

fn resolve_steps(
    steps: &[MatchStep],
    lists: &HashMap<&str, &SharedListDecl>,
) -> Result<Vec<MatchStep>, String> {
    steps
        .iter()
        .map(|s| {
            let branches = s
                .branches
                .iter()
                .map(|b| resolve_branch(b, lists))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(MatchStep { branches })
        })
        .collect()
}

fn resolve_branch(
    b: &StepBranch,
    lists: &HashMap<&str, &SharedListDecl>,
) -> Result<StepBranch, String> {
    let mut out = b.clone();
    if let Some(g) = &b.guard {
        out.guard = Some(resolve_expr(g, lists)?);
    }
    out.pipe = PipeChain {
        transforms: b.pipe.transforms.clone(),
        measure: b.pipe.measure,
        cmp: b.pipe.cmp,
        threshold: resolve_expr(&b.pipe.threshold, lists)?,
    };
    Ok(out)
}

fn resolve_within(
    w: &WithinSpec,
    lists: &HashMap<&str, &SharedListDecl>,
) -> Result<WithinSpec, String> {
    let resolve_bound = |b: &crate::ast::Bound| -> Result<crate::ast::Bound, String> {
        Ok(crate::ast::Bound {
            open: b.open,
            val: match &b.val {
                BoundVal::Dur { dur, neg } => BoundVal::Dur {
                    dur: *dur,
                    neg: *neg,
                },
                BoundVal::Expr(e) => BoundVal::Expr(resolve_expr(e, lists)?),
            },
        })
    };
    Ok(WithinSpec {
        lo: resolve_bound(&w.lo)?,
        hi: resolve_bound(&w.hi)?,
    })
}

fn resolve_yield(
    y: &YieldClause,
    lists: &HashMap<&str, &SharedListDecl>,
) -> Result<YieldClause, String> {
    let mut out = y.clone();
    for arg in &mut out.args {
        arg.value = resolve_expr(&arg.value, lists)?;
    }
    out.presets = y
        .presets
        .iter()
        .map(|p| {
            Ok(YieldPresetRef {
                name: p.name.clone(),
                args: p
                    .args
                    .iter()
                    .map(|a| resolve_expr(a, lists))
                    .collect::<Result<Vec<_>, String>>()?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(out)
}

fn resolve_conv(
    c: &ConvClause,
    lists: &HashMap<&str, &SharedListDecl>,
) -> Result<ConvClause, String> {
    let mut out = c.clone();
    for chain in &mut out.chains {
        for step in &mut chain.steps {
            *step = resolve_conv_step(step, lists)?;
        }
    }
    Ok(out)
}

fn resolve_conv_step(
    step: &ConvStep,
    lists: &HashMap<&str, &SharedListDecl>,
) -> Result<ConvStep, String> {
    match step {
        ConvStep::Sort(keys) => Ok(ConvStep::Sort(
            keys.iter()
                .map(|k| {
                    Ok(SortKey {
                        expr: resolve_expr(&k.expr, lists)?,
                        descending: k.descending,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
        ConvStep::Dedup(e) => Ok(ConvStep::Dedup(resolve_expr(e, lists)?)),
        ConvStep::Where(e) => Ok(ConvStep::Where(resolve_expr(e, lists)?)),
        other => Ok(other.clone()),
    }
}

/// 递归展开表达式：`in <name>`（单元素 ListRef）→ shared 元素; 其余结构递归。
fn resolve_expr(expr: &Expr, lists: &HashMap<&str, &SharedListDecl>) -> Result<Expr, String> {
    match expr {
        Expr::ListRef(name) => Err(format!(
            "shared list `{name}` can only be referenced as `expr in {name}`"
        )),
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let inner = resolve_expr(inner, lists)?;
            // `in <name>` 单元素引用: 展开为 shared 字面元素。
            if let [Expr::ListRef(name)] = list.as_slice() {
                let decl = lists.get(name.as_str()).ok_or_else(|| {
                    format!("unknown shared list `{name}` (declare it with `shared {name} = (...)` before rules)")
                })?;
                for item in &decl.items {
                    if matches!(item, Expr::ListRef(_)) {
                        return Err(format!(
                            "shared list `{name}` cannot reference another shared list (nested lists unsupported)"
                        ));
                    }
                }
                return Ok(Expr::InList {
                    expr: Box::new(inner),
                    list: decl.items.clone(),
                    negated: *negated,
                });
            }
            // 字面列表: 逐元素递归（ListRef 混入字面列表会被上面的兜底错误捕获）。
            let mut list_out = Vec::with_capacity(list.len());
            for item in list {
                list_out.push(resolve_expr(item, lists)?);
            }
            Ok(Expr::InList {
                expr: Box::new(inner),
                list: list_out,
                negated: *negated,
            })
        }
        Expr::Object(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(crate::ast::ObjectItem {
                    targets: item.targets.clone(),
                    type_hint: item.type_hint.clone(),
                    value: resolve_expr(&item.value, lists)?,
                });
            }
            Ok(Expr::Object(out))
        }
        Expr::Array(items) => Ok(Expr::Array(
            items
                .iter()
                .map(|i| resolve_expr(i, lists))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Expr::BinOp { op, left, right } => Ok(Expr::BinOp {
            op: *op,
            left: Box::new(resolve_expr(left, lists)?),
            right: Box::new(resolve_expr(right, lists)?),
        }),
        Expr::Neg(inner) => Ok(Expr::Neg(Box::new(resolve_expr(inner, lists)?))),
        Expr::Not(inner) => Ok(Expr::Not(Box::new(resolve_expr(inner, lists)?))),
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => Ok(Expr::FuncCall {
            qualifier: qualifier.clone(),
            name: name.clone(),
            args: args
                .iter()
                .map(|a| resolve_expr(a, lists))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => Ok(Expr::IfThenElse {
            cond: Box::new(resolve_expr(cond, lists)?),
            then_expr: Box::new(resolve_expr(then_expr, lists)?),
            else_expr: Box::new(resolve_expr(else_expr, lists)?),
        }),
        other => Ok(other.clone()),
    }
}

fn fail(msg: String) -> LangResult<WflFile> {
    crate::error::fail(LangReason::Compile, msg)
}

#[cfg(test)]
mod tests;
