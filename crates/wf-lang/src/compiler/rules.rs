//! 规则级编译（compiler/mod.rs 拆件，2026-09-04）：规则派发（`compile_rule`，each/
//! pipeline 形态互斥门）与 RulePlan 装配——regular / stats / pipeline 三种形态，
//! 含 stats 窗口/度量/字段投影、pipeline 分阶段命名与中间窗 yield、match key 的
//! let 派生内联（#83/#80）、conv 自动聚合窗（P2c）等组内步骤。

use super::*;

use super::bind_tracking::{
    collect_rule_bind_tracking, compute_needs_field_history, compute_trigger_event_needed,
};
use super::clause_build::{
    compile_conv, compile_entity, compile_joins, compile_limits, compile_pipeline_entity,
    compile_pipeline_stage_yield, compile_score, compile_yield, rewrite_expr_label_refs,
};
use super::match_build::compile_match;

pub(super) fn compile_rule(
    rule: &RuleDecl,
    file: &WflFile,
    schemas: &[WindowSchema],
) -> LangResult<Vec<RulePlan>> {
    if rule.each_clause.is_some() && !rule.pipeline_stages.is_empty() {
        return LangReason::Compile
            .to_err()
            .with_detail("`on each` is not supported together with pipeline stages yet")
            .err();
    }
    if rule
        .pipeline_stages
        .iter()
        .any(|stage| stage.each_clause.is_some())
    {
        return LangReason::Compile
            .to_err()
            .with_detail("`on each` pipeline stages are not supported yet")
            .err();
    }
    if rule.pipeline_stages.is_empty() {
        return Ok(vec![compile_regular_rule(rule, file, schemas)]);
    }
    Ok(compile_pipeline_rule(rule, file, schemas))
}

// ---------------------------------------------------------------------------
// Stats rule compilation（P1 步骤②）
// ---------------------------------------------------------------------------

/// 空 MatchPlan（stats 规则不使用 CEP 路径; 占位使 RulePlan 结构完整）。
fn empty_match_plan() -> MatchPlan {
    MatchPlan {
        keys: Vec::new(),
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Fixed(Duration::from_secs(0)),
        event_steps: Vec::new(),
        close_steps: Vec::new(),
        close_mode: crate::ast::CloseMode::Or,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        match_mode: crate::ast::MatchMode::Seq,
        seq: None,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

/// 编译 stats 规则: stats<window> [group by] [tier] { measures } + entity + yield。
/// 无 match/score/join——与 CEP 路径完全正交。
fn compile_stats_rule(
    rule: &RuleDecl,
    stats: &crate::ast::StatsClause,
    file: &WflFile,
    _schemas: &[WindowSchema],
) -> RulePlan {
    // 1. 窗口规格
    let window_spec = match stats.window.mode {
        crate::ast::StatsWindowMode::Fixed => WindowSpec::Fixed(stats.window.duration),
        crate::ast::StatsWindowMode::Session => WindowSpec::Session(stats.window.duration),
    };

    // 2. 桶键（group by + tier 统一为 ExprPlan）
    let keys: Vec<ExprPlan> = stats.keys.clone();

    // 3. 输出形状
    let output_shape = match stats.output_shape {
        crate::ast::StatsOutputShape::Rows => StatsOutputShapePlan::Rows,
        crate::ast::StatsOutputShape::Columns => StatsOutputShapePlan::Columns,
    };

    // 4. 度量
    let measures: Vec<StatsMeasurePlan> = stats
        .measures
        .iter()
        .map(|m| StatsMeasurePlan {
            label: m.label.clone(),
            source_alias: m.source_alias.clone(),
            where_expr: m.where_expr.clone(),
            agg: match m.agg {
                crate::ast::StatsAgg::Count => StatsAggPlan::Count,
                crate::ast::StatsAgg::Sum => StatsAggPlan::Sum,
                crate::ast::StatsAgg::Avg => StatsAggPlan::Avg,
                crate::ast::StatsAgg::Min => StatsAggPlan::Min,
                crate::ast::StatsAgg::Max => StatsAggPlan::Max,
                crate::ast::StatsAgg::DistinctCount => StatsAggPlan::DistinctCount,
                crate::ast::StatsAgg::Last => StatsAggPlan::Last,
                crate::ast::StatsAgg::Top => StatsAggPlan::Top,
            },
            field: m.field.clone(),
            arg: m.arg,
        })
        .collect();

    // 5. 物化字段投影: 收集度量 field + where + 桶键 引用的字段
    let mut tracked_bind_fields: std::collections::HashMap<String, HashSet<String>> =
        std::collections::HashMap::new();
    for m in &stats.measures {
        // 度量字段（distinct_count(b.bidder) → (b, bidder)）
        if let Some(fr) = &m.field
            && let FieldRef::Qualified(alias, name) = fr
        {
            tracked_bind_fields
                .entry(alias.clone())
                .or_default()
                .insert(name.clone());
        }
    }
    // where + 桶键 表达式引用的字段（粗粒度: 全部归到 measure 的 source_alias）
    let mut global_fields = HashSet::new();
    for m in &stats.measures {
        if let Some(w) = &m.where_expr {
            crate::field_usage::collect_expr_fields(w, &mut global_fields);
        }
    }
    for k in &keys {
        crate::field_usage::collect_expr_fields(k, &mut global_fields);
    }
    for m in &stats.measures {
        let entry = tracked_bind_fields
            .entry(m.source_alias.clone())
            .or_default();
        for name in &global_fields {
            if !name.is_empty() {
                entry.insert(name.clone());
            }
        }
    }

    let stats_plan = StatsPlan {
        window_spec,
        keys,
        output_shape,
        measures,
        tracked_bind_fields,
    };

    RulePlan {
        name: rule.name.clone(),
        binds: compile_binds(&rule.events),
        lets: rule
            .lets
            .iter()
            .map(|l| LetPlan {
                name: l.name.clone(),
                expr: l.expr.clone(),
            })
            .collect(),
        match_plan: empty_match_plan(),
        each_plan: None,
        stats_plan: Some(stats_plan),
        joins: Vec::new(),
        r#where: None,
        entity_plan: compile_entity(&rule.entity, &HashSet::new()),
        yield_plan: compile_yield(&rule.yield_clause, file, &HashSet::new()),
        score_plan: compile_score(&rule.score, &HashSet::new()),
        pattern_origin: None,
        conv_plan: None,
        limits_plan: compile_limits(&rule.limits),
        conv_window: None,
    }
}

/// 把 match key 引用的 let 表达式递归展开为纯事件字段表达式（issue #80）。
/// `FieldRef::Simple(let 名)` → 该 let 的 RHS（继续递归，支持 let 链）；
/// 非 let 引用（事件字段 / Qualified / Path 等）保持原样——引擎求值时按
/// 事件字段解析。`visiting` 防自引用死循环：引用自身时保留原引用（引擎
/// eval 读缺字段 → None → 事件按 key 缺失跳过，与解释路径语义一致）。
fn expand_let_expr(expr: &Expr, lets: &[LetDecl], visiting: &mut Vec<String>) -> Expr {
    match expr {
        Expr::Field(FieldRef::Simple(name)) => {
            if !visiting.iter().any(|v| v == name)
                && let Some(rhs) = lets.iter().find(|l| &l.name == name)
            {
                visiting.push(name.clone());
                let out = expand_let_expr(&rhs.expr, lets, visiting);
                visiting.pop();
                return out;
            }
            expr.clone()
        }
        Expr::BinOp { op, left, right } => Expr::BinOp {
            op: *op,
            left: Box::new(expand_let_expr(left, lets, visiting)),
            right: Box::new(expand_let_expr(right, lets, visiting)),
        },
        Expr::Neg(inner) => Expr::Neg(Box::new(expand_let_expr(inner, lets, visiting))),
        Expr::Not(inner) => Expr::Not(Box::new(expand_let_expr(inner, lets, visiting))),
        Expr::Array(items) => Expr::Array(
            items
                .iter()
                .map(|i| expand_let_expr(i, lets, visiting))
                .collect(),
        ),
        Expr::InList {
            expr: target,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(expand_let_expr(target, lets, visiting)),
            list: list
                .iter()
                .map(|i| expand_let_expr(i, lets, visiting))
                .collect(),
            negated: *negated,
        },
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => Expr::IfThenElse {
            cond: Box::new(expand_let_expr(cond, lets, visiting)),
            then_expr: Box::new(expand_let_expr(then_expr, lets, visiting)),
            else_expr: Box::new(expand_let_expr(else_expr, lets, visiting)),
        },
        Expr::Match {
            expr: subject,
            arms,
            default,
        } => Expr::Match {
            expr: Box::new(expand_let_expr(subject, lets, visiting)),
            arms: arms
                .iter()
                .map(|arm| MatchArm {
                    patterns: arm
                        .patterns
                        .iter()
                        .map(|p| expand_let_expr(p, lets, visiting))
                        .collect(),
                    value: expand_let_expr(&arm.value, lets, visiting),
                })
                .collect(),
            default: default
                .as_ref()
                .map(|d| Box::new(expand_let_expr(d, lets, visiting))),
        },
        Expr::Object(items) => Expr::Object(
            items
                .iter()
                .map(|it| crate::ast::ObjectItem {
                    targets: it.targets.clone(),
                    type_hint: it.type_hint.clone(),
                    value: expand_let_expr(&it.value, lets, visiting),
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
            args: args
                .iter()
                .map(|a| expand_let_expr(a, lets, visiting))
                .collect(),
        },
        // 叶子/无需展开：非 Simple 字段引用与字面量保持原样。
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

fn compile_regular_rule(rule: &RuleDecl, file: &WflFile, schemas: &[WindowSchema]) -> RulePlan {
    // stats 形态: 声明式窗口统计, 无 match/score/join
    if let Some(stats) = &rule.stats_clause {
        return compile_stats_rule(rule, stats, file, schemas);
    }
    // `as label` 归约标签集：`label.field` 编译为 FieldRef::Path（review R2）
    let labels: HashSet<String> = rule
        .joins
        .iter()
        .filter_map(|j| j.reduce.as_ref().and_then(|r| r.label.clone()))
        .collect();
    let score_plan = compile_score(&rule.score, &labels);
    let entity_plan = compile_entity(&rule.entity, &labels);
    let yield_plan = compile_yield(&rule.yield_clause, file, &labels);
    let binds = compile_binds(&rule.events);
    let mut match_plan = compile_match(&rule.match_clause, false, &binds, &rule.joins, schemas);
    // issue #83/#80：派生 key（match key 引用 let 绑定）编译装配。
    // - 纯字段/嵌套路径 let（#83）：内联为等值 FieldRef（与直接写嵌套路径 key
    //   聚合结果一致），key_exprs 槽位为 None——引擎按普通字段/路径提取。
    // - 表达式 let（#80，函数/字面量派生如 coalesce/concat）：无法内联成
    //   FieldRef——keys[i] 保留 `Simple(let 名)` 作逻辑名（ctx 注入/输出/摘要
    //   按此名配对 scope_key 值），key_exprs[i] 存 let RHS（递归内联引用链，
    //   得纯事件字段表达式）由引擎对事件求值。key_mapping 的 logical key 不内联。
    if match_plan.key_map.is_none() {
        // 先收集 key 名及原下标（避免遍历时可变借用 keys），再逐位内联/展开。
        // 必须保留原始位置：keys 里可能混有 Path/Qualified 键，下标错位会
        // 让 key_exprs 与 keys 错配（引擎按位 zip 提取）。
        let named: Vec<(usize, String)> = match_plan
            .keys
            .iter()
            .enumerate()
            .filter_map(|(i, k)| match k {
                FieldRef::Simple(name) => Some((i, name.clone())),
                _ => None,
            })
            .collect();
        // key_exprs 只在真正出现表达式键时分配（保持普通规则的
        // 空 Vec —— 引擎热路径与列式直读不受影响）。
        let mut key_exprs: Option<Vec<Option<ExprPlan>>> = None;
        // 被 let 派生化处理的键名集合：join-then-key 的 resolve_join_key 看不见
        // let（只看 schema 字段），若键名同时命中 join 右窗字段会产生 stale
        // key_join 与 key_exprs 并存——advance 会优先走 key_join 分支而忽略
        // 派生 key（review 1 发现）。命中后 key_join 必须让位清空。
        let mut let_derived: std::collections::HashSet<&str> = Default::default();
        for (i, name) in named.iter() {
            let Some(decl) = rule.lets.iter().find(|l| l.name.as_str() == name.as_str()) else {
                continue;
            };
            let_derived.insert(name.as_str());
            // 统一先展开 let 引用链（支持 `let a = b; let c = a` 逐级别名）；
            // 展开到纯字段/嵌套路径 → 内联为等值 FieldRef（#83，列式直读
            // 路径不变）；展开结果是复合表达式（函数/字面量派生，#80）→
            // 保留 `Simple(let 名)` 作逻辑名，表达式存进 key_exprs 槽位。
            let expanded = expand_let_expr(&decl.expr, &rule.lets, &mut Vec::new());
            match expanded {
                Expr::Field(fr) => match_plan.keys[*i] = fr,
                expr => {
                    key_exprs.get_or_insert_with(|| vec![None; match_plan.keys.len()])[*i] =
                        Some(expr);
                }
            }
        }
        if match_plan.key_join.is_some()
            && match_plan
                .key_join
                .as_ref()
                .is_some_and(|kj| let_derived.contains(kj.key_name.as_str()))
        {
            match_plan.key_join = None;
        }
        match_plan.key_exprs = key_exprs.unwrap_or_default();
    } else {
        match_plan.key_exprs = Vec::new();
    }
    let bind_tracking = collect_rule_bind_tracking(
        &score_plan.expr,
        &entity_plan.entity_id_expr,
        &yield_plan.fields,
    );
    match_plan.tracked_bind_aliases = bind_tracking.aliases;
    match_plan.tracked_bind_fields = bind_tracking.fields;
    match_plan.tracked_plain_fields = bind_tracking.plain_fields;
    let joins = compile_joins(&rule.joins);
    match_plan.needs_field_history = compute_needs_field_history(
        &match_plan,
        &binds,
        &joins,
        &score_plan.expr,
        &entity_plan.entity_id_expr,
        &yield_plan.fields,
    );
    match_plan.trigger_event_needed = compute_trigger_event_needed(
        &match_plan,
        &score_plan.expr,
        &entity_plan.entity_id_expr,
        &yield_plan.fields,
        &joins,
        rule.r#where.as_ref(),
        &rule.lets,
    );

    let conv_plan = compile_conv(&rule.conv);
    // P2c: fixed / hop conv rules get an auto-generated conv aggregation window
    // (shardable — 分片算 + conv stage 全局聚合); sliding/session conv stays
    // inline (not shardable). 2026-08-24: hop 加入——桶对齐 = slide、封口长度
    // = size（hop 实例在 window_start + size 收口，收口事件 window_start 为
    // slide 对齐，conv stage 按 slide 分桶、按 size 封口）。
    let conv_window = conv_plan
        .as_ref()
        .and_then(|_| match match_plan.window_spec.clone() {
            WindowSpec::Fixed(over) => Some(build_conv_window_plan(&match_plan, over, None)),
            WindowSpec::Hop { size, slide } => {
                Some(build_conv_window_plan(&match_plan, size, Some(slide)))
            }
            _ => None,
        });

    RulePlan {
        name: rule.name.clone(),
        binds,
        lets: rule
            .lets
            .iter()
            .map(|l| LetPlan {
                name: l.name.clone(),
                expr: l.expr.clone(),
            })
            .collect(),
        match_plan,
        each_plan: rule.each_clause.as_ref().map(compile_each),
        stats_plan: None,
        joins,
        r#where: rule
            .r#where
            .as_ref()
            .map(|w| rewrite_expr_label_refs(w, &labels)),
        entity_plan,
        yield_plan,
        score_plan,
        pattern_origin: rule.pattern_origin.as_ref().map(|po| PatternOriginPlan {
            pattern_name: po.pattern_name.clone(),
            args: po.args.clone(),
        }),
        conv_plan,
        limits_plan: compile_limits(&rule.limits),
        conv_window,
    }
}

// ---------------------------------------------------------------------------
// Conv aggregation window (P2c)
// ---------------------------------------------------------------------------

/// Build the auto-generated conv aggregation window descriptor for a
/// fixed-window conv rule.
///
/// Only `over` and `keys` are consumed at runtime (the conv stage buckets by
/// `over`); there is no materialized aggregation window, so no window schema
/// fields are derived here (P3-A).
fn build_conv_window_plan(
    match_plan: &MatchPlan,
    over: Duration,
    slide: Option<Duration>,
) -> ConvWindowPlan {
    ConvWindowPlan {
        over,
        slide,
        keys: match_plan.keys.clone(),
    }
}

fn compile_pipeline_rule(
    rule: &RuleDecl,
    file: &WflFile,
    schemas: &[WindowSchema],
) -> Vec<RulePlan> {
    const PIPE_IN_ALIAS: &str = "_in";

    let stage_count = rule.pipeline_stages.len() + 1;
    let mut plans = Vec::with_capacity(stage_count);

    for idx in 0..stage_count {
        let is_final = idx + 1 == stage_count;
        let (match_clause, joins) = if is_final {
            (&rule.match_clause, rule.joins.as_slice())
        } else {
            let stage = &rule.pipeline_stages[idx];
            (&stage.match_clause, stage.joins.as_slice())
        };

        let name = if is_final {
            rule.name.clone()
        } else {
            pipeline_rule_name(&rule.name, idx + 1)
        };

        let binds = if idx == 0 {
            compile_binds(&rule.events)
        } else {
            vec![BindPlan {
                alias: PIPE_IN_ALIAS.to_string(),
                window: pipeline_window_name(&rule.name, idx),
                filter: None,
            }]
        };

        let mut match_plan = compile_match(match_clause, !is_final, &binds, joins, schemas);
        // `as label` 归约标签集（仅最终 stage 的 score/entity/yield 可引用；
        // 非最终 stage 的 yield/entity 为自动生成，无用户表达式）。
        let labels: HashSet<String> = if is_final {
            rule.joins
                .iter()
                .filter_map(|j| j.reduce.as_ref().and_then(|r| r.label.clone()))
                .collect()
        } else {
            HashSet::new()
        };
        let entity_plan = if is_final {
            compile_entity(&rule.entity, &labels)
        } else {
            compile_pipeline_entity(&match_plan.keys)
        };
        let yield_plan = if is_final {
            compile_yield(&rule.yield_clause, file, &labels)
        } else {
            compile_pipeline_stage_yield(match_clause, pipeline_window_name(&rule.name, idx + 1))
        };
        let score_plan = if is_final {
            compile_score(&rule.score, &labels)
        } else {
            ScorePlan {
                expr: crate::ast::Expr::Number(0.0),
            }
        };
        let bind_tracking = collect_rule_bind_tracking(
            &score_plan.expr,
            &entity_plan.entity_id_expr,
            &yield_plan.fields,
        );
        match_plan.tracked_bind_aliases = bind_tracking.aliases;
        match_plan.tracked_bind_fields = bind_tracking.fields;
        match_plan.tracked_plain_fields = bind_tracking.plain_fields;
        let stage_joins = compile_joins(joins);
        match_plan.needs_field_history = compute_needs_field_history(
            &match_plan,
            &binds,
            &stage_joins,
            &score_plan.expr,
            &entity_plan.entity_id_expr,
            &yield_plan.fields,
        );

        plans.push(RulePlan {
            name,
            binds,
            lets: Vec::new(), // pipeline stages: `let` bindings not supported on stage chains (v1)
            match_plan,
            each_plan: None,
            stats_plan: None,
            joins: stage_joins,
            r#where: None, // `where` on pipeline stages rejected at parse time (v1)
            entity_plan,
            yield_plan,
            score_plan,
            pattern_origin: if is_final {
                rule.pattern_origin.as_ref().map(|po| PatternOriginPlan {
                    pattern_name: po.pattern_name.clone(),
                    args: po.args.clone(),
                })
            } else {
                None
            },
            conv_plan: if is_final {
                compile_conv(&rule.conv)
            } else {
                None
            },
            limits_plan: if is_final {
                compile_limits(&rule.limits)
            } else {
                None
            },
            conv_window: None,
        });
    }

    plans
}

fn compile_each(each_clause: &EachClause) -> EachPlan {
    EachPlan {
        alias: each_clause.alias.clone(),
        filter: each_clause.filter.clone(),
    }
}

fn pipeline_rule_name(rule_name: &str, stage_index: usize) -> String {
    format!("__wf_pipe_{}_s{}", rule_name, stage_index)
}

fn pipeline_window_name(rule_name: &str, stage_index: usize) -> String {
    format!("__wf_pipe_{}_w{}", rule_name, stage_index)
}

// ---------------------------------------------------------------------------
// Binds
// ---------------------------------------------------------------------------

fn compile_binds(events: &EventsBlock) -> Vec<BindPlan> {
    events
        .decls
        .iter()
        .map(|decl| BindPlan {
            alias: decl.alias.clone(),
            window: decl.window.clone(),
            filter: decl.filter.clone(),
        })
        .collect()
}
