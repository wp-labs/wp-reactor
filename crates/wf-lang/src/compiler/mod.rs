use std::collections::HashSet;
use std::time::Duration;

use crate::ast::{
    BoundVal, CloseMode, EachClause, EntityClause, EntityTypeVal, EventsBlock, Expr, FieldRef,
    MatchClause, Measure, PathSegment, RuleDecl, ScoreExpr, SeqSkip, WflFile, WindowMode,
    WithinSpec, YieldClause,
};
use crate::checker::check_wfl;
use crate::plan::{
    AggPlan, BindPlan, BranchPlan, ConvChainPlan, ConvOpPlan, ConvPlan, ConvWindowPlan, EachPlan,
    EntityPlan, ExceedAction, ExprPlan, JoinCondPlan, JoinKeyPlan, JoinPlan, KeyMapPlan, LetPlan,
    LimitsPlan, MatchPlan, PatternOriginPlan, RateSpec, RulePlan, ScorePlan, SeqPlan, SeqSkipPlan,
    SeqStepPlan, SortKeyPlan, StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan,
    StepPlan, WindowSpec, YieldField, YieldPlan,
};
use crate::schema::WindowSchema;
use crate::yield_preset::expand_yield_args;
use crate::{LangReason, LangResult};
use orion_error::conversion::ToStructError;

pub mod shared_list;

#[cfg(test)]
mod tests;

/// Compile a parsed WFL file into executable `RulePlan`s.
///
/// Runs semantic checks (`check_wfl`) first; returns an error if any check
/// fails. This validates the current file against the provided schemas,
/// including intermediate-window system fields and file-local yield topology.
///
/// Contracts, use declarations, and meta blocks are stripped — only rule
/// logic is compiled.
pub fn compile_wfl(file: &WflFile, schemas: &[WindowSchema]) -> LangResult<Vec<RulePlan>> {
    // 公共允许列表引用（issue #73）先展开——checker 只见到字面 InList（既有
    // 类型检查原样生效）, 未知名/非法位置在此报错。
    let file = shared_list::resolve_shared_list_refs(file)?;
    let errors = check_wfl(&file, schemas);
    let hard_errors: Vec<_> = errors
        .iter()
        .filter(|e| e.severity == crate::checker::Severity::Error)
        .collect();
    if !hard_errors.is_empty() {
        let msgs: Vec<String> = hard_errors.iter().map(|e| e.to_string()).collect();
        return LangReason::Compile
            .to_err()
            .with_detail(format!("semantic errors:\n{}", msgs.join("\n")))
            .err();
    }
    compile_wfl_after_semantic_checks(&file, schemas)
}

pub(crate) fn compile_wfl_after_semantic_checks(
    file: &WflFile,
    schemas: &[WindowSchema],
) -> LangResult<Vec<RulePlan>> {
    let mut plans = Vec::new();
    for rule in &file.rules {
        plans.extend(compile_rule(rule, file, schemas)?);
    }
    Ok(plans)
}

fn compile_rule(
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

// ---------------------------------------------------------------------------
// Match
// ---------------------------------------------------------------------------

fn compile_match(
    mc: &MatchClause,
    inject_implicit_stage_labels: bool,
    binds: &[BindPlan],
    joins: &[crate::ast::JoinClause],
    schemas: &[WindowSchema],
) -> MatchPlan {
    let (keys, key_map) = if let Some(ref km) = mc.key_mapping {
        // When key mapping is present, use logical key names as keys
        let logical_names: Vec<FieldRef> = km
            .iter()
            .map(|item| FieldRef::Simple(item.logical_name.clone()))
            .collect();
        // Deduplicate logical names (same logical name maps from multiple sources)
        let mut seen = std::collections::HashSet::new();
        let deduped: Vec<FieldRef> = logical_names
            .into_iter()
            .filter(|f| {
                if let FieldRef::Simple(name) = f {
                    seen.insert(name.clone())
                } else {
                    true
                }
            })
            .collect();
        let key_map_plans: Vec<KeyMapPlan> = km
            .iter()
            .filter_map(|item| {
                if let FieldRef::Qualified(alias, field) = &item.source_field {
                    Some(KeyMapPlan {
                        logical_name: item.logical_name.clone(),
                        source_alias: alias.clone(),
                        source_field: field.clone(),
                    })
                } else {
                    None
                }
            })
            .collect();
        (deduped, Some(key_map_plans))
    } else {
        (mc.keys.clone(), None)
    };

    let key_join = resolve_join_key(&keys, &mc.key_mapping, binds, joins, schemas);

    MatchPlan {
        keys,
        key_map,
        key_join,
        window_spec: match mc.window_mode {
            WindowMode::Sliding => WindowSpec::Sliding(mc.duration),
            WindowMode::Fixed => WindowSpec::Fixed(mc.duration),
            WindowMode::Session(gap) => WindowSpec::Session(gap),
            WindowMode::Hop { size, slide } => WindowSpec::Hop { size, slide },
        },
        event_steps: if let Some(chain) = &mc.seq {
            // Chain rules: emit ordered use-steps into event_steps so the existing
            // ordered progression + fire-and-reset machinery drives execution.
            // Negation steps are excluded here (enforced via SeqPlan in L2).
            chain
                .steps
                .iter()
                .filter(|s| !s.neg)
                .map(|s| StepPlan {
                    branches: vec![compile_branch(&s.branch, inject_implicit_stage_labels)],
                })
                .collect()
        } else {
            mc.on_event
                .iter()
                .map(|s| compile_step(s, inject_implicit_stage_labels))
                .collect()
        },
        close_steps: mc
            .on_close
            .as_ref()
            .map(|cb| {
                cb.steps
                    .iter()
                    .map(|s| compile_step(s, inject_implicit_stage_labels))
                    .collect()
            })
            .unwrap_or_default(),
        close_mode: mc
            .on_close
            .as_ref()
            .map(|cb| cb.mode)
            .unwrap_or(CloseMode::Or),
        match_mode: mc.match_mode,
        seq: mc.seq.as_ref().map(|chain| SeqPlan {
            consec: chain.consec,
            skip: match chain.skip {
                SeqSkip::PastLast => SeqSkipPlan::PastLast,
                SeqSkip::ToNext => SeqSkipPlan::ToNext,
            },
            steps: chain
                .steps
                .iter()
                .map(|s| SeqStepPlan {
                    neg: s.neg,
                    within: s.within,
                    branch: compile_branch(&s.branch, inject_implicit_stage_labels),
                })
                .collect(),
        }),
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        accu: mc.accu,
        needs_field_history: false, // set by the caller after binds/joins/yield are known
        trigger_event_needed: false, // set by the caller (compute_trigger_event_needed)
    }
}

/// Resolve a join-then-key (Path A) descriptor for a rule whose match key is a
/// single simple field absent from every driver bind window but present on a
/// snapshot join's right window. Mirrors checker K1b/K1c — compile only runs
/// after semantic checks pass, so `Some` here means the checker accepted it.
///
/// Conservative on unknowns: if any bind window schema can't be found (e.g. a
/// generated pipeline stage window), no join key is produced — the checker
/// rejects join keys in those stages anyway.
fn resolve_join_key(
    keys: &[FieldRef],
    key_mapping: &Option<Vec<crate::ast::KeyMapItem>>,
    binds: &[BindPlan],
    joins: &[crate::ast::JoinClause],
    schemas: &[WindowSchema],
) -> Option<JoinKeyPlan> {
    // v1 constraints (checker enforces; mirror keeps the invariant):
    // exactly one simple key, no key mapping.
    if key_mapping.is_some() {
        return None;
    }
    let [FieldRef::Simple(field)] = keys else {
        return None;
    };

    // Driver field present → ordinary key. Unknown bind schema → conservative
    // None (can't prove the field is absent from the driver).
    for bind in binds {
        let schema = schemas.iter().find(|s| s.name == bind.window)?;
        if schema.fields.iter().any(|f| f.name == *field) {
            return None;
        }
    }

    // Exactly one snapshot join whose target window provides the field.
    let mut found: Vec<(usize, &crate::ast::JoinClause)> = Vec::new();
    for (idx, join) in joins.iter().enumerate() {
        if join.mode != crate::ast::JoinMode::Snapshot {
            continue;
        }
        let Some(schema) = schemas.iter().find(|s| s.name == join.target_window) else {
            continue;
        };
        if schema.fields.iter().any(|f| f.name == *field) {
            found.push((idx, join));
        }
    }
    if found.len() != 1 {
        return None;
    }
    let (join_idx, join) = found[0];

    let cond = join
        .conditions
        .first()
        .expect("checker K1c guarantees exactly one condition for join-key joins");
    let left_field = cond.left.clone();
    let right_key_field = match &cond.right {
        FieldRef::Simple(f) | FieldRef::Qualified(_, f) | FieldRef::Bracketed(_, f) => f.clone(),
        // checker rejects nested join-condition paths — defensive: no silent
        // empty key field (an empty key_field would make every join miss).
        _ => return None,
    };
    Some(JoinKeyPlan {
        join_idx,
        right_window: join.target_window.clone(),
        left_field,
        right_key_field,
        right_field: field.clone(),
        key_name: field.clone(),
    })
}

#[derive(Default)]
pub(crate) struct BindTracking {
    pub aliases: HashSet<String>,
    pub fields: std::collections::HashMap<String, HashSet<String>>,
    pub plain_fields: HashSet<String>,
}

#[cfg(test)]
pub(crate) fn collect_rule_bind_tracking_aliases(
    score_expr: &Expr,
    entity_expr: &Expr,
    yield_fields: &[YieldField],
) -> HashSet<String> {
    collect_rule_bind_tracking(score_expr, entity_expr, yield_fields).aliases
}

pub(crate) fn collect_rule_bind_tracking(
    score_expr: &Expr,
    entity_expr: &Expr,
    yield_fields: &[YieldField],
) -> BindTracking {
    let mut tracking = BindTracking::default();
    collect_bind_tracking(score_expr, &mut tracking);
    collect_bind_tracking(entity_expr, &mut tracking);
    for field in yield_fields {
        collect_bind_tracking(&field.value, &mut tracking);
    }
    tracking
}

#[cfg(test)]
pub(crate) fn collect_bind_tracking_aliases(expr: &Expr, aliases: &mut HashSet<String>) {
    let mut tracking = BindTracking {
        aliases: std::mem::take(aliases),
        fields: std::collections::HashMap::new(),
        plain_fields: HashSet::new(),
    };
    collect_bind_tracking(expr, &mut tracking);
    *aliases = tracking.aliases;
}

pub(crate) fn collect_bind_tracking(expr: &Expr, tracking: &mut BindTracking) {
    match expr {
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => {
            if qualifier.as_deref() == Some("stat") {
                collect_stat_bind_tracking(name, args, tracking);
                return;
            }
            if qualifier.is_none() && is_stat_selector(name) {
                return;
            }
            if qualifier.is_none()
                && is_series_func(name)
                && let Some(Expr::Field(
                    FieldRef::Qualified(alias, field) | FieldRef::Bracketed(alias, field),
                )) = args.first()
            {
                track_bind_field(tracking, alias, field);
            }
            for arg in args {
                collect_bind_tracking(arg, tracking);
            }
        }
        Expr::BinOp { left, right, .. } => {
            collect_bind_tracking(left, tracking);
            collect_bind_tracking(right, tracking);
        }
        Expr::Neg(inner) => collect_bind_tracking(inner, tracking),
        Expr::Not(inner) => collect_bind_tracking(inner, tracking),
        Expr::InList { expr, list, .. } => {
            collect_bind_tracking(expr, tracking);
            for item in list {
                collect_bind_tracking(item, tracking);
            }
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            collect_bind_tracking(cond, tracking);
            collect_bind_tracking(then_expr, tracking);
            collect_bind_tracking(else_expr, tracking);
        }
        Expr::Field(FieldRef::Qualified(alias, field) | FieldRef::Bracketed(alias, field)) => {
            track_bind_field(tracking, alias, field);
        }
        Expr::Field(FieldRef::Path { alias, segments }) => {
            // Track the root object/array field so it reaches the match/close
            // yield context; nested traversal happens at evaluation time.
            if let Some(PathSegment::Field(root)) = segments.first() {
                track_bind_field(tracking, alias, root);
            }
        }
        Expr::Field(FieldRef::Simple(field)) => {
            tracking.plain_fields.insert(field.clone());
        }
        Expr::Object(items) => {
            // Structured yields recurse so that field references inside
            // `object { ... }` members reach the match/close eval context too.
            for item in items {
                collect_bind_tracking(&item.value, tracking);
            }
        }
        Expr::Array(items) => {
            for item in items {
                collect_bind_tracking(item, tracking);
            }
        }
        _ => {}
    }
}

fn collect_stat_bind_tracking(name: &str, args: &[Expr], tracking: &mut BindTracking) {
    if name != "count" || args.len() != 1 {
        return;
    }
    let Expr::FuncCall {
        qualifier: None,
        name: selector,
        args: selector_args,
    } = &args[0]
    else {
        return;
    };
    if selector != "window_event" || selector_args.len() != 1 {
        return;
    }
    if let Expr::Field(FieldRef::Simple(alias)) = &selector_args[0] {
        tracking.aliases.insert(alias.clone());
    }
}

fn is_stat_selector(name: &str) -> bool {
    matches!(
        name,
        "window_event" | "match_event" | "match_distinct" | "trigger" | "final"
    )
}

fn track_bind_field(tracking: &mut BindTracking, alias: &str, field: &str) {
    tracking.aliases.insert(alias.to_string());
    tracking
        .fields
        .entry(alias.to_string())
        .or_default()
        .insert(field.to_string());
}

fn is_series_func(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "collect_set"
            | "collect_list"
            | "first"
            | "last"
            | "stddev"
            | "percentile"
    )
}

/// L3 series functions that read the `_step_field` array of collected values —
/// they require the per-field value *history*, not just the triggering event.
fn is_l3_series_func(name: &str) -> bool {
    matches!(
        name,
        "collect_set" | "collect_list" | "first" | "last" | "stddev" | "percentile"
    )
}

/// Event-accessor stat functions (`match_event`, `window_event`, `trigger`, …)
/// that read the step's collected event data — also require the history.
///
/// `final(label)` is deliberately NOT here: the eval side resolves it to the
/// close-accumulated measure value injected by `build_eval_context` (`ctx.fields
/// .get(label)`), never the per-event field history. Including it forces
/// empty-key close rules (q15: 12 measures × `stat.value(final(...))`) to keep
/// the per-event `collect_event_fields` on for nothing.
fn is_event_accessor(name: &str) -> bool {
    matches!(
        name,
        "window_event" | "match_event" | "match_distinct" | "trigger"
    )
}

/// Whether an expression references an L3 series or event-accessor function
/// anywhere (recursive) — either way the per-field value *history* is needed.
fn expr_uses_l3_series(e: &Expr) -> bool {
    match e {
        Expr::Number(_)
        | Expr::StringLit(_)
        | Expr::Bool(_)
        | Expr::SystemVar(_)
        | Expr::WfuMeta(_)
        | Expr::Field(_)
        | Expr::PresetParam(_)
        | Expr::ListRef(_) => false,
        Expr::BinOp { left, right, .. } => expr_uses_l3_series(left) || expr_uses_l3_series(right),
        Expr::Neg(inner) => expr_uses_l3_series(inner),
        Expr::Not(inner) => expr_uses_l3_series(inner),
        Expr::FuncCall { name, args, .. } => {
            is_l3_series_func(name)
                || is_event_accessor(name)
                || args.iter().any(expr_uses_l3_series)
        }
        Expr::Object(items) => items.iter().any(|i| expr_uses_l3_series(&i.value)),
        Expr::Array(items) => items.iter().any(expr_uses_l3_series),
        Expr::InList { expr, list, .. } => {
            expr_uses_l3_series(expr) || list.iter().any(expr_uses_l3_series)
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            expr_uses_l3_series(cond)
                || expr_uses_l3_series(then_expr)
                || expr_uses_l3_series(else_expr)
        }
    }
}

/// Whether the rule needs the per-field value *history* (`field_values`).
///
/// A single-bind on-event rule whose yield reads scalar fields can instead read
/// them from the triggering event (passed through `MatchedContext`), so it needs
/// no history and `collect_alias_event` can be skipped. Everything else — close
/// steps (fire with no event), multi-bind (yield may read a non-trigger alias),
/// joins, or L3 series in yield/score/entity — keeps collecting the history.
pub(crate) fn compute_needs_field_history(
    match_plan: &MatchPlan,
    binds: &[BindPlan],
    joins: &[JoinPlan],
    score_expr: &Expr,
    entity_expr: &Expr,
    yield_fields: &[YieldField],
) -> bool {
    if !match_plan.close_steps.is_empty() {
        // Close steps accumulate per event, but the field *history* (the
        // per-event `field_values` collection feeding close-time `Field`
        // resolution) is only needed when the close-path outputs reference a
        // field that resolves outside the match keys — `build_eval_context`
        // serves keys from `scope_key` with precedence over the history.
        // q12-style count rules whose yields read only the key (or literals /
        // system vars) skip the per-event `collect_alias_event` entirely.
        return close_path_reads_non_key_fields(match_plan, score_expr, entity_expr, yield_fields);
    }
    if binds.len() > 1 || !joins.is_empty() {
        return true;
    }
    if expr_uses_l3_series(score_expr)
        || expr_uses_l3_series(entity_expr)
        || yield_fields.iter().any(|f| expr_uses_l3_series(&f.value))
    {
        return true;
    }
    false
}

/// Close path needs the per-event field history iff the score/entity/yield
/// expressions read a field that is not one of the match keys (which the eval
/// ctx serves from `scope_key`) or call a function that consumes collected
/// values / alias counts (L3 series, `stat.*`, event accessors).
fn close_path_reads_non_key_fields(
    match_plan: &MatchPlan,
    score_expr: &Expr,
    entity_expr: &Expr,
    yield_fields: &[YieldField],
) -> bool {
    if expr_uses_l3_series(score_expr)
        || expr_uses_l3_series(entity_expr)
        || yield_fields.iter().any(|f| expr_uses_l3_series(&f.value))
    {
        return true;
    }
    let key_names: HashSet<&str> = match_plan
        .keys
        .iter()
        .map(crate::field_usage::field_ref_name)
        .collect();
    let mut refs = HashSet::new();
    crate::field_usage::collect_expr_fields(score_expr, &mut refs);
    crate::field_usage::collect_expr_fields(entity_expr, &mut refs);
    for f in yield_fields {
        crate::field_usage::collect_expr_fields(&f.value, &mut refs);
    }
    refs.into_iter()
        .any(|name| name.is_empty() || name.starts_with('_') || !key_names.contains(name.as_str()))
}

/// Whether on-event fires need the triggering event materialized
/// (`MatchedContext.trigger_event`).
///
/// `build_eval_context` serves match keys from `scope_key` (with precedence over
/// the history / trigger event), so an on-event fire needs the trigger event
/// only when score/entity/yield, a join condition **left field**
/// (`first_join_key` reads it from the ctx — missing ⇒ join miss ⇒ all skips,
/// the F3 lesson), or the post-join `where` reads a field that is not one of
/// the match keys. When false, the fire path skips `event.to_event()` — a
/// per-event full HashMap clone on every-event-fire rules (Q5/Q7/Q12/Q13,
/// 2026-08 nexmark hotpath bench: match+join emit 1690 → 1553 ns/evt after the
/// F3 ctx-narrowing; the clone remains and is this field's target).
fn compute_trigger_event_needed(
    match_plan: &MatchPlan,
    score_expr: &Expr,
    entity_expr: &Expr,
    yield_fields: &[YieldField],
    joins: &[JoinPlan],
    r#where: Option<&Expr>,
) -> bool {
    let key_names: HashSet<&str> = match_plan
        .keys
        .iter()
        .map(crate::field_usage::field_ref_name)
        .collect();
    let mut refs = HashSet::new();
    crate::field_usage::collect_expr_fields(score_expr, &mut refs);
    crate::field_usage::collect_expr_fields(entity_expr, &mut refs);
    for f in yield_fields {
        crate::field_usage::collect_expr_fields(&f.value, &mut refs);
    }
    for join in joins {
        for cond in &join.conds {
            crate::field_usage::collect_expr_fields(&Expr::Field(cond.left.clone()), &mut refs);
        }
    }
    if let Some(w) = r#where {
        crate::field_usage::collect_expr_fields(w, &mut refs);
    }
    refs.into_iter()
        .any(|name| name.is_empty() || name.starts_with('_') || !key_names.contains(name.as_str()))
}

fn compile_step(step: &crate::ast::MatchStep, inject_implicit_stage_labels: bool) -> StepPlan {
    StepPlan {
        branches: step
            .branches
            .iter()
            .map(|b| compile_branch(b, inject_implicit_stage_labels))
            .collect(),
    }
}

fn compile_branch(
    branch: &crate::ast::StepBranch,
    inject_implicit_stage_labels: bool,
) -> BranchPlan {
    BranchPlan {
        label: branch.label.clone().or_else(|| {
            if inject_implicit_stage_labels {
                Some(measure_output_name(branch.pipe.measure).to_string())
            } else {
                None
            }
        }),
        source: branch.source.clone(),
        field: branch.field.clone(),
        guard: branch.guard.clone(),
        agg: AggPlan {
            transforms: branch.pipe.transforms.clone(),
            measure: branch.pipe.measure,
            cmp: branch.pipe.cmp,
            threshold: branch.pipe.threshold.clone(),
        },
    }
}

// ---------------------------------------------------------------------------
// Entity
// ---------------------------------------------------------------------------

fn compile_entity(entity: &EntityClause, labels: &HashSet<String>) -> EntityPlan {
    let raw = match &entity.entity_type {
        EntityTypeVal::Ident(s) | EntityTypeVal::StringLit(s) => s.clone(),
    };
    EntityPlan {
        entity_type: raw.to_ascii_lowercase(),
        entity_id_expr: rewrite_expr_label_refs(&entity.id_expr, labels),
    }
}

// ---------------------------------------------------------------------------
// Score
// ---------------------------------------------------------------------------

fn compile_score(score: &ScoreExpr, labels: &HashSet<String>) -> ScorePlan {
    ScorePlan {
        expr: rewrite_expr_label_refs(&score.expr, labels),
    }
}

// ---------------------------------------------------------------------------
// Yield
// ---------------------------------------------------------------------------

fn compile_yield(
    yield_clause: &YieldClause,
    file: &WflFile,
    labels: &HashSet<String>,
) -> YieldPlan {
    let args = expand_yield_args(&file.yield_presets, yield_clause)
        .expect("yield presets should have been validated before compilation");
    YieldPlan {
        target: yield_clause.target.clone(),
        version: yield_clause.version,
        fields: args
            .iter()
            .map(|arg| YieldField {
                name: arg.name.clone(),
                value: rewrite_expr_label_refs(&arg.value, labels),
            })
            .collect(),
    }
}

fn compile_pipeline_stage_yield(match_clause: &MatchClause, target: String) -> YieldPlan {
    let mut fields = Vec::new();

    if let Some(key_mapping) = &match_clause.key_mapping {
        let mut seen = std::collections::HashSet::new();
        for item in key_mapping {
            let name = item.logical_name.clone();
            if !seen.insert(name.clone()) {
                continue;
            }
            fields.push(YieldField {
                name: name.clone(),
                value: crate::ast::Expr::Field(FieldRef::Simple(name)),
            });
        }
    } else {
        for key in &match_clause.keys {
            let out_name = key_output_name(key);
            fields.push(YieldField {
                name: out_name.clone(),
                value: crate::ast::Expr::Field(FieldRef::Simple(out_name)),
            });
        }
    }

    for step in &match_clause.on_event {
        for branch in &step.branches {
            let name = branch
                .label
                .clone()
                .unwrap_or_else(|| measure_output_name(branch.pipe.measure).to_string());
            fields.push(YieldField {
                name: name.clone(),
                value: crate::ast::Expr::Field(FieldRef::Simple(name)),
            });
        }
    }
    if let Some(close) = &match_clause.on_close {
        for step in &close.steps {
            for branch in &step.branches {
                let name = branch
                    .label
                    .clone()
                    .unwrap_or_else(|| measure_output_name(branch.pipe.measure).to_string());
                fields.push(YieldField {
                    name: name.clone(),
                    value: crate::ast::Expr::Field(FieldRef::Simple(name)),
                });
            }
        }
    }

    YieldPlan {
        target,
        version: None,
        fields,
    }
}

fn compile_pipeline_entity(match_keys: &[FieldRef]) -> EntityPlan {
    let entity_id_expr = match_keys
        .first()
        .map(|k| crate::ast::Expr::Field(FieldRef::Simple(key_output_name(k))))
        .unwrap_or_else(|| crate::ast::Expr::StringLit("__pipeline".to_string()));
    EntityPlan {
        entity_type: "pipeline".to_string(),
        entity_id_expr,
    }
}

fn measure_output_name(measure: Measure) -> &'static str {
    match measure {
        Measure::Count => "count",
        Measure::Sum => "sum",
        Measure::Avg => "avg",
        Measure::Min => "min",
        Measure::Max => "max",
    }
}

fn key_output_name(key: &FieldRef) -> String {
    match key {
        FieldRef::Simple(name) => name.clone(),
        FieldRef::Qualified(_, field) | FieldRef::Bracketed(_, field) => field.clone(),
        FieldRef::Path { segments, .. } => crate::explain::format_path_segments(segments),
    }
}

// ---------------------------------------------------------------------------
// Joins
// ---------------------------------------------------------------------------

fn compile_joins(joins: &[crate::ast::JoinClause]) -> Vec<JoinPlan> {
    // `as label` 引用（`label.field`）编译为 FieldRef::Path（review R2）——
    // 归约整行以裸键 object value 注入 eval context，裸名会丢限定词取错行。
    let labels: HashSet<String> = joins
        .iter()
        .filter_map(|j| j.reduce.as_ref().and_then(|r| r.label.clone()))
        .collect();
    joins
        .iter()
        .map(|j| JoinPlan {
            right_window: j.target_window.clone(),
            mode: j.mode.clone(),
            conds: j
                .conditions
                .iter()
                .map(|c| JoinCondPlan {
                    left: c.left.clone(),
                    right: c.right.clone(),
                })
                .collect(),
            within: j
                .within
                .as_ref()
                .map(|w| rewrite_within_label_refs(w, &labels)),
            reduce: j.reduce.clone(),
            emit_at: j
                .emit_at
                .as_ref()
                .map(|e| rewrite_expr_label_refs(e, &labels)),
        })
        .collect()
}

/// 将 `Qualified(label, field)` 重写为 `FieldRef::Path { alias: label, segments: [field] }`
///（`as label` 归约结果的 object 访问；review R2）。非 label 限定符原样保留。
fn rewrite_expr_label_refs(expr: &Expr, labels: &HashSet<String>) -> Expr {
    match expr {
        Expr::Field(FieldRef::Qualified(alias, field)) if labels.contains(alias) => {
            Expr::Field(FieldRef::Path {
                alias: alias.clone(),
                segments: vec![PathSegment::Field(field.clone())],
            })
        }
        Expr::Field(fr) => Expr::Field(fr.clone()),
        Expr::Number(_)
        | Expr::StringLit(_)
        | Expr::Bool(_)
        | Expr::SystemVar(_)
        | Expr::WfuMeta(_)
        | Expr::PresetParam(_)
        // 编译期已展开（resolve_shared_list_refs 在 checker 前）; 防御性保留。
        | Expr::ListRef(_) => expr.clone(),
        Expr::Object(items) => Expr::Object(
            items
                .iter()
                .map(|item| crate::ast::ObjectItem {
                    targets: item.targets.clone(),
                    type_hint: item.type_hint.clone(),
                    value: rewrite_expr_label_refs(&item.value, labels),
                })
                .collect(),
        ),
        Expr::Array(items) => Expr::Array(
            items
                .iter()
                .map(|i| rewrite_expr_label_refs(i, labels))
                .collect(),
        ),
        Expr::BinOp { op, left, right } => Expr::BinOp {
            op: *op,
            left: Box::new(rewrite_expr_label_refs(left, labels)),
            right: Box::new(rewrite_expr_label_refs(right, labels)),
        },
        Expr::Neg(inner) => Expr::Neg(Box::new(rewrite_expr_label_refs(inner, labels))),
        Expr::Not(inner) => Expr::Not(Box::new(rewrite_expr_label_refs(inner, labels))),
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => Expr::FuncCall {
            qualifier: qualifier.clone(),
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_expr_label_refs(a, labels))
                .collect(),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(rewrite_expr_label_refs(expr, labels)),
            list: list
                .iter()
                .map(|a| rewrite_expr_label_refs(a, labels))
                .collect(),
            negated: *negated,
        },
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => Expr::IfThenElse {
            cond: Box::new(rewrite_expr_label_refs(cond, labels)),
            then_expr: Box::new(rewrite_expr_label_refs(then_expr, labels)),
            else_expr: Box::new(rewrite_expr_label_refs(else_expr, labels)),
        },
    }
}

/// `within` 界表达式里的 `label.field` 引用同样重写为 Path。
fn rewrite_within_label_refs(within: &WithinSpec, labels: &HashSet<String>) -> WithinSpec {
    let rewrite_bound = |b: &crate::ast::Bound| crate::ast::Bound {
        open: b.open,
        val: match &b.val {
            BoundVal::Dur { dur, neg } => BoundVal::Dur {
                dur: *dur,
                neg: *neg,
            },
            BoundVal::Expr(e) => BoundVal::Expr(rewrite_expr_label_refs(e, labels)),
        },
    };
    WithinSpec {
        lo: rewrite_bound(&within.lo),
        hi: rewrite_bound(&within.hi),
    }
}

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

fn compile_limits(limits: &Option<crate::ast::LimitsBlock>) -> Option<LimitsPlan> {
    let limits = limits.as_ref()?;

    let mut max_memory_bytes = None;
    let mut max_instances = None;
    let mut max_throttle = None;
    let mut on_exceed = ExceedAction::Throttle; // default

    for item in &limits.items {
        match item.key.as_str() {
            "max_memory" => {
                max_memory_bytes = parse_byte_size(&item.value);
            }
            "max_instances" => {
                max_instances = item.value.parse::<usize>().ok();
            }
            "max_throttle" => {
                max_throttle = parse_rate_spec(&item.value);
            }
            "on_exceed" => {
                on_exceed = match item.value.as_str() {
                    "throttle" => ExceedAction::Throttle,
                    "drop_oldest" => ExceedAction::DropOldest,
                    "fail_rule" => ExceedAction::FailRule,
                    _ => ExceedAction::Throttle,
                };
            }
            _ => {}
        }
    }

    Some(LimitsPlan {
        max_memory_bytes,
        max_instances,
        max_throttle,
        on_exceed,
    })
}

fn parse_byte_size(s: &str) -> Option<usize> {
    let s_upper = s.to_uppercase();
    if let Some(num_str) = s_upper.strip_suffix("GB") {
        num_str
            .trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(1024)?.checked_mul(1024)?.checked_mul(1024))
    } else if let Some(num_str) = s_upper.strip_suffix("MB") {
        num_str
            .trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(1024)?.checked_mul(1024))
    } else if let Some(num_str) = s_upper.strip_suffix("KB") {
        num_str
            .trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(1024))
    } else {
        s.parse::<usize>().ok()
    }
}

fn parse_rate_spec(s: &str) -> Option<RateSpec> {
    let parts: Vec<&str> = s.splitn(2, '/').collect();
    if parts.len() != 2 {
        return None;
    }
    let count = parts[0].trim().parse::<u64>().ok()?;
    let per = match parts[1].trim() {
        "s" | "sec" => Duration::from_secs(1),
        "m" | "min" => Duration::from_secs(60),
        "h" | "hr" | "hour" => Duration::from_secs(3600),
        "d" | "day" => Duration::from_secs(86400),
        _ => return None,
    };
    Some(RateSpec { count, per })
}

// ---------------------------------------------------------------------------
// Conv
// ---------------------------------------------------------------------------

fn compile_conv(conv: &Option<crate::ast::ConvClause>) -> Option<ConvPlan> {
    let conv = conv.as_ref()?;
    Some(ConvPlan {
        chains: conv
            .chains
            .iter()
            .map(|chain| ConvChainPlan {
                ops: {
                    // 跟踪最近 Sort 键（top_ties 并列判定需要前导 sort 的键）。
                    let mut last_sort_keys: Vec<SortKeyPlan> = Vec::new();
                    chain
                        .steps
                        .iter()
                        .map(|step| match step {
                            crate::ast::ConvStep::Sort(keys) => {
                                let plans: Vec<SortKeyPlan> = keys
                                    .iter()
                                    .map(|k| SortKeyPlan {
                                        expr: k.expr.clone(),
                                        descending: k.descending,
                                    })
                                    .collect();
                                last_sort_keys = plans.clone();
                                ConvOpPlan::Sort(plans)
                            }
                            crate::ast::ConvStep::Top(n) => ConvOpPlan::Top(*n),
                            crate::ast::ConvStep::TopTies(n) => ConvOpPlan::TopTies {
                                n: *n,
                                sort_keys: last_sort_keys.clone(),
                            },
                            crate::ast::ConvStep::Dedup(e) => ConvOpPlan::Dedup(e.clone()),
                            crate::ast::ConvStep::Where(e) => ConvOpPlan::Where(e.clone()),
                        })
                        .collect()
                },
            })
            .collect(),
    })
}
