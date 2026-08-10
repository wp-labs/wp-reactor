use std::collections::HashSet;
use std::time::Duration;

use crate::ast::{
    CloseMode, EachClause, EntityClause, EntityTypeVal, EventsBlock, Expr, FieldRef, MatchClause,
    Measure, PathSegment, RuleDecl, ScoreExpr, SeqSkip, WflFile, WindowMode, YieldClause,
};
use crate::checker::check_wfl;
use crate::plan::{
    AggPlan, BindPlan, BranchPlan, ConvChainPlan, ConvOpPlan, ConvPlan, EachPlan, EntityPlan,
    ExceedAction, JoinCondPlan, JoinPlan, KeyMapPlan, LimitsPlan, MatchPlan, PatternOriginPlan,
    RateSpec, RulePlan, ScorePlan, SeqPlan, SeqSkipPlan, SeqStepPlan, SortKeyPlan, StepPlan,
    WindowSpec, YieldField, YieldPlan,
};
use crate::schema::WindowSchema;
use crate::yield_preset::expand_yield_args;
use crate::{LangReason, LangResult};
use orion_error::conversion::ToStructError;

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
    let errors = check_wfl(file, schemas);
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
    compile_wfl_after_semantic_checks(file)
}

pub(crate) fn compile_wfl_after_semantic_checks(file: &WflFile) -> LangResult<Vec<RulePlan>> {
    let mut plans = Vec::new();
    for rule in &file.rules {
        plans.extend(compile_rule(rule, file)?);
    }
    Ok(plans)
}

fn compile_rule(rule: &RuleDecl, file: &WflFile) -> LangResult<Vec<RulePlan>> {
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
        return Ok(vec![compile_regular_rule(rule, file)]);
    }
    Ok(compile_pipeline_rule(rule, file))
}

fn compile_regular_rule(rule: &RuleDecl, file: &WflFile) -> RulePlan {
    let score_plan = compile_score(&rule.score);
    let entity_plan = compile_entity(&rule.entity);
    let yield_plan = compile_yield(&rule.yield_clause, file);
    let mut match_plan = compile_match(&rule.match_clause, false);
    let bind_tracking = collect_rule_bind_tracking(
        &score_plan.expr,
        &entity_plan.entity_id_expr,
        &yield_plan.fields,
    );
    match_plan.tracked_bind_aliases = bind_tracking.aliases;
    match_plan.tracked_bind_fields = bind_tracking.fields;
    match_plan.tracked_plain_fields = bind_tracking.plain_fields;

    RulePlan {
        name: rule.name.clone(),
        binds: compile_binds(&rule.events),
        match_plan,
        each_plan: rule.each_clause.as_ref().map(compile_each),
        joins: compile_joins(&rule.joins),
        entity_plan,
        yield_plan,
        score_plan,
        pattern_origin: rule.pattern_origin.as_ref().map(|po| PatternOriginPlan {
            pattern_name: po.pattern_name.clone(),
            args: po.args.clone(),
        }),
        conv_plan: compile_conv(&rule.conv),
        limits_plan: compile_limits(&rule.limits),
    }
}

fn compile_pipeline_rule(rule: &RuleDecl, file: &WflFile) -> Vec<RulePlan> {
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

        let mut match_plan = compile_match(match_clause, !is_final);
        let entity_plan = if is_final {
            compile_entity(&rule.entity)
        } else {
            compile_pipeline_entity(&match_plan.keys)
        };
        let yield_plan = if is_final {
            compile_yield(&rule.yield_clause, file)
        } else {
            compile_pipeline_stage_yield(match_clause, pipeline_window_name(&rule.name, idx + 1))
        };
        let score_plan = if is_final {
            compile_score(&rule.score)
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

        plans.push(RulePlan {
            name,
            binds,
            match_plan,
            each_plan: None,
            joins: compile_joins(joins),
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

fn compile_match(mc: &MatchClause, inject_implicit_stage_labels: bool) -> MatchPlan {
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

    MatchPlan {
        keys,
        key_map,
        window_spec: match mc.window_mode {
            WindowMode::Sliding => WindowSpec::Sliding(mc.duration),
            WindowMode::Fixed => WindowSpec::Fixed(mc.duration),
            WindowMode::Session(gap) => WindowSpec::Session(gap),
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
    }
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

fn compile_entity(entity: &EntityClause) -> EntityPlan {
    let raw = match &entity.entity_type {
        EntityTypeVal::Ident(s) | EntityTypeVal::StringLit(s) => s.clone(),
    };
    EntityPlan {
        entity_type: raw.to_ascii_lowercase(),
        entity_id_expr: entity.id_expr.clone(),
    }
}

// ---------------------------------------------------------------------------
// Score
// ---------------------------------------------------------------------------

fn compile_score(score: &ScoreExpr) -> ScorePlan {
    ScorePlan {
        expr: score.expr.clone(),
    }
}

// ---------------------------------------------------------------------------
// Yield
// ---------------------------------------------------------------------------

fn compile_yield(yield_clause: &YieldClause, file: &WflFile) -> YieldPlan {
    let args = expand_yield_args(&file.yield_presets, yield_clause)
        .expect("yield presets should have been validated before compilation");
    YieldPlan {
        target: yield_clause.target.clone(),
        version: yield_clause.version,
        fields: args
            .iter()
            .map(|arg| YieldField {
                name: arg.name.clone(),
                value: arg.value.clone(),
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
        })
        .collect()
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
                ops: chain
                    .steps
                    .iter()
                    .map(|step| match step {
                        crate::ast::ConvStep::Sort(keys) => ConvOpPlan::Sort(
                            keys.iter()
                                .map(|k| SortKeyPlan {
                                    expr: k.expr.clone(),
                                    descending: k.descending,
                                })
                                .collect(),
                        ),
                        crate::ast::ConvStep::Top(n) => ConvOpPlan::Top(*n),
                        crate::ast::ConvStep::Dedup(e) => ConvOpPlan::Dedup(e.clone()),
                        crate::ast::ConvStep::Where(e) => ConvOpPlan::Where(e.clone()),
                    })
                    .collect(),
            })
            .collect(),
    })
}
