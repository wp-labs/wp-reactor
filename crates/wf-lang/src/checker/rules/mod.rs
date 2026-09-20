mod conv_check;
mod joins;
mod keys;
mod limits;
mod scope_build;
mod score_entity;
mod steps;
mod yield_check;
pub(crate) mod yield_version;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::ast::{
    EachClause, Expr, FieldRef, MatchClause, Measure, PipelineStage, RuleDecl, SeqClause, SeqSkip,
    StatsAgg, StatsClause,
};
use crate::checker::scope::{Scope, StatLabelInfo, StatLabelStage};
use crate::checker::types::{
    ExprPosition, ValType, check_expr_position, check_expr_type, infer_type, rule_expr_position,
};
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};
use crate::wfu_meta::WFU_PREFIX;

use super::{CheckError, Severity};

const PIPE_IN_ALIAS: &str = "_in";

/// Check a single rule declaration against the provided schemas.
pub(crate) fn check_rule(rule: &RuleDecl, schemas: &[WindowSchema], errors: &mut Vec<CheckError>) {
    let name = &rule.name;

    // 规则级 `let` 名必须唯一：scope 的 let 类型表按声明顺序覆盖（后者胜），而
    // 常量内联按声明顺序取首（前者胜）——重名会让「接受/拒绝」与绑定语义随行序
    // 变化（warp-fusion#101 review）。
    let mut let_names = HashSet::new();
    for l in &rule.lets {
        if !let_names.insert(l.name.as_str()) {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(name.to_string()),
                test: None,
                message: format!("duplicate rule-level `let` name `{}`", l.name),
            });
        }
    }

    check_let_chain_depth(rule, name, errors);

    if rule.events.decls.iter().any(|d| d.alias == PIPE_IN_ALIAS) {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(name.to_string()),
            test: None,
            message: format!(
                "event alias `{}` is reserved for pipeline stage inputs",
                PIPE_IN_ALIAS
            ),
        });
    }

    // Build scope from events block
    let mut base_scope = scope_build::build_scope(rule, schemas, name, errors);
    populate_stat_labels(&mut base_scope, &rule.match_clause);
    if let Some(stats) = &rule.stats_clause {
        // let 派生字段（2026-08-31，issue #79）：stats 规则（声明式窗口聚合）
        // 未接入 per-event let 求值，显式拒绝而非静默忽略（plan 虽编译进了
        // lets，引擎 stats 路径从不求值）。
        if !rule.lets.is_empty() {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(name.to_string()),
                test: None,
                message: "stats 规则暂不支持 `let` 派生字段（let 求值仅接入 on-each / match / deferred / close 路径）"
                    .to_string(),
            });
        }
        populate_stats_measure_labels(&mut base_scope, stats);
        check_stats_keys(stats, &base_scope, name, errors);
        check_stats_measures(stats, &base_scope, name, errors);
    }

    if rule.each_clause.is_some() && !rule.pipeline_stages.is_empty() {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(name.to_string()),
            test: None,
            message: "`on each` is not supported together with pipeline stages yet".to_string(),
        });
    }

    // P3：deferred join（`emit at`）v1 仅支持 on-each 驱动形态——match 形态无挂起
    // 承载点（rule_task 的挂起逻辑在 each 分支），否则 join 静默无输出。
    if rule.each_clause.is_none() && rule.joins.iter().any(|j| j.emit_at.is_some()) {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(name.to_string()),
            test: None,
            message: "deferred join（`emit at`）v1 仅支持 on-each 驱动形态（`on each <alias>`）；match 形态的 deferred 输出留待后续".to_string(),
        });
    }
    if rule
        .pipeline_stages
        .iter()
        .any(|stage| stage.each_clause.is_some())
    {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(name.to_string()),
            test: None,
            message: "`on each` pipeline stages are not supported yet".to_string(),
        });
    }

    if rule.pipeline_stages.is_empty() {
        if let Some(each_clause) = &rule.each_clause {
            check_each_clause(each_clause, &base_scope, name, errors);
            joins::check_joins_list(&rule.joins, schemas, &base_scope, name, errors);
            check_on_each_exprs(rule, &base_scope, errors);
        } else {
            check_stage(
                &rule.match_clause,
                &rule.joins,
                &base_scope,
                schemas,
                &rule.lets,
                true,
                name,
                errors,
            );
        }

        // Check score expression (T27)
        score_entity::check_score(rule, &base_scope, errors);

        // Check `where` post-join filter: must be a bool expression; join target
        // windows are registered in scope_build, so `join_window.field` resolves.
        if let Some(w) = &rule.r#where {
            if rule.joins.is_empty() {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(name.to_string()),
                    test: None,
                    message: "`where` requires at least one `join` clause (it filters the joined fields; use a bind filter / `on each where` for event-only conditions)"
                        .to_string(),
                });
            }
            check_expr_type(w, &base_scope, name, errors);
            // post-join `where` 的运行期求值位置：on each 规则在单事件上下文（L3
            // 恒为空 → 严格语义下静默丢输出），match/close 规则在 instance 上下文
            // （L3 可用，但 `window.has` / `baseline` 不可用）。
            check_expr_position(w, rule_expr_position(rule), name, errors);
            if let Some(t) = infer_type(w, &base_scope)
                && t != ValType::Bool
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(name.to_string()),
                    test: None,
                    message: format!("`where` expression must be bool, got {:?}", t),
                });
            }
        }

        // Check entity clause (T33)
        score_entity::check_entity(rule, &base_scope, errors);

        // Check yield clause
        yield_check::check_yield(rule, schemas, &base_scope, errors);
    } else {
        let mut stage_outputs: Vec<WindowSchema> = Vec::new();

        for (idx, stage) in rule.pipeline_stages.iter().enumerate() {
            // Pipeline stages derive their intermediate output schema from `on event`
            // steps; `on event seq`/`any` stages would produce an empty stage output
            // and silently break downstream stages — reject them.
            if stage.match_clause.seq.is_some()
                || stage.match_clause.match_mode == crate::ast::MatchMode::Any
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(name.to_string()),
                    test: None,
                    message: format!(
                        "pipeline stage {} uses `on event seq`/`any`, which is not supported in pipeline stages",
                        idx + 1
                    ),
                });
                continue;
            }
            if idx == 0 {
                let mut stage_scope = Scope::new();
                stage_scope.aliases = base_scope.aliases.clone();
                stage_scope.join_windows = base_scope.join_windows.clone();
                // `reduce ... as label` 标签注册（pipeline 分支手工建 scope，不走 build_scope）
                scope_build::register_reduce_labels(&mut stage_scope, &stage.joins);
                populate_stat_labels(&mut stage_scope, &stage.match_clause);
                check_stage(
                    &stage.match_clause,
                    &stage.joins,
                    &stage_scope,
                    schemas,
                    &[],
                    false,
                    name,
                    errors,
                );
                stage_outputs.push(build_pipeline_stage_output_schema(
                    stage,
                    &stage_scope,
                    name,
                    idx,
                    errors,
                ));
                continue;
            }

            let mut stage_scope = Scope::new();
            stage_scope
                .aliases
                .insert(PIPE_IN_ALIAS, &stage_outputs[idx - 1]);
            scope_build::register_reduce_labels(&mut stage_scope, &stage.joins);
            populate_stat_labels(&mut stage_scope, &stage.match_clause);
            check_stage(
                &stage.match_clause,
                &stage.joins,
                &stage_scope,
                schemas,
                &[],
                false,
                name,
                errors,
            );
            stage_outputs.push(build_pipeline_stage_output_schema(
                stage,
                &stage_scope,
                name,
                idx,
                errors,
            ));
        }

        let mut final_scope = Scope::new();
        if let Some(prev) = stage_outputs.last() {
            final_scope.aliases.insert(PIPE_IN_ALIAS, prev);
        }
        scope_build::register_reduce_labels(&mut final_scope, &rule.joins);
        populate_stat_labels(&mut final_scope, &rule.match_clause);
        check_stage(
            &rule.match_clause,
            &rule.joins,
            &final_scope,
            schemas,
            &[],
            false,
            name,
            errors,
        );

        // Final stage outputs (score/entity/yield) resolve against `_in`.
        score_entity::check_score(rule, &final_scope, errors);
        score_entity::check_entity(rule, &final_scope, errors);
        yield_check::check_yield(rule, schemas, &final_scope, errors);
    }

    // Check limits
    limits::check_limits(rule, name, errors);

    // Check conv (L3: requires fixed window)
    conv_check::check_conv(rule, name, errors);
}

fn populate_stat_labels(scope: &mut Scope<'_>, match_clause: &MatchClause) {
    scope.stat_labels.clear();
    for step in &match_clause.on_event {
        for branch in &step.branches {
            if let Some(label) = &branch.label {
                scope.stat_labels.insert(
                    label.clone(),
                    StatLabelInfo {
                        stage: StatLabelStage::Event,
                        uses_distinct: branch
                            .pipe
                            .transforms
                            .contains(&crate::ast::Transform::Distinct),
                        measure: branch.pipe.measure,
                    },
                );
            }
        }
    }
    if let Some(close_block) = &match_clause.on_close {
        for step in &close_block.steps {
            for branch in &step.branches {
                if let Some(label) = &branch.label {
                    scope.stat_labels.insert(
                        label.clone(),
                        StatLabelInfo {
                            stage: StatLabelStage::Close,
                            uses_distinct: branch
                                .pipe
                                .transforms
                                .contains(&crate::ast::Transform::Distinct),
                            measure: branch.pipe.measure,
                        },
                    );
                }
            }
        }
    }
    if let Some(seq) = &match_clause.seq {
        for step in &seq.steps {
            if let Some(label) = &step.branch.label {
                scope.stat_labels.insert(
                    label.clone(),
                    StatLabelInfo {
                        stage: StatLabelStage::Event,
                        uses_distinct: step
                            .branch
                            .pipe
                            .transforms
                            .contains(&crate::ast::Transform::Distinct),
                        measure: step.branch.pipe.measure,
                    },
                );
            }
        }
    }
}

/// stats 规则的 measure 标签 → `stat.value(final(label))` 校验（Close 阶段）。
///
/// stats 形态无 match 步骤, 但 yield 的 stat 选择器仍需按 measure 标签校验——
/// 标签全部落在 Close 阶段（度量在窗口关闭时产出终值）。
fn populate_stats_measure_labels(scope: &mut Scope<'_>, stats: &StatsClause) {
    for m in &stats.measures {
        scope.stat_labels.insert(
            m.label.clone(),
            StatLabelInfo {
                stage: StatLabelStage::Close,
                uses_distinct: m.agg == StatsAgg::DistinctCount,
                measure: match m.agg {
                    StatsAgg::Sum | StatsAgg::SumSq => Measure::Sum,
                    StatsAgg::Avg => Measure::Avg,
                    StatsAgg::Min => Measure::Min,
                    StatsAgg::Max => Measure::Max,
                    // Count / DistinctCount / Last / Top: 检查器只对
                    // stat.count(match_event/distinct) 细分 measure, 此处够用
                    _ => Measure::Count,
                },
            },
        );
    }
}

/// stats 桶键（`group by` / `tier`）编译期白名单。
///
/// 引擎的桶键求值只实现三种形态（`stats_exec/eval/rowkey.rs::eval_row_bucket_key`）：
/// `Field` / `bucket(field, 'day'|'hour'|'minute'|'second')` / `tier(field, b1, b2, …)`；
/// 其它表达式（字段算术、任意函数、L3 集合函数、`window.has`、`baseline`…）在行式
/// 路径恒返回 `None`，而 `exec.rs` 遇到 `None` 直接 `continue` —— **整行被跳过、桶永远
/// 为空、全程无告警**（warp-fusion#101 同类：编译放行 + 运行期静默无输出）。
/// 此前 checker 只校验度量（`check_stats_measures`），桶键完全不看。
fn check_stats_keys(
    stats: &StatsClause,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let mut err = |msg: String| {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: msg,
        });
    };
    for key in &stats.keys {
        match key {
            Expr::Field(fr) => {
                if let Err(e) = scope.resolve_field_ref(fr) {
                    err(format!("stats bucket key: {e}"));
                }
            }
            Expr::FuncCall {
                qualifier: None,
                name,
                args,
            } if name == "bucket" || name == "tier" => {
                // 首参：`bucket` 只认字段，`tier` 认字段或常量（对齐引擎 `field_value_of`）
                let first_ok = matches!(args.first(), Some(Expr::Field(_)))
                    || (name == "tier" && matches!(args.first(), Some(Expr::Number(_))));
                if !first_ok {
                    err(format!(
                        "stats bucket key `{name}(...)`: the first argument must be a field reference"
                    ));
                    continue;
                }
                if name == "bucket" {
                    // 单位白名单：未知单位 → 引擎 `bucket_unit_nanos` 返回 None → 整行被跳过
                    let unit_ok = matches!(
                        args.get(1),
                        Some(Expr::StringLit(u))
                            if matches!(u.as_str(), "day" | "hour" | "minute" | "second")
                    );
                    if !unit_ok {
                        err(
                            "stats bucket key `bucket(...)`: second argument must be one of \
                             'day' / 'hour' / 'minute' / 'second'"
                                .to_string(),
                        );
                    }
                } else if !args[1..].iter().all(|b| matches!(b, Expr::Number(_))) {
                    // 边界非数值 → 引擎侧 `collect::<Option<_>>()` 得 None → 整行被跳过
                    err(
                        "stats bucket key `tier(...)`: bounds after the field must be numeric
                         literals (ascending)"
                            .to_string(),
                    );
                }
            }
            _ => err(
                "stats bucket key must be a plain field or `bucket(field, '<unit>')` / \
                 `tier(field, b1, …)`: any other expression evaluates to `None` in the engine's \
                 bucket-key evaluator, which silently skips every row (the bucket stays empty)"
                    .to_string(),
            ),
        }
    }
}

/// stats 度量校验: source alias 存在 + field 引用可解析 + where 为 bool 表达式。
///
/// checker 原不感知 stats_clause——度量里的字段拼写错误会在运行时静默失效
/// （eval 返回 None → 不累计, 无任何告警）, 这里补上编译期拦截。
fn check_stats_measures(
    stats: &StatsClause,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    for m in &stats.measures {
        if !scope.aliases.contains_key(m.source_alias.as_str()) {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "stats measure `{}` source `{}` is not a declared event alias",
                    m.label, m.source_alias
                ),
            });
            continue; // alias 无效 → 字段/where 校验无意义
        }
        if let Some(fr) = &m.field
            && let Err(e) = scope.resolve_field_ref(fr)
        {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!("stats measure `{}`: {}", m.label, e),
            });
        }
        if let Some(w) = &m.where_expr {
            let mut fields = Vec::new();
            collect_expr_field_refs(w, &mut fields);
            for fr in fields {
                if let Err(e) = scope.resolve_field_ref(fr) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("stats measure `{}` where: {}", m.label, e),
                    });
                }
            }
            check_expr_type(w, scope, rule_name, errors);
            // stats 度量 `where` 由引擎逐行求值（stats_exec 每行建 ctx 后
            // `eval_bool_expr`）：无实例序列 / 窗口表 / 滚动状态，位置依赖函数
            // 在此恒求值为空 → 度量静默不累计（issue #101 同类位置）。
            check_expr_position(w, ExprPosition::StatsWhere, rule_name, errors);
            if let Some(t) = infer_type(w, scope)
                && t != ValType::Bool
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "stats measure `{}` where expression must be bool, got {:?}",
                        m.label, t
                    ),
                });
            }
        }
    }
}

/// 收集表达式内全部 FieldRef（供 where 的字段存在性校验）。
fn collect_expr_field_refs<'a>(expr: &'a Expr, out: &mut Vec<&'a FieldRef>) {
    match expr {
        Expr::Field(fr) => out.push(fr),
        Expr::Neg(inner) => collect_expr_field_refs(inner, out),
        Expr::Not(inner) => collect_expr_field_refs(inner, out),
        Expr::BinOp { left, right, .. } => {
            collect_expr_field_refs(left, out);
            collect_expr_field_refs(right, out);
        }
        Expr::InList {
            expr: target, list, ..
        } => {
            collect_expr_field_refs(target, out);
            for item in list {
                collect_expr_field_refs(item, out);
            }
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            collect_expr_field_refs(cond, out);
            collect_expr_field_refs(then_expr, out);
            collect_expr_field_refs(else_expr, out);
        }
        Expr::Object(items) => {
            for item in items {
                collect_expr_field_refs(&item.value, out);
            }
        }
        Expr::Array(items) => {
            for item in items {
                collect_expr_field_refs(item, out);
            }
        }
        Expr::FuncCall { args, .. } => {
            for arg in args {
                collect_expr_field_refs(arg, out);
            }
        }
        // non_exhaustive: 其余变体（字面量/系统变量等）无字段引用
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn check_stage(
    match_clause: &MatchClause,
    joins_list: &[crate::ast::JoinClause],
    scope: &Scope<'_>,
    schemas: &[WindowSchema],
    lets: &[crate::ast::LetDecl],
    derived_ok: bool,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    keys::check_match_keys_clause(
        match_clause,
        joins_list,
        scope,
        lets,
        derived_ok,
        rule_name,
        errors,
    );
    keys::check_session_gap_clause(match_clause, rule_name, errors);
    keys::check_key_mapping_clause(match_clause, scope, rule_name, errors);

    let mut labels_seen = HashSet::new();
    steps::check_match_steps(
        &match_clause.on_event,
        scope,
        rule_name,
        lets,
        errors,
        &mut labels_seen,
    );
    if let Some(ref close_block) = match_clause.on_close {
        steps::check_match_steps(
            &close_block.steps,
            scope,
            rule_name,
            lets,
            errors,
            &mut labels_seen,
        );
    }

    // `on event<accu>` — within-window accumulation: scoped to a single `on event`
    // step with no close block and no seq chain (multi-step accumulation and
    // close interplay are not defined yet).
    if match_clause.accu {
        if match_clause.on_close.is_some() {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: "on event<accu> is not supported together with an `on close` / `and close` block"
                    .to_string(),
            });
        }
        if match_clause.seq.is_some() {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: "on event<accu> is not supported with `on event seq { ... }` chain syntax"
                    .to_string(),
            });
        }
        if match_clause.on_event.len() != 1
            || match_clause
                .on_event
                .first()
                .is_some_and(|step| step.branches.len() != 1)
        {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: "on event<accu> requires exactly one step with a single branch"
                    .to_string(),
            });
        }
    }

    // Chain steps: reuse the match-step checks (alias / field / pipe) plus
    // chain-specific checks. Chain rules never have `on event`/close steps.
    if let Some(chain) = &match_clause.seq {
        let chain_steps: Vec<crate::ast::MatchStep> = chain
            .steps
            .iter()
            .map(|s| crate::ast::MatchStep {
                branches: vec![s.branch.clone()],
            })
            .collect();
        let mut chain_labels = HashSet::new();
        steps::check_match_steps(
            &chain_steps,
            scope,
            rule_name,
            lets,
            errors,
            &mut chain_labels,
        );
        check_seq(chain, match_clause.duration, rule_name, errors);
    }

    for key in &match_clause.keys {
        let key_name = match key {
            FieldRef::Simple(n) | FieldRef::Qualified(_, n) | FieldRef::Bracketed(_, n) => {
                n.as_str()
            }
            #[allow(unreachable_patterns)]
            _ => continue,
        };
        if labels_seen.contains(key_name) {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "step label `{}` conflicts with match key field of the same name",
                    key_name
                ),
            });
        }
    }

    joins::check_joins_list(joins_list, schemas, scope, rule_name, errors);
}

/// Chain-specific checks: `within` bounds vs window duration, `not` placement,
/// and `not` applied to an aggregate step.
fn check_seq(
    seq: &SeqClause,
    window_duration: Duration,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    if seq.skip == SeqSkip::ToNext {
        errors.push(CheckError {
            severity: Severity::Warning,
            rule: Some(rule_name.to_string()),
            test: None,
            message: "`skip = to_next` is deferred to L3; P2 uses `past_last` (fire-and-reset)"
                .to_string(),
        });
    }
    if let Some(first) = seq.steps.first()
        && first.neg
    {
        errors.push(CheckError {
            severity: Severity::Warning,
            rule: Some(rule_name.to_string()),
            test: None,
            message: "`not` as the first chain step anchors to the window start; verify intent"
                .to_string(),
        });
    }
    for (i, step) in seq.steps.iter().enumerate() {
        if let Some(w) = step.within
            && w > window_duration
        {
            errors.push(CheckError {
                severity: Severity::Warning,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "chain step {} `within {}s` exceeds the match window duration ({}s); within is redundant",
                    i + 1,
                    w.as_secs(),
                    window_duration.as_secs()
                ),
            });
        }
        if step.neg && step.branch.field.is_some() {
            // The runtime treats a `not` step as "no event matching the alias", ignoring
            // any field/aggregation on the branch. Reject rather than silently misbehave.
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "chain step {} is `not` but references a field aggregation (unsupported); negation must target an event alias (`has <alias>` step)",
                    i + 1
                ),
            });
        }
    }
}

fn check_each_clause(
    each_clause: &EachClause,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    if !scope.aliases.contains_key(each_clause.alias.as_str()) {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "`on each` references undeclared event alias `{}`",
                each_clause.alias
            ),
        });
    }

    if let Some(filter) = &each_clause.filter {
        check_expr_type(filter, scope, rule_name, errors);
        // filter 由引擎逐事件求值（`each_exec`：`eval_bool_expr(filter)`），该路径
        // **没有**窗口表 / 实例收集序列 / 滚动基线状态 → 位置依赖函数（`window.has`
        // / L3 集合函数 / `baseline`）在此恒求值不下 → 每个事件被静默滤掉（规则永不
        // 触发，无任何报错）。此前 L3 / `baseline` 由 `is_disallowed_on_each_func`
        // 顺带拦住，**`window.has` 不在该名单里** → 限定形态可编译通过（warp-fusion#101
        // 同类位置）。
        check_expr_position(filter, ExprPosition::OnEach, rule_name, errors);
        if let Some(t) = infer_type(filter, scope)
            && t != ValType::Bool
        {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!("`on each where` expression must be bool, got {:?}", t),
            });
        }
    }
}

fn check_on_each_exprs(rule: &RuleDecl, scope: &Scope<'_>, errors: &mut Vec<CheckError>) {
    let name = &rule.name;
    let mut exprs: Vec<&Expr> = vec![&rule.score.expr, &rule.entity.id_expr];
    if let Some(each_clause) = &rule.each_clause
        && let Some(filter) = &each_clause.filter
    {
        exprs.push(filter);
    }
    exprs.extend(rule.yield_clause.args.iter().map(|arg| &arg.value));

    for expr in exprs {
        check_on_each_expr(expr, scope, name, errors);
    }
}

fn check_on_each_expr(
    expr: &Expr,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    match expr {
        Expr::Field(FieldRef::Simple(name))
            if name == "close_reason" || scope.aliases.contains_key(name.as_str()) =>
        {
            let detail = if name == "close_reason" {
                "close_reason is not available in `on each`"
            } else {
                "set-level alias references are not allowed in `on each` expressions"
            };
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: detail.to_string(),
            });
        }
        Expr::Field(FieldRef::Qualified(_, field)) | Expr::Field(FieldRef::Bracketed(_, field))
            if field == "close_reason" =>
        {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: "close_reason is not available in `on each`".to_string(),
            });
        }
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } if is_disallowed_on_each_func(qualifier.as_deref(), name) => {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!("function `{name}` is not allowed in `on each`"),
            });
            for arg in args {
                check_on_each_expr(arg, scope, rule_name, errors);
            }
        }
        Expr::BinOp { left, right, .. } => {
            check_on_each_expr(left, scope, rule_name, errors);
            check_on_each_expr(right, scope, rule_name, errors);
        }
        Expr::Neg(inner) => check_on_each_expr(inner, scope, rule_name, errors),
        Expr::Not(inner) => check_on_each_expr(inner, scope, rule_name, errors),
        Expr::FuncCall { args, .. } => {
            for arg in args {
                check_on_each_expr(arg, scope, rule_name, errors);
            }
        }
        Expr::InList { expr, list, .. } => {
            check_on_each_expr(expr, scope, rule_name, errors);
            for item in list {
                check_on_each_expr(item, scope, rule_name, errors);
            }
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            check_on_each_expr(cond, scope, rule_name, errors);
            check_on_each_expr(then_expr, scope, rule_name, errors);
            check_on_each_expr(else_expr, scope, rule_name, errors);
        }
        Expr::SystemVar(_) | Expr::WfuMeta(_) => {}
        _ => {}
    }
}

/// `on each` 上下文里不可用的函数。
///
/// 只收 **`on each` 特有的窗口/实例状态访问器**。依赖三类运行期上下文的那批
/// （L3 集合函数 / `baseline` / `window.has`）**不在这里**——它们由 `position_gate`
/// （`ExprPosition::OnEach`）统一拒绝，单一事实源是 `is_l3_instance_func` +
/// `required_capability`；两份名单各写一遍会漂移（新加一个 L3 函数时，只有闸门
/// 会拦住它，而 `on each` 的这份名单会腐成漏网）。
fn is_disallowed_on_each_func(qualifier: Option<&str>, name: &str) -> bool {
    (qualifier == Some("stat") && matches!(name, "count" | "value"))
        || (qualifier.is_none()
            && matches!(
                name,
                "count"
                    | "sum"
                    | "avg"
                    | "min"
                    | "max"
                    | "distinct"
                    | "window_event"
                    | "match_event"
                    | "match_distinct"
                    | "trigger"
                    | "final"
            ))
}

fn build_pipeline_stage_output_schema(
    stage: &PipelineStage,
    scope: &Scope<'_>,
    rule_name: &str,
    stage_index: usize,
    errors: &mut Vec<CheckError>,
) -> WindowSchema {
    let mut fields: Vec<FieldDef> = Vec::new();
    let mut seen = HashSet::new();

    let key_fields: Vec<(String, FieldType)> =
        if let Some(mapping) = &stage.match_clause.key_mapping {
            let mut dedup = HashSet::new();
            mapping
                .iter()
                .filter_map(|item| {
                    if !dedup.insert(item.logical_name.clone()) {
                        return None;
                    }
                    let ty = scope
                        .resolve_field_ref(&item.source_field)
                        .ok()
                        .flatten()
                        .and_then(val_type_to_field_type)
                        .unwrap_or(FieldType::Base(BaseType::Chars));
                    Some((item.logical_name.clone(), ty))
                })
                .collect()
        } else {
            stage
                .match_clause
                .keys
                .iter()
                .map(|key| {
                    let name = key_output_name(key);
                    let ty = scope
                        .resolve_field_ref(key)
                        .ok()
                        .flatten()
                        .and_then(val_type_to_field_type)
                        .unwrap_or(FieldType::Base(BaseType::Chars));
                    (name, ty)
                })
                .collect()
        };

    for (name, field_type) in key_fields {
        push_stage_field(
            &mut fields,
            &mut seen,
            name,
            field_type,
            rule_name,
            stage_index,
            errors,
        );
    }

    for steps in stage
        .match_clause
        .on_close
        .as_ref()
        .map(|c| std::iter::once(&c.steps))
        .into_iter()
        .flatten()
        .chain(std::iter::once(&stage.match_clause.on_event))
    {
        for step in steps {
            for branch in &step.branches {
                let field_name = branch
                    .label
                    .clone()
                    .unwrap_or_else(|| measure_output_name(branch.pipe.measure).to_string());
                let field_type = match branch.pipe.measure {
                    Measure::Avg => FieldType::Base(BaseType::Float),
                    _ => FieldType::Base(BaseType::Digit),
                };
                push_stage_field(
                    &mut fields,
                    &mut seen,
                    field_name,
                    field_type,
                    rule_name,
                    stage_index,
                    errors,
                );
            }
        }
    }

    WindowSchema {
        name: format!("__wf_pipeline_{}_{}", rule_name, stage_index + 1),
        streams: vec![],
        time_field: None,
        over: stage.match_clause.duration,
        fields,
    }
}

fn push_stage_field(
    fields: &mut Vec<FieldDef>,
    seen: &mut HashSet<String>,
    name: String,
    field_type: FieldType,
    rule_name: &str,
    stage_index: usize,
    errors: &mut Vec<CheckError>,
) {
    if seen.insert(name.clone()) {
        fields.push(FieldDef { name, field_type });
        return;
    }
    errors.push(CheckError {
        severity: Severity::Error,
        rule: Some(rule_name.to_string()),
        test: None,
        message: format!(
            "pipeline stage {} has duplicate implicit output field `{}`",
            stage_index + 1,
            name
        ),
    });
}

fn key_output_name(key: &FieldRef) -> String {
    match key {
        FieldRef::Simple(name) => name.clone(),
        FieldRef::Qualified(_, field) | FieldRef::Bracketed(_, field) => field.clone(),
        FieldRef::Path { segments, .. } => crate::explain::format_path_segments(segments),
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

fn val_type_to_field_type(v: ValType) -> Option<FieldType> {
    match v {
        ValType::Base(bt) => Some(FieldType::Base(bt)),
        ValType::ArrayAny => Some(FieldType::ArrayAny),
        ValType::Array(bt) => Some(FieldType::Array(bt)),
        ValType::Object => Some(FieldType::Object),
        ValType::EmptyArray => Some(FieldType::ArrayAny),
        ValType::Bool => Some(FieldType::Base(BaseType::Bool)),
        ValType::Numeric => Some(FieldType::Base(BaseType::Digit)),
    }
}

/// 规则级 `let` 引用链的层数上限。
///
/// 多个下游都按链**递归**展开 let 引用（键派生 `expand_let_expr`、列式 yield
/// `inline_lets`、阈值常量内联），链长完全由用户输入决定——无上限时深链会耗尽
/// 线程栈（栈溢出不可捕获 → 进程 abort）。与表达式嵌套同口径：**5 层**。
/// 超出（或成环）在编译期报错，而不是留给下游去递归。
const MAX_LET_CHAIN: usize = crate::ast::MAX_NESTING_LEVELS;

/// 迭代式求解每条 let 的最长引用链深（不递归：否则检查本身就会栈溢出）。
///
/// 松弛 MAX+1 轮：仍在增长说明存在环（链无界），同样报错。
fn check_let_chain_depth(rule: &RuleDecl, rule_name: &str, errors: &mut Vec<CheckError>) {
    if rule.lets.is_empty() {
        return;
    }
    let refs: Vec<(String, Vec<String>)> = rule
        .lets
        .iter()
        .map(|l| {
            (
                l.name.clone(),
                crate::ast::collect_rule_let_refs(&l.expr, &rule.lets),
            )
        })
        .collect();
    let mut depth: HashMap<String, usize> = refs.iter().map(|(n, _)| (n.clone(), 1)).collect();

    for _ in 0..=MAX_LET_CHAIN {
        let mut changed = false;
        for (name, deps) in &refs {
            let mut d = 1;
            for dep in deps {
                d = d.max(depth.get(dep).copied().unwrap_or(1) + 1);
            }
            if d > depth[name] {
                depth.insert(name.clone(), d);
                if d > MAX_LET_CHAIN {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "rule-level `let` reference chain through `{name}` is deeper than {MAX_LET_CHAIN} levels (or cyclic); inline the value instead"
                        ),
                    });
                    return;
                }
                changed = true;
            }
        }
        if !changed {
            return;
        }
    }
    errors.push(CheckError {
        severity: Severity::Error,
        rule: Some(rule_name.to_string()),
        test: None,
        message: format!(
            "rule-level `let` reference chain is cyclic (deeper than {MAX_LET_CHAIN} levels)"
        ),
    });
}
