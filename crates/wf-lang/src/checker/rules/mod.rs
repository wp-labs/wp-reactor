mod conv_check;
mod joins;
mod keys;
mod limits;
mod scope_build;
mod score_entity;
mod steps;
mod yield_check;
pub(crate) mod yield_version;

use std::collections::HashSet;

use crate::ast::{EachClause, Expr, FieldRef, MatchClause, Measure, PipelineStage, RuleDecl};
use crate::checker::scope::Scope;
use crate::checker::types::{ValType, check_expr_type, infer_type};
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};

use super::{CheckError, Severity};

/// System fields that must not appear in yield named arguments.
const SYSTEM_FIELDS: &[&str] = &[
    "rule_name",
    "emit_time",
    "score",
    "entity_type",
    "entity_id",
    "origin",
    "score_contrib",
];
pub(crate) const WFU_PREFIX: &str = "__wfu_";

const PIPE_IN_ALIAS: &str = "_in";

/// Check a single rule declaration against the provided schemas.
pub fn check_rule(rule: &RuleDecl, schemas: &[WindowSchema], errors: &mut Vec<CheckError>) {
    let name = &rule.name;

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
    let base_scope = scope_build::build_scope(rule, schemas, name, errors);

    if rule.each_clause.is_some() && !rule.pipeline_stages.is_empty() {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(name.to_string()),
            test: None,
            message: "`on each` is not supported together with pipeline stages yet".to_string(),
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
                name,
                errors,
            );
        }

        // Check score expression (T27)
        score_entity::check_score(rule, &base_scope, errors);

        // Check entity clause (T33)
        score_entity::check_entity(rule, &base_scope, errors);

        // Check yield clause
        yield_check::check_yield(rule, schemas, &base_scope, errors);
    } else {
        let mut stage_outputs: Vec<WindowSchema> = Vec::new();

        for (idx, stage) in rule.pipeline_stages.iter().enumerate() {
            if idx == 0 {
                check_stage(
                    &stage.match_clause,
                    &stage.joins,
                    &base_scope,
                    schemas,
                    name,
                    errors,
                );
                stage_outputs.push(build_pipeline_stage_output_schema(
                    stage,
                    &base_scope,
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
            check_stage(
                &stage.match_clause,
                &stage.joins,
                &stage_scope,
                schemas,
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
        check_stage(
            &rule.match_clause,
            &rule.joins,
            &final_scope,
            schemas,
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

fn check_stage(
    match_clause: &MatchClause,
    joins_list: &[crate::ast::JoinClause],
    scope: &Scope<'_>,
    schemas: &[WindowSchema],
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    keys::check_match_keys_clause(match_clause, scope, rule_name, errors);
    keys::check_session_gap_clause(match_clause, rule_name, errors);
    keys::check_key_mapping_clause(match_clause, scope, rule_name, errors);

    let mut labels_seen = HashSet::new();
    steps::check_match_steps(
        &match_clause.on_event,
        scope,
        rule_name,
        errors,
        &mut labels_seen,
    );
    if let Some(ref close_block) = match_clause.on_close {
        steps::check_match_steps(
            &close_block.steps,
            scope,
            rule_name,
            errors,
            &mut labels_seen,
        );
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
        } if qualifier.is_none() && is_disallowed_on_each_func(name) => {
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
        Expr::SystemVar(_) => {}
        _ => {}
    }
}

fn is_disallowed_on_each_func(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "distinct"
            | "baseline"
            | "collect_set"
            | "collect_list"
            | "first"
            | "last"
            | "stddev"
            | "percentile"
    )
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
