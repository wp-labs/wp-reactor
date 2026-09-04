//! 规则部件 plan 构建（compiler/mod.rs 拆件，2026-09-04）：entity / score / yield
//! （含 pipeline stage 形态）/ joins（`as label` 归约引用重写 review R2）/ limits /
//! conv 的 AST clause → plan 编译，及 measure/key 输出命名助手。

use super::*;

// ---------------------------------------------------------------------------
// Entity
// ---------------------------------------------------------------------------

pub(super) fn compile_entity(entity: &EntityClause, labels: &HashSet<String>) -> EntityPlan {
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

pub(super) fn compile_score(score: &ScoreExpr, labels: &HashSet<String>) -> ScorePlan {
    ScorePlan {
        expr: rewrite_expr_label_refs(&score.expr, labels),
    }
}

// ---------------------------------------------------------------------------
// Yield
// ---------------------------------------------------------------------------

pub(super) fn compile_yield(
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

pub(super) fn compile_pipeline_stage_yield(
    match_clause: &MatchClause,
    target: String,
) -> YieldPlan {
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

pub(super) fn compile_pipeline_entity(match_keys: &[FieldRef]) -> EntityPlan {
    let entity_id_expr = match_keys
        .first()
        .map(|k| crate::ast::Expr::Field(FieldRef::Simple(key_output_name(k))))
        .unwrap_or_else(|| crate::ast::Expr::StringLit("__pipeline".to_string()));
    EntityPlan {
        entity_type: "pipeline".to_string(),
        entity_id_expr,
    }
}

pub(super) fn measure_output_name(measure: Measure) -> &'static str {
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

pub(super) fn compile_joins(joins: &[crate::ast::JoinClause]) -> Vec<JoinPlan> {
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
pub(super) fn rewrite_expr_label_refs(expr: &Expr, labels: &HashSet<String>) -> Expr {
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
        // 编译期已展开（resolve_list_refs 在 checker 前）; 防御性保留。
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
        Expr::Match {
            expr,
            arms,
            default,
        } => Expr::Match {
            expr: Box::new(rewrite_expr_label_refs(expr, labels)),
            arms: arms
                .iter()
                .map(|arm| MatchArm {
                    patterns: arm
                        .patterns
                        .iter()
                        .map(|p| rewrite_expr_label_refs(p, labels))
                        .collect(),
                    value: rewrite_expr_label_refs(&arm.value, labels),
                })
                .collect(),
            default: default
                .as_ref()
                .map(|d| Box::new(rewrite_expr_label_refs(d, labels))),
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

pub(super) fn compile_limits(limits: &Option<crate::ast::LimitsBlock>) -> Option<LimitsPlan> {
    let limits = limits.as_ref()?;

    let mut max_memory_bytes = None;
    let mut max_instances = None;
    let mut max_throttle = None;
    let mut on_exceed = ExceedAction::Throttle; // default
    let mut disk_provider = None;
    let mut max_disk_bytes = None;

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
            "disk_provider" | "spill" => {
                // `spill` 为兼容别名（2026-08-27 改名 disk_provider）: 旧键仍生效。
                disk_provider = match item.value.as_str() {
                    "redb" => Some(SpillMode::Redb),
                    _ => None,
                };
            }
            "max_disk" => {
                max_disk_bytes = parse_byte_size(&item.value);
            }
            "max_spill_bytes" => {
                // 兼容别名（2026-08-27 改名 max_disk）: 旧键仍生效。
                max_disk_bytes = parse_byte_size(&item.value);
            }
            _ => {}
        }
    }

    Some(LimitsPlan {
        max_memory_bytes,
        max_instances,
        max_throttle,
        on_exceed,
        disk_provider,
        max_disk_bytes,
    })
}

pub(crate) fn parse_byte_size(s: &str) -> Option<usize> {
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

pub(super) fn compile_conv(conv: &Option<crate::ast::ConvClause>) -> Option<ConvPlan> {
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
