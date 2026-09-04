//! Match 结构编译（compiler/mod.rs 拆件，2026-09-04）：`compile_match`（key/key_map
//! 去重、window、on_event/on_close/seq 步骤）与 join-then-key 路径 A 解析
//! （`resolve_join_key`，镜像 checker K1b/K1c），步骤/分支编译（`compile_step`/
//! `compile_branch`）。Binds/Entity 等其余部件见 rules.rs / clause_build.rs。

use super::*;

use super::clause_build::measure_output_name;

// ---------------------------------------------------------------------------
// Match
// ---------------------------------------------------------------------------

pub(super) fn compile_match(
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
        key_exprs: Vec::new(), // 调用方（compile_regular_rule #83/#80 内联装配）填充
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
