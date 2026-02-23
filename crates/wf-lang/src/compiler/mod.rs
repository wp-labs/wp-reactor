use crate::ast::{EntityTypeVal, RuleDecl, WflFile};
use crate::checker::check_wfl;
use crate::plan::{
    AggPlan, BindPlan, BranchPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StepPlan,
    WindowSpec, YieldField, YieldPlan,
};
use crate::schema::WindowSchema;

#[cfg(test)]
mod tests;

/// Compile a parsed WFL file into executable `RulePlan`s.
///
/// Runs semantic checks (`check_wfl`) first; returns an error if any check
/// fails.  This guarantees that a successful return implies the AST was both
/// syntactically and semantically valid — callers never need to remember to
/// call `check_wfl` separately.
///
/// Contracts, use declarations, and meta blocks are stripped — only rule
/// logic is compiled.
pub fn compile_wfl(file: &WflFile, schemas: &[WindowSchema]) -> anyhow::Result<Vec<RulePlan>> {
    let errors = check_wfl(file, schemas);
    if !errors.is_empty() {
        let msgs: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        anyhow::bail!("semantic errors:\n{}", msgs.join("\n"));
    }
    file.rules.iter().map(compile_rule).collect()
}

fn compile_rule(rule: &RuleDecl) -> anyhow::Result<RulePlan> {
    Ok(RulePlan {
        name: rule.name.clone(),
        binds: compile_binds(rule),
        match_plan: compile_match(rule),
        joins: vec![],
        entity_plan: compile_entity(rule),
        yield_plan: compile_yield(rule),
        score_plan: compile_score(rule),
        conv_plan: None,
        limits_plan: None,
    })
}

// ---------------------------------------------------------------------------
// Binds
// ---------------------------------------------------------------------------

fn compile_binds(rule: &RuleDecl) -> Vec<BindPlan> {
    rule.events
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

fn compile_match(rule: &RuleDecl) -> MatchPlan {
    let mc = &rule.match_clause;
    MatchPlan {
        keys: mc.keys.clone(),
        key_map: None,
        window_spec: WindowSpec::Sliding(mc.duration),
        event_steps: mc.on_event.iter().map(compile_step).collect(),
        close_steps: mc
            .on_close
            .as_ref()
            .map(|steps| steps.iter().map(compile_step).collect())
            .unwrap_or_default(),
    }
}

fn compile_step(step: &crate::ast::MatchStep) -> StepPlan {
    StepPlan {
        branches: step.branches.iter().map(compile_branch).collect(),
    }
}

fn compile_branch(branch: &crate::ast::StepBranch) -> BranchPlan {
    BranchPlan {
        label: branch.label.clone(),
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

fn compile_entity(rule: &RuleDecl) -> EntityPlan {
    let raw = match &rule.entity.entity_type {
        EntityTypeVal::Ident(s) | EntityTypeVal::StringLit(s) => s.clone(),
    };
    EntityPlan {
        entity_type: raw.to_ascii_lowercase(),
        entity_id_expr: rule.entity.id_expr.clone(),
    }
}

// ---------------------------------------------------------------------------
// Score
// ---------------------------------------------------------------------------

fn compile_score(rule: &RuleDecl) -> ScorePlan {
    ScorePlan {
        expr: rule.score.expr.clone(),
    }
}

// ---------------------------------------------------------------------------
// Yield
// ---------------------------------------------------------------------------

fn compile_yield(rule: &RuleDecl) -> YieldPlan {
    YieldPlan {
        target: rule.yield_clause.target.clone(),
        fields: rule
            .yield_clause
            .args
            .iter()
            .map(|arg| YieldField {
                name: arg.name.clone(),
                value: arg.value.clone(),
            })
            .collect(),
    }
}
