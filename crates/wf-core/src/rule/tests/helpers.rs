use std::time::Duration;

use wf_lang::ast::{BinOp, CloseMode, CmpOp, Expr, FieldRef, Measure};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StepPlan,
    WindowSpec, YieldPlan,
};

use crate::rule::match_engine::{Event, Value};

pub fn event(fields: Vec<(&str, Value)>) -> Event {
    Event {
        fields: fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    }
}

pub fn num(n: f64) -> Value {
    Value::Number(n)
}

pub fn str_val(s: &str) -> Value {
    Value::Str(s.to_string())
}

pub fn count_ge(n: f64) -> AggPlan {
    AggPlan {
        transforms: vec![],
        measure: Measure::Count,
        cmp: CmpOp::Ge,
        threshold: Expr::Number(n),
    }
}

pub fn simple_key(name: &str) -> FieldRef {
    FieldRef::Simple(name.to_string())
}

pub fn simple_plan(keys: Vec<FieldRef>, steps: Vec<StepPlan>) -> MatchPlan {
    MatchPlan {
        keys,
        key_map: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps: steps,
        close_steps: vec![],
        close_mode: CloseMode::Or,
    }
}

pub fn branch(source: &str, agg: AggPlan) -> BranchPlan {
    BranchPlan {
        label: None,
        source: source.to_string(),
        field: None,
        guard: None,
        agg,
    }
}

pub fn branch_with_label(source: &str, label: &str, agg: AggPlan) -> BranchPlan {
    BranchPlan {
        label: Some(label.to_string()),
        source: source.to_string(),
        field: None,
        guard: None,
        agg,
    }
}

pub fn step(branches: Vec<BranchPlan>) -> StepPlan {
    StepPlan { branches }
}

pub fn plan_with_close(
    keys: Vec<FieldRef>,
    event_steps: Vec<StepPlan>,
    close_steps: Vec<StepPlan>,
    window_dur: Duration,
) -> MatchPlan {
    MatchPlan {
        keys,
        key_map: None,
        window_spec: WindowSpec::Sliding(window_dur),
        event_steps,
        close_steps,
        close_mode: CloseMode::And,
    }
}

pub fn fixed_plan(keys: Vec<FieldRef>, dur: Duration, steps: Vec<StepPlan>) -> MatchPlan {
    MatchPlan {
        keys,
        key_map: None,
        window_spec: WindowSpec::Fixed(dur),
        event_steps: steps,
        close_steps: vec![],
        close_mode: CloseMode::Or,
    }
}

pub fn fixed_plan_with_close(
    keys: Vec<FieldRef>,
    dur: Duration,
    event_steps: Vec<StepPlan>,
    close_steps: Vec<StepPlan>,
) -> MatchPlan {
    MatchPlan {
        keys,
        key_map: None,
        window_spec: WindowSpec::Fixed(dur),
        event_steps,
        close_steps,
        close_mode: CloseMode::And,
    }
}

pub fn close_reason_guard(reason: &str) -> Expr {
    Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("close_reason".to_string()))),
        right: Box::new(Expr::StringLit(reason.to_string())),
    }
}

/// Build a minimal [`RulePlan`] suitable for executor tests.
///
/// Defaults:
/// - `score_expr`: the score expression (e.g. `Expr::Number(70.0)`)
/// - `entity_type`: `"ip"`
/// - `entity_id_expr`: the entity id expression
/// - `match_plan`: provided
pub fn simple_rule_plan(
    name: &str,
    match_plan: MatchPlan,
    score_expr: Expr,
    entity_type: &str,
    entity_id_expr: Expr,
) -> RulePlan {
    RulePlan {
        name: name.to_string(),
        binds: vec![BindPlan {
            alias: "fail".to_string(),
            window: "w".to_string(),
            filter: None,
        }],
        match_plan,
        joins: vec![],
        entity_plan: EntityPlan {
            entity_type: entity_type.to_string(),
            entity_id_expr,
        },
        yield_plan: YieldPlan {
            target: "alerts".to_string(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan { expr: score_expr },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    }
}
