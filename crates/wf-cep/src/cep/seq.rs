//! Chain semantics (L2): per-step `within` gaps, negation steps, and `consec`
//! strict adjacency — layered on top of the existing ordered `event_steps` machine.
//!
//! The ordered progression (step i+1 only evaluates after step i) is already
//! provided by `CepStateMachine`'s `current_step`. This module adds the
//! chain-specific constraints on top.

use std::time::Duration;

use wf_lang::plan::{BranchPlan, MatchPlan, SeqStepPlan};

use super::eval::eval_expr_ext;
use super::state::Instance;
use super::types::{FieldSource, Value, WindowLookup};
use crate::masks::GuardMasks;

/// A negation step: the event must NOT match within its window.
pub(super) struct NegCheck {
    pub(super) branch: BranchPlan,
    /// Window relative to the preceding use-step's completion.
    pub(super) within: Option<Duration>,
    /// Index (into `plan.event_steps`) of the preceding use step; `None` = window start.
    pub(super) prev_step_idx: Option<usize>,
}

/// Chain semantics for one rule, precomputed from `MatchPlan.seq`.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub(super) struct SeqRuntime {
    /// `within` per event-step index (aligned with `plan.event_steps`).
    pub(super) within: Vec<Option<Duration>>,
    pub(super) negs: Vec<NegCheck>,
    pub(super) consec: bool,
}

impl SeqRuntime {
    /// Build from the compiled chain plan. `steps` excludes nothing — negation
    /// steps are filtered here into `negs` and use-steps into `within`.
    pub(super) fn build(steps: &[SeqStepPlan], consec: bool) -> Self {
        let mut within = Vec::new();
        let mut negs = Vec::new();
        let mut use_count = 0usize;
        for step in steps {
            if step.neg {
                negs.push(NegCheck {
                    branch: step.branch.clone(),
                    within: step.within,
                    prev_step_idx: if use_count == 0 {
                        None
                    } else {
                        Some(use_count - 1)
                    },
                });
            } else {
                within.push(step.within);
                use_count += 1;
            }
        }
        Self {
            within,
            negs,
            consec,
        }
    }
}

/// Scan an incoming event against negation steps. When the event matches a neg
/// step's source within its window, mark the instance as violated.
// Hot per-row path; flat args keep it allocation-free on the neg scan loop.
#[allow(clippy::too_many_arguments)]
pub(super) fn scan_negations<E: FieldSource>(
    meta: &SeqRuntime,
    instance: &mut Instance,
    alias: &str,
    event: &E,
    now_nanos: i64,
    windows: Option<&dyn WindowLookup>,
    row: usize,
    masks: Option<&GuardMasks>,
) {
    if meta.negs.is_empty() {
        return;
    }
    for (neg_idx, neg) in meta.negs.iter().enumerate() {
        if neg.branch.source != alias {
            continue;
        }
        // Window anchor: the preceding use-step's completion (`event_last_time_nanos`).
        // The negation window is active only AFTER that step completes — a matching
        // event that arrives before it must not count as a violation.
        let anchor = match neg.prev_step_idx {
            Some(i) => {
                match instance
                    .completed_steps
                    .get(i)
                    .and_then(|sd| sd.event_last_time_nanos)
                {
                    Some(t) => t,
                    None => continue, // preceding step not yet completed → window inactive
                }
            }
            // `not` as the first chain step anchors to the window start.
            None => instance.created_at,
        };
        // Only events within [anchor, anchor + within] can violate.
        if now_nanos < anchor {
            continue;
        }
        if let Some(w) = neg.within
            && now_nanos - anchor > w.as_nanos() as i64
        {
            continue;
        }
        // Guard must pass for the event to count as a violation ("must be true"
        // semantics — a null / missing field reads false in the two-valued mask).
        let guard_ok = match masks.and_then(|m| m.neg_value(neg_idx, 0, row)) {
            Some(ok) => ok,
            None => match &neg.branch.guard {
                Some(g) => matches!(
                    eval_expr_ext(g, event, windows, &mut instance.baselines),
                    Some(Value::Bool(true))
                ),
                None => true,
            },
        };
        if guard_ok {
            instance.neg_violated = true;
        }
    }
}

/// Strict adjacency: in `consec` mode, an event whose alias does not match the
/// current step's branch breaks the chain. Returns `true` when the chain is
/// broken and the event should be discarded.
pub(super) fn consec_broken(
    meta: &SeqRuntime,
    instance: &Instance,
    plan: &MatchPlan,
    alias: &str,
) -> bool {
    if !meta.consec {
        return false;
    }
    let Some(cur) = plan.event_steps.get(instance.current_step) else {
        return false;
    };
    !cur.branches.iter().any(|b| b.source == alias)
}
