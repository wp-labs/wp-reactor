use std::collections::{HashMap, HashSet};

use wf_lang::plan::MatchPlan;

use super::types::{RollingStats, Value};

// ---------------------------------------------------------------------------
// Internal — per-branch / per-step / per-instance state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(super) struct BranchState {
    pub(super) count: u64,
    pub(super) sum: f64,
    pub(super) min: f64,
    pub(super) max: f64,
    pub(super) min_val: Option<Value>,
    pub(super) max_val: Option<Value>,
    pub(super) avg_sum: f64,
    pub(super) avg_count: u64,
    pub(super) distinct_set: HashSet<String>,
}

impl BranchState {
    pub(super) fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            min_val: None,
            max_val: None,
            avg_sum: 0.0,
            avg_count: 0,
            distinct_set: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct StepState {
    pub(super) branch_states: Vec<BranchState>,
}

impl StepState {
    pub(super) fn new(branch_count: usize) -> Self {
        Self {
            branch_states: (0..branch_count).map(|_| BranchState::new()).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Instance {
    pub(super) scope_key: Vec<Value>,
    pub(super) created_at: i64,
    pub(super) current_step: usize,
    pub(super) event_ok: bool,
    pub(super) step_states: Vec<StepState>,
    pub(super) completed_steps: Vec<super::types::StepData>,
    pub(super) close_step_states: Vec<StepState>,
    pub(super) baselines: HashMap<String, RollingStats>,
}

impl Instance {
    pub(super) fn new(plan: &MatchPlan, scope_key: Vec<Value>, now_nanos: i64) -> Self {
        let step_states = plan
            .event_steps
            .iter()
            .map(|sp| StepState::new(sp.branches.len()))
            .collect();
        let close_step_states = plan
            .close_steps
            .iter()
            .map(|sp| StepState::new(sp.branches.len()))
            .collect();
        Self {
            scope_key,
            created_at: now_nanos,
            current_step: 0,
            event_ok: false,
            step_states,
            completed_steps: Vec::new(),
            close_step_states,
            baselines: HashMap::new(),
        }
    }

    pub(super) fn reset(&mut self, plan: &MatchPlan, now_nanos: i64) {
        self.created_at = now_nanos;
        self.current_step = 0;
        self.event_ok = false;
        self.step_states = plan
            .event_steps
            .iter()
            .map(|sp| StepState::new(sp.branches.len()))
            .collect();
        self.completed_steps.clear();
        self.close_step_states = plan
            .close_steps
            .iter()
            .map(|sp| StepState::new(sp.branches.len()))
            .collect();
        self.baselines.clear();
    }
}
