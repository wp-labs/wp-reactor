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
    // L3: collected values for collect_set/list, first/last, stddev/percentile
    pub(super) collected_values: Vec<Value>,
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
            collected_values: Vec::new(),
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
    pub(super) last_event_nanos: i64,
    pub(super) current_step: usize,
    pub(super) event_ok: bool,
    pub(super) event_emitted: bool,
    pub(super) step_states: Vec<StepState>,
    pub(super) completed_steps: Vec<super::types::StepData>,
    pub(super) close_step_states: Vec<StepState>,
    pub(super) baselines: HashMap<String, RollingStats>,
}

impl Instance {
    /// Create a new instance with the given `created_at` timestamp.
    ///
    /// For sliding windows, `created_at` is the event time.
    /// For fixed windows, `created_at` is the bucket start.
    pub(super) fn new_at(plan: &MatchPlan, scope_key: Vec<Value>, created_at: i64) -> Self {
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
            created_at,
            last_event_nanos: created_at,
            current_step: 0,
            event_ok: false,
            event_emitted: false,
            step_states,
            completed_steps: Vec::new(),
            close_step_states,
            baselines: HashMap::new(),
        }
    }

    pub(super) fn estimated_bytes(&self) -> usize {
        let mut size: usize = 128; // base struct overhead

        // scope_key
        for val in &self.scope_key {
            size += val_estimated_bytes(val);
        }

        // step_states + close_step_states
        for ss in self.step_states.iter().chain(self.close_step_states.iter()) {
            for bs in &ss.branch_states {
                // base branch fields (~80 bytes) + distinct_set
                size += 80 + bs.distinct_set.iter().map(|s| s.len() + 24).sum::<usize>();
            }
        }

        // completed_steps
        size += self.completed_steps.len() * 64;

        // baselines
        size += self.baselines.len() * 128;

        size
    }

    /// Estimate bytes for a new instance that hasn't been created yet.
    ///
    /// Accounts for struct overhead, scope key, and empty branch states
    /// from the plan (same layout as `Instance::new` would produce).
    pub(super) fn base_estimated_bytes(plan: &MatchPlan, scope_key: &[Value]) -> usize {
        let mut size: usize = 128; // base struct overhead

        for val in scope_key {
            size += val_estimated_bytes(val);
        }

        // empty branch states: 80 bytes each
        let branch_count: usize = plan
            .event_steps
            .iter()
            .chain(plan.close_steps.iter())
            .map(|sp| sp.branches.len())
            .sum();
        size += branch_count * 80;

        size
    }

    pub(super) fn reset(&mut self, plan: &MatchPlan, created_at: i64) {
        self.created_at = created_at;
        self.last_event_nanos = created_at;
        self.current_step = 0;
        self.event_ok = false;
        self.event_emitted = false;
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

fn val_estimated_bytes(v: &Value) -> usize {
    match v {
        Value::Str(s) => s.len() + 24,
        Value::Number(_) | Value::Bool(_) => 8,
        Value::Array(arr) => {
            24 + arr.iter().map(val_estimated_bytes).sum::<usize>()
        }
    }
}
