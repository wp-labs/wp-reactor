use std::collections::VecDeque;

use wf_lang::plan::MatchPlan;

use super::key::ValueKey;
use super::types::{BindData, EngineHashMap, EngineHashSet, FieldSource, RollingStats, Value};

// ---------------------------------------------------------------------------
// Internal — per-branch / per-step / per-instance state
// ---------------------------------------------------------------------------

#[derive(::moju_derive::MoJu, Debug, Clone)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub(super) struct BranchState {
    pub(super) count: u64,
    pub(super) sum: f64,
    pub(super) min: f64,
    pub(super) max: f64,
    // Values at which min/max were reached (for yield). Boxed so count/sum
    // rules carry ~16B instead of ~112B (wp-reactor#19 instance state).
    pub(super) min_val: Option<Box<Value>>,
    pub(super) max_val: Option<Box<Value>>,
    pub(super) avg_sum: f64,
    pub(super) avg_count: u64,
    /// Lazy, boxed (None = 8B vs HashSet 48B): only `distinct` transforms allocate.
    /// Foldhash (not std SipHash): the distinct set is pure internal state —
    /// no cross-process determinism contract — and foldhash inserts are ~3×
    /// cheaper on the q17-style per-event distinct hot path.
    #[allow(clippy::box_collection)] // intentional per-instance memory saving (wp-reactor#19)
    pub(super) distinct_set: Option<Box<EngineHashSet<ValueKey>>>,
    pub(super) event_first_time_nanos: Option<i64>,
    pub(super) event_last_time_nanos: Option<i64>,
    // L3: collected values for collect_set/list, first/last, stddev/percentile.
    // Lazy, boxed — only L3 collection measures allocate.
    // `VecDeque`（环形）：push_capped 维护最近 MAX 个值时 push_back + pop_front
    // 均 O(1)；旧实现用 `Vec::drain(..1)` 每 push 一次 memmove 整个数组（O(1024)），
    // q15 每事件 8 branch 收集 → 每事件 8×32KB memmove，实测占 q15 88% CPU（sample）。
    #[allow(clippy::box_collection)] // intentional per-instance memory saving (wp-reactor#19)
    pub(super) collected_values: Option<Box<VecDeque<Value>>>,
    /// Per-field value history for yield / L3 collection. Lazy, boxed — a
    /// count rule never allocates this.
    /// 值类型 VecDeque（环形）：push_capped 维护最近 MAX 个值时 O(1)；旧 Vec::drain
    /// 每 push 一次 memmove 整个数组（q15 88% CPU 的根因，见 push_capped 注释）。
    #[allow(clippy::box_collection)] // intentional per-instance memory saving (wp-reactor#19)
    pub(super) field_values: Option<Box<EngineHashMap<String, VecDeque<Value>>>>,
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
            distinct_set: None,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: None,
            field_values: None,
        }
    }

    /// Mutable access to the field-value history, allocating lazily.
    pub(super) fn field_values_mut(&mut self) -> &mut EngineHashMap<String, VecDeque<Value>> {
        self.field_values
            .get_or_insert_with(|| Box::new(EngineHashMap::default()))
    }

    /// Mutable access to the L3 collected-values list, allocating lazily.
    /// VecDeque 环形（push_back/pop_front O(1)，见 push_capped 注释）。
    pub(super) fn collected_values_mut(&mut self) -> &mut VecDeque<Value> {
        self.collected_values
            .get_or_insert_with(|| Box::new(VecDeque::new()))
    }
}

#[derive(Debug, Clone)]
pub(super) struct AliasState {
    pub(super) count: u64,
    /// Lazy, boxed — only aliases with tracked bind fields allocate.
    pub(super) field_values: Option<Box<EngineHashMap<String, VecDeque<Value>>>>,
}

impl AliasState {
    pub(super) fn new() -> Self {
        Self {
            count: 0,
            field_values: None,
        }
    }

    pub(super) fn field_values_mut(&mut self) -> &mut EngineHashMap<String, VecDeque<Value>> {
        self.field_values
            .get_or_insert_with(|| Box::new(EngineHashMap::default()))
    }
}

#[derive(::moju_derive::MoJu, Debug, Clone)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub(crate) struct StepState {
    pub(super) branch_states: Vec<BranchState>,
}

impl StepState {
    pub(crate) fn new(branch_count: usize) -> Self {
        Self {
            branch_states: (0..branch_count).map(|_| BranchState::new()).collect(),
        }
    }
}

#[derive(::moju_derive::MoJu, Debug, Clone)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub(super) struct Instance {
    // Note: no `scope_key` here — the instance key (String form) lives in the
    // `InstanceKey` that keys the instance map and the expiry heap. Storing a
    // `Vec<Value>` copy here duplicated the key for every instance, the dominant
    // per-instance memory at high entity cardinality (wp-reactor#19). The close
    // output reconstructs it from the `InstanceKey` on demand.
    pub(super) machine_id: String,
    pub(super) created_at: i64,
    pub(super) last_event_nanos: i64,
    pub(super) current_step: usize,
    pub(super) event_ok: bool,
    pub(super) event_emitted: bool,
    pub(super) step_states: Vec<StepState>,
    pub(super) completed_steps: Vec<super::types::StepData>,
    pub(super) close_step_states: Vec<StepState>,
    /// Lazy, boxed (None = 8B vs HashMap 48B): rules that never track alias
    /// bind fields don't allocate this.
    /// Lazy, boxed (None = 8B vs HashMap 48B): rules that never track alias
    /// bind fields don't allocate this.
    pub(super) alias_states: Option<Box<EngineHashMap<String, AliasState>>>,
    pub(super) baselines: EngineHashMap<String, RollingStats>,
    /// Chain negation violated — chain must not fire.
    pub(super) neg_violated: bool,
    /// Per-step satisfaction flags for `on event any` (unordered) mode, aligned
    /// with `event_steps`.
    pub(super) satisfied_flags: Vec<bool>,
    /// Estimated size of this instance at creation time (fixed per the plan +
    /// tracked bind fields). Used for O(1) memory accounting (entry-based
    /// admission charges it once; permanent removes via `remove_instance`);
    /// exact state growth is corrected by periodic `recalibrate_memory()`.
    pub(super) base_cost: usize,
}

impl Instance {
    /// Create a new instance with the given `created_at` timestamp.
    ///
    /// For sliding windows, `created_at` is the event time.
    /// For fixed windows, `created_at` is the bucket start.
    pub(super) fn new_at(plan: &MatchPlan, machine_id: String, created_at: i64) -> Self {
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
            machine_id,
            created_at,
            last_event_nanos: created_at,
            current_step: 0,
            event_ok: false,
            event_emitted: false,
            step_states,
            completed_steps: Vec::new(),
            close_step_states,
            alias_states: None,
            baselines: EngineHashMap::default(),
            neg_violated: false,
            satisfied_flags: vec![false; plan.event_steps.len()],
            base_cost: 0,
        }
    }

    pub(super) fn estimated_bytes(&self) -> usize {
        let mut size: usize = 128; // base struct overhead
        size += 32; // InstanceKey string (short ip key), per instance

        // step_states + close_step_states
        for ss in self.step_states.iter().chain(self.close_step_states.iter()) {
            for bs in &ss.branch_states {
                // base branch fields (~80 bytes) + distinct_set
                size += 80
                    + bs.distinct_set
                        .as_deref()
                        .map(|set| {
                            set.iter()
                                .map(|value| value.estimated_bytes() + 24)
                                .sum::<usize>()
                        })
                        .unwrap_or(0);
                size += bs
                    .field_values
                    .as_deref()
                    .map(|fv| {
                        fv.iter()
                            .map(|(field, values)| {
                                field.len()
                                    + 24
                                    + values.iter().map(val_estimated_bytes).sum::<usize>()
                            })
                            .sum::<usize>()
                    })
                    .unwrap_or(0);
            }
        }

        // completed_steps
        size += self.completed_steps.len() * 64;

        // alias_states
        if let Some(alias_states) = &self.alias_states {
            for (alias, state) in &**alias_states {
                size += alias.len()
                    + 24
                    + 8
                    + state
                        .field_values
                        .as_deref()
                        .map(|fv| {
                            fv.iter()
                                .map(|(field, values)| {
                                    field.len()
                                        + 24
                                        + values.iter().map(val_estimated_bytes).sum::<usize>()
                                })
                                .sum::<usize>()
                        })
                        .unwrap_or(0);
            }
        }

        // baselines
        size += self.baselines.len() * 128;

        size
    }

    /// Estimate bytes for a new instance that hasn't been created yet.
    ///
    /// Accounts for struct overhead, scope key, and empty branch states
    /// from the plan (same layout as `Instance::new` would produce).
    pub(super) fn base_estimated_bytes<E: FieldSource>(
        plan: &MatchPlan,
        _scope_key: &[Value],
        alias: &str,
        event: &E,
    ) -> usize {
        let mut size: usize = 128; // base struct overhead
        size += 32; // InstanceKey string (short ip key), per instance

        // empty branch states: 80 bytes each
        let branch_count: usize = plan
            .event_steps
            .iter()
            .chain(plan.close_steps.iter())
            .map(|sp| sp.branches.len())
            .sum();
        size += branch_count * 80;

        if plan.tracked_bind_aliases.contains(alias)
            || !plan
                .event_steps
                .iter()
                .chain(plan.close_steps.iter())
                .flat_map(|step| step.branches.iter())
                .any(|branch| branch.source == alias)
        {
            size += alias.len() + 24 + 8;
            size += estimated_tracked_event_fields_bytes(plan, alias, event);
        }

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
        self.alias_states = None;
        self.baselines.clear();
        self.neg_violated = false;
        self.satisfied_flags = vec![false; plan.event_steps.len()];
    }

    /// `on event<accu>` — after firing, reset only the "fired" state so the step
    /// re-evaluates on the next qualifying event, while KEEPING the accumulation
    /// state (branch counters, collected values/evidence, bind counts, window
    /// start) so the running cumulative values continue across fires.
    pub(super) fn rearm(&mut self, plan: &MatchPlan) {
        self.current_step = 0;
        self.event_ok = false;
        self.event_emitted = false;
        self.completed_steps.clear();
        self.neg_violated = false;
        self.satisfied_flags = vec![false; plan.event_steps.len()];
        // Kept: created_at, last_event_nanos, step_states, close_step_states,
        // alias_states, baselines, machine_id.
    }

    pub(super) fn observe_seen_event_time(&mut self, event_time_nanos: i64) {
        if event_time_nanos > self.last_event_nanos {
            self.last_event_nanos = event_time_nanos;
        }
    }
}

pub(super) fn snapshot_bind_data(
    alias_states: Option<&EngineHashMap<String, AliasState>>,
) -> Vec<BindData> {
    let Some(alias_states) = alias_states else {
        return Vec::new();
    };
    let mut aliases: Vec<_> = alias_states.keys().cloned().collect();
    aliases.sort();
    aliases
        .into_iter()
        .filter_map(|alias| {
            alias_states.get(&alias).map(|state| BindData {
                alias,
                count: state.count,
                field_values: state
                    .field_values
                    .as_deref()
                    .map(|m| {
                        m.iter()
                            .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
                            .collect()
                    })
                    .unwrap_or_default(),
            })
        })
        .collect()
}

fn val_estimated_bytes(v: &Value) -> usize {
    match v {
        Value::Str(s) => s.len() + 24,
        Value::Number(_) | Value::Bool(_) => 8,
        Value::Array(arr) => 24 + arr.iter().map(val_estimated_bytes).sum::<usize>(),
        Value::Object(map) => {
            24 + map
                .iter()
                .map(|(key, value)| key.len() + val_estimated_bytes(value))
                .sum::<usize>()
        }
    }
}

fn estimated_tracked_event_fields_bytes<E: FieldSource>(
    plan: &MatchPlan,
    alias: &str,
    event: &E,
) -> usize {
    match plan.tracked_bind_fields.get(alias) {
        Some(fields) => fields
            .iter()
            .filter_map(|field| {
                event
                    .field_value(field.as_str())
                    .map(|value| field.len() + 24 + val_estimated_bytes(&value))
            })
            .sum(),
        // No tracked set: estimate from every non-null field. `field_names` covers
        // the whole schema/map; null/missing cells read `None` → 0 bytes, matching
        // the eager event (batch_to_events drops nulls from the map).
        None => event
            .field_names()
            .into_iter()
            .filter_map(|field| {
                event
                    .field_value(field)
                    .map(|value| field.len() + 24 + val_estimated_bytes(&value))
            })
            .sum(),
    }
}
