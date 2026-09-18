use std::collections::VecDeque;

use wf_lang::plan::MatchPlan;

use super::key::ValueKey;
use super::step::push_capped;
use super::types::{BindData, EngineHashMap, EngineHashSet, FieldSource, RollingStats, Value};

// ---------------------------------------------------------------------------
// Internal — per-branch / per-step / per-instance state
// ---------------------------------------------------------------------------

#[derive(::jumo_derive::Jumo, Debug, Clone)]
#[jumo(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
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
    /// 被环形裁剪丢弃的最早值（warp-fusion#100）。Lazy, boxed — 只有事件数
    /// 超过 `MAX_TRACKED_FIELD_VALUES` 的实例才分配。
    pub(super) pinned_first: Option<Box<PinnedFirst>>,
}

/// 字段历史被有界队列裁剪掉的最早样本（warp-fusion#100）。
///
/// 历史只保留最近 `MAX_TRACKED_FIELD_VALUES` 个样本，但 `first(x)` 的语义是
/// 「实例内最早事件的值」——裁剪不能改变它，否则用 `first(x)` 组成的聚合唯一键 /
/// `alert_id` 会随窗口增长漂移（同一实例被下游识别为多条记录）。
///
/// 只在**首次**裁剪时写入：此后的裁剪丢弃的值都晚于已记录的首值，因此每个
/// 序列最多 1 个额外值，内存仍为 O(MAX)。
#[derive(Debug, Clone, Default)]
pub(super) struct PinnedFirst {
    /// branch `collected_values` 序列被丢弃的首值。
    pub(super) collected: Option<Value>,
    /// 各字段历史序列被丢弃的首值。
    pub(super) fields: EngineHashMap<String, Value>,
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
            pinned_first: None,
        }
    }

    /// Mutable access to the L3 collected-values list, allocating lazily.
    /// VecDeque 环形（push_back/pop_front O(1)，见 push_capped 注释）。
    ///
    /// 仅本模块内部使用：所有写入必须经 [`Self::push_collected`]，否则会绕过
    /// 首值钉扎（warp-fusion#100）。
    fn collected_values_mut(&mut self) -> &mut VecDeque<Value> {
        self.collected_values
            .get_or_insert_with(|| Box::new(VecDeque::new()))
    }

    /// 追加 branch 的 L3 收集值：维护 1024 上限，并钉扎首个被裁剪的值。
    pub(super) fn push_collected(&mut self, value: Value) {
        let dropped = push_capped(self.collected_values_mut(), value);
        pin_dropped_collected(&mut self.pinned_first, dropped);
    }

    /// 追加某字段的历史值：维护 1024 上限，并钉扎该字段首个被裁剪的值。
    pub(super) fn push_field_value(&mut self, field_name: &str, value: Value) {
        push_capped_field(
            &mut self.field_values,
            &mut self.pinned_first,
            field_name,
            value,
        );
    }

    /// L3 `collected_values` 的完整有界序列：`[首值] ++ 最近样本`。
    pub(super) fn collected_series(&self) -> Vec<Value> {
        series_with_pinned_first(self.collected_values.as_deref(), self.pinned_collected())
    }

    /// 各字段历史的完整有界序列（`[首值] ++ 最近样本`）。
    pub(super) fn field_series(&self) -> EngineHashMap<String, Vec<Value>> {
        field_series_with_first(
            self.field_values.as_deref(),
            self.pinned_first.as_deref().map(|p| &p.fields),
        )
    }

    fn pinned_collected(&self) -> Option<&Value> {
        self.pinned_first
            .as_deref()
            .and_then(|p| p.collected.as_ref())
    }
}

#[derive(Debug, Clone, ::jumo_derive::Jumo)]
#[jumo(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub(super) struct AliasState {
    pub(super) count: u64,
    /// Lazy, boxed — only aliases with tracked bind fields allocate.
    pub(super) field_values: Option<Box<EngineHashMap<String, VecDeque<Value>>>>,
    /// 被环形裁剪丢弃的最早值（warp-fusion#100，见 [`PinnedFirst`]）。
    pub(super) pinned_first: Option<Box<PinnedFirst>>,
}

impl AliasState {
    pub(super) fn new() -> Self {
        Self {
            count: 0,
            field_values: None,
            pinned_first: None,
        }
    }

    /// 追加某字段的历史值：维护 1024 上限，并钉扎该字段首个被裁剪的值。
    pub(super) fn push_field_value(&mut self, field_name: &str, value: Value) {
        push_capped_field(
            &mut self.field_values,
            &mut self.pinned_first,
            field_name,
            value,
        );
    }

    /// 各字段历史的完整有界序列（`[首值] ++ 最近样本`）。
    pub(super) fn field_series(&self) -> EngineHashMap<String, Vec<Value>> {
        field_series_with_first(
            self.field_values.as_deref(),
            self.pinned_first.as_deref().map(|p| &p.fields),
        )
    }
}

#[derive(::jumo_derive::Jumo, Debug, Clone)]
#[jumo(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct StepState {
    pub(super) branch_states: Vec<BranchState>,
}

impl StepState {
    pub fn new(branch_count: usize) -> Self {
        Self {
            branch_states: (0..branch_count).map(|_| BranchState::new()).collect(),
        }
    }
}

#[derive(::jumo_derive::Jumo, Debug, Clone)]
#[jumo(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub(super) struct Instance {
    // Note: no `scope_key` here — the instance key (String form) lives in the
    // `InstanceKey` that keys the instance map and the expiry heap. Storing a
    // `Vec<Value>` copy here duplicated the key for every instance, the dominant
    // per-instance memory at high entity cardinality (wp-reactor#19). The close
    // output reconstructs it from the `InstanceKey` on demand.
    pub(super) machine_id: String,
    pub(super) created_at: i64,
    pub(super) last_event_nanos: i64,
    /// 实例内首条被接受（推进到实例）事件的事件时间（issue #82 方案 A）：
    /// 候选事件跨度起点 `@event_first_time`。fixed 窗口下独立于桶起点
    /// `created_at`（桶内首事件可能晚于桶起点）。None = 尚未收到事件。
    pub(super) first_event_nanos: Option<i64>,
    /// 实例首次完整命中（match/close 输出）的引擎处理墙钟（issue #82）：
    /// 首次命中时赋值，accu rearm 保持、reset 清空；未命中为 None。
    pub(super) first_hit_wall_nanos: Option<i64>,
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
            first_event_nanos: None,
            first_hit_wall_nanos: None,
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
                size += bs
                    .pinned_first
                    .as_deref()
                    .map(pinned_first_bytes)
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
                        .unwrap_or(0)
                    + state
                        .pinned_first
                        .as_deref()
                        .map(pinned_first_bytes)
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
        // 新实例周期（issue #82 方案 A）：候选事件跨度随实例重置。
        self.first_event_nanos = None;
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
        // 新实例周期（issue #82）：first_match 随实例重置。
        self.first_hit_wall_nanos = None;
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
        // Kept: created_at, last_event_nanos, first_event_nanos, step_states,
        // close_step_states, alias_states, baselines, machine_id,
        // first_hit_wall_nanos (issue #82: accu 保持候选事件跨度与首次命中墙钟
        // 不变)。
    }

    pub(super) fn observe_seen_event_time(&mut self, event_time_nanos: i64) {
        // 候选事件跨度（issue #82 方案 A）：first = 到达序首条被接受事件。
        if self.first_event_nanos.is_none() {
            self.first_event_nanos = Some(event_time_nanos);
        }
        if event_time_nanos > self.last_event_nanos {
            self.last_event_nanos = event_time_nanos;
        }
    }

    /// 窗口实例候选事件跨度（issue #82 方案 A，`@event_first_time`/
    /// `@event_last_time`）：first 为实例内首条被接受事件时间（无事件时回退
    /// `fallback_first`），last 为最后一条被接受事件时间（`observe` 已并入当前
    /// 触发事件，故 ≥ 当前事件时间）。
    pub(super) fn event_span(&self, fallback_first: i64) -> (i64, i64) {
        let first = self.first_event_nanos.unwrap_or(fallback_first);
        (first, self.last_event_nanos)
    }

    /// 返回实例首次命中墙钟（issue #82）：已记录则保持首次值；未记录时写入
    /// `wall_nanos`（None = 当前驱动未提供处理墙钟，保持未命中状态）。
    pub(super) fn first_hit_wall(&mut self, wall_nanos: Option<i64>) -> Option<i64> {
        if self.first_hit_wall_nanos.is_none() {
            self.first_hit_wall_nanos = wall_nanos;
        }
        self.first_hit_wall_nanos
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
                field_values: state.field_series(),
            })
        })
        .collect()
}

/// `[首值] ++ 最近样本`：把被裁剪掉的序列首值拼回序列头部（warp-fusion#100）。
///
/// 未发生裁剪（`pinned_first == None`）时序列与历史逐位一致；发生裁剪时序列
/// 长度上界 1025（首值 + 最多 1024 个最近样本）。按消费者分类：
///
/// - **`first(x)` 稳定为最早样本**（本修复的目的，`x` 可为裸别名或限定字段）；
/// - `last(x)` 与裸字段读取（`values.last()`）、`count(alias)` /
///   `stat.count(window_event(alias))`（独立累加器）语义不变；
/// - `collect_list` / `collect_set` / `stddev` / `percentile` 额外得到最早样本；
/// - **限定字段聚合也看到这个额外样本**：`sum/avg/min/max(alias.field)` 走同一序列，
///   因此 `min(alias.field)` 会取到首个样本（`count()` 不接受字段投影，不存在
///   `count(alias.field)` 形态）。
fn series_with_pinned_first(stored: Option<&VecDeque<Value>>, first: Option<&Value>) -> Vec<Value> {
    let stored_len = stored.map_or(0, VecDeque::len);
    // 预分配：避免 `insert(0, ..)` 在每条序列上重分配 + O(1024) memmove（暴露在
    // per-fire 热路径上，见 push_capped 关于 O(1024) 搬运的注释）。
    let mut out = Vec::with_capacity(stored_len + usize::from(first.is_some()));
    if let Some(first) = first {
        out.push(first.clone());
    }
    if let Some(stored) = stored {
        out.extend(stored.iter().cloned());
    }
    out
}

fn field_series_with_first(
    values: Option<&EngineHashMap<String, VecDeque<Value>>>,
    first: Option<&EngineHashMap<String, Value>>,
) -> EngineHashMap<String, Vec<Value>> {
    let Some(values) = values else {
        return EngineHashMap::default();
    };
    // 不变量：`pinned_first.fields` 的键集是 `values` 键集的子集（两者只在
    // `push_capped_field` 里同步写入，无删除路径）——按名查找才不会漏拼。
    debug_assert!(first.is_none_or(|pins| {
        pins.keys()
            .all(|field_name| values.contains_key(field_name))
    }));
    values
        .iter()
        .map(|(field_name, stored)| {
            let series =
                series_with_pinned_first(Some(stored), first.and_then(|f| f.get(field_name)));
            (field_name.clone(), series)
        })
        .collect()
}

/// 追加某字段的历史值（`BranchState` / `AliasState` 同一语义）：维护 1024 上限，
/// 并钉扎该字段首个被裁剪的值。
///
/// 字段名已存在时走 `contains_key` + `get_mut`，热路径无 `String` 分配
/// （count 类规则每事件重复收集同一批字段）；钉扎槽已存在时直接返回，
/// 只有**首次**裁剪才分配字段名。
fn push_capped_field(
    field_values: &mut Option<Box<EngineHashMap<String, VecDeque<Value>>>>,
    pinned_first: &mut Option<Box<PinnedFirst>>,
    field_name: &str,
    value: Value,
) {
    let dropped = {
        let fvm = field_values.get_or_insert_with(|| Box::new(EngineHashMap::default()));
        if !fvm.contains_key(field_name) {
            fvm.insert(field_name.to_string(), VecDeque::new());
        }
        push_capped(fvm.get_mut(field_name).expect("just inserted"), value)
    };
    let Some(dropped) = dropped else {
        return;
    };
    let fields = &mut pinned_first.get_or_insert_with(Box::default).fields;
    if !fields.contains_key(field_name) {
        fields.insert(field_name.to_string(), dropped);
    }
}

/// 钉扎 `collected_values` 首个被裁剪的值（首值一旦记录即不再覆盖）。
fn pin_dropped_collected(pinned_first: &mut Option<Box<PinnedFirst>>, dropped: Option<Value>) {
    if let Some(dropped) = dropped {
        pinned_first
            .get_or_insert_with(Box::default)
            .collected
            .get_or_insert(dropped);
    }
}

/// [`PinnedFirst`] 的字节数（每序列至多 1 个值，仅 >1024 事件的实例分配）。
fn pinned_first_bytes(pinned: &PinnedFirst) -> usize {
    24 + pinned
        .collected
        .as_ref()
        .map(val_estimated_bytes)
        .unwrap_or(0)
        + pinned
            .fields
            .iter()
            .map(|(field, value)| field.len() + 24 + val_estimated_bytes(value))
            .sum::<usize>()
}

fn val_estimated_bytes(v: &Value) -> usize {
    match v {
        Value::Str(s) => s.len() + 24,
        Value::Float(_) | Value::Int(_) | Value::Bool(_) => 8,
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
