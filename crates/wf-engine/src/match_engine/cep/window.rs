//! cep 单窗实例推进面：`advance_window` —— 一个事件对一个窗口实例的完整推进
//! （实例准入/limits/驱逐、链语义、close 步骤累积、步骤求值与 close-mode 发射/重置），
//! 外加实例簿记 helper `push_expiry_candidate`（expiry heap 去重入堆，仅被本推进
//! 路径使用）。事件路由/诊断在 `advance.rs`，到期收口在 `expiry.rs`；结构定义/构造
//! 与共享簿记（remove/release/tracks_memory/expire_time_for）留在 `super`。

use std::cmp::Reverse;

use wf_lang::ast::CloseMode;
use wf_lang::plan::{ExceedAction, WindowSpec};

use super::close::evidence_time_range;
use super::key::{InstanceKey, ScopeKey, flatten_scope_values};
use super::seq::{consec_broken, scan_negations};
use super::state::{AliasState, Instance, snapshot_bind_data};
use super::step::{
    StepEvaluationInput, StepProgressCapture, collect_alias_event, evaluate_step_with_progress,
};
use super::types::{
    EngineHashMap, FieldSource, MACHINE_ID, MatchedContext, StepData, StepOutcome, StepResult,
    WindowLookup,
};
use super::{
    CepStateMachine, accumulate_close_steps, fail_rule, should_track_bind_alias, step_outcome,
    throttle_allows,
};
use crate::match_engine::columnar::GuardMasks;
use crate::match_engine::event_bridge::TriggerEvent;

impl CepStateMachine {
    /// Process one event against one window instance (fixed/hop buckets
    /// carry `window_start`; sliding/session pass `None`). Extracted from
    /// `advance_at_with_diagnostics` so HOP can fan a single event out to
    /// every covering window.
    #[allow(clippy::too_many_arguments)] // HOP 扇出: 事件/时间/查找/掩码/行/进度/键借用/窗口键 8 组参数
    pub(super) fn advance_window<E: FieldSource>(
        &mut self,
        alias: &str,
        event: &E,
        now_nanos: i64,
        windows: Option<&dyn WindowLookup>,
        row: usize,
        masks: Option<&GuardMasks>,
        capture_progress: bool,
        skey: &ScopeKey,
        window_start: Option<i64>,
        // M3 §11.6：owned trigger capture（deferred 列式快照），None → 物化回退。
        trigger: Option<&TriggerEvent>,
    ) -> StepOutcome {
        let instance_key = match window_start {
            Some(ws) => InstanceKey::fixed(skey, ws),
            None => InstanceKey::sliding(skey),
        };
        // 2. Get or create instance (with limits check)
        let is_new = !self.instances.contains_key(&instance_key);
        // N1: whether THIS call holds a shared instance-slot reservation for the
        // incoming key. Every early return between the reservation and the
        // actual instance insert below must release it, or the shared budget
        // leaks one slot per throttled/failing new key until it is exhausted.
        let mut shared_slot_reserved = false;
        if is_new
            && let Some(ref limits) = self.limits
            && let Some(max_inst) = limits.max_instances
        {
            // P2b: with shared limits the budget is the cross-shard instance total.
            // Use an exact CAS reservation (`try_reserve_instance`) instead of a
            // read-then-act check — two shards can no longer both pass a stale
            // count and overshoot the cap (P1②).
            let reserved = match &self.shared {
                Some(shared) => {
                    let ok = shared.try_reserve_instance(max_inst);
                    shared_slot_reserved = ok;
                    ok
                }
                None => self.instances.len() < max_inst,
            };
            if !reserved {
                match limits.on_exceed {
                    ExceedAction::Throttle => return step_outcome(StepResult::Accumulate, None),
                    // P3-B: under shared limits this evicts this shard's LOCAL
                    // oldest instance, not the global oldest across shards (a
                    // cross-shard priority queue is out of scope). The shared
                    // count stays exact either way; eviction fairness is
                    // per-shard.
                    ExceedAction::DropOldest => {
                        // Evict the local oldest instance, releasing its shared
                        // slot, then re-reserve so the new instance is counted
                        // exactly. If this shard has no local instance to evict
                        // (budget held by other shards), reject the new key.
                        if let Some(oldest_key) = self
                            .instances
                            .iter()
                            .min_by_key(|(_, inst)| inst.created_at)
                            .map(|(k, _)| k.clone())
                            && self.remove_instance(&oldest_key).is_some()
                            && let Some(shared) = &self.shared
                        {
                            shared.release_instance();
                        }
                        let re_reserved = match &self.shared {
                            Some(shared) => {
                                let ok = shared.try_reserve_instance(max_inst);
                                shared_slot_reserved = ok;
                                ok
                            }
                            None => self.instances.len() < max_inst,
                        };
                        if !re_reserved {
                            return step_outcome(StepResult::Accumulate, None);
                        }
                    }
                    ExceedAction::FailRule => {
                        fail_rule(&mut self.failed, &self.shared);
                        return step_outcome(StepResult::Accumulate, None);
                    }
                }
            }
        }

        // New-instance base cost: reused for the max_memory check below and for
        // O(1) per-instance accounting in insert/remove (exact state growth is
        // corrected by periodic `recalibrate_memory`).
        let new_base = if is_new && self.tracks_memory_bytes() {
            Some(Instance::base_estimated_bytes(
                &self.plan,
                &[],
                alias,
                event,
            ))
        } else {
            None
        };

        // max_memory_bytes: total estimated memory across all instances.
        // Runs on every event to catch both new instance creation and
        // existing instance growth (e.g. distinct_set expansion).
        //
        // P1②: under sharding this is an *approximate* budget — the shared total
        // is a check-then-act read plus per-insert base-cost deltas, so concurrent
        // shards may transiently overshoot by ≤ shard_count-1 new instances, and
        // per-instance state growth (distinct_set etc.) is only corrected by the
        // periodic `recalibrate_memory`. The eviction loop + recalibrate keep it
        // bounded; an exact CAS reserve is impractical for memory (grows
        // non-atomically). `max_instances` above IS exact.
        // 2026-08-31 摊还：非新实例 + 无每事件增长（纯 count/sum/min/max/avg，无
        // distinct/历史/seq）时 `estimated_memory_bytes` 只在 insert/remove 变化
        // （两处已精确记账），逐事件检查纯冗余 → 只在新实例准入或可增长规则上
        // 执行（qradar/真实规则全带 limits，逐事件检查是每事件每规则浪费）。
        if let Some(ref limits) = self.limits
            && let Some(max_bytes) = limits.max_memory_bytes
            && (is_new || self.memory_grows_per_event)
        {
            let new_cost = new_base.unwrap_or(0);
            // P2b: with shared limits the budget is the cross-shard memory total.
            let shared_total = self
                .shared
                .as_ref()
                .map(|s| s.memory_bytes())
                .unwrap_or(self.estimated_memory_bytes);
            let mut total = shared_total + new_cost;
            if total >= max_bytes {
                match limits.on_exceed {
                    ExceedAction::Throttle => {
                        // N1: admission reserved a shared slot for this new key
                        // but we return before inserting — release or it leaks.
                        if shared_slot_reserved {
                            self.release_shared_instance();
                        }
                        return step_outcome(StepResult::Accumulate, None);
                    }
                    ExceedAction::DropOldest => {
                        // Evict oldest instances in a loop until under limit or nothing left.
                        // If the current key is the oldest it gets evicted too — its
                        // accumulated state is lost and entry() re-creates a fresh instance.
                        // We add the re-creation base cost to the budget so the loop
                        // keeps evicting until the fresh instance actually fits.
                        // N2: when the incoming key's own instance is evicted here, the
                        // re-creation below inherits its shared slot — releasing it now
                        // would under-count and over-admit later keys. The flag lets an
                        // early return still give the slot back if the re-creation
                        // never happens.
                        let mut slot_inherited_for_incoming = false;
                        while total >= max_bytes {
                            if let Some(oldest_key) = self
                                .instances
                                .iter()
                                .min_by_key(|(_, inst)| inst.created_at)
                                .map(|(k, _)| k.clone())
                            {
                                let evicting_current = oldest_key == instance_key;
                                if let Some(removed) = self.remove_instance(&oldest_key) {
                                    total = total.saturating_sub(removed.estimated_bytes());
                                    if evicting_current {
                                        // N2: the fresh instance created below takes
                                        // over this slot; shared count stays exact.
                                        slot_inherited_for_incoming = true;
                                    } else {
                                        // P1②: permanent eviction releases the shared slot.
                                        self.release_shared_instance();
                                    }
                                }
                                // Current key will be re-created — account for base cost
                                if evicting_current && !is_new {
                                    total += Instance::base_estimated_bytes(
                                        &self.plan,
                                        &[],
                                        alias,
                                        event,
                                    );
                                }
                            } else {
                                // No instances to evict — cannot make room. The
                                // re-creation will not happen, so a held/inherited
                                // slot must go back (N1/N2).
                                if shared_slot_reserved || slot_inherited_for_incoming {
                                    self.release_shared_instance();
                                }
                                return step_outcome(StepResult::Accumulate, None);
                            }
                        }
                    }
                    ExceedAction::FailRule => {
                        fail_rule(&mut self.failed, &self.shared);
                        // N1: release the un-consumed reservation (see Throttle arm).
                        if shared_slot_reserved {
                            self.release_shared_instance();
                        }
                        return step_outcome(StepResult::Accumulate, None);
                    }
                }
            }
        }

        if is_new {
            self.push_expiry_candidate(&instance_key, window_start.unwrap_or(now_nanos));
        }
        // A1（2026-08-24，hop 热路径）：entry 替代 take/put 往返——原实现每窗口
        // contains_key + remove + insert 三次哈希操作（remove/insert 还破坏 HashMap
        // 缓存局部性），且 remove/insert 上的内存镜像每次事件 add/sub 两次
        // AtomicU64（q17 高触发路径净零churn 也是开销）；现在 contains_key（判
        // is_new，limits 检查需在 entry 前改 map）+ 一次 entry。实例从不移出 map：
        // 各 early return 无需归还（借用自动结束），新实例入场时统一记一次
        // base_cost（见下）。语义不变：Occupied 借用原实例，Vacant 构造并插入。
        // 旧 take_instance/put_instance 已随此优化删除（2026-08-24）。
        // 2026-09-02 摊还（qradar rules 段逐事件开销归因）：steady（非新实例 +
        // 非 memory_grows_per_event）时，两 limits 块（max_instances 门 is_new、
        // max_memory 门 is_new||grows）在探针后不可能改 map——`get_mut` 免每事件
        // `instance_key.clone()`（entry 需要 owned key，占用高频 case 纯浪费）；
        // 新实例/增长规则（distinct 等，DropOldest 可能驱逐当前 key）仍走 entry
        // 重建语义不变。
        let tracks_memory = self.tracks_memory_bytes();
        let instance = if !is_new && !self.memory_grows_per_event {
            self.instances
                .get_mut(&instance_key)
                .expect("probed non-new && no limits mutation between probe and fetch")
        } else {
            match self.instances.entry(instance_key.clone()) {
                std::collections::hash_map::Entry::Occupied(o) => o.into_mut(),
                std::collections::hash_map::Entry::Vacant(v) => {
                    let created = window_start.unwrap_or(now_nanos);
                    let machine_id = Self::extract_event_str(event, MACHINE_ID);
                    let mut inst = Instance::new_at(&self.plan, machine_id, created);
                    inst.base_cost = new_base.unwrap_or(0);
                    v.insert(inst)
                }
            }
        };
        if is_new {
            // A freshly created instance enters the map here — account its base
            // cost once at admission. The old take/put round-trip was net-zero
            // (no mirror churn) and charged nothing, so the entry-based path
            // must charge the admission explicitly; permanent removes still go
            // through remove_instance.
            if tracks_memory {
                self.estimated_memory_bytes = self
                    .estimated_memory_bytes
                    .saturating_add(instance.base_cost);
                if let Some(shared) = &self.shared {
                    shared.add_memory(instance.base_cost);
                }
            }
        }
        let plan = &self.plan;

        instance.observe_seen_event_time(now_nanos);

        if should_track_bind_alias(plan, alias) {
            let tracked_fields = plan.tracked_bind_fields.get(alias);
            collect_alias_event(
                event,
                instance
                    .alias_states
                    .get_or_insert_with(|| Box::new(EngineHashMap::default()))
                    .entry(alias.to_string())
                    .or_insert_with(AliasState::new),
                tracked_fields,
            );
        }

        // 2b. Chain semantics: negation scan + strict adjacency.
        let seq_broken = if let Some(meta) = self.seq_meta.as_ref() {
            scan_negations(meta, instance, alias, event, now_nanos, windows, row, masks);
            consec_broken(meta, instance, plan, alias)
        } else {
            false
        };
        if seq_broken {
            let reset_at = window_start.unwrap_or(now_nanos);
            // A negation violation must persist across a `consec` adjacency break;
            // otherwise an in-window violation could be wiped and the chain re-fire.
            let neg_violated = instance.neg_violated;
            instance.reset(plan, reset_at);
            instance.neg_violated = neg_violated;
            self.push_expiry_candidate(&instance_key, reset_at);
            return step_outcome(StepResult::Accumulate, None);
        }

        // 3. Accumulate close steps (if any) — happens on every event
        if !plan.close_steps.is_empty() {
            accumulate_close_steps(
                alias,
                event,
                now_nanos,
                plan,
                &mut instance.close_step_states,
                windows,
                &mut instance.baselines,
                row,
                masks,
            );
        }

        // 3b. Any-mode (unordered co-occurrence): evaluate all steps in parallel and
        // fire once every step has satisfied its threshold, regardless of order.
        if plan.match_mode == wf_lang::ast::MatchMode::Any {
            for step_idx in 0..plan.event_steps.len() {
                if instance.satisfied_flags[step_idx] {
                    continue;
                }
                let step_plan = &plan.event_steps[step_idx];
                let (satisfied, _) = {
                    let step_state = &mut instance.step_states[step_idx];
                    evaluate_step_with_progress(
                        StepEvaluationInput {
                            alias,
                            event,
                            event_time_nanos: now_nanos,
                            windows,
                            progress: None,
                            step_index: step_idx,
                            row,
                            masks,
                            collect_step_values: plan.needs_field_history,
                        },
                        step_plan,
                        step_state,
                        &mut instance.baselines,
                    )
                };
                if let Some((branch_idx, measure_value)) = satisfied {
                    let label = step_plan.branches[branch_idx].label.clone();
                    let (first, last, collected, field_vals) = {
                        let bs = &instance.step_states[step_idx].branch_states[branch_idx];
                        (
                            bs.event_first_time_nanos,
                            bs.event_last_time_nanos,
                            bs.collected_values
                                .as_deref()
                                .map(|q| q.iter().cloned().collect())
                                .unwrap_or_default(),
                            bs.field_values
                                .as_deref()
                                .map(|m| {
                                    m.iter()
                                        .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
                                        .collect()
                                })
                                .unwrap_or_default(),
                        )
                    };
                    instance.completed_steps.push(StepData {
                        satisfied_branch_index: branch_idx,
                        label,
                        measure_value,
                        event_first_time_nanos: first,
                        event_last_time_nanos: last,
                        collected_values: collected,
                        field_values: field_vals,
                    });
                    instance.satisfied_flags[step_idx] = true;
                }
            }

            if instance.satisfied_flags.iter().all(|&f| f) {
                // Rate limiting before emitting (mirror the no-close path).
                if let Some(rate) = self.limits.as_ref().and_then(|l| l.max_throttle.clone())
                    && !throttle_allows(
                        &self.shared,
                        &mut self.emit_count,
                        &mut self.emit_window_start,
                        now_nanos,
                        &rate,
                    )
                {
                    let on_exceed = self
                        .limits
                        .as_ref()
                        .map(|l| l.on_exceed.clone())
                        .unwrap_or(ExceedAction::Throttle);
                    match on_exceed {
                        ExceedAction::Throttle | ExceedAction::DropOldest => {
                            // `on event<accu>`: a throttled re-fire suppresses the
                            // alert but keeps the running accumulation.
                            if plan.accu {
                                instance.rearm(plan);
                            } else {
                                let reset_at = window_start.unwrap_or(now_nanos);
                                instance.reset(plan, reset_at);
                                self.push_expiry_candidate(&instance_key, reset_at);
                            }
                            return step_outcome(StepResult::Accumulate, None);
                        }
                        ExceedAction::FailRule => {
                            fail_rule(&mut self.failed, &self.shared);
                            return step_outcome(StepResult::Accumulate, None);
                        }
                    }
                }
                let (evidence_first, evidence_last) =
                    evidence_time_range(instance.completed_steps.iter())
                        .unwrap_or((now_nanos, now_nanos));
                // first_match_time（issue #82）：实例首次完整命中墙钟——首次 fire
                // 赋值，accu rearm 保持、reset 清空。墙钟由驱动方按批注入（`@emit_time` 同源）。
                let (event_first_nanos, event_last_nanos) = instance.event_span(evidence_first);
                let first_match_time_nanos = instance.first_hit_wall(self.processing_wall_nanos);
                let ctx = MatchedContext {
                    rule_name: self.rule_name.clone(),
                    scope_key: flatten_scope_values(skey),
                    step_data: instance.completed_steps.clone(),
                    bind_data: snapshot_bind_data(instance.alias_states.as_deref()),
                    event_time_nanos: now_nanos,
                    event_first_time_nanos: event_first_nanos,
                    event_last_time_nanos: event_last_nanos,
                    evidence_first_time_nanos: evidence_first,
                    evidence_last_time_nanos: evidence_last,
                    first_match_time_nanos,
                    window_start_time_nanos: instance.created_at,
                    window_end_time_nanos: Self::expire_time_for(&plan.window_spec, instance),
                    machine_id: instance.machine_id.clone(),
                    // trigger_event 只在 score/entity/yield + join 左字段 + where
                    // 引用非 key 字段时需要（编译器 compute_trigger_event_needed）。
                    // 不需要时跳过 per-fire `event.to_event()` 全量 clone——
                    // Q5/Q7/Q12/Q13 每事件命中 fire 的热路径（2026-08）。
                    trigger_event: if plan.trigger_event_needed {
                        // M3（2026-09-02）：机器内不再每 fire 物化——预捕获列式快照
                        // 直接携带；None 回退 to_event（row-mode / 测试）。
                        Some(match trigger {
                            Some(t) => t.clone(),
                            None => TriggerEvent::Event(std::sync::Arc::new(event.to_event())),
                        })
                    } else {
                        None
                    },
                };
                if plan.accu {
                    // `on event<accu>` — keep accumulating across fires.
                    instance.rearm(plan);
                } else {
                    let reset_at = window_start.unwrap_or(now_nanos);
                    instance.reset(plan, reset_at);
                    self.push_expiry_candidate(&instance_key, reset_at);
                }
                return step_outcome(StepResult::Matched(ctx), None);
            }

            return step_outcome(StepResult::Accumulate, None);
        }

        let mut progress = None;
        let result = 'process: {
            // 4. If event already emitted (OR mode), just accumulate for close
            if instance.event_emitted {
                break 'process StepResult::Accumulate;
            }

            // 5. If event steps already complete (AND mode), just accumulate for close
            if instance.event_ok {
                break 'process StepResult::Accumulate;
            }

            // 6. Current step plan
            if instance.current_step >= plan.event_steps.len() {
                break 'process StepResult::Accumulate;
            }
            let step_idx = instance.current_step;
            let step_plan = &plan.event_steps[step_idx];

            // 6. Evaluate step
            // 发射 key 值只在 debug 进度捕获时需要；`capture_progress.then`
            // 惰性计算（生产 debug off 时零开销）。
            let emit_key_values = capture_progress.then(|| flatten_scope_values(skey));
            let evaluation = {
                let step_state = &mut instance.step_states[step_idx];
                evaluate_step_with_progress(
                    StepEvaluationInput {
                        alias,
                        event,
                        event_time_nanos: now_nanos,
                        windows,
                        progress: capture_progress.then_some(StepProgressCapture {
                            rule_name: &self.rule_name,
                            scope_key: emit_key_values.as_deref().unwrap_or(&[]),
                            machine_id: &instance.machine_id,
                            step_index: step_idx,
                        }),
                        step_index: step_idx,
                        row,
                        masks,
                        collect_step_values: plan.needs_field_history,
                    },
                    step_plan,
                    step_state,
                    &mut instance.baselines,
                )
            };
            let (satisfied, evaluation_progress) = evaluation;
            let Some((branch_idx, measure_value)) = satisfied else {
                progress = evaluation_progress;
                break 'process StepResult::Accumulate;
            };
            progress = evaluation_progress;

            let label = step_plan.branches[branch_idx].label.clone();
            let step_state = &instance.step_states[step_idx];
            // Collect the values from the satisfied branch for L3 functions
            let collected_values = step_state.branch_states[branch_idx]
                .collected_values
                .as_deref()
                .map(|q| q.iter().cloned().collect())
                .unwrap_or_default();
            instance.completed_steps.push(StepData {
                satisfied_branch_index: branch_idx,
                label,
                measure_value,
                event_first_time_nanos: step_state.branch_states[branch_idx].event_first_time_nanos,
                event_last_time_nanos: step_state.branch_states[branch_idx].event_last_time_nanos,
                collected_values,
                field_values: step_state.branch_states[branch_idx]
                    .field_values
                    .as_deref()
                    .map(|m| {
                        m.iter()
                            .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
                            .collect()
                    })
                    .unwrap_or_default(),
            });

            // Chain `within`: the completing step must land within its gap of the
            // previous step's completion (window start for the first step).
            let within_violated = if let Some(meta) = self.seq_meta.as_ref() {
                meta.within
                    .get(step_idx)
                    .copied()
                    .flatten()
                    .is_some_and(|w| {
                        // Completion time = the event that completed the step
                        // (`event_last_time_nanos`). For aggregate steps this differs
                        // from `event_first_time_nanos` (threshold-met time, not
                        // first-event time).
                        let this_last = step_state.branch_states[branch_idx]
                            .event_last_time_nanos
                            .unwrap_or(now_nanos);
                        let prev_last = if step_idx == 0 {
                            instance.created_at
                        } else {
                            instance
                                .completed_steps
                                .get(step_idx - 1)
                                .and_then(|sd| sd.event_last_time_nanos)
                                .unwrap_or(instance.created_at)
                        };
                        // The gap must be non-negative and within `w`: an
                        // out-of-order completion (this before prev) violates
                        // "within" just as a gap that is too large does.
                        let gap = this_last - prev_last;
                        gap < 0 || gap > w.as_nanos() as i64
                    })
            } else {
                false
            };
            if within_violated {
                let reset_at = window_start.unwrap_or(now_nanos);
                // Preserve a negation violation across a `within` reset, matching the
                // `consec`-break reset: an in-window violation must not be wiped so the
                // chain can re-fire.
                let neg_violated = instance.neg_violated;
                instance.reset(plan, reset_at);
                instance.neg_violated = neg_violated;
                self.push_expiry_candidate(&instance_key, reset_at);
                break 'process StepResult::Accumulate;
            }
            instance.current_step += 1;

            if instance.current_step < plan.event_steps.len() {
                break 'process StepResult::Advance;
            }

            // Chain negation: a violated negation step must suppress the emit.
            if instance.neg_violated {
                let reset_at = window_start.unwrap_or(now_nanos);
                instance.reset(plan, reset_at);
                self.push_expiry_candidate(&instance_key, reset_at);
                break 'process StepResult::Accumulate;
            }

            if plan.close_steps.is_empty() {
                // Rate limiting check before emitting
                if let Some(rate) = self.limits.as_ref().and_then(|l| l.max_throttle.clone())
                    && !throttle_allows(
                        &self.shared,
                        &mut self.emit_count,
                        &mut self.emit_window_start,
                        now_nanos,
                        &rate,
                    )
                {
                    let on_exceed = self
                        .limits
                        .as_ref()
                        .map(|l| l.on_exceed.clone())
                        .unwrap_or(ExceedAction::Throttle);
                    match on_exceed {
                        ExceedAction::Throttle | ExceedAction::DropOldest => {
                            // `on event<accu>`: a throttled re-fire suppresses the
                            // alert but keeps the running accumulation.
                            if plan.accu {
                                instance.rearm(plan);
                            } else {
                                // Suppress the match — reset instance for future use
                                let reset_at = window_start.unwrap_or(now_nanos);
                                instance.reset(plan, reset_at);
                                self.push_expiry_candidate(&instance_key, reset_at);
                            }
                            break 'process StepResult::Accumulate;
                        }
                        ExceedAction::FailRule => {
                            fail_rule(&mut self.failed, &self.shared);
                            break 'process StepResult::Accumulate;
                        }
                    }
                }

                // No close steps → M14 backward compat: Matched + reset, or
                // `on event<accu>` rearm (keep accumulating across fires).
                let (evidence_first, evidence_last) =
                    evidence_time_range(instance.completed_steps.iter())
                        .unwrap_or((now_nanos, now_nanos));
                // first_match_time（issue #82）：实例首次完整命中墙钟——首次 fire
                // 赋值，accu rearm 保持、reset 清空。墙钟由驱动方按批注入（`@emit_time` 同源）。
                let (event_first_nanos, event_last_nanos) = instance.event_span(evidence_first);
                let first_match_time_nanos = instance.first_hit_wall(self.processing_wall_nanos);
                let ctx = MatchedContext {
                    rule_name: self.rule_name.clone(),
                    scope_key: flatten_scope_values(skey),
                    step_data: instance.completed_steps.clone(),
                    bind_data: snapshot_bind_data(instance.alias_states.as_deref()),
                    event_time_nanos: now_nanos,
                    event_first_time_nanos: event_first_nanos,
                    event_last_time_nanos: event_last_nanos,
                    evidence_first_time_nanos: evidence_first,
                    evidence_last_time_nanos: evidence_last,
                    first_match_time_nanos,
                    window_start_time_nanos: instance.created_at,
                    window_end_time_nanos: Self::expire_time_for(&plan.window_spec, instance),
                    machine_id: instance.machine_id.clone(),
                    // trigger_event 只在 score/entity/yield + join 左字段 + where
                    // 引用非 key 字段时需要（编译器 compute_trigger_event_needed）。
                    // 不需要时跳过 per-fire `event.to_event()` 全量 clone——
                    // Q5/Q7/Q12/Q13 每事件命中 fire 的热路径（2026-08）。
                    trigger_event: if plan.trigger_event_needed {
                        Some(match trigger {
                            Some(t) => t.clone(),
                            None => TriggerEvent::Event(std::sync::Arc::new(event.to_event())),
                        })
                    } else {
                        None
                    },
                };
                if plan.accu {
                    // `on event<accu>` — keep accumulating across fires.
                    instance.rearm(plan);
                } else {
                    let reset_at = window_start.unwrap_or(now_nanos);
                    instance.reset(plan, reset_at);
                    self.push_expiry_candidate(&instance_key, reset_at);
                }
                StepResult::Matched(ctx)
            } else if plan.close_mode == CloseMode::Or {
                // OR mode: emit from event path immediately, keep instance alive for close
                if let Some(rate) = self.limits.as_ref().and_then(|l| l.max_throttle.clone())
                    && !throttle_allows(
                        &self.shared,
                        &mut self.emit_count,
                        &mut self.emit_window_start,
                        now_nanos,
                        &rate,
                    )
                {
                    let on_exceed = self
                        .limits
                        .as_ref()
                        .map(|l| l.on_exceed.clone())
                        .unwrap_or(ExceedAction::Throttle);
                    match on_exceed {
                        ExceedAction::Throttle | ExceedAction::DropOldest => {
                            instance.event_emitted = true;
                            break 'process StepResult::Accumulate;
                        }
                        ExceedAction::FailRule => {
                            fail_rule(&mut self.failed, &self.shared);
                            break 'process StepResult::Accumulate;
                        }
                    }
                }
                instance.event_emitted = true;
                let (evidence_first, evidence_last) =
                    evidence_time_range(instance.completed_steps.iter())
                        .unwrap_or((now_nanos, now_nanos));
                // first_match_time（issue #82）：实例首次完整命中墙钟——首次 fire
                // 赋值，accu rearm 保持、reset 清空。墙钟由驱动方按批注入（`@emit_time` 同源）。
                let (event_first_nanos, event_last_nanos) = instance.event_span(evidence_first);
                let first_match_time_nanos = instance.first_hit_wall(self.processing_wall_nanos);
                let ctx = MatchedContext {
                    rule_name: self.rule_name.clone(),
                    scope_key: flatten_scope_values(skey),
                    step_data: instance.completed_steps.clone(),
                    bind_data: snapshot_bind_data(instance.alias_states.as_deref()),
                    event_time_nanos: now_nanos,
                    event_first_time_nanos: event_first_nanos,
                    event_last_time_nanos: event_last_nanos,
                    evidence_first_time_nanos: evidence_first,
                    evidence_last_time_nanos: evidence_last,
                    first_match_time_nanos,
                    window_start_time_nanos: instance.created_at,
                    window_end_time_nanos: Self::expire_time_for(&plan.window_spec, instance),
                    machine_id: instance.machine_id.clone(),
                    // trigger_event 只在 score/entity/yield + join 左字段 + where
                    // 引用非 key 字段时需要（编译器 compute_trigger_event_needed）。
                    // 不需要时跳过 per-fire `event.to_event()` 全量 clone——
                    // Q5/Q7/Q12/Q13 每事件命中 fire 的热路径（2026-08）。
                    trigger_event: if plan.trigger_event_needed {
                        Some(match trigger {
                            Some(t) => t.clone(),
                            None => TriggerEvent::Event(std::sync::Arc::new(event.to_event())),
                        })
                    } else {
                        None
                    },
                };
                StepResult::Matched(ctx)
            } else {
                // AND mode: mark event_ok, keep accumulating
                instance.event_ok = true;
                StepResult::Advance
            }
        };
        if let Some(progress) = &mut progress {
            progress.instances = self.instances.len();
        }
        step_outcome(result, progress)
    }

    pub(super) fn push_expiry_candidate(&mut self, key: &InstanceKey, created_at: i64) {
        // Only schedule one pending candidate per key. Per-event/reset pushes
        // (high-fire rules) would otherwise stack duplicate heap entries that
        // never get deduplicated — the dominant memory leak on pass-through
        // rules (wp-reactor leak investigation).
        if !self.pending_expiry.insert(key.clone()) {
            return;
        }
        let expire_time = match self.plan.window_spec {
            WindowSpec::Sliding(d) | WindowSpec::Fixed(d) | WindowSpec::Session(d) => {
                created_at + d.as_nanos() as i64
            }
            WindowSpec::Hop { size, .. } => created_at + size.as_nanos() as i64,
        };
        self.expiry_heap.push(Reverse((expire_time, key.clone())));
    }
}
