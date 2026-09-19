//! cep 单窗实例推进面：`advance_window` —— 一个事件对一个窗口实例的推进编排
//! （实例准入/limits 门 → 实例簿记 → 链语义 → close 累积 → Any/顺序模式发射）。
//!
//! 2026-09-18 降复杂度拆分（纯搬移，语义不变）：准入 `admit_new_instance`、内存门
//! `enforce_memory_limit` + `evict_until_memory_fits`、基础成本 `new_instance_base_cost` +
//! `account_admission_memory`、链语义 `chain_break_phase`、Any 模式 `advance_any_mode`、
//! 顺序模式 `step_phase` / `emit_phase` 各自独立；推进期机器侧字段经 [`AdvanceView`]
//! 借用拆分（`instance` 借自 `machine.instances`，与之不重叠），三处重复的限速门与
//! `MatchedContext` 构造分别归一到 `AdvanceView::throttle_exceeded` /
//! `AdvanceView::matched_context`。事件路由/诊断在 `advance.rs`，到期收口在
//! `expiry.rs`；结构定义/构造与共享簿记（remove/release/tracks_memory/
//! expire_time_for）留在 `super`。

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use wf_lang::ast::CloseMode;
use wf_lang::plan::{ExceedAction, LimitsPlan, MatchPlan, WindowSpec};

use super::close::evidence_time_range;
use super::key::{InstanceKey, ScopeKey, flatten_scope_values};
use super::seq::{SeqRuntime, consec_broken, scan_negations};
use super::state::{AliasState, Instance, snapshot_bind_data};
use super::step::{
    StepEvaluationInput, StepProgressCapture, collect_alias_event, evaluate_step_with_progress,
};
use super::types::{
    EngineHashMap, EngineHashSet, FieldSource, MACHINE_ID, MatchedContext, StepData, StepOutcome,
    StepProgress, StepResult, WindowLookup,
};
use super::{
    CepStateMachine, SharedLimits, accumulate_close_steps, fail_rule, should_track_bind_alias,
    step_outcome, throttle_allows,
};
use crate::masks::GuardMasks;
use crate::row_views::TriggerEvent;

/// expiry heap 去重入堆：同一 key 只保留一个待到期候选。
///
/// 逐事件/reset 路径（高频规则）重复入堆会让堆里堆积永不消解的重复项——这是
/// pass-through 规则上的主要内存泄漏（wp-reactor leak investigation）。本函数是
/// `CepStateMachine::push_expiry_candidate` 的实现体，也是 [`AdvanceView::push_expiry`]
/// 的共用实现（推进期 `self` 不可整体可变借用）。
fn push_expiry_candidate(
    plan: &MatchPlan,
    pending_expiry: &mut EngineHashSet<InstanceKey>,
    expiry_heap: &mut BinaryHeap<Reverse<(i64, InstanceKey)>>,
    key: &InstanceKey,
    created_at: i64,
) {
    // Only schedule one pending candidate per key. Per-event/reset pushes
    // (high-fire rules) would otherwise stack duplicate heap entries that
    // never get deduplicated — the dominant memory leak on pass-through
    // rules (wp-reactor leak investigation).
    if !pending_expiry.insert(key.clone()) {
        return;
    }
    let expire_time = match plan.window_spec {
        WindowSpec::Sliding(d) | WindowSpec::Fixed(d) | WindowSpec::Session(d) => {
            created_at + d.as_nanos() as i64
        }
        WindowSpec::Hop { size, .. } => created_at + size.as_nanos() as i64,
    };
    expiry_heap.push(Reverse((expire_time, key.clone())));
}

/// 准入记账：新实例入场时把它的 base_cost 记入本机（及共享）内存镜像。
///
/// 旧的 take/put 往返净零且不计费，故 entry 路径必须在此显式记账；永久移除仍由
/// `CepStateMachine::remove_instance` 扣减。
fn account_admission_memory(
    estimated_memory_bytes: &mut usize,
    shared: &Option<std::sync::Arc<SharedLimits>>,
    base_cost: usize,
    tracks_memory: bool,
) {
    if !tracks_memory {
        return;
    }
    *estimated_memory_bytes = estimated_memory_bytes.saturating_add(base_cost);
    if let Some(shared) = shared {
        shared.add_memory(base_cost);
    }
}

/// [`CepStateMachine::evict_until_memory_fits`] 的结果。
enum EvictOutcome {
    /// 已腾够空间（`total < max_bytes`）。
    Fits,
    /// 无实例可驱逐（预算被其他分片占用）——调用方需归还持有/继承的槽位并早退。
    Exhausted { slot_inherited: bool },
}

/// `advance_window` 推进期的机器侧借用拆分视图。
///
/// `instance`（`&mut machine.instances[k]`）在推进期一直存活，机器侧其余字段与它
/// 不重叠，故可单独借出并随 [`Instance`] 一起传给各阶段 helper：发射限速/闭锁、
/// 计划与规则名、expiry 簿记各一组。
struct AdvanceView<'a> {
    limits: Option<&'a LimitsPlan>,
    shared: &'a Option<std::sync::Arc<SharedLimits>>,
    failed: &'a mut bool,
    emit_count: &'a mut u64,
    emit_window_start: &'a mut i64,
    plan: &'a MatchPlan,
    rule_name: &'a str,
    processing_wall_nanos: Option<i64>,
    pending_expiry: &'a mut EngineHashSet<InstanceKey>,
    expiry_heap: &'a mut BinaryHeap<Reverse<(i64, InstanceKey)>>,
}

impl AdvanceView<'_> {
    /// 限速门：允许发射 → `None`；被限流 → `Some(on_exceed)`。
    ///
    /// 三处 emit 站点（Any / 无 close / OR）原本各自重复一遍 `max_throttle` 查找、
    /// `throttle_allows` 调用与 `on_exceed` 兜底，现统一到此。
    fn throttle_exceeded(&mut self, now_nanos: i64) -> Option<ExceedAction> {
        let rate = self.limits.and_then(|l| l.max_throttle.clone())?;
        if throttle_allows(
            self.shared,
            self.emit_count,
            self.emit_window_start,
            now_nanos,
            &rate,
        ) {
            return None;
        }
        Some(
            self.limits
                .map(|l| l.on_exceed.clone())
                .unwrap_or(ExceedAction::Throttle),
        )
    }

    /// expiry 候选入堆（去重），见 free fn [`push_expiry_candidate`]。
    fn push_expiry(&mut self, key: &InstanceKey, created_at: i64) {
        push_expiry_candidate(
            self.plan,
            self.pending_expiry,
            self.expiry_heap,
            key,
            created_at,
        );
    }

    /// 命中上下文；三处 emit 站点原本各自重复约 28 行构造。
    ///
    /// `trigger_event` 只在 `plan.trigger_event_needed` 时物化（Q5/Q7/Q12/Q13 每
    /// 事件命中 fire 的热路径，2026-08 起免全量 `to_event()` clone）。
    fn matched_context<E: FieldSource>(
        &self,
        instance: &mut Instance,
        skey: &ScopeKey,
        now_nanos: i64,
        event: &E,
        trigger: Option<&TriggerEvent>,
    ) -> MatchedContext {
        let plan = self.plan;
        let (evidence_first, evidence_last) =
            evidence_time_range(instance.completed_steps.iter()).unwrap_or((now_nanos, now_nanos));
        // first_match_time（issue #82）：实例首次完整命中墙钟——首次 fire
        // 赋值，accu rearm 保持、reset 清空。墙钟由驱动方按批注入（`@emit_time` 同源）。
        let (event_first_nanos, event_last_nanos) = instance.event_span(evidence_first);
        let first_match_time_nanos = instance.first_hit_wall(self.processing_wall_nanos);
        MatchedContext {
            rule_name: self.rule_name.to_string(),
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
            window_end_time_nanos: CepStateMachine::expire_time_for(&plan.window_spec, instance),
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
        }
    }
}

/// Any 模式（无序共现）：并行求值所有步骤，全部达阈值即发射一次，与顺序无关。
///
/// 返回本次事件的终态（`Matched` / `Accumulate`）。
#[allow(clippy::too_many_arguments)] // 阶段 helper：视图 + 实例/事件/时间/查找/行域/键/窗口 7 组参数
fn advance_any_mode<E: FieldSource>(
    view: &mut AdvanceView<'_>,
    instance: &mut Instance,
    alias: &str,
    event: &E,
    now_nanos: i64,
    windows: Option<&dyn WindowLookup>,
    row: usize,
    masks: Option<&GuardMasks>,
    skey: &ScopeKey,
    instance_key: &InstanceKey,
    window_start: Option<i64>,
    trigger: Option<&TriggerEvent>,
) -> StepResult {
    let plan = view.plan;
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
                    bs.collected_series(),
                    bs.field_series(),
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
        if let Some(on_exceed) = view.throttle_exceeded(now_nanos) {
            match on_exceed {
                ExceedAction::Throttle | ExceedAction::DropOldest => {
                    // `on event<accu>`: a throttled re-fire suppresses the
                    // alert but keeps the running accumulation.
                    if plan.accu {
                        instance.rearm(plan);
                    } else {
                        let reset_at = window_start.unwrap_or(now_nanos);
                        instance.reset(plan, reset_at);
                        view.push_expiry(instance_key, reset_at);
                    }
                    return StepResult::Accumulate;
                }
                ExceedAction::FailRule => {
                    fail_rule(view.failed, view.shared);
                    return StepResult::Accumulate;
                }
            }
        }
        let ctx = view.matched_context(instance, skey, now_nanos, event, trigger);
        if plan.accu {
            // `on event<accu>` — keep accumulating across fires.
            instance.rearm(plan);
        } else {
            let reset_at = window_start.unwrap_or(now_nanos);
            instance.reset(plan, reset_at);
            view.push_expiry(instance_key, reset_at);
        }
        return StepResult::Matched(ctx);
    }

    StepResult::Accumulate
}

/// 顺序模式：推进当前步（求值 → `within` 链校验 → negation 抑制）。
///
/// 返回 `Some(result)` = 本次事件到此结束；`None` = 步已完成，调用方继续
/// [`emit_phase`]。`progress` 由调用方持有，本函数就地更新。
#[allow(clippy::too_many_arguments)] // 阶段 helper：视图 + 实例/事件/时间/查找/行域/进度/键 8 组参数
fn step_phase<E: FieldSource>(
    view: &mut AdvanceView<'_>,
    instance: &mut Instance,
    alias: &str,
    event: &E,
    now_nanos: i64,
    windows: Option<&dyn WindowLookup>,
    row: usize,
    masks: Option<&GuardMasks>,
    skey: &ScopeKey,
    capture_progress: bool,
    seq_meta: Option<&SeqRuntime>,
    window_start: Option<i64>,
    instance_key: &InstanceKey,
    progress: &mut Option<StepProgress>,
) -> Option<StepResult> {
    let plan = view.plan;
    // 4. If event already emitted (OR mode), just accumulate for close
    if instance.event_emitted {
        return Some(StepResult::Accumulate);
    }

    // 5. If event steps already complete (AND mode), just accumulate for close
    if instance.event_ok {
        return Some(StepResult::Accumulate);
    }

    // 6. Current step plan
    if instance.current_step >= plan.event_steps.len() {
        return Some(StepResult::Accumulate);
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
                    rule_name: view.rule_name,
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
        *progress = evaluation_progress;
        return Some(StepResult::Accumulate);
    };
    *progress = evaluation_progress;

    let label = step_plan.branches[branch_idx].label.clone();
    let step_state = &instance.step_states[step_idx];
    // Collect the values from the satisfied branch for L3 functions
    let branch_state = &step_state.branch_states[branch_idx];
    instance.completed_steps.push(StepData {
        satisfied_branch_index: branch_idx,
        label,
        measure_value,
        event_first_time_nanos: branch_state.event_first_time_nanos,
        event_last_time_nanos: branch_state.event_last_time_nanos,
        collected_values: branch_state.collected_series(),
        field_values: branch_state.field_series(),
    });

    // Chain `within`: the completing step must land within its gap of the
    // previous step's completion (window start for the first step).
    let within_violated = if let Some(meta) = seq_meta {
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
        view.push_expiry(instance_key, reset_at);
        return Some(StepResult::Accumulate);
    }
    instance.current_step += 1;

    if instance.current_step < plan.event_steps.len() {
        return Some(StepResult::Advance);
    }

    // Chain negation: a violated negation step must suppress the emit.
    if instance.neg_violated {
        let reset_at = window_start.unwrap_or(now_nanos);
        instance.reset(plan, reset_at);
        view.push_expiry(instance_key, reset_at);
        return Some(StepResult::Accumulate);
    }
    None
}

/// 顺序模式发射：close 步骤为空 → 立即 `Matched`（或 `on event<accu>` rearm）；
/// OR 模式 → 标记 `event_emitted` 并立即 `Matched`（实例留给 close）；
/// AND 模式 → 标记 `event_ok` 继续累积等 close。
#[allow(clippy::too_many_arguments)] // 阶段 helper：视图 + 实例 + 键/时间/窗口 + 事件/触发 4 组参数
fn emit_phase<E: FieldSource>(
    view: &mut AdvanceView<'_>,
    instance: &mut Instance,
    skey: &ScopeKey,
    now_nanos: i64,
    window_start: Option<i64>,
    instance_key: &InstanceKey,
    event: &E,
    trigger: Option<&TriggerEvent>,
) -> StepResult {
    let plan = view.plan;
    if plan.close_steps.is_empty() {
        // Rate limiting check before emitting
        if let Some(on_exceed) = view.throttle_exceeded(now_nanos) {
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
                        view.push_expiry(instance_key, reset_at);
                    }
                    return StepResult::Accumulate;
                }
                ExceedAction::FailRule => {
                    fail_rule(view.failed, view.shared);
                    return StepResult::Accumulate;
                }
            }
        }
        // No close steps → M14 backward compat: Matched + reset, or
        // `on event<accu>` rearm (keep accumulating across fires).
        let ctx = view.matched_context(instance, skey, now_nanos, event, trigger);
        if plan.accu {
            // `on event<accu>` — keep accumulating across fires.
            instance.rearm(plan);
        } else {
            let reset_at = window_start.unwrap_or(now_nanos);
            instance.reset(plan, reset_at);
            view.push_expiry(instance_key, reset_at);
        }
        StepResult::Matched(ctx)
    } else if plan.close_mode == CloseMode::Or {
        // OR mode: emit from event path immediately, keep instance alive for close
        if let Some(on_exceed) = view.throttle_exceeded(now_nanos) {
            match on_exceed {
                ExceedAction::Throttle | ExceedAction::DropOldest => {
                    instance.event_emitted = true;
                    return StepResult::Accumulate;
                }
                ExceedAction::FailRule => {
                    fail_rule(view.failed, view.shared);
                    return StepResult::Accumulate;
                }
            }
        }
        instance.event_emitted = true;
        let ctx = view.matched_context(instance, skey, now_nanos, event, trigger);
        StepResult::Matched(ctx)
    } else {
        // AND mode: mark event_ok, keep accumulating
        instance.event_ok = true;
        StepResult::Advance
    }
}

/// 链语义（`2b`）：negation 扫描 + `consec` 严格相邻性；被打破则重置实例。
///
/// 返回 `Some(Accumulate)` = 链被打破、调用方直接返回；`None` = 链完好。negation
/// 违规必须跨 `consec` 相邻性断裂保留（否则窗口内违规会被抹掉、链可能重新触发），
/// 故 reset 前后显式保存/恢复 `neg_violated`。
#[allow(clippy::too_many_arguments)] // 阶段 helper：视图 + 实例/事件/时间/查找/行域/键/窗口 6 组参数
fn chain_break_phase<E: FieldSource>(
    view: &mut AdvanceView<'_>,
    instance: &mut Instance,
    seq_meta: Option<&SeqRuntime>,
    plan: &MatchPlan,
    alias: &str,
    event: &E,
    now_nanos: i64,
    windows: Option<&dyn WindowLookup>,
    row: usize,
    masks: Option<&GuardMasks>,
    window_start: Option<i64>,
    instance_key: &InstanceKey,
) -> Option<StepResult> {
    let seq_broken = if let Some(meta) = seq_meta {
        scan_negations(meta, instance, alias, event, now_nanos, windows, row, masks);
        consec_broken(meta, instance, plan, alias)
    } else {
        false
    };
    if !seq_broken {
        return None;
    }
    let reset_at = window_start.unwrap_or(now_nanos);
    let neg_violated = instance.neg_violated;
    instance.reset(plan, reset_at);
    instance.neg_violated = neg_violated;
    view.push_expiry(instance_key, reset_at);
    Some(StepResult::Accumulate)
}

impl CepStateMachine {
    /// 新实例 base cost（仅当实例化本机内存镜像时才估算）。
    ///
    /// 供 `max_memory_bytes` 门与 insert/remove 的 O(1) 记账复用；精确状态增长由
    /// 周期性 `recalibrate_memory` 校正。
    fn new_instance_base_cost<E: FieldSource>(
        &self,
        is_new: bool,
        alias: &str,
        event: &E,
    ) -> Option<usize> {
        if is_new && self.tracks_memory_bytes() {
            Some(Instance::base_estimated_bytes(
                &self.plan,
                &[],
                alias,
                event,
            ))
        } else {
            None
        }
    }

    /// 新实例准入（`max_instances`）：共享预算用 CAS 预留（`try_reserve_instance`），
    /// 无共享时比对本机 `instances.len()`。
    ///
    /// 返回 `Some(early)` → 调用方直接返回；`None` → 准入通过。N1：
    /// `shared_slot_reserved` 回写本次调用是否持有共享实例槽位；此后到实例真正插入
    /// 之间的每个 early return 都必须归还，否则共享预算每被限流/拒绝一个新 key 就
    /// 泄漏一个槽位直到耗尽。
    fn admit_new_instance(
        &mut self,
        is_new: bool,
        shared_slot_reserved: &mut bool,
    ) -> Option<StepResult> {
        if !is_new {
            return None;
        }
        let limits = self.limits.as_ref()?;
        let max_inst = limits.max_instances?;
        let on_exceed = limits.on_exceed.clone();
        // P2b: with shared limits the budget is the cross-shard instance total.
        // Use an exact CAS reservation (`try_reserve_instance`) instead of a
        // read-then-act check — two shards can no longer both pass a stale
        // count and overshoot the cap (P1②).
        let reserved = match &self.shared {
            Some(shared) => {
                let ok = shared.try_reserve_instance(max_inst);
                *shared_slot_reserved = ok;
                ok
            }
            None => self.instances.len() < max_inst,
        };
        if !reserved {
            match on_exceed {
                ExceedAction::Throttle => return Some(StepResult::Accumulate),
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
                            *shared_slot_reserved = ok;
                            ok
                        }
                        None => self.instances.len() < max_inst,
                    };
                    if !re_reserved {
                        return Some(StepResult::Accumulate);
                    }
                }
                ExceedAction::FailRule => {
                    fail_rule(&mut self.failed, &self.shared);
                    return Some(StepResult::Accumulate);
                }
            }
        }
        None
    }

    /// `max_memory_bytes` 门：仅在新实例准入或每事件可增长规则上执行。
    ///
    /// P1②：分片下这是**近似**预算——共享总量是 check-then-act 读加上每实例插入的
    /// base-cost 增量，并发分片可能瞬时超出 ≤ shard_count-1 个新实例；`distinct_set`
    /// 等实例内增长只由周期性 `recalibrate_memory` 校正。驱逐循环 + recalibrate 把它
    /// 兜住；内存增长非原子，精确 CAS 预留不现实。`max_instances`（上一函数）则是精确的。
    ///
    /// 2026-08-31 摊还：非新实例 + 无每事件增长（纯 count/sum/min/max/avg，无
    /// distinct/历史/seq）时 `estimated_memory_bytes` 只在 insert/remove 变化（两处已
    /// 精确记账），逐事件检查纯冗余 → 只在新实例准入或可增长规则上执行
    /// （qradar/真实规则全带 limits，逐事件检查是每事件每规则浪费）。
    fn enforce_memory_limit<E: FieldSource>(
        &mut self,
        is_new: bool,
        shared_slot_reserved: bool,
        new_base: Option<usize>,
        instance_key: &InstanceKey,
        alias: &str,
        event: &E,
    ) -> Option<StepResult> {
        let limits = self.limits.as_ref()?;
        let max_bytes = limits.max_memory_bytes?;
        if !(is_new || self.memory_grows_per_event) {
            return None;
        }
        let on_exceed = limits.on_exceed.clone();
        let new_cost = new_base.unwrap_or(0);
        // P2b: with shared limits the budget is the cross-shard memory total.
        let shared_total = self
            .shared
            .as_ref()
            .map(|s| s.memory_bytes())
            .unwrap_or(self.estimated_memory_bytes);
        let total = shared_total + new_cost;
        if total >= max_bytes {
            match on_exceed {
                ExceedAction::Throttle => {
                    // N1: admission reserved a shared slot for this new key
                    // but we return before inserting — release or it leaks.
                    if shared_slot_reserved {
                        self.release_shared_instance();
                    }
                    return Some(StepResult::Accumulate);
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
                    // DropOldest：循环驱逐本地最旧实例直到腾出空间；若当前 key 自己的实例被驱逐，
                    // 下面 entry 重建会**继承**它的共享槽位（N2），此刻归还会少计；无实例可驱逐
                    // （预算被其他分片占用）则归还持有/继承的槽位并早退（N1/N2）。
                    if let EvictOutcome::Exhausted { slot_inherited } = self
                        .evict_until_memory_fits(
                            instance_key,
                            alias,
                            event,
                            is_new,
                            total,
                            max_bytes,
                        )
                    {
                        if shared_slot_reserved || slot_inherited {
                            self.release_shared_instance();
                        }
                        return Some(StepResult::Accumulate);
                    }
                }
                ExceedAction::FailRule => {
                    fail_rule(&mut self.failed, &self.shared);
                    // N1: release the un-consumed reservation (see Throttle arm).
                    if shared_slot_reserved {
                        self.release_shared_instance();
                    }
                    return Some(StepResult::Accumulate);
                }
            }
        }
        None
    }

    /// `DropOldest` 驱逐：循环移除本地最旧实例直到 `total < max_bytes`。
    ///
    /// P3-B：只驱逐本分片的本地最旧实例（跨分片优先队列不在范围内），共享计数保持
    /// 精确、驱逐公平性按分片。无实例可驱逐时返回 [`EvictOutcome::Exhausted`] 并带上
    /// `slot_inherited`，调用方据此归还槽位（N1/N2）。
    fn evict_until_memory_fits<E: FieldSource>(
        &mut self,
        instance_key: &InstanceKey,
        alias: &str,
        event: &E,
        is_new: bool,
        mut total: usize,
        max_bytes: usize,
    ) -> EvictOutcome {
        let mut slot_inherited_for_incoming = false;
        while total >= max_bytes {
            let Some(oldest_key) = self
                .instances
                .iter()
                .min_by_key(|(_, inst)| inst.created_at)
                .map(|(k, _)| k.clone())
            else {
                return EvictOutcome::Exhausted {
                    slot_inherited: slot_inherited_for_incoming,
                };
            };
            let evicting_current = oldest_key == *instance_key;
            if let Some(removed) = self.remove_instance(&oldest_key) {
                total = total.saturating_sub(removed.estimated_bytes());
                if evicting_current {
                    // N2: 下面重建的新实例接管该槽位，共享计数保持精确。
                    slot_inherited_for_incoming = true;
                } else {
                    // P1②: 永久驱逐释放共享槽位。
                    self.release_shared_instance();
                }
            }
            // 当前 key 将被重建 —— 把 base cost 记回预算，循环才会继续驱逐到装得下。
            if evicting_current && !is_new {
                total += Instance::base_estimated_bytes(&self.plan, &[], alias, event);
            }
        }
        EvictOutcome::Fits
    }

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
        // N1: 本次调用是否持有共享实例槽位（见 `admit_new_instance`）——推进期
        // 每个 early return 都必须归还，否则共享预算逐 key 泄漏直到耗尽。
        let mut shared_slot_reserved = false;
        if let Some(result) = self.admit_new_instance(is_new, &mut shared_slot_reserved) {
            return step_outcome(result, None);
        }
        // New-instance base cost：供 max_memory 门与 insert/remove 的 O(1) 记账复用
        // （精确增长由周期性 `recalibrate_memory` 校正）。
        let new_base = self.new_instance_base_cost(is_new, alias, event);
        // max_memory_bytes 门：P1② 的近似预算口径与 2026-08-31 摊还条件见
        // `enforce_memory_limit`。
        if let Some(result) = self.enforce_memory_limit(
            is_new,
            shared_slot_reserved,
            new_base,
            &instance_key,
            alias,
            event,
        ) {
            return step_outcome(result, None);
        }
        if is_new {
            self.push_expiry_candidate(&instance_key, window_start.unwrap_or(now_nanos));
        }
        // A1（2026-08-24 hop 热路径）+ 2026-09-02 摊还（qradar rules 段逐事件开销
        // 归因）：contains_key 判 is_new（两 limits 块需在 entry 前改 map）后一次 entry
        // 取代 take/put 往返；steady（非新实例 + 非 memory_grows_per_event）走 `get_mut`，
        // 免每事件 `instance_key.clone()`。语义不变：Occupied 借用原实例，Vacant 构造并
        // 插入；新实例入场时统一记一次 base_cost，永久移除仍走 remove_instance。
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
            // 新实例在此入场：统一记一次 base_cost（永久移除仍走 remove_instance）。
            account_admission_memory(
                &mut self.estimated_memory_bytes,
                &self.shared,
                instance.base_cost,
                tracks_memory,
            );
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
        let mut view = AdvanceView {
            limits: self.limits.as_ref(),
            shared: &self.shared,
            failed: &mut self.failed,
            emit_count: &mut self.emit_count,
            emit_window_start: &mut self.emit_window_start,
            plan,
            rule_name: &self.rule_name,
            processing_wall_nanos: self.processing_wall_nanos,
            pending_expiry: &mut self.pending_expiry,
            expiry_heap: &mut self.expiry_heap,
        };
        // 2b. Chain semantics: negation scan + strict adjacency.
        if let Some(result) = chain_break_phase(
            &mut view,
            instance,
            self.seq_meta.as_ref(),
            plan,
            alias,
            event,
            now_nanos,
            windows,
            row,
            masks,
            window_start,
            &instance_key,
        ) {
            return step_outcome(result, None);
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
            return step_outcome(
                advance_any_mode(
                    &mut view,
                    instance,
                    alias,
                    event,
                    now_nanos,
                    windows,
                    row,
                    masks,
                    skey,
                    &instance_key,
                    window_start,
                    trigger,
                ),
                None,
            );
        }

        let mut progress = None;
        let result = match step_phase(
            &mut view,
            instance,
            alias,
            event,
            now_nanos,
            windows,
            row,
            masks,
            skey,
            capture_progress,
            self.seq_meta.as_ref(),
            window_start,
            &instance_key,
            &mut progress,
        ) {
            Some(result) => result,
            None => emit_phase(
                &mut view,
                instance,
                skey,
                now_nanos,
                window_start,
                &instance_key,
                event,
                trigger,
            ),
        };
        if let Some(progress) = &mut progress {
            progress.instances = self.instances.len();
        }
        step_outcome(result, progress)
    }

    /// expiry 候选入堆（去重）——实现见 free fn [`push_expiry_candidate`]。
    pub(super) fn push_expiry_candidate(&mut self, key: &InstanceKey, created_at: i64) {
        push_expiry_candidate(
            &self.plan,
            &mut self.pending_expiry,
            &mut self.expiry_heap,
            key,
            created_at,
        );
    }
}
