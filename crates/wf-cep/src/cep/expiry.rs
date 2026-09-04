//! cep 到期收口面：手工 close（`close`）/ 到期扫描（`scan_expired*`）/ 全量收口
//! （`close_all*`），含窗口完整性判定、`rate_limit_close` close 侧限速与 conv 收口
//! 包装（`apply_conv_filtered` 实现在 `super`）。事件推进在 `advance.rs`/`window.rs`，
//! 结构定义/构造与共享簿记（remove/release/expire_time_for）留在 `super`。

use std::cmp::Reverse;

use wf_lang::ast::CloseMode;
use wf_lang::plan::{ConvPlan, ExceedAction, WindowSpec};

use super::close::evaluate_close;
use super::key::{InstanceKey, scope_key_from_values};
use super::types::{CloseOutput, CloseReason, Value};
use super::{CepStateMachine, apply_conv_filtered, fail_rule, throttle_allows};

/// Max expiry candidates processed per `scan_expired_at` call (incremental
/// expiry). Bounds each sweep so a far-ahead watermark cannot pop the whole
/// heap in one call and starve the pipeline (see `scan_expired_at`).
const MAX_EXPIRY_SCAN_BUDGET: usize = 1024;

impl CepStateMachine {
    /// Close a specific instance by scope key, evaluating close_steps.
    ///
    /// Removes the instance from the map and returns the [`CloseOutput`].
    /// Returns `None` if no instance exists for the given scope key.
    ///
    /// For fixed windows, multiple bucket instances may exist for the same
    /// scope key. This method closes the **oldest** bucket instance (by
    /// `created_at`). Call repeatedly to drain all buckets.
    pub fn close(&mut self, scope_key: &[Value], reason: CloseReason) -> Option<CloseOutput> {
        let skey = scope_key_from_values(scope_key);

        let instance_key = match self.plan.window_spec {
            WindowSpec::Sliding(_) | WindowSpec::Session(_) => InstanceKey::sliding(&skey),
            WindowSpec::Fixed(_) | WindowSpec::Hop { .. } => self
                .instances
                .iter()
                .filter(|(k, _)| k.matches_scope(&skey))
                .min_by_key(|(_, inst)| inst.created_at)
                .map(|(k, _)| k.clone())?,
        };

        let instance = self.remove_instance(&instance_key)?;
        // P1②: closing an instance is a permanent remove — release its slot.
        self.release_shared_instance();
        let mut output = evaluate_close(
            &self.rule_name,
            &self.plan,
            instance,
            instance_key.scope_key_values(),
            reason,
            self.watermark_nanos,
            self.processing_wall_nanos,
        );
        self.rate_limit_close(&mut output, self.watermark_nanos);
        Some(output)
    }

    /// Scan all instances for maxspan expiry using the internal watermark.
    ///
    /// Used by the scheduler on periodic ticks.
    pub fn scan_expired(&mut self) -> Vec<CloseOutput> {
        self.scan_expired_at(self.watermark_nanos)
    }

    /// Scan all instances for maxspan expiry using an explicit watermark,
    /// returning every expired instance's [`CloseOutput`] (qualified or not) —
    /// the full-close contract used by the oracle and tests.
    ///
    /// Each expired instance's close output uses `created_at + maxspan` as its
    /// watermark (the logical expiry time), rather than the detection-time
    /// watermark. This makes `fired_at` deterministic regardless of batch size
    /// or scan frequency.
    pub fn scan_expired_at(&mut self, watermark_nanos: i64) -> Vec<CloseOutput> {
        self.scan_expired_at_impl(watermark_nanos, false, MAX_EXPIRY_SCAN_BUDGET)
    }

    /// Like [`Self::scan_expired_at`], but skips building [`CloseOutput`]s for
    /// instances that can never produce an alert.
    ///
    /// For rules with **no close steps** the qualification is decidable from the
    /// instance alone without building a CloseOutput:
    ///
    ///   - `And` mode: qualifies iff `event_ok` (`close_ok` is always true)
    ///   - `Or` mode: never qualifies (empty `close_step_data`)
    ///
    /// `event_ok` is a cheap bool on the instance. At 100M-scale count rules
    /// (q5) the vast majority of expiring instances never matched, so
    /// `evaluate_close` (close-steps eval + bind snapshot + completed-steps
    /// move) for each of them is pure waste that monopolizes the rule task and
    /// starves push consumption. The instance is removed identically either
    /// way, so skipping neither defers expiry nor holds memory. Callers that
    /// only process qualifying closes (the rule-task hot path, conv stage)
    /// can use this and observe identical output.
    pub fn scan_expired_at_skip_non_alerting(&mut self, watermark_nanos: i64) -> Vec<CloseOutput> {
        self.scan_expired_at_impl(watermark_nanos, true, MAX_EXPIRY_SCAN_BUDGET)
    }

    /// Like [`Self::scan_expired_at_skip_non_alerting`], but with an **unbounded**
    /// expiry budget. Only safe off the event hot path (periodic `scan_timeouts`,
    /// where the push pipeline is idle): a far-ahead watermark here pops the whole
    /// remaining heap in one call instead of deferring — fixed-window rules whose
    /// final bucket expires past the last event time depend on this sweep to close
    /// (q16 30M dropped the final bucket: 1.48M vs 1.89M ideal with a 1024 budget).
    pub fn scan_expired_at_skip_non_alerting_unbounded(
        &mut self,
        watermark_nanos: i64,
    ) -> Vec<CloseOutput> {
        self.scan_expired_at_impl(watermark_nanos, true, usize::MAX)
    }

    pub(super) fn scan_expired_at_impl(
        &mut self,
        watermark_nanos: i64,
        skip_non_alerting: bool,
        budget: usize,
    ) -> Vec<CloseOutput> {
        let mut results = Vec::new();
        // Incremental expiry: bound each sweep so a far-ahead watermark cannot
        // pop millions of candidates in a single call and starve push
        // consumption (q5/q6/q7 froze at 30M+ — the sweep occupied the rule
        // task, the push channel filled, the pipeline froze). Remaining
        // candidates stay in the heap and are processed on the next scan
        // (per-row in the deferred loop + periodic `scan_timeouts`).
        let mut budget = budget;
        while let Some(Reverse((candidate_expire, key))) = self.expiry_heap.peek().cloned() {
            if candidate_expire > watermark_nanos || budget == 0 {
                break;
            }
            budget -= 1;
            self.expiry_heap.pop();
            self.pending_expiry.remove(&key);

            let current_expire = match self.instances.get(&key) {
                Some(instance) => Self::expire_time_for(&self.plan.window_spec, instance),
                None => continue, // stale candidate for an already-removed instance
            };

            if current_expire > watermark_nanos {
                // Session windows refresh expiry as events arrive. Re-queue
                // this key with the up-to-date expiry and continue.
                self.pending_expiry.insert(key.clone());
                self.expiry_heap.push(Reverse((current_expire, key)));
                continue;
            }

            if let Some(instance) = self.remove_instance(&key) {
                // P1②: expiry is a permanent remove — release its slot.
                self.release_shared_instance();
                let skip_close = skip_non_alerting
                    && self.plan.close_steps.is_empty()
                    && match self.plan.close_mode {
                        CloseMode::And => !instance.event_ok,
                        CloseMode::Or => true,
                    };
                if skip_close {
                    continue;
                }
                let mut output = evaluate_close(
                    &self.rule_name,
                    &self.plan,
                    instance,
                    key.scope_key_values(),
                    CloseReason::Timeout,
                    current_expire,
                    self.processing_wall_nanos,
                );
                self.rate_limit_close(&mut output, current_expire);
                results.push(output);
            }
        }
        results
    }

    /// Scan expired instances and apply conv transformations if configured.
    ///
    /// Filters out non-qualifying outputs (`!event_ok || !close_ok`) before
    /// applying conv, so that `top`/`dedup` operate only on entries that
    /// would actually produce alerts.
    pub fn scan_expired_at_with_conv(
        &mut self,
        watermark_nanos: i64,
        conv_plan: Option<&ConvPlan>,
    ) -> Vec<CloseOutput> {
        let outputs = self.scan_expired_at(watermark_nanos);
        apply_conv_filtered(outputs, conv_plan, &self.plan.keys)
    }

    /// [`Self::scan_expired_at_with_conv`] over the skip-non-alerting scan — for
    /// the rule-task hot path where non-qualifying closes are discarded anyway.
    pub fn scan_expired_at_with_conv_skip_non_alerting(
        &mut self,
        watermark_nanos: i64,
        conv_plan: Option<&ConvPlan>,
    ) -> Vec<CloseOutput> {
        let outputs = self.scan_expired_at_skip_non_alerting(watermark_nanos);
        apply_conv_filtered(outputs, conv_plan, &self.plan.keys)
    }

    /// [`Self::scan_expired_at_with_conv_skip_non_alerting`] with the unbounded
    /// expiry budget (off the event hot path only, see
    /// [`Self::scan_expired_at_skip_non_alerting_unbounded`]).
    pub fn scan_expired_at_with_conv_skip_non_alerting_unbounded(
        &mut self,
        watermark_nanos: i64,
        conv_plan: Option<&ConvPlan>,
    ) -> Vec<CloseOutput> {
        let outputs = self.scan_expired_at_skip_non_alerting_unbounded(watermark_nanos);
        apply_conv_filtered(outputs, conv_plan, &self.plan.keys)
    }

    /// Close all active instances with optional conv transformations.
    ///
    /// Like [`close_all`], but applies conv to the qualifying outputs
    /// (where `event_ok && close_ok`) before returning.
    pub fn close_all_with_conv(
        &mut self,
        reason: CloseReason,
        conv_plan: Option<&ConvPlan>,
    ) -> Vec<CloseOutput> {
        let outputs = self.close_all(reason);
        apply_conv_filtered(outputs, conv_plan, &self.plan.keys)
    }

    /// 窗口粒度向上对齐（真 ceil）：`wm` 恰在边界时返回 `wm` 本身（不再 +1
    /// 档——旧 `div_euclid+1` 在整除时多对齐一档，会把下一桶误判完整）。
    pub(super) fn ceil_align(wm: i64, step_ns: i64) -> i64 {
        let q = wm.div_euclid(step_ns);
        if wm.rem_euclid(step_ns) == 0 {
            wm
        } else {
            q.saturating_add(1).saturating_mul(step_ns)
        }
    }

    /// Close all active instances, returning a [`CloseOutput`] for each.
    ///
    /// Used during shutdown to flush all in-flight state.
    ///
    /// 2026-08-23 q5 修复：HOP/Fixed 窗口只收口**完整窗口**（`w_start + size ≤
    /// 最终事件时间 watermark`）——尾部未完整窗口（w_end 超出数据末尾）flush
    /// 强制收口会多输出（oracle/Flink 事件时间到末尾即止，未关闭窗口不发射；
    /// q5 10M 多 3 条：992/994/996s 窗口 w_end=1002/1004/1006 > 1000s）。
    /// 实例仍被移除并释放共享槽（不泄漏），只是不产出 CloseOutput。
    pub fn close_all(&mut self, reason: CloseReason) -> Vec<CloseOutput> {
        // Sort by (created_at, key) for fully deterministic rate limiting
        // order, same rationale as scan_expired_at.
        let mut keys: Vec<(InstanceKey, i64)> = self
            .instances
            .iter()
            .map(|(k, inst)| (k.clone(), inst.created_at))
            .collect();
        keys.sort_by(|(k1, t1), (k2, t2)| t1.cmp(t2).then_with(|| k1.cmp(k2)));
        let mut results = Vec::with_capacity(keys.len());
        let wm = self.watermark_nanos;
        // 2026-08-30 修复（q7 尾桶缺失，verify_file L3 对拍定位）：
        // - 窗口终点用**窗口起点 + size**（`floor(created_at/size)×size`），而非
        //   created_at + size（实例创建可能晚于窗口起点——尾桶第一个事件 > 桶起点
        //   时 created_at + size 虚高一个桶内偏移 → 误判未完整）；
        // - 水位按窗口粒度**向上对齐到桶边界**（`ceil(wm/size)×size`），对齐 oracle
        //   流末用数据覆盖末尾（scenario 边界 eos_nanos）扫收口的语义——尾桶
        //   （窗口终点恰在数据覆盖末尾）完整收口。此前用最后事件时间判 incomplete
        //   → 尾桶被丢（q7 oracle 10 vs 引擎 9 实测）。
        // sliding 无窗口完整性概念，全部收口。
        let aligned_wm: Option<i64> = if wm > 0 {
            match &self.plan.window_spec {
                // 2026-08-30 对齐粒度修正：hop 窗口在 **slide** 边界收口
                // （w_end = k*slide + size，k 为 slide 倍数），水位对齐应取 slide
                // 粒度——用 size 会把尾部未收口的 hop 窗（end ∈ (wm, ceil(wm/size))
                // 段内）误判为完整（hop 单测回归：10s/2s 窗 wm=T+6s 时 size 对齐到
                // T+10s → T+8s/T+10s 两个未完整窗被 flush 发射；slide 对齐到
                // T+6s → 无）。fixed 的 slide == size，无差异。
                // 向上对齐必须用**真 ceil**（wm 恰在边界时不再 +1 档）：旧
                // `div_euclid+1` 在整除时多对齐一档 → wm=整边界时把下一桶误判完整。
                WindowSpec::Hop { slide, .. } => {
                    let step_ns = slide.as_nanos() as i64;
                    Some(Self::ceil_align(wm, step_ns))
                }
                WindowSpec::Fixed(size) => {
                    let size_ns = size.as_nanos() as i64;
                    // 水位按桶大小向上对齐（尾桶终点恰在数据覆盖末尾 → 完整）
                    Some(Self::ceil_align(wm, size_ns))
                }
                _ => None,
            }
        } else {
            None
        };
        for (key, _) in keys {
            if let Some(instance) = self.remove_instance(&key) {
                // P1②: close_all is a permanent remove — release each slot.
                self.release_shared_instance();
                // 尾部未完整窗口：释放实例但不输出（oracle/Flink 语义）。
                // wm ≤ 0 表示无事件时间推进（空流/测试直接 close_all）——
                // 不适用窗口完整性判定，保留旧行为（全部收口）。
                // 2026-08-23 q11 修复：session 同源——会话到期 = 最后事件时间
                // + gap（随事件延长），尾部未超时会话（last_event + gap > wm）
                // 释放实例但不发射（Q11 10M 实测多 204/197095≈0.1%）。
                let incomplete = wm > 0
                    && match &self.plan.window_spec {
                        WindowSpec::Hop { size, slide } => {
                            let size_ns = size.as_nanos() as i64;
                            let step_ns = slide.as_nanos() as i64;
                            // hop 窗口起点按 slide 对齐
                            let w_start = instance
                                .created_at
                                .div_euclid(step_ns)
                                .saturating_mul(step_ns);
                            w_start.saturating_add(size_ns) > aligned_wm.unwrap_or(i64::MAX)
                        }
                        WindowSpec::Fixed(size) => {
                            let size_ns = size.as_nanos() as i64;
                            // fixed 窗口起点按 size 对齐（slide = size）
                            let w_start = instance
                                .created_at
                                .div_euclid(size_ns)
                                .saturating_mul(size_ns);
                            w_start.saturating_add(size_ns) > aligned_wm.unwrap_or(i64::MAX)
                        }
                        WindowSpec::Session(gap) => {
                            instance
                                .last_event_nanos
                                .saturating_add(gap.as_nanos() as i64)
                                > wm
                        }
                        _ => false,
                    };
                if incomplete {
                    continue;
                }
                let mut output = evaluate_close(
                    &self.rule_name,
                    &self.plan,
                    instance,
                    key.scope_key_values(),
                    reason,
                    wm,
                    self.processing_wall_nanos,
                );
                self.rate_limit_close(&mut output, wm);
                results.push(output);
            }
        }
        self.expiry_heap.clear();
        self.pending_expiry.clear();
        results
    }

    /// Apply max_throttle to a close output that would produce an alert.
    ///
    /// If the output would emit (`event_ok && close_ok`) and the rate limit
    /// is exceeded, suppresses emission by clearing `close_ok`. This shares
    /// the same sliding-window counter used by the match path.
    pub(super) fn rate_limit_close(&mut self, output: &mut CloseOutput, now_nanos: i64) {
        // P2c: shards in raw-conv mode skip inline throttle — the conv stage
        // applies the (shared) rate limit on the aggregated batch.
        if self.raw_conv_mode {
            return;
        }
        // Check if this output would emit based on close mode
        let would_emit = match output.close_mode {
            CloseMode::And => output.event_ok && output.close_ok,
            CloseMode::Or => output.close_ok && !output.close_step_data.is_empty(),
        };
        if !would_emit {
            return; // won't emit an alert anyway
        }
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
                    output.close_ok = false;
                }
                ExceedAction::FailRule => {
                    fail_rule(&mut self.failed, &self.shared);
                    output.close_ok = false;
                }
            }
        }
    }
}
