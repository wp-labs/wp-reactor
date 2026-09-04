//! cep 事件推进编排面：`CepStateMachine` 的事件入口（`advance` 家族）与逐事件
//! 路由/诊断（`advance_at_with_diagnostics`：scope-key 解析、join-then-key 预解析
//! 复核、HOP/固定窗按窗扇出）。单窗实例推进在 `window.rs`（`advance_window`），
//! 到期收口在 `expiry.rs`。
//!
//! 结构定义/构造与共享簿记留在 `super`（mod.rs）——本子模块作为 cep 后代对父
//! 模块私有字段/方法直接可达（可见性只向下流）；原私有方法下沉提 `pub(super)`
//! 保持原 cep 子树可见域（含 sibling 测试直调面）。

use wf_lang::plan::WindowSpec;

use super::key::{ScopeKey, extract_scope_key_mixed, scope_key_from_values};
use super::types::{Event, FieldSource, StepOutcome, StepResult, Value, WindowLookup};
use super::{CepStateMachine, merge_step_outcome, resolve_key_join_scope_key, step_outcome};
use crate::masks::GuardMasks;
use crate::row_views::TriggerEvent;

impl CepStateMachine {
    /// Feed one event (arriving on `alias`) into the state machine.
    ///
    /// Extracts event time from the configured `time_field`, falling back to 0.
    pub fn advance(&mut self, alias: &str, event: &Event) -> StepResult {
        self.advance_with(alias, event, None)
    }

    /// Feed one event with optional window lookup for `window.has()` in guards.
    pub fn advance_with(
        &mut self,
        alias: &str,
        event: &Event,
        windows: Option<&dyn WindowLookup>,
    ) -> StepResult {
        let event_nanos = self.extract_event_time(event);
        self.advance_at_with(alias, event, event_nanos, windows)
    }

    /// Feed one event with an explicit event-time timestamp (nanoseconds since epoch).
    pub fn advance_at(&mut self, alias: &str, event: &Event, now_nanos: i64) -> StepResult {
        self.advance_at_with(alias, event, now_nanos, None)
    }

    /// Feed one event with explicit timestamp and optional window lookup.
    pub fn advance_at_with(
        &mut self,
        alias: &str,
        event: &Event,
        now_nanos: i64,
        windows: Option<&dyn WindowLookup>,
    ) -> StepResult {
        self.advance_at_with_masks(alias, event, now_nanos, windows, 0, None)
    }

    /// Like [`Self::advance_at_with`], but with batch-level columnar branch-guard
    /// masks and the row index within the current batch. `masks` may be `None`
    /// (interpreted fallback for every branch).
    ///
    /// Generic over [`FieldSource`]: the eager path passes `&Event`, the
    /// deferred columnar path passes `&ColumnarEvent` (P3 FieldView — hit rows
    /// are fed straight from the batch, no HashMap materialization).
    pub fn advance_at_with_masks<E: FieldSource>(
        &mut self,
        alias: &str,
        event: &E,
        now_nanos: i64,
        windows: Option<&dyn WindowLookup>,
        row: usize,
        masks: Option<&GuardMasks>,
    ) -> StepResult {
        self.advance_at_with_diagnostics(
            alias, event, now_nanos, windows, row, masks, false, None, None,
        )
        .result
    }

    /// [`Self::advance_at_with_masks`] with a batch-precomputed join-then-key
    /// scope key (2026-08-23 批级 join-then-key): rule_task resolves the
    /// key_join lookup once per unique driver key for a whole batch, then feeds
    /// each row's result here — the per-event index lookup + `values_equal`
    /// re-check + key-field materialization (q4/q6 advance 88.8%) moves out of
    /// the per-event loop. `key_override` semantics: `Some(Some(keys))` = use;
    /// `Some(None)` = pre-resolved miss (skip); `None` = internal resolution.
    #[allow(clippy::too_many_arguments)]
    pub fn advance_at_with_masks_key<E: FieldSource>(
        &mut self,
        alias: &str,
        event: &E,
        now_nanos: i64,
        windows: Option<&dyn WindowLookup>,
        row: usize,
        masks: Option<&GuardMasks>,
        key_override: Option<&Option<Vec<Value>>>,
    ) -> StepResult {
        self.advance_at_with_masks_key_capture(
            alias,
            event,
            now_nanos,
            windows,
            row,
            masks,
            key_override,
            None,
        )
    }

    /// [`Self::advance_at_with_masks_key`] plus an owned trigger-row capture
    /// (M3 §11.6): when a fire needs a trigger event, use the caller's
    /// prebuilt [`TriggerEvent`] (deferred path = owned columnar snapshot, no
    /// per-fire `to_event()`) instead of materializing from `event`. `None` =
    /// fall back to materializing `event.to_event()` (row-mode / tests).
    #[allow(clippy::too_many_arguments)]
    pub fn advance_at_with_masks_key_capture<E: FieldSource>(
        &mut self,
        alias: &str,
        event: &E,
        now_nanos: i64,
        windows: Option<&dyn WindowLookup>,
        row: usize,
        masks: Option<&GuardMasks>,
        key_override: Option<&Option<Vec<Value>>>,
        trigger: Option<&TriggerEvent>,
    ) -> StepResult {
        self.advance_at_with_diagnostics(
            alias,
            event,
            now_nanos,
            windows,
            row,
            masks,
            false,
            key_override,
            trigger,
        )
        .result
    }

    /// Feed one event and return both the state-machine result and diagnostic
    /// progress for the evaluated step, when progress can be captured.
    pub fn advance_at_with_progress<E: FieldSource>(
        &mut self,
        alias: &str,
        event: &E,
        now_nanos: i64,
        windows: Option<&dyn WindowLookup>,
    ) -> StepOutcome {
        self.advance_at_with_diagnostics(
            alias, event, now_nanos, windows, 0, None, true, None, None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn advance_at_with_diagnostics<E: FieldSource>(
        &mut self,
        alias: &str,
        event: &E,
        now_nanos: i64,
        windows: Option<&dyn WindowLookup>,
        row: usize,
        masks: Option<&GuardMasks>,
        capture_progress: bool,
        // 批级 join-then-key 预解析（2026-08-23）：rule_task 对一批事件批级去重
        // join lookup 后，每行传 `Some(Some(keys))`（用预解析 key，跳过内部
        // 每事件 lookup）；`Some(None)` = 该行预解析 miss（跳过，同内部 miss）；
        // `None` = 无预解析（走原逻辑，含 key_join / extract_key）。
        key_override: Option<&Option<Vec<Value>>>,
        // M3 §11.6：owned trigger-row capture（deferred 列式快照）。None =
        // fire 时回退 `event.to_event()`（row-mode / 测试）。
        trigger: Option<&TriggerEvent>,
    ) -> StepOutcome {
        // FailRule: once the rule has failed, reject all future events.
        // P2b: with shared limits, a FailRule latch on any shard fails the rule.
        if self.failed || self.shared.as_ref().is_some_and(|s| s.is_failed()) {
            return step_outcome(StepResult::Accumulate, None);
        }

        // Update watermark
        if now_nanos > self.watermark_nanos {
            self.watermark_nanos = now_nanos;
        }

        // 1. Extract the rule's typed match scope key. Join-then-key (Path A):
        //    when the key lives on a snapshot join's right window (plan.key_join),
        //    resolve it by looking the event's join-left value up in the joined
        //    window and reading the key field off the joined row. A miss anywhere
        //    (no lookup, missing left field, join miss, key absent on the row) is
        //    the same as a missing key field: skip the event.
        //    `key_override` 存在时（rule_task 批级预解析）直接用其结果，跳过内部
        //    每事件 lookup（q4/q6 join-then-key 热路径：每 bid 一次索引 lookup
        //    + values_equal 复核 + key 字段物化，占 advance 88.8%）。
        //    普通规则走 `FieldSource::extract_scope_key`：ColumnarEvent 列式直读
        //    （免 `Value`/`Vec` 分配，qradar 单 key 热路径），Event 走行式等价转换。
        let skey: ScopeKey = if let Some(kjp) = &self.plan.key_join {
            let scope_key = if let Some(override_key) = key_override {
                let Some(keys) = override_key else {
                    return step_outcome(StepResult::Accumulate, None); // 预解析 miss
                };
                keys.clone()
            } else {
                let Some(scope_key) = resolve_key_join_scope_key(kjp, event, windows) else {
                    return step_outcome(StepResult::Accumulate, None);
                };
                scope_key
            };
            scope_key_from_values(&scope_key)
        } else if self.plan.key_map.is_none() && self.plan.key_exprs.iter().any(Option::is_some) {
            // issue #80 派生 key（表达式 let，如 coalesce/concat/case 结果作 key）：
            // 无法列式直读，逐位混提——有表达式槽的键位对触发事件求值，None 键位
            // 按普通字段/路径提取；任一键缺失/求值失败 → skip（与普通 key 缺失语义一致）。
            // key_map 存在时禁用（编译器保证 key_exprs 只在 key_map.is_none() 时装配，
            // 此条件防御手工构造的 plan 并存）。
            match extract_scope_key_mixed(event, &self.plan.keys, &self.plan.key_exprs, alias) {
                Some(skey) => skey,
                None => return step_outcome(StepResult::Accumulate, None),
            }
        } else {
            match event.extract_scope_key(&self.plan.keys, self.plan.key_map.as_deref(), alias) {
                Some(skey) => skey,
                None => return step_outcome(StepResult::Accumulate, None), // missing key field → skip
            }
        };

        // H1（2026-08-26，q5 hop 键 churn）：typed skey 提升到窗口扇出循环外。
        // 原先每窗口各自 `scope_key_from_values` 重建 + `scope_key.clone()`
        // （Vec<Value> 堆分配）——hop(10s,2s) 每事件 5 窗口 = 5 次重建 + 5 次
        // 分配；skey 只建一次，非命中窗口零分配（ctx 仅在命中时 to_vec，成本
        // 与旧每次 clone 相同，命中最坏持平、非命中纯省）。语义不变：
        // scope_key_from_values 确定性纯函数，同输入同结果（对拍测试锁定）。

        // Build per-window routing. HOP windows (size, slide): one event belongs
        // to `size/slide` overlapping windows aligned to epoch slide boundaries;
        // each (scope x window_start) is a separate instance (fixed-style keys).
        let mut best: Option<StepOutcome> = None;
        match self.plan.window_spec {
            WindowSpec::Hop { size, slide } => {
                let size_ns = size.as_nanos() as i64;
                let slide_ns = slide.as_nanos() as i64;
                let k_min = (now_nanos - size_ns).div_euclid(slide_ns) + 1;
                let k_max = now_nanos.div_euclid(slide_ns);
                for k in k_min..=k_max {
                    let out = self.advance_window(
                        alias,
                        event,
                        now_nanos,
                        windows,
                        row,
                        masks,
                        capture_progress,
                        &skey,
                        Some(k * slide_ns),
                        trigger,
                    );
                    best = Some(match best {
                        Some(prev) => merge_step_outcome(prev, out),
                        None => out,
                    });
                }
            }
            WindowSpec::Fixed(dur) => {
                let dur_nanos = dur.as_nanos() as i64;
                let bucket_start = (now_nanos / dur_nanos) * dur_nanos;
                best = Some(self.advance_window(
                    alias,
                    event,
                    now_nanos,
                    windows,
                    row,
                    masks,
                    capture_progress,
                    &skey,
                    Some(bucket_start),
                    trigger,
                ));
            }
            WindowSpec::Sliding(_) | WindowSpec::Session(_) => {
                best = Some(self.advance_window(
                    alias,
                    event,
                    now_nanos,
                    windows,
                    row,
                    masks,
                    capture_progress,
                    &skey,
                    None,
                    trigger,
                ));
            }
        }
        best.unwrap_or_else(|| step_outcome(StepResult::Accumulate, None))
    }
}
