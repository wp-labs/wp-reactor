//! 列式 branch-guard 掩码（P4-B0 下沉）：`GuardMasks`。
//!
//! 批级预编译守卫掩码：`match` 状态机逐事件求值的三类 guard 位点
//! （event / close / neg）以列式 `BooleanArray` 掩码缓存替代解释求值。
//! 引擎（columnar_compile / rule_exec / cep 状态机）经
//! `wf_engine::match_engine::columnar::GuardMasks` re-export 消费，路径不变。
//! 纯 arrow 数据面（墙内允许）；不触 IO/async。

use arrow::array::{Array, BooleanArray};

use crate::value::EngineHashMap;

/// Batch-level columnar **branch-guard** masks for the three guard sites the
/// state machine evaluates per event:
///
/// - `event` — `match_plan.event_steps` (keyed `(event_step_idx, branch_idx)`);
/// - `close` — `match_plan.close_steps` accumulation guard (keyed
///   `(close_step_idx, branch_idx)`);
/// - `neg` — `match_plan.seq` negation steps (keyed `(neg_idx, 0)`, the same
///   negation-only ordering `SeqRuntime::build` produces).
#[derive(Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.ColumnarBatch")]
pub struct GuardMasks {
    event: EngineHashMap<(usize, usize), BooleanArray>,
    close: EngineHashMap<(usize, usize), BooleanArray>,
    neg: EngineHashMap<(usize, usize), BooleanArray>,
}

impl GuardMasks {
    pub fn insert_event(&mut self, step: usize, branch: usize, mask: BooleanArray) {
        self.event.insert((step, branch), mask);
    }

    pub fn insert_close(&mut self, step: usize, branch: usize, mask: BooleanArray) {
        self.close.insert((step, branch), mask);
    }

    pub fn insert_neg(&mut self, neg: usize, branch: usize, mask: BooleanArray) {
        self.neg.insert((neg, branch), mask);
    }

    /// Two-valued lookup (null → false) for "must be true" guards (event steps).
    /// `None` = no columnar mask for this `(step, branch)`.
    pub fn event_value(&self, step: usize, branch: usize, row: usize) -> Option<bool> {
        self.event.get(&(step, branch)).map(|m| m.value(row))
    }

    /// Three-valued lookup for permissive guards (close steps): `Some(Some(b))`
    /// = explicit bool, `Some(None)` = null / missing field (permissive), `None`
    /// = no columnar mask for this `(step, branch)`.
    pub fn close_value(&self, step: usize, branch: usize, row: usize) -> Option<Option<bool>> {
        self.close.get(&(step, branch)).map(|m| {
            if m.is_null(row) {
                None
            } else {
                Some(m.value(row))
            }
        })
    }

    /// Two-valued lookup (null → false) for negation guards. `None` = no
    /// columnar mask for this `(neg, branch)`.
    pub fn neg_value(&self, neg: usize, branch: usize, row: usize) -> Option<bool> {
        self.neg.get(&(neg, branch)).map(|m| m.value(row))
    }

    pub fn is_empty(&self) -> bool {
        self.event.is_empty() && self.close.is_empty() && self.neg.is_empty()
    }
}
