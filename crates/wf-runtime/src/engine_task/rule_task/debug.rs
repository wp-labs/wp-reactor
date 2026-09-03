//! 规则批级调试统计（debug_stats）：批次处理计数 + 摘要日志限流。
//! 实现在主模块 `RuleTask` 的 dump_profiling / 批摘要路径使用。

use std::collections::HashSet;

use wf_engine::alert::OutputRecord;

/// 单批 detail 日志条数上限（超限计 suppressed）。
pub(crate) const DEBUG_DETAIL_LIMIT: usize = 20;

#[derive(Default)]
pub(crate) struct RuleBatchDebugStats {
    pub(crate) input_events: usize,
    pub(crate) alias_passed: usize,
    pub(crate) alias_rejected: usize,
    pub(crate) accumulated: usize,
    pub(crate) advanced: usize,
    pub(crate) matched: usize,
    pub(crate) output_emitted: usize,
    pub(crate) output_none: usize,
    pub(crate) intermediate_emitted: usize,
    pub(crate) errors: usize,
    pub(crate) detail_logged: usize,
    pub(crate) detail_suppressed: usize,
}

impl RuleBatchDebugStats {
    pub(crate) fn can_log_detail(&self) -> bool {
        self.detail_logged < DEBUG_DETAIL_LIMIT
    }

    pub(crate) fn allow_detail(&mut self) -> bool {
        if self.detail_logged < DEBUG_DETAIL_LIMIT {
            self.detail_logged += 1;
            true
        } else {
            self.detail_suppressed += 1;
            false
        }
    }

    pub(crate) fn count_output(
        &mut self,
        record: &OutputRecord,
        intermediate_targets: &HashSet<String>,
    ) {
        if intermediate_targets.contains(&*record.yield_target) {
            self.intermediate_emitted += 1;
        } else {
            self.output_emitted += 1;
        }
    }
}
