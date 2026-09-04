//! 规则执行层（match_engine）：把编译产物 [`MatchPlan`](wf_lang::plan::MatchPlan) 驱动成告警。
//!
//! 按依赖方向分层：
//! - `cep/` —— 单实例 CEP 状态机（[`CepStateMachine`]）+ 逐事件解释求值器
//!   （`cep::eval`，issue #82/#83 的复杂路径回退），见
//!   `docs/design/columnar-match-state-machine.md`；
//! - `executor/` —— 批级执行编排：RuleExecutor / StatsExecutor / 列式路径与
//!   列式表达式后端（`executor::eval`），见
//!   `docs/design/columnar-execution-design.md`、`hot-path-vectorization-design.md`；
//! - `columnar` / `event_bridge` —— 列式批 ↔ 行事件桥；`spill` / `async_persist`
//!   —— 落盘与持久化（`docs/design/async-persist.md`）；
//! - `contract` / `tests` —— 对外契约与两后端一致性对拍（tests/l2·l3·regression）。

pub mod async_persist;
// cep 同步执行核 2026-09-04 下沉 wf_cep::cep（P4-B1）：对外 pub 面见下方
// `pub use wf_cep::cep::…`；本 pub(crate) shim 保 crate 内 `match_engine::cep::…`
// 旧路径（含 eval/key/cmp 子模块深路径），零剩码改动。
pub(crate) mod cep {
    #[allow(unused_imports)] // shim：只承接 engine 实际引用的旧路径子集
    pub(crate) use wf_cep::cep::{
        AsofLookup, BindData, CepStateMachine, CloseOutput, CloseReason, EngineHashMap,
        EngineHashSet, Event, FieldSource, JoinKey, MACHINE_ID, MatchedContext, RollingStats,
        ScopeKey, SharedLimits, StepData, StepOutcome, StepProgress, StepResult, StepState, Value,
        ValueKey, WindowLookup, accumulate_close_steps, apply_conv, close_is_qualified, eval_expr,
        eval_expr_ext, eval_field_value, eval_field_value_src, extract_key_simple,
        extract_scope_key_from_row, extract_scope_key_mixed, field_ref_name,
        precompute_join_then_keys, push_i64_exact_decimal, scope_key_from_values,
        scope_key_shard_index, value_to_string, values_equal,
    };
    pub(crate) mod eval {
        #[allow(unused_imports)]
        pub(crate) use wf_cep::cep::eval::{eval_expr, eval_expr_ext, values_equal};
        pub(crate) mod cmp {
            pub(crate) use wf_cep::cep::eval::cmp::{apply_fmt_template, timestamp_nanos_to_utc};
        }
    }
    pub(crate) mod key {
        #[allow(unused_imports)]
        pub(crate) use wf_cep::cep::key::{
            ScopeKey, StrSink, ValueKey, field_ref_name, push_i64_exact_decimal,
            scope_key_from_values, scope_key_shard_index,
        };
    }
}
mod cidr_cache {
    pub(crate) use wf_cep::cidr_cache::*;
}
pub mod columnar;
pub mod contract;
pub mod event_bridge;
mod executor;
// 纯叶已下沉 wf-cep；shim 保持 crate 内引用路径不变
mod regex_cache {
    pub(crate) use wf_cep::regex_cache::*;
}
pub mod spill;

#[cfg(test)]
mod tests;

pub use columnar::{GuardMasks, mask_to_indices};
pub use event_bridge::{
    ColumnarEvent, FieldIndex, JoinRow, TriggerEvent, WFL_FIELD_TYPE_ARRAY,
    WFL_FIELD_TYPE_METADATA_KEY, WFL_FIELD_TYPE_OBJECT, batch_event_time_nanos,
    batch_event_time_nanos_at, batch_raw_ts_nanos, batch_time_col_index, batch_to_events,
    batch_to_events_filtered, batch_to_timestamped_rows, build_field_index, column_scalar_string,
    columnar_join_rows, columnar_timestamped_join_rows, is_wfl_structured_field, materialize_rows,
    materialize_rows_filtered, wfl_structured_field_kind,
};
pub use executor::{
    DeferredLeft, DeferredPending, DistinctKey, DistinctSet, EachDirectBatchStats, ExecutionPath,
    ExecutionPathContext, PipeEachRow, PipeRowSink, RowFieldLayout, RowFields, RuleExecutor,
    RuleExecutorOptions, StatsAccum, StatsBucketAccs, StatsExecutor, StatsMaskCache,
    StatsWindowState,
};
pub use wf_cep::cep::apply_conv;
pub use wf_cep::cep::close_is_qualified;
pub use wf_cep::cep::{
    AsofLookup, BindData, CepStateMachine, CloseOutput, CloseReason, Event, FieldSource, JoinKey,
    MACHINE_ID, MatchedContext, ScopeKey, SharedLimits, StepData, StepOutcome, StepProgress,
    StepResult, Value, WindowLookup, field_ref_name, precompute_join_then_keys, values_equal,
};
pub use wf_cep::cep::{EngineHashMap, EngineHashSet};
pub(crate) use wf_cep::cep::{
    extract_key_simple, extract_scope_key_mixed, scope_key_from_values, scope_key_shard_index,
};
