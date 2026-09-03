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
mod cep;
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

pub use cep::apply_conv;
pub use cep::close_is_qualified;
pub use cep::{
    AsofLookup, BindData, CepStateMachine, CloseOutput, CloseReason, Event, FieldSource, JoinKey,
    MACHINE_ID, MatchedContext, ScopeKey, SharedLimits, StepData, StepOutcome, StepProgress,
    StepResult, Value, WindowLookup, field_ref_name, precompute_join_then_keys, values_equal,
};
pub use cep::{EngineHashMap, EngineHashSet};
pub(crate) use cep::{
    extract_key_simple, extract_scope_key_mixed, scope_key_from_values, scope_key_shard_index,
};
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
