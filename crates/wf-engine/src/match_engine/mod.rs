pub mod columnar;
pub mod contract;
pub mod event_bridge;
mod executor;
#[allow(clippy::module_inception)]
mod match_engine;

#[cfg(test)]
mod tests;

pub use columnar::{GuardMasks, mask_to_indices};
pub use event_bridge::{
    ColumnarEvent, FieldIndex, JoinRow, WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY,
    WFL_FIELD_TYPE_OBJECT, batch_event_time_nanos, batch_event_time_nanos_at, batch_raw_ts_nanos,
    batch_time_col_index, batch_to_events, batch_to_events_filtered, batch_to_timestamped_rows,
    build_field_index, column_scalar_string, columnar_join_rows, columnar_timestamped_join_rows,
    is_wfl_structured_field, materialize_rows, materialize_rows_filtered,
    wfl_structured_field_kind,
};
pub use executor::{EachDirectBatchStats, RuleExecutor, RuleExecutorOptions, StatsExecutor};
pub use match_engine::apply_conv;
pub use match_engine::close_is_qualified;
pub use match_engine::{
    AsofLookup, CepStateMachine, CloseOutput, CloseReason, Event, FieldSource, JoinKey, MACHINE_ID,
    MatchedContext, ScopeKey, SharedLimits, StepData, StepOutcome, StepProgress, StepResult, Value,
    WindowLookup, field_ref_name,
};
pub use match_engine::{EngineHashMap, EngineHashSet};
pub(crate) use match_engine::{extract_key_simple, scope_key_from_values, scope_key_shard_index};
