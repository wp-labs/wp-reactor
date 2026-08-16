pub mod contract;
pub mod event_bridge;
mod executor;
#[allow(clippy::module_inception)]
mod match_engine;

#[cfg(test)]
mod tests;

pub use event_bridge::{
    WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY, WFL_FIELD_TYPE_OBJECT, batch_to_events,
    batch_to_events_filtered, batch_to_timestamped_rows, is_wfl_structured_field,
    wfl_structured_field_kind,
};
pub use executor::{EachDirectBatchStats, RuleExecutor, RuleExecutorOptions};
pub use match_engine::{EngineHashMap, EngineHashSet};
pub use match_engine::{
    CepStateMachine, CloseOutput, CloseReason, Event, JoinKey, MACHINE_ID, MatchedContext,
    StepData, StepOutcome, StepProgress, StepResult, Value, WindowLookup,
};
pub(crate) use match_engine::{extract_key_simple, shard_index};
