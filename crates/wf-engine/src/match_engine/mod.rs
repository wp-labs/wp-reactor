pub mod contract;
pub mod event_bridge;
mod executor;
#[allow(clippy::module_inception)]
mod match_engine;

#[cfg(test)]
mod tests;

pub use event_bridge::{batch_to_events, batch_to_timestamped_rows};
pub use executor::RuleExecutor;
pub use match_engine::{
    CepStateMachine, CloseOutput, CloseReason, Event, MACHINE_ID, MatchedContext, StepData,
    StepResult, Value, WindowLookup,
};
