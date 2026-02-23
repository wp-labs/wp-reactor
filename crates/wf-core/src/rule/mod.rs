pub mod contract;
pub mod event_bridge;
mod executor;
mod match_engine;

#[cfg(test)]
mod tests;

pub use event_bridge::batch_to_events;
pub use executor::RuleExecutor;
pub use match_engine::{
    CepStateMachine, CloseOutput, CloseReason, Event, MatchedContext, StepData, StepResult, Value,
    WindowLookup,
};
