mod dispatch;
mod runtime;

#[cfg(test)]
mod coverage_r4;

pub use dispatch::SinkDispatcher;
pub use runtime::{SinkRuntime, WfMetaDisableMatcher};
