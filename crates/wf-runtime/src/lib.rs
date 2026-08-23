#[macro_use]
mod log_macros;

pub mod cli;

pub(crate) mod alert_task;
pub(crate) mod engine_task;
pub mod error;
mod evictor_task;
pub mod external;
pub mod hot_reload;
pub mod lifecycle;
pub mod metrics;
pub mod perf_diag;
pub mod receiver;
mod schema_bridge;
pub mod sink_build;
pub mod source;
pub mod tracing_init;
