mod actor;
mod buffer;
mod commit;
mod evictor;
mod fanout;
mod progress;
pub mod provider;
mod registry;
mod router;

pub use actor::{
    WINDOW_CHANNEL_DEPTH, WindowAppendReport, WindowMailbox, WindowMsg, acquire_window_budget,
    run_window_actor,
};
pub use buffer::{AppendOutcome, Window, WindowParams, content_bytes, events_bytes};
pub use evictor::{EvictReport, EvictionGate, Evictor, WindowEvictCount};
pub(crate) use fanout::scope_key_columnar;
pub(crate) use fanout::scope_key_from_column;
pub use fanout::{RuleFanout, RulePush};
pub use progress::WindowProgress;
pub use provider::ProviderWindow;
pub use registry::{WindowDef, WindowRegistry};
pub use router::{ParsedRoute, ParsedWindow, RouteReport, Router, WindowRouteOutcome};
