use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;

use wf_engine::match_engine::{CepStateMachine, RuleExecutor};
use wf_engine::window::{Router, RulePush, Window};

use crate::alert_task::SinkFanout;
use crate::metrics::RuntimeMetrics;

// ---------------------------------------------------------------------------
// WindowSource -- one window a rule task reads from
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct WindowSource {
    pub window_name: String,
    pub window: Arc<RwLock<Window>>,
    pub notify: Arc<Notify>,
    /// Rule aliases that consume rows from this window.
    pub aliases: Vec<String>,
}

// ---------------------------------------------------------------------------
// RuleTaskConfig -- everything needed to construct a RuleTask
// ---------------------------------------------------------------------------

pub(crate) struct RuleTaskConfig {
    pub machine: Option<CepStateMachine>,
    pub each_alias: Option<String>,
    pub each_time_field: Option<String>,
    pub executor: RuleExecutor,
    pub window_sources: Vec<WindowSource>,
    /// Sink delivery fanout: the rule task broadcasts each emitted alert to the
    /// per-sink channels (resolved by yield_target).
    pub sink_fanout: Arc<SinkFanout>,
    pub cancel: CancellationToken,
    pub timeout_scan_interval: Duration,
    /// Shared router for WindowLookup (joins + has()).
    pub router: Arc<Router>,
    pub metrics: Option<Arc<RuntimeMetrics>>,
    /// Yield targets that should be written back into windows for downstream rules.
    pub intermediate_targets: HashSet<String>,
    /// Output/intermediate relay targets (pipe design): every rule's yield target
    /// as a pipe. Used by the emit path to route through the pipe abstraction.
    pub pipe_registry: std::sync::Arc<wf_engine::pipe::PipeRegistry>,
    /// End-of-stream counter: incremented each time the input sources report
    /// the stream ended. The rule task flushes its instances on every EOS
    /// (counter change) but keeps running so a daemon can accept multiple
    /// finite inputs.
    pub eos_flush: watch::Receiver<u64>,
    /// Push-mode input channel. When `Some`, the rule task consumes pushed
    /// `Arc<Vec<Arc<Event>>>` from it instead of pulling from the window read lock
    /// (R1). When `None`, the task falls back to the legacy notify + pull loop.
    pub push_rx: Option<mpsc::Receiver<RulePush>>,
}
