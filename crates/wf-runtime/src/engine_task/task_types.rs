use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;

use wf_engine::alert::OutputRecord;
use wf_engine::match_engine::{CepStateMachine, RuleExecutor};
use wf_engine::window::{Router, Window};

use crate::metrics::RuntimeMetrics;

// ---------------------------------------------------------------------------
// WindowSource -- one window a rule task reads from
// ---------------------------------------------------------------------------

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
    /// All alert consumer senders; the rule task round-robins emits across them
    /// so output processing is not capped by a single consumer task.
    pub alert_txs: Vec<mpsc::Sender<OutputRecord>>,
    pub cancel: CancellationToken,
    pub timeout_scan_interval: Duration,
    /// Shared router for WindowLookup (joins + has()).
    pub router: Arc<Router>,
    pub metrics: Option<Arc<RuntimeMetrics>>,
    /// Yield targets that should be written back into windows for downstream rules.
    pub intermediate_targets: HashSet<String>,
    /// End-of-stream counter: incremented each time the input sources report
    /// the stream ended. The rule task flushes its instances on every EOS
    /// (counter change) but keeps running so a daemon can accept multiple
    /// finite inputs.
    pub eos_flush: watch::Receiver<u64>,
}
