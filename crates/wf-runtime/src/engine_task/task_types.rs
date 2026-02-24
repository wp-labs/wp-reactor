use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use wf_core::alert::AlertRecord;
use wf_core::rule::{CepStateMachine, RuleExecutor};
use wf_core::window::{Router, Window};

// ---------------------------------------------------------------------------
// WindowSource -- one window a rule task reads from
// ---------------------------------------------------------------------------

pub(crate) struct WindowSource {
    pub window_name: String,
    pub window: Arc<RwLock<Window>>,
    pub notify: Arc<Notify>,
    /// Stream names that flow into this window.
    pub stream_names: Vec<String>,
}

// ---------------------------------------------------------------------------
// RuleTaskConfig -- everything needed to construct a RuleTask
// ---------------------------------------------------------------------------

pub(crate) struct RuleTaskConfig {
    pub machine: CepStateMachine,
    pub executor: RuleExecutor,
    pub window_sources: Vec<WindowSource>,
    /// stream_name -> Vec<alias>: which CEP aliases receive events from each stream.
    pub stream_aliases: HashMap<String, Vec<String>>,
    pub alert_tx: mpsc::Sender<AlertRecord>,
    pub cancel: CancellationToken,
    pub timeout_scan_interval: Duration,
    /// Shared router for WindowLookup (joins + has()).
    pub router: Arc<Router>,
}
