use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertConfig {
    /// Alert output destinations (URI list, e.g. `"file:///var/log/wf-alerts.jsonl"`).
    pub sinks: Vec<String>,
}
