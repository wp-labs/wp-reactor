use serde::{Deserialize, Serialize};

use crate::types::HumanDuration;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeConfig {
    /// Rule execution parallelism (Semaphore upper limit).
    pub executor_parallelism: usize,
    /// Single rule execution timeout.
    pub rule_exec_timeout: HumanDuration,
    /// Window Schema (.wfs) file paths.
    pub window_schemas: Vec<String>,
    /// WFL rule (.wfl) file paths.
    pub wfl_rules: Vec<String>,
}
