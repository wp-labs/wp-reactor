use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    /// Listen address, e.g. `"tcp://127.0.0.1:9800"`.
    pub listen: String,
}
