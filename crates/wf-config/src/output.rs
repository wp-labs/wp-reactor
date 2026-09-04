use serde::{Deserialize, Serialize};

pub const DEFAULT_OUTPUT_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f";

#[derive(
    ::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default,
)]
#[serde(rename_all = "snake_case")]
#[moju(kind = "state", domain = "Config", module = "Config.ConfigIo")]
pub enum OutputTimeZone {
    #[default]
    Utc,
}

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
pub struct OutputConfig {
    pub time_format: String,
    #[serde(default)]
    pub time_zone: OutputTimeZone,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            time_format: DEFAULT_OUTPUT_TIME_FORMAT.to_string(),
            time_zone: OutputTimeZone::Utc,
        }
    }
}
