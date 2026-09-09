use serde::{Deserialize, Serialize};

pub const DEFAULT_OUTPUT_TIME_FORMAT: &str = wf_lang::DEFAULT_OUTPUT_TIME_FORMAT;

#[derive(
    ::jumo_derive::Jumo, Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default,
)]
#[serde(rename_all = "snake_case")]
#[jumo(kind = "state", domain = "Config", module = "Config.ConfigIo")]
pub enum OutputTimeZone {
    #[default]
    Utc,
}

#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[jumo(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
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
