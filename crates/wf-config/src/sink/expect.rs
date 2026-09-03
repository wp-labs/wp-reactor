use serde::Deserialize;

/// Group-level expect specification.
///
/// Controls expected delivery behavior for a sink group (e.g. retry, timeout).
#[derive(Debug, Clone, Default, Deserialize, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Config", module = "Config.SinkConfig")]
pub struct GroupExpectSpec {
    /// Whether delivery to this group is mandatory.
    #[serde(default)]
    pub required: bool,
}

/// Per-sink expect overrides within a group.
#[derive(Debug, Clone, Default, Deserialize, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Config", module = "Config.SinkConfig")]
pub struct SinkExpectOverride {
    /// Whether delivery to this specific sink is mandatory.
    #[serde(default)]
    pub required: bool,
}
