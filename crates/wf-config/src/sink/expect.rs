use serde::Deserialize;

/// Group-level expect specification.
///
/// Controls expected delivery behavior for a sink group (e.g. retry, timeout).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GroupExpectSpec {
    /// Whether delivery to this group is mandatory.
    #[serde(default)]
    pub required: bool,
}

/// Per-sink expect overrides within a group.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SinkExpectOverride {
    /// Whether delivery to this specific sink is mandatory.
    #[serde(default)]
    pub required: bool,
}
