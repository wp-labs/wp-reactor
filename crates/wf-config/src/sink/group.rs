use wp_connector_api::SinkSpec as ResolvedSinkSpec;

use super::expect::GroupExpectSpec;
use super::types::WildArray;

#[derive(::moju_derive::MoJu, Debug)]
#[moju(kind = "struct", domain = "Config", module = "Config.SinkConfig")]
pub struct ResolvedRouteSink {
    pub spec: ResolvedSinkSpec,
    pub fields: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// FlexGroup — resolved business routing group
// ---------------------------------------------------------------------------

/// A business routing group with compiled window patterns and resolved sink specs.
///
/// Created from a `RouteGroup` after connector resolution and parameter merging.
#[derive(::moju_derive::MoJu, Debug)]
#[moju(kind = "struct", domain = "Config", module = "Config.SinkConfig")]
pub struct FlexGroup {
    /// Group name.
    pub name: String,
    /// Max parallel writers (1..=10).
    pub parallel: usize,
    /// Compiled wildcard patterns for yield-target window matching.
    pub windows: WildArray,
    /// Merged tags (defaults + group + sink level).
    pub tags: Vec<String>,
    /// WarpFusion-managed `__wfu_*` metadata fields to disable in this group's output.
    pub wf_meta_disable: Vec<String>,
    /// Group-level expect specification.
    pub expect: Option<GroupExpectSpec>,
    /// Resolved sink specifications (ready for factory building).
    pub sinks: Vec<ResolvedRouteSink>,
}

// ---------------------------------------------------------------------------
// FixedGroup — resolved infra group (default / error)
// ---------------------------------------------------------------------------

/// An infrastructure group (default or error fallback) without window patterns.
#[derive(::moju_derive::MoJu, Debug)]
#[moju(kind = "struct", domain = "Config", module = "Config.SinkConfig")]
pub struct FixedGroup {
    /// Group name (e.g. `"__default"`, `"__error"`).
    pub name: String,
    /// Group-level expect specification.
    pub expect: Option<GroupExpectSpec>,
    /// WarpFusion-managed `__wfu_*` metadata fields to disable in this group's output.
    pub wf_meta_disable: Vec<String>,
    /// Resolved sink specifications.
    pub sinks: Vec<ResolvedRouteSink>,
    /// Max parallel writers.
    pub parallel: usize,
}
