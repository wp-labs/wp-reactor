use wp_connector_api::SinkSpec as ResolvedSinkSpec;

use super::expect::GroupExpectSpec;
use super::types::WildArray;

// ---------------------------------------------------------------------------
// FlexGroup — resolved business routing group
// ---------------------------------------------------------------------------

/// A business routing group with compiled window patterns and resolved sink specs.
///
/// Created from a `RouteGroup` after connector resolution and parameter merging.
#[derive(Debug)]
pub struct FlexGroup {
    /// Group name.
    pub name: String,
    /// Max parallel writers (1..=10).
    pub parallel: usize,
    /// Compiled wildcard patterns for yield-target window matching.
    pub windows: WildArray,
    /// Merged tags (defaults + group + sink level).
    pub tags: Vec<String>,
    /// Group-level expect specification.
    pub expect: Option<GroupExpectSpec>,
    /// Resolved sink specifications (ready for factory building).
    pub sinks: Vec<ResolvedSinkSpec>,
}

// ---------------------------------------------------------------------------
// FixedGroup — resolved infra group (default / error)
// ---------------------------------------------------------------------------

/// An infrastructure group (default or error fallback) without window patterns.
#[derive(Debug)]
pub struct FixedGroup {
    /// Group name (e.g. `"__default"`, `"__error"`).
    pub name: String,
    /// Group-level expect specification.
    pub expect: Option<GroupExpectSpec>,
    /// Resolved sink specifications.
    pub sinks: Vec<ResolvedSinkSpec>,
    /// Max parallel writers.
    pub parallel: usize,
}
