//! Pipe abstraction: the output / intermediate target of a rule's `yield`.
//!
//! A Pipe is what a rule yields to — either a sink-facing output target (e.g.
//! `network_alerts`) or an intermediate `|>` relay between pipeline stages.
//! Unlike an input [`crate::window::Window`], a Pipe carries **no match state**:
//! it is a named relay with a schema (for output cropping) and a retention
//! `over`. Subscribers (sinks, downstream rules, and — later — transform
//! operators) are fanned out by the pipe name at emit time.
//!
//! This is the engine-side metadata half of the Pipe design; the runtime fan-out
//! (sink channels + downstream rule channels) is coordinated by the pipe name in
//! the rule task's emit path (see `wf-runtime`).
//!
//! Reference: `docs/design/rule-sharding-and-aggregation-window.md`.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use arrow::datatypes::SchemaRef;

/// An output / intermediate relay target of a rule's `yield`.
///
/// - `over == Duration::ZERO`: pure pass-through (relay to subscribers, no
///   retention).
/// - `over > 0`: retains the recent window (used when a downstream rule matches
///   against this pipe's data with a time window).
///
/// The schema is used to crop / type the output row (the role the output
/// "window" previously served); `time_col_index` is the position of the time
/// field in `schema`, so the relay can place the event time on the right column.
#[derive(Debug, Clone)]
pub struct Pipe {
    pub name: String,
    pub schema: SchemaRef,
    pub over: Duration,
    /// Position of the time field in `schema` (from the target window's
    /// `time_field`), so the pipeline relay can place event time on the right
    /// column even for user-named intermediates (not just `__wf_pipe_*`).
    pub time_col_index: Option<usize>,
}

/// Registry of pipes by name. Built at bootstrap from the compiled yield
/// topology (output targets + `|>` intermediate stages).
#[derive(Default)]
pub struct PipeRegistry {
    pipes: RwLock<HashMap<String, Pipe>>,
}

impl PipeRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a pipe. Duplicate names overwrite (last wins).
    pub fn register(&self, pipe: Pipe) {
        let mut pipes = self.pipes.write().expect("pipe registry lock poisoned");
        pipes.insert(pipe.name.clone(), pipe);
    }

    /// Look up a pipe by name.
    pub fn get(&self, name: &str) -> Option<Pipe> {
        let pipes = self.pipes.read().expect("pipe registry lock poisoned");
        pipes.get(name).cloned()
    }

    /// Whether `name` is a registered pipe.
    pub fn contains(&self, name: &str) -> bool {
        let pipes = self.pipes.read().expect("pipe registry lock poisoned");
        pipes.contains_key(name)
    }

    /// Iterate over all pipes.
    pub fn iter(&self) -> Vec<Pipe> {
        let pipes = self.pipes.read().expect("pipe registry lock poisoned");
        pipes.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("sip", DataType::Utf8, true)]))
    }

    #[test]
    fn register_and_lookup_pipe() {
        let reg = PipeRegistry::new();
        assert!(!reg.contains("alerts"));
        reg.register(Pipe {
            name: "alerts".into(),
            schema: schema(),
            over: Duration::ZERO,
            time_col_index: None,
        });
        assert!(reg.contains("alerts"));
        let pipe = reg.get("alerts").expect("pipe present");
        assert_eq!(pipe.name, "alerts");
        assert_eq!(pipe.over, Duration::ZERO);
    }

    #[test]
    fn iter_yields_all_pipes() {
        let reg = PipeRegistry::new();
        reg.register(Pipe {
            name: "a".into(),
            schema: schema(),
            over: Duration::ZERO,
            time_col_index: None,
        });
        reg.register(Pipe {
            name: "b".into(),
            schema: schema(),
            over: Duration::from_secs(60),
            time_col_index: None,
        });
        let mut names: Vec<_> = reg.iter().into_iter().map(|p| p.name).collect();
        names.sort();
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
    }
}
