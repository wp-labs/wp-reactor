use std::collections::{HashMap, HashSet};

use wf_core::rule::{Value, WindowLookup, batch_to_events};
use wf_core::window::Router;

// ---------------------------------------------------------------------------
// RegistryLookup -- WindowLookup adapter backed by the shared Router
// ---------------------------------------------------------------------------

/// Implements [`WindowLookup`] by snapshotting windows from the shared
/// [`Router`]'s registry. Used for `window.has()` guards and join evaluation.
pub(super) struct RegistryLookup<'a>(pub(super) &'a Router);

impl WindowLookup for RegistryLookup<'_> {
    fn snapshot_field_values(&self, window: &str, field: &str) -> Option<HashSet<String>> {
        let batches = self.0.registry().snapshot(window)?;
        let mut values = HashSet::new();
        for batch in &batches {
            for event in batch_to_events(batch) {
                if let Some(val) = event.fields.get(field) {
                    // Convert all value types to string for set membership
                    match val {
                        Value::Str(s) => {
                            values.insert(s.clone());
                        }
                        Value::Number(n) => {
                            values.insert(n.to_string());
                        }
                        Value::Bool(b) => {
                            values.insert(b.to_string());
                        }
                    }
                }
            }
        }
        Some(values)
    }

    fn snapshot(&self, window: &str) -> Option<Vec<HashMap<String, Value>>> {
        let batches = self.0.registry().snapshot(window)?;
        let mut rows = Vec::new();
        for batch in &batches {
            for event in batch_to_events(batch) {
                rows.push(event.fields);
            }
        }
        Some(rows)
    }
}
