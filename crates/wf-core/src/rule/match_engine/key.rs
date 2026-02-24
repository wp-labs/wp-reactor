use wf_lang::ast::FieldRef;

use super::types::{Event, Value};

// ---------------------------------------------------------------------------
// Instance key — structured, unambiguous map key
// ---------------------------------------------------------------------------

/// Structured instance key for the `CepStateMachine` instances map.
///
/// For sliding windows: `scope_key` identifies the instance, `bucket_start`
/// is `None`. For fixed windows: each `(scope_key, bucket_start)` pair is
/// a separate instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct InstanceKey {
    pub scope_key_str: String,
    pub bucket_start: Option<i64>,
}

impl InstanceKey {
    pub fn sliding(scope_key: &[Value]) -> Self {
        Self {
            scope_key_str: make_scope_key_str(scope_key),
            bucket_start: None,
        }
    }

    pub fn fixed(scope_key: &[Value], bucket_start: i64) -> Self {
        Self {
            scope_key_str: make_scope_key_str(scope_key),
            bucket_start: Some(bucket_start),
        }
    }

    /// Check if this key belongs to the given scope (ignoring bucket).
    pub fn matches_scope(&self, scope_key_str: &str) -> bool {
        self.scope_key_str == scope_key_str
    }
}

// ---------------------------------------------------------------------------
// Key extraction
// ---------------------------------------------------------------------------

/// Extract the scope key values from an event using the plan's key fields.
///
/// When `key_map` is provided, uses alias-specific field mappings to extract
/// the key from different source fields depending on the event's alias.
///
/// Returns `None` if any key field is missing from the event.
/// Returns `Some(vec![])` if the key list is empty (shared instance).
pub(super) fn extract_key(
    event: &Event,
    keys: &[FieldRef],
    key_map: Option<&[wf_lang::plan::KeyMapPlan]>,
    alias: &str,
) -> Option<Vec<Value>> {
    let km = match key_map {
        Some(km) => km,
        None => return extract_key_simple(event, keys),
    };

    // Collect unique logical key names (preserving order)
    let mut logical_names = Vec::new();
    for entry in km {
        if !logical_names.contains(&entry.logical_name) {
            logical_names.push(entry.logical_name.clone());
        }
    }

    if logical_names.is_empty() && keys.is_empty() {
        return Some(vec![]);
    }

    // For each logical key, try to extract a value:
    //   1. From this alias's mapped source field
    //   2. Fallback: from the event using the logical name directly
    let mut result = Vec::with_capacity(logical_names.len());
    for logical in &logical_names {
        // Try alias-specific mapping first
        let mapped = km
            .iter()
            .find(|e| e.logical_name == *logical && e.source_alias == alias)
            .and_then(|e| event.fields.get(&e.source_field));

        if let Some(val) = mapped {
            result.push(val.clone());
            continue;
        }

        // Fallback: field named after the logical key
        if let Some(val) = event.fields.get(logical.as_str()) {
            result.push(val.clone());
            continue;
        }
    }

    if result.is_empty() && !keys.is_empty() {
        return extract_key_simple(event, keys);
    }

    // Reject partial keys: all logical keys must be present
    if result.len() != logical_names.len() {
        return None;
    }

    Some(result)
}

fn extract_key_simple(event: &Event, keys: &[FieldRef]) -> Option<Vec<Value>> {
    let mut result = Vec::with_capacity(keys.len());
    for key in keys {
        let field_name = field_ref_name(key);
        let val = event.fields.get(field_name)?;
        result.push(val.clone());
    }
    Some(result)
}

pub(crate) fn field_ref_name(fr: &FieldRef) -> &str {
    match fr {
        FieldRef::Simple(name) => name,
        FieldRef::Qualified(_, name) | FieldRef::Bracketed(_, name) => name,
        _ => "",
    }
}

pub(super) fn make_scope_key_str(scope_key: &[Value]) -> String {
    scope_key
        .iter()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join("\x1f")
}

pub(crate) fn value_to_string(v: &Value) -> String {
    match v {
        Value::Number(n) => n.to_string(),
        Value::Str(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
    }
}
