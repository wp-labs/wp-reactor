use std::collections::HashMap;

use crate::ast::{FieldRef, Measure, PathSegment};
use crate::schema::{BaseType, FieldType, WindowSchema};

use super::types::ValType;

/// Scope built from a rule's events block, `let` bindings and join clauses.
pub struct Scope<'a> {
    /// Event alias → WindowSchema mapping.
    pub aliases: HashMap<&'a str, &'a WindowSchema>,
    /// Per-event `let` bindings → value type (referenced by bare name).
    pub let_types: HashMap<String, ValType>,
    /// Join target window names (registered in aliases but not event sources).
    pub join_windows: Vec<&'a str>,
    /// Match/close step label metadata for stat selector validation.
    pub stat_labels: HashMap<String, StatLabelInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatLabelInfo {
    pub stage: StatLabelStage,
    pub uses_distinct: bool,
    pub measure: Measure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatLabelStage {
    Event,
    Close,
}

impl<'a> Scope<'a> {
    pub fn new() -> Self {
        Scope {
            aliases: HashMap::new(),
            let_types: HashMap::new(),
            join_windows: Vec::new(),
            stat_labels: HashMap::new(),
        }
    }

    /// Resolve a FieldRef to a ValType using this scope.
    /// Returns Ok(Some(t)) for scalar fields, Ok(None) for set-level alias references,
    /// and Err(message) for invalid references.
    pub fn resolve_field_ref(&self, fref: &FieldRef) -> Result<Option<ValType>, String> {
        match fref {
            FieldRef::Simple(name) => self.resolve_simple(name),
            FieldRef::Qualified(alias, field) => self.resolve_qualified(alias, field).map(Some),
            FieldRef::Bracketed(alias, key) => self.resolve_qualified(alias, key).map(Some),
            FieldRef::Path { alias, segments } => {
                // Nested paths validate the root field only; deep segments have
                // no schema (object/array carry no nested type), so the leaf type
                // is determined at runtime.
                let Some(PathSegment::Field(root)) = segments.first() else {
                    return Err("nested field path must start with a member name".to_string());
                };
                self.resolve_qualified(alias, root).map(Some)
            }
        }
    }

    fn resolve_simple(&self, name: &str) -> Result<Option<ValType>, String> {
        // Per-event `let` bindings take precedence over event-source fields:
        // `let parts = split(...)` is a value binding, not a field access.
        if let Some(t) = self.let_types.get(name) {
            return Ok(Some(t.clone()));
        }
        // First check if it's an alias (set-level reference, e.g. count(fail))
        if self.aliases.contains_key(name) {
            return Ok(None); // Valid reference but no scalar type
        }
        // Search all aliases for this field name. Must find at least one match.
        let mut found: Option<ValType> = None;
        for schema in self.aliases.values() {
            if let Some(fd) = schema.fields.iter().find(|f| f.name == name) {
                let vt = field_type_to_val(&fd.field_type);
                if let Some(ref prev) = found
                    && *prev != vt
                {
                    return Err(format!(
                        "field `{}` has conflicting types across event sources",
                        name
                    ));
                }
                found = Some(vt);
            }
        }
        found
            .map(|t| Ok(Some(t)))
            .unwrap_or_else(|| Err(format!("field `{}` not found in any event source", name)))
    }

    fn resolve_qualified(&self, alias: &str, field: &str) -> Result<ValType, String> {
        if let Some(schema) = self.aliases.get(alias) {
            return match schema.fields.iter().find(|f| f.name == field) {
                Some(fd) => Ok(field_type_to_val(&fd.field_type)),
                None => Err(format!(
                    "field `{}` not found in window `{}`",
                    field, schema.name
                )),
            };
        }
        Err(format!("`{}` is not a declared alias or step label", alias))
    }

    /// Check whether a field exists in a specific alias's window.
    pub fn alias_has_field(&self, alias: &str, field: &str) -> bool {
        self.aliases
            .get(alias)
            .is_some_and(|s| s.fields.iter().any(|f| f.name == field))
    }

    /// Get the field type for a field that exists in a specific alias.
    pub fn get_field_type_for_alias(&self, alias: &str, field: &str) -> Option<ValType> {
        self.aliases.get(alias).and_then(|s| {
            s.fields
                .iter()
                .find(|f| f.name == field)
                .map(|f| field_type_to_val(&f.field_type))
        })
    }
}

/// Convert a schema FieldType to our ValType.
pub fn field_type_to_val(ft: &FieldType) -> ValType {
    match ft {
        FieldType::Base(bt) => match bt {
            BaseType::Bool => ValType::Bool,
            other => ValType::Base(other.clone()),
        },
        FieldType::ArrayAny => ValType::ArrayAny,
        FieldType::Array(bt) => ValType::Array(bt.clone()),
        FieldType::Object => ValType::Object,
    }
}
