use std::collections::HashMap;

use crate::ast::{FieldRef, Measure, PathSegment};
use crate::schema::{BaseType, FieldType, WindowSchema};

use super::types::ValType;

/// Scope built from a rule's events block, `let` bindings and join clauses.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangChecker")]
pub(crate) struct Scope<'a> {
    /// Event alias → WindowSchema mapping.
    pub aliases: HashMap<&'a str, &'a WindowSchema>,
    /// Per-event `let` bindings → value type (referenced by bare name).
    pub let_types: HashMap<String, ValType>,
    /// Join target window names (registered in aliases but not event sources).
    pub join_windows: Vec<&'a str>,
    /// Match/close step label metadata for stat selector validation.
    pub stat_labels: HashMap<String, StatLabelInfo>,
    /// `reduce ... as label` 归约标签：`label.field` 解析为 object 访问
    ///（review R2——归约整行以裸键 object value 注入 eval context）。
    pub reduce_labels: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangChecker")]
pub(crate) struct StatLabelInfo {
    pub stage: StatLabelStage,
    pub uses_distinct: bool,
    pub measure: Measure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangChecker")]
pub(crate) enum StatLabelStage {
    Event,
    Close,
}

impl<'a> Scope<'a> {
pub(crate) fn new() -> Self {
        Scope {
            aliases: HashMap::new(),
            let_types: HashMap::new(),
            join_windows: Vec::new(),
            stat_labels: HashMap::new(),
            reduce_labels: Vec::new(),
        }
    }

    /// Resolve a FieldRef to a ValType using this scope.
    /// Returns Ok(Some(t)) for scalar fields, Ok(None) for set-level alias references,
    /// and Err(message) for invalid references.
pub(crate) fn resolve_field_ref(&self, fref: &FieldRef) -> Result<Option<ValType>, String> {
        match fref {
            FieldRef::Simple(name) => self.resolve_simple(name),
            FieldRef::Qualified(alias, field) => {
                // `reduce ... as label` 的 `label.field`：object 访问，叶子类型运行时确定
                //（与嵌套 Path 一致 → Ok(None)，解析通过但无静态标量类型）
                if self.reduce_labels.iter().any(|l| l == alias) {
                    return Ok(None);
                }
                self.resolve_qualified(alias, field).map(Some)
            }
            FieldRef::Bracketed(alias, key) => self.resolve_qualified(alias, key).map(Some),
            FieldRef::Path { alias, segments } => {
                // Nested paths validate the root field only; deep segments have
                // no schema (object/array carry no nested type), so the leaf type
                // is determined at runtime.
                let Some(PathSegment::Field(root)) = segments.first() else {
                    return Err("nested field path must start with a member name".to_string());
                };
                // reduce 标签的路径访问（`winner.bidder`）同样只验根、叶子类型运行时确定
                if self.reduce_labels.iter().any(|l| l == alias) {
                    return Ok(None);
                }
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
        // reduce 标签裸引用（`winner` 自身）——object 访问，无静态标量类型
        if self.reduce_labels.iter().any(|l| l == name) {
            return Ok(None);
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
pub(crate) fn alias_has_field(&self, alias: &str, field: &str) -> bool {
        self.aliases
            .get(alias)
            .is_some_and(|s| s.fields.iter().any(|f| f.name == field))
    }

    /// Get the field type for a field that exists in a specific alias.
pub(crate) fn get_field_type_for_alias(&self, alias: &str, field: &str) -> Option<ValType> {
        self.aliases.get(alias).and_then(|s| {
            s.fields
                .iter()
                .find(|f| f.name == field)
                .map(|f| field_type_to_val(&f.field_type))
        })
    }
}

/// Convert a schema FieldType to our ValType.
pub(crate) fn field_type_to_val(ft: &FieldType) -> ValType {
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
