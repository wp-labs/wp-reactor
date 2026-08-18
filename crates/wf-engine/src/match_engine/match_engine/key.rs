use smol_str::SmolStr;
use wf_lang::ast::{FieldRef, PathSegment};

use super::types::{EngineHashMap, Event, Value};

// ---------------------------------------------------------------------------
// Value key — typed, hashable key for distinct-like state
// ---------------------------------------------------------------------------

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq, Hash)]
#[moju(kind = "state", domain = "Engine", module = "Engine.MatchEngine")]
pub(super) enum ValueKey {
    Number(u64),
    Str(String),
    Bool(bool),
    Array(Vec<ValueKey>),
    Object(Vec<(String, ValueKey)>),
}

impl ValueKey {
    pub(super) fn from_value(value: &Value) -> Self {
        match value {
            Value::Number(n) => Self::Number(canonical_f64_bits(*n)),
            Value::Str(s) => Self::Str(s.to_string()),
            Value::Bool(b) => Self::Bool(*b),
            Value::Array(values) => Self::Array(values.iter().map(Self::from_value).collect()),
            Value::Object(map) => {
                let mut values: Vec<_> = map
                    .iter()
                    .map(|(key, value)| (key.to_string(), Self::from_value(value)))
                    .collect();
                values.sort_by(|a, b| a.0.cmp(&b.0));
                Self::Object(values)
            }
        }
    }

    pub(super) fn estimated_bytes(&self) -> usize {
        match self {
            Self::Number(_) | Self::Bool(_) => 8,
            Self::Str(s) => s.len() + 24,
            Self::Array(values) => 24 + values.iter().map(Self::estimated_bytes).sum::<usize>(),
            Self::Object(values) => {
                24 + values
                    .iter()
                    .map(|(key, value)| key.len() + value.estimated_bytes())
                    .sum::<usize>()
            }
        }
    }
}

fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0f64.to_bits()
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

// ---------------------------------------------------------------------------
// Scope key — structured match key (SHARD routing)
// ---------------------------------------------------------------------------
//
// The match key (e.g. Q2 `match<auction:10m>` → `auction`) is a small set of
// scalar fields (number / timestamp / string, or their pairings). Instead of
// serializing the key to a string just to hash it for sharding (the
// `make_scope_key_str` → FNV path, the dominant per-event cost on Q2/Q5/Q7
// sharded match), we build a typed [`ScopeKey`] directly from the source and
// hash that. Byte-consistency with sharding is preserved by driving **both**
// the columnar path and the row-based path through the same canonicalization
// ([`ScopeKey::from_value`] / `scope_key_from_column`).

/// A typed match-key. `Pair` supports two key fields (the common case); deeper
/// nesting builds up via `Pair`. Integer-valued numbers collapse to `Int`
/// (including `Timestamp(Ns)`, read as `i64`), so a columnar `Int64` column and
/// the row-based `Value::Number(f64)` (integer, `<2^53`) produce the **same**
/// variant and hash equal.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub(crate) enum ScopeKey {
    #[default]
    Empty,
    Int(i64),
    Float(u64), // canonical f64 bits
    Str(SmolStr),
    Pair(Box<ScopeKey>, Box<ScopeKey>),
}

/// <2^53 where every integer is exactly representable as f64 (matches the
/// existing native-int columnar dispatch / `number_literal`).
const TWO_POW_53: f64 = 9_007_199_254_740_992.0;

/// Canonical f64 bits (0.0 → +0.0, NaN → canonical NaN), matching
/// [`canonical_f64_bits`](super::super::match_engine::ValueKey) semantics.
fn canonical_bits(n: f64) -> u64 {
    if n == 0.0 {
        0.0f64.to_bits()
    } else if n.is_nan() {
        f64::NAN.to_bits()
    } else {
        n.to_bits()
    }
}

impl ScopeKey {
    /// Build a [`ScopeKey`] from a [`Value`] (row-based path). Integer-valued
    /// numbers (and full-precision integers) → `Int`; fractional / huge floats
    /// → `Float`; strings → `Str`. Structured values fall back to their string
    /// form so they still shard deterministically.
    pub(crate) fn from_value(value: &Value) -> Self {
        match value {
            Value::Number(n) => {
                if n.fract() == 0.0 && n.abs() < TWO_POW_53 {
                    ScopeKey::Int(*n as i64)
                } else {
                    ScopeKey::Float(canonical_bits(*n))
                }
            }
            Value::Str(s) => ScopeKey::Str(s.clone()),
            Value::Bool(b) => ScopeKey::Str(if *b { "true" } else { "false" }.into()),
            // Structured values: fixed deterministic token (rare as a match key);
            // no `String` allocation on the typed-key path.
            Value::Array(_) => ScopeKey::Str(SmolStr::new_static("[array]")),
            Value::Object(_) => ScopeKey::Str(SmolStr::new_static("[object]")),
        }
    }
}

/// Build a [`ScopeKey`] for a sequence of extracted [`Value`]s (row-based
/// `extract_key_simple` output), in plan field order. Mirrors
/// [`crate::window::fanout::scope_key_columnar`]'s pairing order so both
/// columnar and row-based paths produce the same key.
pub(crate) fn scope_key_from_values(scope_key: &[Value]) -> ScopeKey {
    let mut acc: Option<ScopeKey> = None;
    for v in scope_key {
        let k = ScopeKey::from_value(v);
        acc = Some(match acc {
            None => k,
            Some(prev) => ScopeKey::Pair(Box::new(prev), Box::new(k)),
        });
    }
    acc.unwrap_or(ScopeKey::Empty)
}

/// FNV-1a shard index over a [`ScopeKey`]'s normative bytes. Kept deterministic
/// and independent of `HashMap`'s random seed, like the old string-hash
/// [`shard_index`] it replaces — but it hashes the **typed** key (tag + raw
/// payload) instead of a re-serialized string, so building the key is cheap.
pub(crate) fn scope_key_shard_index(key: &ScopeKey, shard_count: usize) -> usize {
    if shard_count <= 1 {
        return 0;
    }
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let tag = match key {
        ScopeKey::Empty => 0u8,
        ScopeKey::Int(_) => 1,
        ScopeKey::Float(_) => 2,
        ScopeKey::Str(_) => 3,
        ScopeKey::Pair(_, _) => 4,
    };
    mix_byte(&mut hash, tag);
    nested_bytes(&mut hash, key);
    (hash as usize) % shard_count
}

fn mix_byte(hash: &mut u64, b: u8) {
    *hash ^= u64::from(b);
    *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        mix_byte(hash, b);
    }
}

fn nested_bytes(hash: &mut u64, key: &ScopeKey) {
    // Tag + payload for a nested key, so left/right order matters.
    match key {
        ScopeKey::Empty => {}
        ScopeKey::Int(v) => hash_bytes(hash, &v.to_ne_bytes()),
        ScopeKey::Float(bits) => hash_bytes(hash, &bits.to_ne_bytes()),
        ScopeKey::Str(s) => hash_bytes(hash, s.as_bytes()),
        ScopeKey::Pair(a, b) => {
            nested_bytes(hash, a);
            hash_bytes(hash, &[0x1f]);
            nested_bytes(hash, b);
        }
    }
}

/// Structured instance key for the `CepStateMachine` instances map.
///
/// For sliding windows: `scope_key` identifies the instance, `bucket_start`
/// is `None`. For fixed windows: each `(scope_key, bucket_start)` pair is
/// a separate instance. The scope is a typed [`ScopeKey`] (not a re-serialized
/// string), so building the key for the per-event lookup is cheap — no
/// number-to-string formatting on the hot path.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub(super) struct InstanceKey {
    pub scope_key: ScopeKey,
    pub bucket_start: Option<i64>,
}

impl InstanceKey {
    pub fn sliding(scope_key: &ScopeKey) -> Self {
        Self {
            scope_key: scope_key.clone(),
            bucket_start: None,
        }
    }

    pub fn fixed(scope_key: &ScopeKey, bucket_start: i64) -> Self {
        Self {
            scope_key: scope_key.clone(),
            bucket_start: Some(bucket_start),
        }
    }

    /// Check if this key belongs to the given scope (ignoring bucket).
    pub fn matches_scope(&self, scope_key: &ScopeKey) -> bool {
        &self.scope_key == scope_key
    }

    /// Rebuild the scope-key `Value`s from the typed key, for close/match
    /// output. Numeric components come back as `Str` (their Display form),
    /// preserving the pre-refactor type-erased behaviour of the old string key.
    pub fn scope_key_values(&self) -> Vec<Value> {
        flatten_scope_values(&self.scope_key)
    }
}

/// Flatten a possibly-`Pair`ed [`ScopeKey`] into its leaf [`Value`]s (all `Str`),
/// matching the old `\x1f`-split string reconstruction.
fn flatten_scope_values(key: &ScopeKey) -> Vec<Value> {
    match key {
        ScopeKey::Empty => vec![],
        ScopeKey::Int(v) => vec![Value::Str(v.to_string().into())],
        ScopeKey::Float(bits) => vec![Value::Str(f64::from_bits(*bits).to_string().into())],
        ScopeKey::Str(s) => vec![Value::Str(s.clone())],
        ScopeKey::Pair(a, b) => {
            let mut out = flatten_scope_values(a);
            out.extend(flatten_scope_values(b));
            out
        }
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
            .and_then(|e| event.fields.get(e.source_field.as_str()));

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

pub(crate) fn extract_key_simple(event: &Event, keys: &[FieldRef]) -> Option<Vec<Value>> {
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
        // Flat-lookup fallback for non-expression consumers: use the root field.
        FieldRef::Path { segments, .. } => match segments.first() {
            Some(PathSegment::Field(root)) => root,
            _ => "",
        },
        _ => "",
    }
}

/// The leaf field name of a reference — for a `Path` this is the last member
/// segment (used by `window.has` to infer the column to look up, matching how
/// `e.sip` infers `sip`).
pub(crate) fn field_ref_leaf_name(fr: &FieldRef) -> Option<&str> {
    match fr {
        FieldRef::Simple(name) => Some(name),
        FieldRef::Qualified(_, field) | FieldRef::Bracketed(_, field) => Some(field),
        FieldRef::Path { segments, .. } => segments.iter().rev().find_map(|seg| match seg {
            PathSegment::Field(name) => Some(name.as_str()),
            PathSegment::Index(_) => None,
            _ => None,
        }),
        _ => None,
    }
}

/// Resolve a field reference against a flat field map. `FieldRef::Path` walks
/// nested `object` / `array` values; any missing member, out-of-bounds index, or
/// type mismatch yields `None` (which the yield layer degrades to an omitted
/// field). Other variants use the existing flat lookup.
pub(crate) fn eval_field_value(
    fields: &EngineHashMap<smol_str::SmolStr, Value>,
    fr: &FieldRef,
) -> Option<Value> {
    let FieldRef::Path { segments, .. } = fr else {
        return fields.get(field_ref_name(fr)).cloned();
    };
    let mut iter = segments.iter();
    let Some(PathSegment::Field(root)) = iter.next() else {
        return None;
    };
    let mut value = fields.get(root.as_str())?.clone();
    for segment in iter {
        match segment {
            PathSegment::Field(name) => match value {
                Value::Object(map) => value = map.get(name.as_str())?.clone(),
                _ => return None,
            },
            PathSegment::Index(idx) => match value {
                Value::Array(items) => value = items.get(*idx)?.clone(),
                _ => return None,
            },
            _ => return None,
        }
    }
    Some(value)
}

/// Deterministic shard index for a scope key (superseded by
/// [`scope_key_shard_index`], which hashes the typed [`ScopeKey`] instead of a
/// re-serialized string). Kept inline for reference / legacy tests.
pub(crate) fn value_to_string(v: &Value) -> String {
    match v {
        Value::Number(n) => n.to_string(),
        Value::Str(s) => s.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Array(_) => "[array]".to_string(),
        Value::Object(_) => "[object]".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wf_lang::ast::PathSegment;

    fn fields(pairs: &[(&str, Value)]) -> EngineHashMap<smol_str::SmolStr, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect()
    }

    fn path(alias: &str, segments: &[PathSegment]) -> FieldRef {
        FieldRef::Path {
            alias: alias.to_string(),
            segments: segments.to_vec(),
        }
    }

    #[test]
    fn eval_path_walks_object() {
        let f = fields(&[(
            "roles_obj",
            Value::Object(EngineHashMap::from_iter([(
                "source".into(),
                Value::Object(EngineHashMap::from_iter([(
                    "uid".into(),
                    Value::Str("abc".into()),
                )])),
            )])),
        )]);
        let fr = path(
            "e",
            &[
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("source".into()),
                PathSegment::Field("uid".into()),
            ],
        );
        assert_eq!(eval_field_value(&f, &fr), Some(Value::Str("abc".into())));
    }

    #[test]
    fn eval_path_walks_array_index() {
        let f = fields(&[(
            "arr",
            Value::Array(vec![
                Value::Str("first".into()),
                Value::Str("second".into()),
            ]),
        )]);
        let fr = path(
            "e",
            &[PathSegment::Field("arr".into()), PathSegment::Index(1)],
        );
        assert_eq!(eval_field_value(&f, &fr), Some(Value::Str("second".into())));
    }

    #[test]
    fn eval_path_missing_member_is_none() {
        let f = fields(&[(
            "roles_obj",
            Value::Object(EngineHashMap::from_iter([(
                "source".into(),
                Value::Object(EngineHashMap::default()),
            )])),
        )]);
        let fr = path(
            "e",
            &[
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("missing".into()),
            ],
        );
        assert_eq!(eval_field_value(&f, &fr), None);
    }

    #[test]
    fn eval_path_out_of_bounds_is_none() {
        let f = fields(&[("arr", Value::Array(vec![Value::Str("x".into())]))]);
        let fr = path(
            "e",
            &[PathSegment::Field("arr".into()), PathSegment::Index(5)],
        );
        assert_eq!(eval_field_value(&f, &fr), None);
    }

    #[test]
    fn eval_path_type_mismatch_is_none() {
        let f = fields(&[(
            "roles_obj",
            Value::Object(EngineHashMap::from_iter([(
                "source".into(),
                Value::Str("s".into()),
            )])),
        )]);
        // `source` is a string, not an object → next member fails.
        let fr = path(
            "e",
            &[
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("source".into()),
                PathSegment::Field("x".into()),
            ],
        );
        assert_eq!(eval_field_value(&f, &fr), None);
    }

    #[test]
    fn eval_flat_ref_still_works() {
        let f = fields(&[("sip", Value::Str("10.0.0.1".into()))]);
        assert_eq!(
            eval_field_value(&f, &FieldRef::Qualified("e".into(), "sip".into())),
            Some(Value::Str("10.0.0.1".into()))
        );
    }

    #[test]
    fn eval_path_root_field_missing_is_none() {
        // The root object field itself is absent from the flat map.
        let f = fields(&[("sip", Value::Str("10.0.0.1".into()))]);
        let fr = path(
            "e",
            &[
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("source".into()),
            ],
        );
        assert_eq!(eval_field_value(&f, &fr), None);
    }

    #[test]
    fn eval_path_index_on_object_is_none() {
        // An index segment applied to a non-array value is a type mismatch.
        let f = fields(&[(
            "roles_obj",
            Value::Object(EngineHashMap::from_iter([(
                "x".into(),
                Value::Str("s".into()),
            )])),
        )]);
        let fr = path(
            "e",
            &[
                PathSegment::Field("roles_obj".into()),
                PathSegment::Index(0),
            ],
        );
        assert_eq!(eval_field_value(&f, &fr), None);
    }

    #[test]
    fn eval_path_member_on_array_is_none() {
        // A member segment applied to a non-object value is a type mismatch.
        let f = fields(&[("arr", Value::Array(vec![Value::Str("x".into())]))]);
        let fr = path(
            "e",
            &[
                PathSegment::Field("arr".into()),
                PathSegment::Field("name".into()),
            ],
        );
        assert_eq!(eval_field_value(&f, &fr), None);
    }

    #[test]
    fn eval_path_empty_segments_is_none() {
        // Defensive: a Path with no segments resolves to nothing.
        let f = fields(&[("x", Value::Str("s".into()))]);
        let fr = FieldRef::Path {
            alias: "e".to_string(),
            segments: vec![],
        };
        assert_eq!(eval_field_value(&f, &fr), None);
    }

    #[test]
    fn eval_path_deep_mixed_walk() {
        // object → array → index → object → member.
        let f = fields(&[(
            "roles_obj",
            Value::Object(EngineHashMap::from_iter([(
                "related".into(),
                Value::Array(vec![Value::Object(EngineHashMap::from_iter([(
                    "process".into(),
                    Value::Object(EngineHashMap::from_iter([(
                        "name".into(),
                        Value::Str("evil.exe".into()),
                    )])),
                )]))]),
            )])),
        )]);
        let fr = path(
            "e",
            &[
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("related".into()),
                PathSegment::Index(0),
                PathSegment::Field("process".into()),
                PathSegment::Field("name".into()),
            ],
        );
        assert_eq!(
            eval_field_value(&f, &fr),
            Some(Value::Str("evil.exe".into()))
        );
    }

    #[test]
    fn leaf_name_matches_flat_refs_and_paths() {
        // Leaf inference for window.has: flat refs use their field, nested paths
        // use their last member segment.
        assert_eq!(
            field_ref_leaf_name(&FieldRef::Simple("sip".into())),
            Some("sip")
        );
        assert_eq!(
            field_ref_leaf_name(&FieldRef::Qualified("e".into(), "sip".into())),
            Some("sip")
        );
        assert_eq!(
            field_ref_leaf_name(&FieldRef::Bracketed("e".into(), "detail.sha256".into())),
            Some("detail.sha256")
        );
        assert_eq!(
            field_ref_leaf_name(&path(
                "e",
                &[
                    PathSegment::Field("roles_obj".into()),
                    PathSegment::Field("source".into()),
                    PathSegment::Field("process".into()),
                    PathSegment::Field("uid".into()),
                ]
            )),
            Some("uid")
        );
        // A path that ends in an index has no leaf member → falls back to the
        // last member before it ("related").
        assert_eq!(
            field_ref_leaf_name(&path(
                "e",
                &[
                    PathSegment::Field("roles_obj".into()),
                    PathSegment::Field("related".into()),
                    PathSegment::Index(0),
                ]
            )),
            Some("related")
        );
    }
}
