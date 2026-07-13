use std::collections::BTreeMap;

use wp_connector_api::{ConnectorDef, SinkSpec as ResolvedSinkSpec};

use super::defaults::DefaultsBody;
use super::group::{FixedGroup, FlexGroup, ResolvedRouteSink};
use super::route::{RouteGroup, RouteSink};
use super::types::{ParamMap, WildArray};
use crate::error::{ConfigReason, ConfigResult};

const WFU_META_PREFIX: &str = "__wfu_";

// ---------------------------------------------------------------------------
// Parameter merging
// ---------------------------------------------------------------------------

/// Merge override parameters into base parameters, respecting the allow-list.
///
/// - Parameters in `base` are always included.
/// - Parameters from `overrides` are included only if their key appears in
///   `allow_override`.
/// - An override key not in the allow-list causes an error.
pub fn merge_params_with_allowlist(
    base: &ParamMap,
    overrides: &ParamMap,
    allow_override: &[String],
) -> ConfigResult<ParamMap> {
    let mut merged = base.clone();
    for (key, value) in overrides {
        if !allow_override.contains(key) {
            return ConfigReason::Sink.fail(format!(
                "parameter {:?} is not in allow_override list {:?}",
                key, allow_override,
            ));
        }
        merged.insert(key.clone(), value.clone());
    }
    Ok(merged)
}

// ---------------------------------------------------------------------------
// Tag merging
// ---------------------------------------------------------------------------

/// Merge tags with three-level priority: defaults < group < sink.
///
/// Tags are deduplicated by key (the part before the first `:`). Later levels
/// override earlier ones.
fn merge_tags(
    default_tags: &[String],
    group_tags: Option<&[String]>,
    sink_tags: Option<&[String]>,
) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();

    // Add defaults
    result.extend_from_slice(default_tags);

    // Override with group tags
    if let Some(tags) = group_tags {
        for tag in tags {
            let key = tag_key(tag);
            result.retain(|t| tag_key(t) != key);
            result.push(tag.clone());
        }
    }

    // Override with sink tags
    if let Some(tags) = sink_tags {
        for tag in tags {
            let key = tag_key(tag);
            result.retain(|t| tag_key(t) != key);
            result.push(tag.clone());
        }
    }

    result
}

/// Extract the key portion of a tag (before the first `:`).
fn tag_key(tag: &str) -> &str {
    tag.split(':').next().unwrap_or(tag)
}

// ---------------------------------------------------------------------------
// FlexGroup building
// ---------------------------------------------------------------------------

/// Build a `FlexGroup` from a `RouteGroup`, connector definitions, and defaults.
///
/// Resolves each `RouteSink` into a `ResolvedSinkSpec` by:
/// 1. Looking up the connector by `connect` id
/// 2. Merging parameters with the allow-list
/// 3. Merging tags (defaults → group → sink)
pub fn build_flex_group(
    route_group: &RouteGroup,
    connectors: &BTreeMap<String, ConnectorDef>,
    defaults: &DefaultsBody,
) -> ConfigResult<FlexGroup> {
    validate_wf_meta_disable_fields(&route_group.wf_meta_disable)?;

    let parallel = route_group.parallel.unwrap_or(1).clamp(1, 10);

    let windows = match &route_group.windows {
        Some(s) => WildArray::from(s),
        None => WildArray::new(&[]),
    };

    let group_tags = route_group.tags.as_deref();

    let mut sinks = Vec::with_capacity(route_group.sinks.len());
    for (i, rs) in route_group.sinks.iter().enumerate() {
        let spec = resolve_route_sink(
            &route_group.name,
            i,
            rs,
            connectors,
            &defaults.tags,
            group_tags,
        )?;
        sinks.push(spec);
    }

    let merged_tags = merge_tags(&defaults.tags, group_tags, None);

    Ok(FlexGroup {
        name: route_group.name.clone(),
        parallel,
        windows,
        tags: merged_tags,
        wf_meta_disable: route_group.wf_meta_disable.clone(),
        expect: route_group.expect.clone(),
        sinks,
    })
}

/// Build a `FixedGroup` from a `RouteGroup` (for infra default/error groups).
pub fn build_fixed_group(
    route_group: &RouteGroup,
    connectors: &BTreeMap<String, ConnectorDef>,
    defaults: &DefaultsBody,
) -> ConfigResult<FixedGroup> {
    validate_wf_meta_disable_fields(&route_group.wf_meta_disable)?;

    let parallel = route_group.parallel.unwrap_or(1).clamp(1, 10);
    let group_tags = route_group.tags.as_deref();

    let mut sinks = Vec::with_capacity(route_group.sinks.len());
    for (i, rs) in route_group.sinks.iter().enumerate() {
        let spec = resolve_route_sink(
            &route_group.name,
            i,
            rs,
            connectors,
            &defaults.tags,
            group_tags,
        )?;
        sinks.push(spec);
    }

    Ok(FixedGroup {
        name: route_group.name.clone(),
        expect: route_group.expect.clone(),
        wf_meta_disable: route_group.wf_meta_disable.clone(),
        sinks,
        parallel,
    })
}

fn validate_wf_meta_disable_fields(fields: &[String]) -> ConfigResult<()> {
    for field in fields {
        if !field.starts_with(WFU_META_PREFIX) {
            return ConfigReason::Sink.fail(format!(
                "wf_meta_disable field {:?} must start with {:?}",
                field, WFU_META_PREFIX
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal: resolve a single RouteSink → ResolvedSinkSpec
// ---------------------------------------------------------------------------

fn resolve_route_sink(
    group_name: &str,
    index: usize,
    rs: &RouteSink,
    connectors: &BTreeMap<String, ConnectorDef>,
    default_tags: &[String],
    group_tags: Option<&[String]>,
) -> ConfigResult<ResolvedRouteSink> {
    let Some(connector) = connectors.get(&rs.connect) else {
        return ConfigReason::Sink.fail(format!(
            "group {:?} sink [{}]: connector {:?} not found",
            group_name, index, rs.connect,
        ));
    };

    let params = merge_params_with_allowlist(
        &connector.default_params,
        &rs.params,
        &connector.allow_override,
    )
    .map_err(|e| {
        e.with_detail(format!(
            "group {:?} sink [{}] (connect={:?})",
            group_name, index, rs.connect
        ))
    })?;

    let sink_name = rs.name.clone().unwrap_or_else(|| format!("[{}]", index));

    let _tags = merge_tags(default_tags, group_tags, rs.tags.as_deref());

    Ok(ResolvedRouteSink {
        spec: ResolvedSinkSpec {
            group: group_name.to_string(),
            name: sink_name,
            kind: connector.kind.clone(),
            connector_id: connector.id.clone(),
            params,
            filter: None,
        },
        fields: rs.fields.as_ref().map(|fields| fields.0.clone()),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_connector() -> ConnectorDef {
        ConnectorDef {
            id: "file_json".into(),
            kind: "file".into(),
            scope: wp_connector_api::ConnectorScope::Sink,
            allow_override: vec!["base".into(), "file".into(), "sync".into()],
            default_params: {
                let mut m = ParamMap::new();
                m.insert("fmt".into(), serde_json::json!("json"));
                m.insert("base".into(), serde_json::json!("alerts"));
                m.insert("file".into(), serde_json::json!("default.jsonl"));
                m.insert("sync".into(), serde_json::json!(false));
                m
            },
            origin: None,
        }
    }

    #[test]
    fn merge_allowed_param() {
        let base: ParamMap = [("file".into(), serde_json::json!("a.jsonl"))].into();
        let overrides: ParamMap = [("file".into(), serde_json::json!("b.jsonl"))].into();
        let allow = vec!["file".into()];

        let merged = merge_params_with_allowlist(&base, &overrides, &allow).unwrap();
        assert_eq!(merged["file"], serde_json::json!("b.jsonl"));
    }

    #[test]
    fn merge_reject_disallowed_param() {
        let base = ParamMap::new();
        let overrides: ParamMap = [("secret".into(), serde_json::json!("val"))].into();
        let allow: Vec<String> = vec![];

        assert!(merge_params_with_allowlist(&base, &overrides, &allow).is_err());
    }

    #[test]
    fn merge_empty_allowlist_no_overrides() {
        let base: ParamMap = [("file".into(), serde_json::json!("a.jsonl"))].into();
        let overrides = ParamMap::new();
        let allow: Vec<String> = vec![];

        let merged = merge_params_with_allowlist(&base, &overrides, &allow).unwrap();
        assert_eq!(merged["file"], serde_json::json!("a.jsonl"));
    }

    #[test]
    fn tag_merge_three_levels() {
        let defaults = vec!["env:dev".into(), "region:us".into()];
        let group = vec!["env:staging".into()];
        let sink = vec!["env:prod".into(), "team:sec".into()];

        let merged = merge_tags(&defaults, Some(&group), Some(&sink));
        assert!(merged.contains(&"env:prod".into()));
        assert!(merged.contains(&"region:us".into()));
        assert!(merged.contains(&"team:sec".into()));
        assert!(!merged.contains(&"env:dev".into()));
        assert!(!merged.contains(&"env:staging".into()));
    }

    #[test]
    fn build_flex_group_basic() {
        let mut connectors = BTreeMap::new();
        connectors.insert("file_json".into(), sample_connector());

        let route = RouteGroup {
            name: "test_group".into(),
            parallel: None,
            windows: Some(super::super::types::StringOrArray(vec!["sec_*".into()])),
            tags: None,
            wf_meta_disable: vec!["__wfu_rule_name".into()],
            expect: None,
            sinks: vec![RouteSink {
                connect: "file_json".into(),
                name: Some("my_sink".into()),
                fields: Some(super::super::types::StringOrArray(vec![
                    "a".into(),
                    "b".into(),
                ])),
                params: {
                    let mut m = ParamMap::new();
                    m.insert("file".into(), serde_json::json!("sec.jsonl"));
                    m
                },
                tags: None,
                expect: None,
            }],
        };

        let defaults = DefaultsBody::default();
        let group = build_flex_group(&route, &connectors, &defaults).unwrap();

        assert_eq!(group.name, "test_group");
        assert_eq!(group.parallel, 1);
        assert_eq!(group.wf_meta_disable, vec!["__wfu_rule_name".to_string()]);
        assert!(group.windows.matches("sec_alerts"));
        assert!(!group.windows.matches("net_alerts"));
        assert_eq!(group.sinks.len(), 1);
        assert_eq!(group.sinks[0].spec.name, "my_sink");
        assert_eq!(group.sinks[0].spec.kind, "file");
        assert_eq!(
            group.sinks[0].spec.params["base"],
            serde_json::json!("alerts")
        );
        assert_eq!(
            group.sinks[0].spec.params["file"],
            serde_json::json!("sec.jsonl")
        );
        assert_eq!(
            group.sinks[0].fields.as_ref(),
            Some(&vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn build_flex_group_missing_connector() {
        let connectors = BTreeMap::new();
        let route = RouteGroup {
            name: "test".into(),
            parallel: None,
            windows: None,
            tags: None,
            wf_meta_disable: vec![],
            expect: None,
            sinks: vec![RouteSink {
                connect: "missing".into(),
                name: None,
                fields: None,
                params: ParamMap::new(),
                tags: None,
                expect: None,
            }],
        };
        let defaults = DefaultsBody::default();
        assert!(build_flex_group(&route, &connectors, &defaults).is_err());
    }

    #[test]
    fn build_flex_group_rejects_non_wfu_meta_disable_field() {
        let mut connectors = BTreeMap::new();
        connectors.insert("file_json".into(), sample_connector());

        let route = RouteGroup {
            name: "test".into(),
            parallel: None,
            windows: None,
            tags: None,
            wf_meta_disable: vec!["message".into()],
            expect: None,
            sinks: vec![RouteSink {
                connect: "file_json".into(),
                name: None,
                fields: None,
                params: ParamMap::new(),
                tags: None,
                expect: None,
            }],
        };
        let defaults = DefaultsBody::default();
        let err = build_flex_group(&route, &connectors, &defaults).unwrap_err();
        assert!(err.to_string().contains("must start"));
    }

    #[test]
    fn build_flex_group_accepts_all_wfu_meta_disable_wildcard() {
        let mut connectors = BTreeMap::new();
        connectors.insert("file_json".into(), sample_connector());

        let route = RouteGroup {
            name: "test".into(),
            parallel: None,
            windows: None,
            tags: None,
            wf_meta_disable: vec!["__wfu_*".into()],
            expect: None,
            sinks: vec![RouteSink {
                connect: "file_json".into(),
                name: None,
                fields: None,
                params: ParamMap::new(),
                tags: None,
                expect: None,
            }],
        };
        let defaults = DefaultsBody::default();
        let group = build_flex_group(&route, &connectors, &defaults).unwrap();
        assert_eq!(group.wf_meta_disable, vec!["__wfu_*".to_string()]);
    }

    #[test]
    fn build_flex_group_accepts_partial_wf_meta_disable_wildcard() {
        let mut connectors = BTreeMap::new();
        connectors.insert("file_json".into(), sample_connector());

        let route = RouteGroup {
            name: "test".into(),
            parallel: None,
            windows: None,
            tags: None,
            wf_meta_disable: vec!["__wfu_rule_*".into()],
            expect: None,
            sinks: vec![RouteSink {
                connect: "file_json".into(),
                name: None,
                fields: None,
                params: ParamMap::new(),
                tags: None,
                expect: None,
            }],
        };
        let defaults = DefaultsBody::default();
        let group = build_flex_group(&route, &connectors, &defaults).unwrap();
        assert_eq!(group.wf_meta_disable, vec!["__wfu_rule_*".to_string()]);
    }
}
