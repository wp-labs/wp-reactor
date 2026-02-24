use super::io::SinkConfigBundle;

/// Validate that all yield targets have at least one matching sink group.
///
/// If a yield target doesn't match any business group, it must be covered
/// by the `infra_default` group. Returns an error listing uncovered targets
/// if validation fails.
pub fn validate_sink_coverage(
    yield_targets: &[String],
    bundle: &SinkConfigBundle,
) -> anyhow::Result<()> {
    let has_default = bundle.infra_default.is_some();
    let mut uncovered = Vec::new();

    for target in yield_targets {
        let matched = bundle.business.iter().any(|g| g.windows.matches(target));

        if !matched && !has_default {
            uncovered.push(target.as_str());
        }
    }

    if uncovered.is_empty() {
        Ok(())
    } else {
        anyhow::bail!(
            "yield targets not covered by any sink group and no default group configured: {:?}",
            uncovered,
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::defaults::DefaultsBody;
    use super::super::group::{FixedGroup, FlexGroup};
    use super::super::types::WildArray;
    use super::*;
    use std::collections::BTreeMap;

    fn make_bundle(business: Vec<FlexGroup>, has_default: bool) -> SinkConfigBundle {
        SinkConfigBundle {
            connectors: BTreeMap::new(),
            defaults: DefaultsBody::default(),
            business,
            infra_default: if has_default {
                Some(FixedGroup {
                    name: "__default".into(),
                    expect: None,
                    sinks: vec![],
                    parallel: 1,
                })
            } else {
                None
            },
            infra_error: None,
        }
    }

    fn make_flex(patterns: &[&str]) -> FlexGroup {
        FlexGroup {
            name: "test".into(),
            parallel: 1,
            windows: WildArray::new(&patterns.iter().map(|s| s.to_string()).collect::<Vec<_>>()),
            tags: vec![],
            expect: None,
            sinks: vec![],
        }
    }

    #[test]
    fn all_covered() {
        let bundle = make_bundle(
            vec![make_flex(&["security_*"]), make_flex(&["network_*"])],
            false,
        );
        let targets = vec!["security_alerts".into(), "network_events".into()];
        assert!(validate_sink_coverage(&targets, &bundle).is_ok());
    }

    #[test]
    fn uncovered_with_default() {
        let bundle = make_bundle(vec![make_flex(&["security_*"])], true);
        let targets = vec!["security_alerts".into(), "unknown_window".into()];
        assert!(validate_sink_coverage(&targets, &bundle).is_ok());
    }

    #[test]
    fn uncovered_without_default() {
        let bundle = make_bundle(vec![make_flex(&["security_*"])], false);
        let targets = vec!["security_alerts".into(), "unknown_window".into()];
        let err = validate_sink_coverage(&targets, &bundle).unwrap_err();
        assert!(err.to_string().contains("unknown_window"));
    }

    #[test]
    fn empty_targets() {
        let bundle = make_bundle(vec![], false);
        assert!(validate_sink_coverage(&[], &bundle).is_ok());
    }

    #[test]
    fn catch_all_covers_everything() {
        let bundle = make_bundle(vec![make_flex(&["*"])], false);
        let targets = vec!["anything".into(), "whatever".into()];
        assert!(validate_sink_coverage(&targets, &bundle).is_ok());
    }
}
