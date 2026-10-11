//! Monitor-output filtering ([metrics.monitor], warp-fusion#115).
//!
//! Compiles the config's `stage` / `name` / `target` glob rules into matchers
//! and decides which [`MetricsRecord`]s are forwarded to the monitor sinks.
//!
//! Filtering runs on the record's *metadata* (`stage` / `name` / `label`) only,
//! right before dispatch to the monitor fan-out — it never touches rule
//! processing, alert output, or the underlying counters.

use wildmatch::WildMatch;

use wf_config::{MetricsMonitorConfig, MetricsMonitorRule};

use super::MetricsRecord;

/// Whether a monitored metric is emitted with a per-target `label` (per
/// rule/window/source) or as a single global record — the config is probed with
/// a label value the family actually produces, so an omitted-`target` rule on a
/// labelled family (a silent no-op) is not misreported as excluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetricForm {
    Labelled,
    Global,
}

/// Metrics that the repository's bench / diagnostic scripts read back from
/// `metrics.ndjson` (or the `EMIT` counters derived from it, e.g.
/// `wfgen verify-nexmark`, the nexmark/alert_output bench scripts, the memory
/// bisection method). Excluding one is **allowed** — it is the point of the
/// filter — but almost always unintended, so the metrics task emits a WARN at
/// startup listing them; silent loss would make those checks read nothing.
///
/// Keep this list in sync with the consumers listed in
/// `docs/useage/config/metrics.md`.
pub(crate) const EXTERNAL_CONSUMER_METRICS: &[(&str, &str, MetricForm)] = &[
    ("alert", "emitted_total", MetricForm::Labelled),
    ("alert", "dispatch_total", MetricForm::Global),
    ("window", "append_total", MetricForm::Labelled),
    ("window", "acked_lag", MetricForm::Labelled),
    ("window", "evict_total", MetricForm::Labelled),
    ("window", "memory_bytes", MetricForm::Labelled),
    ("rule", "matches_total", MetricForm::Labelled),
    ("rule", "events_total", MetricForm::Labelled),
    ("receiver", "rows_total", MetricForm::Labelled),
    ("receiver", "decode_errors_total", MetricForm::Labelled),
];

/// Label used to probe whether a metric would be excluded for *some* target
/// (the filter matches on the record's `label`, which is not fixed at startup).
const LABEL_PROBE: &str = "__monitor_probe__";

/// Compiled `[metrics.monitor]` filter.
///
/// Precedence: an `exclude` match always drops the record; otherwise an
/// `include` match forwards it; otherwise [`default`](MetricsMonitorConfig::default)
/// decides. With no configured rules (the default) the filter is a no-op.
#[derive(Debug, Clone)]
pub(crate) struct MonitorFilter {
    default_include: bool,
    include: Vec<RuleMatcher>,
    exclude: Vec<RuleMatcher>,
}

#[derive(Debug, Clone)]
struct RuleMatcher {
    /// `None` = match any stage.
    stage: Option<WildMatch>,
    /// `None` = match any name.
    name: Option<WildMatch>,
    target: TargetMatcher,
}

#[derive(Debug, Clone)]
enum TargetMatcher {
    /// Rule omitted `target`: match only global records (no `label` field).
    GlobalOnly,
    /// `target = "*"`: match any record regardless of label.
    Any,
    /// Match the record's `label` against the glob; label-less records never match.
    Pattern(WildMatch),
}

impl MonitorFilter {
    /// Compile a filter from the effective `[metrics.monitor]` config.
    pub(crate) fn from_config(config: &MetricsMonitorConfig) -> Self {
        Self {
            default_include: matches!(config.default, wf_config::MonitorDefault::Include),
            include: config.include.iter().map(RuleMatcher::from_rule).collect(),
            exclude: config.exclude.iter().map(RuleMatcher::from_rule).collect(),
        }
    }

    /// A filter with no rules: forwards every record (used where no monitor
    /// filter applies, e.g. constructing metrics in tests).
    pub(crate) fn allow_all() -> Self {
        Self::from_config(&MetricsMonitorConfig::default())
    }

    /// Returns `true` when the filter has no rules and will forward every record.
    pub(crate) fn is_noop(&self) -> bool {
        self.default_include && self.include.is_empty() && self.exclude.is_empty()
    }

    /// Decide whether `record` is forwarded to the monitor sinks.
    pub(crate) fn allows(&self, record: &MetricsRecord) -> bool {
        let stage = record.field_value("stage").unwrap_or("");
        let name = record.field_value("name").unwrap_or("");
        // An empty `label` is semantically "no target": treat it the same as an
        // absent field so global rules (`target` omitted) still cover it.
        let label = record.field_value("label").filter(|l| !l.is_empty());
        self.allows_metric(stage, name, label)
    }

    /// Decide whether the metric identified by `stage` / `name` / `label`
    /// would be forwarded. Used both when emitting records and when deciding
    /// (at startup) whether a per-key metric family needs to be registered at
    /// all — so both must agree.
    pub(crate) fn allows_metric(&self, stage: &str, name: &str, label: Option<&str>) -> bool {
        if self
            .exclude
            .iter()
            .any(|rule| rule.matches(stage, name, label))
        {
            return false;
        }
        if self
            .include
            .iter()
            .any(|rule| rule.matches(stage, name, label))
        {
            return true;
        }
        self.default_include
    }

    /// Drop the records the filter rejects, preserving order.
    pub(crate) fn retain_allowed(&self, records: &mut Vec<MetricsRecord>) {
        if self.is_noop() {
            return;
        }
        records.retain(|record| self.allows(record));
    }

    /// The subset of [`EXTERNAL_CONSUMER_METRICS`] this filter would exclude.
    ///
    /// Each entry is probed with a label value matching its emitted form
    /// (global vs per-target), so an omitted-`target` rule against a labelled
    /// family — which excludes nothing — is not reported.
    pub(crate) fn excluded_external_metrics(&self) -> Vec<(&'static str, &'static str)> {
        EXTERNAL_CONSUMER_METRICS
            .iter()
            .filter(|entry| {
                let label = match entry.2 {
                    MetricForm::Global => None,
                    MetricForm::Labelled => Some(LABEL_PROBE),
                };
                !self.allows_metric(entry.0, entry.1, label)
            })
            .map(|entry| (entry.0, entry.1))
            .collect()
    }
}

impl RuleMatcher {
    fn from_rule(rule: &MetricsMonitorRule) -> Self {
        Self {
            stage: rule.stage.as_deref().map(WildMatch::new),
            name: rule.name.as_deref().map(WildMatch::new),
            target: match rule.target.as_deref() {
                None => TargetMatcher::GlobalOnly,
                Some("*") => TargetMatcher::Any,
                Some(pattern) => TargetMatcher::Pattern(WildMatch::new(pattern)),
            },
        }
    }

    fn matches(&self, stage: &str, name: &str, label: Option<&str>) -> bool {
        if let Some(pattern) = &self.stage
            && !pattern.matches(stage)
        {
            return false;
        }
        if let Some(pattern) = &self.name
            && !pattern.matches(name)
        {
            return false;
        }
        match &self.target {
            TargetMatcher::GlobalOnly => label.is_none(),
            TargetMatcher::Any => true,
            TargetMatcher::Pattern(pattern) => label.is_some_and(|l| pattern.matches(l)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wf_config::{MetricsMonitorRule, MonitorDefault};

    fn record(stage: &str, name: &str, label: Option<&str>) -> MetricsRecord {
        let mut fields = vec![
            ("stage".to_string(), stage.to_string()),
            ("name".to_string(), name.to_string()),
        ];
        if let Some(label) = label {
            fields.push(("label".to_string(), label.to_string()));
        }
        fields.push(("value".to_string(), "1".to_string()));
        MetricsRecord { fields }
    }

    fn rule(stage: Option<&str>, name: Option<&str>, target: Option<&str>) -> MetricsMonitorRule {
        MetricsMonitorRule {
            stage: stage.map(str::to_string),
            name: name.map(str::to_string),
            target: target.map(str::to_string),
        }
    }

    #[test]
    fn default_config_is_noop_and_allows_everything() {
        let filter = MonitorFilter::from_config(&MetricsMonitorConfig::default());
        assert!(filter.is_noop());
        assert!(filter.allows(&record("rule", "flush_seconds_p50", Some("r1"))));
        assert!(filter.allows(&record("receiver", "decode_seconds_p50", None)));
    }

    #[test]
    fn exclude_matches_glob_stage_and_name_for_any_target() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![rule(Some("rule"), Some("flush_seconds_*"), Some("*"))],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(!filter.allows(&record("rule", "flush_seconds_p50", Some("r1"))));
        assert!(!filter.allows(&record("rule", "flush_seconds_p99", Some("r2"))));
        assert!(filter.allows(&record("rule", "matches_total", Some("r1"))));
        assert!(filter.allows(&record("receiver", "flush_seconds_p50", Some("r1"))));
    }

    #[test]
    fn omitted_target_matches_only_global_records() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![rule(Some("receiver"), Some("decode_seconds_*"), None)],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(!filter.allows(&record("receiver", "decode_seconds_p50", None)));
        // Labelled records under the same stage/name are not matched.
        assert!(filter.allows(&record("receiver", "decode_seconds_p50", Some("tcp"))));
    }

    #[test]
    fn target_glob_matches_label_only() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![rule(Some("rule"), Some("*"), Some("port_*"))],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(!filter.allows(&record("rule", "matches_total", Some("port_scan"))));
        assert!(filter.allows(&record("rule", "matches_total", Some("auth_brute"))));
        assert!(filter.allows(&record("rule", "matches_total", None)));
    }

    #[test]
    fn exclude_takes_precedence_over_include() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Exclude,
            include: vec![rule(Some("rule"), Some("*"), Some("*"))],
            exclude: vec![rule(Some("rule"), Some("flush_seconds_*"), Some("*"))],
        };
        let filter = MonitorFilter::from_config(&cfg);
        // Included by the broad rule, but excluded by the specific one.
        assert!(!filter.allows(&record("rule", "flush_seconds_p50", Some("r1"))));
        assert!(filter.allows(&record("rule", "matches_total", Some("r1"))));
        // Not matched by any include → default exclude.
        assert!(!filter.allows(&record("alert", "emitted_total", Some("r1"))));
    }

    #[test]
    fn default_exclude_keeps_only_included() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Exclude,
            include: vec![rule(Some("alert"), Some("emitted_total"), Some("*"))],
            exclude: vec![],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(filter.allows(&record("alert", "emitted_total", Some("r1"))));
        assert!(!filter.allows(&record("rule", "matches_total", Some("r1"))));
        assert!(!filter.allows(&record("window", "rows", Some("w1"))));
    }

    #[test]
    fn question_mark_and_star_globs_are_supported() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![rule(Some("rule"), Some("flush_seconds_p9?"), Some("*"))],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(!filter.allows(&record("rule", "flush_seconds_p99", Some("r1"))));
        assert!(filter.allows(&record("rule", "flush_seconds_p50", Some("r1"))));
    }

    #[test]
    fn retain_allowed_filters_batch() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![rule(Some("rule"), Some("flush_seconds_*"), Some("*"))],
        };
        let filter = MonitorFilter::from_config(&cfg);
        let mut records = vec![
            record("rule", "flush_seconds_p50", Some("r1")),
            record("rule", "matches_total", Some("r1")),
            record("receiver", "decode_seconds_p50", None),
        ];
        filter.retain_allowed(&mut records);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].field_value("name"), Some("matches_total"));
        assert_eq!(records[1].field_value("name"), Some("decode_seconds_p50"));
    }

    #[test]
    fn empty_label_field_is_treated_as_global() {
        // A `label` field present but empty is semantically "no target": the
        // omitted-target (global-only) rule must still cover it, while a
        // label-pattern rule must not.
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![
                rule(Some("receiver"), Some("decode_seconds_*"), None),
                rule(Some("rule"), Some("matches_total"), Some("port_*")),
            ],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(!filter.allows(&record("receiver", "decode_seconds_p50", Some(""))));
        // Label-pattern rule must not match the empty label.
        assert!(filter.allows(&record("rule", "matches_total", Some(""))));
    }

    #[test]
    fn target_star_also_matches_global_records() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![rule(Some("receiver"), Some("frames_total"), Some("*"))],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(!filter.allows(&record("receiver", "frames_total", None)));
        assert!(filter.allows(&record("receiver", "connections_total", None)));
    }

    #[test]
    fn omitted_stage_and_name_match_any_metric() {
        // Both stage and name omitted → match-everything; excludes only the
        // records the target matcher accepts.
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![rule(None, None, Some("*"))],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(!filter.allows(&record("rule", "matches_total", Some("r1"))));
        assert!(!filter.allows(&record("receiver", "decode_seconds_p50", None)));
    }

    #[test]
    fn rule_with_only_name_matches_across_stages() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![rule(None, Some("frames_total"), Some("*"))],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(!filter.allows(&record("receiver", "frames_total", None)));
        assert!(!filter.allows(&record("router", "frames_total", Some("x"))));
        assert!(filter.allows(&record("receiver", "connections_total", None)));
    }

    #[test]
    fn multiple_rules_are_ored() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![
                rule(Some("rule"), Some("flush_seconds_*"), Some("*")),
                rule(Some("receiver"), Some("decode_seconds_*"), None),
            ],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(!filter.allows(&record("rule", "flush_seconds_p50", Some("r1"))));
        assert!(!filter.allows(&record("receiver", "decode_seconds_p99", None)));
        assert!(filter.allows(&record("rule", "matches_total", Some("r1"))));
    }

    #[test]
    fn include_list_under_default_include_never_drops() {
        // With default=include, an include list is a no-op for records that
        // would already be forwarded — it must never turn into an exclusion.
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![rule(Some("alert"), Some("emitted_total"), Some("*"))],
            exclude: vec![],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(filter.allows(&record("alert", "emitted_total", Some("r1"))));
        assert!(filter.allows(&record("rule", "matches_total", Some("r1"))));
        assert!(filter.allows(&record("receiver", "decode_seconds_p50", None)));
    }

    #[test]
    fn target_question_mark_glob_is_supported() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![rule(Some("rule"), Some("matches_total"), Some("r?"))],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(!filter.allows(&record("rule", "matches_total", Some("r1"))));
        assert!(filter.allows(&record("rule", "matches_total", Some("r12"))));
    }

    #[test]
    fn external_consumer_metrics_reported_when_excluded() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![
                rule(Some("alert"), Some("emitted_total"), Some("*")),
                rule(Some("window"), Some("append_total"), Some("*")),
            ],
        };
        let filter = MonitorFilter::from_config(&cfg);
        let excluded = filter.excluded_external_metrics();
        assert!(excluded.contains(&("alert", "emitted_total")));
        assert!(excluded.contains(&("window", "append_total")));
        assert!(!excluded.contains(&("window", "acked_lag")));
        assert!(!excluded.contains(&("rule", "matches_total")));
    }

    #[test]
    fn external_consumer_metrics_reported_under_default_exclude() {
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Exclude,
            include: vec![rule(Some("alert"), Some("emitted_total"), Some("*"))],
            exclude: vec![],
        };
        let filter = MonitorFilter::from_config(&cfg);
        let excluded = filter.excluded_external_metrics();
        // emitted_total 被 include 保留 → 不报；其余关键指标被 default=exclude 丢掉 → 报。
        assert!(!excluded.contains(&("alert", "emitted_total")));
        assert!(excluded.contains(&("window", "append_total")));
        assert!(excluded.contains(&("rule", "matches_total")));
    }

    #[test]
    fn external_consumer_metrics_empty_when_no_exclusion() {
        let filter = MonitorFilter::from_config(&MetricsMonitorConfig::default());
        assert!(filter.excluded_external_metrics().is_empty());
    }

    #[test]
    fn external_consumer_global_form_reported() {
        // `alert.dispatch_total` 是全局（无 label）记录；省略 target 的排除规则
        // 确实会排除它 → 应报告。
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![MetricsMonitorRule {
                stage: Some("alert".into()),
                name: Some("dispatch_total".into()),
                target: None,
            }],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(
            !filter.allows_metric("alert", "dispatch_total", None),
            "global record is excluded"
        );
        assert!(
            filter
                .excluded_external_metrics()
                .contains(&("alert", "dispatch_total"))
        );
    }

    #[test]
    fn external_consumer_labelled_form_not_reported_for_global_only_rule() {
        // `alert.emitted_total` 是带 label 的 per-rule 指标；省略 target 的规则
        // 只匹配全局记录，对它实际是 no-op → 不应误报。
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![MetricsMonitorRule {
                stage: Some("alert".into()),
                name: Some("emitted_total".into()),
                target: None,
            }],
        };
        let filter = MonitorFilter::from_config(&cfg);
        assert!(
            filter.allows_metric("alert", "emitted_total", Some("r1")),
            "labelled record is not excluded by a global-only rule"
        );
        assert!(
            !filter
                .excluded_external_metrics()
                .contains(&("alert", "emitted_total")),
            "must not warn when the rule excludes nothing"
        );
    }

    #[test]
    fn allows_record_and_allows_metric_agree() {
        // 输出端 `allows(record)` 与注册端 `allows_metric(...)` 必须同源：
        // 控制台复用族在注册时用后者判断，输出时用前者剔除，两者不得分叉。
        let cfg = MetricsMonitorConfig {
            default: MonitorDefault::Include,
            include: vec![],
            exclude: vec![
                rule(Some("rule"), Some("flush_seconds_*"), Some("*")),
                rule(Some("receiver"), Some("decode_seconds_*"), None),
                rule(None, Some("events_total"), Some("port_*")),
            ],
        };
        let filter = MonitorFilter::from_config(&cfg);
        for (stage, name, label) in [
            ("rule", "flush_seconds_p50", Some("r1")),
            ("receiver", "decode_seconds_p99", None),
            ("rule", "events_total", Some("port_scan")),
            ("rule", "events_total", Some("auth")),
            ("rule", "matches_total", Some("r1")),
            ("router", "delivered_total", None),
        ] {
            assert_eq!(
                filter.allows(&record(stage, name, label)),
                filter.allows_metric(stage, name, label),
                "mismatch for {stage}.{name} label={label:?}"
            );
        }
    }
}
