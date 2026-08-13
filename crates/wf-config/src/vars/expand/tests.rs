use super::*;

#[test]
fn simple_expand_and_resolve_apis_return_plain_values() {
    let value: TomlValue = toml::from_str(
        r#"
sinks = "${CASE_PATH}/sinks"

[vars]
CASE_PATH = "/tmp/from-file"
"#,
    )
    .expect("parse value");
    let ctx = ConfigVarContext::new();

    let vars = resolve_value_vars(&value, &ctx).expect("resolve plain vars");
    assert_eq!(
        vars.get("CASE_PATH").map(String::as_str),
        Some("/tmp/from-file")
    );

    let expanded = expand_value(&value, &ctx).expect("expand plain value");
    assert_eq!(
        expanded.get("sinks").and_then(TomlValue::as_str),
        Some("/tmp/from-file/sinks")
    );
}

#[test]
fn public_expand_and_resolve_apis_report_sources() {
    let value: TomlValue = toml::from_str(
        r#"
mode = "batch"
sinks = "${CASE_PATH}/sinks"

[[sources]]
type = "file"
path = "${CASE_PATH}/data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
parse_parallelism = 2
rule_exec_timeout = "30s"
schemas = "${CASE_PATH}/schemas/base/*.wfs"
rules = "${CASE_PATH}/rules/base/*.wfl"

[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.base_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"
"#,
    )
    .expect("parse value");

    let mut explicit = HashMap::new();
    explicit.insert("CASE_PATH".to_string(), "/tmp/from-cli".to_string());
    let ctx = ConfigVarContext::from_explicit_vars(explicit);

    let vars = resolve_value_vars_with_sources(&value, &ctx, |_| None).expect("resolve vars");
    assert_eq!(
        vars.get("CASE_PATH").expect("case path").rendered_sources(),
        "<cli:CASE_PATH>"
    );

    let expanded = expand_value_with_sources(&value, &ctx, |_| None).expect("expand value");
    assert_eq!(
        expanded
            .rendered_source_for("sources")
            .expect("sources provenance"),
        "<cli:CASE_PATH>"
    );
    assert_eq!(
        expanded
            .value
            .get("sinks")
            .and_then(TomlValue::as_str)
            .expect("sinks value"),
        "/tmp/from-cli/sinks"
    );
}
