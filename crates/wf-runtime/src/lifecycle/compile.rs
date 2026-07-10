#![allow(clippy::items_after_test_module)]

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use orion_error::conversion::ToStructError;
use orion_error::prelude::*;

use wf_config::ConfigVarContext;
use wf_config::project::load_wfl_with_context;
use wf_config::resolve_glob;
use wf_config::window::WindowDefaults;
use wf_config::{DistMode, FusionConfig, WindowConfig};
use wf_engine::match_engine::{CepStateMachine, RuleExecutor};
use wf_lang::ast::{FieldRef, Measure};
use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

use crate::error::{RuntimeReason, RuntimeResult};

use super::types::{RunRule, RunRuleKind};

const PIPE_WINDOW_PREFIX: &str = "__wf_pipe_";
const PIPE_EVENT_TIME_FIELD: &str = "__wf_pipe_ts";

struct ParsedRuleFile {
    path: std::path::PathBuf,
    source: String,
    file: wf_lang::ast::WflFile,
}

// ---------------------------------------------------------------------------
// Compile-phase helpers — pure data transforms extracted from start()
// ---------------------------------------------------------------------------

/// Load all `.wfs` schema files matching `glob_pattern` under `base_dir`.
pub(crate) fn load_schemas(
    glob_pattern: &str,
    base_dir: &Path,
) -> RuntimeResult<Vec<wf_lang::WindowSchema>> {
    let wfs_paths = resolve_glob(glob_pattern, base_dir)
        .source_err(RuntimeReason::core_conf(), "resolve schema glob")?;
    let mut all_schemas = Vec::new();
    for full_path in &wfs_paths {
        let content = std::fs::read_to_string(full_path)
            .source_err(RuntimeReason::system_error(), "read schema file")
            .position(full_path.display().to_string())?;
        let schemas = wf_lang::parse_wfs(&content)
            .source_err(RuntimeReason::Bootstrap, "parse schema file")
            .position(full_path.display().to_string())?;
        wf_debug!(conf, file = %full_path.display(), schemas = schemas.len(), "loaded schema file");
        all_schemas.extend(schemas);
    }
    Ok(all_schemas)
}

/// Load, preprocess, parse, and compile all `.wfl` rule files matching
/// `glob_pattern` under `base_dir`, substituting `vars` and validating
/// against the given `schemas`.
pub(crate) fn compile_rules(
    glob_pattern: &str,
    base_dir: &Path,
    ctx: &ConfigVarContext,
    schemas: &[wf_lang::WindowSchema],
) -> RuntimeResult<(Vec<wf_lang::plan::RulePlan>, Vec<wf_lang::WindowSchema>)> {
    let wfl_paths = resolve_glob(glob_pattern, base_dir)
        .source_err(RuntimeReason::core_conf(), "resolve rule glob")?;
    let mut parsed_files = Vec::new();
    let mut all_rules = Vec::new();
    for full_path in &wfl_paths {
        let preprocessed = load_wfl_with_context(full_path, ctx, Some(base_dir))
            .source_err(RuntimeReason::data_error(), "load rule file")
            .position(full_path.display().to_string())?;
        let wfl_file = wf_lang::parse_wfl_with_diagnostics(&preprocessed, full_path)
            .map_err(lang_diagnostic)?;
        all_rules.extend(wfl_file.rules.iter().cloned());
        parsed_files.push(ParsedRuleFile {
            path: full_path.clone(),
            source: preprocessed,
            file: wfl_file,
        });
    }

    let effective_schemas = wf_lang::effective_schemas_for_rules(&all_rules, schemas);
    let mut topology_errors = Vec::new();
    wf_lang::check_intermediate_target_graph(&all_rules, &mut topology_errors);
    let topology_hard_errors: Vec<_> = topology_errors
        .into_iter()
        .filter(|error| error.severity == wf_lang::Severity::Error)
        .collect();
    if !topology_hard_errors.is_empty() {
        let msgs: Vec<String> = topology_hard_errors
            .into_iter()
            .map(|error| format_topology_error(&error, &parsed_files))
            .collect();
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail(msgs.join("\n"))
            .err();
    }

    let mut all_rule_plans = Vec::new();
    for parsed in &parsed_files {
        let plans = wf_lang::compile_wfl_with_diagnostics(
            &parsed.file,
            &effective_schemas,
            &parsed.source,
            &parsed.path,
        )
        .map_err(lang_diagnostic)?;
        wf_debug!(conf, file = %parsed.path.display(), rules = plans.len(), "compiled rule file");
        all_rule_plans.extend(plans);
    }
    Ok((all_rule_plans, effective_schemas))
}

fn lang_diagnostic(error: wf_lang::LangError) -> crate::error::RuntimeError {
    RuntimeReason::Bootstrap
        .to_err()
        .with_detail(error.detail().clone().unwrap_or_else(|| error.to_string()))
}

fn format_topology_error(error: &wf_lang::CheckError, parsed_files: &[ParsedRuleFile]) -> String {
    if let Some(rule_name) = error.rule.as_deref()
        && let Some(parsed) = parsed_files
            .iter()
            .find(|parsed| parsed.file.rules.iter().any(|rule| rule.name == rule_name))
    {
        return wf_lang::diagnostics::format_check_error_with_source(
            error,
            &parsed.file,
            &parsed.source,
            &parsed.path,
        );
    }
    error.to_string()
}

pub(crate) fn build_runtime_var_context(
    config: &FusionConfig,
    base_dir: &Path,
) -> ConfigVarContext {
    let mut vars = config.vars.clone();
    vars.entry("WORK_DIR".to_string())
        .or_insert_with(|| base_dir.to_string_lossy().to_string());
    vars.entry("WORK_ROOT".to_string()).or_insert_with(|| {
        resolve_work_root(config, base_dir)
            .to_string_lossy()
            .to_string()
    });
    ConfigVarContext::from_explicit_vars(vars)
}

pub(crate) fn resolve_work_root(config: &FusionConfig, base_dir: &Path) -> std::path::PathBuf {
    config
        .work_root
        .as_ref()
        .map(|path| base_dir.join(path))
        .unwrap_or_else(|| base_dir.to_path_buf())
}

/// Build synthetic schemas/configs for internal pipeline windows (`|>` desugar).
pub(crate) fn build_pipeline_internal_windows(
    plans: &[wf_lang::plan::RulePlan],
    base_schemas: &[WindowSchema],
    defaults: &WindowDefaults,
) -> (Vec<WindowSchema>, Vec<WindowConfig>) {
    let mut known_schemas: HashMap<String, WindowSchema> = base_schemas
        .iter()
        .map(|s| (s.name.clone(), s.clone()))
        .collect();

    let mut derived = Vec::new();
    for plan in plans {
        let target = &plan.yield_plan.target;
        if !is_pipeline_window_name(target) || known_schemas.contains_key(target) {
            continue;
        }

        let Some(over) = find_pipeline_window_over(plans, target) else {
            wf_warn!(
                conf,
                window = %target,
                "skip internal pipeline window without downstream consumer"
            );
            continue;
        };

        let mut fields = vec![FieldDef {
            name: PIPE_EVENT_TIME_FIELD.to_string(),
            field_type: FieldType::Base(BaseType::Time),
        }];
        fields.extend(infer_pipeline_output_fields(plan, &known_schemas));

        let ws = WindowSchema {
            name: target.clone(),
            // Bind stream alias routing uses schema streams; subscribe this internal
            // window on a synthetic stream equal to its own name.
            streams: vec![target.clone()],
            time_field: Some(PIPE_EVENT_TIME_FIELD.to_string()),
            over,
            fields,
        };
        known_schemas.insert(ws.name.clone(), ws.clone());
        derived.push(ws);
    }

    let configs = derived
        .iter()
        .map(|ws| WindowConfig {
            name: ws.name.clone(),
            mode: DistMode::Local,
            max_window_bytes: defaults.max_window_bytes,
            over_cap: ws.over.into(),
            evict_policy: defaults.evict_policy,
            watermark: defaults.watermark,
            allowed_lateness: defaults.allowed_lateness,
            late_policy: defaults.late_policy,
            table: None,
        })
        .collect();

    (derived, configs)
}

/// Build [`RunRule`] instances from compiled plans, pre-computing stream
/// alias routing and constructing the CEP state machines.
pub(crate) fn build_run_rules(
    plans: &[wf_lang::plan::RulePlan],
    schemas: &[wf_lang::WindowSchema],
) -> Vec<RunRule> {
    let mut rules = Vec::with_capacity(plans.len());
    for plan in plans {
        let window_aliases = build_window_aliases(&plan.binds);
        let executor = RuleExecutor::new_with_yield_field_types(
            plan.clone(),
            resolve_yield_field_types(plan, schemas),
        );
        let kind = if let Some(each_plan) = &plan.each_plan {
            RunRuleKind::Each {
                alias: each_plan.alias.clone(),
                time_field: resolve_alias_time_field(&plan.binds, schemas, &each_plan.alias),
            }
        } else {
            let time_field = resolve_time_field(&plan.binds, schemas);
            let limits = plan.limits_plan.clone();
            let machine = CepStateMachine::with_limits(
                plan.name.clone(),
                plan.match_plan.clone(),
                time_field,
                limits,
            );
            RunRuleKind::Match(Box::new(machine))
        };
        rules.push(RunRule {
            kind,
            executor,
            window_aliases,
        });
    }
    rules
}

pub(crate) fn collect_intermediate_targets(plans: &[wf_lang::plan::RulePlan]) -> HashSet<String> {
    let consumed_windows: HashSet<&str> = plans
        .iter()
        .flat_map(|plan| plan.binds.iter().map(|bind| bind.window.as_str()))
        .collect();

    plans
        .iter()
        .map(|plan| plan.yield_plan.target.as_str())
        .filter(|target| consumed_windows.contains(*target))
        .map(str::to_string)
        .collect()
}

fn resolve_yield_field_types(
    plan: &wf_lang::plan::RulePlan,
    schemas: &[WindowSchema],
) -> HashMap<String, FieldType> {
    let Some(target_schema) = schemas.iter().find(|ws| ws.name == plan.yield_plan.target) else {
        return HashMap::new();
    };
    let schema_fields: HashMap<&str, &FieldType> = target_schema
        .fields
        .iter()
        .map(|field| (field.name.as_str(), &field.field_type))
        .collect();

    plan.yield_plan
        .fields
        .iter()
        .filter_map(|field| {
            schema_fields
                .get(field.name.as_str())
                .map(|field_type| (field.name.clone(), (*field_type).clone()))
        })
        .collect()
}

/// Resolve the event-time field name for a rule from its first bind's window schema.
pub(crate) fn resolve_time_field(
    binds: &[wf_lang::plan::BindPlan],
    schemas: &[wf_lang::WindowSchema],
) -> Option<String> {
    binds.first().and_then(|bind| {
        schemas
            .iter()
            .find(|ws| ws.name == bind.window)
            .and_then(|ws| ws.time_field.clone())
    })
}

fn resolve_alias_time_field(
    binds: &[wf_lang::plan::BindPlan],
    schemas: &[wf_lang::WindowSchema],
    alias: &str,
) -> Option<String> {
    let bind = binds.iter().find(|bind| bind.alias == alias)?;
    schemas
        .iter()
        .find(|ws| ws.name == bind.window)
        .and_then(|ws| ws.time_field.clone())
}

/// Build window_name → alias routing for a rule from its binds.
fn build_window_aliases(binds: &[wf_lang::plan::BindPlan]) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for bind in binds {
        map.entry(bind.window.clone())
            .or_default()
            .push(bind.alias.clone());
    }
    map
}

fn is_pipeline_window_name(name: &str) -> bool {
    name.starts_with(PIPE_WINDOW_PREFIX)
}

fn find_pipeline_window_over(plans: &[wf_lang::plan::RulePlan], window: &str) -> Option<Duration> {
    plans
        .iter()
        .find(|p| p.binds.iter().any(|b| b.window == window))
        .map(|p| match p.match_plan.window_spec {
            wf_lang::plan::WindowSpec::Sliding(d)
            | wf_lang::plan::WindowSpec::Fixed(d)
            | wf_lang::plan::WindowSpec::Session(d) => d,
        })
}

fn infer_pipeline_output_fields(
    plan: &wf_lang::plan::RulePlan,
    schemas: &HashMap<String, WindowSchema>,
) -> Vec<FieldDef> {
    let key_types = infer_key_field_types(plan, schemas);
    let branch_types = infer_branch_output_types(plan);

    plan.yield_plan
        .fields
        .iter()
        .filter_map(|f| {
            if f.name == PIPE_EVENT_TIME_FIELD {
                return None;
            }
            let field_type = key_types
                .get(&f.name)
                .cloned()
                .or_else(|| branch_types.get(&f.name).cloned())
                .unwrap_or(FieldType::Base(BaseType::Chars));
            Some(FieldDef {
                name: f.name.clone(),
                field_type,
            })
        })
        .collect()
}

fn infer_key_field_types(
    plan: &wf_lang::plan::RulePlan,
    schemas: &HashMap<String, WindowSchema>,
) -> HashMap<String, FieldType> {
    let mut out = HashMap::new();

    if let Some(key_map) = &plan.match_plan.key_map {
        for item in key_map {
            if let Some(field_type) = resolve_bind_field_type(
                &plan.binds,
                schemas,
                &item.source_alias,
                &item.source_field,
            ) {
                out.insert(item.logical_name.clone(), field_type);
            }
        }
        return out;
    }

    for key in &plan.match_plan.keys {
        let name = key_output_name(key);
        if let Some(field_type) = resolve_key_field_type(plan, schemas, key) {
            out.insert(name, field_type);
        }
    }

    out
}

fn resolve_key_field_type(
    plan: &wf_lang::plan::RulePlan,
    schemas: &HashMap<String, WindowSchema>,
    key: &FieldRef,
) -> Option<FieldType> {
    match key {
        FieldRef::Qualified(alias, field) | FieldRef::Bracketed(alias, field) => {
            resolve_bind_field_type(&plan.binds, schemas, alias, field)
        }
        FieldRef::Simple(field) => {
            let mut found: Vec<FieldType> = Vec::new();
            for bind in &plan.binds {
                let Some(ws) = schemas.get(&bind.window) else {
                    continue;
                };
                let Some(field_type) = ws
                    .fields
                    .iter()
                    .find(|f| f.name == *field)
                    .map(|f| f.field_type.clone())
                else {
                    continue;
                };
                if !found.contains(&field_type) {
                    found.push(field_type);
                }
            }
            if found.len() == 1 {
                Some(found.remove(0))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn resolve_bind_field_type(
    binds: &[wf_lang::plan::BindPlan],
    schemas: &HashMap<String, WindowSchema>,
    alias: &str,
    field: &str,
) -> Option<FieldType> {
    let bind = binds.iter().find(|b| b.alias == alias)?;
    let ws = schemas.get(&bind.window)?;
    ws.fields
        .iter()
        .find(|f| f.name == field)
        .map(|f| f.field_type.clone())
}

fn infer_branch_output_types(plan: &wf_lang::plan::RulePlan) -> HashMap<String, FieldType> {
    let mut map = HashMap::new();
    for step in plan
        .match_plan
        .event_steps
        .iter()
        .chain(plan.match_plan.close_steps.iter())
    {
        for branch in &step.branches {
            let name = branch
                .label
                .clone()
                .unwrap_or_else(|| measure_output_name(branch.agg.measure).to_string());
            let field_type = match branch.agg.measure {
                Measure::Count => FieldType::Base(BaseType::Digit),
                Measure::Sum | Measure::Avg | Measure::Min | Measure::Max => {
                    FieldType::Base(BaseType::Float)
                }
                _ => FieldType::Base(BaseType::Float),
            };
            map.insert(name, field_type);
        }
    }
    map
}

fn key_output_name(key: &FieldRef) -> String {
    match key {
        FieldRef::Simple(name) => name.clone(),
        FieldRef::Qualified(_, field) | FieldRef::Bracketed(_, field) => field.clone(),
        _ => "__unknown_key".to_string(),
    }
}

fn measure_output_name(measure: Measure) -> &'static str {
    match measure {
        Measure::Count => "count",
        Measure::Sum => "sum",
        Measure::Avg => "avg",
        Measure::Min => "min",
        Measure::Max => "max",
        _ => "measure",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    use wf_config::ConfigVarContext;
    use wf_config::{ByteSize, EvictPolicy, HumanDuration, LatePolicy};
    use wf_lang::parse_wfl;

    fn defaults() -> WindowDefaults {
        WindowDefaults {
            evict_interval: HumanDuration::from(Duration::from_secs(10)),
            max_window_bytes: ByteSize::from(1024 * 1024usize),
            max_total_bytes: ByteSize::from(16 * 1024 * 1024usize),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: HumanDuration::from(Duration::from_secs(0)),
            allowed_lateness: HumanDuration::from(Duration::from_secs(60)),
            late_policy: LatePolicy::Drop,
        }
    }

    #[test]
    fn build_pipeline_internal_windows_derives_schema_and_config() {
        let base_schemas = vec![
            WindowSchema {
                name: "fw_events".into(),
                streams: vec!["syslog".into()],
                time_field: Some("event_time".into()),
                over: Duration::from_secs(3600),
                fields: vec![
                    FieldDef {
                        name: "event_time".into(),
                        field_type: FieldType::Base(BaseType::Time),
                    },
                    FieldDef {
                        name: "sip".into(),
                        field_type: FieldType::Base(BaseType::Ip),
                    },
                    FieldDef {
                        name: "dport".into(),
                        field_type: FieldType::Base(BaseType::Digit),
                    },
                ],
            },
            WindowSchema {
                name: "alerts".into(),
                streams: vec![],
                time_field: Some("emit_time".into()),
                over: Duration::from_secs(3600),
                fields: vec![
                    FieldDef {
                        name: "emit_time".into(),
                        field_type: FieldType::Base(BaseType::Time),
                    },
                    FieldDef {
                        name: "sip".into(),
                        field_type: FieldType::Base(BaseType::Ip),
                    },
                ],
            },
        ];

        let wfl = parse_wfl(
            r#"
rule pipe {
  events { e: fw_events }
  match<sip,dport:5m> {
    on event { c1: e | count >= 1; }
  }
  |> match<sip:10m> {
    on event { c2: _in | count >= 1; }
  } -> score(80.0)
  entity(ip, _in.sip)
  yield alerts (sip = _in.sip)
}
"#,
        )
        .unwrap();
        let plans = wf_lang::compile_wfl(&wfl, &base_schemas).unwrap();

        let (schemas, configs) =
            build_pipeline_internal_windows(&plans, &base_schemas, &defaults());
        assert_eq!(schemas.len(), 1);
        assert_eq!(configs.len(), 1);

        let ws = &schemas[0];
        assert_eq!(ws.name, "__wf_pipe_pipe_w1");
        assert_eq!(ws.streams, vec!["__wf_pipe_pipe_w1".to_string()]);
        assert_eq!(ws.time_field.as_deref(), Some("__wf_pipe_ts"));
        assert_eq!(ws.over, Duration::from_secs(600));
        assert!(ws.fields.iter().any(|f| f.name == "__wf_pipe_ts"));
        assert!(ws.fields.iter().any(|f| f.name == "sip"));
        assert!(ws.fields.iter().any(|f| f.name == "c1"));

        let cfg = &configs[0];
        assert_eq!(cfg.name, ws.name);
        assert_eq!(cfg.mode, DistMode::Local);
    }

    #[test]
    fn compile_rules_reports_source_aware_rule_diagnostics() {
        let dir = tempfile::tempdir().unwrap();
        let rule_path = dir.path().join("bad_rule.wfl");
        std::fs::write(
            &rule_path,
            r#"
rule bad_yield {
  events { e: fw_events }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield missing_alerts (sip = e.sip)
}
"#,
        )
        .unwrap();

        let schemas = vec![WindowSchema {
            name: "fw_events".into(),
            streams: vec!["syslog".into()],
            time_field: Some("event_time".into()),
            over: Duration::from_secs(3600),
            fields: vec![
                FieldDef {
                    name: "event_time".into(),
                    field_type: FieldType::Base(BaseType::Time),
                },
                FieldDef {
                    name: "sip".into(),
                    field_type: FieldType::Base(BaseType::Ip),
                },
            ],
        }];
        let err = compile_rules(
            "*.wfl",
            dir.path(),
            &ConfigVarContext::from_explicit_vars(HashMap::new()),
            &schemas,
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("bad_rule.wfl"), "{text}");
        assert!(text.contains("category: yield"), "{text}");
        assert!(text.contains("rule: bad_yield"), "{text}");
        assert!(text.contains("location: line 6, column 9"), "{text}");
        assert!(text.contains("yield missing_alerts"), "{text}");
    }

    #[test]
    fn compile_rules_reports_source_aware_topology_diagnostics() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.wfl"),
            r#"
rule make_b {
  events { e: win_a }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield win_b (sip = e.sip)
}
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("b.wfl"),
            r#"
rule make_a {
  events { e: win_b }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield win_a (sip = e.sip)
}
"#,
        )
        .unwrap();

        let schemas = vec![
            WindowSchema {
                name: "win_a".into(),
                streams: vec!["a".into()],
                time_field: Some("event_time".into()),
                over: Duration::from_secs(3600),
                fields: vec![
                    FieldDef {
                        name: "event_time".into(),
                        field_type: FieldType::Base(BaseType::Time),
                    },
                    FieldDef {
                        name: "sip".into(),
                        field_type: FieldType::Base(BaseType::Ip),
                    },
                ],
            },
            WindowSchema {
                name: "win_b".into(),
                streams: vec![],
                time_field: Some("event_time".into()),
                over: Duration::from_secs(3600),
                fields: vec![
                    FieldDef {
                        name: "event_time".into(),
                        field_type: FieldType::Base(BaseType::Time),
                    },
                    FieldDef {
                        name: "sip".into(),
                        field_type: FieldType::Base(BaseType::Ip),
                    },
                ],
            },
        ];
        let err = compile_rules(
            "*.wfl",
            dir.path(),
            &ConfigVarContext::from_explicit_vars(HashMap::new()),
            &schemas,
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("category: topology"), "{text}");
        assert!(text.contains("rule:"), "{text}");
        assert!(text.contains("location: line 2, column 1"), "{text}");
        assert!(
            text.contains("rule make_a") || text.contains("rule make_b"),
            "{text}"
        );
    }
}

pub(crate) fn load_static_schemas(
    glob_pattern: &str,
    base_dir: &Path,
) -> RuntimeResult<Vec<wf_lang::StaticWindowSchema>> {
    let mut schemas = Vec::new();
    for path in resolve_glob(glob_pattern, base_dir).map_err(|e| {
        RuntimeReason::Bootstrap
            .to_err()
            .with_detail(format!("glob: {}", e))
    })? {
        let source = std::fs::read_to_string(&path)
            .source_err(RuntimeReason::Bootstrap, format!("read schema {:?}", path))?;
        let parsed = wf_lang::parse_static_wfs(&source).map_err(|e| {
            RuntimeReason::Bootstrap
                .to_err()
                .with_detail(format!("parse static schemas from {:?}: {}", path, e))
        })?;
        schemas.extend(parsed);
    }
    Ok(schemas)
}
