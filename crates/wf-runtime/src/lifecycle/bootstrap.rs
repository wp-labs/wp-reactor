use orion_error::conversion::ToStructError;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use orion_error::prelude::*;
use wp_core_connectors::sinks::blackhole_factory::BlackHoleFactory;
use wp_core_connectors::sinks::file_factory::FileFactory;
use wp_core_connectors::sinks::syslog::SyslogFactory;
use wp_core_connectors::sinks::tcp::TcpFactory;

use wf_config::ConfigVarContext;
use wf_config::FusionConfig;
use wf_engine::window::{ProviderWindow, Router, WindowRegistry};

use crate::error::{RuntimeReason, RuntimeResult};
use crate::receiver::miss::WINDOW_MISS_WINDOW_NAME;
use crate::schema_bridge::schemas_to_window_defs;
use crate::sink_build::{SinkFactoryRegistry, build_sink_dispatcher};

use super::compile::{
    build_pipeline_internal_windows, build_run_rules, build_runtime_var_context,
    collect_intermediate_targets, compile_rules, load_schemas, resolve_work_root,
};
use super::types::BootstrapData;

// ---------------------------------------------------------------------------
// Phase 1: load_and_compile — pure data transforms + async sink build
// ---------------------------------------------------------------------------

/// Load schemas, compile rules, validate config, build engines and sink dispatcher.
pub(super) async fn load_and_compile(
    config: &FusionConfig,
    base_dir: &Path,
) -> RuntimeResult<BootstrapData> {
    // 1. Load .wfs files → Vec<WindowSchema>
    let all_schemas = load_schemas(&config.runtime.schemas, base_dir)?;
    // Load static (provider) window declarations from the same schema files
    let _static_schemas =
        crate::lifecycle::compile::load_static_schemas(&config.runtime.schemas, base_dir)
            .unwrap_or_default();

    // 2. Preprocess .wfl with config.vars → parse → compile → Vec<RulePlan>
    let var_ctx = build_runtime_var_context(config, base_dir);
    let (all_rule_plans, effective_schemas) =
        compile_rules(&config.runtime.rules, base_dir, &var_ctx, &all_schemas)?;
    let intermediate_targets = collect_intermediate_targets(&all_rule_plans);
    let (pipeline_schemas, pipeline_window_configs) = build_pipeline_internal_windows(
        &all_rule_plans,
        &effective_schemas,
        &config.window_defaults,
    );
    let mut runtime_schemas = effective_schemas;
    runtime_schemas.extend(pipeline_schemas);
    let mut runtime_window_configs = config.windows.clone();
    runtime_window_configs.extend(pipeline_window_configs);

    // 3. Cross-validate over vs over_cap
    let window_overs: HashMap<String, Duration> = runtime_schemas
        .iter()
        .map(|ws| (ws.name.clone(), ws.over))
        .collect();
    wf_config::validate_over_vs_over_cap(&runtime_window_configs, &window_overs).source_err(
        RuntimeReason::core_conf(),
        "validate window over vs over_cap",
    )?;
    wf_debug!(
        conf,
        windows = config.windows.len(),
        "over vs over_cap validation passed"
    );

    // 4. Separate provider windows (config has table) from buffer windows
    let (buffer_schemas, _provider_schemas): (Vec<_>, Vec<_>) =
        runtime_schemas.iter().cloned().partition(|s| {
            !runtime_window_configs
                .iter()
                .any(|c| c.name == s.name && c.table.is_some())
        });
    let (buffer_configs, provider_configs): (Vec<_>, Vec<_>) = runtime_window_configs
        .iter()
        .cloned()
        .partition(|c| c.table.is_none());

    // 4a. Build buffer windows
    // Field-usage analysis: only materialize the event fields rules actually
    // read (per window), cutting the dominant peak RSS on wide windows.
    let field_usage = wf_lang::field_usage::compute_window_field_usage(&all_rule_plans);
    let window_defs = schemas_to_window_defs(&buffer_schemas, &buffer_configs, &field_usage)
        .source_err(RuntimeReason::Bootstrap, "build window definitions")?;

    // 5. WindowRegistry::build → registry (buffer windows only)
    let mut registry = WindowRegistry::build(window_defs).conv_err()?;
    register_window_miss_provider(&mut registry, &runtime_window_configs)?;

    // 5.5. Initialize wp_knowledge if knowdb.toml exists
    //      (Redis provider + [fun] registry for external(), CSV/DB tables for windows)
    let knowdb_path = base_dir.join("knowdb.toml");
    if knowdb_path.exists() {
        // Load provider windows (table=)
        if !provider_configs.is_empty() {
            load_knowledge_into_windows(&knowdb_path, base_dir, &mut registry)?;
        }
        // Initialize Redis provider (required for external() to work).
        // Non-fatal: if Redis is unavailable, engine starts in degraded mode —
        // external() calls will return Bool(false) until the backend recovers.
        init_knowledge_redis_if_configured(&knowdb_path, base_dir);
    }

    // 6. Router::new(registry)
    let router = Arc::new(Router::new(registry));
    // 6a. Configure hash-join indexes on windows targeted by rule joins, so
    //     join lookups are O(1) instead of O(rows) snapshot scans.
    configure_join_indexes(&router, &all_rule_plans);

    // 7. Build RunRules (precompute stream_name → alias routing)
    let rules = build_run_rules(&all_rule_plans, &runtime_schemas, &config.output);

    // 8. Build connector-based sink dispatcher
    let sinks_dir = base_dir.join(&config.sinks);
    let work_root = resolve_work_root(config, base_dir);
    let mut scoped_vars = config.vars.clone();
    scoped_vars
        .entry("WORK_DIR".to_string())
        .or_insert_with(|| base_dir.to_string_lossy().to_string());
    scoped_vars
        .entry("WORK_ROOT".to_string())
        .or_insert_with(|| work_root.to_string_lossy().to_string());
    let bundle_ctx = ConfigVarContext::from_explicit_vars(scoped_vars);
    let bundle =
        wf_config::sink::load_sink_config_with_context(&sinks_dir, &bundle_ctx, Some(base_dir))
            .source_err(RuntimeReason::core_conf(), "load sink config")?;
    let mut factory_registry = SinkFactoryRegistry::new();
    factory_registry.register(Arc::new(FileFactory));
    factory_registry.register(Arc::new(SyslogFactory));
    factory_registry.register(Arc::new(TcpFactory));
    factory_registry.register(Arc::new(BlackHoleFactory));
    factory_registry.import_from_global_registry();
    let window_names: Vec<String> = config.windows.iter().map(|w| w.name.clone()).collect();
    let dispatcher = Arc::new(
        match build_sink_dispatcher(&bundle, &factory_registry, &work_root, &window_names).await {
            Ok(d) => d,
            Err(e) => {
                log::error!("build sink dispatcher failed: {e:#}");
                return Err(e);
            }
        },
    );

    // Initialize external function runtime (delegates to wp_knowledge [fun] registry)
    let external_runtime = {
        let rt = Arc::new(crate::external::ExternalRuntime::default());
        wf_engine::external::set_external_handler(rt.clone());
        Some(rt)
    };

    let schema_count = runtime_schemas.len();
    Ok(BootstrapData {
        rules,
        router,
        dispatcher,
        schema_count,
        schemas: runtime_schemas,
        window_configs: runtime_window_configs,
        intermediate_targets,
        external_runtime,
    })
}

/// Build the pipe registry from the yield topology: every rule's `yield` target
/// (output or intermediate) becomes a [`wf_engine::pipe::Pipe`], carrying the
/// target window's schema (for output cropping) and retention `over`. Pipes are
/// the output/intermediate relay abstraction (pipe design, P1); input match
/// windows stay in `window/`.
pub(crate) fn build_pipe_registry(
    all_rule_plans: &[&wf_lang::plan::RulePlan],
    runtime_schemas: &[wf_lang::WindowSchema],
) -> std::sync::Arc<wf_engine::pipe::PipeRegistry> {
    use arrow::datatypes::{Schema, SchemaRef};
    use wf_engine::pipe::{Pipe, PipeRegistry};

    use crate::receiver::schema::field_to_arrow;

    let registry = PipeRegistry::new();
    for plan in all_rule_plans {
        let target = &plan.yield_plan.target;
        if registry.contains(target) {
            continue;
        }
        let (schema, over, time_col_index) = runtime_schemas
            .iter()
            .find(|ws| ws.name == *target)
            .map(|ws| {
                let fields: Vec<arrow::datatypes::Field> = ws
                    .fields
                    .iter()
                    .map(|f| field_to_arrow(&f.name, &f.field_type))
                    .collect();
                let time_col_index = ws
                    .time_field
                    .as_ref()
                    .and_then(|tf| fields.iter().position(|f| f.name() == tf));
                (
                    Arc::new(Schema::new(fields)) as SchemaRef,
                    ws.over,
                    time_col_index,
                )
            })
            .unwrap_or_else(|| (Arc::new(Schema::empty()), std::time::Duration::ZERO, None));
        registry.register(Pipe {
            name: target.clone(),
            schema,
            over,
            time_col_index,
        });
    }
    Arc::new(registry)
}

/// Configure hash-join indexes on buffer windows targeted by rule joins, so
/// join lookups are O(1) hash lookups instead of O(rows) snapshot scans.
/// Each join's first condition's right field becomes the window's join key.
fn configure_join_indexes(router: &Router, plans: &[wf_lang::plan::RulePlan]) {
    let mut keys_by_window: HashMap<String, String> = HashMap::new();
    for plan in plans {
        for join in &plan.joins {
            if let Some(cond) = join.conds.first()
                && let Some(field) = cond.right_field_name()
            {
                keys_by_window
                    .entry(join.right_window.clone())
                    .or_insert_with(|| field.to_string());
            }
        }
    }
    for (window, key_field) in keys_by_window {
        if let Some(win) = router.registry().get_window(&window) {
            win.set_join_key(key_field);
        }
    }
}

fn register_window_miss_provider(
    registry: &mut WindowRegistry,
    window_configs: &[wf_config::WindowConfig],
) -> RuntimeResult<()> {
    if registry.contains(WINDOW_MISS_WINDOW_NAME)
        || window_configs
            .iter()
            .any(|config| config.name == WINDOW_MISS_WINDOW_NAME)
    {
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail(format!(
                "window name {WINDOW_MISS_WINDOW_NAME:?} is reserved for runtime diagnostics"
            ))
            .err();
    }

    registry
        .register_provider(
            WINDOW_MISS_WINDOW_NAME.to_string(),
            ProviderWindow::new(
                WINDOW_MISS_WINDOW_NAME.to_string(),
                "internal://window_miss".to_string(),
                None,
            ),
        )
        .source_err(
            RuntimeReason::Bootstrap,
            "register __window_miss provider window",
        )?;
    Ok(())
}

/// Initialize wp_knowledge Redis provider and [fun] registry from knowdb.toml.
///
/// Non-fatal: if Redis is unreachable, logs a WARN and returns. The engine
/// continues to start — `external()` calls will fail gracefully (return
/// `Bool(false)`) until Redis becomes available and a future init attempt
/// succeeds (or the maintenance task probes and recovers).
fn init_knowledge_redis_if_configured(knowdb_path: &Path, base_dir: &Path) {
    use orion_variate::EnvDict;

    let Ok(content) = std::fs::read_to_string(knowdb_path) else {
        wf_warn!(
            conf,
            "cannot read {}, skipping Redis init",
            knowdb_path.display()
        );
        return;
    };
    let Ok(config) = toml::from_str::<toml::Value>(&content) else {
        wf_warn!(
            conf,
            "cannot parse {}, skipping Redis init",
            knowdb_path.display()
        );
        return;
    };

    // Only init if [provider.redis] is configured
    let has_redis = config
        .get("provider")
        .and_then(|p| p.get("redis"))
        .is_some();
    if !has_redis {
        return;
    }

    wf_info!(
        conf,
        "initializing wp_knowledge Redis provider for external()"
    );
    let authority_path = base_dir.join(".run").join("authority.sqlite");
    match wp_knowledge::facade::init_thread_cloned_from_knowdb(
        base_dir,
        knowdb_path,
        &format!("file:{}?mode=rwc&uri=true", authority_path.display()),
        &EnvDict::default(),
    ) {
        Ok(()) => {}
        Err(e) => {
            wf_warn!(
                conf,
                error = %e,
                "Redis init failed; external() will return Bool(false) until backend recovers"
            );
        }
    }
}

/// Load knowdb CSV tables directly into matching static windows.
fn load_knowledge_into_windows(
    knowdb_path: &Path,
    _base_dir: &Path,
    registry: &mut WindowRegistry,
) -> RuntimeResult<()> {
    use wf_engine::match_engine::Value as EngineValue;

    let content = std::fs::read_to_string(knowdb_path).source_err(
        RuntimeReason::Bootstrap,
        format!("read {}", knowdb_path.display()),
    )?;
    let config: toml::Value = toml::from_str(&content).source_raw_err(
        RuntimeReason::Bootstrap,
        format!("parse {}", knowdb_path.display()),
    )?;

    let tables = config.get("tables").and_then(|t| t.as_array());
    let Some(tables) = tables else {
        return Ok(());
    };

    // Try PG provider if configured
    let use_pg = config
        .get("provider")
        .and_then(|p| p.get("kind"))
        .and_then(|k| k.as_str())
        .map(|k| k == "postgres")
        .unwrap_or(false);

    if use_pg {
        if let Err(e) = load_from_postgres(&config, tables, registry) {
            wf_warn!(conf, error = %e, "PG knowledge load failed, falling back to CSV");
        } else {
            return Ok(());
        }
    }

    // CSV fallback
    let base = config
        .get("base_dir")
        .and_then(|b| b.as_str())
        .unwrap_or(".");
    let data_base_dir = knowdb_path.parent().unwrap_or(Path::new(".")).join(base);

    for table in tables {
        let name = table.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let enabled = table
            .get("enabled")
            .and_then(|e| e.as_bool())
            .unwrap_or(true);
        if !enabled || name.is_empty() {
            continue;
        }

        let dir = table.get("dir").and_then(|d| d.as_str()).unwrap_or(name);
        let data_file = table
            .get("data_file")
            .and_then(|d| d.as_str())
            .unwrap_or("data.csv");
        let csv_path = data_base_dir.join(dir).join(data_file);
        if !csv_path.exists() {
            continue;
        }

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_path(&csv_path)
            .map_err(|e| {
                RuntimeReason::Bootstrap.to_err().with_detail(format!(
                    "open csv {}: {}",
                    csv_path.display(),
                    e
                ))
            })?;

        let headers: Vec<String> = reader
            .headers()
            .map_err(|e| {
                RuntimeReason::Bootstrap
                    .to_err()
                    .with_detail(format!("csv headers: {}", e))
            })?
            .iter()
            .map(|h| h.to_string())
            .collect();

        let mut rows: Vec<std::collections::HashMap<String, EngineValue>> =
            Vec::with_capacity(1024);
        for result in reader.records() {
            let record = result.map_err(|e| {
                RuntimeReason::Bootstrap
                    .to_err()
                    .with_detail(format!("csv row: {}", e))
            })?;
            let mut map = std::collections::HashMap::new();
            for (i, value) in record.iter().enumerate() {
                let field = headers
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("col_{}", i));
                map.insert(field, EngineValue::Str(value.to_string().into()));
            }
            rows.push(map);
        }
        if rows.is_empty() {
            continue;
        }

        let row_count = rows.len();
        let mut pw = wf_engine::window::ProviderWindow::new(
            name.to_string(),
            format!("SELECT * FROM {}", name),
            None,
        );
        pw.load(rows);
        registry
            .register_provider(name.to_string(), pw)
            .source_err(RuntimeReason::Bootstrap, "register provider window")?;
        wf_info!(conf, table = %name, rows = row_count, "knowdb data loaded");
    }
    Ok(())
}

fn load_from_postgres(
    config: &toml::Value,
    tables: &[toml::Value],
    registry: &mut WindowRegistry,
) -> RuntimeResult<()> {
    use wf_engine::match_engine::Value as EngineValue;

    let provider = config.get("provider").expect("provider section checked");
    let uri = provider
        .get("connection_uri")
        .and_then(|u| u.as_str())
        .unwrap_or("");
    let pool_size = provider
        .get("pool_size")
        .and_then(|p| p.as_integer())
        .unwrap_or(4) as u32;

    wp_knowledge::facade::init_postgres_provider(uri, Some(pool_size)).map_err(|e| {
        RuntimeReason::Bootstrap
            .to_err()
            .with_detail(format!("init PG provider: {}", e))
    })?;

    for table in tables {
        let name = table.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let enabled = table
            .get("enabled")
            .and_then(|e| e.as_bool())
            .unwrap_or(true);
        if !enabled || name.is_empty() {
            continue;
        }

        let sql = format!("SELECT * FROM {}", name);
        let result = wp_knowledge::facade::query(&sql).map_err(|e| {
            RuntimeReason::Bootstrap
                .to_err()
                .with_detail(format!("PG query {}: {}", name, e))
        })?;

        let mut rows: Vec<std::collections::HashMap<String, EngineValue>> =
            Vec::with_capacity(result.len());
        for row in &result {
            let mut map = std::collections::HashMap::new();
            for field in row.iter() {
                map.insert(
                    field.name.to_string(),
                    EngineValue::Str(field.value.to_string().into()),
                );
            }
            rows.push(map);
        }
        if rows.is_empty() {
            continue;
        }

        let row_count = rows.len();
        let mut pw = wf_engine::window::ProviderWindow::new(name.to_string(), sql.clone(), None);
        pw.load(rows);
        registry
            .register_provider(name.to_string(), pw)
            .source_err(RuntimeReason::Bootstrap, "register provider window")?;
        wf_info!(conf, table = %name, rows = row_count, "knowdb data loaded from PG");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    use wf_lang::ast::{CloseMode, Expr, MatchMode};
    use wf_lang::plan::{EntityPlan, MatchPlan, RulePlan, ScorePlan, WindowSpec, YieldPlan};

    fn minimal_plan(name: &str, target: &str) -> RulePlan {
        RulePlan {
            conv_window: None,
            name: name.into(),
            binds: vec![],
            lets: Vec::new(),
            match_plan: MatchPlan {
                keys: vec![],
                key_map: None,
                key_join: None,
                window_spec: WindowSpec::Sliding(Duration::from_secs(60)),
                event_steps: vec![],
                close_steps: vec![],
                close_mode: CloseMode::Or,
                tracked_bind_aliases: HashSet::new(),
                tracked_bind_fields: HashMap::new(),
                tracked_plain_fields: HashSet::new(),
                seq: None,
                match_mode: MatchMode::Seq,
                accu: false,
                needs_field_history: false,
            },
            each_plan: None,
            joins: vec![],
            r#where: None,
            entity_plan: EntityPlan {
                entity_type: "ip".into(),
                entity_id_expr: Expr::Bool(false),
            },
            yield_plan: YieldPlan {
                target: target.into(),
                version: None,
                fields: vec![],
            },
            score_plan: ScorePlan {
                expr: Expr::Number(1.0),
            },
            pattern_origin: None,
            conv_plan: None,
            limits_plan: None,
        }
    }

    fn window_schema(name: &str, over: Duration, field: &str) -> wf_lang::WindowSchema {
        wf_lang::WindowSchema {
            name: name.into(),
            streams: vec![],
            time_field: Some(field.into()),
            over,
            fields: vec![wf_lang::FieldDef {
                name: field.into(),
                field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Chars),
            }],
        }
    }

    #[test]
    fn build_pipe_registry_extracts_schema_and_over() {
        let plans = [
            minimal_plan("r1", "alerts"),
            minimal_plan("r2", "__wf_pipe_x"),
        ];
        let plans_ref: Vec<_> = plans.iter().collect();
        let schemas = vec![
            window_schema("alerts", Duration::ZERO, "sip"),
            window_schema("__wf_pipe_x", Duration::from_secs(60), "ev_count"),
        ];

        let reg = build_pipe_registry(&plans_ref, &schemas);

        assert!(reg.contains("alerts"));
        let alerts = reg.get("alerts").expect("alerts pipe");
        assert_eq!(alerts.over, Duration::ZERO);
        assert_eq!(alerts.schema.fields().len(), 1);
        assert_eq!(alerts.schema.fields()[0].name(), "sip");

        let pipe_x = reg.get("__wf_pipe_x").expect("pipeline pipe");
        assert_eq!(pipe_x.over, Duration::from_secs(60));
        assert_eq!(pipe_x.schema.fields()[0].name(), "ev_count");
        // time_field → time_col_index points at it (user-named intermediates too).
        assert_eq!(pipe_x.time_col_index, Some(0));
    }

    #[test]
    fn build_pipe_registry_dedups_yield_targets() {
        // Two rules yielding the same target → the pipe is registered once.
        let plans = [minimal_plan("r1", "alerts"), minimal_plan("r2", "alerts")];
        let plans_ref: Vec<_> = plans.iter().collect();
        let reg = build_pipe_registry(&plans_ref, &[]);

        assert!(reg.contains("alerts"));
        assert_eq!(
            reg.iter().len(),
            1,
            "duplicate yield targets must dedup to one pipe"
        );
    }

    #[test]
    fn build_pipe_registry_unknown_target_gets_empty_schema() {
        // A yield target with no matching window schema falls back to an empty
        // schema + zero over (not a hard failure).
        let plans = [minimal_plan("r1", "orphan_target")];
        let plans_ref: Vec<_> = plans.iter().collect();
        let reg = build_pipe_registry(&plans_ref, &[]);

        let pipe = reg.get("orphan_target").expect("orphan pipe");
        assert_eq!(pipe.over, Duration::ZERO);
        assert!(pipe.schema.fields().is_empty());
    }
}
