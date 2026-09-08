use orion_error::conversion::ToStructError;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use orion_error::prelude::*;
use wp_core_connectors::sinks::blackhole_factory::BlackHoleFactory;
use wp_core_connectors::sinks::file_factory::FileFactory;
use wp_core_connectors::sinks::syslog::SyslogFactory;
use wp_core_connectors::sinks::tcp::TcpFactory;

use wf_config::ConfigVarContext;
use wf_config::FusionConfig;
use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
use wf_engine::match_engine::Value as EngineValue;
use wf_engine::window::{ProviderWindow, Router, WindowRegistry};
use wf_lang::WindowSchema;
use wf_lang::{BaseType, FieldDef, FieldType};

use crate::error::{RuntimeReason, RuntimeResult};
use crate::receiver::miss::WINDOW_MISS_WINDOW_NAME;
use crate::schema_bridge::schemas_to_window_defs;
use crate::sink_build::{SinkFactoryRegistry, build_sink_dispatcher};

use super::compile::{
    build_pipeline_internal_windows, build_run_rules, build_runtime_var_context,
    collect_intermediate_targets, compile_rules, load_schemas, resolve_work_root,
};
use super::types::BootstrapData;

/// 全局周期基线供给（PG）的命名 provider：引擎把 knowdb.conf 扁平
/// `[provider] kind="postgres"` 安装为 wp_knowledge 命名 provider，boot 装载与
/// 周期刷新（`NamedSql` 规格）都经它路由（见 `load_from_postgres`）。
const ENGINE_PG_PROVIDER: &str = "engine_pg";

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

    // 近端 B（S2-M2/B-b/相位同窗 2026-09-08）：先按 runtime 配置安装共享
    // BaselineStore（即使无 warm 文件，judge 也需要正确 k/decay/相位参数）；
    // 然后 warm（如有）。文件缺失/解析失败直接报错——静默空历史会让 judge 全部
    // 不告警，难排查。
    let phase = resolve_baseline_phase(
        &config.runtime.baseline_history_phase_period,
        &config.runtime.baseline_history_phase_bucket,
    )?;
    install_baseline_store(
        config.runtime.baseline_history_k,
        config.runtime.baseline_history_decay,
        phase,
    );
    warm_baseline_history(
        config.runtime.baseline_history.as_deref(),
        config.runtime.baseline_history_k,
        config.runtime.baseline_history_decay,
        phase,
        base_dir,
    )?;

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

    // 3a. 诊断模式（--perf-diag diag=true）：注入内置 __wf_sentinel 窗口。
    //     哨兵帧（wfgen 帧尾追加，tag=__wf_sentinel）路由进该窗口，由独立哨兵
    //     任务消费（写四元组记录 + 驱动诊断点状态机）。不依赖用户 .wfs。
    if crate::perf_diag::perf_diag_enabled() {
        inject_sentinel_window(&mut runtime_schemas, &mut runtime_window_configs)?;
    }

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
    // 2026-08-30: knowdb.toml 允许放 models/schemas/（与 windows.toml/schemas 同目录，
    // nexmark_pk 已迁移）；根目录旧位保留作向后兼容回退。
    if let Some(knowdb_path) = find_knowdb_path(base_dir) {
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
/// 2026-08-30 多 key：收集每个窗口的**全部去重** join 右字段（多个规则以不同
/// key join 同一窗口时各自建索引），不再首键独占。
fn configure_join_indexes(router: &Router, plans: &[wf_lang::plan::RulePlan]) {
    let mut keys_by_window: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
    for plan in plans {
        for join in &plan.joins {
            if let Some(cond) = join.conds.first()
                && let Some(field) = cond.right_field_name()
            {
                keys_by_window
                    .entry(join.right_window.clone())
                    .or_default()
                    .insert(field.to_string());
            }
        }
    }
    for (window, keys) in keys_by_window {
        if let Some(win) = router.registry().get_window(&window) {
            for key_field in keys {
                win.set_join_key(key_field);
            }
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

/// 注入内置哨兵窗口（诊断模式，`--perf-diag diag=true`）。
///
/// `__wf_sentinel` 是保留窗口名：用户 .wfs/windows.toml 不得声明同名窗口
/// （与 `__window_miss` 同级的保留名）。schema 固定 `{round, n, start_ns}`（
/// digit → Int64，`start_ns` 为 epoch nanos，f64 会丢精度）；无时间列——窗口
/// 不推进水位、不拒绝迟到（`append_with_watermark` 对无时间列窗口的处理）。
fn inject_sentinel_window(
    runtime_schemas: &mut Vec<WindowSchema>,
    runtime_window_configs: &mut Vec<WindowConfig>,
) -> RuntimeResult<()> {
    use crate::perf_diag::{PERF_SENTINEL_STREAM, PERF_SENTINEL_WINDOW};

    if runtime_schemas
        .iter()
        .any(|ws| ws.name == PERF_SENTINEL_WINDOW)
        || runtime_window_configs
            .iter()
            .any(|c| c.name == PERF_SENTINEL_WINDOW)
    {
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail(format!(
                "window name {PERF_SENTINEL_WINDOW:?} is reserved for perf-diag diagnostics"
            ))
            .err();
    }

    runtime_schemas.push(WindowSchema {
        name: PERF_SENTINEL_WINDOW.to_string(),
        streams: vec![PERF_SENTINEL_STREAM.to_string()],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "round".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "n".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "start_ns".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
        ],
    });
    runtime_window_configs.push(WindowConfig {
        name: PERF_SENTINEL_WINDOW.to_string(),
        mode: DistMode::Local,
        max_window_bytes: (16 * 1024 * 1024).into(),
        over_cap: Duration::from_secs(3600).into(),
        evict_policy: EvictPolicy::TimeFirst,
        watermark: Duration::from_secs(1).into(),
        allowed_lateness: Duration::from_secs(0).into(),
        late_policy: LatePolicy::Drop,
        table: None,
    });
    Ok(())
}

/// Locate knowdb.toml：优先 `models/schemas/knowdb.toml`（与 windows.toml/schemas 同
/// 目录，nexmark_pk 2026-08-30 迁移位置），回退根目录 `knowdb.toml`（历史位置）。
/// 都找不到返回 None（无静态表/外部函数，正常跳过加载）。
fn find_knowdb_path(base_dir: &Path) -> Option<PathBuf> {
    let candidates = [
        base_dir.join("models/schemas/knowdb.toml"),
        base_dir.join("knowdb.toml"),
    ];
    candidates.into_iter().find(|p| p.exists())
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

/// Infer a typed engine value from a raw knowdb cell (CSV / PG string).
///
/// Numeric-looking cells become `Number` so numeric side-input columns can
/// join numeric expressions (q13: `mod(auction,10000) = side_input.key` —
/// the loader used to store every column as `Str`, so the provider join
/// index key (`JoinKey::Str`) never matched the lookup key
/// (`JoinKey::Number`), every lookup missed the index and fell back to an
/// O(rows) scan → q13b 卡死). `true`/`false` become `Bool`; everything else
/// stays `Str`.
fn infer_knowledge_value(cell: &str) -> EngineValue {
    if let Ok(n) = cell.parse::<f64>()
        && n.is_finite()
    {
        return EngineValue::Number(n);
    }
    match cell {
        "true" => EngineValue::Bool(true),
        "false" => EngineValue::Bool(false),
        _ => EngineValue::Str(cell.to_string().into()),
    }
}

/// 一个待装载的引擎 CSV 表（KnowDB authority 单表）。
struct CsvTable {
    name: String,
    csv_path: PathBuf,
    refresh: Option<Duration>,
    /// (列名, 是否整列数值) —— 数值列生成 REAL，其余 TEXT。
    cols: Vec<(String, bool)>,
}

/// Load knowdb tables into matching static (provider) windows.
///
/// CSV 供给（v1）全部经 **KnowDB loader**（wp_knowledge V2 authority）装载与
/// 刷新——引擎不再自行解析 CSV（`read_knowledge_csv` 已退役，2026-09-07）：
/// bootstrap 逐列探测类型（整列每个非空单元可解析有限 f64 → REAL，否则 TEXT）
/// 在 `base_dir/.run/knowdb_providers/` 生成类型化 create.sql/insert.sql 与派生
/// V2 conf，调 `loader::reload_table_rows` 取 DDL 类型化原生行，边界转引擎行。
/// PG 供给走 [`load_from_postgres`]（boot + 表级 refresh 经 NamedSql 刷新）。
fn load_knowledge_into_windows(
    knowdb_path: &Path,
    base_dir: &Path,
    registry: &mut WindowRegistry,
) -> RuntimeResult<()> {
    crate::lifecycle::provider_refresh::reset_specs();
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

    // -----------------------------------------------------------------------
    // CSV fallback —— KnowDB authority 单表装载（loader 语义，见上 doc）
    // -----------------------------------------------------------------------
    let base = config
        .get("base_dir")
        .and_then(|b| b.as_str())
        .unwrap_or(".");
    let data_base_dir = knowdb_path.parent().unwrap_or(Path::new(".")).join(base);

    let mut found: Vec<CsvTable> = Vec::new();
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
        let cols = sniff_csv_schema(&csv_path)?;
        if cols.is_empty() {
            continue;
        }
        found.push(CsvTable {
            name: name.to_string(),
            csv_path,
            refresh: parse_knowledge_refresh(table),
            cols,
        });
    }
    if found.is_empty() {
        return Ok(());
    }

    // 派生 KnowDB V2 资产（root conf + 每表类型化 DDL），权威库文件共用。
    let assets = base_dir.join(".run").join("knowdb_providers");
    write_derived_knowdb_assets(&assets, &found)?;
    let authority_uri = format!("file:{}", assets.join("authority.sqlite").display());
    let conf_rel = PathBuf::from("knowdb.toml");
    let dict = orion_variate::EnvDict::default();

    for t in &found {
        let native = wp_knowledge::loader::reload_table_rows(
            &assets,
            &conf_rel,
            &authority_uri,
            &t.name,
            &dict,
        )
        .source_err(
            RuntimeReason::Bootstrap,
            format!("knowdb reload table {}", t.name),
        )?;
        if native.is_empty() {
            continue;
        }
        let rows = engine_rows_from_knowdb(native);
        let row_count = rows.len();
        let mut pw = ProviderWindow::new(
            t.name.clone(),
            format!("SELECT * FROM {}", t.name),
            t.refresh,
        );
        pw.load(rows);
        registry
            .register_provider(t.name.clone(), pw)
            .source_err(RuntimeReason::Bootstrap, "register provider window")?;
        wf_info!(
            conf,
            table = %t.name,
            rows = row_count,
            refresh = t.refresh.map(|d| d.as_secs()),
            "knowdb data loaded"
        );
        if let Some(interval) = t.refresh {
            crate::lifecycle::provider_refresh::register_spec(wp_knowledge::refresh::RefreshSpec {
                name: t.name.clone(),
                interval,
                source: wp_knowledge::refresh::RefreshSource::Authority {
                    root: assets.clone(),
                    conf: conf_rel.clone(),
                    authority_uri: authority_uri.clone(),
                    table: t.name.clone(),
                },
            });
        }
    }
    Ok(())
}

/// 探测 CSV 列（顺序 = 表头序）与列数值性：某列每个**非空**单元都能解析为
/// 有限 f64 → 数值列（派生 DDL 用 REAL），否则文本列（TEXT）。空单元不拖累
/// 数值列（老引擎对空单元给 Str('')，与数值共存时按列型会产生歧义，这里数值
/// 列仅由非空样本判定）。空文件（仅表头）也返回列清单。
fn sniff_csv_schema(csv_path: &Path) -> RuntimeResult<Vec<(String, bool)>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(csv_path)
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
    if headers.is_empty() {
        return Ok(Vec::new());
    }
    let mut numeric = vec![true; headers.len()];
    for result in reader.records() {
        let record = result.map_err(|e| {
            RuntimeReason::Bootstrap
                .to_err()
                .with_detail(format!("csv row: {}", e))
        })?;
        for (i, cell) in record.iter().enumerate() {
            if i >= numeric.len() {
                break;
            }
            let cell = cell.trim();
            if cell.is_empty() {
                continue; // 空单元不参与列型判定
            }
            if numeric[i] && !(cell.parse::<f64>().map(|n| n.is_finite()).unwrap_or(false)) {
                numeric[i] = false;
            }
        }
    }
    Ok(headers.into_iter().zip(numeric).collect())
}

/// 写派生 KnowDB V2 资产：`<root>/knowdb.toml`（每表一条 [[tables]]，
/// `columns.by_header` 表头序 + 绝对 `data_file`）与 `<root>/<table>/` 下的
/// 类型化 create.sql/insert.sql（列名直写，loader 的 `{{table}}` 替换为空转）。
/// 每次启动全量重写（确定性）：权威库若有旧 schema，DDL 走 IF NOT EXISTS，
/// 换列需清 `.run/knowdb_providers/authority.sqlite`。
fn write_derived_knowdb_assets(root: &Path, tables: &[CsvTable]) -> RuntimeResult<()> {
    std::fs::create_dir_all(root).source_raw_err(
        RuntimeReason::Bootstrap,
        format!("create {}", root.display()),
    )?;

    let mut conf = String::from(
        "version = 2\nbase_dir = \".\"\n\n[default]\ntransaction = true\nbatch_size = 2000\non_error = \"fail\"\n\n[csv]\nhas_header = true\ndelimiter = \",\"\nencoding = \"utf-8\"\ntrim = true\n",
    );
    for t in tables {
        let cols = t
            .cols
            .iter()
            .map(|(n, _)| format!("\"{}\"", n.replace('"', "\\\"")))
            .collect::<Vec<_>>()
            .join(", ");
        conf.push_str(&format!(
            "\n[[tables]]\nname = \"{}\"\ndir = \"{}\"\nenabled = true\ndata_file = \"{}\"\ncolumns.by_header = [{}]\n",
            t.name.replace('"', "\\\""),
            t.name.replace('"', "\\\""),
            t.csv_path.display().to_string().replace('"', "\\\""),
            cols
        ));
    }
    std::fs::write(root.join("knowdb.toml"), conf).source_raw_err(
        RuntimeReason::Bootstrap,
        format!("write {}", root.join("knowdb.toml").display()),
    )?;

    for t in tables {
        let table_dir = root.join(&t.name);
        std::fs::create_dir_all(&table_dir).source_raw_err(
            RuntimeReason::Bootstrap,
            format!("create {}", table_dir.display()),
        )?;
        let mut create = String::from("CREATE TABLE IF NOT EXISTS ");
        create.push_str(&t.name);
        create.push_str(" (\n");
        for (i, (col, num)) in t.cols.iter().enumerate() {
            if i > 0 {
                create.push_str(",\n");
            }
            create.push_str(&format!("  {} {}", col, if *num { "REAL" } else { "TEXT" }));
        }
        create.push_str("\n);\n");
        std::fs::write(table_dir.join("create.sql"), create).source_raw_err(
            RuntimeReason::Bootstrap,
            format!("write create.sql for {}", t.name),
        )?;

        let cols = t
            .cols
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = (1..=t.cols.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert = format!(
            "INSERT INTO {} ({}) VALUES ({});\n",
            t.name, cols, placeholders
        );
        std::fs::write(table_dir.join("insert.sql"), insert).source_raw_err(
            RuntimeReason::Bootstrap,
            format!("write insert.sql for {}", t.name),
        )?;
    }
    Ok(())
}

/// 引擎行边界转换：KnowDB 原生行（`RowData` = 列 → 值，DDL 类型化）→ 引擎
/// `Value` 行。值语义与 PG 路径一致：`Bool` 直接映射，其余经 Display 走
/// [`infer_knowledge_value`]（数字文本 → Number，true/false → Bool，其余 Str），
/// 与老引擎逐单元推断结果一致（2026-08-23 q13 契约）。
pub(super) fn engine_rows_from_knowdb(
    rows: Vec<wp_knowledge::mem::RowData>,
) -> Vec<std::collections::HashMap<String, EngineValue>> {
    rows.into_iter()
        .map(|row| {
            let mut map = HashMap::new();
            for field in row {
                map.insert(
                    field.get_name().to_string(),
                    match field.get_value() {
                        wp_model_core::model::Value::Null => EngineValue::Str(String::new().into()),
                        wp_model_core::model::Value::Bool(b) => EngineValue::Bool(*b),
                        other => infer_knowledge_value(&other.to_string()),
                    },
                );
            }
            map
        })
        .collect()
}

/// 解析 knowdb [[tables]] 的 `refresh` 键（如 `"5m"`）为刷新周期。
fn parse_knowledge_refresh(table: &toml::Value) -> Option<Duration> {
    let raw = table.get("refresh")?.as_str()?;
    raw.parse::<wf_config::HumanDuration>()
        .ok()
        .map(|d| d.as_duration())
}

/// 解析近端 B 相位配置：`period`/`bucket` 必须成对、且 `0 < bucket ≤ period`。
fn resolve_baseline_phase(
    period: &Option<wf_config::HumanDuration>,
    bucket: &Option<wf_config::HumanDuration>,
) -> RuntimeResult<Option<wf_engine::baseline::Phase>> {
    match (period, bucket) {
        (Some(p), Some(b)) => {
            let period_nanos = p.as_duration().as_nanos();
            let bucket_nanos = b.as_duration().as_nanos();
            if bucket_nanos == 0 || period_nanos < bucket_nanos {
                return RuntimeReason::core_conf()
                    .to_err()
                    .with_detail(
                        "baseline_history_phase：bucket 须 >0 且 ≤ period（如 period=7d bucket=5m）"
                            .to_string(),
                    )
                    .err();
            }
            Ok(Some(wf_engine::baseline::Phase {
                period_nanos: period_nanos as u64,
                bucket_nanos: bucket_nanos as u64,
            }))
        }
        (None, None) => Ok(None),
        _ => RuntimeReason::core_conf()
            .to_err()
            .with_detail("baseline_history_phase_period/bucket 须成对设置（相位模式）".to_string())
            .err(),
    }
}

/// 按 runtime 配置安装共享 BaselineStore（幂等：被占用且参数一致 → 静默空转；
/// 参数不符 → 提示用既有实例）。无 warm 的 judge 也需要正确 k/decay/相位。
fn install_baseline_store(k: usize, decay: bool, phase: Option<wf_engine::baseline::Phase>) {
    let installed_ok = match phase {
        Some(ph) => wf_engine::baseline::install_phased(k, decay, ph),
        None => wf_engine::baseline::install(k, decay),
    };
    let store = wf_engine::baseline::store();
    if !installed_ok || store.k() != k || store.decayed() != decay || store.phase() != phase {
        wf_warn!(
            conf,
            k,
            decay,
            phase_period_ns = phase.map(|p| p.period_nanos),
            phase_bucket_ns = phase.map(|p| p.bucket_nanos),
            "baseline store 已被占用或参数不符，使用既有实例（append/warm 照常写入）"
        );
    }
}

/// 近端 B warm（S2-M2）：读 t_baseline 逐窗 CSV 装载共享 BaselineStore。
/// CSV 头：`entity,metric,win_start,win_end,n,sum,sum_sq`（win_* = epoch
/// 纳秒整数）。见 baseline-online-design.md §11 S2-M2。
fn warm_baseline_history(
    path: Option<&str>,
    k: usize,
    decay: bool,
    phase: Option<wf_engine::baseline::Phase>,
    base_dir: &Path,
) -> RuntimeResult<()> {
    let Some(rel) = path else {
        return Ok(());
    };
    // 兼容直接单测/独立调用：先确保 store 参数正确（load_and_compile 已装 → 幂等空转）。
    install_baseline_store(k, decay, phase);
    let full = base_dir.join(rel);
    let content = std::fs::read_to_string(&full).source_err(
        RuntimeReason::Bootstrap,
        format!("read baseline history {}", full.display()),
    )?;
    let mut lines = content.lines();
    let Some(header_line) = lines.next() else {
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail("baseline history csv 为空".to_string())
            .err();
    };
    // BOM 容错（Excel 导出常带 \u{feff}）。
    let header_line = header_line.trim_start_matches('\u{feff}');
    let header: Vec<&str> = header_line.split(',').map(|s| s.trim()).collect();
    let col = |name: &str| -> RuntimeResult<usize> {
        header.iter().position(|h| *h == name).ok_or_else(|| {
            RuntimeReason::Bootstrap.to_err().with_detail(format!(
                "baseline history csv 缺列 '{name}'：{header_line:?}"
            ))
        })
    };
    let (i_ent, i_met, i_ws, i_we, i_n, i_s, i_ss) = (
        col("entity")?,
        col("metric")?,
        col("win_start")?,
        col("win_end")?,
        col("n")?,
        col("sum")?,
        col("sum_sq")?,
    );

    let store = wf_engine::baseline::store();
    let mut rows = 0u64;
    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let field: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        let at = |i: usize| field.get(i).copied().unwrap_or("");
        let parse_i64 = |i: usize, what: &str| -> RuntimeResult<i64> {
            at(i).parse::<i64>().map_err(|_| {
                RuntimeReason::Bootstrap.to_err().with_detail(format!(
                    "baseline history csv 第 {} 行列 '{what}' 非法：{:?}",
                    rows + 2,
                    at(i)
                ))
            })
        };
        let parse_f64 = |i: usize, what: &str| -> RuntimeResult<f64> {
            at(i).parse::<f64>().map_err(|_| {
                RuntimeReason::Bootstrap.to_err().with_detail(format!(
                    "baseline history csv 第 {} 行列 '{what}' 非法：{:?}",
                    rows + 2,
                    at(i)
                ))
            })
        };
        let entity = at(i_ent);
        let metric = at(i_met);
        if entity.is_empty() || metric.is_empty() {
            continue; // 空隔离键行防御性跳过（不建空键基线）
        }
        let window = wf_engine::baseline::BaselineWindow {
            win_start_nanos: parse_i64(i_ws, "win_start")?,
            win_end_nanos: parse_i64(i_we, "win_end")?,
            n: parse_f64(i_n, "n")?,
            sum: parse_f64(i_s, "sum")?,
            sum_sq: parse_f64(i_ss, "sum_sq")?,
        };
        store.append(entity, metric, window);
        rows += 1;
    }
    wf_info!(conf, path = %full.display(), rows, k, "baseline history warm loaded");
    Ok(())
}

fn load_from_postgres(
    config: &toml::Value,
    tables: &[toml::Value],
    registry: &mut WindowRegistry,
) -> RuntimeResult<()> {
    let provider = config.get("provider").expect("provider section checked");
    let uri = provider
        .get("connection_uri")
        .and_then(|u| u.as_str())
        .unwrap_or("");
    let pool_size = provider
        .get("pool_size")
        .and_then(|p| p.as_integer())
        .unwrap_or(4) as u32;

    // 安装命名 PG provider（幂等）：boot 查询用同步 `query_for`，周期刷新用
    // `NamedSql` 规格的 `query_async_for`，两条路径路由同一 provider。
    if !wp_knowledge::facade::provider_exists(ENGINE_PG_PROVIDER) {
        wp_knowledge::facade::init_postgres_provider_named_uri(
            ENGINE_PG_PROVIDER,
            uri,
            Some(pool_size),
        )
        .map_err(|e| {
            RuntimeReason::Bootstrap
                .to_err()
                .with_detail(format!("init PG provider {}: {}", ENGINE_PG_PROVIDER, e))
        })?;
    }

    for table in tables {
        let name = table.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let enabled = table
            .get("enabled")
            .and_then(|e| e.as_bool())
            .unwrap_or(true);
        if !enabled || name.is_empty() {
            continue;
        }

        // 供给查询：默认 SELECT * FROM <name>；可用表级 `query` 覆盖——PG 模式下
        // 直接聚合事实源（如 baseline_records GROUP BY entity），不再需要外部
        // 中转供给表（2026-09-08，见 pg/baseline_records.sql 说明）。
        // 动态变量：表级 phase_period_s/phase_bucket_s/retention 三键齐全时，
        // query 模板的 $cur/$next/$max_age 由引擎现算（A 通道处理时间近似：
        // 当前/下一相位 + 写死的保留期），boot 与每次刷新同源渲染。
        let sql_template = table
            .get("query")
            .and_then(|q| q.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("SELECT * FROM {}", name));
        // 供给动态变量代码（[[tables]].code，VEL——见 wp_knowledge::vel）：
        // 每行 `$name = 字面量/函数`，由 knowdb 每次刷新按自身时钟求值并替换
        // SQL 里的 `$name`；引擎只读透传。boot 与每次刷新同源渲染。
        let code = table
            .get("code")
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();
        let boot_sql = if code.trim().is_empty() {
            sql_template.clone()
        } else {
            crate::lifecycle::provider_refresh::render_supply_sql(&sql_template, &code).map_err(
                |e| {
                    RuntimeReason::Bootstrap
                        .to_err()
                        .with_detail(format!("PG supply code {}: {}", name, e))
                },
            )?
        };
        let native =
            wp_knowledge::facade::query_for(ENGINE_PG_PROVIDER, &boot_sql).map_err(|e| {
                RuntimeReason::Bootstrap
                    .to_err()
                    .with_detail(format!("PG query {}: {}", name, e))
            })?;
        if native.is_empty() {
            continue;
        }
        let rows = engine_rows_from_knowdb(native);

        // 周期刷新（S2-M3c-PG）：knowdb [[tables]] `refresh` → NamedSql 规格，
        // daemon 下由 RefreshService 周期重跑 SELECT 并搬入本 provider 窗。
        let refresh = parse_knowledge_refresh(table);
        let row_count = rows.len();
        let mut pw =
            wf_engine::window::ProviderWindow::new(name.to_string(), sql_template.clone(), refresh);
        pw.load(rows);
        registry
            .register_provider(name.to_string(), pw)
            .source_err(RuntimeReason::Bootstrap, "register provider window")?;
        wf_info!(
            conf,
            table = %name,
            rows = row_count,
            refresh = refresh.map(|d| d.as_secs()),
            code = !code.is_empty(),
            "knowdb data loaded from PG"
        );
        if let Some(interval) = refresh {
            crate::lifecycle::provider_refresh::register_spec(wp_knowledge::refresh::RefreshSpec {
                name: name.to_string(),
                interval,
                source: wp_knowledge::refresh::RefreshSource::NamedSql {
                    provider: ENGINE_PG_PROVIDER.to_string(),
                    sql: sql_template,
                    code,
                },
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    use wf_lang::ast::{CloseMode, Expr, MatchMode};
    use wf_lang::plan::{EntityPlan, MatchPlan, RulePlan, ScorePlan, WindowSpec, YieldPlan};

    #[test]
    fn infer_knowledge_value_types_numeric_bool_and_string() {
        // 2026-08-23 q13：CSV 全列 Str → 侧输入数字列 join 数字表达式时索引键
        // 类型不匹配（Str vs Number）→ 每次 lookup miss → 回退全表扫描卡死。
        // 类型推断后数字列必须进 Number，join 索引才命中。
        assert_eq!(
            infer_knowledge_value("2345"),
            EngineValue::Number(2345.0),
            "整数字符串 → Number"
        );
        assert_eq!(
            infer_knowledge_value("1.5"),
            EngineValue::Number(1.5),
            "浮点字符串 → Number"
        );
        assert_eq!(
            infer_knowledge_value("true"),
            EngineValue::Bool(true),
            "true → Bool"
        );
        assert_eq!(
            infer_knowledge_value("false"),
            EngineValue::Bool(false),
            "false → Bool"
        );
        assert_eq!(
            infer_knowledge_value("value-0"),
            EngineValue::Str("value-0".into()),
            "非数字字符串保持 Str"
        );
        assert_eq!(
            infer_knowledge_value(""),
            EngineValue::Str("".into()),
            "空串保持 Str"
        );
        assert_eq!(
            infer_knowledge_value("NaN"),
            EngineValue::Str("NaN".into()),
            "非有限数保持 Str（防 NaN/Inf 混入 Number）"
        );
    }

    fn minimal_plan(name: &str, target: &str) -> RulePlan {
        RulePlan {
            conv_window: None,
            name: name.into(),
            binds: vec![],
            lets: Vec::new(),
            match_plan: MatchPlan {
                keys: vec![],
                key_exprs: Vec::new(),
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
                trigger_event_needed: false,
            },
            each_plan: None,
            stats_plan: None,
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

    #[test]
    fn pg_supply_code_rendering_is_passthrough_at_engine_side() {
        // 引擎对供给 code 只做透传 + boot 渲染（值计算/替换在 wp_knowledge）。
        // 这里验证引擎入口渲染函数与 knowdb 同源（固定时刻语义已在 wp_knowledge
        // 单测锁定，此处只验证 code 原样进入渲染、非法 code 报错）。
        let code = "$cur = cur_phase_bucket(240, 15)\n$next = next_phase_bucket(240, 15)";
        let sql = "WHERE phase_bucket IN ('$cur','$next')";
        let rendered =
            crate::lifecycle::provider_refresh::render_supply_sql(sql, code).expect("render");
        assert!(
            rendered.starts_with("WHERE phase_bucket IN ('p"),
            "渲染含标签: {rendered}"
        );
        // 无 code = 原样
        assert_eq!(
            crate::lifecycle::provider_refresh::render_supply_sql(sql, "").unwrap(),
            sql
        );
        // 非法 code → 报错（boot 期暴露）
        assert!(crate::lifecycle::provider_refresh::render_supply_sql(sql, "$x = nope()").is_err());
    }
}

#[cfg(test)]
#[path = "baseline_warm_tests.rs"]
mod baseline_warm_tests;
#[cfg(test)]
#[path = "bootstrap_coverage.rs"]
mod bootstrap_coverage;
#[cfg(test)]
#[path = "bootstrap_coverage_more.rs"]
mod bootstrap_coverage_more;
#[cfg(test)]
#[path = "bootstrap_r4.rs"]
mod bootstrap_r4;
