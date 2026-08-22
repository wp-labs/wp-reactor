//! bootstrap.rs 覆盖测试（注册于 bootstrap.rs 内, 可访问私有函数）。
//!
//! 覆盖点:
//! - `register_window_miss_provider`: 成功注册 + 保留名冲突错误。
//! - `configure_join_indexes`: join 条件 → 窗口 join key; 无 join 时 no-op。
//! - `init_knowledge_redis_if_configured`: 非致命路径（文件缺失 / TOML 非法 /
//!   无 redis provider）。
//! - `load_knowledge_into_windows`: CSV 表加载成功 / 无 tables / 禁用或无名表 /
//!   CSV 缺失 / 空行。

use super::*;
use std::collections::HashSet;

use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use tempfile::TempDir;

use wf_lang::ast::{Expr, FieldRef, JoinMode};
use wf_lang::plan::{
    BindPlan, JoinCondPlan, JoinPlan, MatchPlan, ScorePlan, WindowSpec, YieldPlan,
};

// ---------------------------------------------------------------------------
// register_window_miss_provider
// ---------------------------------------------------------------------------

#[test]
fn register_window_miss_provider_success() {
    let mut registry = WindowRegistry::build(vec![]).expect("empty registry");
    register_window_miss_provider(&mut registry, &[]).expect("register ok");
    assert!(registry.get_provider(WINDOW_MISS_WINDOW_NAME).is_some());
}

#[test]
fn register_window_miss_provider_conflicts_with_config() {
    let mut registry = WindowRegistry::build(vec![]).expect("empty registry");
    let config = wf_config::WindowConfig {
        name: WINDOW_MISS_WINDOW_NAME.to_string(),
        mode: wf_config::DistMode::Local,
        max_window_bytes: (1024 * 1024).into(),
        over_cap: Duration::from_secs(60).into(),
        evict_policy: wf_config::EvictPolicy::TimeFirst,
        watermark: Duration::ZERO.into(),
        allowed_lateness: Duration::ZERO.into(),
        late_policy: wf_config::LatePolicy::Drop,
        table: None,
    };
    let err = register_window_miss_provider(&mut registry, &[config])
        .expect_err("reserved name must fail");
    assert!(err.to_string().contains("reserved"), "got: {err:?}");
}

#[test]
fn register_window_miss_provider_conflicts_with_registry() {
    // A registry that already carries the reserved window name.
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "x",
        DataType::Utf8,
        true,
    )]));
    let def = wf_engine::window::WindowDef {
        params: wf_engine::window::WindowParams {
            name: WINDOW_MISS_WINDOW_NAME.to_string(),
            schema,
            time_col_index: None,
            over: Duration::ZERO,
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec![],
        config: wf_config::WindowConfig {
            name: WINDOW_MISS_WINDOW_NAME.to_string(),
            mode: wf_config::DistMode::Local,
            max_window_bytes: (1024 * 1024).into(),
            over_cap: Duration::from_secs(60).into(),
            evict_policy: wf_config::EvictPolicy::TimeFirst,
            watermark: Duration::ZERO.into(),
            allowed_lateness: Duration::ZERO.into(),
            late_policy: wf_config::LatePolicy::Drop,
            table: None,
        },
    };
    let mut registry = WindowRegistry::build(vec![def]).expect("build registry");
    let err = register_window_miss_provider(&mut registry, &[])
        .expect_err("reserved window in registry must fail");
    assert!(err.to_string().contains("reserved"), "got: {err:?}");
}

// ---------------------------------------------------------------------------
// configure_join_indexes
// ---------------------------------------------------------------------------

fn window_config(name: &str) -> wf_config::WindowConfig {
    wf_config::WindowConfig {
        name: name.into(),
        mode: wf_config::DistMode::Local,
        max_window_bytes: (64 * 1024 * 1024).into(),
        over_cap: Duration::from_secs(3600).into(),
        evict_policy: wf_config::EvictPolicy::TimeFirst,
        watermark: Duration::ZERO.into(),
        allowed_lateness: Duration::from_secs(3600).into(),
        late_policy: wf_config::LatePolicy::Drop,
        table: None,
    }
}

fn build_router_with_window(name: &str) -> Arc<Router> {
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", DataType::Utf8, true),
        ArrowField::new(
            "event_time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let def = wf_engine::window::WindowDef {
        params: wf_engine::window::WindowParams {
            name: name.into(),
            schema,
            time_col_index: Some(1),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec![],
        config: window_config(name),
    };
    let registry = WindowRegistry::build(vec![def]).expect("build registry");
    Arc::new(Router::new(registry))
}

fn plan_with_join(right_window: &str, right_field: &str) -> wf_lang::plan::RulePlan {
    wf_lang::plan::RulePlan {
        name: "join_rule".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        }],
        lets: vec![],
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(Duration::from_secs(60)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: wf_lang::ast::CloseMode::And,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            seq: None,
            tracked_bind_aliases: HashSet::new(),
            tracked_bind_fields: HashMap::new(),
            tracked_plain_fields: HashSet::new(),
            needs_field_history: true,
        },
        each_plan: None,
        stats_plan: None,
        joins: vec![JoinPlan {
            right_window: right_window.into(),
            mode: JoinMode::Inner,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("b".into(), "auction".into()),
                right: FieldRef::Qualified(right_window.into(), right_field.into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }],
        r#where: None,
        entity_plan: wf_lang::plan::EntityPlan {
            entity_type: "e".into(),
            entity_id_expr: Expr::Bool(false),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    }
}

#[test]
fn configure_join_indexes_sets_join_key() {
    let router = build_router_with_window("auction_events");
    let plans = vec![plan_with_join("auction_events", "id")];
    configure_join_indexes(&router, &plans);

    let win = router
        .registry()
        .get_window("auction_events")
        .expect("window exists");
    // Join index enabled → lookup returns Some (empty for an unknown key).
    assert!(
        win.join_lookup(&wf_engine::match_engine::JoinKey::Str("nope".into()))
            .is_some(),
        "join index must be enabled after configure_join_indexes"
    );
}

#[test]
fn configure_join_indexes_no_joins_is_noop() {
    let router = build_router_with_window("auction_events");
    configure_join_indexes(&router, &[]);

    let win = router
        .registry()
        .get_window("auction_events")
        .expect("window exists");
    assert!(
        win.join_lookup(&wf_engine::match_engine::JoinKey::Str("x".into()))
            .is_none()
    );
}

#[test]
fn configure_join_indexes_unknown_target_is_skipped() {
    let router = build_router_with_window("auction_events");
    // Join references a window that is not in the registry — must not panic.
    let plans = vec![plan_with_join("missing_window", "id")];
    configure_join_indexes(&router, &plans);
    let win = router
        .registry()
        .get_window("auction_events")
        .expect("window exists");
    assert!(
        win.join_lookup(&wf_engine::match_engine::JoinKey::Str("x".into()))
            .is_none()
    );
}

// ---------------------------------------------------------------------------
// init_knowledge_redis_if_configured — 非致命路径
// ---------------------------------------------------------------------------

#[test]
fn init_knowledge_redis_missing_file_warns_and_returns() {
    let dir = TempDir::new().expect("tempdir");
    let missing = dir.path().join("knowdb.toml");
    // Missing file → warn path, no panic.
    init_knowledge_redis_if_configured(&missing, dir.path());
}

#[test]
fn init_knowledge_redis_invalid_toml_warns_and_returns() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("knowdb.toml");
    std::fs::write(&path, "this is [ not valid toml ==").expect("write");
    init_knowledge_redis_if_configured(&path, dir.path());
}

#[test]
fn init_knowledge_redis_without_provider_returns() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("knowdb.toml");
    std::fs::write(
        &path,
        r#"
        [tables]
        enabled = false
        "#,
    )
    .expect("write");
    // No [provider.redis] → early return without touching wp_knowledge.
    init_knowledge_redis_if_configured(&path, dir.path());
}

// ---------------------------------------------------------------------------
// load_knowledge_into_windows — CSV 表加载
// ---------------------------------------------------------------------------

#[test]
fn load_knowledge_into_windows_loads_csv_table() {
    let dir = TempDir::new().expect("tempdir");
    let data_dir = dir.path().join("data/countries");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    std::fs::write(
        data_dir.join("data.csv"),
        "code,name\ncn,China\nus,United States\n",
    )
    .expect("write csv");

    let knowdb = dir.path().join("knowdb.toml");
    std::fs::write(
        &knowdb,
        r#"
        base_dir = "data"
        [[tables]]
        name = "countries"
        enabled = true
        dir = "countries"
        data_file = "data.csv"
        "#,
    )
    .expect("write knowdb.toml");

    let mut registry = WindowRegistry::build(vec![]).expect("empty registry");
    load_knowledge_into_windows(&knowdb, dir.path(), &mut registry).expect("load ok");

    let provider = registry.get_provider("countries").expect("provider window");
    let snapshot = provider.read().expect("lock").snapshot();
    assert_eq!(snapshot.len(), 2);
}

#[test]
fn load_knowledge_into_windows_no_tables_ok() {
    let dir = TempDir::new().expect("tempdir");
    let knowdb = dir.path().join("knowdb.toml");
    std::fs::write(&knowdb, "base_dir = 'data'\n").expect("write");
    let mut registry = WindowRegistry::build(vec![]).expect("empty registry");
    load_knowledge_into_windows(&knowdb, dir.path(), &mut registry).expect("ok");
}

#[test]
fn load_knowledge_into_windows_skips_disabled_or_unnamed_tables() {
    let dir = TempDir::new().expect("tempdir");
    let knowdb = dir.path().join("knowdb.toml");
    std::fs::write(
        &knowdb,
        r#"
        base_dir = "data"
        [[tables]]
        name = "disabled_table"
        enabled = false
        [[tables]]
        enabled = true
        "#,
    )
    .expect("write");
    let mut registry = WindowRegistry::build(vec![]).expect("empty registry");
    load_knowledge_into_windows(&knowdb, dir.path(), &mut registry).expect("ok");
    assert!(registry.get_provider("disabled_table").is_none());
}

#[test]
fn load_knowledge_into_windows_missing_csv_skipped() {
    let dir = TempDir::new().expect("tempdir");
    let knowdb = dir.path().join("knowdb.toml");
    std::fs::write(
        &knowdb,
        r#"
        base_dir = "data"
        [[tables]]
        name = "ghost"
        enabled = true
        dir = "ghost"
        "#,
    )
    .expect("write");
    let mut registry = WindowRegistry::build(vec![]).expect("empty registry");
    load_knowledge_into_windows(&knowdb, dir.path(), &mut registry).expect("ok");
    assert!(registry.get_provider("ghost").is_none());
}

#[test]
fn load_knowledge_into_windows_empty_rows_skipped() {
    let dir = TempDir::new().expect("tempdir");
    let data_dir = dir.path().join("data/empty");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    std::fs::write(data_dir.join("data.csv"), "code,name\n").expect("write header-only csv");

    let knowdb = dir.path().join("knowdb.toml");
    std::fs::write(
        &knowdb,
        r#"
        base_dir = "data"
        [[tables]]
        name = "empty_tbl"
        enabled = true
        dir = "empty"
        "#,
    )
    .expect("write knowdb.toml");

    let mut registry = WindowRegistry::build(vec![]).expect("empty registry");
    load_knowledge_into_windows(&knowdb, dir.path(), &mut registry).expect("ok");
    assert!(registry.get_provider("empty_tbl").is_none());
}
