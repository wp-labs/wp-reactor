// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------
// `serial()` 的序列化锁是**故意**跨 await 持有的：门控/诊断档是进程级全局状态，
// 测试必须在 await 期间也独占（否则其它测试会插入改写全局门控）。std Mutex 在
// 测试场景下短期持有无实际风险，clippy 的 await_holding_lock 属误报，模块级豁免。
#![allow(clippy::await_holding_lock)]
use super::*;
use crate::lifecycle::ReloadOutcome;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::time::Duration;

/// 门控/诊断档是进程级全局状态：串行化涉全局的测试，避免并行污染。
fn serial() -> std::sync::MutexGuard<'static, ()> {
    // 测试内 panic（如异步断言失败）会污染互斥锁：恢复后继续串行。
    crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

// -- 门控与初始化 -----------------------------------------------------

#[test]
fn init_disabled_resets_everything() {
    let _g = serial();
    reset_perf_diag();
    assert!(!perf_diag_enabled());
    assert!(!perf_cut_rules());
    assert!(!perf_cut_output());
    set_perf_cuts(true, true, true, true, false);
    reset_perf_diag();
    assert!(!perf_cut_rules(), "reset 必须复位门控");
    assert!(!perf_cut_output());
    assert!(!perf_cut_append());
}

#[test]
fn init_diag_with_stages_applies_first_stage_gates() {
    let _g = serial();
    let cfg = PerfConfig {
        stages: vec![
            PerfStage {
                name: "floor".into(),
                cut_rules: true,
                cut_output: true,
                cut_append: false,
                cut_recv: false,
                cut_sink_write: false,
                rules: None,
            },
            PerfStage {
                name: "full".into(),
                cut_rules: false,
                cut_output: false,
                cut_append: false,
                cut_recv: false,
                cut_sink_write: false,
                rules: None,
            },
        ],
    };
    init_perf_diag(&cfg);
    assert!(perf_diag_enabled());
    assert!(perf_cut_rules(), "stages[0] gates apply at startup");
    assert!(perf_cut_output());
    // 复位，避免污染其它测试。
    reset_perf_diag();
}

#[test]
fn init_without_stages_defaults_gates_false() {
    let _g = serial();
    let cfg = PerfConfig::default();
    init_perf_diag(&cfg);
    assert!(perf_diag_enabled(), "--perf-diag 即入口");
    assert!(!perf_cut_rules(), "无点 → 初始门控全 false");
    assert!(!perf_cut_output());
    reset_perf_diag();
}

#[test]
fn set_perf_cuts_flips_both_gates() {
    let _g = serial();
    reset_perf_diag();
    set_perf_cuts(true, false, true, true, true);
    assert!(perf_cut_rules());
    assert!(!perf_cut_output());
    assert!(perf_cut_append());
    assert!(perf_cut_recv());
    assert!(perf_cut_sink_write());
    set_perf_cuts(false, true, false, true, false);
    assert!(!perf_cut_rules());
    assert!(perf_cut_output());
    assert!(!perf_cut_append());
    assert!(perf_cut_recv());
    assert!(!perf_cut_sink_write());
    // 复位，避免污染其它测试：全局 static 门控，并行测试的 emit 会
    // 被 `perf_cut_output()` 早退丢输出（2026-08-25 实测：deferred_q8
    // EOS 重试 emit 被切 → 断言 left=[]）。
    reset_perf_diag();
}

// -- 诊断模式内存口径（diag_mem_cap / perf_diag_max_total_bytes） ---------

#[test]
fn mem_cap_non_diag_returns_config_unchanged() {
    let _g = serial();
    reset_perf_diag();
    // 非诊断模式：即使设了环境变量也必须返回配置值（生产零污染）。
    unsafe { std::env::set_var(WF_DIAG_MAX_TOTAL_BYTES, "64GB") };
    let (cap, src) = perf_diag_max_total_bytes(2 * 1024 * 1024 * 1024);
    unsafe { std::env::remove_var(WF_DIAG_MAX_TOTAL_BYTES) };
    assert_eq!(cap, 2 * 1024 * 1024 * 1024);
    assert!(src.contains("配置"));
}

#[test]
fn mem_cap_env_bytes_overrides() {
    let (cap, src) = diag_mem_cap(
        2 * 1024 * 1024 * 1024,
        Some(32 * 1024 * 1024 * 1024),
        Some("8GB"),
    );
    assert_eq!(cap, 8 * 1024 * 1024 * 1024);
    assert!(src.contains("8GB"), "source={src}");
    let (cap, src) = diag_mem_cap(
        2 * 1024 * 1024 * 1024,
        Some(32 * 1024 * 1024 * 1024),
        Some("4096MB"),
    );
    assert_eq!(cap, 4 * 1024 * 1024 * 1024);
    assert!(src.contains("4096MB"), "source={src}");
    // 字节大小不需要物理内存探测也能生效。
    let (cap, src) = diag_mem_cap(2 * 1024 * 1024 * 1024, None, Some("8GB"));
    assert_eq!(cap, 8 * 1024 * 1024 * 1024);
    assert!(src.contains("8GB"), "source={src}");
}

#[test]
fn mem_cap_env_percent_scales_with_phys() {
    let phys = 32 * 1024 * 1024 * 1024usize;
    let (cap, src) = diag_mem_cap(2 * 1024 * 1024 * 1024, Some(phys), Some("75%"));
    assert_eq!(cap, (phys as f64 * 0.75) as usize);
    assert!(src.contains("75%"), "source={src}");
    // 百分比但物理内存探测失败 → 回退默认（仍优先于配置）。
    let (cap, _) = diag_mem_cap(2 * 1024 * 1024 * 1024, None, Some("75%"));
    assert_eq!(cap, 2 * 1024 * 1024 * 1024, "探测失败应回退配置");
}

#[test]
fn mem_cap_default_is_sixty_percent_phys() {
    let phys = 64 * 1024 * 1024 * 1024usize;
    let (cap, src) = diag_mem_cap(2 * 1024 * 1024 * 1024, Some(phys), None);
    assert_eq!(cap, (phys as f64 * 0.6) as usize);
    assert!(src.contains("60%"), "source={src}");
    // 未设环境变量 + 物理内存探测失败 → 沿用配置。
    let (cap, src) = diag_mem_cap(2 * 1024 * 1024 * 1024, None, None);
    assert_eq!(cap, 2 * 1024 * 1024 * 1024);
    assert!(src.contains("配置"), "source={src}");
}

#[test]
fn mem_cap_zero_env_disables_override() {
    let (cap, src) = diag_mem_cap(
        2 * 1024 * 1024 * 1024,
        Some(32 * 1024 * 1024 * 1024),
        Some("0"),
    );
    assert_eq!(
        cap,
        2 * 1024 * 1024 * 1024,
        "WF_DIAG_MAX_TOTAL_BYTES=0 关闭覆盖"
    );
    assert!(src.contains("0"), "source={src}");
    let (cap, _) = diag_mem_cap(
        2 * 1024 * 1024 * 1024,
        Some(32 * 1024 * 1024 * 1024),
        Some(""),
    );
    assert_eq!(cap, 2 * 1024 * 1024 * 1024);
}

#[test]
fn mem_cap_never_reduces_below_config() {
    // 显式给更小的值 → 取 max(配置, 计算值)，诊断只放大不缩小。
    let (cap, _) = diag_mem_cap(
        8 * 1024 * 1024 * 1024,
        Some(32 * 1024 * 1024 * 1024),
        Some("1GB"),
    );
    assert_eq!(cap, 8 * 1024 * 1024 * 1024);
    // 无法解析的值 → 回退 60% 物理内存。
    let (cap, _) = diag_mem_cap(
        2 * 1024 * 1024 * 1024,
        Some(32 * 1024 * 1024 * 1024),
        Some("garbage"),
    );
    let phys = (32 * 1024 * 1024 * 1024usize) as f64;
    assert_eq!(cap, (phys * 0.6) as usize);
}

#[test]
fn mem_cap_diag_wrapper_defaults_to_sixty_percent() {
    let _g = serial();
    let cfg = PerfConfig {
        stages: vec![PerfStage {
            name: "floor".into(),
            cut_rules: true,
            cut_output: true,
            cut_append: false,
            cut_recv: false,
            cut_sink_write: false,
            rules: None,
        }],
    };
    init_perf_diag(&cfg);
    unsafe { std::env::remove_var(WF_DIAG_MAX_TOTAL_BYTES) };
    let (cap, src) = perf_diag_max_total_bytes(2 * 1024 * 1024 * 1024);
    // 机器相关：只断言不缩水 + 来源是默认放量（探测成功）或配置（探测失败）。
    assert!(cap >= 2 * 1024 * 1024 * 1024, "诊断模式不得低于配置 cap");
    assert!(
        src.contains("60%") || src.contains("探测失败"),
        "source={src}"
    );
    reset_perf_diag();
}

// -- 输出链消融开关（set_perf_cut_alert_for_test） ------------------------

#[test]
fn cut_alert_test_hook_flips_and_defaults_false() {
    let _g = serial();
    set_perf_cut_alert_for_test(true);
    assert!(perf_cut_alert(), "测试钩子应能强制开启");
    set_perf_cut_alert_for_test(false);
    assert!(!perf_cut_alert(), "用完必须复位");
}

// -- 哨兵载荷解析 -------------------------------------------------------

fn sentinel_batch(rounds: &[i64], ns: &[i64], starts: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("round", DataType::Int64, false),
        Field::new("n", DataType::Int64, false),
        Field::new("start_ns", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(rounds.to_vec())),
            Arc::new(Int64Array::from(ns.to_vec())),
            Arc::new(Int64Array::from(starts.to_vec())),
        ],
    )
    .unwrap()
}

#[test]
fn parse_sentinel_batch_reads_columns_and_injects_emit_ns() {
    let batch = sentinel_batch(&[0, 1], &[100, 200], &[1_000, 2_000]);
    let records = parse_sentinel_batch(&batch, 9_999);
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0],
        SentinelRecord {
            round: 0,
            n: 100,
            start_ns: 1_000,
            emit_ns: 9_999,
        }
    );
    assert_eq!(records[1].round, 1);
    assert_eq!(records[1].n, 200);
    assert_eq!(records[1].start_ns, 2_000);
    assert_eq!(records[1].emit_ns, 9_999);
}

#[test]
fn parse_sentinel_batch_empty_is_empty() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("round", DataType::Int64, false),
        Field::new("n", DataType::Int64, false),
        Field::new("start_ns", DataType::Int64, false),
    ]));
    let cols: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(Int64Array::from(Vec::<i64>::new())) as Arc<dyn arrow::array::Array>,
        Arc::new(Int64Array::from(Vec::<i64>::new())) as Arc<dyn arrow::array::Array>,
        Arc::new(Int64Array::from(Vec::<i64>::new())) as Arc<dyn arrow::array::Array>,
    ];
    let batch = RecordBatch::try_new(schema, cols).unwrap();
    assert!(parse_sentinel_batch(&batch, 0).is_empty());
}

#[test]
fn parse_sentinel_batch_missing_columns_default_to_zero() {
    // 只含 round 列的 batch：n/start_ns 按 0 处理。
    let schema = Arc::new(Schema::new(vec![Field::new(
        "round",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7i64]))]).unwrap();
    let records = parse_sentinel_batch(&batch, 42);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].round, 7);
    assert_eq!(records[0].n, 0);
    assert_eq!(records[0].start_ns, 0);
    assert_eq!(records[0].emit_ns, 42);
}

// -- EPS ----------------------------------------------------------------

#[test]
fn eps_computes_n_over_elapsed_seconds() {
    let rec = SentinelRecord {
        round: 0,
        n: 1_000_000,
        start_ns: 0,
        emit_ns: 100_000_000, // 0.1s
    };
    let eps = rec.eps().unwrap();
    assert!((eps - 10_000_000.0).abs() < 1.0, "eps = {eps}");
}

#[test]
fn eps_none_when_emit_not_after_start() {
    let rec = SentinelRecord {
        round: 0,
        n: 100,
        start_ns: 200,
        emit_ns: 100,
    };
    assert!(rec.eps().is_none(), "emit_ns <= start_ns → None");
    let rec = SentinelRecord {
        round: 0,
        n: 100,
        start_ns: 200,
        emit_ns: 200,
    };
    assert!(rec.eps().is_none(), "zero elapsed → None");
}

// -- 记录构建 -----------------------------------------------------------

#[test]
fn sentinel_output_carries_four_tuple_fields() {
    let rec = SentinelRecord {
        round: 2,
        n: 500,
        start_ns: 1_111,
        emit_ns: 2_222,
    };
    let out = sentinel_record_output(&rec);
    assert_eq!(&*out.yield_target, PERF_SENTINEL_WINDOW);
    let fields: std::collections::HashMap<&str, Value> = out
        .yield_fields
        .iter()
        .map(|(k, v)| (&**k, v.clone()))
        .collect();
    assert_eq!(fields.get("round"), Some(&Value::Number(2.0)));
    assert_eq!(fields.get("n"), Some(&Value::Number(500.0)));
    // start_ns/emit_ns 以字符串携带（epoch nanos 超出 f64 精确范围）。
    assert_eq!(fields.get("start_ns"), Some(&Value::Str("1111".into())));
    assert_eq!(fields.get("emit_ns"), Some(&Value::Str("2222".into())));
    assert_eq!(
        fields.get("record_type"),
        Some(&Value::Str("sentinel".into()))
    );
    // 类型标注：round/n → Digit，start_ns/emit_ns → Chars（JSON 精确整数/字符串）。
    let types: std::collections::HashMap<&str, &FieldType> = out
        .yield_field_types
        .iter()
        .map(|(k, v)| (&**k, v))
        .collect();
    assert_eq!(types.get("round"), Some(&&FieldType::Base(BaseType::Digit)));
    assert_eq!(
        types.get("start_ns"),
        Some(&&FieldType::Base(BaseType::Chars))
    );
    assert_eq!(
        types.get("emit_ns"),
        Some(&&FieldType::Base(BaseType::Chars))
    );
}

#[test]
fn stage_output_carries_current_index() {
    let out = stage_record_output(3);
    let fields: std::collections::HashMap<&str, Value> = out
        .yield_fields
        .iter()
        .map(|(k, v)| (&**k, v.clone()))
        .collect();
    assert_eq!(fields.get("current"), Some(&Value::Number(3.0)));
    assert_eq!(fields.get("record_type"), Some(&Value::Str("stage".into())));
}

// -- 诊断档状态机 -------------------------------------------------------

fn test_config(stages: Vec<PerfStage>) -> PerfConfig {
    PerfConfig { stages }
}

fn floor_stage() -> PerfStage {
    PerfStage {
        name: "floor".into(),
        cut_rules: true,
        cut_output: true,
        cut_append: false,
        cut_recv: false,
        cut_sink_write: false,
        rules: None,
    }
}

fn rules_stage() -> PerfStage {
    PerfStage {
        name: "rules".into(),
        cut_rules: false,
        cut_output: true,
        cut_append: false,
        cut_recv: false,
        cut_sink_write: false,
        rules: None,
    }
}

fn full_stage() -> PerfStage {
    PerfStage {
        name: "full".into(),
        cut_rules: false,
        cut_output: false,
        cut_append: false,
        cut_recv: false,
        cut_sink_write: false,
        rules: None,
    }
}

/// decode 档（cut_append=true, 2026-08-25）: 注入 + 解码（窗口 append 前即丢）。
fn decode_stage() -> PerfStage {
    PerfStage {
        name: "decode".into(),
        cut_rules: false,
        cut_output: false,
        cut_append: true,
        cut_recv: false,
        cut_sink_write: false,
        rules: None,
    }
}

/// recv 档（cut_recv=true, 2026-08-25）: 注入 + TCP 接收（非哨兵帧不解码即丢）。
fn recv_stage() -> PerfStage {
    PerfStage {
        name: "recv".into(),
        cut_rules: false,
        cut_output: false,
        cut_append: false,
        cut_recv: true,
        cut_sink_write: false,
        rules: None,
    }
}

/// emit 档（cut_sink_write=true, 2026-08-25）: 输出构建 + 通道投递完整,
/// sink 收到即丢（不序列化不写）——测输出构建段。
fn emit_stage() -> PerfStage {
    PerfStage {
        name: "emit".into(),
        cut_rules: false,
        cut_output: false,
        cut_append: false,
        cut_recv: false,
        cut_sink_write: true,
        rules: None,
    }
}

/// 无门控 stage（cut 全 false）——只测控制器/哨兵状态机、不测门控翻转的
/// 测试用它，避免并行测试期间全局 `PERF_CUT_*` 被拉高（会切断其它测试的
/// 规则求值/输出链，2026-08-25 实测：deferred_q8 EOS 重试 emit 被切 →
/// 断言 left=[]；lifecycle metrics 规则求值被切 → emitted_total=0）。
fn no_cut_stage(name: &str) -> PerfStage {
    PerfStage {
        name: name.into(),
        cut_rules: false,
        cut_output: false,
        cut_append: false,
        cut_recv: false,
        cut_sink_write: false,
        rules: None,
    }
}

#[tokio::test]
async fn controller_applies_next_stage_on_sentinel() {
    let _g = serial();
    init_perf_diag(&test_config(vec![
        floor_stage(),
        rules_stage(),
        full_stage(),
    ]));
    let controller = PerfDiagController::new();
    assert_eq!(controller.current(), 0, "startup applies stages[0]");
    assert!(controller.has_next());

    // round=0 完成 → 应用点 1（rules：cut_rules=false, cut_output=true）
    let applied = controller.on_sentinel(0).await.expect("transition");
    assert_eq!(applied.index, 1);
    assert!(!applied.reloaded);
    assert_eq!(controller.current(), 1);
    assert!(!perf_cut_rules(), "stage 1: rules 求值恢复");
    assert!(perf_cut_output(), "stage 1: 输出仍切");
    assert!(controller.has_next());

    // round=1 完成 → 应用点 2（full：全开）
    let applied = controller.on_sentinel(1).await.expect("transition");
    assert_eq!(applied.index, 2);
    assert_eq!(controller.current(), 2);
    assert!(!perf_cut_rules());
    assert!(!perf_cut_output());
    assert!(!controller.has_next(), "最后一个点之后无切换");

    // 越界：round=2 → None（无点 3）
    assert!(controller.on_sentinel(2).await.is_none());
    // 重复轮次：round=1 再来一次 → None（幂等）
    assert!(controller.on_sentinel(1).await.is_none());
    assert_eq!(controller.current(), 2);
    reset_perf_diag();
}

/// decode 档（cut_append）经哨兵应用与恢复（2026-08-25 补）。
#[tokio::test]
async fn controller_applies_cut_append_stage() {
    let _g = serial();
    init_perf_diag(&test_config(vec![decode_stage(), full_stage()]));
    let controller = PerfDiagController::new();
    assert!(perf_cut_append(), "stage 0: decode 档切窗口 append");
    assert!(!perf_cut_rules());
    assert!(!perf_cut_output());
    assert!(!perf_cut_recv());

    let applied = controller.on_sentinel(0).await.expect("transition");
    assert_eq!(applied.index, 1);
    assert!(!perf_cut_append(), "stage 1: full 恢复 append");
    reset_perf_diag();
}

/// recv 档（cut_recv）经哨兵应用与恢复（2026-08-25 补）。
#[tokio::test]
async fn controller_applies_cut_recv_stage() {
    let _g = serial();
    init_perf_diag(&test_config(vec![recv_stage(), full_stage()]));
    let controller = PerfDiagController::new();
    assert!(perf_cut_recv(), "stage 0: recv 档切解码");
    assert!(!perf_cut_append());

    let applied = controller.on_sentinel(0).await.expect("transition");
    assert_eq!(applied.index, 1);
    assert!(!perf_cut_recv(), "stage 1: full 恢复");
    reset_perf_diag();
}

/// emit 档（cut_sink_write）经哨兵应用与恢复（2026-08-25 补）。
#[tokio::test]
async fn controller_applies_cut_sink_write_stage() {
    let _g = serial();
    init_perf_diag(&test_config(vec![emit_stage(), full_stage()]));
    let controller = PerfDiagController::new();
    assert!(perf_cut_sink_write(), "stage 0: emit 档切序列化");
    assert!(!perf_cut_output());

    let applied = controller.on_sentinel(0).await.expect("transition");
    assert_eq!(applied.index, 1);
    assert!(!perf_cut_sink_write(), "stage 1: full 恢复");
    reset_perf_diag();
}

#[tokio::test]
async fn controller_idempotent_on_repeat_rounds() {
    let _g = serial();
    init_perf_diag(&test_config(vec![no_cut_stage("a"), no_cut_stage("b")]));
    let controller = PerfDiagController::new();
    // 同一 round 重复（--rounds 2）：只切换一次。
    assert!(controller.on_sentinel(0).await.is_some());
    assert!(
        controller.on_sentinel(0).await.is_none(),
        "repeat round must not re-apply"
    );
    assert_eq!(controller.current(), 1);
    reset_perf_diag();
}

#[tokio::test]
async fn controller_noop_without_stages() {
    let _g = serial();
    init_perf_diag(&PerfConfig::default());
    let controller = PerfDiagController::new();
    assert_eq!(controller.current(), 0);
    assert!(!controller.has_next());
    assert!(controller.on_sentinel(0).await.is_none());
    reset_perf_diag();
}

#[tokio::test]
async fn controller_negative_round_is_noop() {
    let _g = serial();
    init_perf_diag(&test_config(vec![no_cut_stage("a"), no_cut_stage("b")]));
    let controller = PerfDiagController::new();
    assert!(controller.on_sentinel(-1).await.is_none());
    assert_eq!(controller.current(), 0);
    reset_perf_diag();
}

// -- 数据窗排空等待 ----------------------------------------------------

fn drain_router() -> (Arc<Router>, Arc<wf_engine::window::Window>) {
    use arrow::datatypes::Schema;
    use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
    use wf_engine::window::{WindowDef, WindowParams, WindowRegistry};

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let def = WindowDef {
        params: WindowParams {
            name: "data_win".into(),
            schema: Arc::clone(&schema),
            time_col_index: None,
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec!["s".into()],
        config: WindowConfig {
            name: "data_win".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(1).into(),
            allowed_lateness: Duration::from_secs(0).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        },
    };
    let registry = WindowRegistry::build(vec![def]).unwrap();
    let router = Arc::new(Router::new(registry));
    let win = router.registry().get_window("data_win").unwrap();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
    win.append_with_watermark_sized(batch, 8, None).unwrap();
    (router, win)
}

#[tokio::test]
async fn data_drain_waits_until_all_consumers_ack() {
    let (router, win) = drain_router();
    assert_eq!(win.next_seq(), 1, "appended one batch");
    let cancel = CancellationToken::new();
    // 活消费者槽（未 ack）：min_acked=0 < next_seq=1 → 排空等待应阻塞。
    let slot = router.registry().progress("data_win").unwrap().register();
    let wait = tokio::spawn({
        let router = Arc::clone(&router);
        async move { wait_for_data_drain(&router, &cancel).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!wait.is_finished(), "未 ack 时排空等待必须阻塞");
    // 消费者 ack 追平 → 排空返回。
    slot.store(win.next_seq(), std::sync::atomic::Ordering::Release);
    tokio::time::timeout(Duration::from_secs(2), wait)
        .await
        .expect("ack 后排空等待应返回")
        .unwrap();
}

#[tokio::test]
async fn data_drain_cancellation_unblocks() {
    let (router, _win) = drain_router();
    let cancel = CancellationToken::new();
    let wait = tokio::spawn({
        let router = Arc::clone(&router);
        let cancel = cancel.clone();
        async move { wait_for_data_drain(&router, &cancel).await }
    });
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), wait)
        .await
        .expect("cancel 后排空等待应返回")
        .unwrap();
}

/// Row-partitioned（key/行号分片）窗口：最快分片追平 **不代表** 排空——
/// 慢分片仍在处理自己的行子集（2026-08-25 review 修复：旧 `max||min`
/// 在最快分片处提前排空）。
#[tokio::test]
async fn data_drain_waits_for_slowest_row_shard() {
    let (router, win) = drain_router();
    let next = win.next_seq();
    assert_eq!(next, 1, "appended one batch");
    let cancel = CancellationToken::new();
    let progress = router.registry().progress("data_win").unwrap();
    // 两个 key 分片消费者：fast 已追平 next_seq，slow 还在 0。
    let fast = progress.register_row_partitioned();
    let slow = progress.register_row_partitioned();
    fast.store(next, std::sync::atomic::Ordering::Release);
    let wait = tokio::spawn({
        let router = Arc::clone(&router);
        async move { wait_for_data_drain(&router, &cancel).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !wait.is_finished(),
        "最快分片已追平但慢分片未处理 → 必须阻塞（旧 max||min 会提前排空）"
    );
    slow.store(next, std::sync::atomic::Ordering::Release);
    tokio::time::timeout(Duration::from_secs(2), wait)
        .await
        .expect("全部分片追平后排空应返回")
        .unwrap();
}

/// Round-robin（whole-batch）分片窗口：min 恒停在最慢 shard（每片只 ack
/// 自己的批次），排空只能看 max（q13 分片卡尾的修复——不能被 min 卡死）。
#[tokio::test]
async fn data_drain_completes_when_round_robin_max_catches_up() {
    let (router, win) = drain_router();
    let next = win.next_seq();
    assert_eq!(next, 1, "appended one batch");
    let cancel = CancellationToken::new();
    let progress = router.registry().progress("data_win").unwrap();
    // 2 个 round-robin shard：拿到最后一批（seq=0）的 shard ack=1；
    // 另一个 shard 只 ack 自己最后一批（next=1 时它没有批次 → 停在 0）。
    let owner = progress.register();
    let _other = progress.register();
    owner.store(next, std::sync::atomic::Ordering::Release);
    assert_eq!(progress.min_acked(), 0, "min 停滞在无批次 shard");
    let wait = tokio::spawn({
        let router = Arc::clone(&router);
        async move { wait_for_data_drain(&router, &cancel).await }
    });
    tokio::time::timeout(Duration::from_secs(2), wait)
        .await
        .expect("round-robin max 追平即排空（不能被 min 卡死）")
        .unwrap();
}

// -- 哨兵任务与记录投递 ------------------------------------------------

#[test]
fn wall_nanos_advances() {
    let a = wall_nanos();
    std::thread::sleep(Duration::from_millis(2));
    let b = wall_nanos();
    assert!(a > 0);
    assert!(b > a, "wall clock must advance");
}

fn test_fanout() -> (Arc<SinkFanout>, tokio::sync::mpsc::Receiver<AlertBatch>) {
    use std::collections::HashMap;
    let (tx, rx) = tokio::sync::mpsc::channel::<AlertBatch>(8);
    let mut cache: HashMap<String, _> = HashMap::new();
    cache.insert(
        PERF_SENTINEL_WINDOW.to_string(),
        Arc::new(vec![(1usize, Arc::new(vec![tx]))]),
    );
    (SinkFanout::from_resolved(cache), rx)
}

#[tokio::test]
async fn emit_sentinel_records_sends_to_sink() {
    let (fanout, mut rx) = test_fanout();
    let rec = SentinelRecord {
        round: 0,
        n: 100,
        start_ns: 1_000,
        emit_ns: 2_000,
    };
    emit_sentinel_records(
        vec![stage_record_output(1), sentinel_record_output(&rec)],
        &fanout,
    )
    .await;
    let batch = rx.try_recv().expect("record must reach the sink channel");
    assert_eq!(batch.len(), 2, "stage + sentinel 两条记录");
    assert!(rx.try_recv().is_err(), "只有一批");
}

#[tokio::test]
async fn emit_sentinel_records_empty_is_noop() {
    let (fanout, mut rx) = test_fanout();
    emit_sentinel_records(Vec::new(), &fanout).await;
    assert!(rx.try_recv().is_err(), "空记录不发送");
}

#[tokio::test]
async fn emit_sentinel_records_no_sink_returns_quietly() {
    let fanout = SinkFanout::closed();
    let rec = SentinelRecord {
        round: 0,
        n: 1,
        start_ns: 1,
        emit_ns: 2,
    };
    emit_sentinel_records(vec![sentinel_record_output(&rec)], &fanout).await;
    // 无 sink：warn 一次后静默返回（不 panic）。
}

#[tokio::test]
async fn emit_sentinel_records_skips_unbuildable_record() {
    let (fanout, mut rx) = test_fanout();
    // 保留前缀字段（__wfu_*）→ append_record 失败 → 跳过整批。
    let mut out = sentinel_record_output(&SentinelRecord {
        round: 0,
        n: 1,
        start_ns: 1,
        emit_ns: 2,
    });
    out.yield_fields
        .push(("__wfu_reserved".into(), Value::Number(1.0)));
    emit_sentinel_records(vec![out], &fanout).await;
    assert!(rx.try_recv().is_err(), "构建失败 → 不发送");
}

#[tokio::test]
async fn emit_sentinel_records_falls_back_to_blocking_send() {
    // 容量 1 通道预占满：try_send 满 → 阻塞 send 等接收端排空后成功。
    use std::collections::HashMap;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<AlertBatch>(1);
    tx.try_send(AlertBatch::Rows(Arc::new(Vec::new())))
        .expect("占满槽位");
    let mut cache: HashMap<String, _> = HashMap::new();
    cache.insert(
        PERF_SENTINEL_WINDOW.to_string(),
        Arc::new(vec![(1usize, Arc::new(vec![tx]))]),
    );
    let fanout = SinkFanout::from_resolved(cache);
    let drainer = tokio::spawn(async move {
        let _dummy = rx.recv().await; // 先排掉占位
        rx.recv().await // 哨兵批
    });
    let rec = SentinelRecord {
        round: 0,
        n: 1,
        start_ns: 1,
        emit_ns: 2,
    };
    emit_sentinel_records(vec![sentinel_record_output(&rec)], &fanout).await;
    let batch = drainer.await.unwrap().expect("阻塞 send 送达");
    assert_eq!(batch.len(), 1);
}

fn sentinel_push(batch: Option<RecordBatch>) -> RulePush {
    RulePush {
        window_name: Arc::from(PERF_SENTINEL_WINDOW),
        events: None,
        batch: batch.map(Arc::new),
        materialize_fields: None,
        seq: 0,
        shard_rows: None,
    }
}

#[tokio::test]
async fn process_sentinel_push_missing_batch_is_noop() {
    let (fanout, mut rx) = test_fanout();
    let controller = PerfDiagController::new();
    process_sentinel_push(sentinel_push(None), &fanout, &controller).await;
    assert!(rx.try_recv().is_err(), "无 batch 不产出");
}

#[tokio::test]
async fn process_sentinel_push_empty_batch_is_noop() {
    let (fanout, mut rx) = test_fanout();
    let controller = PerfDiagController::new();
    let empty = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "round",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
    )
    .unwrap();
    process_sentinel_push(sentinel_push(Some(empty)), &fanout, &controller).await;
    assert!(rx.try_recv().is_err(), "0 行不产出");
}

#[tokio::test]
async fn process_sentinel_push_writes_stage_then_sentinel() {
    let _g = serial();
    init_perf_diag(&test_config(vec![no_cut_stage("a"), no_cut_stage("b")]));
    let (fanout, mut rx) = test_fanout();
    let controller = PerfDiagController::new();
    let batch = sentinel_batch(&[0], &[100], &[1_000]);
    process_sentinel_push(sentinel_push(Some(batch)), &fanout, &controller).await;
    // 先 stage{current=1}（切换完成信号）后 sentinel{round=0}：同批两记录。
    let first = rx.try_recv().expect("第一批（stage + sentinel）");
    assert_eq!(first.len(), 2);
    assert!(rx.try_recv().is_err());
    reset_perf_diag();
}

#[tokio::test]
async fn run_sentinel_task_processes_then_exits_on_cancel() {
    let (router, _win) = drain_router();
    let _g = serial();
    init_perf_diag(&test_config(vec![no_cut_stage("a")]));
    let controller = PerfDiagController::new();
    let (fanout, mut rx) = test_fanout();
    let (tx, rx_ch) = tokio::sync::mpsc::channel::<RulePush>(8);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(run_sentinel_task(SentinelTaskConfig {
        router,
        sink_fanout: fanout,
        controller: controller.clone(),
        cancel: cancel.clone(),
        rx: rx_ch,
    }));
    // 启动即写 stage{current=0} 初始信号（轮询等任务完成启动）。
    let init = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(batch) = rx.try_recv() {
                return batch;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("初始 stage 记录");
    assert_eq!(init.len(), 1);
    // 投一条哨兵 → 处理后 cancel → 任务返回。
    tx.send(sentinel_push(Some(sentinel_batch(&[0], &[10], &[1]))))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("task 应退出")
        .expect("join ok");
    assert!(result.is_ok());
    reset_perf_diag();
}

#[tokio::test]
async fn controller_reloads_rules_subset_via_control_handle() {
    let _g = serial();
    init_perf_diag(&test_config(vec![
        no_cut_stage("floor"),
        PerfStage {
            name: "c_family".into(),
            cut_rules: false,
            cut_output: false,
            cut_append: false,
            cut_recv: false,
            cut_sink_write: false,
            rules: Some("models/rules/c_family.wfl".into()),
        },
    ]));
    let controller = PerfDiagController::new();

    // 基线：临时 wfusion.toml（真实 loader 产物）。
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("models")).unwrap();
    std::fs::write(
        dir.path().join("models/windows.toml"),
        r#"[window_defaults]
evict_interval = "1s"
max_window_bytes = "512MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "1s"
allowed_lateness = "30m"
late_policy = "drop"
"#,
    )
    .unwrap();
    std::fs::write(
        dir.path().join("wfusion.toml"),
        r#"
mode = "daemon"
windows = "models/windows.toml"
sinks = "sinks"

[[sources]]
type = "file"
name = "seed"
path = "seed.ndjson"
stream_tag = "syslog"
data_format = "ndjson"

[runtime]
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules = "rules/basic.wfl"
"#,
    )
    .unwrap();
    let cfg_path = dir.path().join("wfusion.toml");
    let ctx = wf_config::ConfigVarContext::new();
    let loader = wf_config::FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(dir.path()));
    let base_raw = loader.load_raw().expect("load raw");
    let base_config = loader.load().expect("load config");
    assert_eq!(base_config.runtime.rules, "rules/basic.wfl");

    // 模拟 Reactor 侧的 reload 消费者：收到请求 → 校验 rules → 回 Applied。
    let (tx, mut rx) = mpsc::channel::<crate::lifecycle::ReloadRequest>(8);
    let handle = RuntimeControlHandle::new(tx, CancellationToken::new());
    controller.set_reload_handle(handle, base_raw, base_config);

    let consumer = tokio::spawn(async move {
        let req = rx.recv().await.expect("reload request");
        match req {
            crate::lifecycle::ReloadRequest::Reload { config, reply, .. } => {
                assert_eq!(config.runtime.rules, "models/rules/c_family.wfl");
                let plan = wf_config::FusionReloadPlan::default();
                let _ = reply.send(Ok(ReloadOutcome::Applied(plan)));
            }
            other => panic!("unexpected request: {other:?}"),
        }
    });

    let applied = controller
        .on_sentinel(0)
        .await
        .expect("transition with reload");
    assert_eq!(applied.index, 1);
    assert!(applied.reloaded, "rules subset change must trigger reload");
    consumer.await.unwrap();

    // 基线已推进：再触发同一目标（重复轮次）→ 幂等短路，不再 reload。
    assert!(controller.on_sentinel(0).await.is_none());
    reset_perf_diag();
}

/// 构造带真实 loader 基线 + 空控制通道的控制器（reload 路径测试用）。
async fn controller_with_baseline(
    stages: Vec<PerfStage>,
) -> (
    Arc<PerfDiagController>,
    tokio::sync::mpsc::Receiver<crate::lifecycle::ReloadRequest>,
) {
    init_perf_diag(&test_config(stages));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("models")).unwrap();
    std::fs::write(
        dir.path().join("models/windows.toml"),
        r#"[window_defaults]
evict_interval = "1s"
max_window_bytes = "512MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "1s"
allowed_lateness = "30m"
late_policy = "drop"
"#,
    )
    .unwrap();
    let cfg_path = dir.path().join("wfusion.toml");
    std::fs::write(
        &cfg_path,
        r#"
mode = "daemon"
windows = "models/windows.toml"
sinks = "sinks"

[[sources]]
type = "file"
name = "seed"
path = "seed.ndjson"
stream_tag = "syslog"
data_format = "ndjson"

[runtime]
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules = "rules/basic.wfl"
"#,
    )
    .unwrap();
    let ctx = wf_config::ConfigVarContext::new();
    let loader = wf_config::FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(dir.path()));
    let raw = loader.load_raw().expect("load raw");
    let config = loader.load().expect("load config");
    let controller = PerfDiagController::new();
    let (tx, rx) = tokio::sync::mpsc::channel::<crate::lifecycle::ReloadRequest>(8);
    controller.set_reload_handle(
        RuntimeControlHandle::new(tx, CancellationToken::new()),
        raw,
        config,
    );
    (controller, rx)
}

#[tokio::test]
async fn controller_reload_failure_still_advances() {
    let _g = serial();
    let (controller, mut rx) = controller_with_baseline(vec![
        no_cut_stage("floor"),
        PerfStage {
            name: "c_family".into(),
            cut_rules: false,
            cut_output: false,
            cut_append: false,
            cut_recv: false,
            cut_sink_write: false,
            rules: Some("models/rules/c_family.wfl".into()),
        },
    ])
    .await;
    let consumer = tokio::spawn(async move {
        let req = rx.recv().await.expect("reload request");
        match req {
            crate::lifecycle::ReloadRequest::Reload { reply, .. } => {
                use orion_error::conversion::ToStructError;
                let _ = reply.send(Err(crate::error::RuntimeReason::Shutdown.to_err()));
            }
            other => panic!("unexpected: {other:?}"),
        }
    });
    let applied = controller.on_sentinel(0).await.expect("reload 失败仍切换");
    consumer.await.unwrap();
    assert_eq!(applied.index, 1);
    assert!(!applied.reloaded, "reload 失败 → reloaded=false");
    assert_eq!(controller.current(), 1, "门控已翻即算切换完成");
    reset_perf_diag();
}

#[tokio::test]
async fn controller_reload_same_rules_is_noop_reload() {
    let _g = serial();
    // 目标点 rules 与基线相同 → changed=false → 不触发 reload（reloaded=false）。
    let (controller, mut rx) = controller_with_baseline(vec![
        no_cut_stage("floor"),
        PerfStage {
            name: "same_rules".into(),
            cut_rules: false,
            cut_output: false,
            cut_append: false,
            cut_recv: false,
            cut_sink_write: false,
            rules: Some("rules/basic.wfl".into()),
        },
    ])
    .await;
    let applied = controller.on_sentinel(0).await.expect("transition");
    assert_eq!(applied.index, 1);
    assert!(!applied.reloaded, "rules 未变 → 不 reload");
    // 无 reload 请求发出。
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "同 rules 不得触发 reload"
    );
    reset_perf_diag();
}

#[tokio::test]
async fn controller_reload_without_baseline_applies_without_reload() {
    let _g = serial();
    init_perf_diag(&test_config(vec![
        no_cut_stage("floor"),
        PerfStage {
            name: "c_family".into(),
            cut_rules: false,
            cut_output: false,
            cut_append: false,
            cut_recv: false,
            cut_sink_write: false,
            rules: Some("models/rules/c_family.wfl".into()),
        },
    ]));
    let controller = PerfDiagController::new();
    // 注入控制句柄但无基线（set_reload_handle 未调用）：changed 由基线推导，
    // 基线缺失 → 不触发 reload，门控仍翻转、切换正常完成。
    let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::lifecycle::ReloadRequest>(8);
    *controller.control.write().unwrap() =
        Some(RuntimeControlHandle::new(tx, CancellationToken::new()));
    let applied = controller.on_sentinel(0).await.expect("无基线也应切换");
    assert_eq!(applied.index, 1);
    assert!(!applied.reloaded);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "无基线不触发 reload"
    );
    reset_perf_diag();
}
