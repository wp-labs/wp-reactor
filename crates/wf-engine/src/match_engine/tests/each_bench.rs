//! Q1 `on each` 列式路径的逐分量微基准——cut A/B/C/D 的 ns/行归因（数据版）。
//!
//! 与端到端切刀（docs/design/archive/q1-throughput-bisection.md §15）互补：切刀在 load/工具量化
//! 噪声下给「占比」，这里在同一进程内直接测每个分量的绝对 ns/行成本。
//!
//! 运行：
//!   cargo test --release -p wf-engine each_bench -- --ignored --nocapture
//!
//! 测量对象（Q1 `q1_bid_passthrough` 真实形状：score=1.0、entity=b.auction、
//! yield 4 字段 id/alert_type/detail/request_count）：
//!   baseline : `execute_each_direct_batch_columnar` 完整路径（256 行分段，同生产）
//!   wfx_id   : `EachWfxPrefix::wfx_id`（cut A：批级 FNV 前缀 + 每行时间后缀哈希）
//!   fired_at : `format_nanos_utc`（cut B：UTC 格式化）
//!   entity   : `ColumnarEvent::field_value` + `value_to_string`（cut D）
//!   fill     : `begin_row` + 4×`stage_yield_cell` + `commit_each_row`（cut C）
use std::sync::Arc;

use std::collections::HashMap;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use smol_str::SmolStr;
use wf_lang::ast::{BinOp, Expr, FieldRef, JoinMode};
use wf_lang::plan::{EachPlan, JoinCondPlan, JoinPlan, YieldField};
use wf_lang::{BaseType, FieldType};

use crate::alert::{AlertColumnBuilder, AlertOrigin, EachRowCells};
use crate::match_engine::event_bridge::{ColumnarEvent, materialize_rows};
use crate::match_engine::executor::{EachWfxPrefix, format_nanos_utc};
use crate::match_engine::match_engine::{field_ref_name, value_to_string};
use crate::match_engine::{
    Event, JoinKey, JoinRow, RuleExecutor, Value, WindowLookup, columnar_join_rows,
};
use crate::window::{Window, WindowParams};
use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};

use super::helpers::{simple_plan, simple_rule_plan};

const N: usize = 1_000_000;
const ALERT_BATCH_SIZE: usize = 256;
const NANOS: i64 = 1_750_000_000_000_000_000;

/// Q1 `q1_bid_passthrough` 形状的 RuleExecutor（列式安全门必须放行）。
fn q1_plan_rule() -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "q1_bid_passthrough",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q1_passthrough".into()),
        },
        YieldField {
            name: "detail".into(),
            value: Expr::StringLit("bid".into()),
        },
        YieldField {
            name: "request_count".into(),
            value: Expr::Number(1.0),
        },
    ];
    RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Float)),
            ("alert_type".into(), FieldType::Base(BaseType::Chars)),
            ("detail".into(), FieldType::Base(BaseType::Chars)),
            ("request_count".into(), FieldType::Base(BaseType::Float)),
        ]),
    )
}

/// Q1 形态的 bid_events 批（7 列与 nexmark_pk 一致；auction 递增保证
/// wfx_id 哈希输入逐行变化，贴近真实访问模式）。
/// 数字/字符串形态对齐真实 nexmark 数据（9-12 位 auction、40-60B url）——
/// 固定短串会系统性低估 wfx_id 的渲染与哈希成本。
fn q1_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("dateTime", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
    ]));
    // Deterministic pseudo-random so failures reproduce; shapes match
    // nexmark_pk generator output (auction ~1e9..1e12, url ~40-60 chars).
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = |range: u64| {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (rng >> 33) % range
    };
    let mut auction = Vec::with_capacity(n);
    let mut bidder = Vec::with_capacity(n);
    let mut price = Vec::with_capacity(n);
    let mut date_time = Vec::with_capacity(n);
    let mut url = Vec::with_capacity(n);
    for _ in 0..n {
        auction.push((next(1_000_000_000) + 1) as i64);
        bidder.push((next(1_000_000_000) + 1) as i64);
        price.push((next(10_000_000) + 1) as i64);
        date_time.push(1_700_000_000_000i64 + next(86_400_000) as i64);
        let len = 40 + next(20) as usize;
        let mut s = String::with_capacity(len + 11);
        s.push_str("http://example.com/p/");
        for _ in 0..len {
            s.push((b'a' + next(26) as u8) as char);
        }
        url.push(s);
    }
    let channel = vec!["mobile"; n];
    let extra = vec!["x"; n];
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction)) as ArrayRef,
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(price)),
            Arc::new(StringArray::from(channel)),
            Arc::new(StringArray::from(url)),
            Arc::new(Int64Array::from(date_time)),
            Arc::new(StringArray::from(extra)),
        ],
    )
    .unwrap()
}

struct Report {
    name: &'static str,
    per_ns: f64,
}

impl Report {
    fn line(&self, baseline_ns: f64) {
        let mps = 1e9 / self.per_ns / 1e6;
        eprintln!(
            "[each-bench] {:<22} {:>7.1} ns/row  ({:>5.1}M rows/s)  = {:>5.1}% of baseline",
            self.name,
            self.per_ns,
            mps,
            self.per_ns / baseline_ns * 100.0
        );
    }
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine each_bench -- --ignored --nocapture"]
fn q1_each_components_per_row() {
    let exec = q1_plan_rule();
    assert!(
        exec.each_plan_columnar_safe(),
        "bench rule must pass the columnar safety gate"
    );
    let batch = q1_batch(N);
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..N).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();

    // ---- baseline：完整列式路径（256 行分段，同生产 ALERT_BATCH_SIZE） ----
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let mut total_appended = 0usize;
    let start = Instant::now();
    for chunk in rows.chunks(ALERT_BATCH_SIZE) {
        let stats =
            exec.execute_each_direct_batch_columnar(chunk, NANOS, &mut builder, &mut appended);
        total_appended += stats.appended;
    }
    let baseline_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    assert_eq!(total_appended, N, "baseline must append all rows");
    assert_eq!(builder.len(), N);
    let baseline = Report {
        name: "baseline(完整列式)",
        per_ns: baseline_ns,
    };
    baseline.line(baseline_ns);

    // ---- cut A：wfx_id（批级 FNV 前缀 + 每行时间后缀哈希） ----
    let origin = AlertOrigin::Event;
    let wfx_prefix = EachWfxPrefix::new("q1_bid_passthrough");
    let start = Instant::now();
    let mut sum = 0usize;
    for i in 0..N {
        let wfx = wfx_prefix.wfx_id(NANOS + i as i64, &origin);
        sum += wfx.len();
    }
    let a = Report {
        name: "wfx_id(cut A)",
        per_ns: start.elapsed().as_secs_f64() * 1e9 / N as f64,
    };
    a.line(baseline_ns);
    assert!(sum > 0);

    // ---- cut B：fired_at 格式化 ----
    let start = Instant::now();
    let mut sum = 0usize;
    for i in 0..N {
        sum += format_nanos_utc(NANOS + i as i64).len();
    }
    let b = Report {
        name: "fired_at(cut B)",
        per_ns: start.elapsed().as_secs_f64() * 1e9 / N as f64,
    };
    b.line(baseline_ns);
    assert!(sum > 0);

    // ---- cut D：entity 取列 + value_to_string ----
    let fr = FieldRef::Qualified("b".into(), "auction".into());
    let fname = field_ref_name(&fr);
    let start = Instant::now();
    let mut sum = 0usize;
    for ev in &col_events {
        if let Some(v) = ev.field_value(fname) {
            sum += value_to_string(&v).len();
        }
    }
    let d = Report {
        name: "entity(cut D)",
        per_ns: start.elapsed().as_secs_f64() * 1e9 / N as f64,
    };
    d.line(baseline_ns);
    assert!(sum > 0);

    // ---- cut C：builder 输出填充（begin_row + 4×stage + commit） ----
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let ft_float = Some(FieldType::Base(BaseType::Float));
    let ft_str = Some(FieldType::Base(BaseType::Chars));
    let name_id: Arc<str> = Arc::from("id");
    let name_at: Arc<str> = Arc::from("alert_type");
    let name_det: Arc<str> = Arc::from("detail");
    let name_cnt: Arc<str> = Arc::from("request_count");
    let rule_name: Arc<str> = Arc::from("q1_bid_passthrough");
    let entity_type: Arc<str> = Arc::from("digit");
    let origin: Arc<str> = Arc::from(AlertOrigin::Event.as_str());
    let close_reason: Arc<str> = Arc::from("");
    let emit_time: Arc<str> = Arc::from("2026-08-18T00:00:00.000000000Z");
    let summary: Arc<str> = Arc::from("summary");
    let start = Instant::now();
    for ev in &col_events {
        builder.begin_row();
        // id = b.auction（Field：取列 + coerce + stage）
        let v = ev
            .field_value("auction")
            .unwrap_or_else(|| Value::Str(SmolStr::default()));
        let v = RuleExecutor::coerce_yield_field_value_with("id", ft_float.as_ref(), v)
            .unwrap()
            .unwrap();
        builder
            .stage_yield_cell(&name_id, ft_float.as_ref(), &v)
            .unwrap();
        // alert_type / detail（StringLit：克隆 + coerce + stage）
        let v = Value::Str("q1_passthrough".into());
        let v = RuleExecutor::coerce_yield_field_value_with("alert_type", ft_str.as_ref(), v)
            .unwrap()
            .unwrap();
        builder
            .stage_yield_cell(&name_at, ft_str.as_ref(), &v)
            .unwrap();
        let v = Value::Str("bid".into());
        let v = RuleExecutor::coerce_yield_field_value_with("detail", ft_str.as_ref(), v)
            .unwrap()
            .unwrap();
        builder
            .stage_yield_cell(&name_det, ft_str.as_ref(), &v)
            .unwrap();
        // request_count（Number 字面量）
        let v = Value::Number(1.0);
        let v = RuleExecutor::coerce_yield_field_value_with("request_count", ft_float.as_ref(), v)
            .unwrap()
            .unwrap();
        builder
            .stage_yield_cell(&name_cnt, ft_float.as_ref(), &v)
            .unwrap();
        // 系统列（wfx_id/entity/fired_at 用常量——各自的生成成本由 cut A/D/B 单独计）
        builder.commit_each_row(EachRowCells {
            wfx_id: String::from("wf_0000000000000000001").into(),
            score: 1.0,
            entity_id: String::from("1").into(),
            fired_at: String::from("2026-08-18T00:00:00.000000000Z"),
            rule_name: &rule_name,
            entity_type: &entity_type,
            origin: &origin,
            close_reason: &close_reason,
            emit_time: &emit_time,
            summary: &summary,
        });
    }
    let c = Report {
        name: "fill(cut C)",
        per_ns: start.elapsed().as_secs_f64() * 1e9 / N as f64,
    };
    c.line(baseline_ns);
    assert_eq!(builder.len(), N, "fill must append all rows");

    // ---- fill 分解 A：只 stage（begin_row + 4×stage_yield_cell，不 commit） ----
    {
        let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
        let ft_float = Some(FieldType::Base(BaseType::Float));
        let ft_str = Some(FieldType::Base(BaseType::Chars));
        let name_id: Arc<str> = Arc::from("id");
        let name_at: Arc<str> = Arc::from("alert_type");
        let name_det: Arc<str> = Arc::from("detail");
        let name_cnt: Arc<str> = Arc::from("request_count");
        let start = Instant::now();
        for ev in &col_events {
            builder.begin_row();
            let v = ev
                .field_value("auction")
                .unwrap_or_else(|| Value::Str(SmolStr::default()));
            let v = RuleExecutor::coerce_yield_field_value_with("id", ft_float.as_ref(), v)
                .unwrap()
                .unwrap();
            builder
                .stage_yield_cell(&name_id, ft_float.as_ref(), &v)
                .unwrap();
            let v = Value::Str("q1_passthrough".into());
            let v = RuleExecutor::coerce_yield_field_value_with("alert_type", ft_str.as_ref(), v)
                .unwrap()
                .unwrap();
            builder
                .stage_yield_cell(&name_at, ft_str.as_ref(), &v)
                .unwrap();
            let v = Value::Str("bid".into());
            let v = RuleExecutor::coerce_yield_field_value_with("detail", ft_str.as_ref(), v)
                .unwrap()
                .unwrap();
            builder
                .stage_yield_cell(&name_det, ft_str.as_ref(), &v)
                .unwrap();
            let v = Value::Number(1.0);
            let v =
                RuleExecutor::coerce_yield_field_value_with("request_count", ft_float.as_ref(), v)
                    .unwrap()
                    .unwrap();
            builder
                .stage_yield_cell(&name_cnt, ft_float.as_ref(), &v)
                .unwrap();
        }
        let per = start.elapsed().as_secs_f64() * 1e9 / N as f64;
        eprintln!(
            "[each-bench] fill_stage_only: {:>7.1} ns/row  ({:>5.1}% of baseline)  [4×stage, 无 commit]",
            per,
            per / baseline_ns * 100.0
        );
    }

    // ---- fill 分解 B：只 commit（系统列 push，staged 为空） ----
    {
        let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
        let rule_name: Arc<str> = Arc::from("q1_bid_passthrough");
        let entity_type: Arc<str> = Arc::from("digit");
        let origin: Arc<str> = Arc::from(AlertOrigin::Event.as_str());
        let close_reason: Arc<str> = Arc::from("");
        let emit_time: Arc<str> = Arc::from("2026-08-18T00:00:00.000000000Z");
        let summary: Arc<str> = Arc::from("summary");
        let start = Instant::now();
        for _ in 0..N {
            builder.commit_each_row(EachRowCells {
                wfx_id: String::from("wf_0000000000000000001").into(),
                score: 1.0,
                entity_id: String::from("1").into(),
                fired_at: String::from("2026-08-18T00:00:00.000000000Z"),
                rule_name: &rule_name,
                entity_type: &entity_type,
                origin: &origin,
                close_reason: &close_reason,
                emit_time: &emit_time,
                summary: &summary,
            });
        }
        let per = start.elapsed().as_secs_f64() * 1e9 / N as f64;
        eprintln!(
            "[each-bench] fill_commit_only: {:>7.1} ns/row  ({:>5.1}% of baseline)  [commit, 无 stage]",
            per,
            per / baseline_ns * 100.0
        );
    }

    // ---- cut A 分解：String 分配裸成本（wfx_id 每行一次 40B hex 分配） ----
    let start = Instant::now();
    let mut sum = 0usize;
    for _ in 0..N {
        let s = std::hint::black_box(String::from("wf_0000000000000000001"));
        sum += s.len();
    }
    let alloc = Report {
        name: "string_alloc(裸)",
        per_ns: start.elapsed().as_secs_f64() * 1e9 / N as f64,
    };
    alloc.line(baseline_ns);
    assert!(sum > 0);

    // ---- cut A 分解：f64 Display 格式化裸成本（wfx_id 对 4 个数字列各做一次） ----
    let start = Instant::now();
    let mut sum = 0usize;
    let mut scratch = String::new();
    for i in 0..N {
        scratch.clear();
        let _ = std::fmt::Write::write_fmt(&mut scratch, format_args!("{}", i as f64));
        sum += scratch.len();
    }
    let fmt = Report {
        name: "f64_format(裸)",
        per_ns: start.elapsed().as_secs_f64() * 1e9 / N as f64,
    };
    fmt.line(baseline_ns);
    assert!(sum > 0);

    // ---- 端到端并发模拟：10 线程同时跑 execute（模拟 10 worker 的
    // 缓存/带宽/分配器竞争）。每线程独立构造自己的 100k 行 batch
    // （真实端到端里各 worker 处理不同 batch），Barrier 同步后计时只
    // 覆盖 execute。 ----
    {
        let workers = 10usize;
        let per_worker = N / workers;
        let exec = Arc::new(q1_plan_rule());
        let barrier = Arc::new(std::sync::Barrier::new(workers + 1));
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                let exec = Arc::clone(&exec);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let batch = q1_batch(per_worker);
                    let col_events: Vec<ColumnarEvent<'_>> = (0..per_worker)
                        .map(|r| ColumnarEvent::new(&batch, r))
                        .collect();
                    let rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
                        .iter()
                        .enumerate()
                        .map(|(i, ev)| (ev, NANOS + w as i64 * per_worker as i64 + i as i64))
                        .collect();
                    barrier.wait();
                    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
                    let mut appended = Vec::new();
                    let mut total = 0usize;
                    for chunk in rows.chunks(ALERT_BATCH_SIZE) {
                        let stats = exec.execute_each_direct_batch_columnar(
                            chunk,
                            NANOS,
                            &mut builder,
                            &mut appended,
                        );
                        total += stats.appended;
                    }
                    total
                })
            })
            .collect();
        barrier.wait();
        let start = Instant::now();
        let total_rows: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let el = start.elapsed();
        let per = el.as_secs_f64() * 1e9 / total_rows as f64;
        eprintln!(
            "[each-bench] 10线程并发 execute: {:>7.1} ns/row  ({:>5.1}M rows/s 合计, 加速比 {:>4.2}x)  = {:>5.1}% of baseline",
            per,
            total_rows as f64 / el.as_secs_f64() / 1e6,
            baseline_ns / per,
            per / baseline_ns * 100.0
        );
    }

    // ---- 归因校验：四项之和 ≈ baseline（差值 = 行访问/循环/遥测弥散开销） ----
    let sum_parts = a.per_ns + b.per_ns + c.per_ns + d.per_ns;
    eprintln!(
        "[each-bench] 分量之和 {:.1} ns/row vs baseline {:.1} ns/row（差值 = 弥散开销 {:.1} ns/row, {:.1}%）",
        sum_parts,
        baseline_ns,
        (baseline_ns - sum_parts).max(0.0),
        ((baseline_ns - sum_parts).max(0.0)) / baseline_ns * 100.0
    );
}

// ---------------------------------------------------------------------------
// Q20 snapshot join 列式路径（2026-08-24 Arc<JoinRow> 修复后）
// ---------------------------------------------------------------------------

/// q20 形状的 each + Snapshot join + where 规则：`b.auction ==
/// auction_events.id` + where `auction_events.category == 10` + yield 读左窗
/// `b.auction` + 右窗 `auction_events.category`（对齐 direct_tests 的
/// `each_join_plan_rule`）。
fn q20_plan_rule() -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "q20_shape",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    plan.joins = vec![JoinPlan {
        right_window: "auction_events".into(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "auction".into()),
            right: FieldRef::Qualified("auction_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Qualified(
            "auction_events".into(),
            "category".into(),
        ))),
        right: Box::new(Expr::Number(10.0)),
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "category".into(),
            value: Expr::Field(FieldRef::Qualified(
                "auction_events".into(),
                "category".into(),
            )),
        },
    ];
    RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Digit)),
            ("category".into(), FieldType::Base(BaseType::Digit)),
        ]),
    )
}

/// q20 形状的 join 目标窗口：`n` 行 auction（id 唯一递增 + category + payload），
/// join 索引建在 `id` 上。category 20% 为 10（where 通过率对齐生产）。
fn q20_auction_window(n: usize) -> Window {
    use std::time::Duration;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let win = Window::new(
        WindowParams {
            name: "auction_events".into(),
            schema: schema.clone(),
            time_col_index: None,
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        WindowConfig {
            name: "auction_events".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(5).into(),
            allowed_lateness: Duration::from_secs(0).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        },
    );
    win.set_join_key("id".into());
    let ids: Vec<i64> = (0..n as i64).collect();
    let category: Vec<i64> = (0..n as i64)
        .map(|i| if i % 5 == 0 { 10 } else { 99 })
        .collect();
    let payload: Vec<String> = (0..n).map(|i| format!("p{}", i % 100)).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(Int64Array::from(category)),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .unwrap();
    win.append(batch).unwrap();
    win
}

/// 把真实 join 索引窗口包装成 [`WindowLookup`]（bench 用；join_lookup 走真实
/// 索引 O(1) 路径，与生产 RegistryLookup 同源）。
struct Q20WinLookup<'a> {
    win: &'a Window,
}

impl WindowLookup for Q20WinLookup<'_> {
    fn snapshot_field_values(
        &self,
        _w: &str,
        _f: &str,
    ) -> Option<std::collections::HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        Some(columnar_join_rows(self.win.snapshot(), None))
    }
    fn snapshot_with_timestamps(&self, _w: &str) -> Option<Vec<(i64, JoinRow)>> {
        None
    }
    fn join_lookup(&self, _w: &str, _kf: &str, key: &Value) -> Option<Vec<JoinRow>> {
        let jk = JoinKey::from_value(key)?;
        self.win.join_lookup("id", &jk, None)
    }
}

/// q20 形状 bid 批：`n` 行（auction 列）。`hot` 时 25% 行落在 8 个热键上
/// （同桶多行共享 Arc<JoinRow> 路径），其余引用窗口内唯一键；`hot=false` 全唯一。
fn q20_bid_batch(n: usize, hot: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        false,
    )]));
    let auction: Vec<i64> = if hot {
        (0..n as i64)
            .map(|i| {
                if i % 4 == 0 {
                    i % 8 // 热键 0..8，同桶多行
                } else {
                    1_000_000 + i // 窗口内唯一键（窗口 ≥ 1M 行）
                }
            })
            .collect()
    } else {
        (0..n as i64).collect()
    };
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(auction)) as ArrayRef],
    )
    .unwrap()
}

/// q20 列式 each+snapshot join 段处理基准（4096 行/段 = 生产 ALERT_BATCH_SIZE，
/// 真实 join 索引窗口 1M 行）：全唯一键 vs 热键混合（Arc 共享）的 ns/row，
/// 附行式参考。
///
/// 运行：cargo test --release -p wf-engine each_bench -- --ignored --nocapture
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine each_bench -- --ignored --nocapture"]
fn q20_columnar_join_per_row() {
    const SEG: usize = 4096;
    const REPS: usize = 3000;
    const NANOS: i64 = 1_750_000_000_000_000_000;

    let exec = q20_plan_rule();
    assert!(
        exec.each_join_columnar_ready() && exec.each_plan_columnar_safe(),
        "q20 形状必须过列式 join 门"
    );
    let win = q20_auction_window(1_000_000);
    let lookup = Q20WinLookup { win: &win };

    for (name, hot) in [("unique", false), ("hot8+unique", true)] {
        let batch = q20_bid_batch(SEG, hot);
        let col_events: Vec<ColumnarEvent<'_>> =
            (0..SEG).map(|r| ColumnarEvent::new(&batch, r)).collect();
        let rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
            .iter()
            .enumerate()
            .map(|(i, ev)| (ev, NANOS + i as i64))
            .collect();

        // 预热（一次完整段 + 行式参考路径）。
        let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
        let mut idx = Vec::new();
        let s = exec.execute_each_direct_batch_columnar_join(
            &rows,
            &lookup,
            NANOS,
            &mut builder,
            &mut idx,
        );
        assert!(s.appended > 0, "{name}: 段必须有输出");

        let start = Instant::now();
        let mut total = 0usize;
        for _ in 0..REPS {
            let mut b = AlertColumnBuilder::new(Arc::from("alerts"));
            let mut out = Vec::new();
            let s = exec
                .execute_each_direct_batch_columnar_join(&rows, &lookup, NANOS, &mut b, &mut out);
            total += s.appended;
            std::hint::black_box(&out);
        }
        let per = start.elapsed().as_secs_f64() * 1e9 / (REPS as f64 * SEG as f64);
        let mps = 1e9 / per / 1e6;
        eprintln!(
            "[each-join-bench] columnar each+snapshot-join {:>12}: {:>7.1} ns/row  ({:>5.1}M rows/s)  appended/rep={}",
            name,
            per,
            mps,
            total / REPS
        );
    }

    // 行式参考（热键混合）：同 executor 的 execute_each_direct_batch。
    let batch = q20_bid_batch(SEG, true);
    let events: Vec<Event> = materialize_rows(&batch, &(0..SEG as u32).collect::<Vec<_>>());
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let start = Instant::now();
    let mut total = 0usize;
    for _ in 0..REPS {
        let mut b = AlertColumnBuilder::new(Arc::from("alerts"));
        let mut out = Vec::new();
        let s = exec.execute_each_direct_batch(&rows, &lookup, &[], NANOS, &mut b, &mut out);
        total += s.appended;
        std::hint::black_box(&out);
    }
    let per = start.elapsed().as_secs_f64() * 1e9 / (REPS as f64 * SEG as f64);
    let mps = 1e9 / per / 1e6;
    eprintln!(
        "[each-join-bench] row    each+snapshot-join {:>12}: {:>7.1} ns/row  ({:>5.1}M rows/s)  appended/rep={}",
        "hot8+unique",
        per,
        mps,
        total / REPS
    );
}
