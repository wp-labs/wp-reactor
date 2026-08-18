//! Q1 `on each` 列式路径的逐分量微基准——cut A/B/C/D 的 ns/行归因（数据版）。
//!
//! 与端到端切刀（q1-throughput-bisection.md §15）互补：切刀在 load/工具量化
//! 噪声下给「占比」，这里在同一进程内直接测每个分量的绝对 ns/行成本。
//!
//! 运行：
//!   cargo test --release -p wf-engine each_bench -- --ignored --nocapture
//!
//! 测量对象（Q1 `q1_bid_passthrough` 真实形状：score=1.0、entity=b.auction、
//! yield 4 字段 id/alert_type/detail/request_count）：
//!   baseline : `execute_each_direct_batch_columnar` 完整路径（256 行分段，同生产）
//!   wfx_id   : `build_each_wfx_id_columnar_reusing`（cut A：FNV 哈希 + 列渲染）
//!   fired_at : `format_nanos_utc`（cut B：UTC 格式化）
//!   entity   : `ColumnarEvent::field_value` + `value_to_string`（cut D）
//!   fill     : `begin_row` + 4×`stage_yield_cell` + `commit_each_row`（cut C）

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use smol_str::SmolStr;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{EachPlan, YieldField};
use wf_lang::{BaseType, FieldType};

use crate::alert::{AlertColumnBuilder, AlertOrigin, EachRowCells};
use crate::match_engine::event_bridge::{ColumnarEvent, sorted_fields_for};
use crate::match_engine::executor::{build_each_wfx_id_columnar_reusing, format_nanos_utc};
use crate::match_engine::match_engine::{field_ref_name, value_to_string};
use crate::match_engine::{RuleExecutor, Value};

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
    let auction: Vec<i64> = (0..n as i64).collect();
    let bidder = vec![1i64; n];
    let price = vec![7i64; n];
    let date_time = vec![1_700_000_000_000i64; n];
    let channel = vec!["mobile"; n];
    let url = vec!["http://example.com/1"; n];
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
    let sorted_fields = sorted_fields_for(&batch);

    // ---- baseline：完整列式路径（256 行分段，同生产 ALERT_BATCH_SIZE） ----
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let mut total_appended = 0usize;
    let start = Instant::now();
    for chunk in rows.chunks(ALERT_BATCH_SIZE) {
        let stats = exec.execute_each_direct_batch_columnar(
            chunk,
            &sorted_fields,
            NANOS,
            &mut builder,
            &mut appended,
        );
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

    // ---- cut A：wfx_id 哈希 + 列渲染 ----
    let origin = AlertOrigin::Event;
    let mut scratch = String::new();
    let start = Instant::now();
    let mut sum = 0usize;
    for (i, ev) in col_events.iter().enumerate() {
        let wfx = build_each_wfx_id_columnar_reusing(
            "q1_bid_passthrough",
            NANOS + i as i64,
            ev,
            &sorted_fields,
            &origin,
            &mut scratch,
        );
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
            wfx_id: String::from("wf_0000000000000000001"),
            score: 1.0,
            entity_id: String::from("1"),
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

    // ---- 归因校验：四项之和 ≈ baseline（差值 = 行访问/循环/遥测弥散开销） ----
    let sum_parts = a.per_ns + b.per_ns + c.per_ns + d.per_ns;
    eprintln!(
        "[each-bench] 分量之和 {:.1} ns/row vs baseline {:.1} ns/row（差值 = 弥散开销 {:.1} ns/row, {:.1}%）",
        sum_parts,
        baseline_ns,
        (baseline_ns - sum_parts).max(0.0),
        ((baseline_ns - sum_parts).max(0.0)) / baseline_ns * 100.0
    );
    assert!(
        c.per_ns > a.per_ns,
        "fill 不应小于 wfx_id（占比结论的 sanity check）"
    );
}
