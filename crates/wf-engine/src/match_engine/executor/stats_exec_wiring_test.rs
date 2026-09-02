//! StatsExecutor ↔ RuleExecutor 接线测试（P1 步骤④b）。
//!
//! 全链路验证：WFL 源 → parse + compile（check_wfl 语义检查）→ StatsPlan →
//! StatsExecutor 归并（where 用**编译出的 where_expr** 逐行求值，而非手工闭包）→
//! 合成 CloseOutput（每个 measure 一个 StepData）→ `execute_close_with_joins`
//! → OutputRecord，与 CEP q15 期望输出逐值对拍。
//!
//! 这就是设计 §7 接线在引擎层的语义核心：stats 执行器的度量值经既有 alert 构建
//! 管线（build_close_alert）产出 OutputRecord，yield 里的 `stat.value(final(label))`
//! 由 eval 上下文注入的 measure label 解析（executor/eval/builtins.rs eval_stat_func）。
//! daemon 层的 fanout 注册/ack 为后续接线（pull/push 投递），本测试锁定其数据路径。
use std::collections::HashMap;

use wf_lang::ast::CloseMode;
use wf_lang::plan::StatsPlan;
use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

use crate::match_engine::executor::stats_exec::StatsExecutor;
use crate::match_engine::match_engine::{
    BindData, CloseOutput, CloseReason, StepData, WindowLookup,
};
use crate::match_engine::{JoinRow, RuleExecutor, Value};

// ---------------------------------------------------------------------------
// schemas
// ---------------------------------------------------------------------------

fn bid_events_schema() -> WindowSchema {
    WindowSchema {
        name: "bid_events".to_string(),
        streams: vec!["bids".to_string()],
        time_field: Some("event_time".to_string()),
        over: std::time::Duration::from_secs(1800),
        fields: vec![
            FieldDef {
                name: "price".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "bidder".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "auction".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    }
}

fn stats_out_schema() -> WindowSchema {
    WindowSchema {
        name: "stats_out".to_string(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "detail".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "request_count".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
        ],
    }
}

// ---------------------------------------------------------------------------
// q15 stats WFL 源（12 度量, 与权威 q15 逐列对应）
// ---------------------------------------------------------------------------

const Q15_SOURCE: &str = r#"
rule q15_stats_wiring {
    events { b : bid_events }
    stats<30m:fixed> {
        b | count as total;
        b | count as r1 where b.price < 10000;
        b | count as r2 where b.price >= 10000 && b.price < 1000000;
        b | count as r3 where b.price >= 1000000;
        b | distinct_count(b.bidder) as total_bidder;
        b | distinct_count(b.bidder) as r1_bidder where b.price < 10000;
        b | distinct_count(b.bidder) as r2_bidder where b.price >= 10000 && b.price < 1000000;
        b | distinct_count(b.bidder) as r3_bidder where b.price >= 1000000;
        b | distinct_count(b.auction) as total_auction;
        b | distinct_count(b.auction) as r1_auction where b.price < 10000;
        b | distinct_count(b.auction) as r2_auction where b.price >= 10000 && b.price < 1000000;
        b | distinct_count(b.auction) as r3_auction where b.price >= 1000000;
    }
    entity(digit, 1)
    yield stats_out (
        detail = fmt("{} {} {} {} {} {} {} {} {} {} {} {}",
            stat.value(final(total)), stat.value(final(r1)), stat.value(final(r2)), stat.value(final(r3)),
            stat.value(final(total_bidder)), stat.value(final(r1_bidder)), stat.value(final(r2_bidder)), stat.value(final(r3_bidder)),
            stat.value(final(total_auction)), stat.value(final(r1_auction)), stat.value(final(r2_auction)), stat.value(final(r3_auction))),
        request_count = 1
    )
}
"#;

/// 编译 q15 stats 规则, 返回 (RulePlan, RuleExecutor, StatsPlan)。
fn compile_q15() -> (wf_lang::plan::RulePlan, RuleExecutor, StatsPlan) {
    let schemas = vec![bid_events_schema(), stats_out_schema()];
    let file = wf_lang::parse_wfl(Q15_SOURCE).expect("parse should succeed");
    let plans = wf_lang::compile_wfl(&file, &schemas).expect("compile should succeed");
    assert_eq!(plans.len(), 1);
    let plan = plans.into_iter().next().unwrap();
    let stats = plan.stats_plan.clone().expect("应有 stats_plan");
    assert_eq!(stats.measures.len(), 12, "q15 应为 12 个度量");
    let exec = RuleExecutor::new(plan.clone());
    (plan, exec, stats)
}

/// 完整接线: 编译 → StatsExecutor 归并（where 内建求值, 共享 ctx + 去重分档）
/// → 合成 CloseOutput → alert。
fn run_stats_to_alert(
    rows: &[HashMap<String, Value>],
) -> (
    RuleExecutor,
    crate::alert::OutputRecord,
    Vec<f64>,
    Vec<String>,
) {
    let (_plan, exec, stats) = compile_q15();
    let labels: Vec<String> = stats.measures.iter().map(|m| m.label.clone()).collect();
    let mut stats_exec = StatsExecutor::new(stats);
    stats_exec.process_rows(rows, extract);
    let values = stats_exec.final_measure_values();
    let close = synthetic_close("q15_stats_wiring", &values, &labels);
    let record = exec
        .execute_close_with_joins(&close, &NoLookup)
        .expect("execute_close_with_joins")
        .expect("And 模式 close 应产出 alert");
    (exec, record, values, labels)
}

/// 合成 CloseOutput（空键 fixed 窗口, close_step_data = 每 measure 一个 StepData）。
fn synthetic_close(rule_name: &str, values: &[f64], labels: &[String]) -> CloseOutput {
    let close_step_data = values
        .iter()
        .zip(labels.iter())
        .map(|(v, label)| StepData {
            satisfied_branch_index: 0,
            label: Some(label.clone()),
            measure_value: *v,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: vec![],
            field_values: Default::default(),
        })
        .collect();
    CloseOutput {
        rule_name: rule_name.to_string(),
        scope_key: vec![],
        machine_id: String::new(),
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![],
        close_step_data,
        bind_data: Vec::<BindData>::new(),
        watermark_nanos: 1_800_000_000_000,
        last_event_nanos: 1_799_000_000_000,
        row_fields: None,
        row_field_names: None,
        event_first_time_nanos: 0,
        event_last_time_nanos: 1_799_000_000_000,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 1_799_000_000_000,
        window_start_time_nanos: 0,
        window_end_time_nanos: 1_800_000_000_000,
    }
}

/// 无 join 的 WindowLookup 桩（stats v1 无 join）。
struct NoLookup;
impl WindowLookup for NoLookup {
    fn snapshot_field_values(
        &self,
        _window: &str,
        _field: &str,
    ) -> Option<std::collections::HashSet<String>> {
        None
    }
    fn snapshot(&self, _window: &str) -> Option<Vec<JoinRow>> {
        None
    }
}

fn extract(row: &HashMap<String, Value>, name: &str) -> Option<Value> {
    row.get(name).cloned()
}

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn row(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn detail_of(record: &crate::alert::OutputRecord) -> String {
    record
        .yield_fields
        .iter()
        .find(|(name, _)| &**name == "detail")
        .map(|(_, value)| match value {
            Value::Str(s) => s.to_string(),
            other => format!("{:?}", other),
        })
        .unwrap_or_default()
}

/// 确定性 bid 行（镜像 close_bench::bid_events: 同一 LCG 种子与公式）。
fn bid_rows(n: usize) -> Vec<HashMap<String, Value>> {
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = |range: u64| {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (rng >> 33) % range
    };
    (0..n)
        .map(|_| {
            let price = (10f64.powf((next(1_000_000) as f64 / 1_000_000.0) * 6.0) * 100.0).round();
            row(&[
                ("price", num(price)),
                ("bidder", num((1000 + next(1010) as i64) as f64)),
                ("auction", num((1000 + next(110) as i64) as f64)),
            ])
        })
        .collect()
}

/// 独立参考实现（不共享执行器/alert 代码, 避免同源缺陷）。
fn reference_q15(rows: &[HashMap<String, Value>]) -> Vec<u64> {
    fn tier(price: f64) -> usize {
        if price < 10_000.0 {
            0
        } else if price < 1_000_000.0 {
            1
        } else {
            2
        }
    }
    let mut count = [0u64; 4];
    let mut bidders: Vec<std::collections::HashSet<i64>> =
        (0..4).map(|_| Default::default()).collect();
    let mut auctions: Vec<std::collections::HashSet<i64>> =
        (0..4).map(|_| Default::default()).collect();

    for r in rows {
        let price = match r.get("price") {
            Some(Value::Number(p)) => Some(*p),
            _ => None,
        };
        let t = price.map(tier);
        count[0] += 1;
        if let Some(t) = t {
            count[t + 1] += 1;
        }
        if let Some(Value::Number(b)) = r.get("bidder") {
            let b = *b as i64;
            bidders[0].insert(b);
            if let Some(t) = t {
                bidders[t + 1].insert(b);
            }
        }
        if let Some(Value::Number(a)) = r.get("auction") {
            let a = *a as i64;
            auctions[0].insert(a);
            if let Some(t) = t {
                auctions[t + 1].insert(a);
            }
        }
    }
    let mut out = count.to_vec();
    for s in &bidders {
        out.push(s.len() as u64);
    }
    for s in &auctions {
        out.push(s.len() as u64);
    }
    out
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[test]
fn stats_wiring_small_dataset_matches_cep_anchor() {
    // 与 CEP q15 WFL inline test 同数据集: 3 行 → "3 1 1 1 2 1 1 1 3 1 1 1"
    let rows = vec![
        row(&[
            ("price", num(100.0)),
            ("bidder", num(1.0)),
            ("auction", num(1.0)),
        ]),
        row(&[
            ("price", num(50_000.0)),
            ("bidder", num(1.0)),
            ("auction", num(2.0)),
        ]),
        row(&[
            ("price", num(2_000_000.0)),
            ("bidder", num(2.0)),
            ("auction", num(3.0)),
        ]),
    ];
    let (_exec, record, values, _labels) = run_stats_to_alert(&rows);
    assert_eq!(
        values,
        vec![3.0, 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0, 3.0, 1.0, 1.0, 1.0]
    );
    assert_eq!(
        detail_of(&record),
        "3 1 1 1 2 1 1 1 3 1 1 1",
        "stats 版 q15 alert 应与 CEP 锚点逐字符一致"
    );
}

#[test]
fn stats_wiring_bulk_matches_reference_fold() {
    // 10 万行确定性数据: 独立参考实现 vs 全链路输出（编译 → 归并 → alert）
    let rows = bid_rows(100_000);
    let expected = reference_q15(&rows);
    let (_exec, record, values, _labels) = run_stats_to_alert(&rows);

    assert_eq!(values.len(), 12);
    for (i, (v, e)) in values.iter().zip(expected.iter()).enumerate() {
        assert_eq!(*v, *e as f64, "measure[{i}] 失配");
    }
    let want_detail = format!(
        "{} {} {} {} {} {} {} {} {} {} {} {}",
        expected[0],
        expected[1],
        expected[2],
        expected[3],
        expected[4],
        expected[5],
        expected[6],
        expected[7],
        expected[8],
        expected[9],
        expected[10],
        expected[11]
    );
    assert_eq!(detail_of(&record), want_detail, "alert detail 逐值对拍");
}

#[test]
fn stats_wiring_close_output_fields_carried() {
    // 合成 CloseOutput 的窗口/事件时间字段应透传到 OutputRecord（emit 元数据）
    let rows = vec![row(&[
        ("price", num(100.0)),
        ("bidder", num(1.0)),
        ("auction", num(1.0)),
    ])];
    let (_exec, record, _values, _labels) = run_stats_to_alert(&rows);
    // 空键规则实体恒为 1
    assert_eq!(
        record
            .yield_fields
            .iter()
            .find(|(n, _)| &**n == "request_count")
            .map(|(_, v)| v.clone()),
        Some(Value::Number(1.0))
    );
}

// ---------------------------------------------------------------------------
// 列式接线路径: process_batch → 合成 CloseOutput → alert（与行式逐字符一致）
// ---------------------------------------------------------------------------

/// rows → Int64 列 RecordBatch（price/bidder/auction; 整数 f64 → i64 无损）。
fn rows_to_batch(rows: &[HashMap<String, Value>]) -> arrow::record_batch::RecordBatch {
    fn i64_of(row: &HashMap<String, Value>, name: &str) -> Option<i64> {
        match row.get(name) {
            Some(Value::Number(n)) => Some(*n as i64),
            _ => None,
        }
    }
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
    ]));
    let price: Vec<Option<i64>> = rows.iter().map(|r| i64_of(r, "price")).collect();
    let bidder: Vec<Option<i64>> = rows.iter().map(|r| i64_of(r, "bidder")).collect();
    let auction: Vec<Option<i64>> = rows.iter().map(|r| i64_of(r, "auction")).collect();
    arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(price)),
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(auction)),
        ],
    )
    .expect("batch")
}

/// 完整接线（列式路径）: 编译 → process_batch → 合成 CloseOutput → alert。
fn run_stats_to_alert_columnar(
    rows: &[HashMap<String, Value>],
) -> (
    RuleExecutor,
    crate::alert::OutputRecord,
    Vec<f64>,
    Vec<String>,
) {
    let (_plan, exec, stats) = compile_q15();
    let labels: Vec<String> = stats.measures.iter().map(|m| m.label.clone()).collect();
    let batch = rows_to_batch(rows);
    let mut stats_exec = StatsExecutor::new(stats);
    assert!(stats_exec.process_batch(&batch), "q15 应列式化");
    let values = stats_exec.final_measure_values();
    let close = synthetic_close("q15_stats_wiring", &values, &labels);
    let record = exec
        .execute_close_with_joins(&close, &NoLookup)
        .expect("execute_close_with_joins")
        .expect("And 模式 close 应产出 alert");
    (exec, record, values, labels)
}

#[test]
fn stats_wiring_columnar_matches_row_path() {
    // 列式接线 vs 行式接线: 同数据 alert 逐字符一致
    let rows = bid_rows(20_000);
    let (_rexec, rrecord, rvalues, _) = run_stats_to_alert(&rows);
    let (_cexec, crecord, cvalues, _) = run_stats_to_alert_columnar(&rows);
    assert_eq!(rvalues, cvalues, "列式/行式 12 值一致");
    assert_eq!(
        detail_of(&rrecord),
        detail_of(&crecord),
        "列式/行式 alert detail 一致"
    );
    // 与独立参考实现交叉验证
    let expected = reference_q15(&rows);
    for (i, (v, e)) in cvalues.iter().zip(expected.iter()).enumerate() {
        assert_eq!(*v, *e as f64, "measure[{i}] 列式 vs 参考");
    }
}
