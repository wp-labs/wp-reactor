//! 整条 advance 热路径微基准（c_sip_3 形态 count 规则）——2026-08-31
//! 「单 key 字符串规则列式直读 ScopeKey」优化的**整链**数据判断依据。
//!
//! 与 `scope_key_bench.rs`（提取微基准，只测新路径 API）的区别：本文件只依赖
//! `advance_at_with_masks_key`（签名在本次改动前后不变），因此同一份 bench 在
//! 新旧引擎下都能编译运行——A/B 用 `git stash push -- <4 个改动文件>` 切换。
//!
//! 运行（release-only）：
//!   cargo test --release -p wf-engine advance_count_bench -- --ignored --nocapture
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::match_engine::match_engine::{CepStateMachine, StepResult};
use crate::match_engine::{ColumnarEvent, build_field_index};

use super::helpers::{branch, count_ge, simple_key, simple_plan, step};

/// qradar conn_events 形态批（sip 长尾 10100、dport、blocked）。
fn qradar_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, false),
        Field::new("dport", DataType::Int64, false),
        Field::new("blocked", DataType::Boolean, false),
    ]));
    let sips: Vec<String> = (0..n)
        .map(|i| {
            // 源 IP 长尾 10100（对齐 gen_events.py），热 IP 每 ~99 事件重复。
            let ip = (i % 10100) as i64;
            format!("10.{}.{}.{}", ip / 256, ip % 256, 1)
        })
        .collect();
    let dports: Vec<i64> = (0..n).map(|i| 1024 + (i % 64000) as i64).collect();
    let blocked: Vec<bool> = (0..n).map(|i| i % 7 == 0).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(sips)) as ArrayRef,
            Arc::new(Int64Array::from(dports)) as ArrayRef,
            Arc::new(BooleanArray::from(blocked)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// c_sip_3 形态：match<sip:2m> count>=3，sliding 窗口，无 limits/close。
/// 复刻 rule_task deferred 路径的每行调用（with_index + advance_at_with_masks_key）。
#[test]
#[ignore]
fn bench_advance_count_rule_sliding() {
    run_count_advance("advance c_sip_3 (sliding 2m count>=3, 列式+index)", None);
}

/// 带 limits（qradar 真实规则：max_memory=512MB / max_instances=100000）的整条
/// advance——2026-08-31 limits 记账摊还的测量载体：旧代码每事件每规则跑
/// max_memory 检查，摊还后单步 count 规则（不可增长）只在新实例准入时检查。
#[test]
#[ignore]
fn bench_advance_count_rule_sliding_with_limits() {
    use wf_lang::plan::{ExceedAction, LimitsPlan};
    let limits = LimitsPlan {
        max_memory_bytes: Some(512 * 1024 * 1024),
        max_instances: Some(100_000),
        max_throttle: None,
        on_exceed: ExceedAction::Throttle,
        disk_provider: None,
        max_disk_bytes: None,
    };
    run_count_advance(
        "advance c_sip_3 + limits(512MB/100k, 摊还后)",
        Some(limits),
    );
}

fn run_count_advance(label: &str, limits: Option<wf_lang::plan::LimitsPlan>) {
    let n = 1_000_000usize;
    let batch = qradar_batch(n);
    let index = build_field_index(&batch);

    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("c", count_ge(3.0))])],
    );
    // qradar `match<sip:2m>`（simple_plan 默认 300s，显式对齐 2m）。
    plan.window_spec = wf_lang::plan::WindowSpec::Sliding(std::time::Duration::from_secs(120));
    let mut sm = match limits {
        Some(limits) => CepStateMachine::with_limits("c_sip_3".into(), plan, None, Some(limits)),
        None => CepStateMachine::new("c_sip_3".into(), plan, None),
    };
    let ts = 1_700_000_000_000_000_000i64;

    // 预热 100k 行：实例表进入稳态（热 IP 计数 + 到期）。
    for row in 0..100_000 {
        let ce = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
        std::hint::black_box(sm.advance_at_with_masks_key(
            "c",
            &ce,
            ts + row as i64,
            None,
            row,
            None,
            None,
        ));
    }

    let mut matched = 0usize;
    let start = Instant::now();
    for row in 100_000..n {
        let ce = ColumnarEvent::with_index(&batch, row, Arc::clone(&index));
        if let StepResult::Matched(_) = sm.advance_at_with_masks_key(
            "c",
            &ce,
            ts + row as i64,
            None,
            row,
            None,
            None,
        ) {
            matched += 1;
        }
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / (n - 100_000) as f64;
    std::hint::black_box(matched);
    eprintln!("{label}: {:.1} ns/event (matched={matched})", ns);
}

/// 发射路径语义锁定：count 规则命中后 `MatchedContext.scope_key` 必须等于键的
/// Value 列表（`flatten_scope_values` 重建，与 close 路径同款）——sip Str 键
/// → `[Value::Str("10.0.0.1")]`。防发射路径回归（早期 extract_key 重提/丢值）。
#[test]
fn advance_count_rule_matched_scope_key() {
    use crate::match_engine::Value;

    let schema = Arc::new(Schema::new(vec![Field::new("sip", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["10.0.0.1"; 3])) as ArrayRef],
    )
    .unwrap();
    let index = build_field_index(&batch);

    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("c", count_ge(3.0))])],
    );
    plan.window_spec = wf_lang::plan::WindowSpec::Sliding(std::time::Duration::from_secs(120));
    let mut sm = CepStateMachine::new("c_sip_3".into(), plan, None);

    let mut matched = None;
    for i in 0..3 {
        let ce = ColumnarEvent::with_index(&batch, i, Arc::clone(&index));
        if let StepResult::Matched(ctx) =
            sm.advance_at_with_masks_key("c", &ce, 1_000 + i as i64, None, i, None, None)
        {
            matched = Some(ctx);
        }
    }
    let ctx = matched.expect("count>=3 应在第 3 个事件命中");
    assert_eq!(ctx.scope_key, vec![Value::Str("10.0.0.1".into())]);
}

/// Bool match key 的发射 scope_key = `Str("true")`（flatten 规范化）。
///
/// 2026-08-31 review 决策记录：`ScopeKey` 无 Bool 变体（canonical 形式为
/// `Str("true"/"false")`，fanout 分片与 close 路径既有行为均如此）；事件路径
/// 本次对齐之。语料/测试集无 bool match key 规则，`build_eval_context` 以
/// scope_key 键绑定优先于事件字段，故锁此行为防意外漂移。
#[test]
fn advance_bool_key_matched_scope_key_is_canonical_str() {
    use crate::match_engine::Value;
    use arrow::array::BooleanArray;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "blocked",
        DataType::Boolean,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(BooleanArray::from(vec![true])) as ArrayRef],
    )
    .unwrap();
    let index = build_field_index(&batch);

    let mut plan = simple_plan(
        vec![simple_key("blocked")],
        vec![step(vec![branch("c", count_ge(1.0))])],
    );
    plan.window_spec = wf_lang::plan::WindowSpec::Sliding(std::time::Duration::from_secs(120));
    let mut sm = CepStateMachine::new("bool_key".into(), plan, None);

    let ce = ColumnarEvent::with_index(&batch, 0, Arc::clone(&index));
    match sm.advance_at_with_masks_key("c", &ce, 1_000, None, 0, None, None) {
        StepResult::Matched(ctx) => assert_eq!(ctx.scope_key, vec![Value::Str("true".into())]),
        other => panic!("expected Matched, got {:?}", other),
    }
}
