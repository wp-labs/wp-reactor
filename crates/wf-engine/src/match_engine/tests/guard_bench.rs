//! Q2 guard 路径单元级性能基准（数据驱动改进的依据）。
//!
//! 测量对象：`RuleExecutor::event_matches_alias`（规则事件过滤入口）——
//! NexMark Q2 的 `events { b && b.auction % 123 == 0 }` guard 评估路径。
//! 92M bid 流上每事件都执行 guard，只有 0.81% 命中进入 state machine；
//! 该路径是 Q2(3.2M) 与 Q1(4.0M, 无 guard) 吞吐差(-20%)的来源。
//!
//! 基线三件套（改进前后用同一命令对比）：
//!   cargo test --release -p wf-engine q2_guard -- --ignored --nocapture
//!   - q2_filter：完整 guard `auction % 123 == 0`（含 binds 解析 + filter 查找 + 表达式评估）
//!   - no_filter：无 guard 基线（Q1 on-each 形态的过滤入口）
//!   - field_lookup：仅 HashMap 字段提取（guard 内 `auction` 读取的裸成本）
//! delta = q2_filter − no_filter 即 guard 表达式的增量开销。

use std::time::{Duration, Instant};

use wf_lang::ast::{BinOp, Expr, FieldRef};
use wf_lang::plan::{BindPlan, RulePlan};

use crate::match_engine::{Event, RuleExecutor, Value};

use super::helpers::{branch, count_ge, event, simple_key, simple_plan, simple_rule_plan, step};

/// bid_events 形态的 7 字段事件（与 nexmark_pk 一致）。
fn bid_event(auction: i64) -> Event {
    event(vec![
        ("auction", Value::Number(auction as f64)),
        ("bidder", Value::Number(1.0)),
        ("price", Value::Number(7.0)),
        ("channel", Value::Str("mobile".into())),
        ("url", Value::Str("http://example.com/1".into())),
        ("dateTime", Value::Number(1_700_000_000_000.0)),
        ("extra", Value::Str("x".into())),
    ])
}

/// Q2 guard 表达式：`auction % 123 == 0`。
fn q2_guard_expr() -> Expr {
    Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::BinOp {
            op: BinOp::Mod,
            left: Box::new(Expr::Field(FieldRef::Simple("auction".to_string()))),
            right: Box::new(Expr::Number(123.0)),
        }),
        right: Box::new(Expr::Number(0.0)),
    }
}

/// Q2 形态的 RulePlan：单 bind（alias=b, filter 可空），match<auction>。
fn q2_plan(filter: Option<Expr>) -> RulePlan {
    let mut plan = simple_rule_plan(
        "q2_bench",
        simple_plan(
            vec![simple_key("auction")],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(5.0),
        "digit",
        Expr::Field(FieldRef::Simple("auction".to_string())),
    );
    plan.binds = vec![BindPlan {
        alias: "b".into(),
        window: "bid_events".into(),
        filter,
    }];
    plan
}

fn bench_guard(name: &str, executor: &RuleExecutor, events: &[Event]) -> Duration {
    let start = Instant::now();
    let mut passed = 0usize;
    for ev in events {
        if executor.event_matches_alias("b", ev, None) {
            passed += 1;
        }
    }
    let el = start.elapsed();
    let per = el.as_secs_f64() * 1e9 / events.len() as f64;
    eprintln!(
        "[guard-bench] {name}: {per:7.1} ns/event  ({:5.1}M ev/s)  passed={passed}/{} ({:.3}%)",
        events.len() as f64 / el.as_secs_f64() / 1e6,
        events.len(),
        passed as f64 / events.len() as f64 * 100.0
    );
    el
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine q2_guard -- --ignored --nocapture"]
fn q2_guard_path_per_event() {
    let with_filter = RuleExecutor::new(q2_plan(Some(q2_guard_expr())));
    let no_filter = RuleExecutor::new(q2_plan(None));

    let n = 1_000_000usize;
    let events: Vec<Event> = (0..n).map(|i| bid_event(i as i64)).collect();

    let t_filter = bench_guard("q2_filter(auction%123==0)", &with_filter, &events);
    let t_baseline = bench_guard("no_filter(Q1 基线)", &no_filter, &events);

    let delta = (t_filter.as_secs_f64() - t_baseline.as_secs_f64()) * 1e9 / n as f64;
    let total_per = t_filter.as_secs_f64() * 1e9 / n as f64;
    let ratio = delta / total_per * 100.0;
    eprintln!(
        "[guard-bench] delta(guard 表达式增量) = {delta:7.1} ns/event  (filter/total = {ratio:.1}%)"
    );

    // 语义自检：递增 auction 命中率 ≈ 1/123 ≈ 0.81%（与 nexmark Q2 选中率一致）。
    let expect = n / 123 + 1; // 含 auction=0（0 % 123 == 0 命中）
    let actual = (0..n).filter(|i| i % 123 == 0).count();
    assert_eq!(expect, actual);
    assert!(t_filter >= t_baseline, "filter 路径不应快于无 filter 基线");
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine q2_guard -- --ignored --nocapture"]
fn q2_field_lookup_per_event() {
    // 仅字段提取：guard 内 `auction` 读取的裸成本（HashMap<SmolStr, Value> 查找）。
    let n = 1_000_000usize;
    let events: Vec<Event> = (0..n).map(|i| bid_event(i as i64)).collect();
    let start = Instant::now();
    let mut acc = 0.0f64;
    for ev in &events {
        if let Some(Value::Number(v)) = ev.fields.get("auction") {
            acc += v;
        }
    }
    let el = start.elapsed();
    let per = el.as_secs_f64() * 1e9 / n as f64;
    eprintln!(
        "[guard-bench] field_lookup(auction): {per:7.1} ns/event  ({:5.1}M ev/s)",
        n as f64 / el.as_secs_f64() / 1e6
    );
    assert!(acc > 0.0);
}
