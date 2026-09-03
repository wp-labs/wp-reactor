//! 行循环端到端基准（2026-09-03，H 系列结构重构后新增）：直接驱动
//! `RuleTask::process_batch` 测 machine / on-each 双路行循环吞吐——machine 两种
//! 喂法（eager events / L2 deferred 列式）+ on-each eager。无 actor / 无 sleep，
//! 只作为结构性重构的前/后**回归信号**（同形状相对值）；绝对值随规则形状与
//! execution_path 变化，勿跨形状比较。
//!
//! 运行：
//!   cargo test --release -p wf-runtime row_loop -- --ignored --nocapture

use super::*;

/// 每批行数（machine 规则：前 `ROW_LOOP_POOL` 个 ip 循环铺行 → count>=3 命中路径
/// 每批存在；on-each：全行接受直发）。
const ROW_LOOP_BATCH_ROWS: usize = 10_000;
/// 批数（每路 ~1M 行）。
const ROW_LOOP_BATCHES: usize = 100;
const ROW_LOOP_POOL: usize = 100;
const ROW_LOOP_TS: i64 = 1_700_000_000_000_000_000i64;

fn row_loop_batch(rows: usize) -> (RecordBatch, Arc<Vec<Arc<wf_engine::match_engine::Event>>>) {
    let sips: Vec<String> = (0..rows)
        .map(|i| format!("10.0.0.{}", i % ROW_LOOP_POOL))
        .collect();
    let owned: Vec<&str> = sips.iter().map(String::as_str).collect();
    let batch = make_batch(&owned, ROW_LOOP_TS);
    let events = Arc::new(batch_to_events(&batch).into_iter().map(Arc::new).collect());
    (batch, events)
}

fn machine_row_loop_task() -> crate::engine_task::rule_task::RuleTask {
    let (plan, machine) = machine_rule();
    let (win, notify) = make_window("auth_events", &test_schema());
    make_task(Spec {
        plan,
        machine: Some(machine),
        window_sources: vec![crate::engine_task::task_types::WindowSource {
            window_name: "auth_events".into(),
            window: win,
            notify,
            aliases: vec!["fail".into()],
        }],
        metrics: Some(metrics()),
        ..Spec::default()
    })
}

fn each_row_loop_task() -> crate::engine_task::rule_task::RuleTask {
    let mut plan = minimal_plan();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    plan.entity_plan.entity_id_expr = Expr::Field(FieldRef::Qualified("b".into(), "sip".into()));
    let (win, notify) = make_window("auth_events", &test_schema());
    make_task(Spec {
        plan,
        each_alias: Some("b".into()),
        each_time_field: Some("event_time".into()),
        window_sources: vec![crate::engine_task::task_types::WindowSource {
            window_name: "auth_events".into(),
            window: win,
            notify,
            aliases: vec!["b".into()],
        }],
        metrics: Some(metrics()),
        ..Spec::default()
    })
}

fn print_row_loop(name: &str, total_ns: u128) {
    let rows = (ROW_LOOP_BATCH_ROWS * ROW_LOOP_BATCHES) as f64;
    let per = total_ns as f64 / rows;
    eprintln!(
        "[row-loop-{name}] rows={rows:.0} batches={ROW_LOOP_BATCHES} ns/row={per:.1} rows/s={:.0}",
        rows / (total_ns as f64 / 1e9)
    );
}

/// machine 行循环 × eager events 喂法（batch=None，RowEvent::Eager 逐行推进）。
#[test]
#[ignore = "row-loop release bench"]
fn row_loop_machine_eager_bench() {
    let (batch, events) = row_loop_batch(ROW_LOOP_BATCH_ROWS);
    let mut task = machine_row_loop_task();
    run_with_dispatch(tracing::Dispatch::none(), move || async move {
        // warm-up（首次含懒初始化，不计时）。
        task.process_batch("auth_events", 0, None, Some(&events), None, None, None)
            .await;
        let start = Instant::now();
        for seq in 1..=ROW_LOOP_BATCHES as u64 {
            task.process_batch("auth_events", seq, None, Some(&events), None, None, None)
                .await;
        }
        print_row_loop("machine-eager", start.elapsed().as_nanos());
        let _ = &batch;
    });
}

/// machine 行循环 × L2 deferred 列式喂法（batch=Some，DeferredRows +
/// ColumnarEvent 命中行视图）。
#[test]
#[ignore = "row-loop release bench"]
fn row_loop_machine_deferred_columnar_bench() {
    let (batch, _events) = row_loop_batch(ROW_LOOP_BATCH_ROWS);
    let mut task = machine_row_loop_task();
    run_with_dispatch(tracing::Dispatch::none(), move || async move {
        task.process_batch("auth_events", 0, None, None, Some(&batch), None, None)
            .await;
        let start = Instant::now();
        for seq in 1..=ROW_LOOP_BATCHES as u64 {
            task.process_batch("auth_events", seq, None, None, Some(&batch), None, None)
                .await;
        }
        print_row_loop("machine-deferred-columnar", start.elapsed().as_nanos());
    });
}

/// on-each 行循环 × eager events 喂法（每行直发/向量化收口路径）。
#[test]
#[ignore = "row-loop release bench"]
fn row_loop_each_rows_bench() {
    let (batch, events) = row_loop_batch(ROW_LOOP_BATCH_ROWS);
    let mut task = each_row_loop_task();
    run_with_dispatch(tracing::Dispatch::none(), move || async move {
        task.process_batch("auth_events", 0, None, Some(&events), None, None, None)
            .await;
        let start = Instant::now();
        for seq in 1..=ROW_LOOP_BATCHES as u64 {
            task.process_batch("auth_events", seq, None, Some(&events), None, None, None)
                .await;
        }
        print_row_loop("each-rows", start.elapsed().as_nanos());
        let _ = &batch;
    });
}
