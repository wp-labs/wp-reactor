//! StatsExecutor spill 集成测试（M3，`docs/design/stats-state-spill-redb.md` §12）。
//!
//! 验证机制（数据驱动）：
//! 1. 超预算 → clock 驱逐最老键到 spill（内存/spill 不相交不变量）
//! 2. 驱逐键再来 → 读回（take）→ 计数继续累积，不丢
//! 3. close → drain + 并入内存 → 每键恰好一次、按 ScopeKey 升序
//! 4. **对拍契约**：spill 与否输出逐值一致（含 last 行字段跨序列化往返）
//! 5. 三层预算阶梯：落盘满 → 回退拒收（不丢内存键）
//! 6. redb store 生命周期：close 后文件删除

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan, WindowSpec};

use crate::match_engine::executor::stats_exec::{RowFieldLayout, StatsExecutor};
use crate::match_engine::match_engine::ScopeKey;
use crate::match_engine::spill::{MemSpillStore, RedbSpillStore};
use crate::match_engine::Value;

// ---------------------------------------------------------------------------
// helpers（与 stats_exec_test 同款）
// ---------------------------------------------------------------------------

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn count_measure(label: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Count,
        field: None,
        arg: None,
    }
}

fn sum_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Sum,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn last_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Last,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn keyed_plan(keys: Vec<Expr>, measures: Vec<StatsMeasurePlan>) -> StatsPlan {
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(1800)),
        keys,
        output_shape: StatsOutputShapePlan::Rows,
        measures,
        tracked_bind_fields: HashMap::new(),
    }
}

fn field_key(alias: &str, name: &str) -> Expr {
    Expr::Field(FieldRef::Qualified(alias.into(), name.into()))
}

fn row(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn extract(row: &HashMap<String, Value>, name: &str) -> Option<Value> {
    row.get(name).cloned()
}

/// count 单度量桶预算（StatsWindowState::bucket_allowance 口径: 256 + 80）。
const COUNT_ALLOWANCE: u64 = 336;

/// 开启 spill 的 executor（row 路径）：
/// `budget_buckets` = 内存可驻留桶数上限；`store` = 存储实现；
/// `subset` = 行字段子集（None = 无 last/top 度量，`StatsExecutor::new`）。
fn exec_with_spill(
    plan: StatsPlan,
    budget_buckets: usize,
    subset: Option<Arc<HashSet<String>>>,
    store: Option<Box<dyn crate::match_engine::spill::SpillStore + Send + Sync>>,
    max_spill_bytes: Option<usize>,
) -> StatsExecutor {
    let mut exec = match subset {
        Some(s) => StatsExecutor::with_row_fields(plan, Some(s)),
        None => StatsExecutor::new(plan),
    };
    let budget = COUNT_ALLOWANCE as usize * budget_buckets;
    exec.set_memory_limit("spill_test", Some(budget));
    exec.set_spill(store, max_spill_bytes);
    exec
}

/// 插入 bidder=k 的一行（count+sum(price)）。
fn bid_row(k: i64, price: f64) -> HashMap<String, Value> {
    row(&[("bidder", num(k as f64)), ("price", num(price))])
}

// ---------------------------------------------------------------------------
// 1. 驱逐 + 读回 + close 合并（Mem store）
// ---------------------------------------------------------------------------

#[test]
fn spill_evicts_over_budget_and_reads_back() {
    let plan = keyed_plan(
        vec![field_key("b", "bidder")],
        vec![count_measure("n"), sum_measure("total", "price")],
    );
    // count+sum 桶预算 = 256+80+80 = 416; 预算 4 桶 → limit 1344（内存驻留 3 桶）
    let mut exec = exec_with_spill(
        plan,
        4,
        None,
        Some(Box::new(MemSpillStore::new())),
        None,
    );

    // 10 个键各 2 行（键 1..=10 创建后都再命中一次——第二次命中已 spill 的键走读回）
    for k in 1..=10 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }
    for k in 1..=10 {
        exec.process_rows(&[bid_row(k, 2.0)], extract);
    }

    // 拒收必须为 0（spill 替代拒收，不丢键）
    assert_eq!(exec.window.over_limit_new_buckets(), 0, "spill 生效不应拒收");
    // 内存有界: 估算 ≤ 预算上限（读回也驱逐，严格有界）
    let est = exec.window.estimated_bytes();
    let limit = COUNT_ALLOWANCE * 4;
    assert!(
        est <= limit,
        "内存估算应 ≤ 预算上限 {limit}: {est}"
    );
    // 总数守恒: 内存桶 + spill 键 = 全部 10 键（不相交不变量）
    let in_memory = exec
        .window
        .buckets
        .values()
        .map(Vec::len)
        .sum::<usize>();
    let spilled = exec.window.spill_index.len();
    let readback = exec.window.readback.len();
    assert_eq!(
        in_memory + spilled,
        10,
        "内存({in_memory}) + spill({spilled}) = 10（不相交不变量）"
    );
    assert!(in_memory > 0 && spilled > 0, "应有键在内存与 spill 两侧");
    // take 只读化（M5-2）: 存储条目 = spill_index + 已读回（close 前仍在库中）
    assert_eq!(
        exec.window.spill.as_ref().unwrap().len(),
        spilled + readback,
        "存储条目 = spill_index + readback"
    );

    // close: 每键恰好一次、count=2、sum=3（读回键计数继续累积不丢）
    let closed = exec.close_window_by_bucket_rows();
    assert_eq!(closed.len(), 10, "10 键全部输出");
    let mut seen: HashSet<i64> = HashSet::new();
    for b in &closed {
        let ScopeKey::Int(k) = b.key else {
            panic!("期望 Int 键");
        };
        assert!(seen.insert(k), "键 {k} 重复输出");
        // 度量序: count, sum
        assert_eq!(b.measures[0][0].measure_value, 2.0, "键 {k} count");
        assert_eq!(b.measures[1][0].measure_value, 3.0, "键 {k} sum");
    }
    // close 后 spill 已并入并清空
    assert!(exec.window.spill.is_none() || exec.window.spill.as_ref().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// 2. 对拍契约：spill vs 不 spill 输出逐值一致（含 last 行字段序列化往返）
// ---------------------------------------------------------------------------

#[test]
fn spill_output_matches_no_spill() {
    // last 度量 + 行字段子集（q18 形态的缩小版）——读回键的 RowFields 经
    // 序列化/反序列化往返后必须逐字段一致。
    let subset = Arc::new(HashSet::from(["price".to_string(), "channel".to_string()]));
    let plan = keyed_plan(
        vec![field_key("b", "bidder")],
        vec![last_measure("last_price", "price"), count_measure("n")],
    );

    let make_rows = || {
        let mut rows = Vec::new();
        // 30 个键（预算 4 桶 → 大量 spill/读回）; 键 5 命中两次（读回路径）
        for k in 1..=30 {
            rows.push(row(&[
                ("bidder", num(k as f64)),
                ("price", num(k as f64 * 10.0)),
                ("channel", Value::Str(format!("ch{k}").into())),
            ]));
        }
        rows.push(row(&[
            ("bidder", num(5.0)),
            ("price", num(555.0)),
            ("channel", Value::Str("ch5b".into())),
        ]));
        rows.push(row(&[
            ("bidder", num(7.0)),
            ("price", num(777.0)),
            ("channel", Value::Str("ch7b".into())),
        ]));
        rows
    };

    // A: 不 spill
    let mut a = StatsExecutor::with_row_fields(plan.clone(), Some(subset.clone()));
    a.process_rows(&make_rows(), extract);
    let a_out = a.close_window_by_bucket_rows();

    // B: spill（预算 4 桶, Mem store, 与 A 同行字段子集——布局一致才对拍）
    let mut b = exec_with_spill(
        plan,
        4,
        Some(subset.clone()),
        Some(Box::new(MemSpillStore::new())),
        None,
    );
    b.process_rows(&make_rows(), extract);
    assert_eq!(b.window.over_limit_new_buckets(), 0);
    let b_out = b.close_window_by_bucket_rows();

    assert_eq!(a_out.len(), b_out.len(), "输出行数一致");
    assert_eq!(a_out.len(), 30, "30 键");
    for (x, y) in a_out.iter().zip(b_out.iter()) {
        assert_eq!(x.key, y.key, "键一致");
        assert_eq!(x.measures.len(), y.measures.len());
        for (mx, my) in x.measures.iter().zip(y.measures.iter()) {
            assert_eq!(mx.len(), my.len(), "度量条目数一致");
            for (ex, ey) in mx.iter().zip(my.iter()) {
                assert_eq!(ex.measure_value, ey.measure_value, "度量值一致");
                // last 行字段逐值对拍（序列化往返后不丢/不坏）
                match (&ex.row_fields, &ey.row_fields) {
                    (Some(rx), Some(ry)) => {
                        let nx: Vec<Option<Value>> = rx.iter_values().collect();
                        let ny: Vec<Option<Value>> = ry.iter_values().collect();
                        assert_eq!(nx, ny, "键 {:?} 行字段一致", x.key);
                    }
                    (None, None) => {}
                    _ => panic!("行字段存在性不一致 键 {:?}", x.key),
                }
            }
        }
    }
    // 读回键 5/7 的 last 值正确（555/777 而非旧值）
    let k5 = a_out
        .iter()
        .find(|b| b.key == ScopeKey::Int(5))
        .expect("键 5");
    assert_eq!(k5.measures[0][0].measure_value, 555.0, "键5 last=555");
    let k7 = b_out
        .iter()
        .find(|b| b.key == ScopeKey::Int(7))
        .expect("键 7");
    assert_eq!(k7.measures[0][0].measure_value, 777.0, "键7 last=777");
}

// ---------------------------------------------------------------------------
// 3. 三层预算阶梯：落盘满 → 回退拒收（不丢已建桶）
// ---------------------------------------------------------------------------

#[test]
fn spill_budget_ladder_falls_back_to_reject() {
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    // 落盘上限只够 2 键（预算 2 桶）——第 3 个键起 spill 满 → 拒收
    let max_spill = COUNT_ALLOWANCE as usize * 2;
    let mut exec = exec_with_spill(
        plan,
        2,
        None,
        Some(Box::new(MemSpillStore::new())),
        Some(max_spill),
    );
    for k in 1..=10 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }
    // 拒收必须 > 0（第 3 键起 spill 满, 回退拒收）
    assert!(
        exec.window.over_limit_new_buckets() > 0,
        "落盘满应回退拒收（计数>0）"
    );
    // 已建桶不丢: 内存 2 桶 + spill 2 键 = 4 键全在（其余拒收）
    let in_memory = exec
        .window
        .buckets
        .values()
        .map(Vec::len)
        .sum::<usize>();
    assert_eq!(in_memory, 2);
    assert_eq!(exec.window.spill_index.len(), 2);
    // close 输出 = 已接收的 4 键（每个 count=1）——无重复无半吊子
    let closed = exec.close_window_by_bucket_rows();
    assert_eq!(closed.len(), 4, "只输出被接收的 4 键");
    for b in &closed {
        assert_eq!(b.measures[0][0].measure_value, 1.0);
    }
}

// ---------------------------------------------------------------------------
// 4. redb store 全链路 + 文件生命周期
// ---------------------------------------------------------------------------

#[test]
fn spill_redb_full_pipeline_and_file_lifecycle() {
    let subset = Arc::new(HashSet::from(["price".to_string()]));
    let plan = keyed_plan(
        vec![field_key("b", "bidder")],
        vec![last_measure("last_price", "price")],
    );
    // row 路径 layout = all_other(子集名排序)——与 executor 行式路径一致
    let mut names: Vec<String> = subset.iter().cloned().collect();
    names.sort();
    let layout = Arc::new(RowFieldLayout::all_other(&names));
    let path = {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wf_spill_m3_{}_{}.rb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    };
    let store = RedbSpillStore::create(&path, layout).expect("create redb store");

    let mut exec = exec_with_spill(plan, 2, Some(subset.clone()), Some(Box::new(store)), None);
    for k in 1..=8 {
        exec.process_rows(&[bid_row(k, k as f64 * 10.0)], extract);
    }
    // 键 3 读回路径（spill 后再次命中）
    exec.process_rows(&[bid_row(3, 333.0)], extract);
    assert_eq!(exec.window.over_limit_new_buckets(), 0);

    let closed = exec.close_window_by_bucket_rows();
    assert_eq!(closed.len(), 8);
    let k3 = closed
        .iter()
        .find(|b| b.key == ScopeKey::Int(3))
        .expect("键 3");
    assert_eq!(k3.measures[0][0].measure_value, 333.0, "键3 读回后 last=333");

    // close（reset_window）→ cleanup → 文件删除
    assert!(!path.exists(), "close 后 redb 文件应删除");
}

// ---------------------------------------------------------------------------
// 5. StatsWindowState 直测：estimated_bytes 有界 + clock 一致性
// ---------------------------------------------------------------------------

#[test]
fn spill_estimated_bytes_bounded_by_budget() {
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let mut exec = exec_with_spill(
        plan,
        5,
        None,
        Some(Box::new(MemSpillStore::new())),
        None,
    );
    // 大量键（每次 process_rows 批末 refresh 重新核算——与增量账本对账）
    for batch in 0..50 {
        let rows: Vec<HashMap<String, Value>> = (1..=100)
            .map(|k| bid_row(batch * 100 + k as i64, 1.0))
            .collect();
        exec.process_rows(&rows, extract);
        let est = exec.window.estimated_bytes();
        assert!(
            est <= COUNT_ALLOWANCE * 5 + COUNT_ALLOWANCE,
            "批 {batch} 后内存估算有界: {est}"
        );
    }
    assert_eq!(exec.window.over_limit_new_buckets(), 0);
    // 5000 键全部保留（内存 + spill）
    let closed = exec.close_window_by_bucket_rows();
    assert_eq!(closed.len(), 5000);
}

// ---------------------------------------------------------------------------
// 6. M4 接线：set_spill_redb 延迟创建（layout 解析后建 store）
// ---------------------------------------------------------------------------

#[test]
fn spill_touch_counter_protects_recently_hit_key() {
    // 预算 2 桶（limit 672, 驱逐目标 336）: 键 1 创建后回访一次（touch=3）——
    // 应存活 3 轮驱逐扫描（每轮 -1），未回访键立即被驱逐。
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let mut exec = exec_with_spill(
        plan,
        2,
        None,
        Some(Box::new(MemSpillStore::new())),
        None,
    );
    exec.process_rows(&[bid_row(1, 1.0)], extract);
    exec.process_rows(&[bid_row(1, 2.0)], extract); // 回访 → touch=TOUCH_MAX
    for k in 2..=5 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }
    // 键 1 已挺过 3 轮驱逐（每轮被扫描减计数）；键 2/3/4 已 spill
    assert!(
        exec.window.find_bucket(&ScopeKey::Int(1)).is_some(),
        "回访键应存活 3 轮驱逐扫描"
    );
    assert_eq!(exec.window.spill_index.len(), 3, "键 2/3/4 已 spill");
    // 第 4 轮驱逐后键 1 才被淘汰（计数耗尽）
    exec.process_rows(&[bid_row(6, 1.0)], extract);
    assert!(
        exec.window.find_bucket(&ScopeKey::Int(1)).is_none(),
        "计数耗尽后键 1 应被驱逐"
    );
    assert_eq!(exec.window.spill_index.len(), 4, "键 1/2/3/4 已 spill（键 5 未扫描仍驻留）");
    // close: 全部 6 键恰好一次（含读回去重过滤）
    let closed = exec.close_window_by_bucket_rows();
    assert_eq!(closed.len(), 6);
    let k1 = closed.iter().find(|b| b.key == ScopeKey::Int(1)).expect("键1");
    assert_eq!(k1.measures[0][0].measure_value, 2.0, "键1 count=2（两次回访）");
}

#[test]
fn spill_redb_deferred_create_via_executor() {
    let subset = Arc::new(HashSet::from(["price".to_string()]));
    let plan = keyed_plan(
        vec![field_key("b", "bidder")],
        vec![last_measure("last_price", "price")],
    );
    let path = {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wf_spill_m4_{}_{}.rb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    };

    let mut exec = StatsExecutor::with_row_fields(plan, Some(subset));
    exec.set_memory_limit("spill_test", Some(COUNT_ALLOWANCE as usize * 2));
    exec.set_spill_redb(&path, None);
    // 未处理任何数据前不建 store（延迟创建）
    assert!(exec.window.spill.is_none(), "首次 process 前不创建 store");
    assert!(!path.exists(), "首次 process 前不落文件");

    // 首次 process（行式路径）→ 建 store（layout = all_other(子集)）
    for k in 1..=6 {
        exec.process_rows(&[bid_row(k, k as f64 * 10.0)], extract);
    }
    assert!(exec.window.spill.is_some(), "首次 process 后 store 已建");
    assert!(path.exists(), "spill 文件已落盘");
    assert_eq!(exec.window.over_limit_new_buckets(), 0, "spill 生效不拒收");
    // 读回键 3 后 last 值正确（跨序列化往返）
    exec.process_rows(&[bid_row(3, 333.0)], extract);

    let closed = exec.close_window_by_bucket_rows();
    assert_eq!(closed.len(), 6, "6 键全部输出");
    let k3 = closed
        .iter()
        .find(|b| b.key == ScopeKey::Int(3))
        .expect("键 3");
    assert_eq!(k3.measures[0][0].measure_value, 333.0, "键3 读回后 last=333");
    // close（reset_window → cleanup）→ 文件删除
    assert!(!path.exists(), "close 后 redb 文件应删除");
    // 下一窗口沿用同一路径（create 语义重建）——再 process 应重建 store
    exec.set_spill_redb(&path, None);
    exec.process_rows(&[bid_row(1, 1.0)], extract);
    assert!(exec.window.spill.is_some(), "下一窗口重建 store");
    assert!(path.exists());
}

// ---------------------------------------------------------------------------
// 7. M5-3 流式 close drain：分批读回 + 归并排序 + 内存有界
// ---------------------------------------------------------------------------

#[test]
fn spill_streaming_close_matches_batch_close() {
    // 流式 close（take_next_close_batch 循环）vs 非流式
    // （close_window_by_bucket_rows 全量）输出逐键一致——对拍契约。
    let subset = Arc::new(HashSet::from(["price".to_string()]));
    let plan = keyed_plan(
        vec![field_key("b", "bidder")],
        vec![last_measure("last_price", "price"), count_measure("n")],
    );
    let make_rows = || {
        let mut rows = Vec::new();
        // 30 键（预算 3 桶 → 大量 spill）；键 5/7 回访（读回路径）
        for k in 1..=30 {
            rows.push(row(&[
                ("bidder", num(k as f64)),
                ("price", num(k as f64 * 10.0)),
            ]));
        }
        rows.push(row(&[("bidder", num(5.0)), ("price", num(555.0))]));
        rows.push(row(&[("bidder", num(7.0)), ("price", num(777.0))]));
        rows
    };

    // A: 非流式（close_window_by_bucket_rows 全量）
    let mut a = exec_with_spill(
        plan.clone(),
        3,
        Some(subset.clone()),
        Some(Box::new(MemSpillStore::new())),
        None,
    );
    a.process_rows(&make_rows(), extract);
    let a_out = a.close_window_by_bucket_rows();

    // B: 流式（take_next_close_batch 分批, 批大小 5）
    let mut b = exec_with_spill(
        plan,
        3,
        Some(subset.clone()),
        Some(Box::new(MemSpillStore::new())),
        None,
    );
    b.process_rows(&make_rows(), extract);
    let mut b_keys = Vec::new();
    loop {
        let batch = b.take_next_close_batch(5);
        if batch.is_empty() {
            break;
        }
        // 批内必须有序（对拍契约）
        assert!(
            batch.windows(2).all(|w| w[0].0 <= w[1].0),
            "流式批内必须 ScopeKey 升序"
        );
        b_keys.extend(batch.into_iter().map(|(k, _)| k));
    }
    b.finish_close_window();

    // 流式输出 = 非流式输出：**键集合一致 + 每键恰好一次**（批间无序是既有
    // 契约——原 take_buckets_up_to 流式 close 同样批间无序, 设计 §9 已确认）。
    let mut a_keys: Vec<ScopeKey> = a_out.iter().map(|c| c.key.clone()).collect();
    a_keys.sort();
    let mut b_sorted = b_keys.clone();
    b_sorted.sort();
    assert_eq!(b_sorted, a_keys, "流式 vs 非流式键集合一致");
    assert_eq!(b_keys.len(), 30);
    assert_eq!(
        b_sorted.iter().collect::<std::collections::HashSet<_>>().len(),
        30,
        "每键恰好一次（无重复）"
    );
    // 读回键 5 在输出中（readback 过滤后不重复）
    assert!(b_keys.contains(&ScopeKey::Int(5)), "键5 在输出中");
}

#[test]
fn spill_streaming_close_memory_bounded() {
    // 流式 close 内存有界：spill 分批进来, buckets 不膨胀（每次取走后再
    // 读下一批）——close 峰值 = 批大小, 不是全量。
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let mut exec = exec_with_spill(
        plan,
        2,
        None,
        Some(Box::new(MemSpillStore::new())),
        None,
    );
    for k in 1..=20 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }
    let spilled = exec.window.spill_index.len();
    assert!(spilled > 0, "spill 已生效");
    // 流式 close：每批取走后 buckets 保持 ≤ 批大小（不累积全量）
    let mut total = 0usize;
    loop {
        let batch = exec.take_next_close_batch(4);
        if batch.is_empty() {
            break;
        }
        total += batch.len();
        let in_mem = exec.window.buckets.values().map(Vec::len).sum::<usize>();
        assert!(
            in_mem <= 4,
            "close 中 buckets 应 ≤ 批大小, 实测 {in_mem}"
        );
    }
    assert_eq!(total, 20, "全部键输出");
}
