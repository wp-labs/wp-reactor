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

use crate::match_engine::Value;
use crate::match_engine::executor::stats_exec::{RowFieldLayout, StatsExecutor};
use crate::match_engine::match_engine::ScopeKey;
use crate::match_engine::spill::{MemSpillStore, RedbSpillStore, SpillStore};

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

fn distinct_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::DistinctCount,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn top_measure(label: &str, field: &str, n: u64) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Top,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: Some(n),
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

/// 计划的实际桶预算（`bucket_allowance` 口径）——2026-08-27 远程 SoA 重构后
/// count 单度量走 Numeric 载体（264B）, 含 last/top 走 Classic（2.2 校准后
/// 739B）, 单一常量失效。预算语义是「桶数」, 用 allowance 换算。
fn allowance_for(plan: &StatsPlan) -> u64 {
    let soa = plan.measures.iter().all(|m| {
        matches!(
            m.agg,
            StatsAggPlan::Count
                | StatsAggPlan::Sum
                | StatsAggPlan::Avg
                | StatsAggPlan::Min
                | StatsAggPlan::Max
        )
    });
    crate::match_engine::executor::stats_exec::StatsWindowState::bucket_allowance(plan, soa)
}

/// 开启 spill 的 executor（row 路径）：
/// `budget_buckets` = 内存可驻留桶数上限；`store` = 存储实现；
/// `subset` = 行字段子集（None = 无 last/top 度量，`StatsExecutor::new`）。
fn exec_with_spill(
    plan: &StatsPlan,
    budget_buckets: usize,
    subset: Option<Arc<HashSet<String>>>,
    store: Option<Box<dyn crate::match_engine::spill::SpillStore + Send + Sync>>,
    max_spill_bytes: Option<usize>,
) -> StatsExecutor {
    let mut exec = match subset {
        Some(s) => StatsExecutor::with_row_fields(plan.clone(), Some(s)),
        None => StatsExecutor::new(plan.clone()),
    };
    let budget = allowance_for(plan) as usize * budget_buckets;
    exec.set_memory_limit("spill_test", Some(budget));
    exec.set_spill(store, max_spill_bytes, None);
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
    let mut exec = exec_with_spill(&plan, 4, None, Some(Box::new(MemSpillStore::new())), None);

    // 10 个键各 2 行（键 1..=10 创建后都再命中一次——第二次命中已 spill 的键走读回）
    for k in 1..=10 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }
    for k in 1..=10 {
        exec.process_rows(&[bid_row(k, 2.0)], extract);
    }

    // 拒收必须为 0（spill 替代拒收，不丢键）
    assert_eq!(
        exec.window.over_limit_new_buckets(),
        0,
        "spill 生效不应拒收"
    );
    // 内存有界: 估算 ≤ 预算上限（读回也驱逐，严格有界）
    let est = exec.window.estimated_bytes();
    let limit = allowance_for(&plan) * 4;
    assert!(est <= limit, "内存估算应 ≤ 预算上限 {limit}: {est}");
    // 总数守恒: 内存桶 + spill 键 = 全部 10 键（不相交不变量）
    let in_memory = exec.window.buckets.values().map(Vec::len).sum::<usize>();
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
        &plan,
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
    let max_spill = allowance_for(&plan) as usize * 2;
    let mut exec = exec_with_spill(
        &plan,
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
    let in_memory = exec.window.buckets.values().map(Vec::len).sum::<usize>();
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

    let mut exec = exec_with_spill(&plan, 2, Some(subset.clone()), Some(Box::new(store)), None);
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
    assert_eq!(
        k3.measures[0][0].measure_value, 333.0,
        "键3 读回后 last=333"
    );

    // close（reset_window）→ cleanup → 文件删除
    assert!(!path.exists(), "close 后 redb 文件应删除");
}

// ---------------------------------------------------------------------------
// 5. StatsWindowState 直测：estimated_bytes 有界 + clock 一致性
// ---------------------------------------------------------------------------

#[test]
fn spill_estimated_bytes_bounded_by_budget() {
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let mut exec = exec_with_spill(&plan, 5, None, Some(Box::new(MemSpillStore::new())), None);
    // 大量键（每次 process_rows 批末 refresh 重新核算——与增量账本对账）
    for batch in 0..50 {
        let rows: Vec<HashMap<String, Value>> = (1..=100)
            .map(|k| bid_row(batch * 100 + k as i64, 1.0))
            .collect();
        exec.process_rows(&rows, extract);
        let est = exec.window.estimated_bytes();
        assert!(
            est <= allowance_for(&plan) * 5 + allowance_for(&plan),
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
    let mut exec = exec_with_spill(&plan, 2, None, Some(Box::new(MemSpillStore::new())), None);
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
    assert_eq!(
        exec.window.spill_index.len(),
        4,
        "键 1/2/3/4 已 spill（键 5 未扫描仍驻留）"
    );
    // close: 全部 6 键恰好一次（含读回去重过滤）
    let closed = exec.close_window_by_bucket_rows();
    assert_eq!(closed.len(), 6);
    let k1 = closed
        .iter()
        .find(|b| b.key == ScopeKey::Int(1))
        .expect("键1");
    assert_eq!(
        k1.measures[0][0].measure_value, 2.0,
        "键1 count=2（两次回访）"
    );
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

    let mut exec = StatsExecutor::with_row_fields(plan.clone(), Some(subset));
    // last 度量每桶 allowance ≈ 845（含行字段共享 2.2 校准）——预算 3 桶:
    // 3 键 2535 = 预算恰好不驱逐, 第 4 键超限触发驱逐。
    exec.set_memory_limit("spill_test", Some(allowance_for(&plan) as usize * 3));
    exec.set_spill_redb(&path, None, None);
    // 未处理任何数据前不建 store（延迟创建）
    assert!(exec.window.spill.is_none(), "首次 process 前不创建 store");
    assert!(!path.exists(), "首次 process 前不落文件");

    // 预算内 process（3 键, 不驱逐）→ 仍不建 store（P0 修复: 零驱逐窗口零开销）
    exec.process_rows(
        &[bid_row(1, 10.0), bid_row(2, 20.0), bid_row(3, 30.0)],
        extract,
    );
    assert!(exec.window.spill.is_none(), "零驱逐窗口不建 store");
    assert!(!path.exists(), "零驱逐窗口不落文件");
    assert_eq!(exec.window.over_limit_new_buckets(), 0, "预算内不拒收");

    // 第 4 键超限 → 驱逐 → 首次创建 store（惰性）
    exec.process_rows(&[bid_row(4, 40.0)], extract);
    assert!(exec.window.spill.is_some(), "首次驱逐时创建 store");
    assert!(path.exists(), "spill 文件已落盘");
    assert_eq!(exec.window.over_limit_new_buckets(), 0, "spill 生效不拒收");
    // 键 1 被驱逐（最老）后回访 → 读回路径（跨序列化往返）
    exec.process_rows(&[bid_row(1, 111.0)], extract);

    let closed = exec.close_window_by_bucket_rows();
    assert_eq!(closed.len(), 4, "4 键全部输出");
    let k1 = closed
        .iter()
        .find(|b| b.key == ScopeKey::Int(1))
        .expect("键 1");
    assert_eq!(
        k1.measures[0][0].measure_value, 111.0,
        "键1 读回后 last=111"
    );
    // close（reset_window → cleanup）→ 文件删除
    assert!(!path.exists(), "close 后 redb 文件应删除");
    // 下一窗口沿用同一路径（create 语义重建）——零驱逐不建, 驱逐时重建
    exec.set_spill_redb(&path, None, None);
    exec.process_rows(&[bid_row(1, 1.0)], extract);
    assert!(exec.window.spill.is_none(), "新窗口零驱逐不建 store");
    exec.process_rows(
        &[bid_row(2, 2.0), bid_row(3, 3.0), bid_row(4, 4.0)],
        extract,
    );
    assert!(exec.window.spill.is_some(), "新窗口驱逐时重建 store");
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
        &plan,
        3,
        Some(subset.clone()),
        Some(Box::new(MemSpillStore::new())),
        None,
    );
    a.process_rows(&make_rows(), extract);
    let a_out = a.close_window_by_bucket_rows();

    // B: 流式（take_next_close_batch 分批, 批大小 5）
    let mut b = exec_with_spill(
        &plan,
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
        b_sorted
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
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
    let mut exec = exec_with_spill(&plan, 2, None, Some(Box::new(MemSpillStore::new())), None);
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
        assert!(in_mem <= 4, "close 中 buckets 应 ≤ 批大小, 实测 {in_mem}");
    }
    assert_eq!(total, 20, "全部键输出");
}

// ---------------------------------------------------------------------------
// 8. max_disk 规则级共享预算（2026-08-27, 旧键 max_spill_bytes 别名）
// ---------------------------------------------------------------------------

/// 开启 spill + 规则级共享落盘计数（模拟 spawn 层为同规则分片注入的同一个
/// `Arc<AtomicU64>`）：预算仍按桶数给定, 计数跨 executor 共享。
fn exec_with_shared_spill(
    plan: &StatsPlan,
    budget_buckets: usize,
    max_spill_bytes: Option<usize>,
    shared: &std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> StatsExecutor {
    let mut exec = StatsExecutor::new(plan.clone());
    let budget = allowance_for(plan) as usize * budget_buckets;
    exec.set_memory_limit("spill_shared", Some(budget));
    exec.set_spill(
        Some(Box::new(MemSpillStore::new())),
        max_spill_bytes,
        Some(std::sync::Arc::clone(shared)),
    );
    exec
}

/// 规则级共享预算语义（两个 executor 模拟同规则两个分片）：
/// 1. 各自驱逐 → 落盘计数跨分片累计（a{1,2,3} + b{7,8} = 5×allowance_for(&plan)）
/// 2. 共享预算耗尽 → 另一分片驱逐回退拒收（规则总上限, 与分片数无关）
/// 3. 各自 close → 只扣减自己的份额（预算随窗口释放可复用）
#[test]
fn spill_shared_counter_rule_budget() {
    use std::sync::atomic::Ordering;
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    // 规则总上限 = 5 桶（a 驱逐 3 + b 驱逐 2）, 与「每分片 3 桶内存预算」无关。
    let shared = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let limit = (allowance_for(&plan) * 5) as usize;
    let mut a = exec_with_shared_spill(&plan, 3, Some(limit), &shared);
    let mut b = exec_with_shared_spill(&plan, 3, Some(limit), &shared);

    // a: 6 键 → 内存 3 + spill {1,2,3}（第 6 键的驱逐仍在共享预算内）
    for k in 1..=6 {
        a.process_rows(&[bid_row(k, 1.0)], extract);
    }
    assert_eq!(a.window.over_limit_new_buckets(), 0, "a 不应拒收");
    assert_eq!(
        shared.load(Ordering::SeqCst),
        allowance_for(&plan) * 3,
        "a 落盘 3 键"
    );

    // b: 12 键 → 内存 3 + spill {7,8}; 第 12 键驱逐时共享预算耗尽 → 拒收。
    //   （b 自身 store 还有余量——拒收源于**规则总上限**, 即共享语义生效）
    for k in 7..=12 {
        b.process_rows(&[bid_row(k, 1.0)], extract);
    }
    assert_eq!(
        shared.load(Ordering::SeqCst),
        allowance_for(&plan) * 5,
        "a{{1,2,3}}+b{{7,8}} 跨分片累计"
    );
    assert_eq!(
        b.window.over_limit_new_buckets(),
        1,
        "b 第 12 键被共享上限拒收"
    );

    // a close → 只扣 a 的份额（3 键）, 共享计数剩 b 的 2 键
    let a_out = a.close_window_by_bucket_rows();
    assert_eq!(a_out.len(), 6, "a 输出 6 键（内存 3 + spill 3）");
    assert_eq!(
        shared.load(Ordering::SeqCst),
        allowance_for(&plan) * 2,
        "close 后只剩 b 的落盘份额"
    );

    // b close → 扣 b 的份额（2 键）→ 归零（预算释放, 下一窗口可复用）
    let b_out = b.close_window_by_bucket_rows();
    assert_eq!(b_out.len(), 5, "b 输出 5 键（内存 3 + spill 2, 拒收 1）");
    assert_eq!(
        shared.load(Ordering::SeqCst),
        0,
        "全部 close 后共享计数归零"
    );
}

/// 规则级共享内存预算（两个 executor 模拟同规则两个分片）：
/// 1. `max_memory` = 规则**总**驻留上限——A 用满后 B 的首个新键被拒收
///    （旧语义 2GB/片 × 10 = 20GB, 用户配 2GB 实际给到 20GB, 语义错误）
/// 2. spill 驱逐腾内存 → 共享内存计数回落（内存有界不破总量）
/// 3. 各自 close → 释放自己的份额 → 预算归零, 后续可复用
#[test]
fn mem_shared_counter_rule_budget() {
    use std::sync::atomic::Ordering;
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let shared = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let limit = (allowance_for(&plan) * 3) as usize; // 规则总内存 = 3 桶

    let mk = |with_spill: bool| -> StatsExecutor {
        let mut exec = StatsExecutor::new(plan.clone());
        exec.set_memory_limit_shared(
            "mem_shared",
            Some(limit),
            Some(std::sync::Arc::clone(&shared)),
        );
        if with_spill {
            exec.set_spill(Some(Box::new(MemSpillStore::new())), None, None);
        }
        exec
    };

    // A: 有 spill, feed 6 键 → 内存 3 桶 + spill 3 键（驱逐回落共享计数）
    let mut a = mk(true);
    for k in 1..=6 {
        a.process_rows(&[bid_row(k, 1.0)], extract);
    }
    assert_eq!(a.window.over_limit_new_buckets(), 0, "A 驱逐不拒收");
    assert_eq!(a.window.spill_index.len(), 3, "A spill 3 键");
    assert_eq!(
        shared.load(Ordering::SeqCst),
        allowance_for(&plan) * 3,
        "A 内存占满 3 桶（驱逐回落再建桶, 不破总量）"
    );

    // B: 无 spill, 首个键尝试建桶 → 共享已满（A 占 3 桶）→ 拒收（规则级语义）
    let mut b = mk(false);
    b.process_rows(&[bid_row(7, 1.0)], extract);
    assert_eq!(
        b.window.over_limit_new_buckets(),
        1,
        "B 首个键被共享总量拒收（规则内存满, 不是每片配额）"
    );

    // A close → 释放自身份额 → 共享归零 → B 可再建
    let a_out = a.close_window_by_bucket_rows();
    assert_eq!(a_out.len(), 6, "A 输出 6 键（内存 3 + spill 3）");
    assert_eq!(shared.load(Ordering::SeqCst), 0, "A close 释放全部份额");
    b.process_rows(&[bid_row(7, 1.0)], extract);
    assert_eq!(
        b.window.over_limit_new_buckets(),
        1,
        "释放后 B 键 7 建桶（拒收不增）"
    );
    let b_out = b.close_window_by_bucket_rows();
    assert_eq!(b_out.len(), 1, "B 输出 1 键");
    assert_eq!(shared.load(Ordering::SeqCst), 0, "B close 后共享归零");
}

// ---------------------------------------------------------------------------
// 9. distinct / top 度量驱逐-读回全链路（对拍契约, 2026-08-27 review 补）
// ---------------------------------------------------------------------------

/// distinct 度量全链路：驱逐落盘 → 回访读回 → DistinctSet 合并——集合不丢。
/// 序列化往返单测在 spill.rs 已覆盖, 这里补「驱逐-读回-合并」的机制层验证。
#[test]
fn spill_distinct_readback_merges() {
    let subset = None;
    let plan = keyed_plan(
        vec![field_key("b", "bidder")],
        vec![distinct_measure("d", "price")],
    );

    let make_rows = || {
        let mut rows = Vec::new();
        // 30 键 × 各 2 个 price 值（预算 4 桶 → 大量驱逐/读回）; 键 5 第三次
        // 行触发读回（spill 后回访, DistinctSet 需合并旧集合 + 新值）。
        for k in 1..=30 {
            rows.push(row(&[
                ("bidder", num(k as f64)),
                ("price", num(100.0 + k as f64)),
            ]));
            rows.push(row(&[
                ("bidder", num(k as f64)),
                ("price", num(200.0 + k as f64)),
            ]));
        }
        rows.push(row(&[("bidder", num(5.0)), ("price", num(999.0))]));
        rows
    };

    // A: 不 spill（参照）
    let mut a = StatsExecutor::new(plan.clone());
    a.process_rows(&make_rows(), extract);
    let a_out = a.close_window_by_bucket_rows();

    // B: spill（预算 4 桶）
    let mut b = exec_with_spill(&plan, 4, subset, Some(Box::new(MemSpillStore::new())), None);
    b.process_rows(&make_rows(), extract);
    assert_eq!(b.window.over_limit_new_buckets(), 0, "spill 不拒收");
    let b_out = b.close_window_by_bucket_rows();

    assert_eq!(a_out.len(), b_out.len(), "输出行数一致");
    assert_eq!(a_out.len(), 30, "30 键");
    for (x, y) in a_out.iter().zip(b_out.iter()) {
        assert_eq!(x.key, y.key, "键一致");
        // distinct count: 每键 2 个值（键 5 有第 3 个）
        let expect = if x.key == ScopeKey::Int(5) { 3.0 } else { 2.0 };
        assert_eq!(
            x.measures[0][0].measure_value, expect,
            "参照 键 {:?} distinct count",
            x.key
        );
        assert_eq!(
            y.measures[0][0].measure_value, expect,
            "spill 键 {:?} distinct count（驱逐读回后集合完整）",
            y.key
        );
    }
}

/// top 度量全链路：驱逐落盘 → 读回 → TopEntry（含行字段）完整、rank 序正确。
/// top 是行序敏感度量（spawn 门控不分片）——spill 读回后条目与不 spill 一致。
#[test]
fn spill_top_readback_roundtrip() {
    let subset = Arc::new(HashSet::from(["price".to_string(), "channel".to_string()]));
    let plan = keyed_plan(
        vec![field_key("b", "bidder")],
        vec![top_measure("t", "price", 3)],
    );

    let make_rows = || {
        let mut rows = Vec::new();
        // 30 键 × 3 行（top 3 全满）; 键 5 第 4 行触发读回后 top 更新。
        for k in 1..=30 {
            for v in 1..=3 {
                rows.push(row(&[
                    ("bidder", num(k as f64)),
                    ("price", num(k as f64 * 10.0 + v as f64)),
                    ("channel", Value::Str(format!("ch{k}").into())),
                ]));
            }
        }
        rows.push(row(&[
            ("bidder", num(5.0)),
            ("price", num(999.0)),
            ("channel", Value::Str("ch5b".into())),
        ]));
        rows
    };

    // A: 不 spill（参照）
    let mut a = StatsExecutor::with_row_fields(plan.clone(), Some(subset.clone()));
    a.process_rows(&make_rows(), extract);
    let a_out = a.close_window_by_bucket_rows();

    // B: spill（预算 4 桶）
    let mut b = exec_with_spill(
        &plan,
        4,
        Some(subset.clone()),
        Some(Box::new(MemSpillStore::new())),
        None,
    );
    b.process_rows(&make_rows(), extract);
    assert_eq!(b.window.over_limit_new_buckets(), 0, "spill 不拒收");
    let b_out = b.close_window_by_bucket_rows();

    assert_eq!(a_out.len(), b_out.len(), "输出行数一致");
    assert_eq!(a_out.len(), 30, "30 键");
    for (x, y) in a_out.iter().zip(b_out.iter()) {
        assert_eq!(x.key, y.key, "键一致");
        // top 条目: 键 5 = [999, 53, 52]（第 4 行插入后 51 被挤出）; 其余 = 3 条
        let expect_top = if x.key == ScopeKey::Int(5) {
            vec![999.0, 53.0, 52.0]
        } else {
            let k = match x.key {
                ScopeKey::Int(k) => k,
                _ => panic!("Int 键"),
            };
            vec![
                k as f64 * 10.0 + 3.0,
                k as f64 * 10.0 + 2.0,
                k as f64 * 10.0 + 1.0,
            ]
        };
        let vals_a: Vec<f64> = x.measures[0].iter().map(|e| e.measure_value).collect();
        let vals_b: Vec<f64> = y.measures[0].iter().map(|e| e.measure_value).collect();
        assert_eq!(vals_a, expect_top, "参照 键 {:?} top", x.key);
        assert_eq!(
            vals_b, expect_top,
            "spill 键 {:?} top（读回后 rank 序一致）",
            y.key
        );
        // 行字段逐值对拍（TopEntry 的 row 序列化往返不丢）
        for (ex, ey) in x.measures[0].iter().zip(y.measures[0].iter()) {
            let nx: Vec<Option<Value>> = ex
                .row_fields
                .as_ref()
                .map(|r| r.iter_values().collect())
                .unwrap_or_default();
            let ny: Vec<Option<Value>> = ey
                .row_fields
                .as_ref()
                .map(|r| r.iter_values().collect())
                .unwrap_or_default();
            assert_eq!(nx, ny, "键 {:?} top 行字段一致", x.key);
        }
    }
}

// ---------------------------------------------------------------------------
// 10. P0 惰性创建 × 规则级共享计数（set_spill_redb 路径, 2026-08-27 review 补）
// ---------------------------------------------------------------------------

/// 惰性创建与共享落盘计数的联动（两个 executor 模拟同规则两分片）:
/// 1. 预算内零驱逐 → store 未创建（惰性）
/// 2. 首次驱逐 → 惰性创建 store, 驱逐记账走 ensure 预置的共享计数
/// 3. 共享预算耗尽 → 另一分片驱逐回退拒收
/// 4. 各自 close → 扣自身份额（预算跨窗口复用）
/// 覆盖 spawn 注入路径（set_spill_redb）而非测试直连的 set_spill。
#[test]
fn spill_lazy_create_shared_counter_rule_budget() {
    use std::sync::atomic::Ordering;
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let shared = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let limit = (allowance_for(&plan) * 5) as usize; // 规则总落盘 = 5 桶

    let mk = |tag: &str| -> (StatsExecutor, std::path::PathBuf) {
        let mut exec = StatsExecutor::new(plan.clone());
        exec.set_memory_limit(tag, Some(allowance_for(&plan) as usize * 3));
        let path = {
            let mut p = std::env::temp_dir();
            p.push(format!(
                "wf_spill_lazy_shared_{}_{}_{}.rb",
                tag,
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            p
        };
        exec.set_spill_redb(&path, Some(limit), Some(std::sync::Arc::clone(&shared)));
        (exec, path)
    };
    let (mut a, path_a) = mk("lazy_a");
    let (mut b, path_b) = mk("lazy_b");

    // a 预算内（1 键 < 3 桶）: 零驱逐 → store 未创建（惰性）
    a.process_rows(&[bid_row(1, 1.0)], extract);
    assert!(a.window.spill.is_none(), "预算内不建 store");
    assert!(!path_a.exists(), "预算内不落文件");

    // a 驱逐 3 键（键 2..=6 触发）→ 首次驱逐惰性创建 + 共享计数 3 桶
    for k in 2..=6 {
        a.process_rows(&[bid_row(k, 1.0)], extract);
    }
    assert!(a.window.spill.is_some(), "驱逐时惰性创建 store");
    assert!(path_a.exists(), "首次驱逐后落文件");
    assert_eq!(a.window.over_limit_new_buckets(), 0, "a 不应拒收");
    assert_eq!(
        shared.load(Ordering::SeqCst),
        allowance_for(&plan) * 3,
        "a 落盘 3 键进共享计数（ensure 预置的计数生效）"
    );

    // b 驱逐 → 共享预算耗尽（5）→ 第 12 键拒收
    for k in 7..=12 {
        b.process_rows(&[bid_row(k, 1.0)], extract);
    }
    assert!(b.window.spill.is_some(), "b 驱逐时惰性创建");
    assert_eq!(
        shared.load(Ordering::SeqCst),
        allowance_for(&plan) * 5,
        "a{{1,2,3}}+b{{7,8}} 跨分片累计"
    );
    assert_eq!(
        b.window.over_limit_new_buckets(),
        1,
        "b 第 12 键被共享上限拒收"
    );

    // a close → 扣 a 份额（3 键）; b close → 扣 b 份额 → 归零 + 文件删除
    let a_out = a.close_window_by_bucket_rows();
    assert_eq!(a_out.len(), 6, "a 输出 6 键（内存 3 + spill 3）");
    assert_eq!(
        shared.load(Ordering::SeqCst),
        allowance_for(&plan) * 2,
        "close 后只剩 b 的落盘份额"
    );
    assert!(!path_a.exists(), "a close 后 redb 文件删除");

    let b_out = b.close_window_by_bucket_rows();
    assert_eq!(b_out.len(), 5, "b 输出 5 键（内存 3 + spill 2, 拒收 1）");
    assert_eq!(
        shared.load(Ordering::SeqCst),
        0,
        "全部 close 后共享计数归零"
    );
    assert!(!path_b.exists(), "b close 后 redb 文件删除");
}

// ---------------------------------------------------------------------------
// 11. 文件生命周期（2026-08-27 review 补）：同规则连续窗口无冲突 + 旧文件防污染
// ---------------------------------------------------------------------------

/// 同一规则连续窗口（q12 型 fixed 多窗）：spill 文件**不按窗口命名**——每任务
/// 实例每分片一个文件（`spill_{rule}_{pid}{_shard}.rb`），跨顺序窗口**复用同一
/// 路径**。窗口在单任务内严格串行（一个 window 状态, close 即 reset）, 配合
/// close 清理删文件 → 下一窗惰性重建空库, 正常路径无冲突。本测试用
/// `set_spill_redb`（生产 spawn 注入路径）驱动两窗, 断言：
/// 1. 窗 1 驱逐落盘 → close 后文件删除 + 输出仅窗 1 键
/// 2. 窗 2 驱逐 → 同路径重建 store **必须为空库**（无窗 1 残留键）
/// 3. 窗 2 close 输出仅窗 2 键（无旧窗键混入——close drain 遍历全表的污染场景）
#[test]
fn spill_consecutive_windows_same_rule_fresh_each_window() {
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let path = {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wf_spill_2win_{}_{}.rb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    };
    let mut exec = StatsExecutor::new(plan.clone());
    exec.set_memory_limit("spill_test", Some(allowance_for(&plan) as usize * 3));
    exec.set_spill_redb(&path, None, None);

    // 窗 1：键 1..=6（内存 3 桶 + 驱逐 3 桶）
    for k in 1..=6 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }
    assert!(exec.window.spill.is_some(), "窗 1 驱逐 → 惰性创建 store");
    assert!(path.exists(), "窗 1 驱逐后落文件");
    let w1 = exec.close_window_by_bucket_rows();
    assert_eq!(w1.len(), 6, "窗 1 输出 6 键（内存 3 + spill 3）");
    assert!(!path.exists(), "窗 1 close 后文件删除");

    // 窗 2：键 7..=12（同一路径重建 store）
    for k in 7..=12 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }
    assert!(exec.window.spill.is_some(), "窗 2 驱逐 → 同路径重建 store");
    assert_eq!(
        exec.window.spill.as_ref().unwrap().len(),
        3,
        "重建 store 仅含窗 2 驱逐的 3 键（无窗 1 残留——打开前清空旧文件生效）"
    );
    let w2 = exec.close_window_by_bucket_rows();
    assert_eq!(w2.len(), 6, "窗 2 输出 6 键");
    for b in &w2 {
        let ScopeKey::Int(i) = b.key else {
            panic!("键应为 Int");
        };
        assert!(
            (7..=12).contains(&i),
            "窗 2 输出不含窗 1 键（{i}）——无旧窗污染"
        );
    }
    assert!(!path.exists(), "窗 2 close 后文件删除");
    std::fs::remove_file(&path).ok(); // 幂等清理（失败忽略）
}

/// 打开前清空旧文件（2026-08-27 review）：路径上已存在的库文件（上一窗口
/// cleanup rm 失败残留 / 崩溃残留）不得被打开——直接打开会把旧键混进新窗口
/// （close drain 遍历全表）。`RedbSpillStore::create` 必须删旧建新（空库）。
#[test]
fn spill_create_over_stale_file_starts_fresh() {
    use crate::match_engine::executor::StatsAccum;
    let subset = Arc::new(HashSet::from(["price".to_string()]));
    let mut names: Vec<String> = subset.iter().cloned().collect();
    names.sort();
    let layout = Arc::new(RowFieldLayout::all_other(&names));
    let path = {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wf_spill_stale_{}_{}.rb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    };

    // 旧库：写入 2 键后**不 cleanup**（模拟 rm 失败/崩溃残留——文件留在磁盘）
    let mut stale = RedbSpillStore::create(&path, Arc::clone(&layout)).expect("create stale");
    stale
        .put_batch(vec![
            (1, ScopeKey::Int(1), Vec::<StatsAccum>::new()),
            (2, ScopeKey::Int(2), Vec::<StatsAccum>::new()),
        ])
        .expect("put stale");
    drop(stale); // Drop 只停写 worker, 不删文件
    assert!(path.exists(), "残留文件仍在");
    // 假 .rbr 侧车残留（模拟崩溃遗留的 WAL）——pre-delete 必须一并清掉:
    // 不清的话 redb 打开时要么 WAL 损坏报错, 要么打开旧库带出旧键。
    let rbr = path.with_extension("rbr");
    std::fs::write(&rbr, b"junk wal").expect("write fake rbr");
    assert!(rbr.exists(), "侧车残留仍在");

    // 新建：同一路径 → pre-delete 清主库+侧车 → 空库起步（旧条目不得被打开）
    let mut fresh = RedbSpillStore::create(&path, Arc::clone(&layout))
        .expect("create fresh（侧车残留被清, 打开成功）");
    assert_eq!(fresh.len(), 0, "旧库残留不得被打开（空库起步）");
    assert!(!rbr.exists(), "create 后侧车残留已清");
    fresh.cleanup();
    assert!(!path.exists(), "cleanup 删除文件");
}

// ---------------------------------------------------------------------------
// 12. 补充用例（2026-08-27 review 后）：redb 流式 close / 共享计数跨窗 /
//     cleanup 幂等 / .rbr 侧车残留
// ---------------------------------------------------------------------------

/// redb + 流式 close（生产 q18 主路径: `take_next_close_batch` 分批 drain）。
/// 现有流式 close 测试（§7）只用 Mem store——redb 的 `drain_up_to` 游标续读、
/// 读前 flush、`drain_cursor` 状态在真实持久层上未被覆盖。断言：
/// 1. 小批（4）多轮读完 30 键, 批内 ScopeKey 升序（对拍契约）
/// 2. 每键恰好一次（readback 键 5 不重复）
/// 3. close 后文件删除 + 窗口状态清空
#[test]
fn spill_redb_streaming_close_full_pipeline() {
    let subset = Arc::new(HashSet::from(["price".to_string()]));
    let mut names: Vec<String> = subset.iter().cloned().collect();
    names.sort();
    let layout = Arc::new(RowFieldLayout::all_other(&names));
    let path = {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wf_spill_redb_stream_{}_{}.rb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    };
    let store = RedbSpillStore::create(&path, layout).expect("create redb store");
    let plan = keyed_plan(
        vec![field_key("b", "bidder")],
        vec![last_measure("last_price", "price"), count_measure("n")],
    );
    let mut exec = exec_with_spill(&plan, 3, Some(subset), Some(Box::new(store)), None);
    for k in 1..=30 {
        exec.process_rows(&[bid_row(k, k as f64 * 10.0)], extract);
    }
    exec.process_rows(&[bid_row(5, 555.0)], extract); // 键 5 读回（take 只读, 旧条目留库）
    assert!(exec.window.spill_index.len() > 0, "spill 已生效");

    // 流式 close：小批 4 → redb 游标多轮续读
    let mut keys: Vec<(ScopeKey, crate::match_engine::executor::StatsBucketAccs)> = Vec::new();
    loop {
        let batch = exec.take_next_close_batch(4);
        if batch.is_empty() {
            break;
        }
        assert!(
            batch.windows(2).all(|w| w[0].0 <= w[1].0),
            "redb 流式批内必须 ScopeKey 升序"
        );
        keys.extend(batch);
    }
    exec.finish_close_window();
    assert_eq!(keys.len(), 30, "30 键全部输出");
    let unique = keys
        .iter()
        .map(|(k, _)| k.clone())
        .collect::<std::collections::HashSet<_>>()
        .len();
    assert_eq!(unique, 30, "每键恰好一次（readback 键 5 不重复）");
    assert!(
        keys.iter().any(|(k, _)| *k == ScopeKey::Int(5)),
        "键 5 在输出中"
    );
    assert!(exec.window.buckets.is_empty(), "close 后窗口状态清空");
    assert!(!path.exists(), "流式 close 后文件删除");
    std::fs::remove_file(&path).ok(); // 幂等清理（失败忽略）
}

/// 共享落盘计数跨窗口复用（§19 语义）：窗 1 close 归零 → 窗 2 驱逐重新累计
/// → close 归零。`spill_consecutive_windows` 未注入共享计数, 本测试补上。
#[test]
fn spill_shared_counter_resets_across_windows() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let shared = Arc::new(AtomicU64::new(0));
    let path = {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wf_spill_shared_2win_{}_{}.rb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    };
    let mut exec = StatsExecutor::new(plan.clone());
    exec.set_memory_limit("spill_test", Some(allowance_for(&plan) as usize * 3));
    exec.set_spill_redb(
        &path,
        Some((allowance_for(&plan) * 10) as usize),
        Some(Arc::clone(&shared)),
    );

    // 窗 1：键 1..=6（内存 3 + 落盘 3）
    for k in 1..=6 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }
    assert_eq!(
        shared.load(Ordering::SeqCst),
        allowance_for(&plan) * 3,
        "窗 1 落盘 3 键进共享计数"
    );
    exec.close_window_by_bucket_rows();
    assert_eq!(
        shared.load(Ordering::SeqCst),
        0,
        "窗 1 close 后计数归零（预算跨窗口复用）"
    );

    // 窗 2：键 7..=12（同路径重建 store, 共享计数重新累计）
    for k in 7..=12 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }
    assert_eq!(
        shared.load(Ordering::SeqCst),
        allowance_for(&plan) * 3,
        "窗 2 驱逐重新累计到共享计数"
    );
    exec.close_window_by_bucket_rows();
    assert_eq!(shared.load(Ordering::SeqCst), 0, "窗 2 close 后计数归零");
    assert!(!path.exists(), "窗 2 close 后文件删除");
    std::fs::remove_file(&path).ok();
}

/// cleanup 幂等：二次调用不 panic（writer/db 已 take, 删文件幂等）——防回归
/// （若 cleanup 二次进入 remove_file 流程或 worker shutdown 二次 join 会挂/崩）。
#[test]
fn spill_cleanup_idempotent() {
    use crate::match_engine::executor::StatsAccum;
    let subset = Arc::new(HashSet::from(["price".to_string()]));
    let mut names: Vec<String> = subset.iter().cloned().collect();
    names.sort();
    let layout = Arc::new(RowFieldLayout::all_other(&names));
    let path = {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wf_spill_cleanup_idem_{}_{}.rb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    };
    let mut store = RedbSpillStore::create(&path, layout).expect("create");
    store
        .put_batch(vec![(1, ScopeKey::Int(1), Vec::<StatsAccum>::new())])
        .expect("put");
    store.cleanup();
    assert!(!path.exists(), "首次 cleanup 删文件");
    store.cleanup(); // 幂等：不得 panic / 挂死
    assert!(!path.exists());
}

// ---------------------------------------------------------------------------
// 13. 驱逐记账一致 × 共享计数不过度（2026-08-27 修复回归）
// ---------------------------------------------------------------------------

/// 驱逐记账一致（单片语义）：驱逐 + 内存残留 = 注入总量（无拒收不丢键）,
/// 驱逐后内存停在 [target, limit] 区间（不过度——修复前并发下每片各驱逐
/// 水位差）。count 度量无 distinct, 估算 = 桶数×allowance 精确。
#[test]
fn spill_evicts_to_target_exact_overage() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let mem_shared = Arc::new(AtomicU64::new(0));
    let spill_shared = Arc::new(AtomicU64::new(0));
    let limit = allowance_for(&plan) * 100; // 100 桶
    let target = allowance_for(&plan) * 90; // 90 桶（90% 驱逐目标）

    let path = std::env::temp_dir().join(format!(
        "wf_spill_target_{}_{}.rb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut exec = StatsExecutor::new(plan.clone());
    exec.set_memory_limit_shared("t1", Some(limit as usize), Some(Arc::clone(&mem_shared)));
    exec.set_spill_redb(&path, None, Some(Arc::clone(&spill_shared)));
    for k in 1..=150i64 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }

    // 驱逐后内存停在 [target, limit]（不过度）
    let est = exec.window.estimated_bytes();
    assert!(
        est >= target && est <= limit,
        "驱逐后内存应在 [target, limit], 实测 {est}"
    );
    // 记账一致：驱逐键 + 内存键 = 150（无拒收）
    let mem_buckets = exec.window.buckets.len() as u64;
    let total = exec.window.spill_evictions() + mem_buckets;
    assert_eq!(
        total,
        150,
        "驱逐({}) + 内存({}) = 150（无拒收）",
        exec.window.spill_evictions(),
        mem_buckets
    );
    // 内存共享计数与驱逐一致（= 本片估算）; 落盘计数 = 驱逐量
    assert_eq!(
        mem_shared.load(Ordering::SeqCst),
        est,
        "内存共享计数 = 本片估算"
    );
    assert_eq!(
        spill_shared.load(Ordering::SeqCst),
        exec.window.spill_evictions() * allowance_for(&plan),
        "落盘共享计数 = 驱逐量 × allowance"
    );
    // close 全键输出, 计数归零
    assert_eq!(
        exec.close_window_by_bucket_rows().len(),
        150,
        "150 键全输出"
    );
    assert_eq!(mem_shared.load(Ordering::SeqCst), 0, "close 后内存计数归零");
    assert_eq!(
        spill_shared.load(Ordering::SeqCst),
        0,
        "close 后落盘计数归零"
    );
    std::fs::remove_file(&path).ok();
}

/// 多片**并发**超限（模拟 q18 10 片共享 25GB 同刻超限）：修复前每片各驱逐
/// 水位差 → 总驱逐 = 水位差 × 片数（过度, 共享计数降到 target - 水位差）;
/// 修复后逐链预订共享计数（单一事实源）, 总驱逐 = 超限部分, 共享计数停在
/// target 附近。两线程 barrier 同步同刻注入。断言共享计数 ≥ target - 竞态
/// 余量（10 链）——修复前并发下会显著低于 target。
#[test]
fn spill_shared_memory_counter_no_over_eviction_under_concurrency() {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicU64, Ordering};
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let mem_shared = Arc::new(AtomicU64::new(0));
    let spill_shared = Arc::new(AtomicU64::new(0));
    let limit = allowance_for(&plan) * 100;
    let target = allowance_for(&plan) * 90;
    let barrier = Arc::new(Barrier::new(2));

    let worker = |tag: String,
                  base: i64,
                  barrier: Arc<Barrier>,
                  shared: Arc<AtomicU64>,
                  spill: Arc<AtomicU64>| {
        let plan = plan.clone();
        std::thread::spawn(move || {
            let path = std::env::temp_dir().join(format!(
                "wf_spill_conc_{}_{}_{}.rb",
                tag,
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let mut exec = StatsExecutor::new(plan);
            exec.set_memory_limit_shared(&tag, Some(limit as usize), Some(Arc::clone(&shared)));
            exec.set_spill_redb(&path, None, Some(Arc::clone(&spill)));
            // 第一波 80 键（共享 160 > 100, 注入过程已驱逐）
            for k in 0..80i64 {
                exec.process_rows(&[bid_row(base + k, 1.0)], extract);
            }
            barrier.wait(); // 两片都就位 → 同刻注入第二波触发并发驱逐
            for k in 80..120i64 {
                exec.process_rows(&[bid_row(base + k, 1.0)], extract);
            }
            (exec, path)
        })
    };
    let h_a = worker(
        "conc_a".to_string(),
        1,
        Arc::clone(&barrier),
        Arc::clone(&mem_shared),
        Arc::clone(&spill_shared),
    );
    let h_b = worker(
        "conc_b".to_string(),
        1001,
        Arc::clone(&barrier),
        Arc::clone(&mem_shared),
        Arc::clone(&spill_shared),
    );
    let (a, path_a) = h_a.join().expect("a panicked");
    let (b, path_b) = h_b.join().expect("b panicked");

    let used = mem_shared.load(Ordering::SeqCst);
    assert!(
        used >= target.saturating_sub(10 * allowance_for(&plan)),
        "并发驱逐过度: 共享计数 {used}B < target {target}B - 竞态余量\
         （修复前每片各驱逐水位差, q18 实测 25GB 配置每片驱逐 2.5GB×10）"
    );
    assert!(used <= limit, "共享计数有界");
    // 驱逐不凭空产生：每片驱逐 ≤ 各自注入键数
    assert!(a.window.spill_evictions() <= 120, "a 驱逐 ≤ 120");
    assert!(b.window.spill_evictions() <= 120, "b 驱逐 ≤ 120");
    std::fs::remove_file(&path_a).ok();
    std::fs::remove_file(&path_b).ok();
}

// ---------------------------------------------------------------------------
// 14. 预订归还路径（2026-08-27 逐链预订修复的失败分支）
// ---------------------------------------------------------------------------

/// put_batch 恒失败的 store——模拟写失败（磁盘满/IO 错），验证驱逐预订归还。
struct FailingStore;

impl crate::match_engine::spill::SpillStore for FailingStore {
    fn contains(&self, _hash: u64) -> bool {
        false
    }
    fn put_batch(
        &mut self,
        _entries: Vec<(
            u64,
            ScopeKey,
            Vec<crate::match_engine::executor::StatsAccum>,
        )>,
    ) -> Result<(), crate::match_engine::spill::SpillError> {
        Err(crate::match_engine::spill::SpillError::Closed)
    }
    fn take(
        &mut self,
        _hash: u64,
    ) -> Option<(ScopeKey, Vec<crate::match_engine::executor::StatsAccum>)> {
        None
    }
    fn drain_up_to(
        &mut self,
        _n: usize,
    ) -> Vec<(ScopeKey, Vec<crate::match_engine::executor::StatsAccum>)> {
        Vec::new()
    }
    fn cleanup(&mut self) {}
    fn len(&self) -> usize {
        0
    }
}

/// 写失败归还路径（2026-08-27 逐链预订修复）：驱逐循环预订扣减共享计数 →
/// `put_batch` 失败 → 按 `reserved` 归还（estimated_bytes + 共享计数恢复）→
/// 键仍在内存（buckets 未删、clock 已 pop）→ spill_failed 置位 → 后续新键
/// 拒收兜底。断言：归还后 estimated 不虚降、已建键不丢、close 输出完整。
#[test]
fn spill_write_failure_returns_reserved_and_rejects() {
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let mut exec = exec_with_spill(
        &plan,
        3,
        None,
        Some(Box::new(FailingStore)),
        None, // max_disk 不限——只看写失败分支
    );
    for k in 1..=10 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }

    // 归还生效：estimated 恢复 = 内存 3 桶（驱逐尝试失败, 键未删）
    assert_eq!(
        exec.window.estimated_bytes(),
        allowance_for(&plan) * 3,
        "写失败归还后 estimated 不虚降（键仍在内存记账）"
    );
    // 键全在内存（驱逐从未成功）
    assert_eq!(
        exec.window.buckets.values().map(Vec::len).sum::<usize>(),
        3,
        "驱逐失败的键仍在内存"
    );
    assert_eq!(exec.window.spill_evictions(), 0, "写失败不算驱逐");
    assert_eq!(exec.window.spill_index.len(), 0, "无键落盘");
    // spill_failed 置位 → 后续键拒收（内存冻结, 不丢已建键）
    assert!(exec.window.over_limit_new_buckets() > 0, "写失败后回退拒收");
    // close 输出 = 已接收的 3 键（每个 count=1）——无半吊子
    let closed = exec.close_window_by_bucket_rows();
    assert_eq!(closed.len(), 3, "只输出被接收的 3 键");
    for b in &closed {
        assert_eq!(b.measures[0][0].measure_value, 1.0);
    }
}

/// 落盘上限满时的预订归还（2026-08-27 逐链预订修复）：驱逐循环预订扣共享
/// 计数 → 写前检查 `spill_used + add > max_disk` → 归还预订 + 拒收兜底。
/// 断言：归还后 estimated 不虚降（与 [`spill_budget_ladder_falls_back_to_reject`]
/// 互补——那个测拒收计数, 这个显式锁归还）。
#[test]
fn spill_disk_full_returns_reserved_and_rejects() {
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    // 预算 3 桶, 落盘上限 1 键
    let max_spill = allowance_for(&plan) as usize;
    let mut exec = exec_with_spill(
        &plan,
        3,
        None,
        Some(Box::new(MemSpillStore::new())),
        Some(max_spill),
    );
    for k in 1..=10 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }

    // 归还生效：estimated = 内存 3 桶（被 max_disk 挡下的驱逐未虚降）
    assert_eq!(
        exec.window.estimated_bytes(),
        allowance_for(&plan) * 3,
        "落盘满归还后 estimated 不虚降"
    );
    // 落盘只够 1 键, 其余驱逐尝试被挡 → 拒收
    assert_eq!(exec.window.spill_index.len(), 1, "只落盘 1 键");
    assert!(exec.window.over_limit_new_buckets() > 0, "落盘满回退拒收");
    // close 输出 = 内存 3 + 落盘 1 = 4 键
    let closed = exec.close_window_by_bucket_rows();
    assert_eq!(closed.len(), 4, "只输出被接收的 4 键");
}
