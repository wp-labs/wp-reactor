//! Stats spill 单片行为与对拍（2026-09-04 自 stats_spill_test.rs 拆出；
//! `#[path]` 兄弟子模块）：Mem store / 无 spill 快速路径下的驱逐-读回-close
//! 合并、spill vs 不 spill 输出对拍、三层预算阶梯拒收、estimated_bytes 有界、
//! touch/clock 驱逐保护、Mem 流式 close drain、distinct/top 度量读回、驱逐
//! 写失败 / 落盘满的预订归还拒收路径。共享 harness（import/计划与度量构建/
//! `exec_with_spill` 等）在父模块 stats_spill_test.rs，此处经 `use super::*` 复用。

use super::*;

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

/// 无 spill（快速路径 entry 单查）vs 有 spill 但不驱逐（spill 路径双查）——
/// 输出逐键一致（对拍契约: 快速路径与 spill 路径的建桶/累计/close 等价）。
/// 另断言无 spill 时 clock 空（快速路径不维护——零开销确认）。
#[test]
fn spill_fast_path_matches_spill_path_without_eviction() {
    let plan = keyed_plan(
        vec![field_key("b", "bidder")],
        vec![count_measure("n"), sum_measure("total", "price")],
    );
    let limit = (allowance_for(&plan) * 50) as usize; // 预算 50 桶, 20 键不驱逐

    // A: 无 spill（快速路径）
    let mut a = StatsExecutor::new(plan.clone());
    a.set_memory_limit("fast", Some(limit));
    for k in 1..=20i64 {
        a.process_rows(&[bid_row(k, k as f64)], extract);
    }
    // B: 有 spill 但不驱逐（spill 路径）
    let mut b = StatsExecutor::new(plan.clone());
    b.set_memory_limit("spill_path", Some(limit));
    b.set_spill(Some(Box::new(MemSpillStore::new())), None, None);
    for k in 1..=20i64 {
        b.process_rows(&[bid_row(k, k as f64)], extract);
    }

    // 快速路径不维护 clock（零开销）; 两路径均零驱逐零拒收
    assert!(a.window.clock.is_empty(), "无 spill 快速路径不维护 clock");
    assert_eq!(b.window.clock.len(), 20, "spill 路径维护 clock");
    assert_eq!(a.window.over_limit_new_buckets(), 0);
    assert_eq!(b.window.over_limit_new_buckets(), 0);
    assert_eq!(a.window.spill_evictions(), 0);
    assert_eq!(b.window.spill_evictions(), 0);

    // 输出对拍: 键集合 + 每度量 measure_value + 条目数一致
    let a_out = a.close_window_by_bucket_rows();
    let b_out = b.close_window_by_bucket_rows();
    assert_eq!(a_out.len(), 20, "A 20 键");
    assert_eq!(a_out.len(), b_out.len(), "两路径输出键数一致");
    for (x, y) in a_out.iter().zip(b_out.iter()) {
        assert_eq!(x.key, y.key, "键一致");
        assert_eq!(x.measures.len(), y.measures.len(), "度量数一致");
        for (mx, my) in x.measures.iter().zip(y.measures.iter()) {
            assert_eq!(mx.len(), my.len(), "条目数一致");
            for (ex, ey) in mx.iter().zip(my.iter()) {
                assert_eq!(
                    ex.measure_value, ey.measure_value,
                    "键 {:?} 度量值一致",
                    x.key
                );
            }
        }
    }
}

/// 无 spill 快速路径限额拒收（`account_bucket_allowed` 本片口径）:
/// 预算 3 桶注入 8 键 → 拒收 5, 只输出 3（不建桶不丢已建）。
#[test]
fn spill_fast_path_no_spill_rejects_over_limit() {
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let limit = (allowance_for(&plan) * 3) as usize;
    let mut exec = StatsExecutor::new(plan.clone());
    exec.set_memory_limit("fast_reject", Some(limit));
    for k in 1..=8i64 {
        exec.process_rows(&[bid_row(k, 1.0)], extract);
    }
    assert!(exec.window.over_limit_new_buckets() > 0, "快速路径超限拒收");
    assert_eq!(exec.window.buckets.len(), 3, "只建 3 桶");
    let closed = exec.close_window_by_bucket_rows();
    assert_eq!(closed.len(), 3, "只输出 3 键");
}
