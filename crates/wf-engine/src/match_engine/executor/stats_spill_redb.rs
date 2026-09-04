//! redb store 全链路与文件生命周期（2026-09-04 自 stats_spill_test.rs 拆出；
//! `#[path]` 兄弟子模块）：RedbSpillStore 驱逐落盘-读回-close 合并、
//! `set_spill_redb` 延迟创建/重建、同路径连续窗口防旧窗污染、打开前清空残留
//! （主库 + .rbr 侧车）、redb 流式 close 游标续读、cleanup 幂等与文件删除、
//! 惰性注册快速路径不误判。共享 harness 在父模块 stats_spill_test.rs，此处
//! 经 `use super::*` 复用。

use super::*;

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
    assert!(!exec.window.spill_index.is_empty(), "spill 已生效");

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

/// 惰性创建前快速路径不误判: `set_spill_redb` 已注册（`spill_create` Some）但
/// 零驱逐未建 store → bucket 仍维护 touch/clock（spill 路径）——首次驱逐时
/// store 才建。断言 clock 非空（快速路径条件 `spill_create.is_none()` 生效）。
#[test]
fn spill_lazy_registered_keeps_clock() {
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let path = {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wf_spill_lazy_clock_{}_{}.rb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    };
    let mut exec = StatsExecutor::new(plan.clone());
    exec.set_memory_limit("lazy_clock", Some((allowance_for(&plan) * 100) as usize));
    exec.set_spill_redb(&path, None, None);
    exec.process_rows(&[bid_row(1, 1.0), bid_row(2, 2.0)], extract);
    // 零驱逐（预算充足）→ store 未建, 但 spill_create 已注册 → 走 spill 路径
    assert!(exec.window.spill.is_none(), "零驱逐不建 store");
    assert_eq!(
        exec.window.clock.len(),
        2,
        "spill 路径维护 clock（快速路径不误判）"
    );
    // close → reset_window → clock 清空（新窗口）
    exec.close_window_by_bucket_rows();
    assert!(exec.window.clock.is_empty(), "close 后 clock 清空");
    std::fs::remove_file(&path).ok();
}
