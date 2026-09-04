//! 规则级共享预算（2026-09-04 自 stats_spill_test.rs 拆出；`#[path]` 兄弟
//! 子模块）：同规则多分片经 `Arc<AtomicU64>` 共享落盘/内存计数——跨分片累计、
//! 预算耗尽拒收、逐链预订不虚扣、并发驱逐不过度、惰性创建 × 共享计数、共享
//! 计数跨窗口归零复用、无 spill 快速路径共享口径。共享 harness 在父模块
//! stats_spill_test.rs，此处经 `use super::*` 复用。

use super::*;

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

/// 惰性创建与共享落盘计数的联动（两个 executor 模拟同规则两分片）:
/// 1. 预算内零驱逐 → store 未创建（惰性）
/// 2. 首次驱逐 → 惰性创建 store, 驱逐记账走 ensure 预置的共享计数
/// 3. 共享预算耗尽 → 另一分片驱逐回退拒收
/// 4. 各自 close → 扣自身份额（预算跨窗口复用）
///    覆盖 spawn 注入路径（set_spill_redb）而非测试直连的 set_spill。
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

/// 两片都无 spill（纯快速路径）+ 规则级共享计数: A 占满 3 桶 → B 首键被共享
/// 总量拒收（`account_bucket_allowed` 共享口径——本次合并修复的核心语义）。
#[test]
fn spill_fast_path_shared_budget_two_no_spill() {
    use std::sync::atomic::Ordering;
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let shared = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let limit = (allowance_for(&plan) * 3) as usize; // 规则总内存 3 桶
    let mk = |tag: &str| -> StatsExecutor {
        let mut exec = StatsExecutor::new(plan.clone());
        exec.set_memory_limit_shared(tag, Some(limit), Some(Arc::clone(&shared)));
        exec
    };
    let mut a = mk("fast_a");
    let mut b = mk("fast_b");
    for k in 1..=3i64 {
        a.process_rows(&[bid_row(k, 1.0)], extract);
    }
    b.process_rows(&[bid_row(7, 1.0)], extract);
    assert_eq!(
        b.window.over_limit_new_buckets(),
        1,
        "B 首键被共享总量拒收（快速路径共享口径）"
    );
    assert_eq!(
        shared.load(Ordering::SeqCst),
        allowance_for(&plan) * 3,
        "A 3 桶进共享计数, B 拒收不建"
    );
    assert_eq!(a.close_window_by_bucket_rows().len(), 3, "A 3 键");
    assert_eq!(b.close_window_by_bucket_rows().len(), 0, "B 0 键");
    assert_eq!(shared.load(Ordering::SeqCst), 0, "close 后共享归零");
}
