//! BaselineStore 主逻辑基准（2026-09-08）——近端 B 判定热路径 vs 收盘 append。
//!
//! 被测路径（baseline-online-design.md §11.3/S2-4）：
//!   - `append`：producer 每窗收盘写入（锁 + HashMap 定位 + VecDeque 保序插入
//!     与 K 裁剪）——长期运行每窗每键一次；
//!   - `deviation_at`：judge 每事件判定（`windows_for` 对全 store 做 entity/metric
//!     过滤 + 排序 → `merge` 推导 μ/σ → z）——**每事件一次，热路径**。
//!
//! 关注的缩放维度：
//!   1. 实体规模（共享同一 store 的 (entity,metric) 键数）——judge 每事件按
//!      entity 判定，但 windows_for 是全表扫描过滤 → 成本应随键数线性涨；
//!   2. 相位开/关：相位把每键拆成 period/bucket 个桶 → 键数放大、每桶窗口更少；
//!   3. decay 开/关：merge 内权重计算。
//!
//! 运行（release，否则数据无意义）：
//!   cargo test --release -p wf-cep baseline_bench -- --ignored --nocapture

use std::time::{Duration, Instant};

use crate::baseline::{BaselineStore, BaselineWindow, Phase};

const K: usize = 8;

fn win(start_ns: i64) -> BaselineWindow {
    BaselineWindow {
        win_start_nanos: start_ns,
        win_end_nanos: start_ns + 15_000_000_000,
        n: 100.0,
        sum: 100_000.0,
        sum_sq: 100_096_000.0, // μ=1000, σ=20（与 run_loop 种子一致）
    }
}

/// 时间预算测量：warmup 后跑到 ~budget，返回 ns/op。
fn measure_ns<F: FnMut()>(mut op: F) -> f64 {
    let budget = Duration::from_millis(250);
    // warmup（首调含 HashMap 分配/缓存预热）
    op();
    let start = Instant::now();
    let mut ops = 0u64;
    loop {
        for _ in 0..64 {
            std::hint::black_box(op());
            ops += 1;
        }
        if start.elapsed() >= budget {
            break;
        }
    }
    start.elapsed().as_secs_f64() * 1e9 / ops as f64
}

/// 构造 n 实体 × 每键 K 窗已收盘历史（相位关：K 个连续窗；相位开：K 个跨周期
/// 同相位窗 + 相邻桶各 1 窗，模拟真实分桶态）。返回每实体判定用的 `at`。
fn populate(store: &BaselineStore, entities: usize, phase: bool) -> Vec<i64> {
    let period = 60_000_000_000i64;
    let bucket = 15_000_000_000i64;
    let mut ats = Vec::with_capacity(entities);
    for e in 0..entities {
        let ent = format!("e{e}");
        if !phase {
            for w in 0..K {
                store.append(&ent, "flow", win(w as i64 * 15_000_000_000));
            }
            ats.push(K as i64 * 15_000_000_000 + 7_000_000_000);
        } else {
            // 目标桶（slot=2）跨 K 周期各一窗；相邻桶各一窗保证键数 = 3×实体
            for p in 0..K {
                store.append(&ent, "flow", win(p as i64 * period + 2 * bucket));
            }
            store.append(&ent, "flow", win(bucket)); // slot1
            store.append(&ent, "flow", win(3 * bucket)); // slot3
            ats.push((K as i64) * period + 2 * bucket + 7_000_000_000);
        }
    }
    ats
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-cep baseline_bench -- --ignored --nocapture"]
fn baseline_store_append_vs_judge_heat_path() {
    let cases: [(usize, bool, bool); 4] = [
        // (实体数, 相位开, decay 开) —— 相位关时只探规模；相位开单独一组
        (1, false, true),
        (100, false, true),
        (10_000, false, true),
        (100, true, true),
    ];
    eprintln!();
    eprintln!("=== BaselineStore 主逻辑基准（release; ns/op；K=8 窗/键；μ=1000 σ=20）===");
    eprintln!(
        "{:<26} {:>11} {:>11} {:>11}   {:>10} {:>10}",
        "形态", "append", "summary_at", "deviation_at", "judge/s", "备注"
    );
    for (entities, phase, decay) in cases {
        let ent_s = if entities >= 1000 {
            "1万".to_string()
        } else {
            entities.to_string()
        };
        let label = format!(
            "{ent_s}实体 {}decay {}相位",
            if decay { "开" } else { "关" },
            if phase { "开" } else { "关" }
        );
        // 用空 store 预热 append；judge 用带历史 store
        let mut st = store_for(entities, phase, decay);
        let ats = populate(&mut st, entities, phase);
        let at = ats[0];

        let mut next_start = 9_000_000_000_000i64;
        let append_ns = measure_ns(|| {
            st.append("e0", "flow", win(next_start)); // 每 op 新窗 → 追加 + K 裁剪
            next_start += 15_000_000_000;
        });
        let summary_ns = measure_ns(|| {
            std::hint::black_box(st.summary_at("e0", "flow", Some(at)));
        });
        let dev_ns = measure_ns(|| {
            std::hint::black_box(st.deviation_at("e0", "flow", 1000.0, Some(at)));
        });
        let judge_s = 1e9 / dev_ns;
        // 键数（诊断）
        let note = if phase {
            format!("键≈{}×3（桶拆分）", entities)
        } else {
            format!("键≈{}", entities)
        };
        eprintln!(
            "{:<26} {:>8.0} ns {:>8.0} ns {:>8.0} ns   {:>9.0}   {}",
            label, append_ns, summary_ns, dev_ns, judge_s, note
        );
        // 规模缩放：万实体下 deviation 应显著劣于单实体（全表扫描过滤）——
        // 若 ≤2× 说明近 O(1)（有按键索引）；若 ~N× 说明全表扫描是主成本。
    }

    // 规模缩放对比：单实体 vs 万实体（相位关，decay 开）
    eprintln!();
    eprintln!("=== 缩放（deviation_at，相位关） ===");
    let mut prev: Option<f64> = None;
    for entities in [1usize, 100, 10_000] {
        let mut st = store_for(entities, false, true);
        let ats = populate(&mut st, entities, false);
        let at = ats[0];
        let ns = measure_ns(|| {
            std::hint::black_box(st.deviation_at("e0", "flow", 1000.0, Some(at)));
        });
        let ratio = prev.map(|p| ns / p);
        eprintln!(
            "{:<8} 实体: {:>8.0} ns/判定   {:>7.1}× vs 上一档{}",
            entities,
            ns,
            ratio.unwrap_or(1.0),
            if ratio.map_or(false, |r| r > 50.0) {
                "  ⚠ 全表扫描主导：键数线性放大（windows_for 每事件扫全 store）"
            } else {
                ""
            }
        );
        prev = Some(ns);
    }
    eprintln!();
    eprintln!(
        "口径：append=收盘（已满 K 追加）；deviation_at=judge 每事件（含全表过滤+排序+μ/σ 推导）；"
    );
    eprintln!("     summary_at=同 dev 但不算 z。数据仅在同一 release/profile 下可比。");
}

fn store_for(entities: usize, phase: bool, decay: bool) -> BaselineStore {
    let _ = entities; // store 本身不预分配；键随 append 增长
    if phase {
        BaselineStore::phased(
            K,
            decay,
            Phase {
                period_nanos: 60_000_000_000,
                bucket_nanos: 15_000_000_000,
            },
        )
    } else if decay {
        BaselineStore::decaying(K)
    } else {
        BaselineStore::new(K)
    }
}
