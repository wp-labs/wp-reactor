//! close_bench 拆出的兄弟子模块（2026-09-04）：q15 EOS 归并
//! （`StatsExecutor::merge_partial`）逐分量微基准——生产 clone+extend 实现 vs
//! move（免 clone）/ move+reserve / move+小并大 候选（`merge_accum_*` 内联副本,
//! 与 stats_exec::merge_accum 同构），含分片数敏感度。共享 harness/import 在
//! 父模块 close_bench.rs，此处经 `use super::*` 复用；切片内独占构造随迁。

use super::*;

// ---------------------------------------------------------------------------
// q15 EOS 归并（`StatsExecutor::merge_partial`）逐分量微基准（2026-08-24）
// ---------------------------------------------------------------------------
//
// 背景：q15 输入分区分片后，协调片 EOS 归并 ~883ms 串行（9 片 × 8 distinct 集
// union ≈ 68M 次 insert）。本基准量化当前 `merge_accum`（`os.clone()` + 无
// reserve 的 `extend`）的成本构成，并对比候选优化：
//   move        : merge_partial 改用 owned `into_iter`——协调片 None 时直接
//                 move 整个 set（免 68M 元素克隆）; extend 也 move 元素免 clone
//   move+reserve: extend 前 `reserve(o.len())` 预扩容，免多轮 rehash
//   move+小并大  : 小集插大集（union by size），rehash 次数按小集容量增长
//
// 运行：
//   cargo test --release -p wf-engine close_bench q15_merge_partial -- --ignored --nocapture

/// 生产 merge_accum 逻辑的内联副本（bench 隔离: 不依赖 executor 内部）。
/// 与生产 `merge_partial` 一致——借用 `&StatsAccum`（每度量不额外 clone）:
/// None 时整集 `os.clone()`（协调片首次归并）, 否则逐元素 `iter().cloned()`。
/// 2026-08-26 对齐度量专用累加器（match 变体, 与 stats_exec::merge_accum 同构）。
fn merge_accum_cur(t: &mut StatsAccum, o: &StatsAccum) {
    match (t, o) {
        (StatsAccum::Numeric(t), StatsAccum::Numeric(o)) => {
            t.count += o.count;
            t.sum += o.sum;
            t.min = match (t.min, o.min) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            t.max = match (t.max, o.max) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
        }
        (StatsAccum::Distinct(t), StatsAccum::Distinct(o)) => t.extend_other(o),
        _ => {}
    }
}

/// 候选: owned move（免克隆; 协调片 None 时直接吞 set）。
fn merge_accum_move(t: &mut StatsAccum, o: StatsAccum) {
    match (t, o) {
        (StatsAccum::Numeric(t), StatsAccum::Numeric(o)) => {
            t.count += o.count;
            t.sum += o.sum;
            t.min = match (t.min, o.min) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            t.max = match (t.max, o.max) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
        }
        (StatsAccum::Distinct(t), StatsAccum::Distinct(o)) => t.extend(*o),
        _ => {}
    }
}

/// 候选: owned move + extend 前 reserve 预扩容。
fn merge_accum_move_reserve(t: &mut StatsAccum, o: StatsAccum) {
    match (t, o) {
        (StatsAccum::Numeric(t), StatsAccum::Numeric(o)) => {
            t.count += o.count;
            t.sum += o.sum;
            t.min = match (t.min, o.min) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            t.max = match (t.max, o.max) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
        }
        (StatsAccum::Distinct(t), StatsAccum::Distinct(o)) => {
            t.reserve(o.len());
            t.extend(*o);
        }
        _ => {}
    }
}

/// 候选: owned move + 小并大（union by size——小集插大集, 扩容按小集增长）。
fn merge_accum_move_small_into_big(t: &mut StatsAccum, o: StatsAccum) {
    match (t, o) {
        (StatsAccum::Numeric(t), StatsAccum::Numeric(o)) => {
            t.count += o.count;
            t.sum += o.sum;
            t.min = match (t.min, o.min) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            t.max = match (t.max, o.max) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
        }
        (StatsAccum::Distinct(t), StatsAccum::Distinct(mut o)) => {
            if t.len() < o.len() {
                std::mem::swap(t, &mut o);
            }
            t.extend(*o);
        }
        _ => {}
    }
}

/// 构造一个 `StatsAccum`（distinct 度量形状）: distinct_set 填 `n` 个确定性
/// i64 键（LCG, 域 = [0, domain)）。2026-08-26 对齐度量专用累加器（Distinct 变体）。
fn shard_accum(n: usize, domain: u64, seed: u64) -> StatsAccum {
    use crate::match_engine::{DistinctKey, DistinctSet, StatsAccum};
    let mut set = DistinctSet::default();
    set.reserve(n);
    let mut rng = seed;
    let mut next = |range: u64| {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (rng >> 33) % range
    };
    for _ in 0..n {
        set.insert(DistinctKey::Int((next(domain) + 1) as i64));
    }
    StatsAccum::Distinct(Box::new(set))
}

/// 运行一轮：协调片已含自己 1/N 数据, 依次 merge 其余 partial。
/// 两个变体的**调用侧成本一致**：每轮从模板 clone 一份 partial（模拟从
/// channel 收到的 owned 包——生产里 `merge_partial(buckets, count)` 直接消费
/// owned, 无额外成本）; `borrowed` 仅决定 merge 函数内部用借用还是 move。
fn run_merge(
    borrowed: bool,
    merge_borrowed: fn(&mut StatsAccum, &StatsAccum),
    merge_owned: fn(&mut StatsAccum, StatsAccum),
    per_shard_distinct: usize,
    domain: u64,
) -> f64 {
    run_merge_shards(
        borrowed,
        merge_borrowed,
        merge_owned,
        9,
        per_shard_distinct,
        domain,
    )
}

/// 与 [`run_merge`] 同构, 但分片数可配置（分片数敏感度: 协调片 + N-1 partial）。
fn run_merge_shards(
    borrowed: bool,
    merge_borrowed: fn(&mut StatsAccum, &StatsAccum),
    merge_owned: fn(&mut StatsAccum, StatsAccum),
    shards: usize,
    per_shard_distinct: usize,
    domain: u64,
) -> f64 {
    const N_MEASURES: usize = 12; // 4 count + 8 distinct（q15 形状）

    // 协调片自己 1/N 数据（distinct 度量索引 4..12）。
    let coord: Vec<StatsAccum> = (0..N_MEASURES)
        .map(|m| {
            if m >= 4 {
                shard_accum(per_shard_distinct, domain, 0x1000 + m as u64)
            } else {
                StatsAccum::default()
            }
        })
        .collect();
    // N-1 个 partial（各 12 个 StatsAccum, distinct 度量带 own 数据）。
    let partials: Vec<Vec<StatsAccum>> = (1..shards)
        .map(|s| {
            (0..N_MEASURES)
                .map(|m| {
                    if m >= 4 {
                        shard_accum(
                            per_shard_distinct,
                            domain,
                            0x2000 + s as u64 * 16 + m as u64,
                        )
                    } else {
                        StatsAccum::default()
                    }
                })
                .collect()
        })
        .collect();

    // 预热一轮（含 alloc）。
    {
        let mut c = coord.clone();
        let mut p = partials.clone();
        if borrowed {
            for src in &p {
                for (t, o) in c.iter_mut().zip(src.iter()) {
                    merge_borrowed(t, o);
                }
            }
        } else {
            for src in p.drain(..) {
                for (t, o) in c.iter_mut().zip(src) {
                    merge_owned(t, o);
                }
            }
        }
    }

    let start = Instant::now();
    let mut c = coord.clone();
    let mut p = partials.clone();
    if borrowed {
        for src in &p {
            for (t, o) in c.iter_mut().zip(src.iter()) {
                merge_borrowed(t, o);
            }
        }
    } else {
        for src in p.drain(..) {
            for (t, o) in c.iter_mut().zip(src) {
                merge_owned(t, o);
            }
        }
    }
    std::hint::black_box(&c);
    start.elapsed().as_secs_f64() * 1e9
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine close_bench q15_merge_partial -- --ignored --nocapture"]
fn q15_merge_partial_profile() {
    eprintln!(
        "[close-bench] ===== q15 EOS 归并 profile（merge_partial 9 片 × 8 distinct 集）====="
    );

    // 规模: q15 30M 行分 9 片, 每片每 distinct 集 ~1M 元素（域 8M 高度重叠,
    // 近似生产 68M 次 insert）。小规模做敏感性检查。
    for (label, per_shard, domain) in [
        ("250k/集", 250_000usize, 2_000_000u64),
        ("1M/集(生产)", 1_000_000usize, 8_000_000u64),
    ] {
        eprintln!(
            "[close-bench] -- 规模 {label}: 每片每 distinct 集 {per_shard} 元素（域 {domain}）--"
        );
        let mut base_ns = 1.0;
        for (name, borrowed, fb, fo) in [
            (
                "cur(生产: clone+extend)",
                true,
                merge_accum_cur as fn(&mut StatsAccum, &StatsAccum),
                merge_accum_move as fn(&mut StatsAccum, StatsAccum),
            ),
            (
                "move(免 clone)",
                false,
                merge_accum_cur as fn(&mut StatsAccum, &StatsAccum),
                merge_accum_move as fn(&mut StatsAccum, StatsAccum),
            ),
            (
                "move+reserve",
                false,
                merge_accum_cur as fn(&mut StatsAccum, &StatsAccum),
                merge_accum_move_reserve as fn(&mut StatsAccum, StatsAccum),
            ),
            (
                "move+小并大",
                false,
                merge_accum_cur as fn(&mut StatsAccum, &StatsAccum),
                merge_accum_move_small_into_big as fn(&mut StatsAccum, StatsAccum),
            ),
        ] {
            let ns = run_merge(borrowed, fb, fo, per_shard, domain);
            if name.starts_with("cur") {
                base_ns = ns;
            }
            eprintln!(
                "[close-bench]   {:<22} {:>8.1} ms  ({:>6.1}% of cur)",
                name,
                ns / 1e6,
                ns / base_ns * 100.0
            );
        }
    }

    // ---- 分片数敏感度（配置层决策）: 域固定（8M）, 总行数固定（30M）, 分片
    // 越多 → 每片 distinct 越少但**重复 insert 总数越多**（域重叠被反复插入）;
    // 分片越少 → 单核瓶颈越大。量化归并成本 vs 分片数的关系, 供 rule_shards
    // 配置取舍（q15 归并是协调片单核尾部, 每多一片多一轮全量 extend）。
    eprintln!(
        "[close-bench] -- 分片数敏感度: 域 8M/度量, 每片 distinct 反比于分片数（总 distinct 域不变）--"
    );
    for shards in [4usize, 9, 16] {
        // 每片 distinct ≈ 域 / 片数（30M 行充分采样, 片间域重叠按均匀覆盖近似）;
        // 片数越多, 单片 merge 量越小但归并轮数越多——量化墙钟关系。
        let per_shard = 8_000_000usize / shards;
        let ns = run_merge_shards(
            true,
            merge_accum_cur as fn(&mut StatsAccum, &StatsAccum),
            merge_accum_move as fn(&mut StatsAccum, StatsAccum),
            shards,
            per_shard,
            8_000_000u64,
        );
        eprintln!(
            "[close-bench]   分片 {:>2}: 每片 {:<8} 元素/集 → merge 串行 {:>7.1} ms（每片 {:.0} ns/元素）",
            shards,
            per_shard,
            ns / 1e6,
            ns / (per_shard as f64 * 8.0 * (shards - 1) as f64)
        );
    }
}
