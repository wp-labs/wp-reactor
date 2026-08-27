//! redb spill 批量写基准——随机插入 vs 批内排序插入（2026-08-27）。
//!
//! 背景：spill 驱逐是"批量随机 hash 插入"（`put_batch` 逐条 insert 无序 hash，
//! q18 实测 157MB/批 25.4s @64MB 页缓存）。B+树随机插入 → 页分裂多/页缓存
//! 命中差。候选优化 = 驱逐批内按 hash 排序 → 近似顺序插入。本基准量化两种
//! 写法的耗时差，用数据决定是否实施（见 `docs/design/async-persist.md` 访问
//! 模式节）。
//!
//! 运行：
//!   cargo test --release -p wf-engine spill_write_bench -- --ignored --nocapture
//!
//! 变量：
//!   - 批大小：6.2 万键（256MB/片 预算单批）/ 24.8 万键（1GB/片 预算单批）
//!   - 页缓存：64MB（默认 WF_SPILL_CACHE_MB）/ 256MB
//!   - 值大小：~633B（q18 每键状态实测；以 1KB 档对照看值大小敏感性）
//!   - 对照组：顺序 key（0..n 连续）——理论下限，验证排序优化的逼近程度
use std::time::Instant;

use redb::Database;

const TABLE: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("spill_bench");

fn temp_db_path(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("spill_bench_{}_{}.rb", tag, std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

/// 确定性伪随机 u64（xorshift）——模拟 `spill_hash` 的随机分布。
fn pseudo_random(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

fn gen_entries(n: usize, value_bytes: usize) -> Vec<(u64, Vec<u8>)> {
    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    (0..n)
        .map(|_| {
            let h = pseudo_random(&mut seed);
            let v = (0..value_bytes)
                .map(|i| (h.wrapping_mul(i as u64 + 1) >> 16) as u8)
                .collect();
            (h, v)
        })
        .collect()
}

/// 一次完整 redb 批量插入计时（新临时库，页缓存参数化）。
fn time_insert(entries: &[(u64, Vec<u8>)], cache_mb: usize) -> f64 {
    let path = temp_db_path("insert");
    let db = Database::builder()
        .set_cache_size(cache_mb.saturating_mul(1024 * 1024))
        .create(&path)
        .expect("create");
    {
        let txn = db.begin_write().expect("begin_write");
        let _ = txn.open_table(TABLE).expect("open");
        txn.commit().expect("commit init");
    }
    let t0 = Instant::now();
    let txn = db.begin_write().expect("begin_write");
    {
        let mut table = txn.open_table(TABLE).expect("open");
        for (h, v) in entries {
            table.insert(*h, v.as_slice()).expect("insert");
        }
    }
    txn.commit().expect("commit");
    let ms = t0.elapsed().as_secs_f64() * 1e3;
    drop(db);
    let _ = std::fs::remove_file(&path);
    ms
}

#[test]
#[ignore]
fn redb_write_random_vs_sorted() {
    eprintln!("[spill-bench] redb 批量 insert: 随机(当前) vs 批内排序 vs 顺序key(理论上限)");
    eprintln!("[spill-bench] 值大小 = q18 每键状态 ~633B");
    for value_bytes in [633usize, 1024] {
        eprintln!("[spill-bench] ---- 值 {value_bytes}B ----");
        eprintln!("[spill-bench] 批       页缓存   随机      排序      提升   顺序key   vs随机");
        for (label, n) in [("62k", 62_000usize), ("248k", 248_000usize)] {
            for cache_mb in [64usize, 256] {
                let entries = gen_entries(n, value_bytes);
                // 当前实现：无序插入
                let random_ms = time_insert(&entries, cache_mb);
                // 候选优化：批内按 hash 排序后插入
                let mut sorted = entries.clone();
                sorted.sort_by_key(|e| e.0);
                let sorted_ms = time_insert(&sorted, cache_mb);
                // 理论上限：连续 key（完全顺序写）
                let seq: Vec<(u64, Vec<u8>)> =
                    (0..n).map(|i| (i as u64, vec![0u8; value_bytes])).collect();
                let seq_ms = time_insert(&seq, cache_mb);
                let speedup = random_ms / sorted_ms;
                let seq_ratio = seq_ms / random_ms;
                eprintln!(
                    "[spill-bench] {label:>5}  {cache_mb:>4}MB  {random_ms:>8.1}ms  {sorted_ms:>8.1}ms  {speedup:>4.1}x  {seq_ms:>7.1}ms  {seq_ratio:>4.1}x"
                );
            }
        }
    }
}
