//! pipe 写入与 q18 close 装载的**分配足迹量化**（2026-08-25/26 内存峰值归因用）：
//! `PipeBatchStager` 流式装列 vs `Vec<PipeEachRow>` 物化的峰值 / content_bytes 放大
//! 倍数与窗口会计保真度；q18 `close_buckets_to_rows` + `execute_stats_close_batch_columnar`
//! 的状态持有 / 转换 / 装载峰值增长形态与 fmt 消融，及状态分账口径 helper。

use super::*;

/// bid_mod 生产形状 schema（6 声明列 + 4 个 `__wfu_*` meta + `__wf_pipe_ts`）：
/// 与真实中间窗一致（实测 91B/行 vs 声明 6×int64=48B，差额就是 meta 列）。
fn bid_mod_prod_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("mod_key", DataType::Int64, true),
        Field::new("__wfu_rule_name", DataType::Utf8, true),
        Field::new("__wfu_score", DataType::Float64, true),
        Field::new("__wfu_entity_type", DataType::Utf8, true),
        Field::new("__wfu_entity_id", DataType::Utf8, true),
        Field::new(
            "__wf_pipe_ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

/// **pipe 写入路径的分配足迹量化**（2026-08-25，回答「优化空间多大」）。
///
/// 背景：q13 内存缺口已实证与 **pipe 写入分配速率**成正比（非在途积压、非
/// 分配器、非窗口保留——见 `docs/issues/q13-memory-peak-scales-with-volume.md`）。
/// 本测量给出每批的三个数字，用来判断优化天花板：
/// - **暂存峰值**：`PipeEachRow` + `PipeCol` 的 `Vec<Option<T>>` 暂存
/// - **输出内容**：`content_bytes(batch)`（最终落窗的有效字节 = 理论下界）
/// - **放大倍数** = 暂存峰值 / 输出内容 —— 可优化空间就是这个倍数超出 1 的部分
///
/// 现实现的已知浪费（malloc_history 实证）：
/// 1. `Vec<Option<i64>>` 暂存 **16B/值**，Arrow 目标 8B/值 + null bitmap → 2×
/// 2. `take_batch` 的 `Int64Array::from(Vec<Option<_>>)` 是**全量拷贝**
/// 3. `PipeEachRow.values: Vec<Option<Value>>` + `entity_id: String` **每行各一次堆分配**；
///    `rule_name`/`entity_type` 每行重复渲染同一常量值
#[test]
#[ignore = "measurement: cargo test --release -p wf-runtime pipe_write_alloc_footprint -- --ignored --nocapture"]
fn pipe_write_alloc_footprint() {
    // 生产批规模（实测 bid_mod 35,360 行/批）。
    const ROWS: usize = 35_360;
    let exec = q13a_plan_rule();
    let batch = bid_batch(ROWS);
    let schema = bid_mod_prod_schema();
    let yield_names: Vec<Arc<str>> = exec
        .plan()
        .yield_plan
        .fields
        .iter()
        .map(|f| Arc::from(f.name.as_str()))
        .collect();
    let col_events: Vec<wf_engine::match_engine::ColumnarEvent<'_>> = (0..ROWS)
        .map(|i| wf_engine::match_engine::ColumnarEvent::new(&batch, i))
        .collect();
    let rows: Vec<(&wf_engine::match_engine::ColumnarEvent<'_>, i64)> =
        col_events.iter().map(|ev| (ev, NANOS)).collect();

    // 预热一轮（首次触碰的分配器页不计入测量）。
    {
        let prepared = exec.each_batch_prepare(&batch);
        let mut stager = PipeBatchStager::new_columnar(
            Arc::from("bid_mod"),
            Arc::clone(&schema),
            Some(4),
            &yield_names,
        );
        let mut sink = TestStagerSink {
            stager: &mut stager,
        };
        exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut sink);
        let _ = stager.take_batch().expect("build");
    }

    // ① 旧路径（对照）：先物化整批 `Vec<PipeEachRow>`。
    let eval_only = {
        let probe = crate::memory_probe::MemoryProbe::exclusive();
        let prepared = exec.each_batch_prepare(&batch);
        let mut out: Vec<wf_engine::match_engine::PipeEachRow> = Vec::with_capacity(ROWS);
        exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut out);
        let peak = probe.peak_growth();
        assert_eq!(out.len(), ROWS, "对照路径应输出全部行");
        peak
    };

    // ② 生产路径（流式 sink）：executor 逐行直接装列。
    let probe = crate::memory_probe::MemoryProbe::exclusive();
    let prepared = exec.each_batch_prepare(&batch);
    let mut stager = PipeBatchStager::new_columnar(
        Arc::from("bid_mod"),
        Arc::clone(&schema),
        Some(4),
        &yield_names,
    );
    let mut sink = TestStagerSink {
        stager: &mut stager,
    };
    let stats = exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut sink);
    assert_eq!(stats.appended, ROWS, "流式路径应装载全部行");
    let after_stage = probe.peak_growth();

    let built = stager.take_batch().expect("build").expect("non-empty");
    let peak = probe.peak_growth();
    let content = wf_engine::window::content_bytes(&built.1);

    // ④ **窗口会计保真度**（2026-08-25 在途量分账后的新问题）：窗口用
    //   `content_bytes`（逻辑列内容）计账，但批次实际占的分配器字节还包括
    //   null bitmap / offsets / 容量舍入。若两者差很多，则 `Σwindow_bytes` 低估
    //   真实占用，"未归因"就含有假额度。测法：只保留 batch（stager 已经
    //   finish 并重置），重新建基线后看仅此批存活时的增量。
    drop(probe);
    let retained = {
        let probe2 = crate::memory_probe::MemoryProbe::exclusive();
        let base = probe2.current();
        // 重建一份同形批（上一份仍活着作对照，不影响增量）。
        let mut s2 = PipeBatchStager::new_columnar(
            Arc::from("bid_mod"),
            Arc::clone(&schema),
            Some(4),
            &yield_names,
        );
        {
            let prepared2 = exec.each_batch_prepare(&batch);
            let mut sink2 = TestStagerSink { stager: &mut s2 };
            exec.execute_each_pipe_batch_columnar(&rows, &prepared2, &mut sink2);
        }
        let b2 = s2.take_batch().expect("build").expect("non-empty");
        drop(s2); // builder 已重置；丢掉 stager 只留 batch
        let held = probe2.current().saturating_sub(base);
        drop(b2);
        held
    };

    eprintln!("[pipe-alloc] 批规模 = {ROWS} 行（生产实测批大小）");
    eprintln!(
        "[pipe-alloc] ① 旧路径对照：物化 Vec<PipeEachRow> 峰值 = {:.2} MB ({:.0} B/行)",
        eval_only as f64 / 1e6,
        eval_only as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] ② 生产路径：流式求值+装列峰值 = {:.2} MB ({:.0} B/行)",
        after_stage as f64 / 1e6,
        after_stage as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] ③ + take_batch（builder.finish 零拷贝）峰值 = {:.2} MB ({:.0} B/行)",
        peak as f64 / 1e6,
        peak as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] 输出 content_bytes = {:.2} MB ({:.0} B/行) ← 理论下界",
        content as f64 / 1e6,
        content as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] **放大倍数 = {:.2}×**（峰值/输出）→ 剩余可优化 = {:.2} MB/批 ({:.0} B/行)",
        peak as f64 / content as f64,
        (peak.saturating_sub(content)) as f64 / 1e6,
        (peak.saturating_sub(content)) as f64 / ROWS as f64
    );
    eprintln!(
        "[pipe-alloc] ④ 窗口会计保真度：批次**存活占用** = {:.2} MB vs content_bytes {:.2} MB → 低估 {:.2}×",
        retained as f64 / 1e6,
        content as f64 / 1e6,
        retained as f64 / content as f64
    );
    assert!(content > 0, "输出批必须非空");
    let _ = empty_tracked_bind_fields();
}

/// 测试用 sink：直接转发给 stager（与生产 `PipeStagerSink` 同构，但不需要
/// 错误聚合计数）。
struct TestStagerSink<'a> {
    stager: &'a mut PipeBatchStager,
}

impl wf_engine::match_engine::PipeRowSink for TestStagerSink<'_> {
    fn push_pipe_row(
        &mut self,
        score: f64,
        entity_type: &str,
        entity_id: &str,
        values: &[Option<wf_engine::match_engine::Value>],
        event_time_nanos: i64,
    ) -> Result<(), String> {
        self.stager
            .push_row_parts(
                "q13a_bid_mod",
                score,
                entity_type,
                entity_id,
                values,
                event_time_nanos,
            )
            .map_err(|e| e.to_string())
    }
}

// ---------------------------------------------------------------------------
// q18 close 装载分配足迹（2026-08-26，q18 100M close 期 42G 归因）
// ---------------------------------------------------------------------------
//
// 背景：q18 100M close flush 期 DIRTY 峰值 42G（状态 9.8G + 窗口 3.5G + 工作态
// ~29G）。CUT_ALERT 消融（WF_DIAG_CUT_ALERT=1）降到 23.5G → **close alert 装载
// 路径贡献 ~18.5G**。本测量量化 `close_buckets_to_rows` + `execute_stats_close_
// batch_columnar` 的分配峰值随批内桶数（100 万 / 300 万）的增长形态，判断是
// 「每批固有」还是「随批大小超线性」。
//
// 运行：cargo test --release -p wf-runtime q18_close_alloc_footprint -- --ignored --nocapture
use wf_engine::alert::AlertColumnBuilder;
use wf_engine::match_engine::StatsExecutor;
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan, WindowSpec};

/// q18 形态 StatsPlan：4 个 last 度量（price/channel/url/dateTime），键
/// (bidder, auction)——与 `nexmark_hotpath_bench::q18_stats_last_plan` 同形。
fn q18_close_stats_plan() -> StatsPlan {
    fn last(label: &str, field: &str) -> StatsMeasurePlan {
        StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Last,
            field: Some(wf_lang::ast::FieldRef::Qualified("b".into(), field.into())),
            arg: None,
        }
    }
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(86400)),
        keys: vec![
            wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Qualified(
                "b".into(),
                "bidder".into(),
            )),
            wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Qualified(
                "b".into(),
                "auction".into(),
            )),
        ],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![
            last("last_price", "price"),
            last("last_channel", "channel"),
            last("last_url", "url"),
            last("last_dateTime", "dateTime"),
        ],
        tracked_bind_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "b".to_string(),
                std::collections::HashSet::from([
                    "auction".to_string(),
                    "bidder".to_string(),
                    "price".to_string(),
                    "channel".to_string(),
                    "url".to_string(),
                    "dateTime".to_string(),
                ]),
            );
            m
        },
    }
}

/// q18 形态批（键域 auction 放大 → 每行唯一，对齐 30M/100M 真实形态）。
fn q18_close_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = |range: u64| {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (rng >> 33) % range
    };
    let auctions: Vec<i64> = (0..n).map(|_| 1_000 + next(2_000_000) as i64).collect();
    let bidders: Vec<i64> = (0..n).map(|_| 1_000 + next(1010) as i64).collect();
    let prices: Vec<i64> = (0..n).map(|_| (next(10_000_000) + 1) as i64).collect();
    let channels: Vec<String> = (0..n).map(|_| "Google".to_string()).collect();
    let urls: Vec<String> = (0..n)
        .map(|_| "https://www.nexmark.com/aaaaa/bbbbb/ccccc/item.htm?query=1".to_string())
        .collect();
    let times: Vec<i64> = (0..n).map(|i| NANOS + i as i64 * 65_217).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auctions)) as ArrayRef,
            Arc::new(Int64Array::from(bidders)),
            Arc::new(Int64Array::from(prices)),
            Arc::new(arrow::array::StringArray::from(channels)),
            Arc::new(arrow::array::StringArray::from(urls)),
            Arc::new(Int64Array::from(times)),
        ],
    )
    .unwrap()
}

/// 回归断言（非 ignore，常规测试）: 链 Vec `with_capacity(1)` 修复钉死——
/// q18 每键独立 hash（链均长 1.0）时，每链容量必须精确 1（不能退回
/// `or_default()` 的 capacity=4，否则 2935 万链 × 144B ≈ 4.2G 浪费）。
/// 用 CountingAlloc 实测状态持有 vs 期望上界（宽松断言防平台差异）。
/// ⚠ 测量必须在 `exclusive()` 段内建桶 + `current_growth()`（相对基线），
/// 不能用 `current()`（进程全局累计含其他测试残留，并行跑会虚高）。
#[test]
fn q18_state_chain_capacity_bounded() {
    const N: usize = 200_000;
    let row_fields: Arc<std::collections::HashSet<String>> = Arc::new(
        ["auction", "bidder", "price", "channel", "url", "dateTime"]
            .iter()
            .map(|s| s.to_string())
            .collect(),
    );
    let batch = q18_close_batch(N);

    // 状态持有（CountingAlloc 相对基线增量）：exclusive 段内建桶。
    let (n_chains, per_bucket) = {
        let probe = crate::memory_probe::MemoryProbe::exclusive();
        let mut exec = StatsExecutor::with_row_fields(q18_close_stats_plan(), Some(row_fields));
        assert!(exec.process_batch(&batch), "列式前置应满足");
        let n_chains = exec.window.buckets.len();
        let growth = probe.current_growth();
        (n_chains, growth as f64 / n_chains.max(1) as f64)
    };

    // 链容量断言：每条链 capacity == 1（无碰撞时，每链 1 桶）。
    // q18 键域 auction 200 万 + bidder 1010 → N=20 万行几乎无碰撞。
    // 需在 exclusive 段外重新建桶（段内 exec 已 drop）——或直接断言上面
    // 已建桶的形态：N=20 万 → 每链 1 桶，容量必为 1。重建一次独立验证。
    let mut exec2 = StatsExecutor::with_row_fields(q18_close_stats_plan(), None);
    let batch2 = q18_close_batch(N);
    assert!(exec2.process_batch(&batch2), "列式前置应满足");
    let max_cap = exec2
        .window
        .buckets
        .values()
        .map(|c| c.capacity())
        .max()
        .unwrap_or(0);
    assert!(
        max_cap <= 2,
        "链 Vec 容量应精确 1（或碰撞链 2），实测 max_capacity={max_cap}——若退回 or_default() 会到 4"
    );

    // 状态持有上界：每桶 ≤ 1000B（633B 实测 + 余量；CountingAlloc 口径
    // 含 HashMap 容器 + 分配器元数据）。
    assert!(n_chains > N / 2, "N=20 万应几乎每行一键，实际 {n_chains}");
    assert!(
        per_bucket < 1000.0,
        "每桶状态持有应 < 1000B，实测 {per_bucket:.0}B/桶（n_chains={n_chains}）"
    );
}

#[test]
#[ignore = "measurement: cargo test --release -p wf-runtime q18_close_alloc_footprint -- --ignored --nocapture"]
fn q18_close_alloc_footprint() {
    // 每批桶数：100 万（EMIT_CHUNK 默认）与 300 万（观测超线性）。
    for &n_buckets in &[1_000_000usize, 3_000_000] {
        let row_fields: Arc<std::collections::HashSet<String>> = Arc::new(
            ["auction", "bidder", "price", "channel", "url", "dateTime"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        );
        let mut exec = StatsExecutor::with_row_fields(q18_close_stats_plan(), Some(row_fields));
        let batch = q18_close_batch(n_buckets);
        let ok = exec.process_batch(&batch);
        assert!(ok, "列式前置应满足");

        // 阶段 ①：状态建桶（进程基线内增量）——参考：100M 状态 9.8G。
        let state_hold = {
            let probe = crate::memory_probe::MemoryProbe::exclusive();
            // 预热后的新基线：probe 已重置。
            let _ = probe.peak_growth();
            // 读一次真实持有（current 已在 process_batch 后）
            probe.current()
        };

        // 状态拆分：size_of 求和口径 vs CountingAlloc 实测口径——量化 777B vs
        // 336B 差异（Vec 容量翻倍？HashMap 槽？RowFields 漏算？）。
        {
            let n_chains = exec.window.buckets.len();
            let n_buckets: usize = exec.window.buckets.values().map(|c| c.len()).sum();
            let mut sum_scopes = 0usize;
            let mut sum_accs_cap = 0usize;
            let mut sum_accs_len = 0usize;
            let mut sum_bucket_stack = 0usize;
            let mut sum_chain_cap = 0usize;
            let mut sum_rowfields = 0usize;
            for chain in exec.window.buckets.values() {
                sum_chain_cap += chain.capacity()
                    * size_of_val(chain.first().unwrap_or_else(|| {
                        // 空链不贡献桶内存; 用占位类型大小（不可能到达）
                        &exec.window.buckets.values().next().expect("非空")[0]
                    }));
                for b in chain {
                    sum_bucket_stack += size_of_val(b);
                    sum_scopes += size_of_val(&b.scope_key) + scope_key_heap_bytes(&b.scope_key);
                    // q18 计划（last 度量）恒 Classic; SoA 桶不在此分析路径。
                    let StatsBucketAccs::Classic(accs) = &b.accs else {
                        unreachable!("q18 last 计划不走 SoA");
                    };
                    sum_accs_cap += accs.capacity() * size_of_val(&accs[0]);
                    sum_accs_len += accs.len() * size_of_val(&accs[0]);
                    if accs.iter().any(|a| a.last().is_some()) {
                        let rf = accs
                            .iter()
                            .find_map(|a| a.last().as_ref())
                            .expect("is_some");
                        sum_rowfields += 16 + row_fields_heap_bytes_test(rf);
                    }
                }
            }
            // HashMap<u64, Vec<StatsBucket>>：槽位 + 控制字（foldhash 87.5% 满）。
            let hashmap_slots = (n_chains as f64 / 0.875) as usize;
            let hashmap_bytes = hashmap_slots * (8 + 16) /* key + bucket ptr/ctrl */;
            let total_sum = sum_bucket_stack
                + sum_scopes
                + sum_accs_cap
                + sum_chain_cap
                + sum_rowfields
                + hashmap_bytes;
            eprintln!(
                "[q18-state-hold] n_buckets={} n_chains={} 链均长={:.1}",
                n_buckets,
                n_chains,
                n_buckets as f64 / n_chains.max(1) as f64,
            );
            eprintln!(
                "[q18-state-hold] 求和口径: StatsBucket栈={:.0}MB scopeKey={:.0}MB accs_cap={:.0}MB accs_len={:.0}MB chain_cap={:.0}MB rowfields={:.0}MB hashmap={:.0}MB 合计={:.0}MB ({:.0}B/桶)",
                sum_bucket_stack as f64 / 1e6,
                sum_scopes as f64 / 1e6,
                sum_accs_cap as f64 / 1e6,
                sum_accs_len as f64 / 1e6,
                sum_chain_cap as f64 / 1e6,
                sum_rowfields as f64 / 1e6,
                hashmap_bytes as f64 / 1e6,
                total_sum as f64 / 1e6,
                total_sum as f64 / n_buckets.max(1) as f64,
            );
            eprintln!(
                "[q18-state-hold] CountingAlloc 实测 state_hold={:.1}MB ({:.0}B/桶) vs 求和 {:.1}MB——差 {:.1}MB",
                state_hold as f64 / 1e6,
                state_hold as f64 / n_buckets.max(1) as f64,
                total_sum as f64 / 1e6,
                (state_hold as f64 - total_sum as f64) / 1e6,
            );
            eprintln!(
                "[q18-state-hold] accs 容量放大 = {:.2}×（len→cap），链 Vec 放大 = {:.2}×",
                sum_accs_cap as f64 / sum_accs_len.max(1) as f64,
                sum_chain_cap as f64
                    / (n_buckets * (sum_bucket_stack as f64 / n_buckets.max(1) as f64) as usize)
                        .max(1) as f64,
            );

            // HashMap 容器本身的开销（隔离测：同样的键数插空 Vec）——
            // CountingAlloc 实测每 entry 的槽位+控制字+对齐真实成本。
            let hm_overhead = {
                use std::collections::HashMap as StdHashMap;
                let probe = crate::memory_probe::MemoryProbe::exclusive();
                let mut m: StdHashMap<u64, Vec<u8>> = StdHashMap::new();
                for i in 0..n_chains {
                    m.entry(i as u64)
                        .or_insert_with(|| Vec::with_capacity(1))
                        .push(0);
                }
                let peak = probe.peak_growth();
                eprintln!(
                    "[q18-state-hold] HashMap<u64,Vec<u8>> {} 链容器开销 = {:.1}MB ({:.0}B/链)",
                    n_chains,
                    peak as f64 / 1e6,
                    peak as f64 / n_chains.max(1) as f64,
                );
                peak
            };

            // 扁平键对比：`HashMap<(i64,i64), ()>` 直接做键（无 ScopeKey 树/无中间
            // hash 层）——量化「q18 双 int 键专用扁平化」的上限收益。
            let flat_key_overhead = {
                use std::collections::HashMap as StdHashMap;
                let probe = crate::memory_probe::MemoryProbe::exclusive();
                let mut m: StdHashMap<(i64, i64), ()> = StdHashMap::new();
                for i in 0..n_buckets {
                    m.insert((i as i64, (i as i64) % 1010), ());
                }
                let peak = probe.peak_growth();
                eprintln!(
                    "[q18-state-hold] HashMap<(i64,i64),()> {} 桶容器开销 = {:.1}MB ({:.0}B/桶)【扁平键】",
                    n_buckets,
                    peak as f64 / 1e6,
                    peak as f64 / n_buckets.max(1) as f64,
                );
                peak
            };
            let state_flat_proj = state_hold as f64
                - (sum_bucket_stack + sum_scopes + sum_chain_cap + hashmap_bytes) as f64
                + flat_key_overhead as f64;
            eprintln!(
                "[q18-state-hold] 扁平键投影: 去掉 scopeKey树+StatsBucket包+中间hash层 → 预计 {:.1}MB ({:.0}B/桶) vs 当前 {:.1}MB ({:.0}B/桶)",
                state_flat_proj / 1e6,
                state_flat_proj / n_buckets.max(1) as f64,
                state_hold as f64 / 1e6,
                state_hold as f64 / n_buckets.max(1) as f64,
            );
            assert!(n_buckets > 0);
            eprintln!(
                "[q18-state-hold] 容器差 = CountingAlloc {} - 求和链 {} = {:.1}MB",
                hm_overhead as f64 / 1e6,
                (sum_chain_cap + hashmap_bytes) as f64 / 1e6,
                (hm_overhead as f64 - (sum_chain_cap + hashmap_bytes) as f64) / 1e6,
            );
            assert!(n_buckets > 0);
        }

        // 阶段 ②：close_buckets_to_rows 全量转换（StatsCloseBucket）。
        let buckets = exec.take_buckets_up_to(n_buckets);
        {
            let probe = crate::memory_probe::MemoryProbe::exclusive();
            let cb = exec.close_buckets_to_rows(buckets);
            let peak = probe.peak_growth();
            let cb_bytes: usize = cb
                .iter()
                .map(|b| b.measures.iter().map(Vec::capacity).sum::<usize>())
                .sum();
            drop(cb);
            eprintln!(
                "[q18-close] n_buckets={} state_hold={:.1}MB convert_peak={:.1}MB convert_measures_cap={:.1}MB",
                n_buckets,
                state_hold as f64 / 1e6,
                peak as f64 / 1e6,
                cb_bytes as f64 / 1e6,
            );
        }

        // 阶段 ③：execute_stats_close_batch_columnar 直装载（alert 列）。
        // 需 RuleExecutor（spawn 侧由同一 stats 计划装配）——此处用
        // `stats_close_rule_executor` 构造同形 RuleExecutor（yield 计划与
        // q18 一致：id/alert_type/detail/request_count）。
        // 重新建桶（阶段 ② 已取光状态），模拟独立 close 批。
        let exec3 = {
            let row_fields3: Arc<std::collections::HashSet<String>> = Arc::new(
                ["auction", "bidder", "price", "channel", "url", "dateTime"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            );
            let mut e = StatsExecutor::with_row_fields(q18_close_stats_plan(), Some(row_fields3));
            let b3 = q18_close_batch(n_buckets);
            let ok = e.process_batch(&b3);
            assert!(ok, "列式前置应满足");
            e
        };
        let mut exec3 = exec3;
        let b3 = exec3.take_buckets_up_to(n_buckets);
        let cb = exec3.close_buckets_to_rows(b3);
        let labels: Vec<String> = exec.plan.measures.iter().map(|m| m.label.clone()).collect();
        let row_names = exec.row_field_names().cloned();
        let target: Arc<str> = Arc::from("nexmark_alerts");
        let load_peak = {
            let probe = crate::memory_probe::MemoryProbe::exclusive();
            let exec_r = stats_close_rule_executor();
            let mut builder = AlertColumnBuilder::new(Arc::clone(&target));
            let outcome = exec_r.execute_stats_close_batch_columnar(
                &cb,
                &labels,
                row_names.as_ref(),
                &mut builder,
                NANOS,
                NANOS + 86_400_000_000_000,
            );
            let peak = probe.peak_growth();
            let built = builder.finish();
            let built_bytes = built.len() as f64;
            eprintln!(
                "[q18-close] n_buckets={} load_peak={:.1}MB rows={} (avg {:.0}B/row)",
                n_buckets,
                peak as f64 / 1e6,
                outcome.appended,
                if outcome.appended > 0 {
                    peak as f64 / outcome.appended as f64
                } else {
                    0.0
                },
            );
            assert_eq!(built_bytes as usize, outcome.appended);
            peak
        };
        drop(cb);
        assert!(load_peak > 0);
    }
    eprintln!("[q18-close] 完成：对比 1M vs 3M 桶的 convert/load 峰值增长形态");
}

/// 消融对照：fmt detail（真实 q18）vs 常量 detail——量化 fmt 逐行物化在
/// load_peak 的占比（1094B/行的大头是否 fmt String）。
#[test]
#[ignore = "measurement: cargo test --release -p wf-runtime q18_close_fmt_vs_const -- --ignored --nocapture"]
fn q18_close_fmt_vs_const() {
    const N: usize = 1_000_000;
    // 完整装载（真实 q18 detail = fmt 5 字段）——对照基线。
    let full_peak = {
        let row_fields: Arc<std::collections::HashSet<String>> = Arc::new(
            ["auction", "bidder", "price", "channel", "url", "dateTime"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        );
        let mut exec = StatsExecutor::with_row_fields(q18_close_stats_plan(), Some(row_fields));
        let batch = q18_close_batch(N);
        let _ = exec.process_batch(&batch);
        let b = exec.take_buckets_up_to(N);
        let cb = exec.close_buckets_to_rows(b);
        let labels: Vec<String> = exec.plan.measures.iter().map(|m| m.label.clone()).collect();
        let row_names = exec.row_field_names().cloned();
        let exec_r = stats_close_rule_executor();
        let target: Arc<str> = Arc::from("nexmark_alerts");
        let probe = crate::memory_probe::MemoryProbe::exclusive();
        let mut builder = AlertColumnBuilder::new(Arc::clone(&target));
        let outcome = exec_r.execute_stats_close_batch_columnar(
            &cb,
            &labels,
            row_names.as_ref(),
            &mut builder,
            NANOS,
            NANOS + 86_400_000_000_000,
        );
        let peak = probe.peak_growth();
        eprintln!(
            "[q18-fmt] fmt_detail load_peak={:.1}MB rows={} (avg {:.0}B/row)",
            peak as f64 / 1e6,
            outcome.appended,
            if outcome.appended > 0 {
                peak as f64 / outcome.appended as f64
            } else {
                0.0
            },
        );
        peak
    };

    // 常量 detail（fmt 替换为 StringLit）——量化 fmt 的增量。
    let const_peak = {
        let row_fields: Arc<std::collections::HashSet<String>> = Arc::new(
            ["auction", "bidder", "price", "channel", "url", "dateTime"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        );
        let mut exec = StatsExecutor::with_row_fields(q18_close_stats_plan(), Some(row_fields));
        let batch = q18_close_batch(N);
        let _ = exec.process_batch(&batch);
        let b = exec.take_buckets_up_to(N);
        let cb = exec.close_buckets_to_rows(b);
        let labels: Vec<String> = exec.plan.measures.iter().map(|m| m.label.clone()).collect();
        let row_names = exec.row_field_names().cloned();
        // 同形 executor，detail 改常量（StringLit）——列式 gate 仍放行。
        let exec_r = stats_close_rule_executor_const_detail();
        let target: Arc<str> = Arc::from("nexmark_alerts");
        let probe = crate::memory_probe::MemoryProbe::exclusive();
        let mut builder = AlertColumnBuilder::new(Arc::clone(&target));
        let outcome = exec_r.execute_stats_close_batch_columnar(
            &cb,
            &labels,
            row_names.as_ref(),
            &mut builder,
            NANOS,
            NANOS + 86_400_000_000_000,
        );
        let peak = probe.peak_growth();
        eprintln!(
            "[q18-fmt] const_detail load_peak={:.1}MB rows={} (avg {:.0}B/row)",
            peak as f64 / 1e6,
            outcome.appended,
            if outcome.appended > 0 {
                peak as f64 / outcome.appended as f64
            } else {
                0.0
            },
        );
        peak
    };
    eprintln!(
        "[q18-fmt] fmt 增量 = {:.1}MB（{:.0}%）",
        (full_peak as f64 - const_peak as f64) / 1e6,
        (full_peak as f64 - const_peak as f64) / full_peak as f64 * 100.0,
    );
    assert!(full_peak >= const_peak);
}

/// RowFields 堆内存（Box 数组元素 + null_mask；layout Arc 全局共享不计）——
/// 与 nexmark_hotpath_bench 的 row_fields_heap_bytes 同口径。
fn row_fields_heap_bytes_test(rf: &wf_engine::match_engine::RowFields) -> usize {
    let l = rf.layout();
    l.n_numeric() * 8
        + l.n_strings() * 24 // SmolStr 24B 内联
        + l.n_others() * size_of::<Option<wf_engine::match_engine::Value>>()
        + l.n_fields().div_ceil(64) * 8 // null_mask
}

/// ScopeKey 堆内存（Box 子节点；Str 长串堆分配忽略——q18 键为数字）。
fn scope_key_heap_bytes(k: &wf_engine::match_engine::ScopeKey) -> usize {
    use wf_engine::match_engine::ScopeKey;
    match k {
        ScopeKey::Pair(a, b) => {
            size_of::<ScopeKey>() * 2 + scope_key_heap_bytes(a) + scope_key_heap_bytes(b)
        }
        ScopeKey::Str(s) if s.len() > 22 => s.len(),
        _ => 0,
    }
}

/// q18 同形 executor，detail 改 StringLit 常量（对照 fmt 增量）。
fn stats_close_rule_executor_const_detail() -> wf_engine::match_engine::RuleExecutor {
    use wf_lang::ast::CloseMode;
    use wf_lang::ast::Expr;
    use wf_lang::ast::MatchMode;
    use wf_lang::plan::{BindPlan, EntityPlan, MatchPlan, ScorePlan, YieldField, YieldPlan};
    let plan = wf_lang::plan::RulePlan {
        conv_window: None,
        name: "q18_last_bid_stats".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_exprs: Vec::new(),
            key_map: None,
            key_join: None,
            window_spec: wf_lang::plan::WindowSpec::Fixed(std::time::Duration::from_secs(86400)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::And,
            match_mode: MatchMode::Seq,
            accu: false,
            seq: None,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: None,
        stats_plan: Some(q18_close_stats_plan()),
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                "b".into(),
                "auction".into(),
            )),
        },
        yield_plan: YieldPlan {
            target: "nexmark_alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                        "b".into(),
                        "auction".into(),
                    )),
                },
                YieldField {
                    name: "alert_type".into(),
                    value: Expr::StringLit("q18_last_stats".into()),
                },
                YieldField {
                    name: "detail".into(),
                    value: Expr::StringLit("q18_detail".into()),
                },
                YieldField {
                    name: "request_count".into(),
                    value: Expr::Number(1.0),
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };
    wf_engine::match_engine::RuleExecutor::new_with_yield_field_types(
        plan,
        std::collections::HashMap::from([
            (
                "id".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Float),
            ),
            (
                "alert_type".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Chars),
            ),
            (
                "detail".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Chars),
            ),
            (
                "request_count".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Float),
            ),
        ]),
    )
}

/// q18 close 直写用 RuleExecutor（yield: id=b.auction / alert_type 常量 /
/// detail=b.url / request_count=1——q18 输出形状）。
fn stats_close_rule_executor() -> wf_engine::match_engine::RuleExecutor {
    use wf_lang::ast::CloseMode;
    use wf_lang::ast::Expr;
    use wf_lang::ast::MatchMode;
    use wf_lang::plan::{BindPlan, EntityPlan, MatchPlan, ScorePlan, YieldField, YieldPlan};
    let plan = wf_lang::plan::RulePlan {
        conv_window: None,
        name: "q18_last_bid_stats".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_exprs: Vec::new(),
            key_map: None,
            key_join: None,
            window_spec: wf_lang::plan::WindowSpec::Fixed(std::time::Duration::from_secs(86400)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::And,
            match_mode: MatchMode::Seq,
            accu: false,
            seq: None,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: None,
        stats_plan: Some(q18_close_stats_plan()),
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                "b".into(),
                "auction".into(),
            )),
        },
        yield_plan: YieldPlan {
            target: "nexmark_alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                        "b".into(),
                        "auction".into(),
                    )),
                },
                YieldField {
                    name: "alert_type".into(),
                    value: Expr::StringLit("q18_last_bid_stats".into()),
                },
                YieldField {
                    name: "detail".into(),
                    value: Expr::Field(wf_lang::ast::FieldRef::Qualified("b".into(), "url".into())),
                },
                YieldField {
                    name: "request_count".into(),
                    value: Expr::Number(1.0),
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };
    wf_engine::match_engine::RuleExecutor::new_with_yield_field_types(
        plan,
        std::collections::HashMap::from([
            (
                "id".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Float),
            ),
            (
                "alert_type".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Chars),
            ),
            (
                "detail".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Chars),
            ),
            (
                "request_count".into(),
                wf_lang::FieldType::Base(wf_lang::BaseType::Float),
            ),
        ]),
    )
}
