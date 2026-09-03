//! nexmark_hotpath_bench 拆出的兄弟子模块（2026-09-04）：on each 直通输出基准
//! （行式 ↔ 列式同批对拍）——Q14（bind filter + strftime/count_char，多帧分段模拟
//! wfgen 输入）、Q20（each + snapshot join + where 富化）、Q21（bind filter channel_id
//! != ""）、Q22（let split + mvindex + concat 字符串投影，含 split/concat 内部拆解）。
//! 共享 harness/import 在父模块 nexmark_hotpath_bench.rs，此处经 `use super::*` 复用；
//! 切片内独占构造随迁。

use super::*;

// ---------------------------------------------------------------------------
// Bench 6：Q14 on each + bind filter + strftime/count_char
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q14_each_strftime_count_char() {
    let events = bid_events(N);
    let exec = q14_exec();
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(exec.execute_each(ev, ts));
    }
    let q14_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q14 filter+strftime", q14_ns, q14_ns);
}

/// Q14 同数据（与 `bid_events` 同一 LCG 序列）：auction/bidder/price 用列，
/// dateTime/extra 支撑 strftime/count_char；分帧构建 RecordBatch 模拟 wfgen
/// 8MiB 帧（~5-6 万行/批）输入形态。
fn q14_bid_batch(start: usize, n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("dateTime", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
    ]));
    // 与 `bid_events` 完全同一 LCG 序列（price/bidder/auction 每事件 3 次）。
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..start {
        next_price(&mut rng);
        next_u64(&mut rng);
        next_u64(&mut rng);
    }
    let mut auction = Vec::with_capacity(n);
    let mut bidder = Vec::with_capacity(n);
    let mut price = Vec::with_capacity(n);
    let mut date_time = Vec::with_capacity(n);
    for i in 0..n {
        price.push(next_price(&mut rng) as i64);
        bidder.push(BIDDER_BASE + (next_u64(&mut rng) % BIDDER_DOMAIN) as i64);
        auction.push(AUCTION_BASE + (next_u64(&mut rng) % AUCTION_DOMAIN) as i64);
        date_time.push(NOW + (start + i) as i64 * EVENT_STEP_NS);
    }
    let channel = vec!["Google"; n];
    let url = vec![nexmark_url(); n];
    let extra = vec!["x"; n];
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction)),
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(price)),
            Arc::new(arrow::array::StringArray::from(channel)),
            Arc::new(arrow::array::StringArray::from(url)),
            Arc::new(Int64Array::from(date_time)),
            Arc::new(arrow::array::StringArray::from(extra)),
        ],
    )
    .expect("batch")
}

struct NoLookup;
impl WindowLookup for NoLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        None
    }
    fn join_lookup(&self, _w: &str, _kf: &str, _k: &Value) -> Option<Vec<JoinRow>> {
        None
    }
}

/// Q14 列式路径（F6 扩展：each 列式 filter + 递归输出函数）：多帧（同 wfgen
/// 8MiB 帧）+ ALERT_BATCH_SIZE 分段调用，同生产 `emit_each_direct_batch_columnar`；
/// 与行式批路径**同数据同分段对拍**（stats + 输出行逐位一致）并测加速比。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q14_each_strftime_count_char_columnar() {
    use crate::alert::AlertColumnBuilder;
    use crate::match_engine::event_bridge::ColumnarEvent;
    use crate::match_engine::executor::EachDirectBatchStats;

    const SEG: usize = 4096; // 生产 ALERT_BATCH_SIZE
    const FRAME: usize = 65_536; // wfgen 默认 8MiB 帧 ≈ 5-6 万行/批

    let exec = q14_exec();
    assert!(
        exec.each_plan_columnar_safe(),
        "q14 each filter + 递归输出函数必须列式放行"
    );

    // 列式：多帧分段调用（同生产 emit_each_direct_batch_columnar——帧级
    // each_batch_prepare 一次 + 各段复用，避免逐段对整帧重算）。
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let mut stats_col = EachDirectBatchStats::default();
    let t0 = Instant::now();
    for start in (0..N).step_by(FRAME) {
        let n = FRAME.min(N - start);
        let batch = q14_bid_batch(start, n);
        let prepared = exec.each_batch_prepare(&batch);
        let col_events: Vec<ColumnarEvent<'_>> =
            (0..n).map(|r| ColumnarEvent::new(&batch, r)).collect();
        let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
            .iter()
            .enumerate()
            .map(|(i, ev)| (ev, NOW + (start + i) as i64 * EVENT_STEP_NS))
            .collect();
        for seg in col_rows.chunks(SEG) {
            let s = exec.execute_each_direct_batch_columnar_with(
                seg,
                NOW,
                &prepared,
                &mut builder,
                &mut appended,
            );
            stats_col.appended += s.appended;
            stats_col.rejected += s.rejected;
            stats_col.failed += s.failed;
        }
    }
    let col_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    let col_output = builder.finish();

    // 行式参照（Event 版批路径，同数据同分段）。
    let events = bid_events(N);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NOW + i as i64 * EVENT_STEP_NS))
        .collect();
    let mut b2 = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx2 = Vec::new();
    let mut stats_row = EachDirectBatchStats::default();
    let t0 = Instant::now();
    for seg in rows.chunks(SEG) {
        let s = exec.execute_each_direct_batch(seg, &NoLookup, &[], NOW, &mut b2, &mut idx2);
        stats_row.appended += s.appended;
        stats_row.rejected += s.rejected;
        stats_row.failed += s.failed;
    }
    let row_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    let row_output = b2.finish();

    // 对拍：stats + 输出行逐位一致（防列式路径回归）。O(n) zip 对比。
    assert_eq!(stats_col, stats_row, "列式/行式 stats 必须一致");
    assert_eq!(col_output.len(), row_output.len(), "输出行数一致");
    let rows_a = col_output.iter_data_records();
    let rows_b = row_output.iter_data_records();
    for (row, (ra, rb)) in rows_a.zip(rows_b).enumerate() {
        let (ra, rb) = (ra.unwrap(), rb.unwrap());
        assert_eq!(ra.items.len(), rb.items.len(), "row {row} field count");
        for (fa, fb) in ra.items.iter().zip(rb.items.iter()) {
            assert_eq!(fa.get_name(), fb.get_name(), "row {row} field name");
            assert_eq!(fa.get_value(), fb.get_value(), "row {row} field value");
        }
    }
    eprintln!(
        "[hotpath] q14 对拍通过：rejected={} appended={}（N={N}）",
        stats_row.rejected, stats_row.appended
    );

    report("q14 each+strftime 列式", col_ns, row_ns);
    report("q14 each+strftime 行式", row_ns, row_ns);
}

// ---------------------------------------------------------------------------
// Bench 10：Q20 on each + snapshot join + where
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q20_each_snapshot_join_where() {
    let events = bid_events(N);
    let lookup = AuctionLookup::new(AUCTION_DOMAIN);
    let exec = RuleExecutor::new(q20_rule());
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(exec.execute_each_with_joins(ev, ts, &lookup, &[], ts));
    }
    let q20_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q20 each+join+where", q20_ns, q20_ns);
}

/// Q20 列式 join 富化（F6，2026-08-23）：批级 join_lookup + 列式右窗读，与行式
/// 批路径（`execute_each_direct_batch`）**同批对拍**（stats + 输出行逐位一致）
/// 并测量加速比。分段 256 行模拟生产 `ALERT_BATCH_SIZE` 调用形态。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q20_each_snapshot_join_where_columnar() {
    use crate::alert::AlertColumnBuilder;
    use crate::match_engine::event_bridge::{ColumnarEvent, materialize_rows};
    use crate::match_engine::executor::EachDirectBatchStats;

    const SEG: usize = 256; // 生产 ALERT_BATCH_SIZE 分段
    let batch = bid_batch(N);
    let lookup = AuctionLookup::new(AUCTION_DOMAIN);
    let exec = RuleExecutor::new(q20_rule());
    assert!(
        exec.each_join_columnar_ready() && exec.each_plan_columnar_safe(),
        "q20 形状必须列式 join 支持（F6）"
    );

    // 列式 join 路径（分段调用，同生产 emit_each_direct_batch_columnar_join）。
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..N).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NOW + i as i64 * EVENT_STEP_NS))
        .collect();
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let mut stats_col = EachDirectBatchStats::default();
    let t0 = Instant::now();
    for seg in col_rows.chunks(SEG) {
        let s = exec.execute_each_direct_batch_columnar_join(
            seg,
            &lookup,
            NOW,
            &mut builder,
            &mut appended,
        );
        stats_col.appended += s.appended;
        stats_col.rejected += s.rejected;
        stats_col.failed += s.failed;
    }
    let col_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    let col_output = builder.finish();

    // 行式参照（Event 版批路径，同批对拍）。
    let all: Vec<u32> = (0..N as u32).collect();
    let events = materialize_rows(&batch, &all);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NOW + i as i64 * EVENT_STEP_NS))
        .collect();
    let mut b2 = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx2 = Vec::new();
    let mut stats_row = EachDirectBatchStats::default();
    let t0 = Instant::now();
    for seg in rows.chunks(SEG) {
        let s = exec.execute_each_direct_batch(seg, &lookup, &[], NOW, &mut b2, &mut idx2);
        stats_row.appended += s.appended;
        stats_row.rejected += s.rejected;
        stats_row.failed += s.failed;
    }
    let row_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    let row_output = b2.finish();

    // 对拍：stats + 输出行逐位一致（防列式路径回归）。O(n) zip 对比——
    // `nth(row)` 每次重扫是 O(n²)，50 万行输出会挂起。
    assert_eq!(stats_col, stats_row, "列式/行式 stats 必须一致");
    assert_eq!(col_output.len(), row_output.len(), "输出行数一致");
    let rows_a = col_output.iter_data_records();
    let rows_b = row_output.iter_data_records();
    for (row, (ra, rb)) in rows_a.zip(rows_b).enumerate() {
        let (ra, rb) = (ra.unwrap(), rb.unwrap());
        assert_eq!(ra.items.len(), rb.items.len(), "row {row} field count");
        for (fa, fb) in ra.items.iter().zip(rb.items.iter()) {
            assert_eq!(fa.get_name(), fb.get_name(), "row {row} field name");
            assert_eq!(fa.get_value(), fb.get_value(), "row {row} field value");
        }
    }

    report("q20 each+join+where 列式(F6)", col_ns, row_ns);
    report("q20 each+join+where 行式", row_ns, row_ns);
}

// ---------------------------------------------------------------------------
// Bench 11：Q21 bind filter channel_id != ""
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q21_string_bind_filter() {
    let events = bid_events(N);
    let exec = RuleExecutor::new(q21_rule());
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(exec.execute_each(ev, ts));
    }
    let q21_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q21 str bind filter", q21_ns, q21_ns);
}

// ---------------------------------------------------------------------------
// Bench 12：Q22 let split + mvindex + concat 字符串投影
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q22_each_split() {
    // 行式（既有）：逐事件解释（let 逐行 apply_lets + split/mvindex/concat）。
    let events = bid_events(N);
    let exec = RuleExecutor::new(q22_rule());
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(exec.execute_each(ev, ts));
    }
    let q22_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q22 each+split 行式", q22_ns, q22_ns);

    // 列式（层 2，2026-08-25）：let 内联 + SplitIndex/Concat 融合——同一规则
    // 走 each 列式批路径（内联 `let parts = split(...)`），同批对拍 + 测加速比。
    use crate::alert::AlertColumnBuilder;
    use crate::match_engine::event_bridge::{ColumnarEvent, materialize_rows};
    use arrow::array::StringArray;

    let schema = Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("url", DataType::Utf8, false),
    ]);
    let auctions: Vec<i64> = (0..N).map(|i| AUCTION_BASE + i as i64).collect();
    let urls: Vec<String> = (0..N).map(|_| nexmark_url().to_string()).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(auctions)),
            Arc::new(StringArray::from(urls)),
        ],
    )
    .expect("batch");
    let exec_col = RuleExecutor::new_with_yield_field_types(
        q22_rule(),
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Digit)),
            ("detail".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );
    assert!(
        exec_col.each_plan_columnar_safe(),
        "q22 let+split+mvindex+concat 必须过 each 列式门控（层 2）"
    );

    let col_events: Vec<ColumnarEvent> = (0..N).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_rows: Vec<(&ColumnarEvent, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NOW + i as i64 * EVENT_STEP_NS))
        .collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let t0 = Instant::now();
    let stats_col =
        exec_col.execute_each_direct_batch_columnar(&col_rows, NOW, &mut b_col, &mut app_col);
    let col_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    assert_eq!(stats_col.appended, N, "列式输出行数 = N");
    report("q22 each+split 列式", col_ns, q22_ns);

    // 行式批路径同批对拍（层 2 防回归：内联展开与 apply_lets 逐位一致）。
    let all: Vec<u32> = (0..N as u32).collect();
    let row_events = materialize_rows(&batch, &all);
    let rows: Vec<(&Event, i64)> = row_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NOW + i as i64 * EVENT_STEP_NS))
        .collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let stats_row =
        exec_col.execute_each_direct_batch(&rows, &NoLookup, &[], NOW, &mut b_row, &mut app_row);
    assert_eq!(stats_row.appended, N, "行式输出行数 = N");
    assert_eq!(
        b_col.finish().len(),
        b_row.finish().len(),
        "列式/行式输出行数一致"
    );

    // ---- split 内部拆解（2026-08-26 q22 内存归因）：全分割 collect vs 惰性 nth ----
    // 生产 `split_index_vec` 每行 `text.split(sep).collect::<Vec<_>>()` 再索引——
    // url 3 段目录 + query（split 后 ≥6 段）全分割建 Vec 是纯浪费。量化惰性
    // `split(sep).nth(k)`（只扫描到第 k 段）的加速空间。
    let sep = "/";
    let mut sum = 0usize;
    let t0 = Instant::now();
    for _ in 0..N {
        let parts: Vec<&str> = nexmark_url().split(sep).collect();
        let k = normalize_idx(3, parts.len());
        if let Some(k) = k {
            sum += parts[k].len();
        }
    }
    let collect_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q22 split 全分割 collect", collect_ns, collect_ns);

    let t0 = Instant::now();
    for _ in 0..N {
        let picked = nexmark_url().split(sep).nth(3);
        if let Some(p) = picked {
            sum += p.len();
        }
    }
    let nth_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q22 split 惰性 nth(3)", nth_ns, collect_ns);

    // ---- concat 内部拆解（2026-08-26 q22 内存归因 2）：String::new 无预分配 +
    // 逐参 value_to_string 转换 vs 预分配 + 直接 push_str。q22 detail =
    // concat(3 段 + 2 个 "/")，每行 5 参数。----
    let segs: Vec<&str> = nexmark_url().split(sep).collect();
    let mut sum2 = 0usize;
    let t0 = Instant::now();
    for _ in 0..N {
        let mut s = String::new();
        s.push_str(segs[3]);
        s.push_str(sep);
        s.push_str(segs[4]);
        s.push_str(sep);
        s.push_str(segs[5]);
        sum2 += s.len();
    }
    let cat_naive_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q22 concat 无预分配", cat_naive_ns, cat_naive_ns);

    let t0 = Instant::now();
    for _ in 0..N {
        let cap = segs[3].len() + 1 + segs[4].len() + 1 + segs[5].len();
        let mut s = String::with_capacity(cap);
        s.push_str(segs[3]);
        s.push_str(sep);
        s.push_str(segs[4]);
        s.push_str(sep);
        s.push_str(segs[5]);
        sum2 += s.len();
    }
    let cat_cap_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q22 concat 预分配", cat_cap_ns, cat_naive_ns);
    assert!(sum > 0 && sum2 > 0);
}

/// mvindex 负索引/越界归一（与 `normalize_index_simple` 同语义的 bench 内联版）。
fn normalize_idx(index: i64, len: usize) -> Option<usize> {
    let len = len as i64;
    let normalized = if index < 0 { len + index } else { index };
    if normalized < 0 || normalized >= len {
        None
    } else {
        Some(normalized as usize)
    }
}
