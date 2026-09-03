//! q13a/q4a 中间窗装载路径微基准与 row vs pipe 列式字节对拍（2026-08-25/26 数据驱动
//! 定位用）：per-record 求值 → `PipeBatchStager::push_record` 行式装载、列式全链
//! （`execute_each_pipe_batch_columnar` + `push_row`）对照、q4a deferred 行式/列式
//! staging 对比，及双路径字节一致性对拍（含 null/负值/时间回退/Missing/meta 边缘）。

use super::*;

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-runtime q13a_pipe_bench -- --ignored --nocapture"]
fn q13a_pipe_bench() {
    let exec = q13a_plan_rule();
    let lookup = NoLookup;

    let batch = bid_batch(N);
    let events: Vec<Arc<wf_engine::match_engine::Event>> =
        batch_to_events(&batch).into_iter().map(Arc::new).collect();

    // ---- ⓪ 物化：batch_to_events（process_batch 的 eager_events 构造，
    // 每批全量物化 Event HashMap——q13a 是 row path，必须物化） ----
    let start = Instant::now();
    for _ in 0..8 {
        let _ = batch_to_events(&batch);
    }
    let materialize_ns = start.elapsed().as_nanos() as f64 / (N as f64 * 8.0);

    // 每个字段的排序（wfx_id hashing 用）——模拟 rule_task 的 each_field_order。
    let first = &events[0];
    let mut field_order: Vec<&smol_str::SmolStr> = first.fields.keys().collect();
    field_order.sort_unstable();

    // ---- ① per-record 求值：execute_each_with_joins → OutputRecord ----
    // 每行产出 OutputRecord（q13a 实际路径：intermediate target 无批量路径）。
    let start = Instant::now();
    let mut records: Vec<OutputRecord> = Vec::with_capacity(N);
    for ev in &events {
        if let Ok(Some(record)) =
            exec.execute_each_with_joins(ev, NANOS, &lookup, &field_order, NANOS)
        {
            records.push(record);
        }
    }
    let per_record_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // ---- ② 中间窗装载：PipeBatchStager::push_record ----
    let mut stager = PipeBatchStager::new(
        Arc::from("bid_mod"),
        bid_mod_schema(),
        Some(4), // dateTime 列
    );
    let start = Instant::now();
    for r in &records {
        stager.push_record(r).expect("stage row");
    }
    let stage_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // ---- 组合 = q13a 每行总成本 ----
    let total_ns = per_record_ns + stage_ns;

    // ---- ③ 对照：execute_each_direct（直写 builder，无 OutputRecord）----
    let mut builder = wf_engine::alert::AlertColumnBuilder::new(Arc::from("alerts"));
    let start = Instant::now();
    for ev in &events {
        let _ = exec.execute_each_direct(ev, NANOS, &lookup, &field_order, NANOS, &mut builder);
    }
    let direct_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // ---- ④ 对照：批量路径 execute_each_direct_batch ----
    let mut builder = wf_engine::alert::AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let rows: Vec<(&wf_engine::match_engine::Event, i64)> =
        events.iter().map(|e| (e.as_ref(), NANOS)).collect();
    let start = Instant::now();
    for chunk in rows.chunks(4096) {
        let _ = exec.execute_each_direct_batch(
            chunk,
            &lookup,
            &field_order,
            NANOS,
            &mut builder,
            &mut appended,
        );
    }
    let batch_ns = start.elapsed().as_nanos() as f64 / N as f64;

    eprintln!("[q13a-pipe-bench] N = {N}, records = {}", records.len());
    report("⓪ 物化 batch_to_events", materialize_ns, total_ns);
    report(
        "① per-record (execute_each_with_joins)",
        per_record_ns,
        total_ns,
    );
    report("② stage (PipeBatchStager::push_record)", stage_ns, total_ns);
    report(
        "q13a process_batch 每行合计 (⓪+①+②)",
        materialize_ns + total_ns,
        total_ns,
    );
    report("q13a 每行合计 (①+②)", total_ns, total_ns);
    report(
        "对照: execute_each_direct (无 OutputRecord)",
        direct_ns,
        total_ns,
    );
    report("对照: execute_each_direct_batch (批量)", batch_ns, total_ns);
    eprintln!(
        "[q13a-pipe-bench] 批量路径 vs q13a 生产路径(含物化) = {:.1}x",
        (materialize_ns + total_ns) / batch_ns
    );
    // 防御断言：批量路径应显著快于 per-record+stage（>1.5x）。
    assert!(
        total_ns > batch_ns * 1.5,
        "批量路径应显著快于 per-record+stage：total={total_ns:.1}ns batch={batch_ns:.1}ns"
    );
    let _ = empty_tracked_bind_fields();
}

/// q13a 中间窗装载的 **列式路径** 微基准（2026-08-25 q13a 列式化）：
/// `execute_each_pipe_batch_columnar`（零 Event/OutputRecord 物化）+ `push_row`
/// （预计算列来源计划）。与 ①② 对照：物化 246ns + per-record 526ns + stage 476ns
/// = 1248ns/行 的 row path，应降至几百 ns 量级。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-runtime q13a_pipe_bench -- --ignored --nocapture"]
fn q13a_pipe_columnar_bench() {
    let exec = q13a_plan_rule();
    assert!(
        exec.each_pipe_columnar_safe(),
        "q13a 形状必须满足 pipe 列式门控"
    );
    let batch = bid_batch(N);
    let col_events: Vec<wf_engine::match_engine::ColumnarEvent<'_>> = (0..N)
        .map(|i| wf_engine::match_engine::ColumnarEvent::new(&batch, i))
        .collect();
    let rows: Vec<(&wf_engine::match_engine::ColumnarEvent<'_>, i64)> =
        col_events.iter().map(|ev| (ev, NANOS)).collect();
    let yield_names: Vec<std::sync::Arc<str>> = exec
        .plan()
        .yield_plan
        .fields
        .iter()
        .map(|f| std::sync::Arc::from(f.name.as_str()))
        .collect();

    // ⑤ 求值 + 装载（列式全链）。
    let start = Instant::now();
    let mut total_appended = 0usize;
    for _ in 0..4 {
        let prepared = exec.each_batch_prepare(&batch);
        let mut out: Vec<wf_engine::match_engine::PipeEachRow> = Vec::with_capacity(N);
        let mut stager = PipeBatchStager::new_columnar(
            Arc::from("bid_mod"),
            bid_mod_schema(),
            Some(4),
            &yield_names,
        );
        let stats = exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut out);
        for row in &out {
            stager
                .push_row("q13a_bench", row, NANOS)
                .expect("stage row");
        }
        total_appended += stats.appended;
    }
    let columnar_ns = start.elapsed().as_nanos() as f64 / (N as f64 * 4.0);
    eprintln!(
        "[q13a-pipe-bench] N = {N}, columnar appended = {total_appended} (expect {})",
        N * 4
    );
    report("⑤ columnar pipe (eval+push_row)", columnar_ns, columnar_ns);
    eprintln!(
        "[q13a-pipe-bench] 列式路径 vs row path(含物化) = {:.1}x",
        (246.0 + 526.0 + 476.0) / columnar_ns
    );
    let _ = empty_tracked_bind_fields();
}

/// q4a deferred 中间窗 schema（`__wf_pipe_ts` + 4 yield 列 + 4 meta 列——
/// `record_window_fields` 行式路径会补的四个 `__wfu_meta_*`）。
fn q4a_stager_schema() -> Arc<arrow::datatypes::Schema> {
    use wf_lang::wfu_meta::WfuIntermediateMetaField;
    Arc::new(Schema::new(vec![
        Field::new(
            "__wf_pipe_ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("id", DataType::Int64, true),
        Field::new("category", DataType::Int64, true),
        Field::new("final", DataType::Float64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new(
            WfuIntermediateMetaField::RuleName.name(),
            DataType::Utf8,
            true,
        ),
        Field::new(WfuIntermediateMetaField::Score.name(), DataType::Utf8, true),
        Field::new(
            WfuIntermediateMetaField::EntityType.name(),
            DataType::Utf8,
            true,
        ),
        Field::new(
            WfuIntermediateMetaField::EntityId.name(),
            DataType::Utf8,
            true,
        ),
    ]))
}

/// q4a deferred emit 中间窗 staging 对比（2026-08-26）：deferred 到期评估的
/// `OutputRecord` → `push_record`（行式：`record_window_fields` 的
/// yield_fields clone + HashSet + meta 名 Arc::from 每行分配）vs
/// `push_record_columnar`（列式：col_sources 预计算 + SmolStr 内联 meta 值）。
///
/// 背景：q4 30M EPS 7.66M → 4.25M（回归），q4a 与 q9 deferred 部分同构但
/// yield 到中间窗（q9 直出 sink）——staging 是 q4 掉速主嫌疑。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-runtime q4a_stage_bench -- --ignored --nocapture"]
fn q4a_stage_bench() {
    use wf_engine::alert::AlertOrigin;
    use wf_engine::match_engine::Value;
    let schema = q4a_stager_schema();
    let yield_names: Vec<Arc<str>> = ["id", "category", "final", "dateTime"]
        .iter()
        .map(|s| Arc::from(*s))
        .collect();
    // q4a 产量形状的 OutputRecord（30M 数据 ≈ 1.67M 行，抽样 N）。
    let records: Vec<OutputRecord> = (0..N)
        .map(|i| OutputRecord {
            wfx_id: format!("id-{i}"),
            rule_name: Arc::from("q4a_auction_finals"),
            score: 20.0,
            entity_type: Arc::from("digit"),
            entity_id: (100_000 + i).to_string(),
            origin: AlertOrigin::Deferred,
            fired_at: "2026-08-26T00:00:00Z".to_string(),
            emit_time: Arc::from("2026-08-26T00:00:00Z"),
            matched_rows: Vec::new(),
            summary: Arc::from(""),
            yield_target: Arc::from("auction_finals"),
            yield_fields: vec![
                (Arc::from("id"), Value::Number(i as f64)),
                (Arc::from("category"), Value::Number((i % 5) as f64)),
                (Arc::from("final"), Value::Number(10.0 + i as f64)),
                // dateTime 缺失 → 时间列回退 event_time_nanos
            ],
            yield_field_types: Vec::new().into(),
            event_time_nanos: NANOS + i as i64,
            machine_id: Arc::from(""),
            scope_key: Arc::from(""),
        })
        .collect();

    // 行式 staging（旧路径）。
    let start = Instant::now();
    for _ in 0..4 {
        let mut stager =
            PipeBatchStager::new(Arc::from("auction_finals"), Arc::clone(&schema), Some(4));
        for r in &records {
            stager.push_record(r).expect("row stage");
        }
        let _ = stager.take_batch().expect("build").expect("rows");
    }
    let row_ns = start.elapsed().as_nanos() as f64 / (N as f64 * 4.0);

    // 列式 staging（2026-08-26 新路径）。
    let start = Instant::now();
    for _ in 0..4 {
        let mut stager = PipeBatchStager::new_columnar(
            Arc::from("auction_finals"),
            Arc::clone(&schema),
            Some(4),
            &yield_names,
        );
        for r in &records {
            stager.push_record_columnar(r).expect("col stage");
        }
        let _ = stager.take_batch().expect("build").expect("rows");
    }
    let col_ns = start.elapsed().as_nanos() as f64 / (N as f64 * 4.0);

    eprintln!(
        "[q4a-stage-bench] N={N} 行式 push_record    {:>9.1} ns/row  ({:>7.2}M rows/s)",
        row_ns,
        1e9 / row_ns / 1e6
    );
    eprintln!(
        "[q4a-stage-bench] N={N} 列式 push_record_columnar {:>9.1} ns/row  ({:>7.2}M rows/s)",
        col_ns,
        1e9 / col_ns / 1e6
    );
    eprintln!(
        "[q4a-stage-bench] 列式/行式 = {:.1}x（staging 每行省 record_window_fields 分配）",
        row_ns / col_ns
    );
}
/// （`execute_each_pipe_batch_columnar` → `push_row`）产出的中间窗批次必须
/// **字节一致**——含 meta 回退列（`__wfu_*`）与 `__wf_pipe_ts` 事件时间列。
#[test]
fn q13a_pipe_columnar_matches_row_path() {
    use arrow::array::Array;
    let exec = q13a_plan_rule();
    let batch = bid_batch(N);
    let events: Vec<std::sync::Arc<wf_engine::match_engine::Event>> = batch_to_events(&batch)
        .into_iter()
        .map(std::sync::Arc::new)
        .collect();
    let first = &events[0];
    let mut field_order: Vec<&smol_str::SmolStr> = first.fields.keys().collect();
    field_order.sort_unstable();
    let lookup = NoLookup;

    // 含 meta 回退列 + 事件时间列的中间窗 schema（bid_mod 字段 + __wfu_* + ts）。
    let schema = Arc::new(Schema::new(vec![
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
    ]));

    // Row path.
    let mut row_stager = PipeBatchStager::new(Arc::from("bid_mod"), Arc::clone(&schema), Some(4));
    for ev in &events {
        let record = exec
            .execute_each_with_joins(ev, NANOS, &lookup, &field_order, NANOS)
            .expect("eval")
            .expect("q13a 无 filter → 必有输出");
        row_stager.push_record(&record).expect("stage");
    }
    let (_, _, row_batch) = row_stager.take_events().expect("build").expect("non-empty");

    // Columnar pipe path.
    let prepared = exec.each_batch_prepare(&batch);
    let col_events: Vec<wf_engine::match_engine::ColumnarEvent<'_>> = (0..N)
        .map(|i| wf_engine::match_engine::ColumnarEvent::new(&batch, i))
        .collect();
    let rows: Vec<(&wf_engine::match_engine::ColumnarEvent<'_>, i64)> =
        col_events.iter().map(|ev| (ev, NANOS)).collect();
    let mut out: Vec<wf_engine::match_engine::PipeEachRow> = Vec::with_capacity(N);
    let stats = exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut out);
    assert_eq!(stats.appended, N, "无 filter → 全行输出");
    let yield_names: Vec<std::sync::Arc<str>> = exec
        .plan()
        .yield_plan
        .fields
        .iter()
        .map(|f| std::sync::Arc::from(f.name.as_str()))
        .collect();
    let mut col_stager = PipeBatchStager::new_columnar(
        Arc::from("bid_mod"),
        Arc::clone(&schema),
        Some(4),
        &yield_names,
    );
    for row in &out {
        col_stager
            .push_row("q13a_bench", row, NANOS)
            .expect("stage");
    }
    let (_, _, col_batch) = col_stager.take_events().expect("build").expect("non-empty");

    assert_eq!(row_batch.num_rows(), col_batch.num_rows());
    assert_eq!(row_batch.num_columns(), col_batch.num_columns());
    for (i, (a, b)) in row_batch
        .columns()
        .iter()
        .zip(col_batch.columns().iter())
        .enumerate()
    {
        assert_eq!(
            a.len(),
            b.len(),
            "col {i} ({}): 长度一致",
            schema.field(i).name()
        );
        for row in 0..row_batch.num_rows() {
            assert_eq!(
                a.is_null(row),
                b.is_null(row),
                "col {i} ({}): row {row} null 位一致",
                schema.field(i).name()
            );
            if !a.is_null(row) {
                assert_eq!(
                    arrow::util::display::array_value_to_string(a, row).expect("display"),
                    arrow::util::display::array_value_to_string(b, row).expect("display"),
                    "col {i} ({}): row {row} 值一致",
                    schema.field(i).name()
                );
            }
        }
    }
    let _ = empty_tracked_bind_fields();
}

/// q13a 双路径 对拍 **边界用例**（2026-08-25 review R1/R4 补盲）：row path vs
/// pipe 列式路径在以下边界的字节一致性——
/// - **null 输入行**（auction/bidder 为 null → Field 缺失 → 空串→coerce→省略/空 cell）；
/// - **负值 mod**（auction=-7 → cvec `int_mod` i64 取模 vs 解释器 f64 取模）；
/// - **schema 时间列回退**（yield 未提供 time_fallback 列 → 回退 event_time_nanos）；
/// - **Missing 源列**（schema 列不在 yield/meta → null cell）；
/// - **meta 列**（`__wfu_entity_id`/`__wfu_score`——null entity 的渲染一致）。
#[test]
fn q13a_pipe_columnar_matches_row_path_edge_cases() {
    use arrow::array::Array;

    // 边缘输入批：auction 含 null 与负值，bidder 含 null。
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("note", DataType::Utf8, true),
    ]));
    let auction = vec![Some(-7i64), Some(0), Some(12345), None, Some(8)];
    let bidder = vec![Some(5i64), None, Some(9), Some(2), Some(3)];
    let price = vec![Some(100i64), Some(200), Some(300), Some(400), Some(500)];
    let date_time: Vec<i64> = (0..5).map(|i| NANOS + i).collect();
    let note = vec!["a", "b", "c", "d", "e"];
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::Int64Array::from(auction)) as ArrayRef,
            Arc::new(arrow::array::Int64Array::from(bidder)),
            Arc::new(arrow::array::Int64Array::from(price)),
            Arc::new(arrow::array::TimestampNanosecondArray::from(date_time)),
            Arc::new(arrow::array::StringArray::from(note)),
        ],
    )
    .unwrap();

    // 边缘计划：id=bidder、mod_key=auction%10000、note=note（Field Utf8）。
    let mut plan = q13a_plan_rule_plan();
    plan.yield_plan.fields = vec![
        wf_lang::plan::YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
        },
        wf_lang::plan::YieldField {
            name: "mod_key".into(),
            value: Expr::BinOp {
                op: wf_lang::ast::BinOp::Mod,
                left: Box::new(Expr::Field(FieldRef::Qualified(
                    "b".into(),
                    "auction".into(),
                ))),
                right: Box::new(Expr::Number(10000.0)),
            },
        },
        wf_lang::plan::YieldField {
            name: "note".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "note".into())),
        },
    ];
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.each_pipe_columnar_safe(),
        "edge 形状必须过 pipe 列式门控"
    );

    // 边缘中间窗 schema：yield 三列 + 时间列回退（time_fallback，yield 未提供）
    // + Missing 源列（missing_col）+ meta 列（entity_id/score）。
    let pipe_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("mod_key", DataType::Int64, true),
        Field::new("note", DataType::Utf8, true),
        Field::new(
            "time_fallback",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("missing_col", DataType::Int64, true),
        Field::new("__wfu_entity_id", DataType::Utf8, true),
        Field::new("__wfu_score", DataType::Float64, true),
    ]));
    let time_col_index = Some(3); // time_fallback 是 schema 时间列
    let events: Vec<std::sync::Arc<wf_engine::match_engine::Event>> = batch_to_events(&batch)
        .into_iter()
        .map(std::sync::Arc::new)
        .collect();
    let first = &events[0];
    let mut field_order: Vec<&smol_str::SmolStr> = first.fields.keys().collect();
    field_order.sort_unstable();
    let lookup = NoLookup;

    // Row path.
    let mut row_stager = PipeBatchStager::new(
        Arc::from("edge_pipe"),
        Arc::clone(&pipe_schema),
        time_col_index,
    );
    for (i, ev) in events.iter().enumerate() {
        let record = exec
            .execute_each_with_joins(ev, NANOS + i as i64, &lookup, &field_order, NANOS)
            .expect("eval")
            .expect("无 filter → 必有输出");
        row_stager.push_record(&record).expect("stage");
    }
    let (_, _, row_batch) = row_stager.take_events().expect("build").expect("non-empty");

    // Columnar pipe path.
    let prepared = exec.each_batch_prepare(&batch);
    let col_events: Vec<wf_engine::match_engine::ColumnarEvent<'_>> = (0..batch.num_rows())
        .map(|i| wf_engine::match_engine::ColumnarEvent::new(&batch, i))
        .collect();
    let rows: Vec<(&wf_engine::match_engine::ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NANOS + i as i64))
        .collect();
    let mut out: Vec<wf_engine::match_engine::PipeEachRow> = Vec::new();
    let stats = exec.execute_each_pipe_batch_columnar(&rows, &prepared, &mut out);
    assert_eq!(stats.appended, 5, "无 filter → 全行输出（含 null 行）");
    let yield_names: Vec<std::sync::Arc<str>> = exec
        .plan()
        .yield_plan
        .fields
        .iter()
        .map(|f| std::sync::Arc::from(f.name.as_str()))
        .collect();
    let mut col_stager = PipeBatchStager::new_columnar(
        Arc::from("edge_pipe"),
        Arc::clone(&pipe_schema),
        time_col_index,
        &yield_names,
    );
    for (row, (_, event_nanos)) in out.iter().zip(rows.iter()) {
        col_stager
            .push_row("edge_rule", row, *event_nanos)
            .expect("stage");
    }
    let (_, _, col_batch) = col_stager.take_events().expect("build").expect("non-empty");

    assert_eq!(row_batch.num_rows(), col_batch.num_rows());
    assert_eq!(row_batch.num_columns(), col_batch.num_columns());
    for (i, (a, b)) in row_batch
        .columns()
        .iter()
        .zip(col_batch.columns().iter())
        .enumerate()
    {
        assert_eq!(a.len(), b.len(), "col {i} 长度一致");
        for row in 0..row_batch.num_rows() {
            assert_eq!(
                a.is_null(row),
                b.is_null(row),
                "col {i} ({}): row {row} null 位一致",
                pipe_schema.field(i).name()
            );
            if !a.is_null(row) {
                assert_eq!(
                    arrow::util::display::array_value_to_string(a, row).expect("display"),
                    arrow::util::display::array_value_to_string(b, row).expect("display"),
                    "col {i} ({}): row {row} 值一致",
                    pipe_schema.field(i).name()
                );
            }
        }
    }
    let _ = empty_tracked_bind_fields();
}
