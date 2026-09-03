//! q13b 生产真实路径微基准与并发对照（2026-08-25 q13 1.52M EPS 定位用）：row path
//! （Event clone + snapshot join + fmt 解释求值）与真实 provider 锁路径的每行成本，
//! 以及 T 线程共享一把 provider RwLock 的并发锁竞争形态量化。

use super::*;

/// q13b 生产真实路径微基准：`on each m` + `join side_input snapshot` +
/// `detail = fmt("{}", side_input.value)`。生产 q13b **不走列式 join 路径**——
/// yield 含 fmt 函数（live join 下 columnar gate 拒绝，回退 row path），每行
/// Event clone + join lookup + fmt 解释求值。q13b_join_bench 只测了列式
/// （462ns/行），本段补 row path 的真实成本（2026-08-25 生产 q13b 每行
/// ~14µs，q13a 被其反压）。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-runtime q13a_pipe_bench -- --ignored --nocapture"]
fn q13b_production_path_bench() {
    use std::collections::HashMap as StdMap;
    use wf_engine::match_engine::{JoinKey, JoinRow};
    use wf_lang::ast::JoinMode;
    use wf_lang::plan::{JoinCondPlan, JoinPlan};

    // q13b 形状：on-each + snapshot join（side_input）+ yield detail=fmt
    let mut plan = RulePlan {
        conv_window: None,
        name: "q13b_bench_row".into(),
        binds: vec![BindPlan {
            alias: "m".into(),
            window: "bid_mod".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_exprs: Vec::new(),
            key_map: None,
            key_join: None,
            window_spec: wf_lang::plan::WindowSpec::Fixed(std::time::Duration::ZERO),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: wf_lang::ast::CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "m".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![JoinPlan {
            right_window: "side_input".into(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("m".into(), "mod_key".into()),
                right: FieldRef::Qualified("side_input".into(), "key".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("m".into(), "bidder".into())),
        },
        yield_plan: YieldPlan {
            target: "nexmark_alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(FieldRef::Qualified("m".into(), "bidder".into())),
                },
                YieldField {
                    name: "alert_type".into(),
                    value: Expr::StringLit("q13_sidejoin".into()),
                },
                // 生产真实形态：fmt("{}", side_input.value)——row path 元凶
                YieldField {
                    name: "detail".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "fmt".into(),
                        args: vec![
                            Expr::StringLit("{}".into()),
                            Expr::Field(FieldRef::Qualified("side_input".into(), "value".into())),
                        ],
                    },
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
    plan.binds[0].alias = "m".into();
    plan.binds[0].window = "bid_mod".into();
    let exec = RuleExecutor::new(plan);
    // 2026-08-25 q13b 列式化：`fmt("{}", 限定字段)` 单参数恒等**已被 gate 放行**
    // （列式 join 富化路径读字段后按 fmt 语义渲染，与解释器逐字节一致——见
    // `fmt_identity_field` 与列式对拍）。此前该形状被拒绝、只能走 row path，
    // 是 q13b 1.3µs/行的元凶。本 bench 仍**直接调用 row path 函数**测历史基线
    // （与 gate 无关），供列式路径对照。
    assert!(
        exec.each_plan_columnar_safe(),
        "fmt(\"{{}}\", 限定字段) 在 live join 下应走列式（q13b 列式化放行）"
    );

    // bid_mod 形状批（mod_key 均匀 0..9999）
    let bm_schema = Arc::new(Schema::new(vec![
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
    ]));
    let id: Vec<i64> = (0..N as i64).collect();
    let bidder: Vec<i64> = (0..N as i64).map(|i| i % 100_000).collect();
    let auction: Vec<i64> = (0..N as i64).map(|i| i * 7).collect();
    let price: Vec<i64> = (0..N as i64).map(|i| (i * 37) % 1_000_000).collect();
    let date_time: Vec<i64> = (0..N as i64).map(|i| NANOS + i).collect();
    let mod_key: Vec<i64> = (0..N as i64).map(|i| i % 10000).collect();
    let bm_batch = RecordBatch::try_new(
        bm_schema,
        vec![
            Arc::new(Int64Array::from(id)) as ArrayRef,
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(auction)),
            Arc::new(Int64Array::from(price)),
            Arc::new(arrow::array::TimestampNanosecondArray::from(date_time)),
            Arc::new(Int64Array::from(mod_key)),
        ],
    )
    .unwrap();
    let events: Vec<Arc<wf_engine::match_engine::Event>> = batch_to_events(&bm_batch)
        .into_iter()
        .map(Arc::new)
        .collect();
    let first = &events[0];
    let mut field_order: Vec<&smol_str::SmolStr> = first.fields.keys().collect();
    field_order.sort_unstable();

    // side_input 静态表 join 索引（O(1) lookup，与生产 set_join_key 一致）
    let mut index: StdMap<JoinKey, Vec<JoinRow>> = StdMap::new();
    for k in 0..10000i64 {
        let mut fields = wf_engine::match_engine::EngineHashMap::default();
        fields.insert(
            "key".into(),
            wf_engine::match_engine::Value::Number(k as f64),
        );
        fields.insert(
            "value".into(),
            wf_engine::match_engine::Value::Str(format!("value-{k}").into()),
        );
        let row = JoinRow::Event(Arc::new(wf_engine::match_engine::Event { fields }));
        index
            .entry(JoinKey::from_value(&wf_engine::match_engine::Value::Number(k as f64)).unwrap())
            .or_default()
            .push(row);
    }
    struct IndexedLookup(StdMap<JoinKey, Vec<JoinRow>>);
    impl WindowLookup for IndexedLookup {
        fn snapshot_field_values(
            &self,
            _w: &str,
            _f: &str,
        ) -> Option<std::collections::HashSet<String>> {
            None
        }
        fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
            None
        }
        fn join_lookup(
            &self,
            _w: &str,
            _kf: &str,
            key: &wf_engine::match_engine::Value,
        ) -> Option<Vec<JoinRow>> {
            Some(self.0.get(&JoinKey::from_value(key)?)?.clone())
        }
    }
    let lookup = IndexedLookup(index.clone());

    // 诊断：lookup 命中率（bench 构造正确性检查）
    let mut hits = 0usize;
    for ev in &events {
        if let Some(v) = ev.fields.get("mod_key")
            && lookup.join_lookup("side_input", "key", v).is_some()
        {
            hits += 1;
        }
    }
    eprintln!(
        "[q13b-prod-bench] join_lookup 命中 = {hits}/{}",
        events.len()
    );

    // 生产 row path：execute_each_direct_batch（含 Event clone + join + fmt）
    let mut builder = wf_engine::alert::AlertColumnBuilder::new(Arc::from("nexmark_alerts"));
    let mut appended = Vec::new();
    let rows: Vec<(&wf_engine::match_engine::Event, i64)> =
        events.iter().map(|e| (e.as_ref(), NANOS)).collect();
    let start = Instant::now();
    let mut total_appended = 0usize;
    let mut total_failed = 0usize;
    let mut total_rejected = 0usize;
    for chunk in rows.chunks(4096) {
        let outcome = exec.execute_each_direct_batch(
            chunk,
            &lookup,
            &field_order,
            NANOS,
            &mut builder,
            &mut appended,
        );
        total_appended += outcome.appended;
        total_failed += outcome.failed;
        total_rejected += outcome.rejected;
    }
    let row_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // 对照：同一 executor 走 columnar join（若可）——但 fmt 拒绝，仅作参照
    eprintln!(
        "[q13b-prod-bench] N = {N}, appended = {total_appended}, failed = {total_failed}, rejected = {total_rejected}\n  row path (Event clone + join + fmt): {:.1} ns/row ({:.2}M/s)",
        row_ns,
        1e9 / row_ns / 1e6
    );

    // ---- 对照：真实 provider 路径（RwLock + 每行 Event 构建，2026-08-25
    // q13 1.52M EPS 定位用）---- 生产 join_lookup 的 provider 分支每行
    // `pw.read()` 锁 + 行→JoinRow::Event 构建（HashMap 分配）；bench 的
    // IndexedLookup 是 Arc clone 零拷贝，低估生产成本。本段量化差距。
    use std::sync::RwLock as StdRwLock;
    struct LockedProviderLookup(StdRwLock<StdMap<JoinKey, Vec<wf_engine::match_engine::JoinRow>>>);
    impl WindowLookup for LockedProviderLookup {
        fn snapshot_field_values(
            &self,
            _w: &str,
            _f: &str,
        ) -> Option<std::collections::HashSet<String>> {
            None
        }
        fn snapshot(&self, _w: &str) -> Option<Vec<wf_engine::match_engine::JoinRow>> {
            None
        }
        fn join_lookup(
            &self,
            _w: &str,
            _kf: &str,
            key: &wf_engine::match_engine::Value,
        ) -> Option<Vec<wf_engine::match_engine::JoinRow>> {
            // 复刻 window_lookup.rs 的 provider 分支：锁 + 索引行→Event 构建。
            let locked = self.0.read().expect("provider lock");
            let rows = locked.get(&JoinKey::from_value(key)?)?;
            Some(
                rows.iter()
                    .map(|row| {
                        let fields: wf_engine::match_engine::EngineHashMap<
                            smol_str::SmolStr,
                            wf_engine::match_engine::Value,
                        > = row
                            .field_names()
                            .into_iter()
                            .map(|n| {
                                let n = n.to_string();
                                (
                                    n.clone().into(),
                                    row.field_value(&n).expect("field").clone(),
                                )
                            })
                            .collect();
                        wf_engine::match_engine::JoinRow::Event(std::sync::Arc::new(
                            wf_engine::match_engine::Event { fields },
                        ))
                    })
                    .collect(),
            )
        }
    }
    let locked_lookup = LockedProviderLookup(StdRwLock::new(index.clone()));
    let mut builder = wf_engine::alert::AlertColumnBuilder::new(Arc::from("nexmark_alerts"));
    let mut appended = Vec::new();
    let start = Instant::now();
    let mut total_appended = 0usize;
    let mut total_failed = 0usize;
    let mut total_rejected = 0usize;
    for chunk in rows.chunks(4096) {
        let outcome = exec.execute_each_direct_batch(
            chunk,
            &locked_lookup,
            &field_order,
            NANOS,
            &mut builder,
            &mut appended,
        );
        total_appended += outcome.appended;
        total_failed += outcome.failed;
        total_rejected += outcome.rejected;
    }
    let locked_ns = start.elapsed().as_nanos() as f64 / N as f64;
    // 计数纳入断言：对照路径必须真的处理完所有行（否则 ns/row 是假的）。
    assert_eq!(
        total_appended + total_failed + total_rejected,
        N,
        "provider 对照路径必须覆盖全部行（appended={total_appended} failed={total_failed} rejected={total_rejected}）"
    );
    eprintln!(
        "[q13b-prod-bench] 对照 provider 路径（RwLock + 每行 Event 构建）: {:.1} ns/row ({:.2}M/s) = {:.2}x of row path",
        locked_ns,
        1e9 / locked_ns / 1e6,
        locked_ns / row_ns
    );
    eprintln!(
        "[q13b-prod-bench] 生产 14µs/行 vs row path {row_ns:.1}ns → 剩余差距在 rule_task 层/并发"
    );
    let _ = empty_tracked_bind_fields();
}

/// q13b 并发对照（2026-08-25 q13 1.52M EPS 定位）：10 个 push worker 共享同一
/// provider 锁。单线程带锁只慢 16%（见 q13b_production_path_bench），若生产
/// 每 worker 6.6µs/行（10 worker 分摊 1.52M），大头顶在**并发锁竞争**——本段
/// 量化：T 线程共享一把 RwLock，各自处理 1/T 数据，总吞吐 vs 单线程×T。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-runtime q13b_concurrent_bench -- --ignored --nocapture"]
fn q13b_concurrent_lock_bench() {
    use std::collections::HashMap as StdMap;
    use std::sync::RwLock as StdRwLock;
    use wf_engine::match_engine::{JoinKey, JoinRow, Value as EValue};

    const THREADS: usize = 10;
    const PER_THREAD: usize = 100_000;
    // 每线程独立 executor（生产分片 worker 各持 executor），共享同一把锁。
    let mut execs = Vec::new();
    for _ in 0..THREADS {
        let mut plan = RulePlan {
            conv_window: None,
            name: "q13b_conc".into(),
            binds: vec![BindPlan {
                alias: "m".into(),
                window: "bid_mod".into(),
                filter: None,
            }],
            lets: Vec::new(),
            match_plan: MatchPlan {
                keys: vec![],
                key_exprs: Vec::new(),
                key_map: None,
                key_join: None,
                window_spec: wf_lang::plan::WindowSpec::Fixed(std::time::Duration::ZERO),
                event_steps: vec![],
                close_steps: vec![],
                close_mode: wf_lang::ast::CloseMode::Or,
                tracked_bind_aliases: std::collections::HashSet::new(),
                tracked_bind_fields: std::collections::HashMap::new(),
                tracked_plain_fields: std::collections::HashSet::new(),
                seq: None,
                match_mode: wf_lang::ast::MatchMode::Seq,
                accu: false,
                needs_field_history: false,
                trigger_event_needed: false,
            },
            each_plan: Some(EachPlan {
                alias: "m".into(),
                filter: None,
            }),
            stats_plan: None,
            joins: vec![wf_lang::plan::JoinPlan {
                right_window: "side_input".into(),
                mode: wf_lang::ast::JoinMode::Snapshot,
                conds: vec![wf_lang::plan::JoinCondPlan {
                    left: FieldRef::Qualified("m".into(), "mod_key".into()),
                    right: FieldRef::Qualified("side_input".into(), "key".into()),
                }],
                within: None,
                reduce: None,
                emit_at: None,
            }],
            r#where: None,
            entity_plan: EntityPlan {
                entity_type: "digit".into(),
                entity_id_expr: Expr::Field(FieldRef::Qualified("m".into(), "bidder".into())),
            },
            yield_plan: YieldPlan {
                target: "nexmark_alerts".into(),
                version: None,
                fields: vec![
                    YieldField {
                        name: "id".into(),
                        value: Expr::Field(FieldRef::Qualified("m".into(), "bidder".into())),
                    },
                    YieldField {
                        name: "detail".into(),
                        value: Expr::FuncCall {
                            qualifier: None,
                            name: "fmt".into(),
                            args: vec![
                                Expr::StringLit("{}".into()),
                                Expr::Field(FieldRef::Qualified(
                                    "side_input".into(),
                                    "value".into(),
                                )),
                            ],
                        },
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
        plan.binds[0].alias = "m".into();
        plan.binds[0].window = "bid_mod".into();
        execs.push(RuleExecutor::new(plan));
    }

    // 共享 provider 锁（模拟生产 registry 里同一个 side_input ProviderWindow）。
    let shared: StdMap<JoinKey, Vec<JoinRow>> = {
        let mut index: StdMap<JoinKey, Vec<JoinRow>> = StdMap::new();
        for k in 0..10000i64 {
            let mut fields = wf_engine::match_engine::EngineHashMap::default();
            fields.insert("key".into(), EValue::Number(k as f64));
            fields.insert("value".into(), EValue::Str(format!("value-{k}").into()));
            let row = JoinRow::Event(Arc::new(wf_engine::match_engine::Event { fields }));
            index
                .entry(JoinKey::from_value(&EValue::Number(k as f64)).unwrap())
                .or_default()
                .push(row);
        }
        index
    };
    struct SharedLock(StdRwLock<StdMap<JoinKey, Vec<JoinRow>>>);
    impl WindowLookup for SharedLock {
        fn snapshot_field_values(
            &self,
            _w: &str,
            _f: &str,
        ) -> Option<std::collections::HashSet<String>> {
            None
        }
        fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
            None
        }
        fn join_lookup(&self, _w: &str, _kf: &str, key: &EValue) -> Option<Vec<JoinRow>> {
            let locked = self.0.read().expect("provider lock");
            Some(locked.get(&JoinKey::from_value(key)?)?.clone())
        }
    }
    let lookup = Arc::new(SharedLock(StdRwLock::new(shared)));

    // 每线程一个 bid_mod 批（mod_key 均匀 0..9999）→ Event 物化。
    let batches: Vec<Vec<Arc<wf_engine::match_engine::Event>>> = (0..THREADS)
        .map(|t| {
            let n = PER_THREAD;
            let mut events = Vec::with_capacity(n);
            for i in 0..n {
                let mut fields = wf_engine::match_engine::EngineHashMap::default();
                fields.insert("id".into(), EValue::Number((t * n + i) as f64));
                fields.insert("bidder".into(), EValue::Number((t * n + i) as f64));
                fields.insert("auction".into(), EValue::Number((t * n + i) as f64));
                fields.insert("price".into(), EValue::Number((t * n + i) as f64));
                fields.insert("dateTime".into(), EValue::Number(NANOS as f64));
                fields.insert("mod_key".into(), EValue::Number((i % 10000) as f64));
                events.push(Arc::new(wf_engine::match_engine::Event { fields }));
            }
            events
        })
        .collect();

    let start = Instant::now();
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let exec = execs[t].clone();
        let events = batches[t].clone();
        let lookup = Arc::clone(&lookup);
        handles.push(std::thread::spawn(move || {
            let mut field_order: Vec<&smol_str::SmolStr> = events[0].fields.keys().collect();
            field_order.sort_unstable();
            let rows: Vec<(&wf_engine::match_engine::Event, i64)> =
                events.iter().map(|e| (e.as_ref(), NANOS)).collect();
            let mut builder =
                wf_engine::alert::AlertColumnBuilder::new(Arc::from("nexmark_alerts"));
            let mut appended = Vec::new();
            for chunk in rows.chunks(4096) {
                let _ = exec.execute_each_direct_batch(
                    chunk,
                    lookup.as_ref(),
                    &field_order,
                    NANOS,
                    &mut builder,
                    &mut appended,
                );
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed().as_nanos() as f64;
    let per_row = elapsed / (THREADS * PER_THREAD) as f64;
    eprintln!(
        "[q13b-conc-bench] {THREADS} 线程共享 RwLock：{per_row:.1} ns/行 → 总 {:.2}M/s（单线程无锁 1.29µs/行 ≈ {:.1}M/s ×{THREADS} 理论）",
        1e9 / per_row / 1e6,
        1e9 / 1286.7 / 1e6
    );
    let _ = empty_tracked_bind_fields();
}
