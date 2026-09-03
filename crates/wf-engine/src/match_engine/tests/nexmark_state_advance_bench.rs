//! nexmark_hotpath_bench 拆出的兄弟子模块（2026-09-04）：match 状态机窗口推进
//! 与命中 emit 归因基准——Q4/Q6（join-then-key：fixed 10m avg close / sliding 10m
//! on-event avg，含批级预解析对拍）、Q5/Q7（fixed 10s + conv sort(-n|-m)|top(1) 归并）、
//! Q11 session / Q12 fixed count、Q13（match<bidder:10m> + snapshot join 富化）、Q6
//! 每事件 emit 路径归因（行式 execute_match_with_joins ↔ 列式批对账），以及 Q16/Q17
//! 键分片 close 累积与 Q18 复合键 close。共享 harness/import 在父模块
//! nexmark_hotpath_bench.rs，此处经 `use super::*` 复用；切片内独占构造随迁。

use super::*;

// ---------------------------------------------------------------------------
// Bench 1：Q4/Q6 join-then-key（固定 10m avg close / 滑动 10m avg on-event）
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q4_q6_join_then_key_advance() {
    let events = bid_events(N);
    let lookup = AuctionLookup::new(AUCTION_DOMAIN);

    // Q4：fixed 10m + close avg —— 每事件 advance 全量累积 close steps
    let mut sm = CepStateMachine::new("q4_bench".to_string(), q4_q6_plan(true), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at_with("b", ev, ts, Some(&lookup)));
    }
    let q4_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q4 join-then-key+close", q4_ns, q4_ns);

    // Q6：sliding 10m + on-event avg —— 每事件状态机推进 + rolling avg
    let mut sm6 = CepStateMachine::new("q6_bench".to_string(), q4_q6_plan(false), None);
    let t1 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm6.advance_at_with("b", ev, ts, Some(&lookup)));
    }
    let q6_ns = t1.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q6 join-then-key+sliding", q6_ns, q6_ns);
}

/// 批级 join-then-key（2026-08-23）：同一批列式行，路径 A 逐事件内部解析
/// （`advance_at_with_masks`）vs 路径 B 批级预解析（`precompute_join_then_keys` +
/// `advance_at_with_masks_key`）。前 K 行收集 StepResult 逐位对拍（防语义发散），
/// 全量计时报告加速比。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q4_q6_join_then_key_batch_precompute() {
    use crate::match_engine::event_bridge::ColumnarEvent;
    use crate::match_engine::precompute_join_then_keys;

    const K: usize = 10_000; // 对拍抽样行
    let batch = bid_batch(N);
    let lookup = AuctionLookup::new(AUCTION_DOMAIN);
    let row_domain: Vec<usize> = (0..N).collect();

    for (label, fixed) in [("q4 fixed10m", true), ("q6 sliding10m", false)] {
        let plan = q4_q6_plan(fixed);
        let kjp = plan.key_join.as_ref().unwrap();
        let keys = precompute_join_then_keys(&batch, &row_domain, kjp, &lookup);
        assert_eq!(keys.len(), N, "{label}: 每行一个预解析 key");

        // 正确性对拍：前 K 行 StepResult 序列逐位一致（同 rule_name）。
        let mut sm_a = CepStateMachine::new("q".into(), plan.clone(), None);
        let mut sm_b = CepStateMachine::new("q".into(), plan.clone(), None);
        for (i, key) in keys.iter().enumerate().take(K) {
            let ev = ColumnarEvent::new(&batch, i);
            let ts = NOW + i as i64 * EVENT_STEP_NS;
            let ra = sm_a.advance_at_with_masks("b", &ev, ts, Some(&lookup), i, None);
            let rb =
                sm_b.advance_at_with_masks_key("b", &ev, ts, Some(&lookup), i, None, Some(key));
            assert_eq!(
                ra, rb,
                "{label} row {i}: 批级预解析 vs 内部解析结果必须一致"
            );
        }
        assert_eq!(
            sm_a.instance_count(),
            sm_b.instance_count(),
            "{label}: 实例数一致"
        );

        // 计时：路径 A（内部解析，基线）。
        let mut sm = CepStateMachine::new("q".into(), plan.clone(), None);
        let t0 = Instant::now();
        for i in 0..N {
            let ev = ColumnarEvent::new(&batch, i);
            let ts = NOW + i as i64 * EVENT_STEP_NS;
            let _ = std::hint::black_box(sm.advance_at_with_masks(
                "b",
                &ev,
                ts,
                Some(&lookup),
                i,
                None,
            ));
        }
        let row_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;

        // 计时：路径 B（批级预解析）。
        let mut sm2 = CepStateMachine::new("q".into(), plan, None);
        let t1 = Instant::now();
        for (i, key) in keys.iter().enumerate().take(N) {
            let ev = ColumnarEvent::new(&batch, i);
            let ts = NOW + i as i64 * EVENT_STEP_NS;
            let _ = std::hint::black_box(sm2.advance_at_with_masks_key(
                "b",
                &ev,
                ts,
                Some(&lookup),
                i,
                None,
                Some(key),
            ));
        }
        let batch_ns = t1.elapsed().as_secs_f64() * 1e9 / N as f64;

        report(&format!("{label} 批级预解析"), batch_ns, row_ns);
        report(&format!("{label} 行式(内部解析)"), row_ns, row_ns);
    }
}

// ---------------------------------------------------------------------------
// Bench 2：Q5/Q7 fixed 10s 窗口 advance + conv sort/top(1) 归并
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q5_q7_window_conv_top() {
    let events = bid_events(N);

    // Q5：fixed 10s count + close count
    let mut sm = CepStateMachine::new("q5_bench".to_string(), q5_q7_plan(false), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at("b", ev, ts));
    }
    let q5_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q5 fixed10s count", q5_ns, q5_ns);

    // Q7：fixed 10s max + close max
    let mut sm7 = CepStateMachine::new("q7_bench".to_string(), q5_q7_plan(true), None);
    let t1 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm7.advance_at("b", ev, ts));
    }
    let q7_ns = t1.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q7 fixed10s max", q7_ns, q7_ns);

    // conv 归并：一批收口 CloseOutput（~1000 行，auction 键域）→ sort(-n)|top(1)
    let plan = conv_top1("n");
    let keys = vec![FieldRef::Simple("auction".into())];
    let mut outputs: Vec<CloseOutput> = Vec::with_capacity(2000);
    let mut rng: u64 = 0x1234_5678_9ABC_DEF0;
    for _ in 0..2000 {
        let auction = (AUCTION_BASE + (next_u64(&mut rng) % AUCTION_DOMAIN) as i64) as f64;
        let count = (next_u64(&mut rng) % 500) as f64;
        outputs.push(close_output(
            "q5_bench",
            vec![Value::Number(auction)],
            "n",
            count,
        ));
    }
    let t2 = Instant::now();
    for _ in 0..100 {
        let out = std::hint::black_box(apply_conv(&plan, &keys, outputs.clone()));
        std::hint::black_box(out.len());
    }
    let conv_ns = t2.elapsed().as_secs_f64() * 1e9 / (100.0 * outputs.len() as f64);
    report("q5/q7 conv sort+top1", conv_ns, conv_ns);
}

// ---------------------------------------------------------------------------
// Bench 3：Q11 session(10s) 状态推进（RSS 17.3GB 查询之一）
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q11_session_advance() {
    let events = bid_events(N);
    let mut sm = CepStateMachine::new("q11_bench".to_string(), q11_q12_plan(true), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at("b", ev, ts));
    }
    let q11_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q11 session(10s)", q11_ns, q11_ns);
}

// ---------------------------------------------------------------------------
// Bench 4：Q12 fixed(10s) count 窗口
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q12_fixed_window_count() {
    let events = bid_events(N);
    let mut sm = CepStateMachine::new("q12_bench".to_string(), q11_q12_plan(false), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at("b", ev, ts));
    }
    let q12_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q12 fixed10s count", q12_ns, q12_ns);
}

// ---------------------------------------------------------------------------
// Bench 5：Q13 match<bidder:10m> + snapshot join 富化
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q13_match_snapshot_join() {
    let events = bid_events(N);
    let lookup = PersonLookup::new(BIDDER_DOMAIN);
    let exec = RuleExecutor::new(q13_rule());

    // advance：状态机推进（每事件命中 → 构造 MatchedContext 的成本在 exec）
    let mut sm = CepStateMachine::new("q13_bench".to_string(), q13_rule().match_plan.clone(), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at("b", ev, ts));
    }
    let adv_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q13 advance", adv_ns, adv_ns);

    // execute_match_with_joins：join 富化 + alert 构建（每事件命中即输出）
    // MatchedContext 一次构造复用（生产由 state machine 构造，构造成本计入
    // advance bench；这里只测富化 + 输出路径）。
    let matched = simple_matched("q13_bench", vec![num(1005.0)], &events[0], NOW);
    let t1 = Instant::now();
    for _ in events.iter().take(N / 10) {
        let _ = std::hint::black_box(exec.execute_match_with_joins(&matched, &lookup));
    }
    let exec_ns = t1.elapsed().as_secs_f64() * 1e9 / (N as f64 / 10.0);
    report("q13 match+join emit", exec_ns, adv_ns);

    // 分量 1：build_eval_context（不含 join）
    let rule = q13_rule();
    let step_plans: Vec<&StepPlan> = rule.match_plan.event_steps.iter().collect();
    let needed = crate::match_engine::executor::CloseCtxFields::All;
    let t2 = Instant::now();
    for _ in 0..(N / 10) {
        let ctx = crate::match_engine::executor::build_eval_context(
            &rule.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            &matched.bind_data,
            &step_plans,
            matched.trigger_event.as_ref(),
            &needed,
            None,
        );
        std::hint::black_box(ctx);
    }
    let ctx_ns = t2.elapsed().as_secs_f64() * 1e9 / (N as f64 / 10.0);
    report("q13 build ctx", ctx_ns, exec_ns);

    // 分量 2：build_eval_context + execute_joins（富化，不含 alert 构建）
    let t3 = Instant::now();
    for _ in 0..(N / 10) {
        let mut ctx = crate::match_engine::executor::build_eval_context(
            &rule.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            &matched.bind_data,
            &step_plans,
            matched.trigger_event.as_ref(),
            &needed,
            None,
        );
        let ok = crate::match_engine::executor::execute_joins(&rule.joins, &mut ctx, &lookup, NOW);
        std::hint::black_box((ctx, ok));
    }
    let ctxjoin_ns = t3.elapsed().as_secs_f64() * 1e9 / (N as f64 / 10.0);
    report("q13 ctx+join (富化)", ctxjoin_ns, exec_ns);
}

// ---------------------------------------------------------------------------
// Bench 5b：Q6 每事件 emit 路径归因（match + join-then-key，live_joins 空，
//          score 常量 + entity/yield 读左窗字段）——q6 26M EMIT 的瓶颈侧。
// ---------------------------------------------------------------------------

/// Q6 形状 RulePlan：`match<seller:10m> avg>=200` + auction snapshot join
/// （键来自 join 右窗 → join 存活但输出全左窗限定 → live_joins 空）。
fn q6_rule() -> RulePlan {
    let mut plan = simple_rule_plan(
        "q6_bench",
        q4_q6_plan(false),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.joins = vec![JoinPlan {
        right_window: "auction_events".to_string(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "auction".into()),
            right: FieldRef::Qualified("auction_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q6_avg200".into()),
        },
        YieldField {
            name: "detail".into(),
            value: Expr::StringLit("avg bid >= 200".into()),
        },
        YieldField {
            name: "request_count".into(),
            value: Expr::Number(1.0),
        },
    ];
    plan
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q6_match_emit() {
    use crate::match_engine::executor::CloseCtxFields;
    use crate::match_engine::executor::build_eval_context;

    let events = bid_events(N);
    let lookup = AuctionLookup::new(AUCTION_DOMAIN);
    let rule = q6_rule();
    let exec = RuleExecutor::new(rule.clone());
    assert!(
        exec.live_joins().is_empty(),
        "q6 输出全左窗限定（yield 读 b.auction）→ join 必须判死，否则富化是纯浪费"
    );

    // 每事件 emit：execute_match_with_joins（live_joins 空 → execute_joins 空转）。
    let matched = simple_matched("q6_bench", vec![num(20.0)], &events[0], NOW);
    let t1 = Instant::now();
    for _ in 0..N {
        let _ = std::hint::black_box(exec.execute_match_with_joins(&matched, &lookup));
    }
    let exec_ns = t1.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q6 match emit(全路径)", exec_ns, exec_ns);

    // 分量 1：build_eval_context（Named 窄化——q6 编译产物只读 b.auction/seller）。
    let step_plans: Vec<&StepPlan> = rule.match_plan.event_steps.iter().collect();
    let needed = CloseCtxFields::Named(HashSet::from(["auction".to_string()]));
    let t2 = Instant::now();
    for _ in 0..N {
        let ctx = build_eval_context(
            &rule.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            &matched.bind_data,
            &step_plans,
            matched.trigger_event.as_ref(),
            &needed,
            None,
        );
        std::hint::black_box(ctx);
    }
    let ctx_ns = t2.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q6 build ctx(窄化)", ctx_ns, exec_ns);

    // 分量 2：build_match_alert（ctx 复用，只测 alert 构建）。
    let ctx = build_eval_context(
        &rule.match_plan.keys,
        &matched.scope_key,
        &matched.step_data,
        &matched.bind_data,
        &step_plans,
        matched.trigger_event.as_ref(),
        &needed,
        None,
    );
    let t3 = Instant::now();
    for _ in 0..N {
        let _ = std::hint::black_box(exec.build_match_alert(&matched, &ctx, NOW).unwrap());
    }
    let alert_ns = t3.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q6 build_match_alert", alert_ns, exec_ns);
}
/// q6 列式批 emit（2026-08-26 对账）：生产 q6 过 `match_plan_columnar_safe`
/// gate → `execute_match_direct_batch_columnar`（列式批，零 OutputRecord 物化），
/// 而 `q6_match_emit` 测的是行式 `execute_match_with_joins`（484ns，非生产形态）。
/// 本 bench 用生产分段（ALERT_BATCH_SIZE 级 chunk）测列式批成本——对账
/// diag 实测 1576ns/evt 的构成。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine q6_match_emit_columnar -- --ignored --nocapture"]
fn q6_match_emit_columnar() {
    use crate::alert::AlertColumnBuilder;

    let events = bid_events(N);
    let rule = q6_rule();
    let exec = RuleExecutor::new(rule.clone());
    assert!(
        exec.match_plan_columnar_safe(),
        "q6 形状必须过列式 gate（生产走 execute_match_direct_batch_columnar）"
    );

    // 每事件命中 1 个 MatchedContext（q6 avg>=200 高频，近似生产 advance 命中）。
    let matched: Vec<MatchedContext> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| {
            simple_matched(
                "q6_bench",
                vec![num(20.0)],
                ev,
                NOW + i as i64 * EVENT_STEP_NS,
            )
        })
        .collect();
    let refs: Vec<&MatchedContext> = matched.iter().collect();

    // 批级列式装载（生产分段形态）。
    const SEG: usize = 256;
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_out = Vec::new();
    let t0 = Instant::now();
    for _ in 0..4 {
        for seg in refs.chunks(SEG) {
            let stats =
                exec.execute_match_direct_batch_columnar(seg, NOW, &mut builder, &mut appended_out);
            std::hint::black_box(&stats);
        }
    }
    let col_ns = t0.elapsed().as_secs_f64() * 1e9 / (N as f64 * 4.0);
    report("q6 match emit(列式批)", col_ns, col_ns);
}

// ---------------------------------------------------------------------------
// Bench 7：Q16/Q17 键分片 close 累积（channel 12 measure / auction 8 measure）
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q16_q17_keyed_close() {
    let events = bid_events(N);

    // Q16：channel 键 fixed 30m + 12 close measure（4 count 档 + 8 distinct）
    let mut sm = CepStateMachine::new("q16_bench".to_string(), q16_plan(), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at("b", ev, ts));
    }
    let q16_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q16 channel-keyed close", q16_ns, q16_ns);

    // Q17：auction 键 fixed 30m + 8 close measure（4 count 档 + min/max/avg/sum）
    let mut sm17 = CepStateMachine::new("q17_bench".to_string(), q17_plan(), None);
    let t1 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm17.advance_at("b", ev, ts));
    }
    let q17_ns = t1.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q17 auction-keyed close", q17_ns, q17_ns);
}

// ---------------------------------------------------------------------------
// Bench 8：Q18 (bidder,auction) 复合键 close count
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q18_composite_key_close() {
    let events = bid_events(N);
    let mut sm = CepStateMachine::new("q18_bench".to_string(), q18_plan(), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at("b", ev, ts));
    }
    let q18_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q18 composite-key close", q18_ns, q18_ns);
}
