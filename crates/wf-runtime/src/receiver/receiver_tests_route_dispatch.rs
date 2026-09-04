//! decode-route-merge：内联 route_and_dispatch 派发与保序回归（2026-09-04 自 receiver/tests.rs 拆出，
//! `#[path]` 兄弟子模块，`use super::*` 继承共享 harness）：
//! - batch_machine_id / machine_batch（独占 helper 随迁）；
//! - route_and_dispatch 内联提交、receiver metrics、per-(source, window) 连续/独立 seq、
//!   dead mailbox 不 hang、unknown stream noop、IngestLimiter 限速不丢数据；
//! - blocking IPC 重放端到端走 actor mailbox（字节预算不死锁）+ 双流交错 append 死锁回归。
use super::*;

fn machine_batch(cols: Vec<(&str, Vec<&str>)>) -> RecordBatch {
    use arrow::array::ArrayRef;

    let fields: Vec<_> = cols
        .iter()
        .map(|(n, _)| Field::new(*n, DataType::Utf8, true))
        .collect();
    let arrays: Vec<ArrayRef> = cols
        .iter()
        .map(|(_, v)| {
            Arc::new(StringArray::from(
                v.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

#[test]
fn test_batch_machine_id() {
    let b = machine_batch(vec![("msg", vec!["hello"])]);
    assert_eq!(batch_machine_id(&b), None);

    let b = machine_batch(vec![(
        wf_engine::match_engine::MACHINE_ID,
        vec!["10.0.0.1"],
    )]);
    assert_eq!(batch_machine_id(&b), Some("10.0.0.1".to_string()));

    let b = machine_batch(vec![(
        wf_engine::match_engine::MACHINE_ID,
        vec!["10.0.0.1", "10.0.0.2"],
    )]);
    assert_eq!(batch_machine_id(&b), Some("10.0.0.1".to_string()));
}

// ---------------------------------------------------------------------------
// decode-route-merge: inline route_and_dispatch (source-side, no parse pool)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn route_and_dispatch_commits_inline() {
    let (router, parse_seq) = make_parse_router("events");
    let batch = make_batch(&test_schema(), &[1_000_000_000, 2_000_000_000], &[1, 2]);
    // Sync mode (no mailbox registered): dispatch_parsed falls back to
    // inline commit_window — the single source loop is already ordered.
    route_and_dispatch(&parse_seq, "src", "events", batch, &router, None, None).await;
    wait_for_rows(&router, 2).await;
}

#[tokio::test]
async fn route_and_dispatch_records_receiver_metrics() {
    let (router, parse_seq) = make_parse_router("events");
    let metrics = Arc::new(RuntimeMetrics::new(
        &[],
        &["test_win".to_string()],
        &["src".to_string()],
        BTreeMap::new(),
    ));
    let batch = make_batch(&test_schema(), &[1_000_000_000], &[1]);
    route_and_dispatch(
        &parse_seq,
        "src",
        "events",
        batch,
        &router,
        Some(&metrics),
        None,
    )
    .await;

    let records = metrics.snapshot().to_records();
    let rows = records.iter().find(|r| {
        r.fields
            .iter()
            .any(|(k, v)| k == "name" && v == "rows_total")
            && r.fields.iter().any(|(k, v)| k == "label" && v == "src")
    });
    let Some(rows) = rows else {
        panic!("expected receiver rows_total metric for source 'src'");
    };
    let value: u64 = rows
        .fields
        .iter()
        .find(|(k, _)| k == "value")
        .expect("value field")
        .1
        .parse()
        .expect("numeric value");
    assert_eq!(value, 1);
    wait_for_rows(&router, 1).await;
}

/// decode-route-merge 保序契约：内联 `route_and_dispatch` 必须为每
/// (source, window) 分配无缝递增的 seq——这是 actor 重排游标的输入。
/// 直接订阅 mailbox，断言 `WindowMsg::Append` 的 seq 流。
#[tokio::test]
async fn route_and_dispatch_allocates_contiguous_window_seqs() {
    use wf_engine::window::{WINDOW_CHANNEL_DEPTH, WindowMailbox, WindowMsg};

    let router = make_multi_stream_router();
    let (tx, mut rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
    router.register_mailbox(
        "win_a",
        WindowMailbox {
            tx,
            budget: Arc::new(tokio::sync::Semaphore::new(4 * 1024 * 1024)),
            budget_bytes: 4 * 1024 * 1024,
        },
    );
    let parse_seq = Arc::new(AtomicU64::new(0));
    let schema = test_schema();
    for round in 0..3u64 {
        let batch = make_batch(
            &schema,
            &[(1_000_000_000 + round * 1_000_000) as i64; 2],
            &[round as i64, round as i64],
        );
        route_and_dispatch(&parse_seq, "src", "a", batch, &router, None, None).await;
    }
    for expected in 0..3u64 {
        match rx.recv().await.expect("mailbox message") {
            WindowMsg::Append { source, seq, .. } => {
                assert_eq!(source.as_ref(), "src");
                assert_eq!(seq, expected, "per-(source, window) seq must be gap-free");
            }
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected non-Append WindowMsg"),
        }
    }
}

/// 每 (source, window) 游标相互独立：不同 source 派发同一窗口时各自从 0 起
/// （多 handle 共享一个 seq 计数器的源配置互不串号）。
#[tokio::test]
async fn route_and_dispatch_per_source_window_seqs_are_independent() {
    use wf_engine::window::{WINDOW_CHANNEL_DEPTH, WindowMailbox, WindowMsg};

    let router = make_multi_stream_router();
    let (tx, mut rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
    router.register_mailbox(
        "win_a",
        WindowMailbox {
            tx,
            budget: Arc::new(tokio::sync::Semaphore::new(4 * 1024 * 1024)),
            budget_bytes: 4 * 1024 * 1024,
        },
    );
    let batch = make_batch(&test_schema(), &[1_000_000_000, 2_000_000_000], &[1, 2]);
    route_and_dispatch(
        &AtomicU64::new(0),
        "src_a",
        "a",
        batch.clone(),
        &router,
        None,
        None,
    )
    .await;
    route_and_dispatch(&AtomicU64::new(0), "src_b", "a", batch, &router, None, None).await;
    for expected_source in ["src_a", "src_b"] {
        match rx.recv().await.expect("mailbox message") {
            WindowMsg::Append { source, seq, .. } => {
                assert_eq!(source.as_ref(), expected_source);
                assert_eq!(seq, 0, "each source's (source, window) cursor starts at 0");
            }
            #[allow(unreachable_patterns)]
            _ => panic!("unexpected non-Append WindowMsg"),
        }
    }
}

/// actor mailbox 已死（接收端 drop）：dispatch 记 warn 并继续，不 hang、不
/// panic——"一个死 actor 不得阻塞源任务"在源侧内联后必须仍然成立。
#[tokio::test]
async fn route_and_dispatch_dead_mailbox_does_not_hang() {
    use wf_engine::window::{WINDOW_CHANNEL_DEPTH, WindowMailbox, WindowMsg};

    let (router, parse_seq) = make_parse_router("events");
    let (tx, rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
    drop(rx);
    router.register_mailbox(
        "test_win",
        WindowMailbox {
            tx,
            budget: Arc::new(tokio::sync::Semaphore::new(4 * 1024 * 1024)),
            budget_bytes: 4 * 1024 * 1024,
        },
    );
    let batch = make_batch(&test_schema(), &[1_000_000_000], &[1]);
    // 两次派发都必须返回（内部 warn + 丢该窗副本、释放预算 permits）。
    route_and_dispatch(
        &parse_seq,
        "src",
        "events",
        batch.clone(),
        &router,
        None,
        None,
    )
    .await;
    route_and_dispatch(&parse_seq, "src", "events", batch, &router, None, None).await;
}

/// 未知流（无订阅窗口）：route/dispatch 空转，不 panic、不落任何窗口；
/// 真实流不受影响。
#[tokio::test]
async fn route_and_dispatch_unknown_stream_is_noop() {
    let (router, parse_seq) = make_parse_router("events");
    let batch = make_batch(&test_schema(), &[1_000_000_000], &[1]);
    route_and_dispatch(
        &parse_seq,
        "src",
        "no_such_stream",
        batch,
        &router,
        None,
        None,
    )
    .await;
    let batch = make_batch(&test_schema(), &[2_000_000_000], &[7]);
    route_and_dispatch(&parse_seq, "src", "events", batch, &router, None, None).await;
    wait_for_rows(&router, 1).await;
}

/// IngestLimiter 令牌桶限速：rate=2/s、每次 1 行 → 前两次耗尽初始令牌，
/// 第三次必须等待补票（~0.5s）；限速不丢数据，三批最终全部落窗。
#[tokio::test]
async fn route_and_dispatch_ingest_limiter_throttles() {
    let (router, parse_seq) = make_parse_router("events");
    let limiter = IngestLimiter::new(2);
    let batch = make_batch(&test_schema(), &[1_000_000_000], &[1]);
    route_and_dispatch(
        &parse_seq,
        "src",
        "events",
        batch.clone(),
        &router,
        None,
        Some(&limiter),
    )
    .await;
    route_and_dispatch(
        &parse_seq,
        "src",
        "events",
        batch.clone(),
        &router,
        None,
        Some(&limiter),
    )
    .await;
    let started = std::time::Instant::now();
    route_and_dispatch(
        &parse_seq,
        "src",
        "events",
        batch,
        &router,
        None,
        Some(&limiter),
    )
    .await;
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(400),
        "third dispatch must wait for token refill (~0.5s), got {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "throttle must not over-sleep, got {elapsed:?}"
    );
    wait_for_rows(&router, 3).await;
}

/// blocking IPC replay 端到端走 actor mailbox：spawn_blocking 线程上的
/// `Handle::block_on` 驱动完整 dispatch（含 mailbox 字节预算 acquire），
/// 预算由 runtime 线程上的 actor 消费释放——阻塞路径不能死锁，批次最终落窗。
/// 这是 §4.3 `route_and_dispatch_blocking` 方案的直接回归测试。
#[tokio::test]
async fn file_arrow_ipc_replay_routes_rows_through_actor_mailbox() {
    use wf_engine::window::{
        EvictionGate, WINDOW_CHANNEL_DEPTH, WindowMailbox, WindowMsg, run_window_actor,
    };

    let (router, parse_seq) = make_parse_router("events");
    let gate = Arc::new(EvictionGate::new(usize::MAX));
    let win = router.registry().get_window("test_win").unwrap();
    let notify = router.registry().get_notifier("test_win").unwrap();
    let (tx, rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
    router.register_mailbox(
        "test_win",
        WindowMailbox {
            tx,
            budget: Arc::new(tokio::sync::Semaphore::new(4 * 1024 * 1024)),
            budget_bytes: 4 * 1024 * 1024,
        },
    );
    let name: Arc<str> = Arc::from("test_win");
    let fanout = Arc::clone(router.fanout());
    let actor_cancel = CancellationToken::new().child_token();
    tokio::spawn(async move {
        run_window_actor(name, win, gate, fanout, notify, rx, actor_cancel, None).await;
    });

    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("events.arrow_ipc");
    let schema = test_schema();
    let batch_a = make_batch(&schema, &[1_000_000_000], &[1]);
    let batch_b = make_batch(&schema, &[2_000_000_000], &[2]);
    {
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch_a).unwrap();
        writer.write(&batch_b).unwrap();
        writer.finish().unwrap();
    }

    replay_arrow_ipc_file(
        &file_path,
        "events",
        "test_source",
        &[wf_lang::WindowSchema {
            name: "test_win".to_string(),
            streams: vec!["events".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                wf_lang::FieldDef {
                    name: "ts".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
                },
                wf_lang::FieldDef {
                    name: "value".to_string(),
                    field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Digit),
                },
            ],
        }],
        Arc::clone(&router),
        None,
        Arc::clone(&parse_seq),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    wait_for_rows(&router, 2).await;
}

/// Actor-mode regression for the full-speed pipeline deadlock: a *global*
/// per-source frame seq leaves permanent holes in each window's mailbox
/// sequence (a window only receives its own stream's frames), so the window
/// actor's reorder cursor parked every frame after the first hole and the
/// parked messages' byte-budget permits were never released. With two
/// interleaved streams the second stream's window never appended a single
/// row (and under sustained load the whole pipeline froze). The fix
/// allocates per-(source, window) contiguous seqs at the serialized
/// source-side frame builder — now the inlined `route_and_dispatch` call in
/// the source loop itself (decode-route-merge).
///
/// Before the fix this test timed out waiting for win_b's rows.
#[tokio::test]
async fn actor_mode_interleaved_streams_append_without_deadlock() {
    use tokio_util::sync::CancellationToken;
    use wf_engine::window::{
        EvictionGate, WINDOW_CHANNEL_DEPTH, WindowMailbox, WindowMsg, run_window_actor,
    };

    let router = make_multi_stream_router();
    let gate = Arc::new(EvictionGate::new(usize::MAX));
    for name in ["win_a", "win_b"] {
        let win = router.registry().get_window(name).unwrap();
        let notify = router.registry().get_notifier(name).unwrap();
        let (tx, rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
        router.register_mailbox(
            name,
            WindowMailbox {
                tx,
                budget: Arc::new(tokio::sync::Semaphore::new(4 * 1024 * 1024)),
                budget_bytes: 4 * 1024 * 1024,
            },
        );
        let name: Arc<str> = Arc::from(name);
        let fanout = Arc::clone(router.fanout());
        let gate = Arc::clone(&gate);
        let cancel = CancellationToken::new();
        let cancel = cancel.child_token();
        // Leak the actor task handle: the test runtime reaps it at teardown.
        tokio::spawn(async move {
            run_window_actor(name, win, gate, fanout, notify, rx, cancel, None).await;
        });
    }

    // Interleave the two streams like the nexmark generator (several frames
    // of one stream, then the other, repeatedly) — enough frames to blow
    // past any single-window seq hole.
    let parse_seq = Arc::new(AtomicU64::new(0));
    let schema = test_schema();
    for round in 0..8u64 {
        for stream in ["a", "b"] {
            let batch = make_batch(
                &schema,
                &[(1_000_000_000 + round * 1_000_000) as i64; 2],
                &[round as i64, round as i64],
            );
            route_and_dispatch(&parse_seq, "src", stream, batch, &router, None, None).await;
        }
    }
    // Both windows must receive every one of their 8 frames. Before the fix
    // win_b's actor parked its first frame (global seq 1 ≠ expected 0) and
    // this timed out with 0 rows.
    wait_for_rows_for(&router, "win_a", 16).await;
    wait_for_rows_for(&router, "win_b", 16).await;
}
