//! fanout 单测（rule_shards）：订阅注册 / 广播分片与剪枝 / 预分片对拍 / 表达式键
//! 逐行求值分片 / 队列积压与吞吐有界等。自 `mod.rs` 原内联 `mod tests` 原样外移
//! 到同级文件（`#[cfg(test)] mod tests;` 声明，同 `mod partition;` 先例）。
//! coverage 测试另见 `window/tests/`。
//!
//! 测试是 `fanout` 模块的子模块，父面私有项（含 `RuleFanout` 私有字段）仍可达，
//! 故可直接读 `fanout.table` 断言剪枝结果。

    use super::partition::{partition_rows, partition_rows_by_key};
    use crate::match_engine::{extract_key_simple, extract_scope_key_mixed, field_ref_name, scope_key_from_values, scope_key_shard_index};
    use super::*;
    use crate::match_engine::{EngineHashMap, ScopeKey, Value};

    fn event(id: &str) -> Event {
        let mut fields = EngineHashMap::default();
        fields.insert("id".into(), Value::Str(id.into()));
        Event { fields }
    }

    fn keys() -> Vec<FieldRef> {
        vec![FieldRef::Simple("id".into())]
    }

    /// 窗口分片冲突检测（2026-08-29 q11/q6 多规则根因）：window_sharding 是每
    /// 窗口单一 (keys) 配置（覆盖式 insert），多规则不同 key 分片同一窗口互相
    /// 覆盖 → 后注册者必须回退单 worker。同 keys 不算冲突（共享分片）。
    #[test]
    fn window_sharding_conflicts_detects_key_mismatch() {
        let fanout = RuleFanout::new();
        let k_bidder = [FieldRef::Simple("bidder".into())];
        let k_auction = [FieldRef::Simple("auction".into())];

        // 未注册 → 不冲突。
        assert!(
            !fanout.window_sharding_conflicts("bid_events", &k_bidder),
            "未注册窗口不冲突"
        );

        // 注册 bidder 分片。
        fanout.register_window_sharding("bid_events", Arc::from(k_bidder.as_slice()), 10);
        assert!(
            !fanout.window_sharding_conflicts("bid_events", &k_bidder),
            "同 keys（q11/q12 都按 bidder）不冲突，共享分片"
        );
        assert!(
            fanout.window_sharding_conflicts("bid_events", &k_auction),
            "不同 keys（q5/q7 按 auction）冲突 → 后注册者回退单 worker"
        );
        assert!(
            fanout.window_sharding_conflicts("bid_events", &[]),
            "空 keys（stats index 分区）与已有 key 分片同样冲突"
        );

        // 不同窗口互不影响。
        assert!(
            !fanout.window_sharding_conflicts("auction_events", &k_auction),
            "其它窗口独立"
        );
    }

    /// `round_robin_only` 驱动中间窗广播裁剪（2026-08-25 q13 分片内存）：
    /// - 无订阅 / 只有 RoundRobin 订阅 → true（生产者可跳过 events 物化，
    ///   batch-only 广播）
    /// - 存在 Single / Sharded / 混合订阅 → false（row-path 中间窗消费者
    ///   依赖 `RulePush::events`，必须保留）
    #[test]
    fn round_robin_only_classifies_subscriptions() {
        let fanout = RuleFanout::new();
        let (tx, _rx) = mpsc::channel::<RulePush>(8);
        let (tx2, _rx2) = mpsc::channel::<RulePush>(8);
        let (tx3, _rx3) = mpsc::channel::<RulePush>(8);

        // 未注册窗口 → true（广播无订阅者，物化 events 是纯浪费）。
        assert!(fanout.round_robin_only("unregistered"));

        // 只有 Single 订阅 → false（row-path 契约需要 events）。
        fanout.register("win_single", tx.clone());
        assert!(!fanout.round_robin_only("win_single"));

        // 只有 Sharded 订阅 → false。
        fanout.register_sharded("win_sharded", vec![tx2.clone()], Arc::from(keys()));
        assert!(!fanout.round_robin_only("win_sharded"));

        // 只有 RoundRobin 订阅 → true（列式安全，batch-only 广播）。
        fanout.register_round_robin("win_rr", vec![tx3.clone()]);
        assert!(fanout.round_robin_only("win_rr"));

        // 混合：RoundRobin + Single → false（任一 row-path 消费者都需要 events）。
        fanout.register("win_mixed", tx.clone());
        fanout.register_round_robin("win_mixed", vec![tx.clone()]);
        assert!(!fanout.round_robin_only("win_mixed"));
    }

    #[tokio::test]
    async fn broadcast_delivers_same_arc_to_registered_channels() {
        let fanout = RuleFanout::new();
        let (tx, mut rx) = mpsc::channel(8);
        fanout.register("win_a", tx);

        let events: Arc<Vec<Arc<Event>>> = Arc::new(Vec::new());
        fanout.broadcast("win_a", &events, 0).await;

        let push = rx
            .try_recv()
            .expect("registered channel should receive a push");
        assert_eq!(&*push.window_name, "win_a");
        assert!(
            push.events
                .as_ref()
                .is_some_and(|e| Arc::ptr_eq(e, &events)),
            "should share the same Arc"
        );
    }

    #[tokio::test]
    async fn broadcast_prunes_closed_channels() {
        let fanout = RuleFanout::new();
        let (tx, rx) = mpsc::channel(8);
        fanout.register("win_a", tx);
        drop(rx); // close the channel

        let events: Arc<Vec<Arc<Event>>> = Arc::new(Vec::new());
        fanout.broadcast("win_a", &events, 0).await;

        let table = fanout.table.read().expect("fanout lock poisoned");
        assert!(
            !table.contains_key("win_a"),
            "closed channel should be pruned on broadcast"
        );
    }

    #[tokio::test]
    async fn sharded_broadcast_partitions_by_key_and_routes_same_key_together() {
        let fanout = RuleFanout::new();
        let (tx0, mut rx0) = mpsc::channel(8);
        let (tx1, mut rx1) = mpsc::channel(8);
        fanout.register_sharded(
            "win_a",
            vec![tx0, tx1],
            Arc::from(keys().into_boxed_slice()),
        );

        // Two distinct keys; each should land on a single (deterministic) shard.
        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![
            Arc::new(event("k1")),
            Arc::new(event("k2")),
            Arc::new(event("k1")),
        ]);
        fanout.broadcast("win_a", &events, 0).await;

        let mut received = Vec::new();
        while let Ok(push) = rx0.try_recv() {
            received.extend(
                push.events
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|e| e.fields["id"].clone()),
            );
        }
        while let Ok(push) = rx1.try_recv() {
            received.extend(
                push.events
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|e| e.fields["id"].clone()),
            );
        }

        // Union of the shards == the original batch (no loss, no dup).
        let mut ids: Vec<String> = received
            .into_iter()
            .map(|v| match v {
                Value::Str(s) => s.to_string(),
                _ => panic!("expected str"),
            })
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["k1", "k1", "k2"]);

        // Same key (`k1`) must land on the SAME shard across broadcasts.
        let idx = scope_key_shard_index(&ScopeKey::Str("k1".into()), 2);
        let again: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("k1"))]);
        fanout.broadcast("win_a", &again, 1).await;
        let got0 = rx0
            .try_recv()
            .map(|p| p.events.as_ref().map(|e| e.len()).unwrap_or(0))
            .unwrap_or(0);
        let got1 = rx1
            .try_recv()
            .map(|p| p.events.as_ref().map(|e| e.len()).unwrap_or(0))
            .unwrap_or(0);
        if idx == 0 {
            assert_eq!(got0, 1);
            assert_eq!(got1, 0);
        } else {
            assert_eq!(got0, 0);
            assert_eq!(got1, 1);
        }
    }

    #[test]
    fn scope_key_shard_index_is_deterministic_and_in_range() {
        let n = 4;
        for id in ["a", "b", "c", "same", "same"] {
            let idx = scope_key_shard_index(&ScopeKey::Str(id.into()), n);
            assert!(idx < n);
        }
        // Same key → same index, across repeated calls.
        assert_eq!(
            scope_key_shard_index(&ScopeKey::Str("same".into()), n),
            scope_key_shard_index(&ScopeKey::Str("same".into()), n)
        );
    }

    #[test]
    fn scope_key_shard_index_single_shard_is_zero() {
        assert_eq!(
            scope_key_shard_index(&ScopeKey::Str("anything".into()), 1),
            0
        );
    }

    #[tokio::test]
    async fn round_robin_broadcast_delivers_whole_batches_and_shares_arcs() {
        let fanout = RuleFanout::new();
        let (tx0, mut rx0) = mpsc::channel(8);
        let (tx1, mut rx1) = mpsc::channel(8);
        fanout.register_round_robin("win_rr", vec![tx0, tx1]);

        // Four distinct batches; round-robin must send each WHOLE batch (same
        // Arc) to alternating workers with no loss / no duplication.
        let mut sent = Vec::new();
        for i in 0..4 {
            let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![
                Arc::new(event(&format!("e{i}a"))),
                Arc::new(event(&format!("e{i}b"))),
            ]);
            sent.push(Arc::clone(&events));
            fanout.broadcast("win_rr", &events, 0).await;
        }

        let mut got0 = Vec::new();
        while let Ok(push) = rx0.try_recv() {
            got0.push(push);
        }
        let mut got1 = Vec::new();
        while let Ok(push) = rx1.try_recv() {
            got1.push(push);
        }

        // Exactly one worker per batch, alternating.
        assert_eq!(got0.len(), 2, "worker 0 receives 2 batches");
        assert_eq!(got1.len(), 2, "worker 1 receives 2 batches");
        // Whole batch preserved: 2 events per delivered push, same Arc as sent.
        let all: Vec<&RulePush> = got0.iter().chain(got1.iter()).collect();
        assert!(
            all.iter()
                .all(|p| p.events.as_ref().map(|e| e.len()).unwrap_or(0) == 2)
        );
        for push in &all {
            assert!(
                sent.iter()
                    .any(|s| push.events.as_ref().is_some_and(|e| Arc::ptr_eq(s, e))),
                "delivered batch must be one of the sent Arcs (zero copy)"
            );
        }
        assert_eq!(&*all[0].window_name, "win_rr");
    }

    #[tokio::test]
    async fn round_robin_broadcast_prunes_closed_shards() {
        let fanout = RuleFanout::new();
        let (tx0, mut rx0) = mpsc::channel(8);
        let (tx1, rx1) = mpsc::channel(8);
        fanout.register_round_robin("win_rr2", vec![tx0, tx1]);
        drop(rx1); // worker 1 shut down

        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("x"))]);
        // Broadcast enough times to hit the closed shard and trigger pruning.
        for _ in 0..2 {
            fanout.broadcast("win_rr2", &events, 0).await;
        }

        // Surviving shard still receives (at least) one delivery.
        let mut delivered = 0;
        while rx0.try_recv().is_ok() {
            delivered += 1;
        }
        assert!(delivered >= 1, "open shard must still receive batches");

        let table = fanout.table.read().expect("fanout lock poisoned");
        let subs = table.get("win_rr2").expect("subscription survives");
        assert!(
            !subs.is_empty(),
            "subscription with one open shard must not be pruned entirely"
        );
    }

    /// P1-② regression: a full (slow-consumer) channel must not head-of-line
    /// block the deliveries to the *other* subscriptions of the same window.
    /// The sends run concurrently, so the fast subscriber receives its copy
    /// immediately even while the slow one's send is still parked.
    #[tokio::test]
    async fn slow_consumer_does_not_block_other_subscribers() {
        let fanout = RuleFanout::new();
        // Slow consumer: capacity 1, never recv'd → its send parks after the
        // first broadcast fills the channel.
        let (slow_tx, _slow_rx_keep) = mpsc::channel::<RulePush>(1);
        // Fast consumer: capacity 8, drained immediately.
        let (fast_tx, mut fast_rx) = mpsc::channel::<RulePush>(8);
        fanout.register("win_hol", slow_tx);
        fanout.register("win_hol", fast_tx);

        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("e1"))]);
        fanout.broadcast("win_hol", &events, 0).await;

        // Second broadcast: the slow channel is full and would block a serial
        // send loop before the fast subscriber's delivery. Drive the
        // broadcast future concurrently with the fast recv — the broadcast
        // stays parked on the slow channel, the fast delivery must not wait.
        let events2: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("e2"))]);
        let broadcast = fanout.broadcast("win_hol", &events2, 1);
        tokio::pin!(broadcast);
        let got = tokio::time::timeout(std::time::Duration::from_millis(500), async {
            tokio::select! {
                biased;
                r = fast_rx.recv() => r,
                // If the broadcast ever completes while the slow channel is
                // still full and undrained, something is wrong; ignore and
                // let the outer timeout fail the test.
                _ = &mut broadcast => fast_rx.try_recv().ok(),
            }
        })
        .await
        .expect("fast subscriber must receive within timeout")
        .expect("fast channel open");
        assert_eq!(
            got.events.as_ref().map(|e| e.len()).unwrap_or(0),
            1,
            "fast subscriber got the second batch"
        );
    }

    /// Same property for sharded subscriptions: a full shard must not block
    /// the other shards' deliveries of the same broadcast.
    #[tokio::test]
    async fn slow_shard_does_not_block_other_shards() {
        let fanout = RuleFanout::new();
        let (slow_tx, _slow_rx_keep) = mpsc::channel::<RulePush>(1);
        let (fast_tx, mut fast_rx) = mpsc::channel::<RulePush>(8);
        // Two shards partitioned by "id"; keys k1/k2 deterministically split.
        fanout.register_sharded(
            "win_sh",
            vec![slow_tx, fast_tx],
            Arc::from(keys().into_boxed_slice()),
        );

        let idx_k1 = scope_key_shard_index(&ScopeKey::Str("k1".into()), 2);
        let (slow_key, fast_key) = if idx_k1 == 0 {
            ("k1", "k2")
        } else {
            ("k2", "k1")
        };

        let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event(slow_key))]);
        fanout.broadcast("win_sh", &events, 0).await;

        // Second broadcast: the slow shard's channel is full; the fast shard
        // must still receive its sub-batch without waiting for it. Drive the
        // broadcast future concurrently with the fast shard's recv.
        let events2: Arc<Vec<Arc<Event>>> =
            Arc::new(vec![Arc::new(event(slow_key)), Arc::new(event(fast_key))]);
        let broadcast = fanout.broadcast("win_sh", &events2, 1);
        tokio::pin!(broadcast);
        let got = tokio::time::timeout(std::time::Duration::from_millis(500), async {
            tokio::select! {
                biased;
                r = fast_rx.recv() => r,
                _ = &mut broadcast => fast_rx.try_recv().ok(),
            }
        })
        .await
        .expect("fast shard must receive within timeout")
        .expect("fast shard channel open");
        assert_eq!(
            got.events.as_ref().map(|e| e.len()).unwrap_or(0),
            1,
            "fast shard got its sub-batch"
        );
    }

    #[test]
    fn partition_rows_matches_row_based_per_row() {
        // 列式分片（partition_rows_by_key，从 batch 列读 key）必须与行式分片
        // （batch_to_events + extract_key_simple + shard_index）逐行落在同一
        // shard —— Q2 键闭包 + 有状态安全的基础。含 null / UTF8 / 多行 key。
        use crate::match_engine::batch_to_events;
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use wf_lang::ast::FieldRef;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("k1"),
                Some("k2"),
                None, // null key → both should land shard 0
                Some("k3"),
                Some("k1"),
            ])) as ArrayRef],
        )
        .unwrap();

        let keys = vec![FieldRef::Simple("id".into())];
        let shards = 3usize;

        // 列式：每行 → shard
        let per = partition_rows_by_key(&batch, &keys, shards).expect("key col present");
        let col_shard = |row: usize| -> usize {
            per.iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap()
        };

        // 行式：每行物化 Event → extract_key_simple → ScopeKey → scope_key_shard_index
        let events = batch_to_events(&batch);
        let row_shard = |row: usize| -> usize {
            extract_key_simple(&events[row], &keys)
                .map(|sk| scope_key_shard_index(&scope_key_from_values(&sk), shards))
                .unwrap_or(0)
        };

        assert_eq!(batch.num_rows(), 5);
        for row in 0..batch.num_rows() {
            assert_eq!(
                col_shard(row),
                row_shard(row),
                "row {row} landed on different shard (columnar vs row-based)"
            );
        }

        // 无丢失、无重复：并集覆盖全部 5 行
        let flat: Vec<u32> = per.iter().flatten().copied().collect();
        let mut flat = flat;
        flat.sort_unstable();
        assert_eq!(flat, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn precompute_shard_rows_equals_partition_rows_by_key() {
        // 方案 A：`precompute_shard_rows`（并行 parse 阶段，读 fanout 的 sharded
        // keys/shard_count）产出的分片，必须与广播内部所用的
        // `partition_rows_by_key` 逐 shard 完全一致（否则提前分片会改变
        // 命中行落子，破坏有状态语义）。逐 shard 比较行子集（含排序后相等）。
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use wf_lang::ast::FieldRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "auction",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7),
                None,
                Some(1),
            ])) as ArrayRef],
        )
        .unwrap();

        let fanout = RuleFanout::new();
        let shard_count = 3usize;
        let (txs, _rxs): (Vec<_>, Vec<_>) = (0..shard_count)
            .map(|_| mpsc::channel::<RulePush>(8))
            .unzip();
        let keys: Arc<[FieldRef]> =
            Arc::from(vec![FieldRef::Simple("auction".into())].into_boxed_slice());
        fanout.register_sharded("win_p", txs, keys.clone());

        let pre = fanout
            .precompute_shard_rows("win_p", &batch)
            .expect("sharded window");
        let internal = partition_rows_by_key(&batch, &keys, shard_count).expect("key col present");
        assert_eq!(pre.len(), internal.len(), "same shard count");
        for i in 0..shard_count {
            let mut a = pre[i].clone();
            let mut b = internal[i].clone();
            a.sort_unstable();
            b.sort_unstable();
            assert_eq!(a, b, "precompute shard {i} differs from internal partition");
        }
    }

    #[test]
    fn unsharded_precompute_shard_rows_returns_none() {
        // 无 sharded 订阅的窗口：`precompute_shard_rows` 返回 None（不该分片），
        // 广播走原路径。
        use arrow::array::ArrayRef;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let fanout = RuleFanout::new();
        let (tx, _rx) = mpsc::channel::<RulePush>(8);
        fanout.register("win_s", tx);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![Some(1)])) as ArrayRef],
        )
        .unwrap();
        assert!(fanout.precompute_shard_rows("win_s", &batch).is_none());
        assert!(fanout.precompute_shard_rows("missing", &batch).is_none());
    }

    #[test]
    fn scope_key_columnar_matches_row_based() {
        // 2b 对拍：`scope_key_columnar`（从列直读原生值）必须与行式
        // `scope_key_from_values(extract_key_simple)` 逐行构造出 **同一个**
        // `ScopeKey`（相等）——覆盖 Utf8、null（→ 缺失 → shard 0）、Int64
        // <2^53、多列 key。
        // 注：>2^53 的 Int64 行式走 f64 丢精度（`Value::Number(v as f64)`），
        // 与列式精确 i64 是已知语义分歧（既有 extract_field_value 行为），此
        // 测试锁 <2^53 一致 + 断言 >2^53 分歧方向。
        use crate::match_engine::batch_to_events;
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use wf_lang::ast::FieldRef;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("k1"),
                    Some("k2"),
                    None,
                    Some("k3"),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(7),
                    Some(9007199254740993), // 2^53+1
                    Some(-3),
                    None,
                ])),
            ],
        )
        .unwrap();

        let keys = vec![FieldRef::Simple("id".into()), FieldRef::Simple("n".into())];
        let col_idx: Vec<usize> = keys
            .iter()
            .map(field_ref_name)
            .map(|name| batch.schema().index_of(name).unwrap())
            .collect();
        let events = batch_to_events(&batch);

        assert_eq!(batch.num_rows(), 4);
        // 2^53+1 是唯一的分歧 lane（行式 f64 丢精度），其余必须逐行相等。
        for (row, event) in events.iter().enumerate() {
            let col = scope_key_columnar(&batch, &col_idx, row);
            let rw = extract_key_simple(event, &keys).map(|sk| scope_key_from_values(&sk));
            if row == 1 {
                // >2^53：列式 Int(2^53+1) vs 行式 f64 舍入 → 分歧（已知语义）。
                assert!(
                    col != rw,
                    "row {row} 2^53+1 columnar vs row-based should differ (f64 loss)"
                );
                continue;
            }
            assert_eq!(
                col, rw,
                "row {row}: columnar ScopeKey {:?} != row-based ScopeKey {:?}",
                col, rw
            );
        }
    }

    #[tokio::test]
    async fn broadcast_batch_only_sharded_sends_row_subsets() {
        // 列式 sharded 广播（broadcast_batch_only，events=None + batch）：
        // 每个 shard 收到 events=None + batch:Some + shard_rows:Some(本 shard 行子集),
        // 且各 shard 行子集并集 = 全批（不丢、不重）。
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use wf_lang::ast::FieldRef;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("k1"),
                Some("k2"),
                Some("k1"),
                Some("k3"),
            ])) as ArrayRef],
        )
        .unwrap();

        let fanout = RuleFanout::new();
        let (tx0, mut rx0) = mpsc::channel(8);
        let (tx1, mut rx1) = mpsc::channel(8);
        fanout.register_sharded(
            "win_a",
            vec![tx0, tx1],
            Arc::from(vec![FieldRef::Simple("id".into())].into_boxed_slice()),
        );

        // (a) Defensive fallback path: no precomputed shard_rows → actor
        // repartitions internally.
        fanout
            .broadcast_batch_only("win_a", &batch, None, None, 0)
            .await;

        let drain = |rx0: &mut mpsc::Receiver<RulePush>, rx1: &mut mpsc::Receiver<RulePush>| {
            let mut seen: Vec<u32> = Vec::new();
            let mut pushed = 0;
            for rx in [rx0, rx1] {
                while let Ok(p) = rx.try_recv() {
                    pushed += 1;
                    assert!(
                        p.events.is_none(),
                        "deferred sharded push must carry no events"
                    );
                    assert!(
                        p.batch.is_some(),
                        "deferred sharded push must carry the batch"
                    );
                    let rows = p.shard_rows.expect("shard_rows set");
                    seen.extend(rows.iter().copied());
                }
            }
            // 非空 shard 各收到一个 push；k3 若单独一个 shard 也各一个。
            assert!((1..=2).contains(&pushed));
            // 并集 = 全批 4 行，无重复。
            seen.sort_unstable();
            assert_eq!(seen, vec![0, 1, 2, 3]);
        };
        drain(&mut rx0, &mut rx1);

        // (b) Parse-side precomputed path: `precompute_shard_rows` (parallel parse
        // stage) must produce a partition that, handed to the broadcast, routes
        // each row to the *same* shard and covers the batch exactly once.
        let pre = fanout
            .precompute_shard_rows("win_a", &batch)
            .expect("sharded");
        let (tx0b, mut rx0b) = mpsc::channel(8);
        let (tx1b, mut rx1b) = mpsc::channel(8);
        fanout.register_sharded(
            "win_a",
            vec![tx0b, tx1b],
            Arc::from(vec![FieldRef::Simple("id".into())].into_boxed_slice()),
        );
        fanout
            .broadcast_batch_only("win_a", &batch, None, Some(pre.as_ref()), 0)
            .await;
        drain(&mut rx0b, &mut rx1b);

        // (c) Defensive fallback on config drift: a precomputed `shard_rows` whose
        // length does not match the live subscription's shard count must be
        // ignored and the full batch repartitioned internally (never drops rows).
        let (tx0c, mut rx0c) = mpsc::channel(8);
        let (tx1c, mut rx1c) = mpsc::channel(8);
        fanout.register_sharded(
            "win_a",
            vec![tx0c, tx1c],
            Arc::from(vec![FieldRef::Simple("id".into())].into_boxed_slice()),
        );
        let stale: Arc<[Vec<u32>]> = Arc::from(vec![vec![0, 1, 2, 3], vec![], vec![], vec![]]); // len 4 != 2 shards
        fanout
            .broadcast_batch_only("win_a", &batch, None, Some(stale.as_ref()), 0)
            .await;
        drain(&mut rx0c, &mut rx1c);
    }

    /// `precompute_shard_rows` is the parse-stage hot path for sharded pull
    /// windows (q5's `bid_events`, ~100k rows/batch partitioned by `auction`).
    /// If it is slow, uneven parse workers delay a batch's `seq`, the actor's
    /// out-of-order `pending` map accumulates, and the append tail never catches
    /// up — the q5 pull-freeze signature. This measures the partition cost as a
    /// diagnostic baseline.
    #[test]
    fn precompute_shard_rows_throughput_is_bounded() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::time::{Duration, Instant};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "auction",
            DataType::Int64,
            false,
        )]));
        let values: Vec<i64> = (0..100_000).map(|i| i % 1024).collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap();

        let fanout = RuleFanout::new();
        fanout.register_window_sharding(
            "win",
            Arc::from(vec![FieldRef::Simple("auction".into())].into_boxed_slice()),
            10,
        );

        // Warm up (allocations, first-hash).
        let _ = fanout.precompute_shard_rows("win", &batch);

        let n = 100u32;
        let t0 = Instant::now();
        for _ in 0..n {
            let rows = fanout
                .precompute_shard_rows("win", &batch)
                .expect("sharded window must partition");
            assert_eq!(rows.len(), 10);
        }
        let per = t0.elapsed() / n;
        assert!(
            per < Duration::from_millis(200),
            "precompute_shard_rows 100k rows took {per:?}; it is a parse bottleneck"
        );
    }
    /// `queued_items`（2026-08-26 输出链在途量）：报（排队批数, 总容量）。
    ///
    /// 为何需要：diag 墙梯把 q13 的 12.5GB 内存增量定位到**输出链**，而窗口会计只
    /// 解释 4.1GB；规则分片通道（10 分片 × 256 槽）是该段唯一未度量的大容器。
    /// 若该 API 静默失效（恒 0），"通道是否为持有者"就无法判定。
    #[tokio::test]
    async fn queued_items_reports_backlog_across_shards() {
        let fanout = RuleFanout::new();
        assert!(
            fanout.queued_items("nope").is_none(),
            "未注册窗口返回 None（区分'无订阅'与'空队'）"
        );

        // 两个分片，各容量 4 → 总容量 8、初始排队 0。
        let (tx1, mut rx1) = mpsc::channel::<RulePush>(4);
        let (tx2, _rx2) = mpsc::channel::<RulePush>(4);
        fanout.register_round_robin("w", vec![tx1.clone(), tx2.clone()]);
        assert_eq!(fanout.queued_items("w"), Some((0, 8)), "空队 = (0, 8)");

        // 往分片 1 压 3 条（不消费）→ 排队 3。
        let mk = || RulePush {
            window_name: "w".into(),
            events: None,
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: 0,
        };
        for _ in 0..3 {
            tx1.send(mk()).await.unwrap();
        }
        assert_eq!(
            fanout.queued_items("w"),
            Some((3, 8)),
            "压入 3 条未消费 → 排队须为 3（这是判断通道是否接近满的依据）"
        );

        // 消费 2 条 → 排队回落到 1（否则会把已消费的算成在途，虚增分账）。
        rx1.recv().await.unwrap();
        rx1.recv().await.unwrap();
        assert_eq!(fanout.queued_items("w"), Some((1, 8)), "消费后排队须回落");
    }

    // =========================================================================
    // issue #80 — 表达式派生 key 分片（fanout 逐行求值）
    // =========================================================================

    /// `concat(src, ":", dst)` 表达式键规格：keys 保留逻辑名 pair，槽位存表达式。
    fn pair_expr_spec() -> ShardKeySpec {
        use wf_lang::ast::Expr;
        ShardKeySpec {
            keys: Arc::from(vec![FieldRef::Simple("pair".into())].into_boxed_slice()),
            key_exprs: Arc::from(
                vec![Some(Expr::FuncCall {
                    qualifier: None,
                    name: "concat".into(),
                    args: vec![
                        Expr::Field(FieldRef::Qualified("s".into(), "src".into())),
                        Expr::StringLit(":".into()),
                        Expr::Field(FieldRef::Qualified("s".into(), "dst".into())),
                    ],
                })]
                .into_boxed_slice(),
            ),
        }
    }

    /// src/dst UTF8 批：rows = [a:b, a:b, c:d, 缺 src, a:b, 缺 dst]。
    fn expr_batch() -> RecordBatch {
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Utf8, true),
            Field::new("dst", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("c"),
                    None, // 缺 src → concat None
                    Some("a"),
                    Some("x"),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("b"),
                    Some("b"),
                    Some("d"),
                    Some("e"),
                    Some("b"),
                    None, // 缺 dst → concat None
                ])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn expr_partition_rows_matches_row_based_per_row() {
        // 列式表达式分片必须与行式（batch_to_events + extract_scope_key_mixed）
        // 逐行落在同一 shard：缺 src/dst（求值 None）→ 双方都 shard 0。
        use crate::match_engine::batch_to_events;
        let batch = expr_batch();
        let spec = pair_expr_spec();
        let shards = 3usize;

        let per = partition_rows(&batch, &spec, shards).expect("expr 分片永不 None");
        let col_shard = |row: usize| -> usize {
            per.iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap()
        };
        let events = batch_to_events(&batch);
        let row_shard = |row: usize| -> usize {
            extract_scope_key_mixed(
                &events[row],
                spec.keys.as_ref(),
                spec.key_exprs.as_ref(),
                "",
            )
            .map(|key| scope_key_shard_index(&key, shards))
            .unwrap_or(0)
        };
        for row in 0..batch.num_rows() {
            assert_eq!(
                col_shard(row),
                row_shard(row),
                "row {row}: 列式表达式分片与行式不一致"
            );
        }
        // 无丢失/重复：覆盖全部 6 行。
        let mut flat: Vec<u32> = per.iter().flatten().copied().collect();
        flat.sort_unstable();
        assert_eq!(flat, vec![0, 1, 2, 3, 4, 5]);
        // 同派生值 a:b 必须同片（行 0/1/4）。
        let s = col_shard(0);
        assert_eq!(col_shard(1), s);
        assert_eq!(col_shard(4), s);
    }

    #[test]
    fn expr_partition_rows_equals_precomputed_field_column() {
        // 最强正确性锁：表达式分片 == 「上游预计算 pair 列 + 纯字段分片」。
        // 同一批行的派生键分片与直接按 pair 列分片逐 shard 一致。
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let expr_b = expr_batch();
        // 预计算列版本：第 3 行 src 缺 → pair = "none"（避免与 expr None 的
        // 缺字段行分片位不同：缺 src/dst 行在 expr 侧求值 None → shard0，
        // 预计算侧若给非空 pair 会落别的片——故对照只在两批**都有值**的行上做）。
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Utf8, true),
            Field::new("dst", DataType::Utf8, true),
            Field::new("pair", DataType::Utf8, true),
        ]));
        let pair_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("c"),
                    None,
                    Some("a"),
                    Some("x"),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("b"),
                    Some("b"),
                    Some("d"),
                    Some("e"),
                    Some("b"),
                    None,
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("a:b"),
                    Some("a:b"),
                    Some("c:d"),
                    None, // 与 expr 侧 None（shard0）对应
                    Some("a:b"),
                    None, // 同上
                ])) as ArrayRef,
            ],
        )
        .unwrap();

        let spec = pair_expr_spec();
        let shards = 3usize;
        let expr_per = partition_rows(&expr_b, &spec, shards).expect("expr partition");
        let field_per =
            partition_rows_by_key(&pair_batch, &[FieldRef::Simple("pair".into())], shards)
                .expect("pair column present");
        let expr_shard = |row: usize| -> usize {
            expr_per
                .iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap()
        };
        let field_shard = |row: usize| -> usize {
            field_per
                .iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap()
        };
        for row in 0..6 {
            assert_eq!(
                expr_shard(row),
                field_shard(row),
                "row {row}: 表达式分片与预计算列分片不一致"
            );
        }
    }

    #[test]
    fn precompute_shard_rows_equals_partition_rows_expr() {
        // pull 模型注册（with_exprs）后 parse 预计算分片 == 广播内部 partition_rows。
        let fanout = RuleFanout::new();
        let batch = expr_batch();
        let spec = pair_expr_spec();
        let (txs, _rxs): (Vec<_>, Vec<_>) = (0..3).map(|_| mpsc::channel::<RulePush>(8)).unzip();
        fanout.register_sharded_with_exprs("win_e", txs, spec.clone());

        let pre = fanout
            .precompute_shard_rows("win_e", &batch)
            .expect("sharded window");
        let internal = partition_rows(&batch, &spec, 3).expect("expr partition");
        assert_eq!(pre.len(), internal.len());
        for i in 0..3 {
            assert_eq!(pre[i].as_ref() as &[u32], internal[i].as_slice());
        }
    }

    #[tokio::test]
    async fn expr_sharded_broadcast_routes_same_key_together() {
        // push 模式：表达式键广播后，同派生 key 的事件必须到同一分片通道。
        use crate::match_engine::batch_to_events;
        let fanout = RuleFanout::new();
        let batch = expr_batch();
        let events: Arc<Vec<Arc<Event>>> =
            Arc::new(batch_to_events(&batch).into_iter().map(Arc::new).collect());
        let (tx0, mut rx0) = mpsc::channel::<RulePush>(16);
        let (_tx1, _rx1) = mpsc::channel::<RulePush>(16);
        fanout.register_sharded_with_exprs("win_b", vec![tx0, _tx1], pair_expr_spec());

        // 每行事件 → 预期 shard（与 fanout 同构计算）。
        let spec = pair_expr_spec();
        let row_shard = |event: &Event| -> usize {
            extract_scope_key_mixed(event, spec.keys.as_ref(), spec.key_exprs.as_ref(), "")
                .map(|k| scope_key_shard_index(&k, 2))
                .unwrap_or(0)
        };
        // 广播只带事件（行式路径 sharded_sends）。
        fanout.broadcast("win_b", &events, 0).await;
        // 收通道 0 的全部：应只含 shard==0 的行（同派生 key 同片）。
        let mut got = Vec::new();
        while let Ok(push) = rx0.try_recv() {
            if let Some(evs) = push.events {
                for e in evs.iter() {
                    got.push(row_shard(e));
                }
            }
        }
        assert!(!got.is_empty(), "shard 0 至少收到行");
        assert!(got.iter().all(|&s| s == 0), "通道 0 只应收到 shard0 的行");
        // 补验：a:b 三行若落在 shard1，则通道 0 为空时 shard1 应有全部——
        // 用实例覆盖断言：所有行要么全在 0 要么全在 1（按预期 shard 归并）。
        let expected_in_0: usize = (0..6)
            .map(|row| row_shard(&batch_to_events(&batch)[row]))
            .filter(|&s| s == 0)
            .count();
        assert_eq!(got.len(), expected_in_0, "通道 0 行数 = 预期 shard0 行数");
    }

    #[test]
    fn window_sharding_conflicts_accounts_expr_slots() {
        let fanout = RuleFanout::new();
        let keys: Arc<[FieldRef]> = Arc::from(vec![FieldRef::Simple("pair".into())]);
        let plain = ShardKeySpec::new(keys.clone());
        let expr = pair_expr_spec();
        // 同 keys、一方带表达式 → 冲突（分区方式不同）。
        fanout.register_window_sharding_with_exprs("w", plain.clone(), 4);
        assert!(
            fanout.window_sharding_conflicts_with_exprs("w", &expr),
            "expr 与纯字段分区方式不同 → 必须判冲突"
        );
        // 相同 spec 再注册 → 不冲突（覆盖式同值，共享分片）。
        let fanout2 = RuleFanout::new();
        fanout2.register_window_sharding_with_exprs("w", expr.clone(), 4);
        assert!(!fanout2.window_sharding_conflicts_with_exprs("w", &expr));
        // keys-only 入口对已注册 expr spec 也判冲突。
        assert!(fanout2.window_sharding_conflicts("w", &[FieldRef::Simple("pair".into())]));
    }

    #[test]
    fn expr_numeric_partition_matches_precomputed_int_column() {
        // review 3：数值表达式键（Int64 相加）分片 == 预计算 Int64 列分片——
        // typed key（Int）在 fanout 层与列直读同构（数字不能因路径不同塌缩出
        // 不同 shard）。
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use wf_lang::ast::{BinOp, Expr};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let expr_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    None,
                    Some(1),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(20),
                    Some(30),
                    Some(40),
                    None,
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        // 预计算 sum 列版本（缺 a/b 的行 sum 也为 null，与 expr 侧 None→shard0 对应）。
        let sum_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("sum", DataType::Int64, true),
        ]));
        let sum_batch = RecordBatch::try_new(
            sum_schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    None,
                    Some(1),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(20),
                    Some(30),
                    Some(40),
                    None,
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(11),
                    Some(22),
                    Some(33),
                    None,
                    None,
                ])) as ArrayRef,
            ],
        )
        .unwrap();

        let add = Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "a".into()))),
            right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "b".into()))),
        };
        let spec = ShardKeySpec {
            keys: Arc::from(vec![FieldRef::Simple("sum".into())].into_boxed_slice()),
            key_exprs: Arc::from(vec![Some(add)].into_boxed_slice()),
        };
        let shards = 3usize;
        let expr_per = partition_rows(&expr_batch, &spec, shards).expect("expr partition");
        let field_per =
            partition_rows_by_key(&sum_batch, &[FieldRef::Simple("sum".into())], shards)
                .expect("sum column present");
        for row in 0..5 {
            let es = expr_per
                .iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap();
            let fs = field_per
                .iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap();
            assert_eq!(es, fs, "row {row}: 数值表达式分片与预计算 Int 列分片不一致");
        }
    }

    #[test]
    fn expr_mixed_key_partition_matches_row_based() {
        // review 3：混合键（普通字段位 None + 表达式位 Some）列式分片 == 行式
        // 逐行 extract_scope_key_mixed（None 槽按字段读、expr 槽按行求值）。
        use crate::match_engine::batch_to_events;
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use wf_lang::ast::Expr;

        let schema = Arc::new(Schema::new(vec![
            Field::new("grp", DataType::Utf8, true),
            Field::new("port", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("x"),
                    Some("x"),
                    Some("y"),
                    None,
                    Some("z"),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(4),
                    None,
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        // 表达式槽：port + 10（读 Int64 列，null → None）。
        let plus = Expr::BinOp {
            op: wf_lang::ast::BinOp::Add,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "port".into()))),
            right: Box::new(Expr::Number(10.0)),
        };
        let spec = ShardKeySpec {
            keys: Arc::from(
                vec![
                    FieldRef::Simple("grp".into()),
                    FieldRef::Simple("port_k".into()),
                ]
                .into_boxed_slice(),
            ),
            key_exprs: Arc::from(vec![None, Some(plus)].into_boxed_slice()),
        };
        let shards = 3usize;
        let per = partition_rows(&batch, &spec, shards).expect("expr partition");
        let events = batch_to_events(&batch);
        for (row, ev) in events.iter().enumerate() {
            let col_s = per
                .iter()
                .position(|rows| rows.contains(&(row as u32)))
                .unwrap();
            let row_s =
                extract_scope_key_mixed(ev, spec.keys.as_ref(), spec.key_exprs.as_ref(), "")
                    .map(|k| scope_key_shard_index(&k, shards))
                    .unwrap_or(0);
            assert_eq!(col_s, row_s, "row {row}: 混合键列式与行式分片不一致");
        }
    }

    /// 100k 行 src/dst UTF8 批（派生 key 值域 1024，避免字符串缓存/热点失真）。
    fn big_expr_batch(n: usize) -> RecordBatch {
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Utf8, true),
            Field::new("dst", DataType::Utf8, true),
        ]));
        let src: Vec<String> = (0..n)
            .map(|i| format!("10.{}.{}.{}", (i / 65_536) % 256, (i / 256) % 256, i % 256))
            .collect();
        let dst: Vec<String> = (0..n).map(|i| format!("dst{}", i % 1024)).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(src)) as ArrayRef,
                Arc::new(StringArray::from(dst)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    /// `partition_rows` 表达式分片是 parse/broadcast 单 writer 路径上的逐行
    /// eval 热点（issue #80）——吞吐必须有界，防止逐行 eval 退化（review R1 的
    /// 批级 field index 提升是它的主要杠杆）。与纯字段版
    /// `precompute_shard_rows_throughput_is_bounded` 对称。
    #[test]
    fn expr_partition_rows_throughput_is_bounded() {
        use std::time::{Duration, Instant};
        let batch = big_expr_batch(100_000);
        let spec = pair_expr_spec();
        let _ = partition_rows(&batch, &spec, 8).expect("expr partition");

        let n = 10u32;
        let t0 = Instant::now();
        for _ in 0..n {
            let per = partition_rows(&batch, &spec, 8).expect("expr partition");
            assert_eq!(per.len(), 8);
        }
        let per = t0.elapsed() / n;
        // 预算 = 实测量级 ×3 余量（CI 抖动）；超限说明逐行求值路径出现量级退化。
        assert!(
            per < Duration::from_millis(900),
            "expr 列式分片 100k rows took {per:?}; 逐行 eval 路径异常"
        );
    }

    /// pull parse 预计算路径（`precompute_shard_rows` + 表达式 spec）同样有界。
    #[test]
    fn expr_precompute_shard_rows_throughput_is_bounded() {
        use std::time::{Duration, Instant};
        let batch = big_expr_batch(100_000);
        let fanout = RuleFanout::new();
        fanout.register_window_sharding_with_exprs("win", pair_expr_spec(), 10);
        let _ = fanout.precompute_shard_rows("win", &batch);

        let n = 10u32;
        let t0 = Instant::now();
        for _ in 0..n {
            let rows = fanout
                .precompute_shard_rows("win", &batch)
                .expect("sharded window must partition");
            assert_eq!(rows.len(), 10);
        }
        let per = t0.elapsed() / n;
        assert!(
            per < Duration::from_millis(900),
            "expr precompute_shard_rows 100k rows took {per:?}; parse 阶段逐行 eval 异常"
        );
    }
