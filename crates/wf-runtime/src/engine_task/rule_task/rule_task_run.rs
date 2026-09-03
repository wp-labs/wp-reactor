//! 任务构造与 pull/push 数据通路（rule_task.rs 拆分，2026-09-04）：`RuleTask::new`
//! 机器构造/窗口接线，pull 路径拉批（`pull_and_advance`）与 push 路径收包
//! （`process_push`/`drain_push_channel`），批时间列 max 与即时 join 追平 gate
//! （`wait_eager_joins_caught_up`）、关机 drain 与实例指标上报。批次编排入口
//! `process_batch`（双路分发）同属本文件。

use super::*;

impl RuleTask {
    pub(crate) fn new(
        config: RuleTaskConfig,
    ) -> (
        Self,
        tokio_util::sync::CancellationToken,
        std::time::Duration,
    ) {
        let RuleTaskConfig {
            machine,
            each_alias,
            each_time_field,
            executor,
            window_sources,
            sink_fanout,
            cancel,
            timeout_scan_interval,
            router,
            metrics,
            intermediate_targets,
            pipe_registry,
            eos_flush,
            push_rx,
            progress,
            conv_sink,
            shard_index,
            shard_count,
            key_partitioned,
        } = config;
        let aliases: HashMap<String, Vec<String>> = window_sources
            .iter()
            .map(|src| (src.window_name.clone(), src.aliases.clone()))
            .collect();
        let ordered_aliases: HashMap<String, Vec<String>> = aliases
            .iter()
            .map(|(window_name, aliases)| {
                let ordered = aliases
                    .iter()
                    .filter(|alias| executor.is_aux_bind_alias(alias.as_str()))
                    .chain(
                        aliases
                            .iter()
                            .filter(|alias| !executor.is_aux_bind_alias(alias.as_str())),
                    )
                    .cloned()
                    .collect();
                (window_name.clone(), ordered)
            })
            .collect();

        // Initialize cursors to current position (skip historical data).
        let cursors: HashMap<String, u64> = window_sources
            .iter()
            .map(|src| {
                let seq = src.window.next_seq();
                (src.window_name.clone(), seq)
            })
            .collect();

        let seq = TASK_SEQ.fetch_add(1, Ordering::Relaxed);
        let rule_name = executor.plan().name.clone();
        let task_id = format!("{}#{}", rule_name, seq);
        let conv_plan = executor.plan().conv_plan.clone();
        // Direct-write on-each emit only when the target is a sink target:
        // intermediate pipes still consume full `OutputRecord` rows.
        let each_direct = executor.plan().each_plan.is_some()
            && !intermediate_targets.contains(executor.plan().yield_plan.target.as_str());
        // P3：第一个带 `emit at` 的 join 是 deferred 驱动（v1 单 deferred join，设计 §9 风险 5）
        let deferred = executor
            .plan()
            .joins
            .iter()
            .position(|j| j.emit_at.is_some())
            .map(|join_idx| {
                // D4：在 join 目标窗口取走保留 pin（spawn 阶段已同步预注册，避免与首批
                // append 竞争；无预注册时当场注册一个）。deferred 规则不从右窗 pull
                //（只做点查询）→ 无消费者槽位 → `min_acked` 对它报 u64::MAX（全部可
                // 驱逐），字节上限一旦成为约束就会静默丢掉到期评估还要用的行
                //（q9/q4a 30M −62%，2026-08-24）。pin 按自身评估前沿推进，见
                // `publish_retention_floor`。
                let retention_pin = router
                    .registry()
                    .get_window(&executor.plan().joins[join_idx].right_window)
                    .and_then(|w| w.take_retention_pin());
                DeferredRuntime {
                    pending: Vec::new(),
                    missed: Vec::new(),
                    watermark: i64::MIN,
                    join_idx,
                    retention_pin,
                    lo_min: i64::MAX,
                    lo_min_dirty: false,
                }
            });
        // D4 扩展（2026-08-24）：snapshot/asof join 目标窗口同样持有保留 pin。
        // snapshot 语义 = join 时刻的完整状态，驱动事件可引用**任意老**的实体行
        // （q3 join person / q6·q20 join auction）——无法像 deferred 那样按
        // `min(lo_ns)` 精确化前沿，只能全保留（`i64::MIN`）直到任务结束（Arc drop
        // → Weak 死 → 自动释放）。实体表目标（person/auction @30M 全量 ~470MB）内存
        // 代价可忽略，且由 `over` 时间驱逐封顶；这正是「2GB 字节上限恰好够大」
        // 背后的预算兵役的引擎化——上限收紧或数据变大时不再静默丢输出。
        let snapshot_pins: Vec<Arc<AtomicI64>> = executor
            .plan()
            .joins
            .iter()
            .filter(|j| {
                j.emit_at.is_none()
                    && matches!(
                        j.mode,
                        wf_lang::ast::JoinMode::Snapshot | wf_lang::ast::JoinMode::Asof { .. }
                    )
            })
            .filter_map(|j| {
                router
                    .registry()
                    .get_window(&j.right_window)
                    .and_then(|w| w.take_retention_pin())
            })
            .collect();

        // 2026-08-29 q6/q20 snapshot join 竞态 gate 的等待目标（编译期固定，
        // 构造时解析一次，免每批 clone live_joins/key_join）：本规则**实际执行**
        // 的即时 join 右窗（去重）。key_join 必需：q6 形态的 join 只喂 match 键、
        // 输出不读右窗字段 → 被 dead-join 消除剔除出 live_joins，但 join 在
        // advance 仍执行——漏掉它则 gate 不触发、竞态照旧（实测从未触发）。
        let eager_join_targets = {
            let mut targets: Vec<String> = Vec::new();
            for join in executor.live_joins() {
                if join.emit_at.is_none() && !targets.contains(&join.right_window) {
                    targets.push(join.right_window.clone());
                }
            }
            if let Some(kjp) = machine.as_ref().and_then(|m| m.plan().key_join.clone())
                && !targets.contains(&kjp.right_window)
            {
                targets.push(kjp.right_window);
            }
            targets
        };

        let task = Self {
            task_id,
            machine,
            each_alias,
            each_time_field,
            executor,
            conv_plan,
            conv_sink,
            sources: window_sources,
            aliases,
            ordered_aliases,
            sink_fanout,
            cursors,
            router,
            metrics,
            intermediate_targets,
            pipe_registry,
            eos_flush,
            last_activity_wall: std::time::Instant::now(),
            wall_advance_ns: 0,
            timeout_scan_interval,
            push_rx,
            pushed_seq: 0,
            shard_index,
            shard_count,
            key_partitioned,
            eager_join_targets,
            last_bailed_frontier: std::sync::Mutex::new(None),
            progress,
            advance_nanos: 0,
            scan_nanos: 0,
            emit_nanos: 0,
            exec_nanos: 0,
            close_exec_nanos: 0,
            append_nanos: std::sync::atomic::AtomicU64::new(0),
            fanout_nanos: std::sync::atomic::AtomicU64::new(0),
            last_profile_dump: std::time::Instant::now(),
            cached_wall_nanos: AtomicU64::new(wall_nanos()),
            emit_sample_remaining: AtomicU32::new(EMIT_METRIC_SAMPLE_INTERVAL),
            append_sample_remaining: AtomicU32::new(EMIT_METRIC_SAMPLE_INTERVAL),
            last_reported_instances: AtomicI64::new(0),
            pending_alerts: std::sync::Mutex::new(PendingAlertColumns::default()),
            pipe_state: std::sync::Mutex::new(PipeState::Uninit),
            each_direct,
            deferred,
            snapshot_pins,
        };
        (task, cancel, timeout_scan_interval)
    }

    pub(super) fn rule_name(&self) -> &str {
        self.executor.plan().name.as_str()
    }

    pub(super) fn instance_count(&self) -> usize {
        self.machine
            .as_ref()
            .map(|machine| machine.instance_count())
            .unwrap_or(0)
    }

    // -- Data processing ----------------------------------------------------

    /// Read new batches from all windows and advance the state machine.
    ///
    /// **Pull-model (M1, window-actor-pull-model.md §3.3).** Columnar pull:
    /// each window's shared `RecordBatch` Arcs are read once (zero data copy)
    /// and fed straight into [`Self::process_batch`]'s columnar entry point —
    /// replacing the legacy `events_since` row-based path. Sharding is handled
    /// without re-partitioning:
    ///
    /// - *Key-partitioned (match) windows* — the parse stage already computed
    ///   the per-shard row subset and stored it in `TimedBatch.shard_rows`. A
    ///   sharded task pulls only its `shard_rows[i]` rows (P2 zero re-partition);
    ///   `read_since_with_shard(cursor, Some(i))` returns that subset.
    /// - *On-each round-robin / unsharded windows* — `read_since_with_shard`
    ///   returns the whole batch; a round-robin task processes a batch only
    ///   when `seq % shard_count == shard_index` (whole-batch round-robin,
    ///   identical to the legacy `register_round_robin` semantics).
    pub(crate) async fn pull_and_advance(&mut self) {
        // Phase 1: collect pulled batches per window. This phase only takes
        // disjoint field borrows, so it must stay free of `&mut self` calls
        // (the `&self.sources` borrow would conflict with `process_batch`).
        let mut pending: PendingAliasRows = Vec::new();
        for source in &self.sources {
            let cursor = self.cursors.get(&source.window_name).copied().unwrap_or(0);
            // Key-partitioned (match) windows yield per-shard row subsets;
            // everything else (on-each round-robin, unsharded) pulls whole
            // batches and is gated below.
            let key_partitioned = self.key_partitioned;
            let pull_shard = if key_partitioned {
                self.shard_index
            } else {
                None
            };
            let (batches, shard_rows_per_batch, new_cursor, gap) =
                source.window.read_since_with_shard(cursor, pull_shard);
            wf_debug!(pipe,
                task_id = %self.task_id,
                window = %source.window_name,
                cursor = cursor,
                new_cursor = new_cursor,
                batches = batches.len(),
                gap = gap,
                "read_since_with_shard"
            );

            if gap {
                wf_warn!(pipe,
                    task_id = %self.task_id,
                    window = %source.window_name,
                    "cursor gap detected — some data was lost to eviction"
                );
                if let Some(metrics) = &self.metrics {
                    metrics.inc_rule_cursor_gap(
                        self.executor.plan().name.as_str(),
                        &source.window_name,
                    );
                }
            }
            let first_batch_seq = new_cursor.saturating_sub(batches.len() as u64);
            let materialize_fields = source.window.materialize_fields().cloned();
            pending.push((
                source.window_name.clone(),
                first_batch_seq,
                batches,
                shard_rows_per_batch,
                materialize_fields,
                key_partitioned,
                new_cursor,
            ));
        }

        // Phase 2: advance read cursors (separate from phase 1 so the mutable
        // borrow does not fight the `&self.sources` iteration above).
        for (window, _, _, _, _, _, new_cursor) in &pending {
            self.cursors.insert(window.clone(), *new_cursor);
        }

        // Phase 3: process each pulled batch (`&mut self`).
        for (
            window,
            first_batch_seq,
            batches,
            shard_rows_per_batch,
            materialize_fields,
            key_partitioned,
            new_cursor,
        ) in pending
        {
            // 分片（round-robin）下本 shard 最后处理的批次 seq——ack 用它而非
            // 读位置（见下方 ack 注释）。
            let mut last_processed: Option<u64> = None;
            for (batch_index, batch) in batches.iter().enumerate() {
                let batch_seq = first_batch_seq + batch_index as u64;
                let shard_rows = shard_rows_per_batch
                    .get(batch_index)
                    .and_then(|opt| opt.as_deref())
                    .map(|rows| rows.as_slice());
                // Key-partitioned windows are expected to carry a precomputed
                // per-shard row subset for *every* batch they own; a `None` here
                // means this batch fell back to whole-batch processing (missing
                // `shard_rows` — e.g. a hot-reload batch or a changed shard
                // count). When several shard instances hit the same gap they
                // each process the whole batch → cross-shard duplicate
                // consumption. That is the lossless-but-duplicating defensive
                // trade-off (duplicates are masked by at-least-once acking), but
                // surface it so a recurring fallback is not silent.
                if key_partitioned && shard_rows.is_none() {
                    wf_warn!(pipe,
                        task_id = %self.task_id,
                        window = %window,
                        shard = ?self.shard_index,
                        batch_seq = batch_seq,
                        "key-partitioned batch missing shard_rows — fell back to whole-batch processing (possible cross-shard duplicate)"
                    );
                }
                // Key-partitioned: `shard_rows` already restricts this task to
                // its own rows, so every pulled batch is processed. Otherwise
                // gate whole-batch (on-each round-robin / unsharded) tasks.
                let should_process = key_partitioned
                    || self.shard_count <= 1
                    || (batch_seq % self.shard_count as u64)
                        == self.shard_index.unwrap_or(0) as u64;
                if !should_process {
                    continue;
                }
                self.process_batch(
                    &window,
                    batch_seq,
                    Some(batch_seq),
                    None,
                    Some(batch),
                    shard_rows,
                    materialize_fields.as_deref(),
                )
                .await;
                last_processed = Some(batch_seq);
            }
            // Ack 语义（2026-08-25 q13a 分片隐患修复）：
            // - 非分片 / key-partitioned：ack **读位置**（`new_cursor`）——
            //   本任务处理全部（行子集）批次，读 = 处理。
            // - whole-batch round-robin 分片：ack **处理位置**（本 shard 份额内
            //   最后处理批次 + 1）。旧代码 ack 读位置（= 全部批次）会让
            //   `min_acked` 追平 `next_seq` → `bid_events` 驱逐无未读保护 →
            //   cap/时间驱逐可能删掉**其他 shard 尚未处理**的批次（cursor gap
            //   静默丢数据，q13a 分片后消费快未触发、语义竞态存在）。处理位置
            //   ack 下，未处理批次恒受 `min_acked` 保护（不丢）；已处理批次
            //   在 cap 超限时正常回收（最慢 shard 推进则 floor 推进）。
            //   `max_acked` 完成信号不受影响：全部批次处理完 = 每 shard 处理到
            //   自己的最后一批 → max = next_seq。`fetch_max` 与 push 路径一致
            //   （乱序防御，单调）。
            if let Some(slot) = self.progress.get(&window) {
                let ack = if key_partitioned || self.shard_count <= 1 {
                    new_cursor
                } else {
                    // 本轮无自己份额（全是别人批次）：ack 不推进（fetch_max(0)
                    // 对已有值无影响）。
                    last_processed.map(|seq| seq + 1).unwrap_or(0)
                };
                slot.fetch_max(ack, std::sync::atomic::Ordering::Release);
            }
        }
        self.update_rule_instances_metric();
    }

    /// 批级时间列 max（raw i64，可向量化）：gate 只需「批 max 事件时间」用于与
    /// 目标窗 frontier（同为 raw）比较——`batch_event_time_nanos_at` 的 f64
    /// round-trip 圆整误差（1.7e18ns 处 ±256ns）相对 250ms 余量可忽略，直读 i64
    /// 让编译器向量化（实测 187µs/批 → ~5µs/批，q20 30M gate 总耗时 139ms→10ms
    /// 量级，消除 gate 的性能回退主项）。
    pub(super) fn batch_time_col_max(batch: &RecordBatch, col_idx: usize) -> i64 {
        let col = batch.column(col_idx);
        let n = batch.num_rows();
        let null_free = col.null_count() == 0;
        match col.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                if let Some(arr) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
                    let mut max_ts = i64::MIN;
                    for row in 0..n {
                        let v = arr.value(row);
                        if null_free || !col.is_null(row) {
                            max_ts = max_ts.max(v);
                        }
                    }
                    return max_ts;
                }
            }
            DataType::Int64 => {
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    let mut max_ts = i64::MIN;
                    for row in 0..n {
                        let v = arr.value(row);
                        if null_free || !col.is_null(row) {
                            max_ts = max_ts.max(v);
                        }
                    }
                    return max_ts;
                }
            }
            DataType::Float64 => {
                if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                    let mut max_ts = i64::MIN;
                    for row in 0..n {
                        if null_free || !col.is_null(row) {
                            max_ts = max_ts.max(arr.value(row) as i64);
                        }
                    }
                    return max_ts;
                }
            }
            _ => {}
        }
        // 其它类型：逐行完整检查（原语义，含 f64 round-trip）。
        let mut max_ts = i64::MIN;
        for row in 0..n {
            max_ts = max_ts.max(batch_event_time_nanos_at(batch, col_idx, row));
        }
        max_ts
    }

    /// Process a single parsed batch (shared `Arc`) against the state machine.
    ///
    /// This is the per-batch body shared by the legacy pull path
    /// ([`Self::pull_and_advance`]) and the push path (channel recv). `batch_seq`
    /// is used only for debug event references.
    /// 等所有即时 join 目标窗的 committed frontier 追平 `batch_max_ts + 余量`（本批
    /// 驱动事件的最大事件时间 + 跨批引用前视余量）。数据生成保证 bid 引用的 auction
    /// 事件时间早于 bid（同一 source 批内交错），故目标窗 commit 覆盖该上界即所有
    /// 引用行可见。余量覆盖「未来 lead 引用」（NEXMark AUCTION_ID_LEAD=10 →
    /// 引用行事件时间可晚于驱动行最多 ~50ms，且行在**下一 source 批**才产出——
    /// 只等 batch_max_ts 会漏掉它们：all 模式实测 q20 196517→193k、q6
    /// 872913→848k 的残余 miss 即此）。放弃等待的安全条件：目标窗提交前沿**停止
    /// 增长**（~30ms 无新提交 = 目标流已排空/终止，或数据尾部 bid/auction 流 max
    /// 天然差几秒——此时引用行早已提交，join 本就命中）；30s 硬截止为防御兜底
    /// （理论上到不了）。
    /// 注：与 deferred 评估 gate（scan_deferred）同一 `committed_frontier_ns`
    /// 健全判据、同一停滞模式（flush 先例 60ms，本 gate 因非终局收紧到 30ms），
    /// 只是把「等目标窗追平本批事件时间」从评估阶段前移到处理阶段。
    pub(super) async fn wait_eager_joins_caught_up(&self, targets: &[String], batch_max_ts: i64) {
        for right_window in targets {
            let Some(target) = self.router.registry().get_window(right_window) else {
                continue; // provider 等无 buffer 窗口的 join 目标：无 actor/frontier
            };
            if target.time_col_index().is_none() {
                continue; // 无时间列 → frontier 无意义（防御：不等待）
            }
            // 目标上界 = 驱动批 max 事件时间 + 跨批引用前视余量（见函数注释）：
            // 引用行可在**下一 source 批**才产出，事件时间晚于驱动批 max 最多
            // 一个 lead 窗口（NEXMark ≤50ms）。250ms = 5× 实测前视（19ms），
            // 且小于单批跨度（~200ms）——等待被目标 actor 的连续提交快速满足。
            const EAGER_JOIN_LEAD_MARGIN_NS: i64 = 250_000_000;
            let target_ts = batch_max_ts.saturating_add(EAGER_JOIN_LEAD_MARGIN_NS);
            let cur0 = target.committed_frontier_ns();
            // 2026-08-30 尾部性能优化：上次 bail（frontier 停滞放行）后目标窗
            // frontier 未再推进 → 目标流已排空/结束（或数据尾部 bid/auction 流
            // max 天然差几秒）→ 本批结论与上次 bail 相同（引用行已提交或确实
            // 缺失）→ 跳过等待。frontier 一旦推进缓存即失效（新数据 → 正常等待）。
            if self
                .last_bailed_frontier
                .lock()
                .expect("bail frontier lock poisoned")
                .as_ref()
                .is_some_and(|(w, f)| w == right_window && *f == cur0)
            {
                continue;
            }
            let mut stalled = 0u32;
            let mut last = cur0;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let cur = target.committed_frontier_ns();
                if cur >= target_ts {
                    break; // 所有引用行（含跨批前视）已提交
                }
                if cur == last {
                    stalled += 1;
                    // 2026-08-30 q3 冷启动保护：目标窗**从未提交**（frontier ==
                    // i64::MIN）时不 bail——停滞可能是目标窗 actor 尚未处理首个
                    // mailbox batch（跨窗 actor 独立调度，冷启动首个 commit 前
                    // frontier 必为 i64::MIN），bail 会让本批对空窗/半索引 join 全
                    // miss。bail 只对「目标流已排空/结束」安全（frontier 停在真实
                    // 提交值，引用行早已落地）。真无数据的目标窗靠 30s 硬截止兜底。
                    if stalled >= 3 && cur != i64::MIN {
                        // 连续 ~30ms 无新提交 → 目标流已排空/终止。记录 bail 时
                        // 的 frontier：后续批 frontier 未动则直接跳过（见上）。
                        *self
                            .last_bailed_frontier
                            .lock()
                            .expect("bail frontier lock poisoned") =
                            Some((right_window.clone(), cur));
                        break;
                    }
                } else {
                    stalled = 0;
                }
                last = cur;
                if std::time::Instant::now() >= deadline {
                    // 限时兜底（防御；actor 必然推进，到不了）。记录 frontier：
                    // 后续批若 frontier 未动（真无数据的目标窗）直接跳过等待。
                    *self
                        .last_bailed_frontier
                        .lock()
                        .expect("bail frontier lock poisoned") = Some((right_window.clone(), cur));
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn process_batch(
        &mut self,
        window_name: &str,
        batch_seq: u64,
        lookup_max_seq: Option<u64>,
        events: Option<&Arc<Vec<Arc<Event>>>>,
        batch: Option<&RecordBatch>,
        shard_rows: Option<&[u32]>,
        materialize_fields: Option<&HashSet<String>>,
    ) {
        // perf-diag cut_rules 门控：规则求值直通（ack 保留——`pull_and_advance`
        // 在 process_batch 返回后推进 cursor，append/ack 仍在 floor 档收敛）。
        // 哨兵窗口由独立哨兵任务处理，不经过本函数，天然豁免。
        if crate::perf_diag::perf_cut_rules() {
            return;
        }
        let Some(aliases) = self.aliases.get(window_name) else {
            return;
        };
        let Some(_ordered_aliases) = self.ordered_aliases.get(window_name) else {
            return;
        };
        // 2026-08-29 q6/q20 snapshot join 竞态 gate：处理驱动批前，等即时
        // （snapshot/asof/inner，非 deferred）join 目标窗 commit 追平本批 max
        // 事件时间。并行 parse + 跨窗口 actor 独立 commit 使 auction_events
        // 的提交可能滞后于 bid_events 消费——即时 join 读目标窗时，已 append
        // 但未 commit 的行不可见 → 静默 miss（q20 p=10 196517→189430、q6
        // 872913→788k~845k、q3 6060→4795，oracle 对拍定位；单规则隔离全对，
        // 多规则同跑竞争放大）。deferred join 已有评估 gate（scan_deferred 的
        // committed frontier），不需要本 gate；无即时 join / 无时间列的规则
        // 零开销。
        // 触发条件的时间列：on-each 规则用 `each_time_field`；match 规则
        // （each_time_field = None，q6/q3 形态）回退到驱动窗的时间列名——
        // match 的 join 也在本批行循环内求值，需要同样的 frontier 保证。
        let batch_time_field = match self.each_time_field.as_deref() {
            Some(tf) => Some(tf.to_string()),
            None => self
                .router
                .registry()
                .get_window(window_name)
                .and_then(|w| {
                    let idx = w.time_col_index()?;
                    Some(w.schema().field(idx).name().clone())
                }),
        };
        let targets = &self.eager_join_targets;
        if !targets.is_empty()
            && let Some(batch) = batch
            && let Some(time_col) = batch_time_field.as_deref()
            && let Ok(col_idx) = batch.schema().index_of(time_col)
        {
            let batch_max_ts = Self::batch_time_col_max(batch, col_idx);
            self.wait_eager_joins_caught_up(targets, batch_max_ts).await;
        }
        // L2 deferred materialization: when the producer broadcast only the raw
        // batch, materialize only the rows the bind filter accepts. The time
        // column is still scanned over every row (watermark/expiry), but the
        // per-row Event is only built for hit rows (Q2 hit ~0.8%).
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);

        // Columnar bind-filter masks, one per alias, computed once per batch
        // from the raw `RecordBatch` (zero-copy). `None` (the inner) means the
        // alias has no filter or a non-columnar filter → fall back to the
        // per-event interpreted path at each row.
        let columnar_masks: HashMap<String, Option<BooleanArray>> = match batch {
            Some(batch) => aliases
                .iter()
                .map(|alias| {
                    (
                        alias.clone(),
                        self.executor.bind_filter_columnar_mask(alias, batch),
                    )
                })
                .collect(),
            None => HashMap::new(),
        };
        // Columnar branch-guard masks for the state machine's event steps.
        let branch_masks = match batch {
            Some(batch) => self.executor.branch_guard_masks(batch),
            None => GuardMasks::default(),
        };

        // 生产执行路径判定（P4 单轨化可观测性基座，见 design §11.3）：唯一事实
        // 源 `RuleExecutor::execution_path`——execution_path.rs 矩阵测试逐形状
        // 断言，断言的路径 ≡ 生产执行的路径，避免内联布尔与断言漂移。解构为
        // 下述两个布尔供各分支复用：
        //   DeferredMachine → defer_materialize：match 规则 P3 FieldView 列式喂
        //     状态机。前置 = raw batch 在 + machine 在 + DEBUG 关 + 该窗 bind
        //     filter 全列式（非列式 filter 在缺失 mask 时全放行会静默丢过滤子
        //     集；被拒行无 Event 可渲染 DEBUG 引用）。relay/push 同时携带物化
        //     events 也走此路径——events 仅作 emit 触发投影（materialize_fields
        //     字节一致），不再是强制逐行物化的理由（q15 eager 1.1µs vs
        //     deferred 326ns，2026-08-22）。
        //   ColumnarEach → columnar_each：on-each 列式快路径（无 per-row Event
        //     物化；q13a pipe 列式化、q13b 放开 events.is_none()）。
        //   EagerRows → 行式 Event 物化兜底（下述 eager events 分支）。
        let (defer_materialize, columnar_each) =
            match self.executor.execution_path(&ExecutionPathContext {
                window_name,
                raw_batch: batch.is_some(),
                machine: self.machine.is_some(),
                shard_rows: shard_rows.is_some(),
                debug_enabled,
                each_direct: self.each_direct,
            }) {
                ExecutionPath::DeferredMachine => (true, false),
                ExecutionPath::DeferredPending => (false, false),
                ExecutionPath::ColumnarEach => (false, true),
                ExecutionPath::EagerRows => (false, false),
            };
        // 注：deferred（emit at）规则不得走列式 each 快路径：挂起/到期评估在
        // 行循环里（deferred_pending_for → scan_deferred）。q8 等 on each +
        // deferred 若走快路径会被列式 join 当 Snapshot 即时输出——deferred
        // 语义丢失（2026-08-23 验证基线暴露：q8 引擎 33k vs oracle 82k）。
        // 该约束已入 `execution_path`（deferred_join 前置），此处仅保留注释。

        // P4 gap-1（q4/q8/q9，2026-09-02）：deferred join 驱动列式挂起视图。
        // 免 eager_events 逐行 Event 物化——挂起队列持 `JoinRow::Columnar`
        // （Arc batch + 行号 + 字段索引 + 投影，同批共享）。有 let 绑定的规则
        // 在 `deferred_pending_for` 挂起时物化一次（回退语义），无 let 全列式。
        // DEBUG 开时保留 eager（被拒行需要 Event 渲染调试详情）。与
        // `ExecutionPath::DeferredPending` 的前置逐项同构（machine 在时是 match
        // 规则，走上面的 DeferredMachine/EagerRows 分支，不在此列）。
        let deferred_columnar: Option<DeferredColumnarBatch> = if !debug_enabled
            && self.machine.is_none()
            && self.deferred.is_some()
            && let Some(batch) = batch
        {
            let batch_arc = Arc::new(batch.clone());
            let index = build_field_index(&batch_arc);
            let projection = materialize_fields.map(|f| Arc::new(f.clone()));
            Some(DeferredColumnarBatch {
                batch: batch_arc,
                index,
                projection,
            })
        } else {
            None
        };

        // Row domain: a **sharded** deferred push only owns the rows partitioned
        // to this shard (`shard_rows`); an unsharded push scans the whole batch.
        // Both the lazy-materialization scan and the main state-machine loop
        // iterate this domain. `DeferredRows` always uses **absolute** batch-row
        // indices (times / hit_indices), so all downstream consumers are
        // unchanged; shard-external rows are simply never iterated here.
        // Row count for the unsharded domain: relay / eager pushes carry
        // materialized `events` (batch is None) so their count comes from the
        // events; deferred pushes carry the raw batch.
        let num_rows = events
            .map(|e| e.len())
            .unwrap_or_else(|| batch.map(|b| b.num_rows()).unwrap_or(0));
        let row_domain = match shard_rows {
            Some(rows) => RowDomain::Sharded(rows),
            None => RowDomain::Full(num_rows),
        };

        let deferred = if defer_materialize {
            Some(self.build_deferred_rows(
                batch.expect("deferral requires the raw batch"),
                aliases,
                &columnar_masks,
                materialize_fields,
                &row_domain,
            ))
        } else {
            None
        };

        // Eager events (full materialization), used by the non-deferred machine
        // path and the `on each` path. 2026-09-02 P4 gap-1：deferred 列式挂起
        // 时同样免物化（行循环经 `DeferredColumnarBatch` 视图读列）。
        let eager_events: Option<Arc<Vec<Arc<Event>>>> =
            if defer_materialize || columnar_each || deferred_columnar.is_some() {
                None
            } else {
                Some(match events {
                    Some(events) => Arc::clone(events),
                    None => {
                        let batch = batch.expect("deferred materialization requires the raw batch");
                        let events = match materialize_fields {
                            Some(fields) => batch_to_events_filtered(batch, fields),
                            None => batch_to_events(batch),
                        };
                        Arc::new(events.into_iter().map(Arc::new).collect())
                    }
                })
            };

        // 2026-09-02 P4 gap-1：deferred 列式挂起（deferred_columnar）时无
        // eager_events 也无 DeferredRows——用整批行数计数（指标/活动墙钟口径
        // 与 eager 路径一致）。
        let input_events = deferred.as_ref().map(|d| d.times.len()).unwrap_or_else(|| {
            eager_events
                .as_ref()
                .map(|e| e.len())
                .unwrap_or_else(|| num_rows)
        });

        let mut stats = RuleBatchDebugStats {
            input_events,
            ..RuleBatchDebugStats::default()
        };
        let rule_name = debug_enabled.then(|| self.rule_name().to_string());
        let rule_name_for_log = rule_name.as_deref().unwrap_or("");
        let aliases_for_log = if debug_enabled {
            Some(aliases.join(","))
        } else {
            None
        };
        self.log_batch_start(
            debug_enabled,
            window_name,
            batch_seq,
            input_events,
            rule_name_for_log,
            aliases_for_log.as_deref(),
        );
        // Track the last wall-clock moment events were processed, so the
        // periodic timeout scan can advance the watermark across idle gaps.
        if input_events > 0 {
            self.last_activity_wall = std::time::Instant::now();
            // 真实事件推进会覆盖 idle 的墙钟信用（避免与事件时间双重计数）。
            self.wall_advance_ns = 0;
            // Cache wall time for the emit path's e2e-latency sample.
            self.cached_wall_nanos
                .store(wall_nanos(), Ordering::Relaxed);
        }
        // M2 (seq-watermark consistency): bound window_lookup to the seq of the
        // batch being processed (pull model only — push keeps the legacy full-
        // window view via `lookup_max_seq = None`). The watermark is scoped to
        // *this* source window only: join targets are independent windows and
        // must not be bounded by this window's seq. See
        // window-actor-pull-model.md §3.5.
        let lookup =
            RegistryLookup::with_source_watermark(&self.router, lookup_max_seq, window_name);
        // on-each: events within a batch share the window schema, so the
        // sorted field order used for wfx_id hashing is computed once per
        // batch instead of collected + sorted per event.
        let each_field_order: Vec<&smol_str::SmolStr> = match (
            self.executor.plan().each_plan.is_some(),
            eager_events.as_ref().and_then(|events| events.first()),
        ) {
            (true, Some(first)) => {
                let mut names: Vec<&smol_str::SmolStr> = first.fields.keys().collect();
                names.sort_unstable();
                names
            }
            _ => Vec::new(),
        };
        // Batch-level emit timestamp: all events in this batch share one
        // (nanos, formatted) pair — the executor caches the formatted string
        // and Arc-shares it across every record it builds this batch.
        let batch_emit_nanos = self.cached_wall_nanos.load(Ordering::Relaxed) as i64;
        // 注入批次墙钟（issue #82：`@first_match_time` 语义）——该批内实例首次
        // 命中时记录的引擎处理时钟与输出 emit_time 同源（批次级一次 now()）。
        if let Some(machine) = self.machine.as_mut() {
            machine.set_processing_wall(batch_emit_nanos);
        }
        // On-each columnar fast path: skip the per-row loop entirely. Hit rows
        // come from the (absent-or-columnar) bind-filter masks — with the gate
        // in `each_plan_columnar_safe`, a `None` mask means no filter (every
        // row passes, exactly like `event_matches_alias` with no filter).
        if columnar_each {
            let batch = batch.expect("columnar each requires the raw batch");
            self.process_batch_columnar_each(
                batch,
                aliases,
                &columnar_masks,
                &lookup,
                batch_emit_nanos,
            )
            .await;
            return;
        }
        // Plan C2 batching: when the per-event detail logs are off, collect
        // the each-direct rows and emit them in one vectorized pass after
        // the loop (debug runs keep the per-event path for exact detail).
        let mut each_direct_rows: Vec<(&wf_engine::match_engine::Event, i64)> = Vec::new();
        // P2③: for conv-sink shards, aggregate raw closes across the whole batch
        // and send ONE ConvCloseBatch (with the max event-time watermark) after
        // the loop — avoids a per-event bounded(32) channel send on the hot path.
        let mut conv_closes: Vec<wf_engine::match_engine::CloseOutput> = Vec::new();
        // Columnar close emit (L4): gate-passing rules accumulate raw closes
        // here and emit them vectorized after the row loop — see
        // `execute_close_direct_batch_columnar`. Debug detail off keeps the
        // per-close log/counts (same gate shape as the on-each columnar path).
        let close_columnar = !debug_enabled && self.executor.close_plan_columnar_safe();
        let mut columnar_closes: Vec<wf_engine::match_engine::CloseOutput> = Vec::new();
        // Columnar match emit (2026-08-23, q6 形态): score 常量 + 输出全
        // Lit/Field + 输出字段不引用非键右窗 —— 命中 ctx 批量直写 builder，免
        // 每命中 OutputRecord 中间物化（`match_plan_columnar_safe` 门控）。
        // 批级 owned 累积（move，零成本）：行内 extend，行循环后统一列式——
        // 避免每命中一次 pending 锁（q6 每行恰命中 1 个，行内批处理反而更慢）。
        let match_columnar = !debug_enabled && self.executor.match_plan_columnar_safe();
        let mut match_rows: Vec<wf_engine::match_engine::MatchedContext> = Vec::new();
        // Records produced by the match/close paths accumulate here and are
        // appended to the pending columnar builder in one lock per
        // ALERT_BATCH_SIZE group (see [`Self::emit_batch`]) — the per-record
        // lock + target lookup was measurable on the q12 close fan-out hot
        // path (emit_nanos dominated the profiling budget).
        let mut staged_outputs: Vec<OutputRecord> = Vec::new();
        // Records produced by the match/close paths accumulate here and are
        // appended to the pending columnar builder in one lock per
        // ALERT_BATCH_SIZE group (see [`Self::emit_batch`]) — the per-record
        // lock + target lookup was measurable on the q12 close fan-out hot
        // path (emit_nanos dominated the profiling budget).
        let mut conv_max_wm: i64 = 0;
        // 批级 join-then-key 预解析（2026-08-23，q4/q6：advance 88.8% 的 join
        // 取键热路径——每 bid 一次索引 lookup + values_equal 复核 + key 字段
        // 物化）。对一批事件按驱动 key 去重 lookup，每行得到预解析 scope key，
        // advance 传入跳过内部每事件解析。非 key_join 规则 → None（原逻辑）。
        let key_join_plan: Option<wf_lang::plan::JoinKeyPlan> = self
            .machine
            .as_ref()
            .and_then(|m| m.plan().key_join.clone());
        let key_overrides: Option<Vec<Option<Vec<wf_engine::match_engine::Value>>>> =
            match (&key_join_plan, batch) {
                (Some(kjp), Some(b)) => Some(precompute_join_then_keys(
                    b,
                    &row_domain.to_vec(),
                    kjp,
                    &lookup,
                )),
                _ => None,
            };
        // Iterate the row domain: `i` is the position within `row_domain`
        // (matches the row-domain-relative `DeferredRows` times/hit_indices),
        // `row_index` is the absolute batch row it maps to.
        // H-1（2026-09-03）：machine 行与 on-each 行在同一批内互斥（self.machine 在
        // process_batch 内恒定，本文件无任何赋值）——把原每行 `if let machine /
        // else if each` 的双路分发上提到行循环外层，两路各持独立循环，machine 深度
        // 交织区（&mut machine + &self.executor + 多收集器）不再与 each 分流共享同一
        // 作用域。machine 仍每行重借（而非整循环持有）：行体内有大量 `&self` 方法
        // 调用（stage_or_emit_record 等），跨行 &mut machine 会导致借用冲突。
        if self.machine.is_some() {
            // H-3（2026-09-03）：machine 行循环整体委托 process_batch_machine_rows——
            // 批级上下文打包 MachineRowsCtx（纯 Copy），lookup / rule_name /
            // ordered_aliases 在方法内自 self 重建（close/match 列式门控同源重算、
            // hit_cursor 为批内局部）；收集器（stats / staged_outputs / conv_closes /
            // columnar_closes / match_rows / conv_max_wm）行循环后 process_batch 尾部
            // 仍要消费，走 &mut 参数。
            let ctx = MachineRowsCtx {
                window_name,
                batch_seq,
                lookup_max_seq,
                batch_emit_nanos,
                debug_enabled,
                row_domain: &row_domain,
                deferred: deferred.as_ref(),
                eager_events: eager_events.as_ref(),
                columnar_masks: &columnar_masks,
                branch_masks: &branch_masks,
                key_overrides: key_overrides.as_deref(),
            };
            self.process_batch_machine_rows(
                &ctx,
                &mut stats,
                &mut staged_outputs,
                &mut conv_closes,
                &mut columnar_closes,
                &mut match_rows,
                &mut conv_max_wm,
            )
            .await;
        } else if let Some(alias) = self
            .each_alias
            .clone()
            .filter(|alias| aliases.iter().any(|candidate| candidate == alias))
        {
            // H-2（2026-09-03）：on-each 行循环整体委托 process_batch_each_rows
            // （本路径 machine 恒 None；lookup 在方法内自 router 重建，调用点
            // 不必持有借自 self.router 的 lookup 跨 &mut self 调用）。
            self.process_batch_each_rows(
                alias.as_str(),
                window_name,
                batch_seq,
                lookup_max_seq,
                batch_emit_nanos,
                debug_enabled,
                &row_domain,
                eager_events.as_ref(),
                deferred_columnar.as_ref(),
                &columnar_masks,
                &each_field_order,
                &mut stats,
                &mut each_direct_rows,
            )
            .await;
        }
        // P2③: one aggregated ConvCloseBatch per batch for conv-sink shards,
        // using the max event-time watermark as the barrier. (Replaces per-event
        // sends — the per-event path saturated the bounded(32) channel.)
        if self.conv_sink.is_some()
            && let Some(sink) = self.conv_sink.as_ref()
        {
            // P3-D: if the conv stage is gone (channel closed), the closes are
            // dropped — log it rather than fail silently.
            let sent = sink
                .tx
                .send(ConvCloseBatch {
                    closes: std::mem::take(&mut conv_closes),
                    watermark: conv_max_wm,
                    drained: false,
                    barrier_index: sink.barrier_index,
                })
                .await;
            if sent.is_err() {
                log::debug!("conv sink channel closed — conv batch dropped");
            }
        }
        // P3：deferred join 到期扫描（本批次事件时间 watermark 已推进）
        if self.deferred.is_some()
            && let Some(wm) = self.deferred.as_ref().map(|d| d.watermark)
        {
            self.scan_deferred(wm, batch_emit_nanos, true).await;
            // D4：到期实例已退场 → 把新的保留前沿发布给 join 目标窗口（批次
            // 级，不在行循环里）。扫描后发布：前沿尽可能向前，窗口尽早释放。
            if let Some(d) = self.deferred.as_mut() {
                d.publish_retention_floor();
            }
        }
        self.log_batch_summary(
            debug_enabled,
            window_name,
            batch_seq,
            rule_name_for_log,
            &stats,
        );
        // Columnar match emit (q6 形态): one pending lock, one target lookup,
        // one columnar batch commit — no per-match OutputRecord. Metrics mirror
        // the per-record path (exact totals; append-failed for eval failures).
        if match_columnar && !match_rows.is_empty() {
            let row_refs: Vec<&wf_engine::match_engine::MatchedContext> =
                match_rows.iter().collect();
            let (outcome, should_flush) = {
                let mut pending = self.pending_alerts.lock().unwrap();
                let builder = pending.builder_for(self.executor.static_yield_target());
                let mut appended_idx = Vec::new();
                let outcome = self.executor.execute_match_direct_batch_columnar(
                    &row_refs,
                    batch_emit_nanos,
                    builder,
                    &mut appended_idx,
                );
                pending.count += outcome.appended;
                (outcome, pending.count >= ALERT_BATCH_SIZE)
            };
            if let Some(metrics) = &self.metrics {
                for _ in 0..outcome.appended {
                    metrics.inc_alert_emitted_total(self.rule_name());
                }
                for _ in 0..outcome.failed {
                    metrics.inc_alert_append_failed();
                }
            }
            if should_flush {
                self.flush_alerts().await;
            }
        }
        // Vectorized close emit for gate-passing rules (L4): one pending lock,
        // one target lookup, one columnar batch commit — no per-close
        // OutputRecord / synthetic ctx. Metrics mirror the per-record path
        // (exact totals; append-failed increments for eval failures).
        if close_columnar && !columnar_closes.is_empty() {
            let _close_exec_start = rule_profiling();
            let (outcome, should_flush) = {
                let mut pending = self.pending_alerts.lock().unwrap();
                let builder = pending.builder_for(self.executor.static_yield_target());
                let outcome = self.executor.execute_close_direct_batch_columnar(
                    &columnar_closes,
                    builder,
                    batch_emit_nanos,
                );
                pending.count += outcome.appended;
                (outcome, pending.count >= ALERT_BATCH_SIZE)
            };
            if let Some(_close_t) = _close_exec_start {
                self.close_exec_nanos += _close_t.elapsed().as_nanos() as u64;
            }
            if let Some(metrics) = &self.metrics {
                for _ in 0..outcome.appended {
                    metrics.inc_alert_emitted_total(self.rule_name());
                }
                for _ in 0..outcome.failed {
                    metrics.inc_alert_append_failed();
                }
            }
            if should_flush {
                self.flush_alerts().await;
            }
        }
        // Deliver any remaining staged outputs (same cadence as the per-event
        // flush — bounds delivery latency to one event batch).
        if !staged_outputs.is_empty() {
            self.emit_batch(std::mem::take(&mut staged_outputs)).await;
        }
        // Deliver any accumulated alert batch (bounds delivery latency to one
        // event batch and flushes test expectations without an explicit EOS).
        self.flush_alerts().await;
        // Same latency bound for staged intermediate (pipe) rows.
        self.flush_pipes().await;
    }

    /// Update the periodic per-rule instance-count gauge.
    ///
    /// P2b: the gauge is the sum across a rule's shards, so each shard reports
    /// the delta since its last report. On drain (flush/EOS) the count drops to
    /// zero and the final delta reconciles the shard's contribution to zero.
    pub(super) fn update_rule_instances_metric(&self) {
        if let Some(metrics) = &self.metrics {
            let rule_name = self.executor.plan().name.as_str();
            let cur = self
                .machine
                .as_ref()
                .map(|machine| machine.instance_count() as i64)
                .unwrap_or(0);
            let last = self.last_reported_instances.swap(cur, Ordering::Relaxed);
            let delta = cur - last;
            if delta != 0 {
                metrics.adjust_rule_instances(rule_name, delta);
            }
        }
    }

    /// Whether every source window's actor has finished its shutdown drain.
    /// A window without an actor (embedded direct-append / provider) reports
    /// drained by default, so the wait below never stalls on it.
    pub(super) fn sources_drained(&self) -> bool {
        self.sources.iter().all(|s| s.window.actor_drained())
    }

    /// Full-shutdown drain: keep pulling until every source window's actor has
    /// committed its queued tail, so the final flush runs against a complete
    /// machine.
    ///
    /// Without this, a rule task can flush — and exit — while the window
    /// actors are still committing their mailbox tail: the flush closes
    /// instances at a stale machine watermark (close:flush alerts with a stale
    /// `fired_at` and tail-triggered alerts lost entirely). That is the
    /// `e2e_datagen_brute_force` CI flake (loaded macos-14 runners: the actor
    /// lags at EOF, the rule task's cancel branch drained an empty channel and
    /// flushed before the actor committed). The poll bound is a safety net for
    /// a stuck actor; normally the drain completes in milliseconds.
    pub(crate) async fn wait_shutdown_drain(&mut self) {
        let deadline = std::time::Instant::now() + SHUTDOWN_DRAIN_TIMEOUT;
        loop {
            self.pull_and_advance().await;
            if self.sources_drained() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                wf_warn!(
                    pipe,
                    task_id = %self.task_id,
                    "shutdown drain timeout — window actor(s) did not report drained; flushing with possibly-stale machine"
                );
                break;
            }
            tokio::time::sleep(SHUTDOWN_DRAIN_POLL).await;
        }
    }

    /// Process a single pushed batch, advancing the per-task push sequence.
    pub(crate) async fn process_push(&mut self, push: RulePush) {
        let seq = self.pushed_seq;
        self.pushed_seq += 1;
        let window_name = push.window_name.clone();
        let push_seq = push.seq;
        self.process_batch(
            window_name.as_ref(),
            seq,
            None,
            push.events.as_ref(),
            push.batch.as_deref(),
            push.shard_rows.as_deref().map(|rows| rows.as_slice()),
            push.materialize_fields.as_deref(),
        )
        .await;
        // Ack the window batch seq so time eviction may reclaim it (the
        // `seq` above is only a per-task debug counter). `fetch_max`:
        // 2026-08-25 q13 中间窗生产者分片后，广播按 append 顺序仍单调，但
        // 并发生产者（多 q13a shard）会让不同 shard 的广播 seq 乱序到达
        // 同一消费者——覆盖写会让 ack 回退（min_acked 倒退、驱逐保护失效的
        // 假象），单调 max 保证 ack 只前进。
        if let Some(slot) = self.progress.get(window_name.as_ref()) {
            slot.fetch_max(
                push_seq.saturating_add(1),
                std::sync::atomic::Ordering::Release,
            );
        }
    }

    /// Consume and process all currently-buffered pushed batches.
    ///
    /// Used by the push loop to drain the channel before a flush (EOS/cancel).
    /// After the source reports EOS no further pushes arrive, so draining via
    /// `try_recv` until empty is complete.
    pub(crate) async fn drain_push_channel(&mut self, rx: &mut mpsc::Receiver<RulePush>) {
        while let Ok(push) = rx.try_recv() {
            self.process_push(push).await;
        }
        self.update_rule_instances_metric();
    }
}
