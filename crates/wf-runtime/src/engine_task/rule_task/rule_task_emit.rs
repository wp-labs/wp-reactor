//! emit 相位家族（rule_task.rs 拆分，2026-09-04）：on-each 列式快路径
//! （`process_batch_columnar_each`）、alert/pipe 逐条与批量 emit
//! （`emit`/`emit_batch`/`emit_each_direct*`）、管道装载与同批收口
//! （`stage_pipe_record`/`flush_pipes`）、列式 alert 批投递（`flush_alerts`）；
//! 输出/调试自由函数（`log_output_emitted`/`output_kind`/`event_time_nanos` 等）
//! 同属。

use super::*;

impl RuleTask {
    /// On-each columnar fast path（2026-08-25 q13a 列式化）：命中行来自
    /// （缺席或列式的）bind-filter 掩码，整批向量化装载/直发——完全跳过行
    /// 循环。`process_batch` 的 columnar_each 分支早退委托（含 pipe 装载的
    /// flush_pipes 同批收口节奏，与行式路径一致）。
    pub(super) async fn process_batch_columnar_each(
        &self,
        batch: &RecordBatch,
        aliases: &[String],
        columnar_masks: &HashMap<String, Option<BooleanArray>>,
        lookup: &RegistryLookup<'_>,
        batch_emit_nanos: i64,
    ) {
        let num_rows = batch.num_rows();
        let mut hit = vec![false; num_rows];
        // 非列式 bind filter 的批级视图索引（逐行解释用，免 per-call
        // schema 线性扫）；无非列式 filter 时不构建。
        let needs_row_filter = aliases.iter().any(|alias| {
            !columnar_masks.contains_key(alias) && self.executor.bind_filter_present(alias)
        });
        let row_index = needs_row_filter.then(|| build_field_index(batch));
        for alias in aliases.iter() {
            match columnar_masks.get(alias) {
                Some(Some(mask)) => {
                    for (row, h) in hit.iter_mut().enumerate() {
                        *h |= mask.value(row);
                    }
                }
                _ => {
                    if self.executor.bind_filter_present(alias) {
                        // 非列式 bind filter（gap-4 2026-09-02）：逐行
                        // `event_matches_alias` 解释（ColumnarEvent 视图直读
                        // 列，与行式路径字节一致）——不再 hit.fill(true)
                        // 静默丢过滤子集。index 缺失（防御）= 无视图，
                        // 走无 index 直读（schema 线性扫，正确性不变）。
                        let index = row_index.as_ref();
                        for (row, h) in hit.iter_mut().enumerate() {
                            if !*h {
                                let ev = match index {
                                    Some(index) => {
                                        ColumnarEvent::with_index(batch, row, Arc::clone(index))
                                    }
                                    None => ColumnarEvent::new(batch, row),
                                };
                                *h = self.executor.event_matches_alias(alias, &ev, Some(lookup));
                            }
                        }
                    } else {
                        hit.fill(true); // 无 filter：全放行（与 event_matches_alias 无 filter 一致）
                    }
                }
            }
        }
        let hit_indices: Vec<u32> = (0..num_rows)
            .filter(|&row| hit[row])
            .map(|row| row as u32)
            .collect();
        let time_col_index = batch_time_col_index(batch, self.each_time_field.as_deref());
        let col_events: Vec<ColumnarEvent<'_>> = hit_indices
            .iter()
            .map(|&row| ColumnarEvent::new(batch, row as usize))
            .collect();
        let rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
            .iter()
            .zip(hit_indices.iter())
            .map(|(ev, &row)| {
                let row = row as usize;
                let event_nanos = time_col_index
                    .map(|col| batch_event_time_nanos_at(batch, col, row))
                    .unwrap_or(0);
                (ev, event_nanos)
            })
            .collect();
        // Metrics parity: the eager path reported the input count before
        // the loop; the unconditional add with 0 is a no-op, so add the
        // real count here.
        if let Some(metrics) = &self.metrics {
            metrics.add_rule_events(self.executor.plan().name.as_str(), rows.len());
        }
        // 列式 each 分流：无活 join（q1 等）走无 join 列式路径；活 join
        // （q20 等，each_join_plan 已解析）走列式 join 富化路径（批级
        // join_lookup + 列式右窗字段读，免每事件 Event clone —— 2026-08-23
        // 列式 join 富化，q20 2.5M/s → 列式量级）。中间管道目标（q13a）
        // 走 pipe 列式装载（2026-08-25 q13a 列式化）。
        if self.executor.live_joins().is_empty() {
            if self.each_direct {
                self.emit_each_direct_batch_columnar(&rows, batch_emit_nanos)
                    .await;
            } else {
                self.emit_each_pipe_batch_columnar(&rows, batch_emit_nanos)
                    .await;
            }
        } else {
            self.emit_each_direct_batch_columnar_join(&rows, lookup, batch_emit_nanos)
                .await;
        }
        // 列式分支早退：pipe 路径已在此装载中间行，必须同批收口（与行式
        // 路径的 flush_pipes 节奏一致）。sink 目标（each_direct）无 pipe
        // 装载——不调用，避免给 q1 等高频规则每批新增一次 pipe_state 锁
        // 争用（2026-08-25 review R2）。
        if !self.each_direct {
            self.flush_pipes().await;
        }
    }

    // -- Alert emission -----------------------------------------------------

    pub(super) async fn emit(&self, record: OutputRecord) {
        if self.intermediate_targets.contains(&*record.yield_target) {
            // 2026-08-23 q4：intermediate 输出也计入 `emitted_total`——
            // verify-nexmark 读 EMIT 对拍，中间窗口行数（q4a→auction_finals
            // 的输出量）是内层语义的体现，不计则 verify 读到 0（oracle 557,204）。
            // alert detail/e2e 不采样（intermediate 非最终告警）。
            if let Some(metrics) = &self.metrics {
                metrics.inc_alert_emitted_total(&record.rule_name);
            }
            // perf-diag cut_output 门控：emitted 计数保留，跳过 pipe 装载。
            if crate::perf_diag::perf_cut_output() {
                return;
            }
            self.stage_pipe_record(record);
            return;
        }
        if let Some(metrics) = &self.metrics {
            // Exact total is cheap (one relaxed atomic); the allocation-heavy
            // detail map + e2e histogram are sampled 1-in-N (batch).
            metrics.inc_alert_emitted_total(&record.rule_name);
            let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
            let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
            if sample == 0 {
                self.emit_sample_remaining
                    .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                metrics.inc_alert_emitted_detail(
                    &record.rule_name,
                    &record.machine_id,
                    &record.scope_key,
                );
                let e2e_nanos = now_nanos.saturating_sub(record.event_time_nanos.max(0) as u64);
                metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
            } else {
                self.emit_sample_remaining
                    .store(sample - 1, Ordering::Relaxed);
            }
        }
        // perf-diag cut_output 门控：emitted 计数已保留（上面），跳过
        // record→列构建/通道/sink 物化+序列化+写——输出链整体直通。
        if crate::perf_diag::perf_cut_output() {
            return;
        }
        // 输出链消融（2026-08-26）：只切 sink alert 构建，保留 pipe/join 消费。
        // 与 cut_output 的区别见 perf_diag::perf_cut_alert 注释——CEP close/match
        // 路径（q12 式）同款门控；stats 规则（q19 式）走 StatsTask 的列式装载/
        // emit_close_record 门控。intermediate 目标在上面已 return（pipe 写入不受
        // 影响）。emitted 计数已保留（上面）。
        if crate::perf_diag::perf_cut_alert() {
            return;
        }
        // Append straight into the per-target columnar batch, sealed and
        // flushed to the sink writers when it fills (amortizing the
        // per-alert fan-out mechanics, matching the wp-motor batch model).
        // The conversion stays on this thread on purpose: records allocated
        // here and freed on a sink thread drive mimalloc into its
        // abandoned-page reclaim path — measured ~2x rule-throughput loss.
        //
        // Append timing is sampled 1-in-`EMIT_METRIC_SAMPLE_INTERVAL` and
        // scaled back up (same sampling pattern as the e2e metrics): two
        // clock_gettime calls per record measured ~2.5% of on-CPU samples,
        // and the per-record timing only feeds diagnostics, not semantics.
        // (The metric covers the record→columns append, the successor of the
        // old to_data_record conversion.)
        let time_this = {
            let rem = self.append_sample_remaining.fetch_sub(1, Ordering::Relaxed);
            if rem == 1 {
                self.append_sample_remaining
                    .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                true
            } else {
                false
            }
        };
        let _append_start = time_this.then(Instant::now);
        let (append_result, should_flush) = {
            let mut pending = self.pending_alerts.lock().unwrap();
            let builder = pending.builder_for(&record.yield_target);
            let result = builder.append_record(&record);
            if result.is_ok() {
                pending.count += 1;
            }
            (result, pending.count >= ALERT_BATCH_SIZE)
        };
        if let Err(e) = append_result {
            if let Some(metrics) = &self.metrics {
                metrics.inc_alert_append_failed();
            }
            log::warn!("alert export error: {e}");
            return;
        }
        if let Some(start) = _append_start {
            let elapsed = start.elapsed().as_nanos() as u64;
            let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64;
            self.append_nanos.fetch_add(scaled, Ordering::Relaxed);
            if let Some(metrics) = &self.metrics {
                metrics.add_alert_append_nanos(scaled);
            }
        }
        if should_flush {
            self.flush_alerts().await;
        }
    }

    /// Batch twin of [`Self::emit`]: append a whole group of already-built
    /// records to the per-target columnar builder under **one** pending lock
    /// and one target lookup, flushing when the pending batch fills. Records
    /// are appended in order; telemetry is exact (same counters as
    /// [`Self::emit`]); the append timing covers the whole group and is
    /// sampled 1-in-`EMIT_METRIC_SAMPLE_INTERVAL` (scaled by group size) —
    /// same diagnostic shape as the per-record sampler.
    ///
    /// The q12-style close/match fan-out emits one record per closed window;
    /// per-record lock + target lookup + await-poll was measurable on the
    /// profiling hot path (emit_nanos dominated the q12 batch budget), while
    /// the append itself is a Vec push per column.
    pub(super) async fn emit_batch(&self, records: Vec<OutputRecord>) {
        let n = records.len();
        if n == 0 {
            return;
        }
        // Exact totals + sampled detail/e2e — identical accounting to
        // [`Self::emit`] (the sampler state lives on the rule task, so the
        // cadence is unchanged whether records arrive one-by-one or in a group).
        if let Some(metrics) = &self.metrics {
            let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
            for record in &records {
                metrics.inc_alert_emitted_total(&record.rule_name);
                let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
                if sample == 0 {
                    self.emit_sample_remaining
                        .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                    metrics.inc_alert_emitted_detail(
                        &record.rule_name,
                        &record.machine_id,
                        &record.scope_key,
                    );
                    let e2e_nanos = now_nanos.saturating_sub(record.event_time_nanos.max(0) as u64);
                    metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
                } else {
                    self.emit_sample_remaining
                        .store(sample - 1, Ordering::Relaxed);
                }
            }
        }
        // Split off intermediate (pipe) targets — same relay semantics as the
        // per-record path, before any sink append.
        let mut pipe_records = Vec::new();
        let mut sink_records: Vec<OutputRecord> = Vec::with_capacity(n);
        for record in records {
            if self.intermediate_targets.contains(&*record.yield_target) {
                pipe_records.push(record);
            } else {
                sink_records.push(record);
            }
        }
        // perf-diag cut_output 门控：emitted 计数已保留，pipe/sink 输出链直通。
        if crate::perf_diag::perf_cut_output() {
            return;
        }
        for record in pipe_records {
            self.stage_pipe_record(record);
        }
        if sink_records.is_empty() {
            return;
        }
        // 输出链消融（2026-08-26）：只切 sink alert 构建，保留 pipe/join 消费。
        // 与 emit 的 cut_alert 同款（CEP close/match 批量路径）；emitted 计数已
        // 保留（上面），pipe 已装载（上面）。
        if crate::perf_diag::perf_cut_alert() {
            return;
        }
        let time_this = {
            let rem = self.append_sample_remaining.fetch_sub(1, Ordering::Relaxed);
            if rem == 1 {
                self.append_sample_remaining
                    .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                true
            } else {
                false
            }
        };
        let _append_start = time_this.then(Instant::now);
        let should_flush = {
            let mut pending = self.pending_alerts.lock().unwrap();
            let builder = pending.builder_for(&sink_records[0].yield_target);
            let mut failed = 0usize;
            for record in &sink_records {
                if builder.append_record(record).is_err() {
                    failed += 1;
                }
            }
            pending.count += sink_records.len() - failed;
            if failed > 0
                && let Some(metrics) = &self.metrics
            {
                for _ in 0..failed {
                    metrics.inc_alert_append_failed();
                }
            }
            pending.count >= ALERT_BATCH_SIZE
        };
        if let Some(start) = _append_start {
            // Sampled 1-in-64 *batches* (the sampler decrements once per group),
            // so the report scales the group's append time by
            // EMIT_METRIC_SAMPLE_INTERVAL only — multiplying by the group size
            // as well double-counted (group duration already covers all n rows).
            let elapsed = start.elapsed().as_nanos() as u64;
            let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64;
            self.append_nanos.fetch_add(scaled, Ordering::Relaxed);
            if let Some(metrics) = &self.metrics {
                metrics.add_alert_append_nanos(scaled);
            }
        }
        if should_flush {
            self.flush_alerts().await;
        }
    }

    /// Accumulate one produced record into `staged`, draining through
    /// [`Self::emit_batch`] once the group reaches [`ALERT_BATCH_SIZE`] — keeps
    /// the flush cadence and pending memory bound of the per-record path.
    pub(super) async fn stage_or_emit_record(
        &self,
        staged: &mut Vec<OutputRecord>,
        record: OutputRecord,
    ) {
        staged.push(record);
        if staged.len() >= ALERT_BATCH_SIZE {
            self.emit_batch(std::mem::take(staged)).await;
        }
    }

    /// Direct-write on-each emit (plan C2): the executor evaluates the event
    /// and appends the row straight into the per-target columnar builder —
    /// no per-record `OutputRecord` materialization. Mirrors [`Self::emit`]'s
    /// telemetry (exact totals, 1-in-N sampled detail/e2e, sampled append
    /// timing) and batch-flush trigger.
    ///
    /// One diagnostic difference from the record path: the sampled detail's
    /// machine id is extracted from the pre-join event (joins that rebind
    /// the machine-id field would show a different label). Only affects the
    /// metric label, not semantics.
    pub(super) async fn emit_each_direct(
        &self,
        event: &Event,
        event_nanos: i64,
        lookup: &RegistryLookup<'_>,
        field_order: &[&smol_str::SmolStr],
        batch_emit_nanos: i64,
    ) -> wf_engine::error::CoreResult<bool> {
        // perf-diag cut_output 门控：on-each 直接写路径在 append 前整体直通
        // （该路径的 emitted 计数与 append 耦合，无法保留计数而切 append）。
        if crate::perf_diag::perf_cut_output() {
            return Ok(false);
        }
        // 输出链消融（2026-08-26）：只切 alert 构建，保留 pipe/join 消费。
        // 与 cut_output 的区别见 perf_diag::perf_cut_alert 注释。
        if crate::perf_diag::perf_cut_alert() {
            return Ok(false);
        }
        // Append timing is sampled 1-in-N and scaled back up (same
        // pattern as `emit`; covers the eval + column append).
        let time_this = {
            let rem = self.append_sample_remaining.fetch_sub(1, Ordering::Relaxed);
            if rem == 1 {
                self.append_sample_remaining
                    .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                true
            } else {
                false
            }
        };
        let _append_start = time_this.then(Instant::now);
        let (result, should_flush) = {
            let mut pending = self.pending_alerts.lock().unwrap();
            let builder = pending.builder_for(self.executor.static_yield_target());
            let result = self.executor.execute_each_direct(
                event,
                event_nanos,
                lookup,
                field_order,
                batch_emit_nanos,
                builder,
            );
            if let Ok(true) = &result {
                pending.count += 1;
            }
            (result, pending.count >= ALERT_BATCH_SIZE)
        };
        if let Ok(true) = &result {
            if let Some(metrics) = &self.metrics {
                // Exact total is cheap; the allocation-heavy detail map +
                // e2e histogram are sampled 1-in-N.
                metrics.inc_alert_emitted_total(self.rule_name());
                let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
                let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
                if sample == 0 {
                    self.emit_sample_remaining
                        .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                    metrics.inc_alert_emitted_detail(
                        self.rule_name(),
                        &RuleExecutor::machine_id_of(event),
                        self.rule_name(),
                    );
                    let e2e_nanos = now_nanos.saturating_sub(event_nanos.max(0) as u64);
                    metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
                } else {
                    self.emit_sample_remaining
                        .store(sample - 1, Ordering::Relaxed);
                }
            }
        } else if let Err(e) = &result {
            if let Some(metrics) = &self.metrics {
                metrics.inc_alert_append_failed();
            }
            log::warn!("alert export error: {e}");
        }
        if let Some(start) = _append_start {
            let elapsed = start.elapsed().as_nanos() as u64;
            let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64;
            self.append_nanos.fetch_add(scaled, Ordering::Relaxed);
            if let Some(metrics) = &self.metrics {
                metrics.add_alert_append_nanos(scaled);
            }
        }
        if should_flush {
            self.flush_alerts().await;
        }
        result
    }

    /// Batched direct-write on-each emit (build_each_direct vectorization):
    /// runs [`RuleExecutor::execute_each_direct_batch`] over the events the
    /// main loop collected for this rule, in segments of `ALERT_BATCH_SIZE`
    /// events so the flush cadence and the pending-alerts memory bound stay
    /// identical to the per-event path.
    ///
    /// Telemetry mirrors [`Self::emit_each_direct`]: exact `emitted_total`
    /// per appended row (via the appended-index list, outside the builder
    /// lock), 1-in-N sampled detail/e2e per appended row, and append
    /// timing sampled per segment and scaled by the per-call average (a
    /// segment covers many "calls", so the scaled estimate stays comparable
    /// to the per-event path's accounting).
    pub(super) async fn emit_each_direct_batch(
        &self,
        rows: &[(&wf_engine::match_engine::Event, i64)],
        lookup: &RegistryLookup<'_>,
        field_order: &[&smol_str::SmolStr],
        batch_emit_nanos: i64,
    ) {
        // perf-diag cut_output 门控（见 [`Self::emit_each_direct`]）。
        if crate::perf_diag::perf_cut_output() {
            return;
        }
        let mut appended_idx: Vec<usize> = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let end = (start + ALERT_BATCH_SIZE).min(rows.len());
            let segment = &rows[start..end];
            let calls = segment.len();
            let time_this = {
                let rem = self.append_sample_remaining.fetch_sub(1, Ordering::Relaxed);
                if rem == 1 {
                    self.append_sample_remaining
                        .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            };
            let _append_start = time_this.then(Instant::now);
            let (outcome, should_flush) = {
                let mut pending = self.pending_alerts.lock().unwrap();
                let builder = pending.builder_for(self.executor.static_yield_target());
                let outcome = self.executor.execute_each_direct_batch(
                    segment,
                    lookup,
                    field_order,
                    batch_emit_nanos,
                    builder,
                    &mut appended_idx,
                );
                pending.count += outcome.appended;
                (outcome, pending.count >= ALERT_BATCH_SIZE)
            };
            // Per-row telemetry outside the builder lock (exact totals,
            // 1-in-N sampled detail/e2e — same accounting as the per-event
            // path).
            if let Some(metrics) = &self.metrics {
                for &idx in appended_idx.iter() {
                    metrics.inc_alert_emitted_total(self.rule_name());
                    let (event, event_nanos) = segment[idx];
                    let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
                    let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
                    if sample == 0 {
                        self.emit_sample_remaining
                            .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                        metrics.inc_alert_emitted_detail(
                            self.rule_name(),
                            &RuleExecutor::machine_id_of(event),
                            self.rule_name(),
                        );
                        let e2e_nanos = now_nanos.saturating_sub(event_nanos.max(0) as u64);
                        metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
                    } else {
                        self.emit_sample_remaining
                            .store(sample - 1, Ordering::Relaxed);
                    }
                }
                for _ in 0..outcome.failed {
                    metrics.inc_alert_append_failed();
                }
            }
            if let Some(append_start) = _append_start {
                let elapsed = append_start.elapsed().as_nanos() as u64;
                // A segment covers `calls` per-event "calls"; scale the
                // sampled segment time back to the per-call average × the
                // sample interval so the accumulator stays comparable with
                // the per-event path's accounting.
                let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64 / calls.max(1) as u64;
                self.append_nanos.fetch_add(scaled, Ordering::Relaxed);
                if let Some(metrics) = &self.metrics {
                    metrics.add_alert_append_nanos(scaled);
                }
            }
            if should_flush {
                self.flush_alerts().await;
            }
            start = end;
        }
    }

    /// Columnar twin of [`Self::emit_each_direct_batch`]: same flush cadence /
    /// pending bound / telemetry accounting, but the executor reads field
    /// values straight from the Arrow columns via [`ColumnarEvent`] (no
    /// per-row `Event` materialization). Caller gates on
    /// `each_plan_columnar_safe()`.
    pub(super) async fn emit_each_direct_batch_columnar(
        &self,
        rows: &[(&ColumnarEvent<'_>, i64)],
        batch_emit_nanos: i64,
    ) {
        // perf-diag cut_output 门控（见 [`Self::emit_each_direct`]）。
        if crate::perf_diag::perf_cut_output() {
            return;
        }
        // 批级列式状态（general-yield cvecs + each-filter 掩码）每帧求值一次，
        // 各段复用——逐段对整帧重算是 O(帧×段)（Q14 65k 帧 × 16 段 4600 ns/evt）。
        let Some((first, _)) = rows.first() else {
            return;
        };
        let prepared = self.executor.each_batch_prepare(first.batch());
        let mut appended_idx: Vec<usize> = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let end = (start + ALERT_BATCH_SIZE).min(rows.len());
            let segment = &rows[start..end];
            let calls = segment.len();
            let time_this = {
                let rem = self.append_sample_remaining.fetch_sub(1, Ordering::Relaxed);
                if rem == 1 {
                    self.append_sample_remaining
                        .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            };
            let _append_start = time_this.then(Instant::now);
            let (outcome, should_flush) = {
                let mut pending = self.pending_alerts.lock().unwrap();
                let builder = pending.builder_for(self.executor.static_yield_target());
                let outcome = self.executor.execute_each_direct_batch_columnar_with(
                    segment,
                    batch_emit_nanos,
                    &prepared,
                    builder,
                    &mut appended_idx,
                );
                pending.count += outcome.appended;
                (outcome, pending.count >= ALERT_BATCH_SIZE)
            };
            // Per-row telemetry outside the builder lock — same accounting as
            // the Event-based batch path; the machine_id comes from the column.
            if let Some(metrics) = &self.metrics {
                for &idx in appended_idx.iter() {
                    metrics.inc_alert_emitted_total(self.rule_name());
                    let (event, event_nanos) = segment[idx];
                    let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
                    let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
                    if sample == 0 {
                        self.emit_sample_remaining
                            .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                        metrics.inc_alert_emitted_detail(
                            self.rule_name(),
                            &event.field_value_str(wf_engine::match_engine::MACHINE_ID),
                            self.rule_name(),
                        );
                        let e2e_nanos = now_nanos.saturating_sub(event_nanos.max(0) as u64);
                        metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
                    } else {
                        self.emit_sample_remaining
                            .store(sample - 1, Ordering::Relaxed);
                    }
                }
                for _ in 0..outcome.failed {
                    metrics.inc_alert_append_failed();
                }
            }
            if let Some(append_start) = _append_start {
                let elapsed = append_start.elapsed().as_nanos() as u64;
                let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64 / calls.max(1) as u64;
                self.append_nanos.fetch_add(scaled, Ordering::Relaxed);
                if let Some(metrics) = &self.metrics {
                    metrics.add_alert_append_nanos(scaled);
                }
            }
            if should_flush {
                self.flush_alerts().await;
            }
            start = end;
        }
    }

    /// Columnar join-enrichment emit (2026-08-23): [`Self::emit_each_direct_batch_columnar`]
    /// for the live-join case — same batching/telemetry/flush, but the executor
    /// runs the batch-level join lookup + columnar right-window reads
    /// (`execute_each_direct_batch_columnar_join`). The per-row telemetry's
    /// machine_id still comes from the driving event column.
    pub(super) async fn emit_each_direct_batch_columnar_join(
        &self,
        rows: &[(&ColumnarEvent<'_>, i64)],
        lookup: &RegistryLookup<'_>,
        batch_emit_nanos: i64,
    ) {
        // perf-diag cut_output 门控（见 [`Self::emit_each_direct`]）。
        if crate::perf_diag::perf_cut_output() {
            return;
        }
        // 输出链消融（2026-08-26）：q13b 列式 join 的 alert 构建段。
        if crate::perf_diag::perf_cut_alert() {
            return;
        }
        let mut appended_idx: Vec<usize> = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let end = (start + ALERT_BATCH_SIZE).min(rows.len());
            let segment = &rows[start..end];
            let calls = segment.len();
            let time_this = {
                let rem = self.append_sample_remaining.fetch_sub(1, Ordering::Relaxed);
                if rem == 1 {
                    self.append_sample_remaining
                        .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            };
            let _append_start = time_this.then(Instant::now);
            let (outcome, should_flush) = {
                let mut pending = self.pending_alerts.lock().unwrap();
                let builder = pending.builder_for(self.executor.static_yield_target());
                let outcome = self.executor.execute_each_direct_batch_columnar_join(
                    segment,
                    lookup,
                    batch_emit_nanos,
                    builder,
                    &mut appended_idx,
                );
                pending.count += outcome.appended;
                (outcome, pending.count >= ALERT_BATCH_SIZE)
            };
            // Per-row telemetry outside the builder lock — same accounting as
            // the join-free columnar path; machine_id comes from the column.
            if let Some(metrics) = &self.metrics {
                for &idx in appended_idx.iter() {
                    metrics.inc_alert_emitted_total(self.rule_name());
                    let (event, event_nanos) = segment[idx];
                    let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
                    let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
                    if sample == 0 {
                        self.emit_sample_remaining
                            .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                        metrics.inc_alert_emitted_detail(
                            self.rule_name(),
                            &event.field_value_str(wf_engine::match_engine::MACHINE_ID),
                            self.rule_name(),
                        );
                        let e2e_nanos = now_nanos.saturating_sub(event_nanos.max(0) as u64);
                        metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
                    } else {
                        self.emit_sample_remaining
                            .store(sample - 1, Ordering::Relaxed);
                    }
                }
                for _ in 0..outcome.failed {
                    metrics.inc_alert_append_failed();
                }
            }
            if let Some(append_start) = _append_start {
                let elapsed = append_start.elapsed().as_nanos() as u64;
                let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64 / calls.max(1) as u64;
                self.append_nanos.fetch_add(scaled, Ordering::Relaxed);
                if let Some(metrics) = &self.metrics {
                    metrics.add_alert_append_nanos(scaled);
                }
            }
            if should_flush {
                self.flush_alerts().await;
            }
            start = end;
        }
    }

    /// Columnar on-each emit for **intermediate pipe targets** (q13a 等
    /// each→pipe，2026-08-25 q13a 列式化）：批级求值（`each_batch_prepare`
    /// 一次）→ 每行 yield 值（`execute_each_pipe_batch_columnar`，零 Event/
    /// OutputRecord 物化）→ 直接装入 `PipeBatchStager` 类型列。装载节奏与
    /// 行式路径一致（每输入批一次 flush_pipes）。
    ///
    /// 注：不采样 append 计时（pipe 装载路径不生成 OutputRecord，与
    /// `flush_pipes` 的批构建/广播共享计时口径）。
    pub(super) async fn emit_each_pipe_batch_columnar(
        &self,
        rows: &[(&ColumnarEvent<'_>, i64)],
        batch_emit_nanos: i64,
    ) {
        if crate::perf_diag::perf_cut_output() {
            return;
        }
        let Some((first, _)) = rows.first() else {
            return;
        };
        let prepared = self.executor.each_batch_prepare(first.batch());
        // 行式路径在 stage_pipe_record 的 Uninit 分支解析形状并建 stager；
        // 列式路径同样惰性解析，但用 new_columnar 预计算列来源计划。
        let yield_names: Vec<Arc<str>> = self
            .executor
            .plan()
            .yield_plan
            .fields
            .iter()
            .map(|f| Arc::from(f.name.as_str()))
            .collect();
        let rule_name = self.rule_name();
        // 2026-08-25（pipe 写入分配足迹）：**先备好 stager，再把它当 sink 传进
        // executor 流式装载**——不再先物化整批 `Vec<PipeEachRow>`（每行一个
        // values Vec + 一个 entity_id String，实测 404 B/行）。锁在求值期间持有：
        // `pipe_state` 是本 RuleTask 独占（分片各自一份，非 Arc 共享），无争用；
        // 锁内全同步（无 await）。
        let mut guard = self.pipe_state.lock().unwrap();
        if matches!(&*guard, PipeState::Uninit) {
            let target = self.executor.static_yield_target().clone();
            match resolve_pipe_shape(&self.pipe_registry, &self.router, &target) {
                Some((schema, time_col_index)) => {
                    *guard = PipeState::Staging(PipeBatchStager::new_columnar(
                        target,
                        schema,
                        time_col_index,
                        &yield_names,
                    ));
                }
                None => {
                    wf_warn!(
                        pipe,
                        task_id = %self.task_id,
                        rule = %rule_name,
                        target = %target,
                        output_kind = "intermediate",
                        reason = "missing_internal_window",
                        "missing internal pipeline window"
                    );
                    *guard = PipeState::Dead;
                }
            }
        }
        let stats = match &mut *guard {
            PipeState::Staging(stager) => {
                let mut sink = PipeStagerSink {
                    stager,
                    rule_name,
                    errors: 0,
                };
                let stats = self
                    .executor
                    .execute_each_pipe_batch_columnar(rows, &prepared, &mut sink);
                if sink.errors > 0 {
                    wf_warn!(
                        pipe,
                        task_id = %self.task_id,
                        rule = %rule_name,
                        output_kind = "intermediate",
                        rows = sink.errors,
                        "stage internal pipeline row failed (columnar)"
                    );
                }
                stats
            }
            // Dead（缺中间窗）/ 其他：不装载，也不计发射数。
            _ => wf_engine::match_engine::EachDirectBatchStats::default(),
        };
        drop(guard);
        if let Some(metrics) = &self.metrics {
            for _ in 0..stats.appended {
                metrics.inc_alert_emitted_total(self.rule_name());
            }
            for _ in 0..stats.failed {
                metrics.inc_alert_append_failed();
            }
        }
        let _ = batch_emit_nanos;
    }

    /// Flush the accumulated columnar alert batches to the sink writers,
    /// grouped by yield_target. Each sink receives one `AlertBatch` (a single
    /// channel send) of columnar records, amortizing the per-alert resolve /
    /// try_send / blocking that dominated the q1 pass-through emit path.
    pub(super) async fn flush_alerts(&self) {
        // Builder-lifetime optimization: only the sealed columns leave the
        // pending slot — the `AlertColumnBuilder` itself stays resident for
        // the rule task's lifetime (its `staged` buffer keeps its capacity;
        // the layout cache is re-resolved on the next first row, see
        // `finish()`). Previously the whole pending (builder included) was
        // taken and dropped every flush, re-instantiating the builder every
        // ALERT_BATCH_SIZE rows.
        let batches: Vec<(Arc<str>, AlertColumnBatch)> = {
            let mut guarded = self.pending_alerts.lock().unwrap();
            if guarded.count == 0 {
                return;
            }
            guarded.count = 0;
            guarded
                .by_target
                .iter_mut()
                .map(|(target, builder)| (Arc::clone(target), builder.finish()))
                .collect()
        };
        let _fan_start = Instant::now();
        for (target, batch) in batches {
            let records_len = batch.len();
            let sink_groups = self.sink_fanout.resolve(&target);
            if sink_groups.is_empty() {
                if let Some(metrics) = &self.metrics {
                    metrics.add_alert_no_sink_records(records_len as u64);
                }
                self.sink_fanout.warn_if_no_sink(&target);
                continue;
            }
            let batch = crate::alert_task::AlertBatch::Columns(Arc::new(batch));
            for (sink_ptr, channels) in sink_groups.iter() {
                // Round-robin across this sink's parallel writers.
                let idx = self.sink_fanout.next_index(*sink_ptr, channels.len());
                let tx = &channels[idx];
                match tx.try_send(batch.clone()) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(batch)) => {
                        if let Some(metrics) = &self.metrics {
                            metrics.inc_alert_channel_full();
                        }
                        // Fall back to blocking send (backpressure).
                        if let Err(e) = tx.send(batch).await {
                            if let Some(metrics) = &self.metrics {
                                metrics.inc_alert_channel_send_failed();
                            }
                            wf_warn!(pipe, error = %e, "alert channel closed");
                        }
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        // Channel is closed — drop the batch
                        if let Some(metrics) = &self.metrics {
                            metrics.inc_alert_channel_send_failed();
                        }
                        wf_warn!(pipe, rule = %target, "alert channel closed, dropping alert batch");
                    }
                }
            }
        }
        self.fanout_nanos
            .fetch_add(_fan_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    /// Stage an intermediate-target row into the columnar pipe buffer
    /// (rule-side channelization). [`Self::flush_pipes`] turns the staged
    /// rows into one batch + one fanout broadcast at the end of the input
    /// batch — the relay semantics of the old per-row `emit_window_record`
    /// (pure relay, no window store, seq `u64::MAX`) with the per-row Arrow
    /// assembly and channel sends amortized away.
    pub(super) fn stage_pipe_record(&self, record: OutputRecord) {
        let mut guard = self.pipe_state.lock().unwrap();
        match &mut *guard {
            PipeState::Dead => {}
            PipeState::Staging(stager) => {
                if let Err(e) = stager.push_record_columnar(&record) {
                    wf_warn!(
                        pipe,
                        task_id = %self.task_id,
                        rule = %record.rule_name,
                        target = %record.yield_target,
                        output_kind = "intermediate",
                        error = %e,
                        "stage internal pipeline row failed"
                    );
                }
            }
            PipeState::Uninit => {
                // Resolve the pipe shape once, lazily (pipe registry schema
                // first, window fallback — same resolution order and failure
                // semantics as the old per-row path).
                let target = Arc::clone(&record.yield_target);
                match resolve_pipe_shape(&self.pipe_registry, &self.router, &target) {
                    Some((schema, time_col_index)) => {
                        // 2026-08-26 q4a：列式装载（new_columnar 预计算列来源
                        // 计划 + push_record_columnar）——deferred 中间窗产量
                        // 大（q4a 每 auction 一条），行式 record_window_fields
                        // 的 clone+HashSet+meta Arc::from 每行分配是 staging
                        // 主成本（q13a row path stage≈476ns/行同源）。
                        let yield_names: Vec<Arc<str>> = self
                            .executor
                            .plan()
                            .yield_plan
                            .fields
                            .iter()
                            .map(|f| Arc::from(f.name.as_str()))
                            .collect();
                        let mut stager = PipeBatchStager::new_columnar(
                            target,
                            schema,
                            time_col_index,
                            &yield_names,
                        );
                        if let Err(e) = stager.push_record_columnar(&record) {
                            wf_warn!(
                                pipe,
                                task_id = %self.task_id,
                                rule = %record.rule_name,
                                output_kind = "intermediate",
                                error = %e,
                                "stage internal pipeline row failed"
                            );
                        }
                        *guard = PipeState::Staging(stager);
                    }
                    None => {
                        wf_warn!(
                            pipe,
                            task_id = %self.task_id,
                            rule = %record.rule_name,
                            target = %target,
                            output_kind = "intermediate",
                            reason = "missing_internal_window",
                            "missing internal pipeline window"
                        );
                        *guard = PipeState::Dead;
                    }
                }
            }
        }
    }

    /// Flush staged intermediate rows: build one N-row `RecordBatch` and hand
    /// it to the pipe's downstream-rule subscribers with a single broadcast.
    /// Called at the end of every input batch (and on timeout/flush emissions),
    /// so delivery latency is bounded exactly like the batched sink-alert
    /// delivery.
    ///
    /// 2026-08-25 q13 分片内存：广播按订阅类型裁剪——
    /// - **RoundRobin-only 订阅**（stateless `on each` 分片消费者，列式安全）
    ///   或**无订阅**：广播 batch-only（`take_batch` 不物化 events）。物化的
    ///   events（36.5k Event ≈ 18MB/批）只增加分片积压在途（q13a 分片放开后
    ///   RSS 28.8GB 平台期主因）；下游从 raw batch 列式读（或自行物化），
    ///   窗口读者（q4b stats）从窗口读，都不需要生产者侧物化。
    /// - **存在 Single/Sharded 订阅**（row-path 中间窗消费者，测试契约依赖
    ///   `RulePush::events`）：保留 events（`take_events` + `broadcast_with_batch`）。
    pub(super) async fn flush_pipes(&self) {
        let built: Option<PipeFlushBatch> = {
            let mut guard = self.pipe_state.lock().unwrap();
            match &mut *guard {
                PipeState::Staging(stager) => {
                    // 决策在 take 之前：round_robin_only 只看 fanout 表，
                    // 不需要 pipe_state 之外的锁。
                    let batch_only = self.router.fanout().round_robin_only(&stager.target);
                    let res = if batch_only {
                        stager
                            .take_batch()
                            .map(|b| b.map(|(target, batch)| (target, None, batch)))
                    } else {
                        stager
                            .take_events()
                            .map(|e| e.map(|(target, events, batch)| (target, Some(events), batch)))
                    };
                    match res {
                        Ok(built) => built,
                        Err(e) => {
                            wf_warn!(
                                pipe,
                                task_id = %self.task_id,
                                output_kind = "intermediate",
                                error = %e,
                                "build internal pipeline batch failed, dropping staged rows"
                            );
                            None
                        }
                    }
                }
                _ => None,
            }
        };
        if let Some((target, events, batch)) = built {
            // 2026-08-23 q4 修复：pipe relay 若只广播（纯 relay，无窗口存储），
            // **pull 模式**的列式下游（stats 任务从窗口读）收不到——
            // q4a→auction_finals→q4b(stats) 默认 pull 双规则链断链（q4b EMIT=0）。
            // 修复：append 到目标窗口（带分片行子集，供 pull 分片消费方读）+
            // 广播（带 batch，供 push 消费方收）。两者共享同一批次，无复制。
            if let Some(win) = self.router.registry().get_window(target.as_ref()) {
                let shard_rows = self
                    .router
                    .fanout()
                    .precompute_shard_rows(target.as_ref(), &batch);
                // 2026-08-23 q13：广播带**真实窗口批次 seq**（append 返回）——
                // 此前固定 u64::MAX 使下游 push 规则的 ack 不反映真实消费进度，
                // 窗口 acked_lag 恒 0，bench 完成判定（等待 lag 归零）在中间
                // 管道下游未消费完时就 SIGTERM（q13b 只处理 2/25 批）。
                let seq = win
                    .append_with_watermark_sized(
                        batch.clone(),
                        wf_engine::window::content_bytes(&batch),
                        shard_rows.map(|s| {
                            let v: Vec<Vec<u32>> = s.iter().cloned().collect();
                            std::sync::Arc::new(v)
                        }),
                    )
                    .map(|(_, seq)| seq)
                    .unwrap_or(0);
                // 2026-08-23 q13：直接 append（不走窗口 actor）不触发窗口 Notify——
                // pull 模型下游（bind 中间窗口的 rule_task）靠 Notify 唤醒，漏通知
                // 则消费停滞（q13b 只处理已拉取的部分，EMIT 严重不足）。append 后
                // 显式 notify_waiters，与 actor 路径的通知语义对齐。
                if let Some(notifier) = self.router.registry().get_notifier(target.as_ref()) {
                    notifier.notify_waiters();
                }
                let fan_start = Instant::now();
                // 2026-08-25：广播按订阅类型裁剪（见 flush_pipes 头注释）——
                // RoundRobin-only/无订阅时 batch-only（不携带物化 events，分片
                // 积压内存主因）；存在 Single/Sharded 订阅时保留 events（row-path
                // 中间窗契约）。真实窗口 seq（append 返回）保持：下游 ack 反映
                // 真实消费进度。
                match events {
                    Some(events) => {
                        self.router
                            .fanout()
                            .broadcast_with_batch(&target, &events, &batch, None, seq)
                            .await;
                    }
                    None => {
                        self.router
                            .fanout()
                            .broadcast_batch_only(&target, &batch, None, None, seq)
                            .await;
                    }
                }
                self.fanout_nanos
                    .fetch_add(fan_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        }
    }
}

pub(super) fn event_debug_ref(
    event: &wf_engine::match_engine::Event,
    batch_seq: u64,
    row_index: usize,
) -> String {
    event
        .fields
        .get("event_id")
        .or_else(|| event.fields.get(WFU_ID))
        .or_else(|| event.fields.get("id"))
        .map(value_debug_string)
        .unwrap_or_else(|| format!("batch:{batch_seq}/row:{row_index}"))
}

/// Debug rendering for a [`RowEvent`]: the Eager arm delegates to the event's
/// fields; the Columnar arm has no materialized fields and is mutually
/// exclusive with debug detail (deferral requires `!debug_enabled`).
pub(super) fn row_event_debug_ref(ev: &RowEvent<'_>, batch_seq: u64, row_index: usize) -> String {
    match ev {
        RowEvent::Eager(e) => event_debug_ref(e, batch_seq, row_index),
        RowEvent::Columnar(_) => format!("batch:{batch_seq}/row:{row_index}"),
    }
}

pub(super) fn value_debug_string(value: &wf_engine::match_engine::Value) -> String {
    match value {
        wf_engine::match_engine::Value::Number(value) => value.to_string(),
        wf_engine::match_engine::Value::Str(value) => value.to_string(),
        wf_engine::match_engine::Value::Bool(value) => value.to_string(),
        wf_engine::match_engine::Value::Array(_) | wf_engine::match_engine::Value::Object(_) => {
            "<structured>".to_string()
        }
    }
}

pub(super) fn debug_scope_key(scope_key: &[wf_engine::match_engine::Value]) -> String {
    scope_key
        .iter()
        .map(value_debug_string)
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn log_output_emitted(
    phase: &'static str,
    origin: &'static str,
    output_kind: &'static str,
    record: &OutputRecord,
    scope_key: &[wf_engine::match_engine::Value],
) {
    wf_debug!(
        pipe,
        rule = %record.rule_name,
        stage = 0,
        phase = phase,
        origin = origin,
        target = %record.yield_target,
        scope_key = %debug_scope_key(scope_key),
        output_kind = output_kind,
        "rule output emitted"
    );
}

pub(super) fn output_kind(
    record: &OutputRecord,
    intermediate_targets: &HashSet<String>,
) -> &'static str {
    if intermediate_targets.contains(&*record.yield_target) {
        "intermediate"
    } else {
        "alert"
    }
}

pub(super) fn log_output_suppressed(
    rule_name: &str,
    phase: &'static str,
    scope_key: Option<&[wf_engine::match_engine::Value]>,
) {
    let scope_present = scope_key.is_some();
    wf_debug!(
        pipe,
        rule = %rule_name,
        stage = 0,
        phase = phase,
        scope_key = %scope_key.map(debug_scope_key).unwrap_or_else(|| "<none>".to_string()),
        scope_present = scope_present,
        reason = "executor_returned_none",
        "rule output suppressed"
    );
}

pub(super) fn event_time_nanos(event: &dyn FieldSource, time_field: Option<&str>) -> i64 {
    time_field
        .and_then(|field| event.field_value(field))
        .and_then(|value| match value {
            wf_engine::match_engine::Value::Number(n) => Some(n as i64),
            _ => None,
        })
        .unwrap_or(0)
}
