//! process_batch 行循环族（rule_task.rs 拆分，2026-09-04）：machine 行循环
//! （`process_batch_machine_rows`）与 on-each 行循环（`process_batch_each_rows`）、
//! L2 延迟物化读取相位（`build_deferred_rows`）；行内 scan/advance 纯相位自由
//! 函数（`scan_expired_and_route_closes`/`advance_machine_row_aliases`/`alias_accepts`）
//! 同属——整组在 H-1..H-5（2026-09-03）收口，逐行 &mut machine 只在本文件内短借。

use super::*;

impl RuleTask {
    /// Machine 行循环（H-3，2026-09-03）：每行 scan_expired/close（conv 分流或
    /// inline conv）→ 命中行 advance（ordered_aliases 循环，进度/命中调试）→
    /// 关闭/命中 emit（列式累积或逐条求值）。machine 恒 Some 的批次才触达
    /// （process_batch 双路分发）；lookup / rule_name / ordered_aliases 在方法内自
    /// self 重建（参数不借调用点，方法与调用点各持 &mut self 无冲突）。收集器为
    /// &mut 参数：行循环后 process_batch 尾部仍要消费（同批尾收口）。
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn process_batch_machine_rows(
        &mut self,
        ctx: &MachineRowsCtx<'_>,
        stats: &mut RuleBatchDebugStats,
        staged_outputs: &mut Vec<OutputRecord>,
        conv_closes: &mut Vec<wf_engine::match_engine::CloseOutput>,
        columnar_closes: &mut Vec<wf_engine::match_engine::CloseOutput>,
        match_rows: &mut Vec<wf_engine::match_engine::MatchedContext>,
        conv_max_wm: &mut i64,
    ) {
        let MachineRowsCtx {
            window_name,
            batch_seq,
            lookup_max_seq,
            batch_emit_nanos,
            debug_enabled,
            row_domain,
            deferred,
            eager_events,
            columnar_masks,
            branch_masks,
            key_overrides,
        } = *ctx;
        let lookup =
            RegistryLookup::with_source_watermark(&self.router, lookup_max_seq, window_name);
        let rule_name = debug_enabled.then(|| self.rule_name().to_string());
        let rule_name_for_log = rule_name.as_deref().unwrap_or("");
        let ordered_aliases = self
            .ordered_aliases
            .get(window_name)
            .expect("machine rows require ordered aliases");
        // 与 process_batch 预计算同源（!debug × executor 门控的批级常量，纯函数）：
        // 行体需要、process_batch 尾部列式 emit 也需要，故两侧各算一份。
        let close_columnar = !debug_enabled && self.executor.close_plan_columnar_safe();
        let match_columnar = !debug_enabled && self.executor.match_plan_columnar_safe();
        // 批级 deferred 命中游标（行域相对；本批自 0 起，不跨批）。
        let mut hit_cursor = 0usize;
        for i in 0..row_domain.len() {
            let row_index = row_domain.row_at(i);
            let event: Option<&Arc<Event>> = match (&deferred, &eager_events) {
                // Deferred hit rows are served as ColumnarEvent views inside the
                // machine branch — no materialized hit events here.
                (Some(_), _) => None,
                (None, Some(events)) => Some(&events[row_index]),
                (None, None) => None,
            };
            let Some(machine) = self.machine.as_mut() else {
                unreachable!("machine rows loop requires the machine");
            };
            let event_nanos = match (&deferred, event) {
                (Some(d), _) => d.times[i],
                (None, Some(event)) => machine.event_time_nanos(event),
                (None, None) => {
                    unreachable!("machine rows are always materialized when eager")
                }
            };
            let _scan_start = rule_profiling();
            // P2c: shards of a conv rule emit raw closes to the conv stage
            // (aggregation window); inline conv is applied only on the
            // legacy single-machine path.
            // Hop 窗口：每 slide 边界恰有一个窗口到期（关闭数受窗口内键数
            // 约束），用无界预算一次性收口——1024 预算会把同一窗口的关闭
            // 拆成多批，inline conv 逐批 top-1 造成同窗口重复 EMIT。
            // H-5：scan_expired 收口 + close 路由（conv-sink 分流或 inline conv，
            // hop 无界预算）委托 scan_expired_and_route_closes——同步纯相位，行内
            // &mut machine 与 &mut 收集器直接让渡（返回 routed/closes 供 emit 相位）。
            let (routed, closes) = scan_expired_and_route_closes(
                machine,
                self.conv_sink.is_some(),
                self.conv_plan.as_ref(),
                event_nanos,
                conv_closes,
                conv_max_wm,
            );
            if let Some(_scan_t) = _scan_start {
                self.scan_nanos += _scan_t.elapsed().as_nanos() as u64;
            }
            // Non-hit rows only need the time-column scan above (watermark /
            // expiry) plus the close emission below; there is no Event to
            // advance and no bind filter would accept them, so skip the
            // state-machine step but keep the close path.
            // Resolve the per-row source: ColumnarEvent for deferred hit rows
            // (P3 FieldView — no HashMap materialization), else the eager
            // event. Debug and defer are mutually exclusive, so the Eager arm
            // is the only one that appears with debug detail enabled.
            let row_source: Option<(RowEvent<'_>, Option<TriggerEvent>)> =
                if let Some(d) = &deferred {
                    (hit_cursor < d.hit_indices.len() && d.hit_indices[hit_cursor] as usize == i)
                        .then(|| {
                            hit_cursor += 1;
                            // M1（P4 终态机制 2026-09-02）：fire `to_event` 用**规则
                            // 读集**投影（ctx 只读该集）而非窗口并集——消除未引用结构化
                            // 列的每 fire JSON 解析。无法窄化（All）→ 回退窗口投影。
                            // 仅影响 to_event；step/guard 的 field_value 直读不看投影。
                            let proj = self
                                .executor
                                .fire_trigger_projection()
                                .or_else(|| d.projection.clone());
                            // M3（2026-09-02）：fire 触发行改携 **owned 列式快照**——
                            // 机器在 Matched 时直接用（不再每 fire `to_event()` 物化
                            // HashMap + JSON 解析）；ctx 经 FieldSource 按需直读列。
                            // 仅当规则需要触发事件字段时预捕获（cheap Arc clone×3）。
                            let trigger = self
                                .executor
                                .plan()
                                .match_plan
                                .trigger_event_needed
                                .then(|| {
                                    TriggerEvent::columnar(
                                        Arc::clone(&d.batch_arc),
                                        row_index,
                                        Arc::clone(&d.index),
                                        proj.clone(),
                                    )
                                });
                            (
                                RowEvent::Columnar(ColumnarEvent::with_index_projected(
                                    d.batch,
                                    row_index,
                                    Arc::clone(&d.index),
                                    proj,
                                )),
                                trigger,
                            )
                        })
                } else {
                    event.map(|ev| (RowEvent::Eager(ev.as_ref()), None))
                };
            let matched = if let Some((row_event, row_trigger)) = row_source {
                let _advance_start = rule_profiling();
                let matched = advance_machine_row_aliases(
                    machine,
                    &self.executor,
                    ordered_aliases,
                    &row_event,
                    event_nanos,
                    &lookup,
                    row_index,
                    columnar_masks,
                    branch_masks,
                    key_overrides.as_ref().map(|ko| &ko[i]),
                    row_trigger.as_ref(),
                    debug_enabled,
                    window_name,
                    batch_seq,
                    rule_name_for_log,
                    stats,
                );
                if let Some(_advance_t) = _advance_start {
                    self.advance_nanos += _advance_t.elapsed().as_nanos() as u64;
                }
                matched
            } else {
                Vec::new()
            };
            let _emit_start = rule_profiling();

            // When routed to the conv stage, the inline close processing is
            // skipped (the closes were already sent in the scan step).
            // Columnar-safety gate: gate-passing rules accumulate raw
            // closes across the batch and emit them vectorized after the
            // row loop (see the batch close emit below) — the q12 close
            // fan-out hot path (per-close OutputRecord + synthetic ctx
            // build measured ~95% of execute_close_with_joins).
            if close_columnar && !routed {
                columnar_closes.extend(closes);
            } else if !routed {
                for close in &closes {
                    let _close_exec_start = rule_profiling();
                    let result = self.executor.execute_close_with_joins(close, &lookup);
                    if let Some(_close_t) = _close_exec_start {
                        self.close_exec_nanos += _close_t.elapsed().as_nanos() as u64;
                    }
                    match result {
                        Ok(Some(record)) => {
                            if debug_enabled {
                                stats.count_output(&record, &self.intermediate_targets);
                            }
                            if debug_enabled && stats.allow_detail() {
                                log_output_emitted(
                                    "execute_close",
                                    "close",
                                    output_kind(&record, &self.intermediate_targets),
                                    &record,
                                    close.scope_key.as_slice(),
                                );
                            }
                            self.stage_or_emit_record(staged_outputs, record).await;
                        }
                        Ok(None) => {
                            if debug_enabled {
                                stats.output_none += 1;
                            }
                            if debug_enabled && stats.allow_detail() {
                                log_output_suppressed(
                                    rule_name_for_log,
                                    "execute_close",
                                    Some(close.scope_key.as_slice()),
                                );
                            }
                        }
                        Err(e) => {
                            if debug_enabled {
                                stats.errors += 1;
                            }
                            wf_warn!(
                                pipe,
                                rule = %rule_name.as_deref().unwrap_or_else(|| self.rule_name()),
                                stage = 0,
                                phase = "execute_close",
                                scope_key = %debug_scope_key(&close.scope_key),
                                error = %e,
                                "rule output failed"
                            )
                        }
                    }
                }
            }

            if match_columnar {
                // 列式：move 整行命中到批级累积（零成本），批后统一
                // 直写 builder——跳过 join 执行（门控保证输出不引用非键
                // 右窗字段，join 已在 advance 阶段完成）。
                if let Some(metrics) = &self.metrics {
                    for _ in 0..matched.len() {
                        metrics.inc_rule_match(self.rule_name());
                    }
                }
                match_rows.extend(matched);
            } else {
                for ctx in &matched {
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_rule_match(self.rule_name());
                    }
                    let _exec_start = rule_profiling();
                    match self
                        .executor
                        .execute_match_with_joins_at(ctx, &lookup, batch_emit_nanos)
                    {
                        Ok(Some(record)) => {
                            if let Some(_exec_t) = _exec_start {
                                self.exec_nanos += _exec_t.elapsed().as_nanos() as u64;
                            }
                            if debug_enabled {
                                stats.count_output(&record, &self.intermediate_targets);
                            }
                            if debug_enabled && stats.allow_detail() {
                                log_output_emitted(
                                    "execute_match",
                                    "event",
                                    output_kind(&record, &self.intermediate_targets),
                                    &record,
                                    ctx.scope_key.as_slice(),
                                );
                            }
                            self.stage_or_emit_record(staged_outputs, record).await;
                        }
                        Ok(None) => {
                            if debug_enabled {
                                stats.output_none += 1;
                            }
                            if debug_enabled && stats.allow_detail() {
                                log_output_suppressed(
                                    rule_name_for_log,
                                    "execute_match",
                                    Some(ctx.scope_key.as_slice()),
                                );
                            }
                        }
                        Err(e) => {
                            if debug_enabled {
                                stats.errors += 1;
                            }
                            wf_warn!(
                                pipe,
                                rule = %rule_name.as_deref().unwrap_or_else(|| self.rule_name()),
                                stage = 0,
                                phase = "execute_match",
                                scope_key = %debug_scope_key(&ctx.scope_key),
                                error = %e,
                                "rule output failed"
                            )
                        }
                    }
                }
            }
            if let Some(_emit_t) = _emit_start {
                self.emit_nanos += _emit_t.elapsed().as_nanos() as u64;
            }
        }
    }

    /// On-each（machine 恒 None）行处理循环（H-2，2026-09-03）：列式/Event 驱动行
    /// 的 bind 过滤 + deferred `emit at` 挂起 / each 直发 / join 求值。machine
    /// 恒 None 的批次才触达（process_batch 双路分发）→ deferred（DeferredRows，
    /// DeferredMachine 专用）恒 None：行源是 eager events 或 deferred_columnar 列式
    /// 视图。lookup / rule_name 局部在方法内重建——参数均不借 self，方法与调用点
    /// 各自持 &mut self 无冲突。
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn process_batch_each_rows<'rows>(
        &mut self,
        alias: &str,
        window_name: &str,
        batch_seq: u64,
        lookup_max_seq: Option<u64>,
        batch_emit_nanos: i64,
        debug_enabled: bool,
        row_domain: &RowDomain<'_>,
        eager_events: Option<&'rows Arc<Vec<Arc<Event>>>>,
        deferred_columnar: Option<&DeferredColumnarBatch>,
        columnar_masks: &HashMap<String, Option<BooleanArray>>,
        each_field_order: &[&smol_str::SmolStr],
        stats: &mut RuleBatchDebugStats,
        each_direct_rows: &mut Vec<(&'rows wf_engine::match_engine::Event, i64)>,
    ) {
        let lookup =
            RegistryLookup::with_source_watermark(&self.router, lookup_max_seq, window_name);
        let rule_name = debug_enabled.then(|| self.rule_name().to_string());
        let rule_name_for_log = rule_name.as_deref().unwrap_or("");
        for i in 0..row_domain.len() {
            let row_index = row_domain.row_at(i);
            let event: Option<&Arc<Event>> = eager_events.as_ref().map(|events| &events[row_index]);
            // deferred join 驱动行视图（P4 gap-1）：列式（无 eager 物化，
            // `DeferredColumnarBatch`）或 Event 回退（DEBUG 开 / 无原始
            // batch）。非 deferred 规则为 None，走下方 eager `event`。
            let deferred_left: Option<DeferredLeft> =
                self.deferred
                    .as_ref()
                    .map(|_| match (&deferred_columnar, &event) {
                        (Some(v), _) => DeferredLeft::Columnar(JoinRow::Columnar {
                            batch: Arc::clone(&v.batch),
                            row: row_index,
                            index: Arc::clone(&v.index),
                            projection: v.projection.clone(),
                        }),
                        (None, Some(ev)) => {
                            let event: &Event = ev;
                            DeferredLeft::Event(event.clone())
                        }
                        (None, None) => unreachable!("deferred without batch or eager events"),
                    });
            let accepted = match &deferred_left {
                Some(left) => alias_accepts(
                    &self.executor,
                    columnar_masks,
                    alias,
                    row_index,
                    left,
                    &lookup,
                ),
                None => {
                    let event = event.expect("each path is always eager");
                    alias_accepts(
                        &self.executor,
                        columnar_masks,
                        alias,
                        row_index,
                        event.as_ref(),
                        &lookup,
                    )
                }
            };
            if accepted {
                if debug_enabled {
                    stats.alias_passed += 1;
                }
                let event_nanos = match &deferred_left {
                    Some(left) => event_time_nanos(left, self.each_time_field.as_deref()),
                    None => event_time_nanos(
                        event.expect("each path is always eager").as_ref(),
                        self.each_time_field.as_deref(),
                    ),
                };
                // P3：deferred join（`emit at`）——驱动事件挂起（expiry = emit at），
                // 不即时输出；到期评估在批次尾的 `scan_deferred`（设计 §5.2）。
                if self.deferred.is_some() {
                    if let Some(deferred) = self.deferred.as_mut()
                        && let Some(left) = &deferred_left
                        && let Some(pending) =
                            self.executor
                                .deferred_pending_for(deferred.join_idx, left, event_nanos)
                    {
                        deferred.watermark = deferred.watermark.max(event_nanos);
                        // 2026-08-25 q4 100M：pending 保持按 expiry 升序——
                        // scan_deferred 据此只取到期前缀（O(due)）而非全量
                        // 扫（O(n)，33M 挂起 × 2740 batch 卡死 28×）。驱动流
                        // 事件时间单调时 expiry 也单调（emit at = expires 随
                        // 事件时间），追加即有序 O(1)；乱序驱动二分插入兜底。
                        let expiry = pending.expiry_nanos;
                        let pos = deferred
                            .pending
                            .partition_point(|p| p.expiry_nanos <= expiry);
                        // lo_min 缓存：插入 O(1) 更新（publish 免全量扫）。
                        // 用插入项的 lo_ns（区间下界）；pending 有序后 min lo
                        // 项几乎总是最早挂起（数据时间单调），dirty 极少。
                        let lo_ns = pending.lo_ns;
                        deferred.pending.insert(pos, pending);
                        deferred.lo_min = deferred.lo_min.min(lo_ns);
                    }
                    if debug_enabled {
                        stats.advanced += 1;
                    }
                    continue;
                }
                // 非 deferred 规则：eager event 恒在（deferred 分支已 continue）。
                let event = event.expect("each path is always eager");
                if self.each_direct {
                    if !debug_enabled {
                        // Plan C2 batched: defer to the vectorized pass
                        // after the loop (same rows, same flush cadence).
                        each_direct_rows.push((event.as_ref(), event_nanos));
                        continue;
                    }
                    // Plan C2 per-event path (debug detail on): the
                    // executor appends straight into the columnar
                    // builder — no per-record OutputRecord.
                    match self
                        .emit_each_direct(
                            event,
                            event_nanos,
                            &lookup,
                            each_field_order,
                            batch_emit_nanos,
                        )
                        .await
                    {
                        Ok(true) => {
                            if debug_enabled {
                                stats.output_emitted += 1;
                            }
                            if debug_enabled && stats.allow_detail() {
                                wf_debug!(pipe,
                                    rule = %rule_name_for_log,
                                    stage = 0,
                                    phase = "execute_each",
                                    target = %self.executor.static_yield_target(),
                                    output_kind = "alert",
                                    "rule output emitted (direct)"
                                );
                            }
                        }
                        Ok(false) => {
                            if debug_enabled {
                                stats.output_none += 1;
                            }
                            if debug_enabled && stats.allow_detail() {
                                log_output_suppressed(rule_name_for_log, "execute_each", None);
                            }
                        }
                        Err(e) => {
                            if debug_enabled {
                                stats.errors += 1;
                            }
                            wf_warn!(
                                pipe,
                                rule = %rule_name.as_deref().unwrap_or_else(|| self.rule_name()),
                                stage = 0,
                                phase = "execute_each",
                                error = %e,
                                "rule output failed"
                            )
                        }
                    }
                } else {
                    match self.executor.execute_each_with_joins(
                        event,
                        event_nanos,
                        &lookup,
                        each_field_order,
                        batch_emit_nanos,
                    ) {
                        Ok(Some(record)) => {
                            if debug_enabled {
                                stats.count_output(&record, &self.intermediate_targets);
                            }
                            if debug_enabled && stats.allow_detail() {
                                log_output_emitted(
                                    "execute_each",
                                    "event",
                                    output_kind(&record, &self.intermediate_targets),
                                    &record,
                                    &[],
                                );
                            }
                            self.emit(record).await;
                        }
                        Ok(None) => {
                            if debug_enabled {
                                stats.output_none += 1;
                            }
                            if debug_enabled && stats.allow_detail() {
                                log_output_suppressed(rule_name_for_log, "execute_each", None);
                            }
                        }
                        Err(e) => {
                            if debug_enabled {
                                stats.errors += 1;
                            }
                            wf_warn!(
                                pipe,
                                rule = %rule_name.as_deref().unwrap_or_else(|| self.rule_name()),
                                stage = 0,
                                phase = "execute_each",
                                error = %e,
                                "rule output failed"
                            )
                        }
                    }
                }
            } else {
                if debug_enabled {
                    stats.alias_rejected += 1;
                }
                if debug_enabled && stats.allow_detail() {
                    // debug_enabled 时 deferred_columnar 必为 None（其前置
                    // 含 !debug）→ eager event 恒在。
                    let event_ref = event_debug_ref(
                        event.expect("each path is always eager"),
                        batch_seq,
                        row_index,
                    );
                    wf_debug!(pipe,
                        rule = %rule_name_for_log,
                        stage = 0,
                        window = %window_name,
                        alias = %alias,
                        event_ref = %event_ref,
                        reason = "bind_filter_false",
                        "rule event rejected"
                    );
                }
            }
        }
        // H-2（2026-09-03）：each 行向量化直发收口自 process_batch 尾部移到本方法——
        // 本方法内 lookup 自 router 重建，调用点不必为它跨 &mut self 持借用；行收集
        // 只在本路径发生，收口位置与原先（行循环后立即）等价。
        if !each_direct_rows.is_empty() {
            self.emit_each_direct_batch(
                each_direct_rows.as_slice(),
                &lookup,
                each_field_order,
                batch_emit_nanos,
            )
            .await;
        }
    }

    /// Deferred rows（L2 延迟物化，2026-08-29 读批相位）：列时间扫描 +
    /// 掩码命中位 + P3 FieldView 列索引——命中行直接从列喂状态机，免逐行
    /// Event HashMap 物化。行域相对（row-domain-relative）语义见
    /// `process_batch` 的 RowDomain 注释。
    pub(super) fn build_deferred_rows<'a>(
        &self,
        batch: &'a RecordBatch,
        aliases: &[String],
        columnar_masks: &HashMap<String, Option<BooleanArray>>,
        materialize_fields: Option<&HashSet<String>>,
        row_domain: &RowDomain<'_>,
    ) -> DeferredRows<'a> {
        let time_field = self.machine.as_ref().and_then(|m| m.time_field());
        // Scan needs the event time for every row (watermark/expiry); read
        // it straight from the time column with the same f64 round-trip the
        // eager path uses (`extract_event_time`). Resolve the column once,
        // then read per row over `row_domain` (whole batch for unsharded,
        // this shard's subset for sharded).
        //
        // `times` / `hit` / `hit_indices` are all **row-domain-relative**
        // (length == `row_domain.len()`; slot i covers `row_domain[i]`), so
        // a sharded push allocates only its own shard's rows — not the whole
        // batch. Absolute batch rows are recovered from `row_domain` at the
        // point they are needed (materialization, hit matching below).
        let time_col_index = batch_time_col_index(batch, time_field);
        // 事件时间列存在时逐行 push（免 `vec![0; n]` 零填 + 覆盖的双写）。
        let mut times = Vec::with_capacity(row_domain.len());
        if let Some(col_idx) = time_col_index {
            for i in 0..row_domain.len() {
                times.push(batch_event_time_nanos_at(
                    batch,
                    col_idx,
                    row_domain.row_at(i),
                ));
            }
        } else {
            times.resize(row_domain.len(), 0);
        }
        // Hit = any alias's columnar bind filter accepts this row. The
        // window-level defer flag guarantees every alias here is columnar;
        // a missing mask is a defensive fallback that materializes all rows.
        let mut hit = vec![false; row_domain.len()];
        for alias in aliases.iter() {
            match columnar_masks.get(alias) {
                Some(Some(mask)) => {
                    for (i, h) in hit.iter_mut().enumerate() {
                        *h |= mask.value(row_domain.row_at(i));
                    }
                }
                _ => {
                    for h in hit.iter_mut() {
                        *h = true;
                    }
                }
            }
        }
        // Row-domain-relative hit positions.
        let hit_indices: Vec<u32> = (0..row_domain.len())
            .filter(|&i| hit[i])
            .map(|i| i as u32)
            .collect();
        // P3 FieldView: hit rows are fed to the state machine straight from
        // the columns — no HashMap materialization. The batch-level field
        // index makes `ColumnarEvent::field_value` O(1) per read; the
        // `materialize_fields` projection keeps the emit-path trigger event
        // byte-identical to the eager deferred path (projected). (The
        // `columnar_each` early path is machine-free, so this branch never
        // runs for it; `materialize_rows[_filtered]` stays only on the
        // eager path below.)
        let index = build_field_index(batch);
        let projection = materialize_fields.map(|f| Arc::new(f.clone()));
        DeferredRows {
            times,
            hit_indices,
            batch,
            batch_arc: Arc::new(batch.clone()),
            index,
            projection,
        }
    }
}

/// Whether `alias`'s bind filter accepts `row` of the current batch, using the
/// precomputed columnar mask when available and falling back to the per-event
/// interpreted path otherwise.
pub(super) fn alias_accepts(
    executor: &RuleExecutor,
    masks: &HashMap<String, Option<BooleanArray>>,
    alias: &str,
    row: usize,
    event: &dyn FieldSource,
    lookup: &RegistryLookup<'_>,
) -> bool {
    match masks.get(alias) {
        Some(Some(mask)) => mask.value(row),
        _ => executor.event_matches_alias(alias, event, Some(lookup)),
    }
}

/// machine 行体的 scan/close 相位（H-5，2026-09-03）：scan_expired 收口到期窗口
/// close 并路由——conv-sink 分片把原始 close 累积到 conv_closes（watermark 同步，
/// 批次尾统一 ConvCloseBatch 直发），否则 inline conv 求值（hop 窗每 slide 边界恰
/// 一个窗口到期，用无界预算一次收口避免同窗重复 EMIT）。同步纯相位（无 await /
/// 无 self 方法调用）：machine / conv 上下文 / 批级收集器以独立参数传入，行内
/// &mut machine 与 &mut 收集器直接让渡，调用方保留相位计时。
pub(super) fn scan_expired_and_route_closes(
    machine: &mut CepStateMachine,
    conv_sink: bool,
    conv_plan: Option<&ConvPlan>,
    event_nanos: i64,
    conv_closes: &mut Vec<wf_engine::match_engine::CloseOutput>,
    conv_max_wm: &mut i64,
) -> (bool, Vec<wf_engine::match_engine::CloseOutput>) {
    let hop = matches!(machine.plan().window_spec, WindowSpec::Hop { .. });
    if conv_sink {
        let raw = if hop {
            machine.scan_expired_at_skip_non_alerting_unbounded(event_nanos)
        } else {
            machine.scan_expired_at_skip_non_alerting(event_nanos)
        };
        // Barrier watermark must reflect the scan's watermark (the
        // event time) — the machine's cached watermark only advances
        // during `advance`, which runs after the scan.
        *conv_max_wm = (*conv_max_wm).max(event_nanos);
        conv_closes.extend(raw.into_iter().filter(close_is_qualified));
        (true, Vec::new())
    } else if hop {
        (
            false,
            machine.scan_expired_at_with_conv_skip_non_alerting_unbounded(event_nanos, conv_plan),
        )
    } else {
        (
            false,
            machine.scan_expired_at_with_conv_skip_non_alerting(event_nanos, conv_plan),
        )
    }
}

/// machine 行体的 advance 相位（H-4，2026-09-03）：命中行（row_source）逐 alias
/// bind 过滤 + 状态机推进 + step 结果统计/调试，返回本行命中的 ctx 列表。同步纯
/// 相位（无 await / 无 self 方法调用）——machine / executor / 上下文以独立参数
/// 传入：这是行循环内 machine 的最后 &mut 使用点，抽成自由函数可避开 &mut
/// self.machine 跨 `&self` 方法调用的借用冲突；行级其余相位（scan / row-source /
/// emit）留在 process_batch_machine_rows。
#[allow(clippy::too_many_arguments)]
pub(super) fn advance_machine_row_aliases(
    machine: &mut CepStateMachine,
    executor: &RuleExecutor,
    ordered_aliases: &[String],
    row_event: &RowEvent<'_>,
    event_nanos: i64,
    lookup: &RegistryLookup<'_>,
    row_index: usize,
    columnar_masks: &HashMap<String, Option<BooleanArray>>,
    branch_masks: &GuardMasks,
    key_override: Option<&Option<Vec<wf_engine::match_engine::Value>>>,
    row_trigger: Option<&TriggerEvent>,
    debug_enabled: bool,
    window_name: &str,
    batch_seq: u64,
    rule_name_for_log: &str,
    stats: &mut RuleBatchDebugStats,
) -> Vec<wf_engine::match_engine::MatchedContext> {
    let mut matched = Vec::new();
    for alias in ordered_aliases {
        if !alias_accepts(
            executor,
            columnar_masks,
            alias,
            row_index,
            row_event,
            lookup,
        ) {
            if debug_enabled {
                stats.alias_rejected += 1;
            }
            if debug_enabled && stats.allow_detail() {
                let event_ref = row_event_debug_ref(row_event, batch_seq, row_index);
                wf_debug!(pipe,
                    rule = %rule_name_for_log,
                    stage = 0,
                    window = %window_name,
                    alias = %alias,
                    event_ref = %event_ref,
                    reason = "bind_filter_false",
                    "rule event rejected"
                );
            }
            continue;
        }
        if debug_enabled {
            stats.alias_passed += 1;
        }
        let should_capture_progress = debug_enabled && stats.can_log_detail();
        let (step_result, progress) = if should_capture_progress {
            // debug 路径走内部解析（结果与预解析一致——批级共享同一
            // lookup + values_equal 语义）。
            let outcome =
                machine.advance_at_with_progress(alias, row_event, event_nanos, Some(lookup));
            (outcome.result, outcome.progress)
        } else {
            (
                machine.advance_at_with_masks_key_capture(
                    alias,
                    row_event,
                    event_nanos,
                    Some(lookup),
                    row_index,
                    Some(branch_masks),
                    key_override,
                    row_trigger,
                ),
                None,
            )
        };
        match step_result {
            StepResult::Accumulate => {
                if debug_enabled {
                    stats.accumulated += 1;
                }
                if debug_enabled && stats.allow_detail() {
                    let instances = machine.instance_count();
                    let event_ref = row_event_debug_ref(row_event, batch_seq, row_index);
                    if let Some(progress) = progress.as_ref() {
                        wf_debug!(pipe,
                            rule = %rule_name_for_log,
                            stage = 0,
                            window = %window_name,
                            alias = %alias,
                            event_ref = %event_ref,
                            scope_key = %debug_scope_key(&progress.scope_key),
                            machine_id = %progress.machine_id,
                            step_index = progress.step_index,
                            step_label = progress.step_label.as_deref().unwrap_or(""),
                            branch_index = progress.branch_index,
                            threshold_checked_branches = progress.threshold_checked_branches,
                            measure_value = progress.measure_value,
                            cmp = %progress.cmp,
                            threshold = %progress.threshold,
                            instances = instances,
                            "rule event accumulated"
                        );
                    } else {
                        wf_debug!(pipe,
                            rule = %rule_name_for_log,
                            stage = 0,
                            window = %window_name,
                            alias = %alias,
                            event_ref = %event_ref,
                            instances = instances,
                            "rule event accumulated"
                        );
                    }
                }
            }
            StepResult::Advance => {
                if debug_enabled {
                    stats.advanced += 1;
                }
                if debug_enabled && stats.allow_detail() {
                    let instances = machine.instance_count();
                    let event_ref = row_event_debug_ref(row_event, batch_seq, row_index);
                    if let Some(progress) = progress.as_ref() {
                        wf_debug!(pipe,
                            rule = %rule_name_for_log,
                            stage = 0,
                            window = %window_name,
                            alias = %alias,
                            event_ref = %event_ref,
                            scope_key = %debug_scope_key(&progress.scope_key),
                            machine_id = %progress.machine_id,
                            step_index = progress.step_index,
                            step_label = progress.step_label.as_deref().unwrap_or(""),
                            branch_index = progress.branch_index,
                            threshold_checked_branches = progress.threshold_checked_branches,
                            measure_value = progress.measure_value,
                            cmp = %progress.cmp,
                            threshold = %progress.threshold,
                            instances = instances,
                            "rule step advanced"
                        );
                    } else {
                        wf_debug!(pipe,
                            rule = %rule_name_for_log,
                            stage = 0,
                            window = %window_name,
                            alias = %alias,
                            event_ref = %event_ref,
                            instances = instances,
                            "rule step advanced"
                        );
                    }
                }
            }
            StepResult::Matched(ctx) => {
                if debug_enabled {
                    stats.matched += 1;
                }
                if debug_enabled && stats.allow_detail() {
                    let event_ref = row_event_debug_ref(row_event, batch_seq, row_index);
                    let step = ctx.step_data.last();
                    wf_debug!(pipe,
                        rule = %rule_name_for_log,
                        stage = 0,
                        window = %window_name,
                        alias = %alias,
                        event_ref = %event_ref,
                        scope_key = %debug_scope_key(&ctx.scope_key),
                        machine_id = %ctx.machine_id,
                        matched_steps = ctx.step_data.len(),
                        step_label = step.and_then(|s| s.label.as_deref()).unwrap_or(""),
                        measure_value = step.map(|s| s.measure_value).unwrap_or_default(),
                        "rule matched"
                    );
                }
                matched.push(ctx);
            }
        }
    }
    matched
}
