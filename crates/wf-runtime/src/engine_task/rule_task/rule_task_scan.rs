//! 批次收口与周期扫描（rule_task.rs 拆分，2026-09-04）：批诊断日志
//! （`log_batch_start`/`log_batch_summary`/`dump_profiling`）、deferred join 到期
//! 评估（`scan_deferred`/`reevaluate_deferred_missed`）、idle 墙钟超时扫描
//! （`scan_timeouts`）与 EOS/关机 flush 收口。

use super::*;

impl RuleTask {
    /// 批前诊断（E 相位）：debug 下输出 `rule batch started`（含 instances_before），
    /// 并上报本批事件数指标。纯 `&self`——调用点可能仍持 `self.aliases` 借用。
    pub(super) fn log_batch_start(
        &self,
        debug_enabled: bool,
        window_name: &str,
        batch_seq: u64,
        input_events: usize,
        rule_name_for_log: &str,
        aliases_for_log: Option<&str>,
    ) {
        if debug_enabled {
            let instances_before = self.instance_count();
            wf_debug!(pipe,
                rule = %rule_name_for_log,
                stage = 0,
                window = %window_name,
                batch_seq = batch_seq,
                rows = input_events,
                aliases = %aliases_for_log.unwrap_or(""),
                instances_before = instances_before,
                "rule batch started"
            );
        }
        if let Some(metrics) = &self.metrics {
            metrics.add_rule_events(self.executor.plan().name.as_str(), input_events);
        }
    }

    /// 批尾诊断（L 相位）：debug 下输出 `rule batch summary`（含 detail 抑制提示），
    /// 并节流 dump 相位耗时累计。统计字段读自 `stats`（行循环已累计）。
    pub(super) fn log_batch_summary(
        &mut self,
        debug_enabled: bool,
        window_name: &str,
        batch_seq: u64,
        rule_name_for_log: &str,
        stats: &RuleBatchDebugStats,
    ) {
        if debug_enabled {
            let instances_after = self.instance_count();
            wf_debug!(pipe,
                rule = %rule_name_for_log,
                stage = 0,
                window = %window_name,
                batch_seq = batch_seq,
                input = stats.input_events,
                alias_passed = stats.alias_passed,
                alias_rejected = stats.alias_rejected,
                accumulated = stats.accumulated,
                advanced = stats.advanced,
                matched = stats.matched,
                outputs = stats.output_emitted,
                output_none = stats.output_none,
                intermediate_outputs = stats.intermediate_emitted,
                errors = stats.errors,
                instances_after = instances_after,
                detail_logged = stats.detail_logged,
                detail_suppressed = stats.detail_suppressed,
                "rule batch summary"
            );
            if stats.detail_suppressed > 0 {
                wf_debug!(pipe,
                    rule = %rule_name_for_log,
                    stage = 0,
                    window = %window_name,
                    batch_seq = batch_seq,
                    detail_logged = stats.detail_logged,
                    detail_suppressed = stats.detail_suppressed,
                    "rule event details suppressed"
                );
            }
        }
        self.dump_profiling();
    }

    /// Log the cumulative advance/scan/emit profiler accumulators once per
    /// second (throttled) so a run's phase split can be read from the log.
    pub(super) fn dump_profiling(&mut self) {
        if self.last_profile_dump.elapsed() < Duration::from_secs(1) {
            return;
        }
        self.last_profile_dump = std::time::Instant::now();
        wf_info!(pipe,
            rule = %self.rule_name(),
            phase = "profile",
            scan_nanos = self.scan_nanos,
            advance_nanos = self.advance_nanos,
            exec_nanos = self.exec_nanos,
            close_exec_nanos = self.close_exec_nanos,
            append_nanos = self.append_nanos.load(Ordering::Relaxed),
            fanout_nanos = self.fanout_nanos.load(Ordering::Relaxed),
            emit_nanos = self.emit_nanos,
            "rule profiling"
        );
    }

    // -- Timeout & shutdown -------------------------------------------------

    /// Scan for expired state machine instances and emit alerts.
    /// P3：deferred join 到期扫描——触发 `expiry ≤ wm` 的挂起实例，评估并输出。
    ///
    /// `wm`：事件时间 watermark（批次尾 / scan_timeouts / flush 收口）；
    /// `emit_time_nanos`：输出记录的墙钟 emit 时间。空集（Q9 无 bid）不输出。
    /// `gate_on_target`：运行期为 true 时把评估前沿压到 join 目标窗口的 append
    /// 位置（见函数内注释——100M q4a 欠发根治）；flush 收口为 false（数据已全
    /// 量 ingest，miss 由 EOS 重试兜底，不能 gate 掉尾部 pending）。
    pub(super) async fn scan_deferred(
        &mut self,
        wm: i64,
        emit_time_nanos: i64,
        gate_on_target: bool,
    ) {
        let Some(deferred) = self.deferred.as_mut() else {
            return;
        };
        let join_idx = deferred.join_idx;
        // 2026-08-25 q4 100M 欠发根治（两轮）＋跨源提交乱序修复：
        // 1) 评估前沿 = min(驱动 watermark, join 目标窗**健全提交前沿**)。
        //    驱动 wm 是 oracle 语义边界（deferred watermark = 最后驱动事件时间），
        //    目标窗前沿（各 source 已提交 max 的 min，`committed_frontier_ns`）
        //    是右行完整性的健全判据——全局 max_event_time 会被跨 source 乱序
        //    提交提前推高（ingress instances=8 + parse 并行），用它会在右行未
        //    落地时提前评估 → 假 miss（30M q4 over=30m -860，2026-08-25 实测）。
        // 2) 防御：目标不存在/未 append（i64::MAX/i64::MIN）→ 退回驱动 wm。
        // 3) 右行年龄保护：pending 期间 pin 挡住时间驱逐（D4 闭环），评估因
        //    前沿等待而延迟时右行不会被 over 删掉。
        let eff_wm = if gate_on_target {
            let frontier = self
                .router
                .registry()
                .get_window(&self.executor.plan().joins[join_idx].right_window)
                .map(|w| w.committed_frontier_ns())
                .unwrap_or(i64::MAX);
            if frontier == i64::MAX {
                // 目标窗不存在（防御，get_window 失败）：退回驱动 wm（旧行为）。
                // 注意：无时间列窗口的 frontier 是 i64::MIN（max 不推进）→
                // 走挂起分支——但 deferred 目标必有时间列（within [lo,hi]
                // 依赖 ts），该路径不可达。
                wm
            } else if frontier == i64::MIN {
                // 目标窗**尚无任何提交**（启动期首个 batch 前）：右行必然不在，
                // 评估即假 miss（对着空窗全 miss → 行到达后已无 pin 保护 →
                // 驱逐 → 欠发）。保持挂起，等首个提交推进前沿。
                i64::MIN
            } else {
                wm.min(frontier)
            }
        } else {
            wm
        };
        // 取到期实例（块内释放 `deferred` 借用，避免与 `self.executor`/`self.emit` 冲突）
        // 2026-08-25 q4 100M：pending 按 expiry 升序 → 到期项是前缀，
        // `partition_point` O(log n) 定位 + drain 前缀 O(due)——替代旧的
        // 全量遍历重建（O(n)/batch，33M 挂起 × 2740 batch 卡死 28×）。
        //
        // 2026-08-25（内存修复）：**drain 到期前缀后必须标 dirty**——lo_min
        // 缓存是插入时单调不增的 min（历史最小 lo_ns），drain 后仍偏保守
        //（更小）→ 正确性安全，但 pin 会永远停在流起点的历史最小值 → 时间
        // 驱逐全被挡（30M q4 over=30m：pin_floor=起点+1ms、evict=0、RSS 9.2GB
        // = 整窗保留，2026-08-25 探针实锤）。publish 下一次重算当前 pending 的
        // min lo（评估 gate 后 pending 很小，O(n) 无压力——旧 O(n²) 担忧是
        // 63% 假 miss 时代 33M 挂起 × 2740 batch 的产物，已不成立）。
        let due: Vec<DeferredPending> = {
            let split = deferred
                .pending
                .partition_point(|p| p.expiry_nanos <= eff_wm);
            if split > 0 {
                deferred.lo_min_dirty = true;
            }
            deferred.pending.drain(..split).collect()
        };
        if due.is_empty() {
            return;
        }
        let lookup = RegistryLookup::new(&self.router);
        let mut stats = RuleBatchDebugStats::default();
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
        // 中间窗轻量化（2026-08-26 q4a）：yield 到中间窗且 yield 表达式不引用
        // `__wfu_*` meta → 评估后走轻量 build（`build_each_alert_pipe`，跳过
        // wfx_id/fired_at/summary 构建——中间窗消费者按列读不需要；q4a 评估
        // 1.7µs 的固定成本大头）。sink 目标 / 引用 meta 的 yield → 全量 build。
        let pipe_light = self
            .intermediate_targets
            .contains(self.executor.static_yield_target().as_ref())
            && self.executor.pipe_light_build_ready();
        // 到期 miss 的收集——join 目标窗口可能 append 滞后（引擎流式 vs oracle
        // 预加载），留到 EOS flush 重试（届时目标完整）；真 miss 重试后仍 miss。
        let mut missed_this = Vec::new();
        for p in due {
            match self.executor.evaluate_deferred_join(join_idx, &p, &lookup) {
                Ok(Some(out_ctx)) => {
                    let record = if pipe_light {
                        self.executor
                            .build_each_alert_pipe(&out_ctx, p.expiry_nanos)
                    } else {
                        self.executor.build_deferred_output(
                            &out_ctx,
                            p.expiry_nanos,
                            emit_time_nanos,
                        )
                    };
                    match record {
                        Ok(Some(record)) => {
                            if debug_enabled {
                                stats.count_output(&record, &self.intermediate_targets);
                            }
                            if debug_enabled && stats.allow_detail() {
                                log_output_emitted(
                                    "execute_deferred",
                                    "deferred",
                                    output_kind(&record, &self.intermediate_targets),
                                    &record,
                                    &[],
                                );
                            }
                            self.emit(record).await;
                        }
                        Ok(None) => {
                            // 到期 miss：join 目标窗口可能未追平（append 滞后）——
                            // 留到 EOS 重试（届时目标完整）。真 miss 重试后仍 miss。
                            missed_this.push(p);
                            if debug_enabled {
                                stats.output_none += 1;
                            }
                            if debug_enabled && stats.allow_detail() {
                                log_output_suppressed(self.rule_name(), "execute_deferred", None);
                            }
                        }
                        Err(e) => {
                            if debug_enabled {
                                stats.errors += 1;
                            }
                            wf_warn!(
                                pipe,
                                task_id = %self.task_id,
                                rule = %self.rule_name(),
                                stage = 0,
                                phase = "execute_deferred",
                                error = %e,
                                "deferred join output failed"
                            );
                        }
                    }
                }
                Ok(None) => {
                    // 到期 miss（评估无匹配）：join 目标窗口可能未追平（append
                    // 滞后）——留到 EOS 重试（届时目标完整）。真 miss 重试后仍 miss。
                    missed_this.push(p);
                    if debug_enabled {
                        stats.output_none += 1;
                    }
                    if debug_enabled && stats.allow_detail() {
                        log_output_suppressed(self.rule_name(), "execute_deferred", None);
                    }
                }
                Err(e) => {
                    if debug_enabled {
                        stats.errors += 1;
                    }
                    wf_warn!(
                        pipe,
                        task_id = %self.task_id,
                        rule = %self.rule_name(),
                        stage = 0,
                        phase = "execute_deferred",
                        error = %e,
                        "deferred join output failed"
                    );
                }
            }
        }
        if !missed_this.is_empty()
            && let Some(deferred) = self.deferred.as_mut()
        {
            // 2026-08-25：missed 不再计入 pin/lo_min——评估 gate 后运行期 miss
            // 即真 miss（右行确实不在区间内），EOS 重试只做确认，不需要保留
            // 右行；missed 的 lo 分布全流，计入会把时间驱逐拖死。
            deferred.missed.extend(missed_this);
        }
    }

    /// EOS 重试：到期评估 miss 的 deferred 实例（join 目标 append 滞后）。
    ///
    /// 重试**仍 miss** 的实例保留回 `missed`，不在此处判定为真 miss：flush 的
    /// 调用方可能是 keep-running EOS（窗口 actors 仍在排空 mailbox，目标窗口
    /// 可能不完整——shutdown 路径因 LIFO 排序无此问题，但 daemon 接收有限输入
    /// 的 EOS 场景是真实竞态，2026-08-23 复现测试锁定）。保留后由窗口确认
    /// 完整时的下一次 flush 再评估——命中补输出，仍 miss 为真 miss（此时任务
    /// 即将退出，保留与否无差别）。命中则补输出。
    pub(super) async fn reevaluate_deferred_missed(&mut self) {
        let missed = {
            let Some(deferred) = self.deferred.as_mut() else {
                return;
            };
            std::mem::take(&mut deferred.missed)
        };
        if missed.is_empty() {
            return;
        }
        let join_idx = self
            .deferred
            .as_ref()
            .expect("deferred state exists")
            .join_idx;
        let lookup = RegistryLookup::new(&self.router);
        let missed_len = missed.len();
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
        let mut hit = 0usize;
        let mut still_miss = Vec::with_capacity(missed_len.min(64));
        for p in missed {
            match self
                .executor
                .execute_deferred_join(join_idx, &p, &lookup, wall_nanos() as i64)
            {
                Ok(Some(record)) => {
                    hit += 1;
                    if debug_enabled {
                        log_output_emitted(
                            "execute_deferred",
                            "deferred-eos-retry",
                            output_kind(&record, &self.intermediate_targets),
                            &record,
                            &[],
                        );
                    }
                    self.emit(record).await;
                }
                // 仍 miss：不判定为真 miss——窗口可能仍不完整（keep-running
                // EOS 竞态）。保留回 missed，等下一次 flush（窗口完整后）。
                Ok(None) => still_miss.push(p),
                Err(e) => {
                    wf_warn!(
                        pipe,
                        task_id = %self.task_id,
                        rule = %self.rule_name(),
                        stage = 0,
                        phase = "execute_deferred_eos_retry",
                        error = %e,
                        "deferred join EOS retry failed"
                    );
                }
            }
        }
        let still_miss_len = still_miss.len();
        if !still_miss.is_empty()
            && let Some(deferred) = self.deferred.as_mut()
        {
            deferred.missed.extend(still_miss);
        }
        // 2026-08-25：missed 不再参与 pin/lo_min，取空重建无需标 dirty。
        //（lo_min 只由 pending 插入维护 + 空集/缓存失效回退全量扫。）
        if hit > 0 && debug_enabled {
            wf_debug!(
                pipe,
                task_id = %self.task_id,
                rule = %self.rule_name(),
                missed = missed_len,
                hit = hit,
                still_miss = still_miss_len,
                "deferred EOS retry: missed instances re-evaluated (still-miss preserved for the next flush)"
            );
        }
    }

    pub(crate) async fn scan_timeouts(&mut self) {
        // P3：deferred join 规则（无 machine）——事件时间 watermark 到期扫描
        //（不叠加墙钟：replay 对拍依赖事件时间序，墙钟推进会提前触发）。
        if self.machine.is_none() && self.deferred.is_some() {
            let wm = self
                .deferred
                .as_ref()
                .map(|d| d.watermark)
                .unwrap_or(i64::MIN);
            if wm > i64::MIN {
                self.scan_deferred(wm, wall_nanos() as i64, true).await;
            }
            // D4：空闲/超时扫描也发布保留前沿（到期实例可能已在此退场）。
            // 注：尚未见过驱动事件时这里会发布 i64::MIN（全保留），而不是释放——
            // 参见 `publish_retention_floor` 的 ⚠ 注释。
            if let Some(d) = self.deferred.as_mut() {
                d.publish_retention_floor();
            }
            return;
        }
        let Some(machine) = &self.machine else {
            return;
        };
        self.cached_wall_nanos
            .store(wall_nanos(), Ordering::Relaxed);
        // 2026-08-23 q11 修复：session 窗口是纯事件时间语义（gap = 事件时间
        // 间隔、会话随事件延长）——墙钟推进会把数据末尾未超时的尾部会话提前
        // 扫出（10M replay 多 204/197095≈0.1%），与 deferred 分支同源（replay
        // 对拍依赖事件时间序，墙钟推进会提前触发）。session 不叠加墙钟；
        // fixed/sliding/hop 保留墙钟兜底（q16 30M 尾桶收口依赖该扫）。
        let event_watermark = machine.watermark_nanos();
        let effective_watermark = if matches!(machine.plan().window_spec, WindowSpec::Session(_)) {
            event_watermark
        } else {
            // Advance the effective watermark by the wall-clock time elapsed since the
            // last event was processed — **capped at one scan interval per scan**. This
            // lets idle instances expire per their window TTL (window semantics, not
            // just event-time), while bounding each sweep: a slow/backpressured
            // pipeline cannot accumulate minutes of wall-clock and snowball into a
            // huge single expiry sweep that starves push consumption (q5/q6/q7 froze
            // at ~22-25M appends on 30M data before this cap).
            //
            // 2026-09-02 memory_stability：单次扫描消费 ≤ interval 的墙钟并把它记入
            // `wall_advance_ns`（累计信用），effective watermark = 事件 watermark +
            // 信用 —— 多次扫描累计到 TTL。旧实现每次扫描都从冻结锚点量「总 idle」再
            // min(·, interval) 直接当推进量，累计被钉死在 interval，TTL > interval 的
            // idle 实例永不释放（daemon instances 钉 10000 不降）。真实事件批处理会
            // 清零信用（事件时间本身推进会覆盖 idle 推进）。
            let wall_advance = self
                .last_activity_wall
                .elapsed()
                .min(self.timeout_scan_interval)
                .as_nanos() as i64;
            self.wall_advance_ns += wall_advance;
            event_watermark.saturating_add(self.wall_advance_ns)
        };
        let started = Instant::now();
        // No input batch is being processed here (timeout scan), so the window
        // lookups read the full window (no `max_seq` watermark).
        let lookup = RegistryLookup::new(&self.router);
        // P2c: shards of a conv rule route raw closes to the conv stage.
        let (rule_name, closes, routed) = {
            let machine = self.machine.as_mut().expect("checked above");
            let rule_name = machine.rule_name().to_string();
            // 注入本次扫描的处理墙钟（issue #82）——与 cached_wall_nanos 同源
            // （闭置后的墙钟兜底扫描也刚刷新过它）。
            machine.set_processing_wall(self.cached_wall_nanos.load(Ordering::Relaxed) as i64);
            if self.conv_sink.is_some() {
                // Timeout scan runs off the event hot path (pipeline idle), so it
                // uses the **unbounded** expiry budget: fixed-window rules whose
                // final bucket expires past the last event time depend on this
                // sweep to close (q16 30M dropped the final bucket otherwise).
                let raw = machine.scan_expired_at_skip_non_alerting_unbounded(effective_watermark);
                // Barrier watermark = the effective (wall-clock advanced) scan
                // watermark, so an idle shard still advances its barrier and the
                // conv stage can seal buckets for the whole rule (without this,
                // an idle shard's stale barrier starves sealing forever).
                let watermark = effective_watermark;
                let qualifying: Vec<_> = raw.into_iter().filter(close_is_qualified).collect();
                if let Some(sink) = self.conv_sink.as_ref() {
                    // P3-D: log when the conv stage is gone (closes dropped).
                    if sink
                        .tx
                        .send(ConvCloseBatch {
                            closes: qualifying,
                            watermark,
                            drained: false,
                            barrier_index: sink.barrier_index,
                        })
                        .await
                        .is_err()
                    {
                        log::debug!("conv sink channel closed — scan batch dropped");
                    }
                }
                (rule_name, Vec::new(), true)
            } else {
                (
                    rule_name,
                    machine.scan_expired_at_with_conv_skip_non_alerting_unbounded(
                        effective_watermark,
                        self.conv_plan.as_ref(),
                    ),
                    false,
                )
            }
        };
        let mut stats = RuleBatchDebugStats::default();
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
        // When routed to the conv stage, skip inline close processing.
        if !routed {
            for close in &closes {
                match self.executor.execute_close_with_joins(close, &lookup) {
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
                        self.emit(record).await;
                    }
                    Ok(None) => {
                        if debug_enabled {
                            stats.output_none += 1;
                        }
                        if debug_enabled && stats.allow_detail() {
                            log_output_suppressed(
                                &rule_name,
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
                            task_id = %self.task_id,
                            rule = %rule_name,
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
        if debug_enabled {
            let instances_after = self.instance_count();
            wf_debug!(
                pipe,
                task_id = %self.task_id,
                rule = %rule_name,
                stage = 0,
                closes = closes.len(),
                outputs = stats.output_emitted,
                output_none = stats.output_none,
                intermediate_outputs = stats.intermediate_emitted,
                errors = stats.errors,
                instances_after = instances_after,
                detail_logged = stats.detail_logged,
                detail_suppressed = stats.detail_suppressed,
                "rule timeout scan summary"
            );
            if stats.detail_suppressed > 0 {
                wf_debug!(
                    pipe,
                    task_id = %self.task_id,
                    rule = %rule_name,
                    stage = 0,
                    detail_logged = stats.detail_logged,
                    detail_suppressed = stats.detail_suppressed,
                    "rule event details suppressed"
                );
            }
        }
        // Re-anchor the O(1) per-instance base-cost memory estimate to the exact
        // sum of live instance state (accumulated field_values / distinct_set
        // growth is otherwise invisible to the running estimate).
        if let Some(machine) = self.machine.as_mut() {
            machine.recalibrate_memory();
        }
        if let Some(metrics) = &self.metrics {
            metrics.observe_rule_scan_timeout(&rule_name, started.elapsed());
            self.update_rule_instances_metric();
        }
        // Timeout closes may have staged intermediate rows — deliver them.
        self.flush_pipes().await;
    }

    /// Close all active instances (shutdown flush) and emit alerts.
    pub(crate) async fn flush(&mut self) {
        // P3：deferred join 规则——EOS/关闭时触发剩余挂起实例
        // （reason=deferred）。按最终事件时间 watermark 到期扫描（与 oracle 一致）：
        // 尾部 expiry > 最终事件时间的实例窗口未完成（事件时间域），不输出——
        // 用 i64::MAX 强评会多出尾部桶（Q8 实证：82446 → 83274，+828 条，
        // oracle/mod.rs EOS 水位扫注释同源）。missed（到期时 join 目标 append
        // 滞后）在窗口完整后重试一次，仍 miss 为真 miss。
        //
        // 2026-08-24 q4/q9 分片后：worker 自身 watermark 停在**最后批次**的事件
        // 时间（其他 worker 拿到更晚批次）→ 只用自身 watermark 会漏掉
        // expiry ≤ 数据末尾的 pending（q4 30M 丢 869 条实测）。改用**驱动窗口
        // 的全局最终事件时间**（共享窗口 max_event_time = true global data
        // tail）——与单 worker 的最终 watermark 同语义：expiry ≤ 末尾全评估，
        // > 末尾不输出。
        if self.machine.is_none() && self.deferred.is_some() {
            let final_wm = self
                .sources
                .iter()
                .map(|s| s.window.max_event_time_nanos())
                .max()
                .unwrap_or(i64::MIN);
            // 2026-08-25 更正：**驱动窗** max 即全局末尾——oracle 的 deferred
            // watermark 语义 = 最后驱动事件时间（wfgen oracle/mod.rs 469 行），
            // 不是 max(驱动, 目标)。曾把目标窗 max 并入（bid 流尾晚 4.6ms）
            // → expiry ∈ (驱动末尾, 目标末尾] 的尾部实例被评估 → 10M +2 多发
            // （oracle 557,204 vs 引擎 557,206，2026-08-25 实测）。
            if final_wm > i64::MIN {
                // 2026-08-25 q4 100M over=30m 欠发修复：flush 时 join 目标窗
                // 的 actor 可能仍在排空 mailbox（keep-running EOS 竞态）——目标
                // 窗提交前沿落后 final_wm，尾部 pending（expiry 近数据末）评估时
                // 右行未落地 → miss → 重试仍 miss → 退出丢行（100M 实测欠发
                // 6-7k 条 ≈ 尾部 10-12s，over=1h 时 ~0-850）。EOS 后全部输入已
                // ingest，actor 必然排空：这里限时等目标窗**提交前沿停止增长**
                // （连续 ~60ms 无新提交），然后一次评估全命中。用
                // `committed_frontier_ns`（各 source 已提交 max 的 min）而非全局
                // max——跨源乱序提交下 max 可能停滞后前沿仍在推进。
                // 不能等 `前沿 ≥ final_wm`：数据尾部 bid/auction 流 max
                // 天然差几行（最后的事件可能是 auction）→ 永远追不平。
                let join_idx = self
                    .deferred
                    .as_ref()
                    .expect("deferred state exists")
                    .join_idx;
                let target = self.executor.plan().joins[join_idx].right_window.clone();
                let mut last_wm: Option<i64> = None;
                let mut stalled = 0u32;
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                loop {
                    let target_wm = self
                        .router
                        .registry()
                        .get_window(&target)
                        .map(|w| w.committed_frontier_ns())
                        .unwrap_or(i64::MAX);
                    if target_wm == i64::MAX {
                        break; // 目标不存在/无时间列（防御：不等待）
                    }
                    if Some(target_wm) == last_wm {
                        stalled += 1;
                        if stalled >= 3 {
                            break; // 连续 ~60ms 无增长 → actor 已排空
                        }
                    } else {
                        stalled = 0;
                    }
                    last_wm = Some(target_wm);
                    if std::time::Instant::now() >= deadline {
                        break; // 限时兜底（理论上到不了）
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                self.scan_deferred(final_wm, wall_nanos() as i64, false)
                    .await;
            }
            // EOS 重试（2026-08-23 q8 修复）：到期时 join 目标窗口 append 滞后
            // 的 miss 实例——EOS 后所有数据已 ingest、目标窗口完整，重试命中
            // （真 miss 重试后仍不输出）。oracle 预加载完整窗口即此理想值。
            self.reevaluate_deferred_missed().await;
            // D4：EOS 后本规则不再需要右窗任何行 → 释放保留 pin（窗口恢复完全
            // 可驱逐，关停阶段不再顶着字节预算）。
            if let Some(d) = self.deferred.as_ref() {
                d.release_retention_floor();
            }
            self.flush_alerts().await;
            self.flush_pipes().await;
            return;
        }
        let Some(_) = &self.machine else {
            // on-each（无 match 状态机、无 deferred）：运行期 emit 按
            // ALERT_BATCH_SIZE 满批 flush，但**最后一个未满批**（<4096）留在
            // pending builder / pipe staging——关机 flush 必须补一次收口，否则
            // 该批被静默丢弃（q1 920000→917469 尾批丢失根因，2026-08-28
            // verify_file.sh 定位；metrics 在 emit 时已计数，只有文件/EMIT
            // 对拍才暴露）。
            self.flush_alerts().await;
            self.flush_pipes().await;
            return;
        };
        self.cached_wall_nanos
            .store(wall_nanos(), Ordering::Relaxed);
        let started = Instant::now();
        // Shutdown flush is not processing any single input batch, so window
        // lookups read the full window (no `max_seq` watermark).
        let lookup = RegistryLookup::new(&self.router);
        // P2c: on flush a conv-rule shard routes ALL remaining raw closes to the
        // conv stage and publishes a drained barrier (i64::MAX via the batch).
        let (rule_name, closes, routed) = {
            let machine = self.machine.as_mut().expect("checked above");
            let rule_name = machine.rule_name().to_string();
            // 注入本次 flush 的处理墙钟（issue #82）——收口 close 首次命中的
            // 处理时钟与 emit 侧 cached_wall_nanos 同源。
            machine.set_processing_wall(self.cached_wall_nanos.load(Ordering::Relaxed) as i64);
            // 2026-08-23 q11 修复（分片尾部边界）：机器水位 = 本 shard 最后
            // 处理行，分片下落后全局数据末尾（尾部几行 bid 的 bidder 在其它
            // shard）——尾部会话 `last_event+gap ≤ 全局末尾` 的会被 close_all
            // 误判未完整而跳过（q11 10M 实测少 1/197095≈0.0005%）。用窗口的
            // raw `max_event_time`（全局末尾）先补扫一次（unbounded，off hot
            // path），再 close_all 收口剩余（expiry > 全局末尾 的仍跳过）。
            let machine_wm = machine.watermark_nanos();
            let final_wm = self
                .sources
                .iter()
                .map(|src| src.window.max_event_time_nanos())
                .max()
                .unwrap_or(machine_wm)
                .max(machine_wm);
            if self.conv_sink.is_some() {
                let mut extra = Vec::new();
                if final_wm > machine_wm {
                    extra = machine.scan_expired_at_skip_non_alerting_unbounded(final_wm);
                }
                let raw = machine.close_all(CloseReason::Flush);
                let watermark = final_wm.max(machine.watermark_nanos());
                let mut qualifying: Vec<_> = raw.into_iter().filter(close_is_qualified).collect();
                qualifying.extend(extra.into_iter().filter(close_is_qualified));
                if let Some(sink) = self.conv_sink.as_ref() {
                    // P3-D: log when the conv stage is gone (drained closes dropped).
                    if sink
                        .tx
                        .send(ConvCloseBatch {
                            closes: qualifying,
                            watermark,
                            drained: true,
                            barrier_index: sink.barrier_index,
                        })
                        .await
                        .is_err()
                    {
                        log::debug!("conv sink channel closed — drained flush dropped");
                    }
                }
                (rule_name, Vec::new(), true)
            } else {
                let mut closes = Vec::new();
                if final_wm > machine_wm {
                    closes = machine.scan_expired_at_with_conv_skip_non_alerting_unbounded(
                        final_wm,
                        self.conv_plan.as_ref(),
                    );
                }
                closes.extend(
                    machine.close_all_with_conv(CloseReason::Flush, self.conv_plan.as_ref()),
                );
                (rule_name, closes, false)
            }
        };
        let mut stats = RuleBatchDebugStats::default();
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
        // When routed to the conv stage, skip inline close processing.
        if !routed {
            for close in &closes {
                match self.executor.execute_close_with_joins(close, &lookup) {
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
                        self.emit(record).await;
                    }
                    Ok(None) => {
                        if debug_enabled {
                            stats.output_none += 1;
                        }
                        if debug_enabled && stats.allow_detail() {
                            log_output_suppressed(
                                &rule_name,
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
                            task_id = %self.task_id,
                            rule = %rule_name,
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
        if debug_enabled {
            let instances_after = self.instance_count();
            wf_debug!(
                pipe,
                task_id = %self.task_id,
                rule = %rule_name,
                stage = 0,
                closes = closes.len(),
                outputs = stats.output_emitted,
                output_none = stats.output_none,
                intermediate_outputs = stats.intermediate_emitted,
                errors = stats.errors,
                instances_after = instances_after,
                detail_logged = stats.detail_logged,
                detail_suppressed = stats.detail_suppressed,
                "rule flush summary"
            );
            if stats.detail_suppressed > 0 {
                wf_debug!(
                    pipe,
                    task_id = %self.task_id,
                    rule = %rule_name,
                    stage = 0,
                    detail_logged = stats.detail_logged,
                    detail_suppressed = stats.detail_suppressed,
                    "rule event details suppressed"
                );
            }
        }
        if let Some(metrics) = &self.metrics {
            metrics.observe_rule_flush(&rule_name, started.elapsed());
            self.update_rule_instances_metric();
        }
        // Drain the batched alert delivery after close emissions.
        self.flush_alerts().await;
        // Drain staged intermediate rows after close emissions（on-each 规则已
        // 在上面分支补收口，这里只覆盖 match 规则 close 发射装载的中间行）。
        self.flush_pipes().await;
    }
}
