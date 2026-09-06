//! `RuntimeMetrics` 核心计数面 + 注册/采样/drain 接口（metrics 结构拆件，
//! 2026-09-04）。
//!
//! [`RuntimeMetrics`] 的 struct 声明留父层 `mod.rs`（字段私有于 metrics 子树，
//! sampling/server 与各测试仍可直读）；全量 `impl` 下沉本文件：计数接口
//! （receiver/router/rule/alert/window/evict 的 inc/observe）、注册（`new`）、
//! 采样（`interval_snapshot`/`interval_rates`/`interval_table`/`summary_line`）
//! 与 drain 成快照（`snapshot` + drain/read helpers）。类型/常量/helper 经
//! `use super::*` 从父层取得。

use super::*;

impl RuntimeMetrics {
    fn total_rule_matches(&self) -> u64 {
        self.rule_matches_total
            .values()
            .map(|v| v.load(Ordering::Relaxed))
            .sum()
    }

    fn total_rule_instances(&self) -> u64 {
        self.rule_instances
            .values()
            .map(|v| v.load(Ordering::Relaxed))
            .map(|v| v.max(0) as u64)
            .sum()
    }

    fn total_alert_dispatch(&self) -> u64 {
        self.alert_dispatch_total.load(Ordering::Relaxed)
    }

    fn total_window_bytes(&self) -> u64 {
        self.window_memory_bytes
            .values()
            .map(|v| v.load(Ordering::Relaxed))
            .sum()
    }

    pub(crate) fn interval_snapshot(&self, at: Instant) -> IntervalSnapshot {
        IntervalSnapshot {
            at,
            rx_rows: self.receiver_rows_total.load(Ordering::Relaxed),
            dropped_late: self.router_dropped_late_total.load(Ordering::Relaxed),
            rule_matches: self.total_rule_matches(),
            rule_instances: self.total_rule_instances(),
            alert_dispatch: self.total_alert_dispatch(),
            window_bytes: self.total_window_bytes(),
        }
    }

    pub(crate) fn interval_rates(
        &self,
        prev: IntervalSnapshot,
        curr: IntervalSnapshot,
    ) -> Option<IntervalRates> {
        let secs = (curr.at - prev.at).as_secs_f64();
        if secs <= 0.0 {
            return None;
        }

        Some(IntervalRates {
            row_s: curr.rx_rows.saturating_sub(prev.rx_rows) as f64 / secs,
            late_s: curr.dropped_late.saturating_sub(prev.dropped_late) as f64 / secs,
            rules_s: curr.rule_matches.saturating_sub(prev.rule_matches) as f64 / secs,
            sm_s: (curr.rule_instances as f64 - prev.rule_instances as f64) / secs,
            out_s: curr.alert_dispatch.saturating_sub(prev.alert_dispatch) as f64 / secs,
            memory_bytes: curr.window_bytes,
        })
    }

    pub(crate) fn interval_table(&self, rates: IntervalRates) -> String {
        let mem = format_bytes(rates.memory_bytes);
        format!(
            "\n+-----------+-----------+-----------+-----------+-------------+-----------+\n\
             | row/s     | late/s    | rules/s   | sm/s      | memory      | out/s     |\n\
             +-----------+-----------+-----------+-----------+-------------+-----------+\n\
             | {row_s:>9.1} | {late_s:>9.1} | {rules_s:>9.1} | {sm_s:>9.1} | {mem:>11} | {out_s:>9.1} |\n\
             +-----------+-----------+-----------+-----------+-------------+-----------+",
            row_s = rates.row_s,
            late_s = rates.late_s,
            rules_s = rates.rules_s,
            sm_s = rates.sm_s,
            out_s = rates.out_s,
        )
    }

    pub fn new(
        rule_names: &[String],
        window_names: &[String],
        source_names: &[String],
        source_types: BTreeMap<String, String>,
    ) -> Self {
        let make_rule_map = || {
            rule_names
                .iter()
                .map(|name| (name.clone(), AtomicU64::new(0)))
                .collect::<BTreeMap<_, _>>()
        };
        // Signed gauge map for `rule_instances` (sum across shards, P2b).
        let make_rule_map_i64 = || {
            rule_names
                .iter()
                .map(|name| (name.clone(), AtomicI64::new(0)))
                .collect::<BTreeMap<_, _>>()
        };
        let make_rule_hist_map = || {
            rule_names
                .iter()
                .map(|name| {
                    (
                        name.clone(),
                        Histogram::from_seconds_bounds(DEFAULT_HISTOGRAM_BUCKETS_SECONDS),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        let make_window_map = || {
            window_names
                .iter()
                .map(|name| (name.clone(), AtomicU64::new(0)))
                .collect::<BTreeMap<_, _>>()
        };
        let make_source_map = || {
            source_names
                .iter()
                .map(|name| (name.clone(), AtomicU64::new(0)))
                .collect::<BTreeMap<_, _>>()
        };
        let mut gap_map = BTreeMap::new();
        for rule in rule_names {
            let mut by_window = BTreeMap::new();
            for window in window_names {
                by_window.insert(window.clone(), AtomicU64::new(0));
            }
            gap_map.insert(rule.clone(), by_window);
        }

        Self {
            receiver_connections_total: AtomicU64::new(0),
            receiver_frames_total: AtomicU64::new(0),
            receiver_rows_total: AtomicU64::new(0),
            receiver_decode_errors_total: AtomicU64::new(0),
            receiver_read_errors_total: AtomicU64::new(0),
            source_types,
            receiver_source_rows_total: make_source_map(),
            receiver_source_machine_rows: Mutex::new(BTreeMap::new()),
            receiver_source_decode_errors_total: make_source_map(),
            receiver_source_read_errors_total: make_source_map(),
            receiver_window_miss_total: Mutex::new(BTreeMap::new()),
            router_route_calls_total: AtomicU64::new(0),
            router_delivered_total: AtomicU64::new(0),
            router_dropped_late_total: AtomicU64::new(0),
            router_skipped_non_local_total: AtomicU64::new(0),
            router_source_route_errors_total: make_source_map(),
            rule_events_total: make_rule_map(),
            rule_matches_total: make_rule_map(),
            rule_instances: make_rule_map_i64(),
            rule_memory_bytes: make_rule_map_i64(),
            rule_cursor_gap_total: gap_map,
            rule_stats_over_limit_total: make_rule_map(),
            alert_emitted_total: make_rule_map(),
            alert_emitted_detail_shards: (0..ALERT_DETAIL_SHARDS)
                .map(|_| Mutex::new(BTreeMap::new()))
                .collect(),
            alert_channel_send_failed_total: AtomicU64::new(0),
            alert_sink_dispatch_failed_total: AtomicU64::new(0),
            alert_channel_full_total: AtomicU64::new(0),
            alert_channel_depth: AtomicU64::new(0),
            alert_append_failed_total: AtomicU64::new(0),
            alert_dispatch_total: AtomicU64::new(0),
            alert_no_sink_records_total: AtomicU64::new(0),
            alert_drain_dropped_records_total: AtomicU64::new(0),
            alert_escalate_failed_total: AtomicU64::new(0),
            alert_append_nanos: AtomicU64::new(0),
            evictor_sweeps_total: AtomicU64::new(0),
            evictor_time_evicted_total: AtomicU64::new(0),
            evictor_memory_evicted_total: AtomicU64::new(0),
            window_memory_bytes: make_window_map(),
            window_allocated_bytes: make_window_map(),
            window_fanout_queued: make_window_map(),
            window_fanout_capacity: make_window_map(),
            window_mailbox_inflight: make_window_map(),
            window_mailbox_budget: make_window_map(),
            window_capacity_bytes: make_window_map(),
            window_rows: make_window_map(),
            window_batches: make_window_map(),
            window_acked_lag: make_window_map(),
            window_append_total: make_window_map(),
            window_evict_total: make_window_map(),
            window_late_total: make_window_map(),
            receiver_decode_seconds: Histogram::from_seconds_bounds(
                DEFAULT_HISTOGRAM_BUCKETS_SECONDS,
            ),
            alert_dispatch_seconds: Histogram::from_seconds_bounds(
                DEFAULT_HISTOGRAM_BUCKETS_SECONDS,
            ),
            event_e2e_latency_seconds: Histogram::from_seconds_bounds(
                DEFAULT_HISTOGRAM_BUCKETS_SECONDS,
            ),
            rule_scan_timeout_seconds: make_rule_hist_map(),
            rule_flush_seconds: make_rule_hist_map(),
        }
    }

    pub fn inc_receiver_connection(&self) {
        self.receiver_connections_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_receiver_frame(&self, rows: usize) {
        self.receiver_frames_total.fetch_add(1, Ordering::Relaxed);
        self.receiver_rows_total
            .fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub fn add_receiver_source_frame(&self, source: &str, rows: usize) {
        if let Some(v) = self.receiver_source_rows_total.get(source) {
            v.fetch_add(rows as u64, Ordering::Relaxed);
        }
    }

    pub fn add_receiver_source_machine_rows(&self, source: &str, machine_id: &str, rows: usize) {
        if machine_id.is_empty() {
            return;
        }
        let mut map = self.receiver_source_machine_rows.lock().unwrap();
        let by_machine = map.entry(source.to_string()).or_default();
        let v = by_machine
            .entry(machine_id.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        v.fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub fn inc_receiver_decode_error(&self) {
        self.receiver_decode_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_receiver_source_decode_error(&self, source: &str) {
        if let Some(v) = self.receiver_source_decode_errors_total.get(source) {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn observe_receiver_decode(&self, elapsed: Duration) {
        self.receiver_decode_seconds.observe_duration(elapsed);
    }

    pub fn inc_receiver_read_error(&self) {
        self.receiver_read_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_receiver_source_read_error(&self, source: &str) {
        if let Some(v) = self.receiver_source_read_errors_total.get(source) {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn add_receiver_window_miss(&self, source: &str, reason: &str, count: usize) {
        if count == 0 {
            return;
        }
        let mut map = self.receiver_window_miss_total.lock().unwrap();
        let by_reason = map.entry(source.to_string()).or_default();
        let v = by_reason
            .entry(reason.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        v.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn inc_router_route_call(&self) {
        self.router_route_calls_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Window-actor path report for one appended window batch (the actor-side
    /// equivalent of folding one `WindowRouteOutcome` into the route totals).
    pub fn report_window_append(&self, window: &str, rows: usize, late: bool) {
        if late {
            self.router_dropped_late_total
                .fetch_add(1, Ordering::Relaxed);
            self.add_window_late(window, rows as u64);
        } else {
            self.router_delivered_total.fetch_add(1, Ordering::Relaxed);
            self.add_window_append(window, rows as u64);
        }
    }

    /// Skip count for non-local subscribers, reported by the parse worker on
    /// the direct-dispatch path.
    pub fn add_router_skipped(&self, count: usize) {
        self.router_skipped_non_local_total
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn add_route_report(&self, report: &RouteReport) {
        self.router_delivered_total
            .fetch_add(report.delivered as u64, Ordering::Relaxed);
        self.router_dropped_late_total
            .fetch_add(report.dropped_late as u64, Ordering::Relaxed);
        self.router_skipped_non_local_total
            .fetch_add(report.skipped_non_local as u64, Ordering::Relaxed);
        for w in &report.per_window {
            if w.late {
                self.add_window_late(&w.window_name, w.rows as u64);
            } else {
                self.add_window_append(&w.window_name, w.rows as u64);
            }
        }
    }

    pub fn inc_route_error(&self, source: &str) {
        if let Some(v) = self.router_source_route_errors_total.get(source) {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn add_rule_events(&self, rule: &str, count: usize) {
        if let Some(v) = self.rule_events_total.get(rule) {
            v.fetch_add(count as u64, Ordering::Relaxed);
        }
    }

    pub fn inc_rule_match(&self, rule: &str) {
        if let Some(v) = self.rule_matches_total.get(rule) {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Adjust the `rule_instances` gauge by a signed delta.
    ///
    /// Each shard of a sharded rule reports `current_count - last_reported`
    /// so the gauge is the *sum* across shards (P2b). A single reporter
    /// (shards=1) yields the same numeric value as the old overwriting store.
    pub fn adjust_rule_instances(&self, rule: &str, delta: i64) {
        if let Some(v) = self.rule_instances.get(rule) {
            v.fetch_add(delta, Ordering::Relaxed);
        }
    }

    /// Adjust the `rule_memory_bytes` gauge by a signed delta（同实例计数：
    /// 每 shard 报 current−last，内存可增可减、recalibrate 会校正，故也走 delta）。
    pub fn adjust_rule_memory_bytes(&self, rule: &str, delta: i64) {
        if let Some(v) = self.rule_memory_bytes.get(rule) {
            v.fetch_add(delta, Ordering::Relaxed);
        }
    }

    pub fn inc_rule_cursor_gap(&self, rule: &str, window: &str) {
        if let Some(by_window) = self.rule_cursor_gap_total.get(rule)
            && let Some(v) = by_window.get(window)
        {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 状态内存 guard 超限拒收增量（stats close 时按窗口 delta 上报; 0 增量自动 no-op）。
    pub fn inc_rule_stats_over_limit(&self, rule: &str, delta: u64) {
        if delta == 0 {
            return;
        }
        if let Some(v) = self.rule_stats_over_limit_total.get(rule) {
            v.fetch_add(delta, Ordering::Relaxed);
        }
    }

    pub fn inc_alert_emitted(&self, rule: &str, machine_id: &str, scope_key: &str) {
        self.inc_alert_emitted_total(rule);
        self.inc_alert_emitted_detail(rule, machine_id, scope_key);
    }

    /// Cheap exact total: one relaxed atomic increment, no allocation.
    pub fn inc_alert_emitted_total(&self, rule: &str) {
        if let Some(v) = self.alert_emitted_total.get(rule) {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Per-machine/per-scope detail — allocation + sharded mutex per call.
    /// Callers on hot alert paths should sample this (see rule_task emit).
    pub fn inc_alert_emitted_detail(&self, rule: &str, machine_id: &str, scope_key: &str) {
        if machine_id.is_empty() && scope_key.is_empty() {
            return;
        }
        let machine = if machine_id.is_empty() {
            "-"
        } else {
            machine_id
        };
        let scope = if scope_key.is_empty() { "-" } else { scope_key };
        // Sharded by rule: only this rule's task contends on its own shard.
        let mut map = self.alert_emitted_detail_shards[alert_detail_shard(rule)]
            .lock()
            .unwrap();
        let by_machine = map.entry(rule.to_string()).or_default();
        let by_scope = by_machine.entry(machine.to_string()).or_default();
        if by_scope.len() >= ALERT_DETAIL_MAX_SCOPES_PER_RULE && !by_scope.contains_key(scope) {
            // Bounded: count only in alert_emitted_total for new scopes.
            return;
        }
        let v = by_scope
            .entry(scope.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        v.fetch_add(1, Ordering::Relaxed);
    }

    fn drain_source_machine_rows(&self) -> BTreeMap<String, BTreeMap<String, u64>> {
        Self::drain_second_level(&self.receiver_source_machine_rows)
    }

    fn drain_emitted_detail(&self) -> BTreeMap<String, BTreeMap<String, BTreeMap<String, u64>>> {
        let mut result = BTreeMap::new();
        for shard in &self.alert_emitted_detail_shards {
            // Swap the whole shard out so the lock is held only for the O(1)
            // `take`, never while iterating a large scope space (the previous
            // global lock was held across the full drain every interval).
            let taken = {
                let mut map = shard.lock().unwrap();
                std::mem::take(&mut *map)
            };
            for (rule, by_machine) in taken {
                let mut machine_map = BTreeMap::new();
                for (machine, by_scope) in by_machine {
                    let drained: BTreeMap<String, u64> = by_scope
                        .into_iter()
                        .map(|(scope, v)| (scope, v.into_inner()))
                        .filter(|(_, v)| *v > 0)
                        .collect();
                    if !drained.is_empty() {
                        machine_map.insert(machine, drained);
                    }
                }
                if !machine_map.is_empty() {
                    result.insert(rule, machine_map);
                }
            }
        }
        result
    }

    fn drain_receiver_window_misses(&self) -> BTreeMap<String, BTreeMap<String, u64>> {
        Self::drain_second_level(&self.receiver_window_miss_total)
    }

    /// 二维计数（`String → String → AtomicU64`）的原位 drain：内层逐计数 swap 清零,
    /// 过滤掉 0, 空内层不出现。receiver source-machine / window-miss 两图共用。
    fn drain_second_level(
        m: &std::sync::Mutex<BTreeMap<String, BTreeMap<String, AtomicU64>>>,
    ) -> BTreeMap<String, BTreeMap<String, u64>> {
        let map = m.lock().unwrap();
        let mut result = BTreeMap::new();
        for (key, inner) in map.iter() {
            let drained: BTreeMap<String, u64> = inner
                .iter()
                .map(|(sub, v)| (sub.clone(), v.swap(0, Ordering::Relaxed)))
                .filter(|(_, v)| *v > 0)
                .collect();
            if !drained.is_empty() {
                result.insert(key.clone(), drained);
            }
        }
        result
    }

    pub fn inc_alert_channel_send_failed(&self) {
        self.alert_channel_send_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_sink_dispatch_failed(&self) {
        self.alert_sink_dispatch_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_alert_append_failed(&self) {
        self.alert_append_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records delivered to a yield target that has no matching sink and were
    /// dropped (silent degradation made explicit, ⑤).
    pub fn add_alert_no_sink_records(&self, records: u64) {
        self.alert_no_sink_records_total
            .fetch_add(records, Ordering::Relaxed);
    }

    /// Records still buffered in a sink channel when the shutdown drain budget
    /// ran out and were dropped.
    pub fn add_sink_drain_dropped_records(&self, records: u64) {
        self.alert_drain_dropped_records_total
            .fetch_add(records, Ordering::Relaxed);
    }

    /// Escalation of a failed dispatch batch to the error sinks failed.
    pub fn inc_alert_escalate_failed(&self) {
        self.alert_escalate_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Nanoseconds spent on the record→列 append（`AlertColumnBuilder::append_record`）
    /// in the rule workers' emit path（worker 侧输出构建; 与 sink 侧序列化区分,
    /// per-run counter, drained each export interval）。
    pub fn add_alert_append_nanos(&self, nanos: u64) {
        self.alert_append_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    pub fn inc_alert_dispatch(&self) {
        self.alert_dispatch_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_alert_dispatch(&self, elapsed: Duration) {
        self.alert_dispatch_seconds.observe_duration(elapsed);
    }

    pub fn observe_rule_scan_timeout(&self, rule: &str, elapsed: Duration) {
        if let Some(hist) = self.rule_scan_timeout_seconds.get(rule) {
            hist.observe_duration(elapsed);
        }
    }

    pub fn inc_alert_channel_full(&self) {
        self.alert_channel_full_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn set_alert_channel_depth(&self, depth: u64) {
        self.alert_channel_depth.store(depth, Ordering::Relaxed);
    }
    pub fn observe_event_e2e_latency(&self, elapsed: Duration) {
        self.event_e2e_latency_seconds.observe_duration(elapsed);
    }
    pub fn add_window_append(&self, window: &str, count: u64) {
        if let Some(c) = self.window_append_total.get(window) {
            c.fetch_add(count, Ordering::Relaxed);
        }
    }
    pub fn add_window_evict(&self, window: &str, count: u64) {
        if let Some(c) = self.window_evict_total.get(window) {
            c.fetch_add(count, Ordering::Relaxed);
        }
    }
    pub fn add_window_late(&self, window: &str, count: u64) {
        if let Some(c) = self.window_late_total.get(window) {
            c.fetch_add(count, Ordering::Relaxed);
        }
    }
    pub fn observe_rule_flush(&self, rule: &str, elapsed: Duration) {
        if let Some(hist) = self.rule_flush_seconds.get(rule) {
            hist.observe_duration(elapsed);
        }
    }
    pub(crate) fn snapshot(&self) -> MetricsSnapshot {
        let source_types = self.source_types.clone();
        MetricsSnapshot {
            source_types,
            receiver_connections: self.drain_counter(&self.receiver_connections_total),
            receiver_frames: self.drain_counter(&self.receiver_frames_total),
            receiver_source_rows: self.drain_map(&self.receiver_source_rows_total),
            receiver_source_machine_rows: self.drain_source_machine_rows(),
            receiver_source_decode_errors: self
                .drain_map(&self.receiver_source_decode_errors_total),
            receiver_source_read_errors: self.drain_map(&self.receiver_source_read_errors_total),
            receiver_window_misses: self.drain_receiver_window_misses(),
            router_route_calls: self.drain_counter(&self.router_route_calls_total),
            router_delivered: self.drain_counter(&self.router_delivered_total),
            router_dropped_late: self.drain_counter(&self.router_dropped_late_total),
            router_skipped_non_local: self.drain_counter(&self.router_skipped_non_local_total),
            router_source_route_errors: self.drain_map(&self.router_source_route_errors_total),
            rule_events: self.drain_map(&self.rule_events_total),
            rule_matches: self.drain_map(&self.rule_matches_total),
            rule_instances: self.read_gauge_map(&self.rule_instances),
            rule_memory_bytes: self.read_gauge_map(&self.rule_memory_bytes),
            rule_cursor_gaps: self.drain_gap_map(&self.rule_cursor_gap_total),
            rule_stats_over_limit: self.drain_map(&self.rule_stats_over_limit_total),
            alert_emitted: self.drain_map(&self.alert_emitted_total),
            alert_emitted_detail: self.drain_emitted_detail(),
            alert_channel_send_failed: self.drain_counter(&self.alert_channel_send_failed_total),
            alert_sink_dispatch_failed: self.drain_counter(&self.alert_sink_dispatch_failed_total),
            alert_channel_full: self.drain_counter(&self.alert_channel_full_total),
            alert_channel_depth: self.alert_channel_depth.load(Ordering::Relaxed),
            alert_append_failed: self.drain_counter(&self.alert_append_failed_total),
            alert_dispatch: self.drain_counter(&self.alert_dispatch_total),
            alert_no_sink_records: self.drain_counter(&self.alert_no_sink_records_total),
            alert_drain_dropped_records: self
                .drain_counter(&self.alert_drain_dropped_records_total),
            alert_escalate_failed: self.drain_counter(&self.alert_escalate_failed_total),
            alert_append_nanos: self.drain_counter(&self.alert_append_nanos),
            evictor_sweeps: self.drain_counter(&self.evictor_sweeps_total),
            evictor_time_evicted: self.drain_counter(&self.evictor_time_evicted_total),
            evictor_memory_evicted: self.drain_counter(&self.evictor_memory_evicted_total),
            window_memory_bytes: self.read_map(&self.window_memory_bytes),
            window_allocated_bytes: self.read_map(&self.window_allocated_bytes),
            window_fanout_queued: self.read_map(&self.window_fanout_queued),
            window_fanout_capacity: self.read_map(&self.window_fanout_capacity),
            window_mailbox_inflight: self.read_map(&self.window_mailbox_inflight),
            window_mailbox_budget: self.read_map(&self.window_mailbox_budget),
            window_capacity_bytes: self.read_map(&self.window_capacity_bytes),
            window_rows: self.read_map(&self.window_rows),
            window_batches: self.read_map(&self.window_batches),
            window_acked_lag: self.read_map(&self.window_acked_lag),
            window_append: self.drain_map(&self.window_append_total),
            window_evict: self.drain_map(&self.window_evict_total),
            window_late: self.drain_map(&self.window_late_total),
            receiver_decode_latency: self.receiver_decode_seconds.snapshot(),
            alert_dispatch_latency: self.alert_dispatch_seconds.snapshot(),
            event_e2e_latency: self.event_e2e_latency_seconds.snapshot(),
            rule_scan_timeout: self
                .rule_scan_timeout_seconds
                .iter()
                .map(|(k, v)| (k.clone(), v.snapshot()))
                .collect(),
            rule_flush: self
                .rule_flush_seconds
                .iter()
                .map(|(k, v)| (k.clone(), v.snapshot()))
                .collect(),
            // 分配器读数（provider 未装入则 None）。
            alloc: alloc_stats::read(),
        }
    }
    fn drain_counter(&self, c: &AtomicU64) -> u64 {
        c.swap(0, Ordering::Relaxed)
    }
    fn drain_map(&self, m: &BTreeMap<String, AtomicU64>) -> BTreeMap<String, u64> {
        m.iter()
            .map(|(k, v)| (k.clone(), v.swap(0, Ordering::Relaxed)))
            .collect()
    }
    fn drain_gap_map(
        &self,
        m: &BTreeMap<String, BTreeMap<String, AtomicU64>>,
    ) -> BTreeMap<String, BTreeMap<String, u64>> {
        m.iter()
            .map(|(rule, windows)| {
                (
                    rule.clone(),
                    windows
                        .iter()
                        .map(|(w, v)| (w.clone(), v.swap(0, Ordering::Relaxed)))
                        .collect(),
                )
            })
            .collect()
    }
    fn read_map(&self, m: &BTreeMap<String, AtomicU64>) -> BTreeMap<String, u64> {
        m.iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect()
    }

    /// Read the signed `rule_instances` gauge, clamping negatives to zero.
    fn read_gauge_map(&self, m: &BTreeMap<String, AtomicI64>) -> BTreeMap<String, u64> {
        m.iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed).max(0) as u64))
            .collect()
    }

    pub fn add_evict_report(&self, report: &EvictReport) {
        self.evictor_sweeps_total.fetch_add(1, Ordering::Relaxed);
        self.evictor_time_evicted_total
            .fetch_add(report.batches_time_evicted as u64, Ordering::Relaxed);
        self.evictor_memory_evicted_total
            .fetch_add(report.batches_memory_evicted as u64, Ordering::Relaxed);
        for w in &report.per_window_evicted {
            self.add_window_evict(&w.window_name, w.time_evicted as u64);
        }
    }

    pub(crate) fn summary_line(&self) -> String {
        format!(
            "rx_rows={} routed={} dropped_late={} matches={} alerts={} window_bytes={}",
            self.receiver_rows_total.load(Ordering::Relaxed),
            self.router_delivered_total.load(Ordering::Relaxed),
            self.router_dropped_late_total.load(Ordering::Relaxed),
            self.total_rule_matches(),
            self.alert_emitted_total
                .values()
                .map(|v| v.load(Ordering::Relaxed))
                .sum::<u64>(),
            self.total_window_bytes()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics() -> RuntimeMetrics {
        RuntimeMetrics::new(&[], &[], &[], BTreeMap::new())
    }

    #[test]
    fn emitted_detail_drain_clears_and_counts() {
        let m = metrics();
        m.inc_alert_emitted("r1", "m1", "s1");
        m.inc_alert_emitted("r1", "m1", "s1");
        m.inc_alert_emitted("r1", "m1", "s2");
        // machine 与 scope 均空 → 不落 detail
        m.inc_alert_emitted_detail("r1", "", "");
        let d = m.drain_emitted_detail();
        assert_eq!(d["r1"]["m1"]["s1"], 2);
        assert_eq!(d["r1"]["m1"]["s2"], 1);
        assert!(!d["r1"].contains_key("-"));
        // 二次 drain 为空
        assert!(m.drain_emitted_detail().is_empty());
    }

    #[test]
    fn source_machine_rows_two_level_drain() {
        let m = metrics();
        m.add_receiver_source_machine_rows("s1", "node-1", 3);
        m.add_receiver_source_machine_rows("s1", "node-1", 2);
        m.add_receiver_source_machine_rows("s1", "node-2", 4);
        let d = m.drain_source_machine_rows();
        assert_eq!(d["s1"]["node-1"], 5);
        assert_eq!(d["s1"]["node-2"], 4);
        assert!(m.drain_source_machine_rows().is_empty());
    }

    #[test]
    fn window_miss_two_level_drain() {
        let m = metrics();
        m.add_receiver_window_miss("src", "late", 2);
        m.add_receiver_window_miss("src", "late", 3);
        m.add_receiver_window_miss("src", "other", 1);
        let d = m.drain_receiver_window_misses();
        assert_eq!(d["src"]["late"], 5);
        assert_eq!(d["src"]["other"], 1);
        assert!(m.drain_receiver_window_misses().is_empty());
    }
}
