use std::sync::atomic::Ordering;
use std::time::Duration;

use super::Window;

impl Window {
    /// Remove front batches whose max event time is older than `now_nanos - over`.
    ///
    /// This is purely event-time based: it does **not** gate on the consumption
    /// floor (`WindowProgress::min_acked`). A slow rule that has not yet read an
    /// expired batch will see it evicted and observe a pull `gap_detected` on its
    /// next `read_since` / `read_since_with_shard`; it then resumes from the
    /// window's new `oldest_seq`, skipping the gap. This trades a slow rule's
    /// completeness for system-wide window boundedness — one slow rule must not
    /// pin every window's eviction and drag the whole engine down.
    ///
    /// D4（2026-08-25 扩展）：**保留 pin 同样约束时间驱逐**（`retention_floor_ns`）
    /// ——deferred join 的挂起实例需要 `[lo, hi]` 内的右窗行，`over` 只是内存保留
    /// 参数，绝不能因调小 over 删掉评估还要用的行（100M q4 over=1h 精确 /
    /// over=30m 欠发 6-9k 的根因，2026-08-25）。`pinned && event_time_range.1
    /// ≥ retention_ns` 的批（可能含挂起实例需要的行）保留；只删整体在 pin 之
    /// 前的批。与 `evict_oldest_acked` 的 D4 检查逐分支一致。
    ///
    /// No-op for windows without a time column or with `over == Duration::ZERO`.
    ///
    /// This unfettered variant is used when the window has no pull consumers
    /// (push mode — the data was already broadcast before landing in the log);
    /// pull windows use [`Self::evict_expired_acked`] so a lagging rule task
    /// never loses unread batches.
    pub fn evict_expired(&self, now_nanos: i64) {
        self.evict_expired_impl(now_nanos, u64::MAX)
    }

    /// Time eviction gated on the consumption floor: only front batches whose
    /// `seq` is below `acked_floor` (every registered pull consumer has already
    /// read them) are dropped, even when their event time is expired. A
    /// lagging rule task therefore never observes a cursor gap from the
    /// time-eviction sweep; its unread expired batches stay until it catches
    /// up (the memory-pressure phase then reclaims them once acked). With no
    /// pull consumers the floor is `u64::MAX` and behaviour matches
    /// [`Self::evict_expired`] exactly.
    pub fn evict_expired_acked(&self, now_nanos: i64, acked_floor: u64) {
        self.evict_expired_impl(now_nanos, acked_floor)
    }

    fn evict_expired_impl(&self, now_nanos: i64, acked_floor: u64) {
        if self.time_col_index.is_none() || self.over == Duration::ZERO {
            return;
        }

        let over_nanos = self.over.as_nanos() as i64;
        // Saturating: an uninitialized event-time watermark (i64::MIN, e.g.
        // windows appended without `append_with_watermark`) must not panic and
        // must not evict anything (no batch time < i64::MIN).
        let cutoff = now_nanos.saturating_sub(over_nanos);
        // D4：保留 pin（deferred join 评估前沿）同样挡住时间驱逐——`pinned &&
        // event_time_range.1 ≥ retention_ns` 的批可能含挂起实例要评估的行，
        // over 不能删它们（正确性不依赖 over）。`i64::MAX` = 无 pin → 跳过。
        let retention_ns = self.retention_floor_ns();
        let pinned = retention_ns != i64::MAX;

        let mut evicted = false;
        {
            let mut log = self.log.write().expect("window log lock poisoned");
            loop {
                let removable = {
                    let Some((_, tb)) = log.first_key_value() else {
                        break;
                    };
                    tb.event_time_range.1 < cutoff
                        && tb.seq < acked_floor
                        && !(pinned && tb.event_time_range.1 >= retention_ns)
                };
                if !removable {
                    break;
                }
                let (_, tb) = log
                    .pop_first()
                    .expect("front vanished between check and remove");
                let byte_size = tb.byte_size;
                let row_count = tb.row_count;
                self.remove_batch_from_index(&tb);
                // Dropping the owned `TimedBatch` destroys the Arrow batch and
                // parsed events eagerly — no deferred (epoch-GC) reclamation.
                drop(tb);
                self.current_bytes.fetch_sub(byte_size, Ordering::Relaxed);
                self.total_rows.fetch_sub(row_count, Ordering::Relaxed);
                self.batch_count.fetch_sub(1, Ordering::Relaxed);
                evicted = true;
            }
        }
        // Content changed (batches removed): bump the generation so has() /
        // snapshot caches keyed to it invalidate. (append_inner bumps it for
        // append + its own eviction; the evictor sweeps must do the same or a
        // cached distinct-value set goes stale after time eviction.)
        if evicted {
            self.generation.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Pop the oldest (front) batch, returning its byte size.
    ///
    /// Returns `None` if the window is empty.
    pub fn evict_oldest(&self) -> Option<usize> {
        let mut log = self.log.write().expect("window log lock poisoned");
        let (_, tb) = log.pop_first()?;
        let byte_size = tb.byte_size;
        let row_count = tb.row_count;
        self.remove_batch_from_index(&tb);
        drop(tb);
        self.current_bytes.fetch_sub(byte_size, Ordering::Relaxed);
        self.total_rows.fetch_sub(row_count, Ordering::Relaxed);
        self.batch_count.fetch_sub(1, Ordering::Relaxed);
        // Content changed: invalidate has()/snapshot caches (see evict_expired).
        self.generation.fetch_add(1, Ordering::Relaxed);
        Some(byte_size)
    }

    /// Whether the oldest batch is currently held by a D4 retention pin (its
    /// event-time range may contain rows a join-target reader still needs).
    ///
    /// The global evictor consults this **before** selecting a window for
    /// memory reclaim: a pinned front batch would make `evict_oldest_acked`
    /// return `None`, and silently breaking out of the sweep there (instead of
    /// signalling memory pressure) would let the engine keep appending past the
    /// global cap — the OOM risk D4's "exceed the budget rather than lose data"
    /// contract is supposed to prevent (2026-08-24 review fix).
    pub fn front_pinned_by_retention(&self) -> bool {
        let retention_ns = self.retention_floor_ns();
        if retention_ns == i64::MAX {
            return false;
        }
        let log = self.log.read().expect("window log lock poisoned");
        match log.first_key_value() {
            Some((_, tb)) => tb.event_time_range.1 >= retention_ns,
            None => false,
        }
    }

    /// Sequence number of the oldest (front) batch, or `None` if the window
    /// is empty. The evictor's memory-pressure phase uses this to decide
    /// whether the front batch is safe to drop (its `seq` is below the
    /// consumption floor, so every live consumer has already read it).
    pub fn oldest_seq(&self) -> Option<u64> {
        let log = self.log.read().expect("window log lock poisoned");
        log.first_key_value().map(|(_, tb)| tb.seq)
    }

    /// Pop the oldest (front) batch **only if** its `seq` is below the
    /// consumption floor `acked_floor` — i.e. every live consumer has
    /// already acked past it. Returns the reclaimed byte size, or `None`
    /// when the window is empty or its oldest batch is still unacked.
    ///
    /// This is the floor-respecting counterpart of [`Window::evict_oldest`].
    /// The old lossy memory backstop dropped the front unconditionally, which
    /// silently discarded pull-mode batches a slow rule had not yet read —
    /// the q3 pull regression. With this variant the evictor can never lose
    /// unread pull data; when nothing is safe to drop it reports
    /// `memory_pressure` and the actor applies backpressure instead.
    ///
    /// D4: the same guarantee is extended to **join-target** readers, which own
    /// no consumer slot — a batch at or after the window's retention frontier
    /// ([`Window::retention_floor_ns`]) is kept too, so the global memory cap
    /// cannot silently truncate a deferred join's input.
    pub fn evict_oldest_acked(&self, acked_floor: u64) -> Option<usize> {
        let retention_ns = self.retention_floor_ns();
        let pinned = retention_ns != i64::MAX;
        let mut log = self.log.write().expect("window log lock poisoned");
        let removable = {
            let (_, tb) = log.first_key_value()?;
            tb.seq < acked_floor && !(pinned && tb.event_time_range.1 >= retention_ns)
        };
        if !removable {
            return None;
        }
        let (_, tb) = log
            .pop_first()
            .expect("front vanished between check and remove");
        let byte_size = tb.byte_size;
        let row_count = tb.row_count;
        self.remove_batch_from_index(&tb);
        drop(tb);
        self.current_bytes.fetch_sub(byte_size, Ordering::Relaxed);
        self.total_rows.fetch_sub(row_count, Ordering::Relaxed);
        self.batch_count.fetch_sub(1, Ordering::Relaxed);
        // Content changed: invalidate has()/snapshot caches (see evict_expired).
        self.generation.fetch_add(1, Ordering::Relaxed);
        Some(byte_size)
    }
}
