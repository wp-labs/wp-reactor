//! ProviderWindow — a window backed by an external data source (knowdb).
//!
//! Unlike BufferWindow which receives events from streams, ProviderWindow
//! loads data from knowdb (CSV/SQLite/Postgres) at bootstrap or on refresh.
//! Join operations read from an in-memory HashMap — no per-event SQL queries.

use std::collections::HashMap;
use std::sync::Arc;

use crate::match_engine::{EngineHashMap, Event, JoinKey, JoinRow, Value};

/// A window whose data comes from an external provider rather than event streams.
///
/// Data is loaded once (or on refresh) into a local HashMap. All lookups are
/// in-memory. Per-event SQL queries are explicitly prohibited by design.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
pub struct ProviderWindow {
    /// knowdb table name this window maps to.
    pub table: String,
    /// SQL query to load data (default: `SELECT * FROM <table>`).
    pub query: String,
    /// Refresh interval. `None` means static (never refresh).
    pub refresh: Option<std::time::Duration>,
    /// Loaded data: field_name → values, keyed by the first column in columns.by_header.
    rows: Vec<HashMap<String, Value>>,
    /// Join key field (set via [`Self::set_join_key`]) and the row index built
    /// from it. Without the index, `join_lookup` scans all rows per event —
    /// fine for tiny tables but O(rows×events) on side-input joins (q13:
    /// 10k rows × 920k bids 卡死). Index build is O(rows) once. Key type is
    /// [`JoinKey`] (same truncation semantics as the buffer-window join index).
    join_key: Option<String>,
    join_index: Option<EngineHashMap<JoinKey, Vec<usize>>>,
    /// 预物化 join 行（`Arc<Event>`，静态表构建一次）——与 `join_index` 同步
    /// 重建。join 命中返回 Arc clone，避免每行重建 `Event` + HashMap（q13b
    /// 30M 行 × 每行 Arc 分配 + 2 字段 HashMap 构建 + Value clone 的 per-row
    /// churn；对齐 q13b_join_bench `IndexedLookup` 的预物化模式）。
    join_rows: Option<EngineHashMap<JoinKey, Vec<Arc<Event>>>>,
}

impl ProviderWindow {
    /// Create a new ProviderWindow with no data loaded yet.
    pub fn new(table: String, query: String, refresh: Option<std::time::Duration>) -> Self {
        Self {
            table,
            query,
            refresh,
            rows: Vec::new(),
            join_key: None,
            join_index: None,
            join_rows: None,
        }
    }

    /// Replace the cached data with newly loaded rows.
    pub fn load(&mut self, new_rows: Vec<HashMap<String, Value>>) {
        self.rows = new_rows;
        // Rebuild the join index if a join key is configured (rows replaced).
        if self.join_key.is_some() {
            self.rebuild_join_index();
        }
    }

    /// Set the join key field and build the O(rows) hash index for O(1) lookups.
    pub fn set_join_key(&mut self, key: String) {
        self.join_key = Some(key);
        self.rebuild_join_index();
    }

    /// 当前 join 键字段（spawn 期 `set_join_key` 设置后不再变；刷新侧在锁外
    /// 整建新窗口前读一次配置用）。`None` = 未配置索引（lookup 回退全表扫描）。
    pub fn join_key(&self) -> Option<&str> {
        self.join_key.as_deref()
    }

    fn rebuild_join_index(&mut self) {
        let Some(key) = self.join_key.as_deref() else {
            self.join_index = None;
            self.join_rows = None;
            return;
        };
        let mut index: EngineHashMap<JoinKey, Vec<usize>> = EngineHashMap::default();
        let mut rows_idx: EngineHashMap<JoinKey, Vec<Arc<Event>>> = EngineHashMap::default();
        for (i, row) in self.rows.iter().enumerate() {
            if let Some(v) = row.get(key)
                && let Some(join_key) = JoinKey::from_value(v)
            {
                index.entry(join_key.clone()).or_default().push(i);
                // 预物化：Arc<Event> 每行一次；字段构建与旧路径（window_lookup
                // 每行重建）字节一致。静态表行不变，之后命中仅 Arc clone。
                let ev = Arc::new(Event {
                    fields: row
                        .iter()
                        .map(|(k, v)| (k.as_str().into(), v.clone()))
                        .collect(),
                });
                rows_idx.entry(join_key).or_default().push(ev);
            }
        }
        self.join_index = Some(index);
        self.join_rows = Some(rows_idx);
    }

    /// Indexed join lookup by the configured key. Returns row references for
    /// every row whose key equals `key` (same [`JoinKey`] truncation semantics
    /// as the scan path). `None` when no index is set or the key misses.
    pub fn join_lookup(&self, key: &Value) -> Option<Vec<&HashMap<String, Value>>> {
        let index = self.join_index.as_ref()?;
        let join_key = JoinKey::from_value(key)?;
        Some(
            index
                .get(&join_key)?
                .iter()
                .map(|&i| &self.rows[i])
                .collect(),
        )
    }

    /// 预物化 join 行 lookup（O(1)）：返回 `Arc<Event>` 行（构建一次，命中仅
    /// Arc clone）——对齐 q13b_join_bench `IndexedLookup` 的预物化模式。
    /// 生产此前（window_lookup）每行重建 Event + HashMap；本方法零重建。
    /// `None` = 无索引 / key 未命中（调用方回退扫描，与原 `join_lookup` 一致）。
    pub fn join_rows_lookup(&self, key: &Value) -> Option<Vec<JoinRow>> {
        let join_key = JoinKey::from_value(key)?;
        Some(
            self.join_rows
                .as_ref()?
                .get(&join_key)?
                .iter()
                .cloned()
                .map(JoinRow::Event)
                .collect(),
        )
    }

    /// Return a snapshot of all loaded rows.
    pub fn snapshot(&self) -> Vec<HashMap<String, Value>> {
        self.rows.clone()
    }

    /// Apply an in-place update to cached rows.
    ///
    /// The mutable rows reference is scoped to the callback so callers cannot
    /// keep it beyond the update.
    pub fn update_rows<R>(&mut self, f: impl FnOnce(&mut Vec<HashMap<String, Value>>) -> R) -> R {
        f(&mut self.rows)
    }

    /// Number of loaded rows.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Whether the window has loaded data.
    pub fn is_loaded(&self) -> bool {
        !self.rows.is_empty()
    }

    /// **锁外整建**：以给定配置（table/query/refresh/join key，均属 spawn 期固定
    /// 配置）与全新 `new_rows` 构建一份完整的新窗口——rows + join 索引 + 预物化
    /// 行在构建时全部完成（O(rows) 重活）。供刷新侧 double-buffer swap：重活不
    /// 占写锁，构建期间读者继续用旧表。
    pub fn rebuilt(
        table: String,
        query: String,
        refresh: Option<std::time::Duration>,
        join_key: Option<String>,
        new_rows: Vec<HashMap<String, Value>>,
    ) -> Self {
        let mut window = Self::new(table, query, refresh);
        window.join_key = join_key;
        // load 在无并发上下文（新对象，尚无读者/写者）重建派生索引。
        window.load(new_rows);
        window
    }

    /// **写锁内 O(1) 整窗换入**（double-buffer swap 收尾）：旧窗口结构体整体被
    /// 替换释放，持读锁的读者因写锁排他在换入前已全部退出——读者看到的是完整
    /// 旧表或完整新表，永无中间态。调用方负责在锁外完成 [`Self::rebuilt`]。
    ///
    /// **并发键配置保护**：锁外整建期间另一线程可能 `set_join_key`（热加载/规则
    /// 变更）——若 `other` 携带的键与当前窗口不一致，以**当前**键为准就地重建
    /// `other` 的索引（此时才 O(rows)，仅在竞态窗口发生；无竞态时纯 O(1) 换入）。
    pub fn swap_in(&mut self, mut other: Self) {
        if self.join_key.as_deref() != other.join_key.as_deref() {
            other.join_key = self.join_key.clone();
            other.rebuild_join_index();
        }
        *self = other;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_window_starts_empty() {
        let w = ProviderWindow::new("test".into(), "SELECT * FROM test".into(), None);
        assert!(w.snapshot().is_empty());
        assert!(!w.is_loaded());
    }

    #[test]
    fn load_and_snapshot() {
        let mut w = ProviderWindow::new("t".into(), "SELECT * FROM t".into(), None);
        let rows = vec![{
            let mut m = HashMap::new();
            m.insert("sip".into(), Value::Str("10.0.0.1".into()));
            m
        }];
        w.load(rows.clone());
        assert!(w.is_loaded());
        assert_eq!(w.row_count(), 1);
        assert_eq!(w.snapshot(), rows);
    }

    #[test]
    fn set_join_key_builds_index_and_lookup_is_o1() {
        // 2026-08-23 q13：provider 窗口 join 索引——无索引时 join_lookup 全表
        // 扫描（10k 行 × 920k 事件卡死）；set_join_key 建 O(rows) 索引，lookup O(1)。
        let mut w = ProviderWindow::new("t".into(), "SELECT * FROM t".into(), None);
        let rows = vec![
            {
                let mut m = HashMap::new();
                m.insert("key".into(), Value::Number(1.0));
                m.insert("value".into(), Value::Str("a".into()));
                m
            },
            {
                let mut m = HashMap::new();
                m.insert("key".into(), Value::Number(2.0));
                m.insert("value".into(), Value::Str("b".into()));
                m
            },
            {
                let mut m = HashMap::new();
                m.insert("key".into(), Value::Number(1.0));
                m.insert("value".into(), Value::Str("c".into()));
                m
            },
        ];
        w.load(rows);
        assert!(
            w.join_lookup(&Value::Number(1.0)).is_none(),
            "无索引时返回 None（回退扫描）"
        );
        w.set_join_key("key".into());
        let hits = w.join_lookup(&Value::Number(1.0)).expect("索引命中");
        assert_eq!(hits.len(), 2, "key=1 两行（a/c）");
        assert!(w.join_lookup(&Value::Number(3.0)).is_none(), "miss → None");
        // load 替换 rows 后索引重建（防陈旧索引返回错行）。
        w.load(vec![{
            let mut m = HashMap::new();
            m.insert("key".into(), Value::Number(5.0));
            m.insert("value".into(), Value::Str("e".into()));
            m
        }]);
        assert_eq!(
            w.join_lookup(&Value::Number(5.0))
                .expect("重建后命中")
                .len(),
            1
        );
        assert!(
            w.join_lookup(&Value::Number(1.0)).is_none(),
            "旧 key 已不在新索引"
        );
    }

    #[test]
    fn rebuilt_swap_in_equals_load_then_set_join_key() {
        // 刷新路径（锁外 rebuilt + 写锁内 swap_in）必须与经典路径
        // （load + set_join_key）行为完全一致：行数、快照、索引命中与 miss 等价。
        let rows = vec![
            {
                let mut m = HashMap::new();
                m.insert("key".into(), Value::Number(1.0));
                m.insert("value".into(), Value::Str("a".into()));
                m
            },
            {
                let mut m = HashMap::new();
                m.insert("key".into(), Value::Number(2.0));
                m.insert("value".into(), Value::Str("b".into()));
                m
            },
            {
                let mut m = HashMap::new();
                m.insert("key".into(), Value::Number(1.0));
                m.insert("value".into(), Value::Str("c".into()));
                m
            },
        ];
        let cfg = ("t".to_string(), "SELECT * FROM t".to_string(), None);

        let mut classic = ProviderWindow::new(cfg.0.clone(), cfg.1.clone(), cfg.2);
        classic.load(rows.clone());
        classic.set_join_key("key".into());

        // 引擎真实流程：目标窗口在 spawn 期已配键（set_join_key），refresh 批次
        // 读取同一键整建——swap 时两键一致，O(1) 换入且保留键。
        let mut swapped = ProviderWindow::new(cfg.0.clone(), cfg.1.clone(), cfg.2);
        swapped.set_join_key("key".into());
        swapped.swap_in(ProviderWindow::rebuilt(
            cfg.0.clone(),
            cfg.1.clone(),
            cfg.2,
            Some("key".into()),
            rows,
        ));

        assert_eq!(swapped.row_count(), classic.row_count());
        assert_eq!(swapped.snapshot(), classic.snapshot());
        let hits_swap = swapped
            .join_rows_lookup(&Value::Number(1.0))
            .expect("新窗口索引命中");
        let hits_classic = classic
            .join_rows_lookup(&Value::Number(1.0))
            .expect("经典路径索引命中");
        assert_eq!(hits_swap.len(), hits_classic.len(), "key=1 两行");
        assert!(
            swapped.join_rows_lookup(&Value::Number(3.0)).is_none(),
            "miss"
        );
        assert_eq!(swapped.join_key(), Some("key"));
    }

    #[test]
    fn refresh_swap_is_atomic_to_concurrent_lookups() {
        // 写者反复 double-buffer swap（gen1: key=1 两行 a1/a2 ↔ gen2: key=1 单行
        // b），读者并发 lookup：每次返回的行数要么来自 gen1（2）要么 gen2（1），
        // 且内容与行数自洽——永无半新半旧中间态，也无死锁/恐慌。
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;

        let gen1 = vec![
            {
                let mut m = HashMap::new();
                m.insert("key".into(), Value::Number(1.0));
                m.insert("value".into(), Value::Str("a1".into()));
                m
            },
            {
                let mut m = HashMap::new();
                m.insert("key".into(), Value::Number(1.0));
                m.insert("value".into(), Value::Str("a2".into()));
                m
            },
        ];
        let gen2 = vec![{
            let mut m = HashMap::new();
            m.insert("key".into(), Value::Number(1.0));
            m.insert("value".into(), Value::Str("b".into()));
            m
        }];
        let window = Arc::new(std::sync::RwLock::new(ProviderWindow::rebuilt(
            "t".into(),
            "SELECT * FROM t".into(),
            None,
            Some("key".into()),
            gen1.clone(),
        )));
        let stop = Arc::new(AtomicBool::new(false));

        let w = Arc::clone(&window);
        let s = Arc::clone(&stop);
        let writer = thread::spawn(move || {
            for i in 0..2000u32 {
                let next = ProviderWindow::rebuilt(
                    "t".into(),
                    "SELECT * FROM t".into(),
                    None,
                    Some("key".into()),
                    if i % 2 == 0 {
                        gen1.clone()
                    } else {
                        gen2.clone()
                    },
                );
                w.write().expect("write lock").swap_in(next);
            }
            s.store(true, Ordering::SeqCst);
        });

        let mut readers = Vec::new();
        for _ in 0..4 {
            let w = Arc::clone(&window);
            let s = Arc::clone(&stop);
            readers.push(thread::spawn(move || {
                while !s.load(Ordering::SeqCst) {
                    let guard = w.read().expect("read lock");
                    let hits = guard
                        .join_rows_lookup(&Value::Number(1.0))
                        .expect("key=1 始终命中");
                    match hits.len() {
                        // 每代数据行数固定——索引与行必须同代（RwLock 原子换入）。
                        2 => {
                            let mut values: Vec<String> = hits
                                .iter()
                                .map(|r| match r.field_value("value") {
                                    Some(Value::Str(v)) => v.to_string(),
                                    _ => panic!("gen1 value 列缺失"),
                                })
                                .collect();
                            values.sort_unstable();
                            assert_eq!(
                                values,
                                ["a1".to_string(), "a2".to_string()],
                                "gen1 两行同代"
                            );
                        }
                        1 => {
                            assert_eq!(
                                hits[0].field_value("value"),
                                Some(Value::Str("b".into())),
                                "gen2 单行同代"
                            );
                        }
                        n => panic!("撕裂读：一次命中 {n} 行"),
                    }
                }
            }));
        }

        writer.join().expect("writer panicked");
        for r in readers {
            r.join().expect("reader panicked");
        }
    }

    #[test]
    fn swap_in_preserves_concurrently_set_join_key() {
        // 锁外整建（仅知旧键 None）与 swap 之间，另一线程 `set_join_key`——
        // swap_in 必须以**当前**键为准（重建数据集索引），不得用旧值覆盖丢键。
        let rows = vec![
            {
                let mut m = HashMap::new();
                m.insert("key".into(), Value::Number(1.0));
                m.insert("value".into(), Value::Str("a".into()));
                m
            },
            {
                let mut m = HashMap::new();
                m.insert("key".into(), Value::Number(2.0));
                m.insert("value".into(), Value::Str("b".into()));
                m
            },
        ];
        let mut current = ProviderWindow::new("t".into(), "SELECT * FROM t".into(), None);
        current.load(rows.clone());
        // 刷新批次在锁外构建时只看到 None 键（旧配置快照）。
        let replacement =
            ProviderWindow::rebuilt("t".into(), "SELECT * FROM t".into(), None, None, rows);
        // 并发方（热加载注册新 join）在 swap 前设键。
        current.set_join_key("key".into());
        // 写锁内换入。
        current.swap_in(replacement);

        assert_eq!(current.join_key(), Some("key"), "并发新键不得被旧快照覆盖");
        assert_eq!(
            current
                .join_rows_lookup(&Value::Number(1.0))
                .expect("索引命中")
                .len(),
            1,
            "按当前键重建的索引可命中"
        );
        assert!(current.join_rows_lookup(&Value::Number(3.0)).is_none());
    }
}
