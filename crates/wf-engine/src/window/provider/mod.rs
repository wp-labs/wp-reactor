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
}
