//! ProviderWindow — a window backed by an external data source (knowdb).
//!
//! Unlike BufferWindow which receives events from streams, ProviderWindow
//! loads data from knowdb (CSV/SQLite/Postgres) at bootstrap or on refresh.
//! Join operations read from an in-memory HashMap — no per-event SQL queries.

use std::collections::HashMap;

use crate::match_engine::Value;

/// A window whose data comes from an external provider rather than event streams.
///
/// Data is loaded once (or on refresh) into a local HashMap. All lookups are
/// in-memory. Per-event SQL queries are explicitly prohibited by design.
pub struct ProviderWindow {
    /// knowdb table name this window maps to.
    pub table: String,
    /// SQL query to load data (default: `SELECT * FROM <table>`).
    pub query: String,
    /// Refresh interval. `None` means static (never refresh).
    pub refresh: Option<std::time::Duration>,
    /// Loaded data: field_name → values, keyed by the first column in columns.by_header.
    rows: Vec<HashMap<String, Value>>,
}

impl ProviderWindow {
    /// Create a new ProviderWindow with no data loaded yet.
    pub fn new(table: String, query: String, refresh: Option<std::time::Duration>) -> Self {
        Self {
            table,
            query,
            refresh,
            rows: Vec::new(),
        }
    }

    /// Replace the cached data with newly loaded rows.
    pub fn load(&mut self, new_rows: Vec<HashMap<String, Value>>) {
        self.rows = new_rows;
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
}
