use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::{bail, Result};
use arrow::record_batch::RecordBatch;
use wf_config::{DistMode, WindowConfig};

use super::buffer::{Window, WindowParams};

// ---------------------------------------------------------------------------
// WindowDef — construction input
// ---------------------------------------------------------------------------

/// Everything needed to create a [`Window`] and wire its subscriptions.
///
/// The caller (compiler bridge) converts `WindowSchema` → `WindowDef` so that
/// wf-core stays free of wf-lang / compiler dependencies.
pub struct WindowDef {
    pub params: WindowParams,
    /// Stream tags this window subscribes to.
    pub streams: Vec<String>,
    pub config: WindowConfig,
}

// ---------------------------------------------------------------------------
// Subscription — internal routing entry
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Subscription {
    window_name: String,
    mode: DistMode,
}

// ---------------------------------------------------------------------------
// WindowRegistry
// ---------------------------------------------------------------------------

/// Central structure holding all [`Window`] instances and a subscription
/// routing table that maps stream tags → windows.
pub struct WindowRegistry {
    windows: HashMap<String, Arc<RwLock<Window>>>,
    subscriptions: HashMap<String, Vec<Subscription>>,
}

impl std::fmt::Debug for WindowRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowRegistry")
            .field("window_count", &self.windows.len())
            .field("subscription_streams", &self.subscriptions.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl WindowRegistry {
    /// Build a registry from a list of window definitions.
    ///
    /// Returns `Err` if two definitions share the same window name.
    pub fn build(defs: Vec<WindowDef>) -> Result<Self> {
        let mut windows = HashMap::with_capacity(defs.len());
        let mut subscriptions: HashMap<String, Vec<Subscription>> = HashMap::new();

        for def in defs {
            let name = def.params.name.clone();
            if windows.contains_key(&name) {
                bail!("duplicate window name: {:?}", name);
            }

            let mode = def.config.mode.clone();
            let window = Window::new(def.params, def.config);
            windows.insert(name.clone(), Arc::new(RwLock::new(window)));

            for stream_tag in def.streams {
                subscriptions
                    .entry(stream_tag)
                    .or_default()
                    .push(Subscription {
                        window_name: name.clone(),
                        mode: mode.clone(),
                    });
            }
        }

        Ok(Self {
            windows,
            subscriptions,
        })
    }

    /// Route a [`RecordBatch`] to all windows subscribed to `stream_tag`.
    ///
    /// Only `DistMode::Local` subscriptions are handled here; `Replicated`
    /// and `Partitioned` are skipped (deferred to M10 Router).
    /// Unknown stream tags are a no-op (returns `Ok(())`).
    pub fn route(&self, stream_tag: &str, batch: RecordBatch) -> Result<()> {
        let Some(subs) = self.subscriptions.get(stream_tag) else {
            return Ok(());
        };

        for sub in subs {
            if !matches!(sub.mode, DistMode::Local) {
                continue;
            }
            let win_lock = self
                .windows
                .get(&sub.window_name)
                .expect("subscription references non-existent window");
            let mut win = win_lock.write().expect("window lock poisoned");
            win.append(batch.clone())?;
        }

        Ok(())
    }

    /// Lookup a window by name.
    pub fn get_window(&self, name: &str) -> Option<&Arc<RwLock<Window>>> {
        self.windows.get(name)
    }

    /// Convenience: acquire a read lock on the named window and return its
    /// snapshot.
    pub fn snapshot(&self, name: &str) -> Option<Vec<RecordBatch>> {
        let win_lock = self.windows.get(name)?;
        let win = win_lock.read().expect("window lock poisoned");
        Some(win.snapshot())
    }

    /// Iterator over all window names.
    pub fn window_names(&self) -> impl Iterator<Item = &str> {
        self.windows.keys().map(|s| s.as_str())
    }

    /// Check whether a window with the given name exists.
    pub fn contains(&self, name: &str) -> bool {
        self.windows.contains_key(name)
    }

    /// Number of windows in the registry.
    pub fn len(&self) -> usize {
        self.windows.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.windows.is_empty()
    }

    /// Returns `(window_name, dist_mode)` pairs for all subscribers of a stream
    /// tag. Used internally by [`super::router::Router`].
    pub(crate) fn subscribers_of(&self, stream_tag: &str) -> Vec<(&str, &DistMode)> {
        match self.subscriptions.get(stream_tag) {
            Some(subs) => subs
                .iter()
                .map(|s| (s.window_name.as_str(), &s.mode))
                .collect(),
            None => Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use std::time::Duration;
    use wf_config::{EvictPolicy, LatePolicy};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn make_batch(schema: &SchemaRef, times: &[i64], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(times.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn test_config() -> WindowConfig {
        WindowConfig {
            name: "default".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(5).into(),
            allowed_lateness: Duration::from_secs(0).into(),
            late_policy: LatePolicy::Drop,
        }
    }

    fn make_def(name: &str, streams: Vec<&str>, mode: DistMode) -> WindowDef {
        let mut config = test_config();
        config.mode = mode;
        WindowDef {
            params: WindowParams {
                name: name.into(),
                schema: test_schema(),
                time_col_index: Some(0),
                over: Duration::from_secs(60),
            },
            streams: streams.into_iter().map(String::from).collect(),
            config,
        }
    }

    // -- 1. build_and_query_windows ------------------------------------------

    #[test]
    fn build_and_query_windows() {
        let reg = WindowRegistry::build(vec![
            make_def("win_a", vec!["s1"], DistMode::Local),
            make_def("win_b", vec!["s2"], DistMode::Local),
        ])
        .unwrap();

        assert_eq!(reg.len(), 2);
        assert!(!reg.is_empty());
        assert!(reg.contains("win_a"));
        assert!(reg.contains("win_b"));
        assert!(!reg.contains("win_c"));
        assert!(reg.get_window("win_a").is_some());
        assert!(reg.get_window("win_b").is_some());
        assert!(reg.get_window("win_c").is_none());

        let mut names: Vec<&str> = reg.window_names().collect();
        names.sort();
        assert_eq!(names, vec!["win_a", "win_b"]);
    }

    // -- 2. duplicate_name_rejected ------------------------------------------

    #[test]
    fn duplicate_name_rejected() {
        let result = WindowRegistry::build(vec![
            make_def("dup", vec!["s1"], DistMode::Local),
            make_def("dup", vec!["s2"], DistMode::Local),
        ]);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("duplicate"), "error should mention duplicate: {msg}");
    }

    // -- 3. route_single_stream ----------------------------------------------

    #[test]
    fn route_single_stream() {
        let reg = WindowRegistry::build(vec![
            make_def("win_auth", vec!["auth"], DistMode::Local),
        ])
        .unwrap();

        let schema = test_schema();
        let batch = make_batch(&schema, &[1_000_000_000], &[42]);
        reg.route("auth", batch).unwrap();

        let snap = reg.snapshot("win_auth").unwrap();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].num_rows(), 1);
    }

    // -- 4. route_multi_stream_union -----------------------------------------

    #[test]
    fn route_multi_stream_union() {
        let reg = WindowRegistry::build(vec![
            make_def("logs", vec!["syslog", "winlog"], DistMode::Local),
        ])
        .unwrap();

        let schema = test_schema();
        reg.route("syslog", make_batch(&schema, &[1_000_000_000], &[10]))
            .unwrap();
        reg.route("winlog", make_batch(&schema, &[2_000_000_000], &[20]))
            .unwrap();

        let snap = reg.snapshot("logs").unwrap();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].num_rows(), 1);
        assert_eq!(snap[1].num_rows(), 1);
    }

    // -- 5. route_to_multiple_windows ----------------------------------------

    #[test]
    fn route_to_multiple_windows() {
        let reg = WindowRegistry::build(vec![
            make_def("win_a", vec!["events"], DistMode::Local),
            make_def("win_b", vec!["events"], DistMode::Local),
        ])
        .unwrap();

        let schema = test_schema();
        reg.route("events", make_batch(&schema, &[1_000_000_000], &[99]))
            .unwrap();

        let snap_a = reg.snapshot("win_a").unwrap();
        let snap_b = reg.snapshot("win_b").unwrap();
        assert_eq!(snap_a.len(), 1);
        assert_eq!(snap_b.len(), 1);
    }

    // -- 6. route_unknown_stream_is_noop -------------------------------------

    #[test]
    fn route_unknown_stream_is_noop() {
        let reg = WindowRegistry::build(vec![
            make_def("win_x", vec!["known"], DistMode::Local),
        ])
        .unwrap();

        let schema = test_schema();
        // Route to a stream with no subscribers.
        reg.route("unknown", make_batch(&schema, &[1_000_000_000], &[1]))
            .unwrap();

        // The existing window should remain empty.
        let snap = reg.snapshot("win_x").unwrap();
        assert!(snap.is_empty());
    }

    // -- 7. snapshot_through_registry ----------------------------------------

    #[test]
    fn snapshot_through_registry() {
        let reg = WindowRegistry::build(vec![
            make_def("snap_win", vec!["data"], DistMode::Local),
        ])
        .unwrap();

        let schema = test_schema();
        reg.route("data", make_batch(&schema, &[1_000_000_000], &[100]))
            .unwrap();
        reg.route("data", make_batch(&schema, &[2_000_000_000], &[200]))
            .unwrap();

        let snap = reg.snapshot("snap_win").unwrap();
        assert_eq!(snap.len(), 2);

        // Non-existent window returns None.
        assert!(reg.snapshot("no_such_window").is_none());
    }

    // -- 8. yield_only_window_not_routed -------------------------------------

    #[test]
    fn yield_only_window_not_routed() {
        let reg = WindowRegistry::build(vec![
            make_def("yield_win", vec![], DistMode::Local),
            make_def("normal_win", vec!["stream_a"], DistMode::Local),
        ])
        .unwrap();

        let schema = test_schema();
        reg.route("stream_a", make_batch(&schema, &[1_000_000_000], &[10]))
            .unwrap();

        // Yield-only window has no data — it has no stream subscriptions.
        let snap_yield = reg.snapshot("yield_win").unwrap();
        assert!(snap_yield.is_empty());

        // Normal window received data.
        let snap_normal = reg.snapshot("normal_win").unwrap();
        assert_eq!(snap_normal.len(), 1);
    }
}
