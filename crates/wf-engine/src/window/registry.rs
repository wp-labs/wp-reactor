use crate::match_engine::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::record_batch::RecordBatch;
use orion_error::conversion::ToStructError;
use orion_error::prelude::*;
use tokio::sync::Notify;
use wf_config::{DistMode, WindowConfig};

use crate::error::{CoreReason, CoreResult};

use super::buffer::{Window, WindowParams};
use super::progress::WindowProgress;
use super::provider::ProviderWindow;

// ---------------------------------------------------------------------------
// WindowDef — construction input
// ---------------------------------------------------------------------------

/// Everything needed to create a [`Window`] and wire its subscriptions.
///
/// The caller (compiler bridge) converts `WindowSchema` → `WindowDef` so that
/// wf-core stays free of wf-lang / compiler dependencies.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
pub struct WindowDef {
    pub params: WindowParams,
    /// Stream names this window subscribes to.
    pub streams: Vec<String>,
    pub config: WindowConfig,
}

// ---------------------------------------------------------------------------
// Subscription — internal routing entry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Subscription {
    window_name: String,
    mode: DistMode,
}

// ---------------------------------------------------------------------------
// WindowRegistry
// ---------------------------------------------------------------------------

/// Central structure holding all [`Window`] instances and a subscription
/// routing table that maps stream names → windows.
///
/// All maps are wrapped in `RwLock` so windows can be **added at runtime**
/// (incremental reload, L2) via [`try_add_window`] while reader tasks
/// (router/rule/evictor) hold the `Arc<Router>` that owns this registry.
/// The registry maps keep `std::sync::RwLock` (cold — registration and reload
/// only); the windows themselves are internally synchronized (single writer
/// via the window actor, `RwLock`-guarded batch log — see [`Window`]).
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
pub struct WindowRegistry {
    windows: RwLock<HashMap<String, Arc<Window>>>,
    provider_windows: RwLock<HashMap<String, Arc<RwLock<ProviderWindow>>>>,
    subscriptions: RwLock<HashMap<String, Vec<Subscription>>>,
    notifiers: RwLock<HashMap<String, Arc<Notify>>>,
    progress: RwLock<HashMap<String, Arc<WindowProgress>>>,
}

impl std::fmt::Debug for WindowRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowRegistry")
            .field(
                "window_count",
                &self.windows.read().expect("windows lock poisoned").len(),
            )
            .field(
                "subscription_streams",
                &self
                    .subscriptions
                    .read()
                    .expect("subscriptions lock poisoned")
                    .keys()
                    .collect::<Vec<_>>(),
            )
            .field(
                "notifier_count",
                &self
                    .notifiers
                    .read()
                    .expect("notifiers lock poisoned")
                    .len(),
            )
            .finish()
    }
}

impl WindowRegistry {
    /// Build a registry from a list of window definitions.
    ///
    /// Returns `Err` if two definitions share the same window name.
    pub fn build(defs: Vec<WindowDef>) -> CoreResult<Self> {
        let mut windows = HashMap::with_capacity(defs.len());
        let mut subscriptions: HashMap<String, Vec<Subscription>> = HashMap::new();
        let mut notifiers = HashMap::with_capacity(defs.len());
        let mut progress = HashMap::with_capacity(defs.len());

        for def in defs {
            let name = def.params.name.clone();
            if windows.contains_key(&name) {
                return CoreReason::WindowBuild
                    .to_err()
                    .with_detail(format!("duplicate window name: {:?}", name))
                    .err();
            }

            let mode = def.config.mode.clone();
            let window = Window::new(def.params, def.config);
            windows.insert(name.clone(), Arc::new(window));
            notifiers.insert(name.clone(), Arc::new(Notify::new()));
            progress.insert(name.clone(), Arc::new(WindowProgress::new()));

            for stream_name in def.streams {
                subscriptions
                    .entry(stream_name)
                    .or_default()
                    .push(Subscription {
                        window_name: name.clone(),
                        mode: mode.clone(),
                    });
            }
        }

        Ok(Self {
            windows: RwLock::new(windows),
            provider_windows: RwLock::new(HashMap::new()),
            subscriptions: RwLock::new(subscriptions),
            notifiers: RwLock::new(notifiers),
            progress: RwLock::new(progress),
        })
    }

    /// Consumption-progress table for the named window.
    ///
    /// Rule tasks register slots here at spawn time; the evictor reads
    /// [`WindowProgress::min_acked`] as the eviction floor.
    pub fn progress(&self, name: &str) -> Option<Arc<WindowProgress>> {
        self.progress
            .read()
            .expect("progress lock poisoned")
            .get(name)
            .cloned()
    }

    /// Register a provider window.
    pub fn register_provider(&mut self, name: String, pw: ProviderWindow) -> CoreResult<()> {
        let mut providers = self
            .provider_windows
            .write()
            .expect("provider lock poisoned");
        if providers.contains_key(&name) {
            return CoreReason::WindowBuild
                .to_err()
                .with_detail(format!("duplicate provider window: {:?}", name))
                .err();
        }
        // Provider replaces buffer window if one exists
        self.windows
            .write()
            .expect("windows lock poisoned")
            .remove(&name);
        providers.insert(name, Arc::new(RwLock::new(pw)));
        Ok(())
    }

    /// Get a buffer window (cloned `Arc` — cheap).
    ///
    /// Returns an owned `Arc` so the borrow does not tie to the registry's read
    /// lock guard, letting callers hold it across `.await` / lock the window
    /// independently.
    pub fn get_window(&self, name: &str) -> Option<Arc<Window>> {
        self.windows
            .read()
            .expect("windows lock poisoned")
            .get(name)
            .cloned()
    }

    /// Get a provider window (cloned `Arc`).
    pub fn get_provider(&self, name: &str) -> Option<Arc<RwLock<ProviderWindow>>> {
        self.provider_windows
            .read()
            .expect("provider lock poisoned")
            .get(name)
            .cloned()
    }

    /// Get a snapshot from a provider window.
    pub fn provider_snapshot(&self, name: &str) -> Option<Vec<HashMap<String, Value>>> {
        let providers = self
            .provider_windows
            .read()
            .expect("provider lock poisoned");
        providers
            .get(name)
            .map(|w| w.read().expect("provider window lock poisoned").snapshot())
    }

    /// Route a [`RecordBatch`] to all windows subscribed to `stream_name`.
    ///
    /// Only `DistMode::Local` subscriptions are handled here; `Replicated`
    /// and `Partitioned` are skipped (deferred to M10 Router).
    /// Unknown stream names are a no-op (returns `Ok(())`).
    #[deprecated(note = "Use Router::route for watermark-aware routing")]
    pub fn route(&self, stream_name: &str, batch: RecordBatch) -> CoreResult<()> {
        let subs_guard = self
            .subscriptions
            .read()
            .expect("subscriptions lock poisoned");
        let Some(subs) = subs_guard.get(stream_name) else {
            return Ok(());
        };
        // Snapshot the local subscriptions so we can drop the subscriptions
        // read guard before taking window write guards (lock ordering).
        let local: Vec<Subscription> = subs
            .iter()
            .filter(|s| matches!(s.mode, DistMode::Local))
            .cloned()
            .collect();
        drop(subs_guard);

        let windows = self.windows.read().expect("windows lock poisoned");
        for sub in local {
            let win = windows
                .get(&sub.window_name)
                .expect("subscription references non-existent window");
            win.append_with_watermark(batch.clone())
                .map(|_| ())
                .source_err(CoreReason::WindowBuild, "append batch to window")?;
        }

        Ok(())
    }

    /// Convenience: acquire a read lock on the named window and return its
    /// snapshot.
    pub fn snapshot(&self, name: &str) -> Option<Vec<RecordBatch>> {
        let windows = self.windows.read().expect("windows lock poisoned");
        let win = windows.get(name)?;
        Some(win.snapshot())
    }

    /// All window names (owned, so it survives independent of the read guard).
    pub fn window_names(&self) -> Vec<String> {
        self.windows
            .read()
            .expect("windows lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Check whether a window with the given name exists.
    pub fn contains(&self, name: &str) -> bool {
        self.windows
            .read()
            .expect("windows lock poisoned")
            .contains_key(name)
    }

    /// Number of windows in the registry.
    pub fn len(&self) -> usize {
        self.windows.read().expect("windows lock poisoned").len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.windows
            .read()
            .expect("windows lock poisoned")
            .is_empty()
    }

    /// Returns `(window_name, dist_mode)` pairs for all subscribers of a stream
    /// name. Owned so the result outlives the subscriptions read guard. Used
    /// internally by [`super::router::Router`].
    pub fn subscribers_of(&self, stream_name: &str) -> Vec<(String, DistMode)> {
        match self
            .subscriptions
            .read()
            .expect("subscriptions lock poisoned")
            .get(stream_name)
        {
            Some(subs) => subs
                .iter()
                .map(|s| (s.window_name.clone(), s.mode.clone()))
                .collect(),
            None => Vec::new(),
        }
    }

    /// Get the notifier for a named window (cloned `Arc`).
    pub fn get_notifier(&self, name: &str) -> Option<Arc<Notify>> {
        self.notifiers
            .read()
            .expect("notifiers lock poisoned")
            .get(name)
            .cloned()
    }

    /// **Runtime** add a new buffer window (L2 incremental reload).
    ///
    /// Pure addition: if the window name already exists this returns `Err` and
    /// touches nothing. Otherwise it inserts the window + its notifier + its
    /// stream subscriptions. Existing windows/subscriptions are untouched.
    ///
    /// Lock order is fixed (windows → notifiers → subscriptions) to stay
    /// deadlock-free; reload is low-frequency so holding three write locks in
    /// sequence is fine.
    pub fn try_add_window(&self, def: WindowDef) -> CoreResult<()> {
        let name = def.params.name.clone();
        let mode = def.config.mode.clone();

        // (1) windows — name check + insert under one write guard (no TOCTOU).
        {
            let mut wins = self.windows.write().expect("windows lock poisoned");
            if wins.contains_key(&name) {
                return CoreReason::WindowBuild
                    .to_err()
                    .with_detail(format!("duplicate window name: {:?}", name))
                    .err();
            }
            let window = Window::new(def.params, def.config);
            wins.insert(name.clone(), Arc::new(window));
        }

        // (2) notifiers
        self.notifiers
            .write()
            .expect("notifiers lock poisoned")
            .insert(name.clone(), Arc::new(Notify::new()));

        // (2b) progress table (fresh — old consumers of a replaced window
        // re-register when their tasks re-spawn).
        self.progress
            .write()
            .expect("progress lock poisoned")
            .insert(name.clone(), Arc::new(WindowProgress::new()));

        // (3) subscriptions — append, never overwrite existing entries.
        {
            let mut subs = self
                .subscriptions
                .write()
                .expect("subscriptions lock poisoned");
            for stream_name in def.streams {
                subs.entry(stream_name).or_default().push(Subscription {
                    window_name: name.clone(),
                    mode: mode.clone(),
                });
            }
        }

        Ok(())
    }

    /// **Runtime** replace an existing buffer window (L3 partial rebuild).
    ///
    /// The old window is removed and a new (empty) one is inserted. Its
    /// notifier and stream subscriptions are **replaced** wholesale (old
    /// entries removed, new ones inserted). Requires that the window name
    /// already exists; returns `Err` otherwise.
    ///
    /// **Intermediate-state safety**: the three write locks are acquired
    /// sequentially (windows → notifiers → subscriptions), so between step
    /// (1) and step (3) readers may briefly see the new window combined
    /// with old subscription entries. This is safe because subscriptions
    /// only carry the window **name** (not an `Arc`), and
    /// `get_window(name)` always returns the current `Arc` from the
    /// registry. Mode changes that could cause routing inconsistency are
    /// blocked by `append_effective_config_blockers`.
    ///
    /// Lock order is fixed (windows → notifiers → subscriptions, same as
    /// [`try_add_window`]) to stay deadlock-free; reload is low-frequency.
    pub fn try_replace_window(&self, def: WindowDef) -> CoreResult<()> {
        let name = def.params.name.clone();
        let mode = def.config.mode.clone();

        // (1) windows — verify existence + replace atomically.
        {
            let mut wins = self.windows.write().expect("windows lock poisoned");
            if !wins.contains_key(&name) {
                return CoreReason::WindowBuild
                    .to_err()
                    .with_detail(format!("cannot replace non-existent window: {:?}", name))
                    .err();
            }
            let window = Window::new(def.params, def.config);
            wins.insert(name.clone(), Arc::new(window));
        }

        // (2) notifiers — replace.
        self.notifiers
            .write()
            .expect("notifiers lock poisoned")
            .insert(name.clone(), Arc::new(Notify::new()));

        // (2b) progress table — replace with a fresh one (re-spawned tasks
        // re-register; stale slots from the old task generation must not pin
        // the rebuilt window).
        self.progress
            .write()
            .expect("progress lock poisoned")
            .insert(name.clone(), Arc::new(WindowProgress::new()));

        // (3) subscriptions — remove old entries for this window, then
        //     insert the new stream subscriptions.
        {
            let mut subs = self
                .subscriptions
                .write()
                .expect("subscriptions lock poisoned");
            // Remove every subscription that referenced the old window.
            for entry in subs.values_mut() {
                entry.retain(|s| s.window_name != name);
            }
            // Insert the new subscriptions.
            for stream_name in def.streams {
                subs.entry(stream_name).or_default().push(Subscription {
                    window_name: name.clone(),
                    mode: mode.clone(),
                });
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use std::time::Duration;
    use wf_config::{EvictPolicy, LatePolicy};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
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
            table: None,
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
                materialize_fields: None,
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

        let mut names: Vec<String> = reg.window_names();
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
        assert!(
            msg.contains("duplicate"),
            "error should mention duplicate: {msg}"
        );
    }

    // -- 3. route_single_stream ----------------------------------------------

    #[test]
    fn route_single_stream() {
        let reg = WindowRegistry::build(vec![make_def("win_auth", vec!["auth"], DistMode::Local)])
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
        let reg = WindowRegistry::build(vec![make_def(
            "logs",
            vec!["syslog", "winlog"],
            DistMode::Local,
        )])
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
        let reg =
            WindowRegistry::build(vec![make_def("win_x", vec!["known"], DistMode::Local)]).unwrap();

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
        let reg = WindowRegistry::build(vec![make_def("snap_win", vec!["data"], DistMode::Local)])
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
