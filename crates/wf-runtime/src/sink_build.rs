use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use wp_connector_api::{SinkBuildCtx, SinkFactory, SinkSpec as ResolvedSinkSpec};

use wf_config::sink::{SinkConfigBundle, WildArray};
use wf_core::sink::{SinkDispatcher, SinkRuntime};

// ---------------------------------------------------------------------------
// SinkFactoryRegistry — maps sink kind → factory
// ---------------------------------------------------------------------------

/// Registry of `SinkFactory` implementations keyed by sink kind (e.g. `"file"`).
pub struct SinkFactoryRegistry {
    factories: HashMap<String, Arc<dyn SinkFactory>>,
}

impl SinkFactoryRegistry {
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// Register a factory. The kind is obtained from `factory.kind()`.
    pub fn register(&mut self, factory: Arc<dyn SinkFactory>) {
        self.factories.insert(factory.kind().to_string(), factory);
    }

    fn get(&self, kind: &str) -> Option<&Arc<dyn SinkFactory>> {
        self.factories.get(kind)
    }
}

// ---------------------------------------------------------------------------
// Build SinkDispatcher from config bundle
// ---------------------------------------------------------------------------

/// Construct a `SinkDispatcher` from the loaded config bundle.
///
/// For each `ResolvedSinkSpec` in the bundle, looks up the factory by kind,
/// validates, builds a `SinkHandle`, and wraps it in a `SinkRuntime`.
///
/// The `window_names` parameter lists all known window names from the config.
/// Routes are pre-resolved at build time: each window name is matched against
/// business group wildcard patterns, and the resulting window→sinks mapping
/// is stored in a `HashMap` for O(1) dispatch lookup.
pub async fn build_sink_dispatcher(
    bundle: &SinkConfigBundle,
    registry: &SinkFactoryRegistry,
    work_root: &Path,
    window_names: &[String],
) -> anyhow::Result<SinkDispatcher> {
    let ctx = SinkBuildCtx::new(work_root.to_path_buf());

    // Build business groups (name, compiled windows, sinks)
    let mut business: Vec<(String, WildArray, Vec<Arc<SinkRuntime>>)> = Vec::new();
    for flex in &bundle.business {
        let sinks = build_sink_runtimes(&flex.sinks, &flex.tags, registry, &ctx).await?;
        let windows = WildArray::new(flex.windows.raw_patterns());
        business.push((flex.name.clone(), windows, sinks));
    }

    // Build infra default sinks
    let default_sinks = if let Some(ref fixed) = bundle.infra_default {
        build_sink_runtimes(&fixed.sinks, &[], registry, &ctx).await?
    } else {
        Vec::new()
    };

    // Build infra error sinks
    let error_sinks = if let Some(ref fixed) = bundle.infra_error {
        build_sink_runtimes(&fixed.sinks, &[], registry, &ctx).await?
    } else {
        Vec::new()
    };

    // Pre-resolve routes: window_name → matched sinks
    let mut routes: HashMap<String, Vec<Arc<SinkRuntime>>> = HashMap::new();
    for name in window_names {
        let mut bound = Vec::new();
        for (_group_name, windows, sinks) in &business {
            if windows.matches(name) {
                bound.extend(sinks.iter().cloned());
            }
        }
        routes.insert(name.clone(), bound);
    }

    Ok(SinkDispatcher::new(routes, default_sinks, error_sinks))
}

/// Build `SinkRuntime` instances from resolved specs.
async fn build_sink_runtimes(
    specs: &[ResolvedSinkSpec],
    tags: &[String],
    registry: &SinkFactoryRegistry,
    ctx: &SinkBuildCtx,
) -> anyhow::Result<Vec<Arc<SinkRuntime>>> {
    let mut runtimes = Vec::with_capacity(specs.len());

    for spec in specs {
        let factory = registry.get(&spec.kind).ok_or_else(|| {
            anyhow::anyhow!(
                "no factory registered for sink kind {:?} (connector={:?})",
                spec.kind,
                spec.connector_id,
            )
        })?;

        factory
            .validate_spec(spec)
            .map_err(|e| anyhow::anyhow!("validate sink {:?}: {e}", spec.name))?;

        let handle = factory
            .build(spec, ctx)
            .await
            .map_err(|e| anyhow::anyhow!("build sink {:?}: {e}", spec.name))?;

        runtimes.push(Arc::new(SinkRuntime {
            name: spec.name.clone(),
            spec: spec.clone(),
            handle: tokio::sync::Mutex::new(handle),
            tags: tags.to_vec(),
        }));
    }

    Ok(runtimes)
}
