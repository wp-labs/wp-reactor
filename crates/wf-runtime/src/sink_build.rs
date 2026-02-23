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
pub async fn build_sink_dispatcher(
    bundle: &SinkConfigBundle,
    registry: &SinkFactoryRegistry,
    work_root: &Path,
) -> anyhow::Result<SinkDispatcher> {
    let ctx = SinkBuildCtx::new(work_root.to_path_buf());

    // Build business groups
    let mut business = Vec::new();
    for flex in &bundle.business {
        let sinks = build_sink_runtimes(&flex.sinks, &flex.tags, registry, &ctx).await?;
        let windows = WildArray::new(flex.windows.raw_patterns());
        business.push((flex.name.clone(), windows, sinks));
    }

    // Build infra default group
    let default_group = if let Some(ref fixed) = bundle.infra_default {
        let sinks = build_sink_runtimes(&fixed.sinks, &[], registry, &ctx).await?;
        Some((fixed.name.clone(), sinks))
    } else {
        None
    };

    // Build infra error group
    let error_group = if let Some(ref fixed) = bundle.infra_error {
        let sinks = build_sink_runtimes(&fixed.sinks, &[], registry, &ctx).await?;
        Some((fixed.name.clone(), sinks))
    } else {
        None
    };

    Ok(SinkDispatcher::new(business, default_group, error_group))
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
