use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use orion_error::conversion::{SourceErr, ToStructError};
use wp_connector_api::{SinkBuildCtx, SinkFactory};

use crate::error::{RuntimeReason, RuntimeResult};
use wf_config::sink::{ResolvedRouteSink, SinkConfigBundle};
use wf_engine::sink::{SinkDispatcher, SinkRuntime, WfMetaDisableMatcher};

// ---------------------------------------------------------------------------
// SinkFactoryRegistry — maps sink kind → factory
// ---------------------------------------------------------------------------

/// Registry of `SinkFactory` implementations keyed by sink kind (e.g. `"file"`).
#[derive(::moju_derive::MoJu)]
#[moju(
    kind = "struct",
    domain = "Orchestra",
    module = "Orchestra.SinkFactory"
)]
pub struct SinkFactoryRegistry {
    factories: HashMap<String, Arc<dyn SinkFactory>>,
}

impl Default for SinkFactoryRegistry {
    fn default() -> Self {
        Self::new()
    }
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

    /// Import all registered factories from the global `wp_core_connectors` registry.
    /// Callers (e.g., application-level startup) can register additional factories
    /// in the global registry before bootstrap, and they will be picked up here.
    pub fn import_from_global_registry(&mut self) {
        for kind in wp_core_connectors::registry::list_sink_kinds() {
            if let Some(factory) = wp_core_connectors::registry::get_sink_factory(&kind) {
                log::info!("imported sink factory from global registry: kind={kind}");
                self.register(factory);
            }
        }
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
/// Business group wildcard patterns are compiled into the dispatcher so routing
/// can match yield targets that are not part of the startup window list.
pub async fn build_sink_dispatcher(
    bundle: &SinkConfigBundle,
    registry: &SinkFactoryRegistry,
    work_root: &Path,
    window_names: &[String],
) -> RuntimeResult<SinkDispatcher> {
    let ctx = SinkBuildCtx::new(work_root.to_path_buf());

    // Build business routes (raw window patterns, sinks). The dispatcher owns
    // the compiled wildmatch matchers.
    let mut routes: Vec<(Vec<String>, Vec<Arc<SinkRuntime>>)> = Vec::new();
    for flex in &bundle.business {
        let sinks = build_sink_runtimes(
            &flex.sinks,
            &flex.tags,
            &flex.wf_meta_disable,
            flex.parallel,
            registry,
            &ctx,
        )
        .await?;
        routes.push((flex.windows.raw_patterns().to_vec(), sinks));
    }

    // Build infra default sinks
    let default_sinks = if let Some(ref fixed) = bundle.infra_default {
        build_sink_runtimes(&fixed.sinks, &[], &fixed.wf_meta_disable, fixed.parallel, registry, &ctx).await?
    } else {
        Vec::new()
    };

    // Build infra error sinks
    let error_sinks = if let Some(ref fixed) = bundle.infra_error {
        build_sink_runtimes(&fixed.sinks, &[], &fixed.wf_meta_disable, fixed.parallel, registry, &ctx).await?
    } else {
        Vec::new()
    };

    let monitor_sinks = if let Some(ref fixed) = bundle.infra_monitor {
        build_sink_runtimes(&fixed.sinks, &[], &fixed.wf_meta_disable, fixed.parallel, registry, &ctx).await?
    } else {
        Vec::new()
    };

    // Startup guard: if no sink can ever receive an alert (no business routes
    // AND no default fallback), every dispatch is a silent no-op. Fail bootstrap
    // so misconfiguration (e.g. wrong sinks/ layout) is caught immediately
    // instead of vanishing matches into the void. error_sinks / monitor_sinks
    // don't count — they only receive on other-sink failure / metrics.
    let total_routes: usize = routes
        .iter()
        .filter(|(patterns, _)| !patterns.is_empty())
        .map(|(_, sinks)| sinks.len())
        .sum();
    if total_routes == 0 && default_sinks.is_empty() {
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail(format!(
                "no sinks configured — every alert would be dropped. \
                 Expected sinks/ layout: business.d/*.toml (route groups), \
                 infra.d/default.toml (fallback), connectors/sink.d/*.toml \
                 (connector defs) relative to the sinks root. windows loaded={}",
                window_names.len()
            ))
            .err();
    }

    Ok(SinkDispatcher::new(
        routes,
        default_sinks,
        error_sinks,
        monitor_sinks,
    ))
}

/// Build `SinkRuntime` instances from resolved specs.
#[allow(clippy::too_many_arguments)]
async fn build_sink_runtimes(
    specs: &[ResolvedRouteSink],
    tags: &[String],
    wf_meta_disable: &[String],
    parallel: usize,
    registry: &SinkFactoryRegistry,
    ctx: &SinkBuildCtx,
) -> RuntimeResult<Vec<Arc<SinkRuntime>>> {
    let mut runtimes = Vec::with_capacity(specs.len());

    for resolved in specs {
        let spec = &resolved.spec;
        log::info!(
            "building sink: name={:?} kind={:?} connector={:?}",
            spec.name,
            spec.kind,
            spec.connector_id,
        );
        let Some(factory) = registry.get(&spec.kind) else {
            return RuntimeReason::Bootstrap
                .to_err()
                .with_detail(format!(
                    "no factory registered for sink kind {:?} (connector={:?})",
                    spec.kind, spec.connector_id,
                ))
                .err();
        };

        factory.validate_spec(spec).source_err(
            RuntimeReason::Bootstrap,
            format!("validate sink {:?}", spec.name),
        )?;

        let handle = factory.build(spec, ctx).await.map_err(|e| {
            log::error!(
                "build sink failed: name={:?} kind={:?} error={}",
                spec.name,
                spec.kind,
                e
            );
            RuntimeReason::Bootstrap.to_err().with_source(e)
        })?;

        runtimes.push(Arc::new(SinkRuntime {
            name: spec.name.clone(),
            spec: spec.clone(),
            handle: tokio::sync::Mutex::new(handle),
            tags: tags.to_vec(),
            output_fields: resolved.fields.clone(),
            wf_meta_disable: wf_meta_disable.to_vec(),
            wf_meta_disable_matcher: WfMetaDisableMatcher::new(wf_meta_disable),
            parallel,
        }));
    }

    Ok(runtimes)
}
